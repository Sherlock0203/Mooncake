// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// W1c integration tests (design §14.2 #12): the three UbWorkers seams —
//   * probe completion queues are polled before business queues,
//   * probe tokens are dispatched by namespace, including on the quiesce and
//     timeout-scan completion paths,
//   * business completions feed the probe health state machine,
//   * a JFC poll failure disables the probe paths on that context,
// and the transport assembly (provider snapshot, scheduler start/join, probe
// parameters). Everything runs against the fake URMA provider, so no hardware
// is required.

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment_manager.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/topology_attrs.h"
#include "tent/transport/ub/ub_transport.h"
#include "tent/transport/ub/workers.h"
#include "ub_fake_topology.h"
#include "ub_fake_urma.h"
#include "ub_probe_test_support.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace fake;
namespace pt = probe_test;

class StaticSegmentRegistry final : public SegmentRegistry {
   public:
    explicit StaticSegmentRegistry(SegmentDescRef desc)
        : desc_(std::move(desc)) {}

    Status getSegmentDesc(SegmentDescRef& desc,
                          const std::string& segment_name) override {
        if (!desc_ || segment_name != desc_->name) {
            return Status::InvalidEntry("unknown fake remote segment");
        }
        desc = desc_;
        return Status::OK();
    }
    Status putSegmentDesc(SegmentDescRef&) override { return Status::OK(); }
    Status deleteSegmentDesc(const std::string&) override {
        return Status::OK();
    }

   private:
    SegmentDescRef desc_;
};

// Remote metadata shapes used by the EID-scope selection tests.
enum class RemoteScopeScenario {
    kSingle,         // one remote device, same scope as the local one
    kSamePlusCross,  // remote devices 0 (same) and 1 (different scope)
    kAllCross,       // both remote devices are in a different scope
};

// Second local/remote device EID used by the scope tests. Its reachability
// prefix (first seven groups) differs from fakeDevice()'s "0001:...".
constexpr const char* kOtherScopeEid =
    "0002:0000:0000:0000:0000:0000:0000:4500";

// Owns one fake URMA data path plus a ProbeLinkManager wired into UbWorkers.
struct ProbeWorkersEnv {
    std::shared_ptr<FakeUrmaAdapter> adapter =
        std::make_shared<FakeUrmaAdapter>(fakeDevice());
    std::shared_ptr<UbContext> context;
    std::shared_ptr<Topology> topology = fakeTopology();
    std::unique_ptr<UbBufferManager> buffers;
    std::unique_ptr<SegmentManager> manager;
    std::unique_ptr<EndpointStore> endpoints;
    RailMonitor rails;
    QuotaManager quota;
    std::array<char, 64> source{};
    std::array<char, 64> target{};
    BufferDesc source_desc;
    BufferDesc target_desc;
    SegmentID remote_id{0};
    std::shared_ptr<FakeJfc> probe_jfc = std::make_shared<FakeJfc>();
    std::shared_ptr<FakeJetty> probe_jetty =
        std::make_shared<FakeJetty>(301, probe_jfc);
    std::unique_ptr<ProbeLinkManager> plm;
    std::unique_ptr<UbWorkers> workers;
    PathVersion version;

    ~ProbeWorkersEnv() { shutdown(); }

    bool initialize(
        bool dedicated_probe_jfc = true,
        RemoteScopeScenario scope_scenario = RemoteScopeScenario::kSingle) {
        for (size_t i = 0; i < source.size(); ++i) {
            source[i] = static_cast<char>(i + 1);
        }
        if (!adapter->initialize().ok()) return false;
        context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
        if (!context->initialize(1, JfcOptions{}).ok()) return false;
        std::vector<UbContextPtr> contexts{context};
        buffers = std::make_unique<UbBufferManager>(adapter, contexts);

        source_desc.addr = reinterpret_cast<uint64_t>(source.data());
        source_desc.length = source.size();
        source_desc.location = kWildcardLocation;
        target_desc.addr = reinterpret_cast<uint64_t>(target.data());
        target_desc.length = target.size();
        target_desc.location = kWildcardLocation;
        MemoryOptions options;
        options.perm = kGlobalReadWrite;
        if (!buffers->addBuffer(source_desc, options).ok()) return false;
        // addBuffer() fills transport_attrs, so the same descriptor can be
        // advertised by the fake remote segment below.
        if (!buffers->addBuffer(target_desc, options).ok()) return false;

        auto remote = std::make_shared<SegmentDesc>();
        remote->name = "remote";
        remote->rpc_server_addr = "remote";
        remote->machine_id = "fake-machine";
        remote->type = SegmentType::Memory;
        MemorySegmentDesc remote_memory;
        remote_memory.topology = *topology;
        if (scope_scenario != RemoteScopeScenario::kSingle) {
            // Advertise a second remote UB device with a different EID scope,
            // and rewrite the target's UB metadata so the peer publishes one
            // segment per remote device.
            Topology::NicEntry nic1{.name = "ub:fake1:eid0",
                                    .pci_bus_id = "0000:00:01.0",
                                    .type = Topology::NIC_UB,
                                    .numa_node = 0};
            remote_memory.topology.nic_list_.push_back(std::move(nic1));
            remote_memory.topology.mem_list_[0].device_list[0].push_back(1);

            UbBufferMetadata metadata;
            metadata.generation = 1;
            metadata.base = target_desc.addr;
            metadata.length = target_desc.length;
            metadata.location = kWildcardLocation;
            metadata.permission = kGlobalReadWrite;
            const std::string scope_eid =
                scope_scenario == RemoteScopeScenario::kAllCross
                    ? std::string(kOtherScopeEid)
                    : fakeDevice().eid;
            metadata.segments.push_back(UbBufferSegmentMetadata{
                0, "fake0", scope_eid, 0,
                makeDescriptor(target_desc.addr, target_desc.length)});
            metadata.segments.push_back(UbBufferSegmentMetadata{
                1, "fake1", kOtherScopeEid, 0,
                makeDescriptor(target_desc.addr, target_desc.length)});
            if (!encodeBufferMetadata(
                     metadata, target_desc.transport_attrs[TransportType::UB])
                     .ok()) {
                return false;
            }
        }
        remote_memory.buffers = {target_desc};
        remote->detail = std::move(remote_memory);

        manager = std::make_unique<SegmentManager>(
            std::make_unique<StaticSegmentRegistry>(remote));
        if (!manager
                 ->updateLocal([&](SegmentDesc& segment) {
                     segment.name = "local";
                     segment.type = SegmentType::Memory;
                     segment.detail = MemorySegmentDesc{};
                     auto& memory = std::get<MemorySegmentDesc>(segment.detail);
                     memory.topology = *topology;
                     memory.buffers = {source_desc, target_desc};
                     return Status::OK();
                 })
                 .ok()) {
            return false;
        }
        // Self transfers target the local segment (id 0) by default; the
        // scope scenarios need the advertised remote segment, so use a
        // non-local id for them.
        if (scope_scenario != RemoteScopeScenario::kSingle) remote_id = 11;
        if (!manager->openRemote(remote_id, "remote").ok()) return false;
        endpoints = std::make_unique<EndpointStore>(adapter, 16, 1);

        ProbePortSupply port;
        port.local_nic = 0;
        port.context = context->handle();
        port.probe_jetty = probe_jetty;
        port.jetty_generation = 1;
        port.probe_jfc = dedicated_probe_jfc ? probe_jfc : nullptr;
        port.source_segment =
            std::make_shared<FakeLocalSegment>(0x900000, 4096);
        port.source_address = port.source_segment->address();
        port.source_length = 1;
        port.source_generation = 1;
        port.control_credit = 1;

        ProbeEdgeSupply edge;
        version = PathVersion{ProbePathKey{0, remote_id, 0}, 1, 1};
        edge.version = version;
        edge.peer = ProbePeerKey{remote_id, "peer@ub:fake0:eid0"};
        edge.area_segment = std::make_shared<FakeRemoteSegment>(
            0xA00000, 4096, makeDescriptor(0xA00000, 4096));
        edge.area_address = 0xA00000;
        edge.area_length = 1;
        edge.area_epoch = 1;
        edge.remote_jetty = RemoteJettyInfo{"eid", 401, 501, 0xACFE};
        edge.node_gate_ok = true;
        edge.port_gate_ok = true;
        edge.profile_gate_ok = true;

        auto probe_params = pt::defaultParams();
        if (!dedicated_probe_jfc) {
            probe_params.jfc_mode = ProbeJfcMode::kShared;
        }
        plm = std::make_unique<ProbeLinkManager>(probe_params);
        plm->setReportSink(&reports);
        if (!plm->initialize(ProbeGraph{1, {port}, {edge}}).ok()) return false;

        UbParams params;
        params.worker_count = 1;
        params.poller_count = 1;
        params.slice_size = 64;
        params.max_retries = 0;
        params.slice_timeout_ms = 500;
        workers = std::make_unique<UbWorkers>(
            adapter, contexts, topology, manager.get(), buffers.get(), &rails,
            &quota, params, makeResolver(),
            [this](const std::shared_ptr<UbEndpoint>& endpoint) {
                if (endpoints) (void)endpoints->retire(endpoint);
            },
            LocalDeviceFailureHandler{}, plm.get(),
            dedicated_probe_jfc
                ? std::vector<ProbeJfcSlot>{ProbeJfcSlot{0, probe_jfc}}
                : std::vector<ProbeJfcSlot>{});
        return true;
    }

    EndpointResolver makeResolver() {
        return [this](const EndpointResolveRequest& request,
                      std::shared_ptr<UbEndpoint>& endpoint) {
            UbEndpointKey key{request.local_context->topologyId(),
                              request.remote_segment_id,
                              request.remote_topology_id, "peer@ub:fake0:eid0"};
            auto status =
                endpoints->getOrCreate(key, request.local_context, endpoint);
            if (!status.ok() || endpoint->ready()) return status;
            UbBootstrapDesc peer;
            peer.local_eid = fakeDevice().eid;
            peer.endpoint_generation = 100;
            peer.jetty_ids = {777};
            return endpoint->bind(peer);
        };
    }

    std::shared_ptr<UbTask> submitWrite() {
        Request request{};
        request.opcode = Request::WRITE;
        request.source = source.data();
        request.target_id = remote_id;
        request.target_offset = reinterpret_cast<uint64_t>(target.data());
        request.length = source.size();
        auto task = UbTask::create(request);
        if (task->addSlice(UbSliceSpec{
                source.data(), reinterpret_cast<uint64_t>(target.data()),
                source.size(), 0, 0}) == nullptr) {
            return nullptr;
        }
        if (!task->seal()) return nullptr;
        if (!workers->submit(task).ok()) return nullptr;
        return task;
    }

    void shutdown() {
        if (workers) {
            EXPECT_TRUE(workers->stop().ok());
            workers.reset();
        }
        plm.reset();
        if (endpoints) {
            EXPECT_TRUE(endpoints->clear().ok());
            endpoints.reset();
        }
        if (buffers) {
            EXPECT_TRUE(buffers->clear().ok());
            buffers.reset();
        }
        manager.reset();
        if (context) {
            EXPECT_TRUE(context->shutdown().ok());
            context.reset();
        }
        EXPECT_TRUE(adapter->shutdown().ok());
    }

    pt::RecordingReportSink reports;
};

TEST(UbProbeIntegrationTest,
     BusinessFailureCreatesCampaignAndProbeJfcRecovers) {
    ProbeWorkersEnv env;
    ASSERT_TRUE(env.initialize());

    // Observe the polling order: probe slots must be polled before business
    // slots in every round (design §11.1).
    std::mutex order_mutex;
    std::vector<std::string> order;
    auto record_order = [&](const char* role) {
        std::lock_guard<std::mutex> lock(order_mutex);
        if (order.size() < 4) order.emplace_back(role);
    };
    env.probe_jfc->setPollObserver([&] { record_order("probe"); });
    auto business_jfc =
        std::dynamic_pointer_cast<FakeJfc>(env.context->jfc(0)->handle());
    ASSERT_NE(business_jfc, nullptr);
    business_jfc->setPollObserver([&] { record_order("business"); });

    ASSERT_TRUE(env.workers->start().ok());
    ASSERT_TRUE(pt::waitUntil([&] {
        std::lock_guard<std::mutex> lock(order_mutex);
        return !order.empty();
    }));
    {
        std::lock_guard<std::mutex> lock(order_mutex);
        EXPECT_EQ(order.front(), "probe");
    }

    // A real business completion failure must feed the health state machine
    // and start a campaign for that peer.
    env.adapter->failNextCompletion(CompletionCategory::REMOTE_PATH_ERROR);
    auto task = env.submitWrite();
    ASSERT_NE(task, nullptr);
    ASSERT_TRUE(
        pt::waitUntil([&] { return task->transferStatus().s != PENDING; }));
    EXPECT_EQ(task->transferStatus().s, FAILED);

    ASSERT_TRUE(
        pt::waitUntil([&] { return !env.plm->campaignView().empty(); }));
    EXPECT_EQ(pt::healthOf(*env.plm, env.version).health,
              LinkHealth::kIsolated);
    EXPECT_EQ(env.plm->stats().probe_evidence_unmapped, 0U);
    EXPECT_EQ(env.plm->stats().campaign_created, 1U);

    // A probe completion arriving on the dedicated probe JFC is routed by
    // token namespace and recovers the path.
    const uint64_t now = steadyNowNs();
    auto actions = env.plm->pendingActions(now);
    ASSERT_FALSE(actions.empty());
    auto record = env.plm->beginPost(actions.front(), makeProbeToken(1), now);
    ASSERT_NE(record, nullptr);
    // The poster's synchronous return, then the CQE (F11: both are needed).
    env.plm->onPostResult(record, ProbePostOutcome::kAccepted, now);
    env.probe_jfc->push(Completion{CompletionCategory::SUCCESS, 0,
                                   record->core.token, 1,
                                   env.probe_jetty->id()});

    ASSERT_TRUE(pt::waitUntil([&] {
        return pt::healthOf(*env.plm, env.version).health ==
               LinkHealth::kHealthy;
    }));
    EXPECT_EQ(env.plm->stats().probe_success, 1U);
    env.shutdown();
}

TEST(UbProbeIntegrationTest, SharedModeRoutesProbeCompletionOnBusinessJfc) {
    ProbeWorkersEnv env;
    // Shared mode: no dedicated probe JFC; the probe completion lands on the
    // business queue and must still be routed by token namespace first.
    ASSERT_TRUE(env.initialize(/*dedicated_probe_jfc=*/false));
    ASSERT_TRUE(env.workers->start().ok());

    env.adapter->failNextCompletion(CompletionCategory::REMOTE_PATH_ERROR);
    auto task = env.submitWrite();
    ASSERT_NE(task, nullptr);
    ASSERT_TRUE(
        pt::waitUntil([&] { return task->transferStatus().s != PENDING; }));
    ASSERT_TRUE(
        pt::waitUntil([&] { return !env.plm->campaignView().empty(); }));

    const uint64_t now = steadyNowNs();
    auto actions = env.plm->pendingActions(now);
    ASSERT_FALSE(actions.empty());
    auto record = env.plm->beginPost(actions.front(), makeProbeToken(1), now);
    ASSERT_NE(record, nullptr);
    env.plm->onPostResult(record, ProbePostOutcome::kAccepted, now);
    auto business_jfc =
        std::dynamic_pointer_cast<FakeJfc>(env.context->jfc(0)->handle());
    ASSERT_NE(business_jfc, nullptr);
    const uint32_t business_jfc_id =
        static_cast<uint32_t>(business_jfc->pollCount());
    business_jfc->push(Completion{CompletionCategory::SUCCESS, 0,
                                  record->core.token, 1, business_jfc_id});

    ASSERT_TRUE(pt::waitUntil([&] {
        return pt::healthOf(*env.plm, env.version).health ==
               LinkHealth::kHealthy;
    }));
    env.shutdown();
}

TEST(UbProbeIntegrationTest, ProbeJfcPollFailureDisablesProbePaths) {
    ProbeWorkersEnv env;
    ASSERT_TRUE(env.initialize());
    ASSERT_TRUE(env.workers->start().ok());

    env.adapter->failNextPoll();
    ASSERT_TRUE(pt::waitUntil([&] {
        return pt::healthOf(*env.plm, env.version).health ==
               LinkHealth::kDisabled;
    }));
    EXPECT_TRUE(pt::healthOf(*env.plm, env.version).pending);
    EXPECT_EQ(env.plm->stats().path_state_disabled, 1U);
    env.shutdown();
}

// EID scope (stress_cluster_bench regression): legacy selection must never
// pair a local device with a remote device in a different reachability scope.
// Remote devices 0 (same scope) and 1 (different scope) yield exactly one
// legal pair; the transfer still completes on that rail.
TEST(UbProbeIntegrationTest, LegacySelectionPairsOnlySameEidScope) {
    ProbeWorkersEnv env;
    ASSERT_TRUE(env.initialize(/*dedicated_probe_jfc=*/true,
                               RemoteScopeScenario::kSamePlusCross));
    ASSERT_TRUE(env.workers->start().ok());

    auto task = env.submitWrite();
    ASSERT_TRUE(task != nullptr);
    ASSERT_TRUE(
        pt::waitUntil([&] { return task->transferStatus().s != PENDING; }));
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    // 2 local x remote combinations, exactly one crosses scopes.
    EXPECT_EQ(env.workers->businessStats().eid_scope_denied, 1U);
    env.shutdown();
}

// EID scope: when every pair crosses scopes the selection fails closed with a
// configuration error instead of retrying into LOCAL_ACCESS_ERROR and the
// endpoint-generation churn it causes.
TEST(UbProbeIntegrationTest, LegacySelectionAllCrossScopeFailsClosed) {
    ProbeWorkersEnv env;
    ASSERT_TRUE(env.initialize(/*dedicated_probe_jfc=*/true,
                               RemoteScopeScenario::kAllCross));
    ASSERT_TRUE(env.workers->start().ok());

    auto task = env.submitWrite();
    ASSERT_TRUE(task != nullptr);
    ASSERT_TRUE(
        pt::waitUntil([&] { return task->transferStatus().s != PENDING; }));
    EXPECT_EQ(task->transferStatus().s, FAILED);
    EXPECT_EQ(env.workers->businessStats().eid_scope_denied, 2U);
    env.shutdown();
}

// F6b: a deadline timeout is the strongest path-fault evidence (nothing
// completed, no native error code). It must feed the same PLM isolation and
// probe-campaign chain as an adapter-level TIMEOUT, otherwise a suspended path
// with no CQE would never start recovery.
TEST(UbProbeIntegrationTest, DeadlineTimeoutIsolatesThePathAndStartsCampaign) {
    ProbeWorkersEnv env;
    ASSERT_TRUE(env.initialize());
    ASSERT_TRUE(env.workers->start().ok());

    const ProbePathKey key = env.version.path;
    const uint64_t epoch_before = env.plm->businessFaultEpoch(key);
    // The provider never completes: only the deadline fence can settle it.
    env.adapter->holdNextCompletion();
    auto task = env.submitWrite();
    ASSERT_TRUE(task != nullptr);

    ASSERT_TRUE(
        pt::waitUntil([&] { return task->transferStatus().s != PENDING; }));
    EXPECT_EQ(task->transferStatus().s, TIMEOUT);
    EXPECT_EQ(env.workers->businessStats().business_deadline_timeouts, 1U);
    EXPECT_GT(env.plm->businessFaultEpoch(key), epoch_before);
    EXPECT_EQ(env.plm->stats().path_state_isolated, 1U);
    EXPECT_EQ(env.plm->campaignView().size(), 1U);
    env.shutdown();
}

std::shared_ptr<ControlService> startFakeControl(
    const std::shared_ptr<Topology>& topology, std::string& segment_name) {
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t rpc_port = 0;
    if (!control->start(rpc_port).ok()) return nullptr;
    segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(rpc_port));
    if (!control->segmentManager()
             .updateLocal([&](SegmentDesc& segment) {
                 segment.name = segment_name;
                 segment.rpc_server_addr = segment_name;
                 segment.machine_id = "fake-machine";
                 segment.type = SegmentType::Memory;
                 segment.detail = MemorySegmentDesc{};
                 std::get<MemorySegmentDesc>(segment.detail).topology =
                     *topology;
                 return Status::OK();
             })
             .ok()) {
        return nullptr;
    }
    return control;
}

std::shared_ptr<Config> makeTransportConfig(bool probe_enabled) {
    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/probe/enabled", probe_enabled);
    return config;
}

// The transport assembly needs a complete fake supply; it never posts because
// no campaign exists.
class FakeProbeProvider final : public ProbeResourceProvider {
   public:
    explicit FakeProbeProvider(ProbeGraph graph) : graph_(std::move(graph)) {}

    Status snapshot(ProbeGraph& graph) override {
        ++snapshots;
        graph = graph_;
        return Status::OK();
    }
    void retireProbeLane(const LaneKey&, uint64_t, uint64_t, const PathVersion&,
                         const Status&) override {
        ++retires;
    }
    void handoffUnproven(const ProbeHandoff&) override { ++handoffs; }

    int snapshots{0};
    int retires{0};
    int handoffs{0};

   private:
    ProbeGraph graph_;
};

TEST(UbProbeIntegrationTest, TransportAssemblesAndTearsDownProbeScheduler) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    auto probe_jfc = std::make_shared<FakeJfc>();
    auto probe_jetty = std::make_shared<FakeJetty>(301, probe_jfc);

    ProbePortSupply port;
    port.local_nic = 0;
    port.probe_jetty = probe_jetty;
    port.jetty_generation = 1;
    port.probe_jfc = probe_jfc;
    port.source_segment = std::make_shared<FakeLocalSegment>(0x900000, 4096);
    port.source_address = port.source_segment->address();
    port.source_length = 1;
    port.source_generation = 1;
    port.control_credit = 1;
    ProbeEdgeSupply edge;
    edge.version = PathVersion{ProbePathKey{0, 42, 0}, 1, 1};
    edge.peer = ProbePeerKey{42, "peer@ub:fake0:eid0"};
    edge.area_segment = std::make_shared<FakeRemoteSegment>(
        0xA00000, 4096, makeDescriptor(0xA00000, 4096));
    edge.area_address = 0xA00000;
    edge.area_length = 1;
    edge.area_epoch = 1;
    edge.remote_jetty = RemoteJettyInfo{"eid", 401, 501, 0xACFE};
    edge.node_gate_ok = true;
    edge.port_gate_ok = true;
    edge.profile_gate_ok = true;

    auto provider =
        std::make_shared<FakeProbeProvider>(ProbeGraph{1, {port}, {edge}});
    auto poster = std::make_shared<pt::FakeProbePoster>();

    auto topology = fakeTopology();
    std::string segment_name;
    auto control = startFakeControl(topology, segment_name);
    ASSERT_NE(control, nullptr);
    auto config = makeTransportConfig(/*probe_enabled=*/true);

    UbTransport transport(adapter, provider, poster);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    EXPECT_EQ(provider->snapshots, 1);
    ASSERT_TRUE(pt::waitUntil([&] { return probe_jfc->pollCount() > 0; }));
    EXPECT_EQ(poster->callCount(), 0U);

    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(provider->handoffs, 0);
}

class StaticPeerDescriptorSource final : public ProbePeerDescriptorSource {
   public:
    Status snapshot(std::vector<ProbePeerDescriptor>& descriptors) override {
        ProbePeerDescriptor descriptor;
        descriptor.local_nic = 0;
        descriptor.peer_segment_id = 42;
        descriptor.remote_nic = 0;
        descriptor.peer_nic_path = "peer@ub:fake0:eid0";
        descriptor.area_descriptor = makeDescriptor(0xA00000, 4096);
        descriptor.area_length = 1;
        descriptor.area_epoch = 1;
        descriptor.area_token = 0xACFE;
        descriptor.remote_jetty = RemoteJettyInfo{"eid", 401, 501, 0xACFE};
        descriptor.node_gate_ok = true;
        descriptor.port_gate_ok = true;
        descriptor.profile_gate_ok = true;
        descriptors = {descriptor};
        return Status::OK();
    }
};

TEST(UbProbeIntegrationTest, TransportBuildsStandaloneProbeProvider) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    auto topology = fakeTopology();
    std::string segment_name;
    auto control = startFakeControl(topology, segment_name);
    ASSERT_NE(control, nullptr);
    auto config = makeTransportConfig(/*probe_enabled=*/true);
    config->set("transports/ub/probe/provider", "standalone");

    StaticPeerDescriptorSource source;
    UbTransport transport(adapter, nullptr, nullptr, &source);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    // The peer probe area was imported by the standalone directory.
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 1U);
    EXPECT_GE(adapter->liveJfcCount(), 2U);  // business JFC + probe JFC

    ASSERT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 0U);
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeIntegrationTest, DeclaredProbeTopologyInstallsWithAnEmptyGraph) {
    auto device = fakeDevice();
    device.kind = DeviceKind::PHYSICAL;
    device.capabilities.transport_modes = 3;  // RC | RM
    device.capabilities.max_jfc = 4;
    device.capabilities.max_jetty = 64;
    device.capabilities.rm_tp_cap = kUrmaTpTypeCtpMask;
    device.capabilities.priority_tp_type_masks[kUrmaMaxPriority] =
        kUrmaTpTypeCtpMask;
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = std::make_shared<Topology>();
    Topology::NicEntry nic;
    nic.name = device.topology_name;
    nic.type = Topology::NIC_UB;
    nic.numa_node = 0;
    encodeTopologyDeviceAttributes(device, 0, nic.device_attrs);
    topology->nic_list_.push_back(std::move(nic));
    Topology::MemEntry memory;
    memory.name = kWildcardLocation;
    memory.type = Topology::MEM_HOST;
    memory.numa_node = -1;
    memory.device_list[0].push_back(0);
    topology->mem_list_.push_back(std::move(memory));

    std::string segment_name;
    auto control = startFakeControl(topology, segment_name);
    ASSERT_NE(control, nullptr);

    auto config = makeTransportConfig(/*probe_enabled=*/true);
    config->set("transports/ub/runtime_profile", "rm_legacy");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/physical_only", true);
    config->set("transports/ub/probe/provider", "standalone");
    config->set("transports/ub/probe/local_ports",
                nlohmann::json::array({nlohmann::json{
                    {"id", "P0"}, {"bind", device.topology_name}}}));
    config->set(
        "transports/ub/probe/peers",
        nlohmann::json{
            {"node-b",
             nlohmann::json{
                 {"ports",
                  nlohmann::json::array({nlohmann::json{
                      {"id", "R0"},
                      {"eid",
                       "0001:0000:0000:0000:0000:0000:0000:0020"}}})}}}});
    config->set(
        "transports/ub/probe/adjacency",
        nlohmann::json{
            {"node-b", nlohmann::json{{"P0", nlohmann::json::array({"R0"})}}}});

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(status.ok()) << status.ToString();
    // The peer publication has not arrived: the graph starts with its declared
    // port and zero edges, and the coordinator retries in the background.
    EXPECT_EQ(transport.probeLinkCount(), 0U);
    EXPECT_GE(transport.probeGeneration(), 1U);
    const auto topology_stats = transport.probeTopologyStats();
    EXPECT_EQ(topology_stats.ports, 1U);
    EXPECT_EQ(topology_stats.edges, 1U);

    // G-1: the published local segment must carry the trusted kind, not the
    // raw discovery "unknown".
    const auto local_segment = control->segmentManager().getLocal();
    ASSERT_NE(local_segment, nullptr);
    bool found_kind = false;
    for (const auto& nic : local_segment->getMemory().topology.nic_list_) {
        if (nic.type != Topology::NIC_UB || nic.name != device.topology_name) {
            continue;
        }
        auto kind = nic.device_attrs.find(std::string(kTopologyDeviceKindAttr));
        ASSERT_NE(kind, nic.device_attrs.end());
        EXPECT_EQ(kind->second, "physical");
        found_kind = true;
    }
    EXPECT_TRUE(found_kind);

    ASSERT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(transport.probeTopologyStats().ports, 0U);
}

TEST(UbProbeIntegrationTest, DeclaredModeOnlyOpensDeclaredBusinessContexts) {
    auto declared = fakeDevice();
    declared.kind = DeviceKind::PHYSICAL;
    declared.capabilities.transport_modes = 3;
    declared.capabilities.max_jfc = 4;
    declared.capabilities.max_jetty = 64;
    declared.capabilities.rm_tp_cap = kUrmaTpTypeCtpMask;
    declared.capabilities.priority_tp_type_masks[kUrmaMaxPriority] =
        kUrmaTpTypeCtpMask;
    auto extra = fakeDevice();
    extra.topology_name = "ub:fake1:eid0";
    extra.native_device_name = "fake1";
    extra.native_device_path = "/fake/fake1";
    extra.eid = "0002:0000:0000:0000:0000:0000:0000:0000";
    extra.kind = DeviceKind::PHYSICAL;
    extra.capabilities.transport_modes = 3;
    extra.capabilities.max_jfc = 4;
    extra.capabilities.max_jetty = 64;
    extra.capabilities.rm_tp_cap = kUrmaTpTypeCtpMask;
    extra.capabilities.priority_tp_type_masks[kUrmaMaxPriority] =
        kUrmaTpTypeCtpMask;

    auto adapter = std::make_shared<FakeUrmaAdapter>(declared);
    adapter->setDiscoveredDevices({declared, extra});
    auto topology = std::make_shared<Topology>();
    for (const auto& device : std::vector<DeviceInfo>{declared, extra}) {
        Topology::NicEntry nic;
        nic.name = device.topology_name;
        nic.type = Topology::NIC_UB;
        nic.numa_node = 0;
        encodeTopologyDeviceAttributes(device, 0, nic.device_attrs);
        topology->nic_list_.push_back(std::move(nic));
    }
    Topology::MemEntry memory;
    memory.name = kWildcardLocation;
    memory.type = Topology::MEM_HOST;
    memory.numa_node = -1;
    memory.device_list[0].push_back(0);
    memory.device_list[0].push_back(1);
    topology->mem_list_.push_back(std::move(memory));

    std::string segment_name;
    auto control = startFakeControl(topology, segment_name);
    ASSERT_NE(control, nullptr);
    auto config = makeTransportConfig(/*probe_enabled=*/true);
    config->set("transports/ub/runtime_profile", "rm_legacy");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/physical_only", true);
    config->set("transports/ub/probe/provider", "standalone");
    config->set("transports/ub/probe/local_ports",
                nlohmann::json::array({nlohmann::json{
                    {"id", "P0"}, {"bind", declared.topology_name}}}));
    config->set(
        "transports/ub/probe/peers",
        nlohmann::json{
            {"node-b",
             nlohmann::json{
                 {"ports",
                  nlohmann::json::array({nlohmann::json{
                      {"id", "R0"},
                      {"eid",
                       "0001:0000:0000:0000:0000:0000:0000:0020"}}})}}}});
    config->set(
        "transports/ub/probe/adjacency",
        nlohmann::json{
            {"node-b", nlohmann::json{{"P0", nlohmann::json::array({"R0"})}}}});

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    EXPECT_EQ(transport.probeTopologyStats().ports, 1U);
    // One business JFC for P0 + one probe JFC; the undeclared device never
    // gets a context or JFC.
    EXPECT_EQ(adapter->liveJfcCount(), 2U);
    EXPECT_EQ(adapter->liveJettyCount(), 1U);
    ASSERT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);
}

TEST(UbProbeIntegrationTest, StandaloneProbeWithoutPeerSourceFailsInstall) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    auto topology = fakeTopology();
    std::string segment_name;
    auto control = startFakeControl(topology, segment_name);
    ASSERT_NE(control, nullptr);
    auto config = makeTransportConfig(/*probe_enabled=*/true);
    config->set("transports/ub/probe/provider", "standalone");

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto status =
        transport.install(mutable_segment_name, control, topology, config);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeIntegrationTest, EnabledProbeWithoutSupplyFailsInstall) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    auto topology = fakeTopology();
    std::string segment_name;
    auto control = startFakeControl(topology, segment_name);
    ASSERT_NE(control, nullptr);
    auto config = makeTransportConfig(/*probe_enabled=*/true);

    // No provider/poster: enabling probing must fail fast instead of silently
    // degrading to "no probing" (design §12.3).
    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto status =
        transport.install(mutable_segment_name, control, topology, config);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
