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
// P1 tests for the standalone probe resource provider: local resource
// creation/release, dedicated vs shared JFC ownership, rollback on invalid
// peer edges, and the P1 handoff/retire recording behavior.

#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <vector>

#include "tent/transport/ub/probe_provider_standalone.h"
#include "ub_fake_urma.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace fake;

ProbeParams makeProbeParams() {
    ProbeParams params;
    params.enabled = true;
    params.jfc_depth = 64;
    return params;
}

class StubPeerDirectory final : public ProbePeerDirectory {
   public:
    explicit StubPeerDirectory(std::vector<ProbeEdgeSupply> edges)
        : edges_(std::move(edges)) {}

    Status snapshotEdges(std::vector<ProbeEdgeSupply>& edges) override {
        edges = edges_;
        return Status::OK();
    }

    void set(std::vector<ProbeEdgeSupply> edges) { edges_ = std::move(edges); }

   private:
    std::vector<ProbeEdgeSupply> edges_;
};

class RecordingAvailabilitySink final : public ProbeAvailabilitySink {
   public:
    Status onPoolAvailabilityChange(const ProbeEvent& event) override {
        events.push_back(event);
        return Status::OK();
    }

    std::vector<ProbeEvent> events;
};

ProbeEdgeSupply makeEdge(Topology::NicID local_nic, SegmentID peer_id,
                         Topology::NicID remote_nic) {
    ProbeEdgeSupply edge;
    edge.version =
        PathVersion{ProbePathKey{local_nic, peer_id, remote_nic}, 1, 1};
    edge.peer = ProbePeerKey{peer_id, "peer"};
    edge.area_segment = std::make_shared<FakeRemoteSegment>(
        0xA00000, 4096, makeDescriptor(0xA00000, 4096));
    edge.area_address = 0xA00000;
    edge.area_length = 1;
    edge.area_epoch = 1;
    edge.remote_jetty = RemoteJettyInfo{"eid", 401, 501, 0xACFE};
    edge.node_gate_ok = true;
    edge.port_gate_ok = true;
    edge.profile_gate_ok = true;
    return edge;
}

TEST(UbProbeStandaloneProviderTest, DedicatedSnapshotCreatesAndReleases) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    auto params = makeProbeParams();
    StandaloneProbeResourceProvider provider(
        adapter, params, {ProbeLocalPortConfig{0, context, nullptr, 1}},
        &directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    EXPECT_TRUE(provider.initialized());
    EXPECT_EQ(graph.graph_epoch, 1U);
    ASSERT_EQ(graph.ports.size(), 1U);
    ASSERT_EQ(graph.edges.size(), 1U);

    const auto& port = graph.ports[0];
    EXPECT_EQ(port.local_nic, 0);
    EXPECT_EQ(port.jetty_generation, 1U);
    EXPECT_NE(port.probe_jetty, nullptr);
    EXPECT_NE(port.probe_jfc, nullptr);
    EXPECT_NE(port.source_segment, nullptr);
    EXPECT_EQ(port.source_length, 1U);
    EXPECT_EQ(port.source_generation, 1U);
    EXPECT_EQ(port.control_credit, 1U);
    EXPECT_EQ(port.peer_budget, params.peer_budget);
    EXPECT_EQ(adapter->liveJfcCount(), 1U);
    EXPECT_EQ(adapter->liveJettyCount(), 1U);
    EXPECT_EQ(adapter->lastJettyTransportMode(), JettyTransportMode::kRm);
    // Source page plus the local probe area page peers write into.
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 2U);

    const auto areas = provider.localAreaViews();
    ASSERT_EQ(areas.size(), 1U);
    EXPECT_EQ(areas[0].local_nic, 0);
    EXPECT_EQ(areas[0].generation, 1U);
    EXPECT_EQ(areas[0].length % static_cast<uint64_t>(::getpagesize()), 0U);
    EXPECT_EQ(areas[0].address % static_cast<uint64_t>(::getpagesize()), 0U);
    EXPECT_EQ(areas[0].token, 0xACFEU);
    EXPECT_EQ(areas[0].epoch, 1U);
    EXPECT_FALSE(areas[0].descriptor.hex.empty());

    const auto views = provider.portViews();
    ASSERT_EQ(views.size(), 1U);
    EXPECT_EQ(views[0].local_nic, 0);
    EXPECT_EQ(views[0].generation, 1U);
    EXPECT_EQ(views[0].native_jetty_id, port.probe_jetty->id());
    EXPECT_EQ(views[0].source_address, port.source_address);
    EXPECT_EQ(views[0].source_generation, 1U);
    EXPECT_TRUE(views[0].owns_jfc);

    ProbeGraph second;
    EXPECT_TRUE(provider.snapshot(second).IsInvalidArgument());

    ASSERT_TRUE(provider.shutdown().ok());
    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(provider.portViews().empty());
    EXPECT_FALSE(provider.initialized());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);

    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, SharedModeBorrowsBusinessJfc) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    JfcPtr business_jfc;
    ASSERT_TRUE(adapter->createJfc(context, JfcOptions{}, business_jfc).ok());
    ASSERT_EQ(adapter->liveJfcCount(), 1U);

    auto params = makeProbeParams();
    params.jfc_mode = ProbeJfcMode::kShared;
    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    StandaloneProbeResourceProvider provider(
        adapter, params, {ProbeLocalPortConfig{0, context, business_jfc, 1}},
        &directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    EXPECT_EQ(graph.ports[0].probe_jfc, nullptr);
    EXPECT_EQ(adapter->liveJfcCount(), 1U);
    EXPECT_EQ(adapter->liveJettyCount(), 1U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 2U);

    ASSERT_TRUE(provider.shutdown().ok());
    // The business JFC is borrowed: the provider must not delete it.
    EXPECT_EQ(adapter->liveJfcCount(), 1U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);

    ASSERT_TRUE(adapter->deleteJfc(business_jfc).ok());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, InvalidEdgeRollsBackCreatedResources) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    // Edge references local port 99, which is not configured.
    StubPeerDirectory directory({makeEdge(99, 42, 0)});
    StandaloneProbeResourceProvider provider(
        adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, context, nullptr, 1}}, &directory);

    ProbeGraph graph;
    const auto status = provider.snapshot(graph);
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_FALSE(provider.initialized());
    EXPECT_TRUE(provider.portViews().empty());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, RejectsMissingOrDuplicateLocalPorts) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    {
        StandaloneProbeResourceProvider empty(adapter, makeProbeParams(), {},
                                              &directory);
        ProbeGraph graph;
        EXPECT_TRUE(empty.snapshot(graph).IsInvalidArgument());
    }
    {
        StandaloneProbeResourceProvider duplicate(
            adapter, makeProbeParams(),
            {ProbeLocalPortConfig{0, context, nullptr, 1},
             ProbeLocalPortConfig{0, context, nullptr, 1}},
            &directory);
        ProbeGraph graph;
        EXPECT_TRUE(duplicate.snapshot(graph).IsInvalidArgument());
    }
    {
        StandaloneProbeResourceProvider null_context(
            adapter, makeProbeParams(),
            {ProbeLocalPortConfig{0, nullptr, nullptr, 1}}, &directory);
        ProbeGraph graph;
        EXPECT_TRUE(null_context.snapshot(graph).IsInvalidArgument());
    }
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, RecordsRetireAndHandoffUntilShutdown) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    StandaloneProbeResourceProvider provider(
        adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, context, nullptr, 1}}, &directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());

    const PathVersion version{ProbePathKey{0, 42, 0}, 1, 1};
    provider.retireProbeLane(LaneKey{0, 1234, 1}, 1, 7, version,
                             Status::InternalError("injected retire"));
    EXPECT_EQ(provider.retireCalls(), 1U);
    // P1 does not rebuild the lane; the resources remain until shutdown.
    EXPECT_EQ(adapter->liveJettyCount(), 1U);

    ProbeHandoff handoff;
    handoff.token = makeProbeToken(1);
    handoff.lane = LaneKey{0, 1234, 1};
    handoff.key.version = version;
    handoff.orphan_reason = OrphanReason::kTimeout;
    handoff.source = std::make_shared<FakeLocalSegment>(0xB00000, 4096);
    handoff.area = std::make_shared<FakeRemoteSegment>(
        0xA00000, 4096, makeDescriptor(0xA00000, 4096));
    provider.handoffUnproven(handoff);
    EXPECT_EQ(provider.quarantineCount(), 1U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_EQ(provider.quarantineCount(), 0U);
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);

    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, RefreshEmitsIncrementalGraphEvents) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerDirectory directory({makeEdge(0, 42, 0), makeEdge(0, 42, 1)});
    auto params = makeProbeParams();
    params.dynamic_edges = true;
    StandaloneProbeResourceProvider provider(
        adapter, params, {ProbeLocalPortConfig{0, context, nullptr, 1}},
        &directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.edges.size(), 2U);

    RecordingAvailabilitySink sink;
    provider.setAvailabilitySink(&sink);

    // Add a new edge and remove both old ones. Additions are emitted before
    // removals so the graph never loses an unrelated edge first.
    directory.set({makeEdge(0, 43, 0)});
    ASSERT_TRUE(provider.refreshPeerEdges().ok());
    ASSERT_EQ(sink.events.size(), 3U);
    std::vector<ProbeEventKind> kinds;
    for (const auto& event : sink.events) kinds.push_back(event.kind);
    EXPECT_EQ(
        std::count(kinds.begin(), kinds.end(), ProbeEventKind::kEdgeAdded), 1);
    EXPECT_EQ(
        std::count(kinds.begin(), kinds.end(), ProbeEventKind::kEdgeRemoved),
        2);
    EXPECT_EQ(sink.events[0].event_seq, 1U);
    EXPECT_EQ(sink.events[1].event_seq, 2U);
    EXPECT_EQ(sink.events[2].event_seq, 3U);

    // Same PathVersion with a new area epoch: in-place replacement.
    sink.events.clear();
    auto refreshed = makeEdge(0, 43, 0);
    refreshed.area_epoch = 2;
    directory.set({refreshed});
    ASSERT_TRUE(provider.refreshPeerEdges().ok());
    ASSERT_EQ(sink.events.size(), 1U);
    EXPECT_EQ(sink.events[0].kind, ProbeEventKind::kAreaReplaced);
    EXPECT_EQ(sink.events[0].event_seq, 4U);

    // Same adjacency path with a new target epoch: one atomic replacement.
    sink.events.clear();
    auto changed = makeEdge(0, 43, 0);
    changed.version.target_epoch = 2;
    directory.set({changed});
    ASSERT_TRUE(provider.refreshPeerEdges().ok());
    ASSERT_EQ(sink.events.size(), 1U);
    EXPECT_EQ(sink.events[0].kind, ProbeEventKind::kTargetEpochChanged);
    ASSERT_TRUE(sink.events[0].edge.has_value());
    EXPECT_EQ(sink.events[0].edge->version.target_epoch, 2U);

    // No diff produces no event.
    sink.events.clear();
    ASSERT_TRUE(provider.refreshPeerEdges().ok());
    EXPECT_TRUE(sink.events.empty());

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, RebuildPushesTheNewPublicationEpoch) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    StandaloneProbeResourceProvider provider(
        adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, context, nullptr, 1}}, &directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    RecordingAvailabilitySink sink;
    provider.setAvailabilitySink(&sink);
    provider.setCompletionSink([](const Completion&) { return Status::OK(); });
    uint64_t pushed_epoch = 0;
    provider.setPublicationChangedCallback(
        [&](uint64_t epoch) { pushed_epoch = epoch; });

    const uint32_t jetty_id = graph.ports[0].probe_jetty->id();
    const PathVersion version{ProbePathKey{0, 42, 0}, 1, 1};
    provider.retireProbeLane(LaneKey{0, jetty_id, 1}, 1, 7, version,
                             Status::InternalError("injected retire"));
    ASSERT_TRUE(provider.waitForRebuilds(2000));
    EXPECT_GT(provider.publicationEpoch(), 1U);
    EXPECT_EQ(pushed_epoch, provider.publicationEpoch());
    // F12: the replacement event carries the drain receipt for generation 1.
    ASSERT_FALSE(sink.events.empty());
    const ProbeEvent& replaced = sink.events.back();
    ASSERT_EQ(replaced.kind, ProbeEventKind::kProbeHandleReplaced);
    ASSERT_TRUE(replaced.port.has_value());
    EXPECT_EQ(replaced.port->previous_generation, 1U);
    EXPECT_TRUE(replaced.port->previous_drained);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, DeclaredPortsPublishNodeIdentityAndArea) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());

    ProbeParams params = makeProbeParams();
    StandaloneProbeResourceProvider provider(adapter, params);
    provider.setNodeIdentity("node-a", 11, 22);

    auto device = fakeDevice();
    device.kind = DeviceKind::PHYSICAL;
    device.capabilities.transport_modes = 3;
    ProbePortSpec spec;
    spec.port_id = "P0";
    spec.local_nic = 0;
    spec.device = device;
    spec.topology_name = device.topology_name;
    spec.native_device_name = device.native_device_name;
    spec.eid = device.eid;
    spec.generation = 1;
    ASSERT_TRUE(provider.bindPorts({spec}).ok());
    // Ports may only be bound once, before resources exist.
    EXPECT_TRUE(provider.bindPorts({spec}).IsInvalidArgument());
    ASSERT_TRUE(provider.prepareLocal().ok());

    const auto publication = provider.publication();
    EXPECT_EQ(publication.segment, "node-a");
    EXPECT_EQ(publication.node_incarnation, 11U);
    EXPECT_EQ(publication.topology_epoch, 22U);
    EXPECT_EQ(publication.publication_epoch, 1U);
    ASSERT_EQ(publication.ports.size(), 1U);
    const auto& port = publication.ports[0];
    EXPECT_EQ(port.port_id, "P0");
    EXPECT_EQ(port.topology_name, device.topology_name);
    EXPECT_EQ(port.eid, device.eid);
    EXPECT_EQ(port.target_epoch, 1U);
    EXPECT_NE(port.jetty.id, 0U);
    EXPECT_EQ(port.jetty.eid, device.eid);
    EXPECT_EQ(port.jetty.token, 0xACFEU);
    EXPECT_EQ(port.area_length % static_cast<uint64_t>(::getpagesize()), 0U);
    EXPECT_EQ(port.area_epoch, 1U);
    EXPECT_EQ(port.area_token, 0xACFEU);
    EXPECT_FALSE(port.area_descriptor.hex.empty());
    EXPECT_TRUE(port.node_gate_ok);
    EXPECT_TRUE(port.port_gate_ok);
    EXPECT_TRUE(port.profile_gate_ok);
    EXPECT_EQ(provider.publicationEpoch(), 1U);

    EXPECT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, DeclaredModeRequiresBindPorts) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());

    ProbeParams params = makeProbeParams();
    params.local_ports.push_back(ProbeLocalPortDecl{"P0", "ub:fake0:eid0", ""});
    StandaloneProbeResourceProvider provider(adapter, params);
    EXPECT_TRUE(provider.discover().IsInvalidArgument());

    EXPECT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

// F1: close safety. A failed native release must keep the page and the
// handles, count the orphan and let a later shutdown() finish the job.
class ProbeCloseTest : public ::testing::Test {
   protected:
    void SetUp() override {
        adapter_ = std::make_shared<FakeUrmaAdapter>(fakeDevice());
        EXPECT_TRUE(adapter_->initialize().ok());
        EXPECT_TRUE(adapter_->openContext(fakeDevice(), context_).ok());
    }

    std::unique_ptr<StandaloneProbeResourceProvider> makeProvider(
        StubPeerDirectory* directory, uint64_t generation = 1) {
        return std::make_unique<StandaloneProbeResourceProvider>(
            adapter_, makeProbeParams(),
            std::vector<ProbeLocalPortConfig>{
                ProbeLocalPortConfig{0, context_, nullptr, generation}},
            directory);
    }

    std::shared_ptr<FakeUrmaAdapter> adapter_;
    ContextPtr context_;
};

TEST_F(ProbeCloseTest, ResetFailureKeepsPageAndRetries) {
    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    auto provider = makeProvider(&directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider->snapshot(graph).ok());

    adapter_->failResets();
    const Status first = provider->shutdown();
    EXPECT_FALSE(first.ok());

    const auto stats = provider->closeStats();
    EXPECT_TRUE(stats.shutdown_incomplete);
    EXPECT_EQ(stats.orphaned_ports, 1U);
    // Source page and local area page both stay mapped.
    EXPECT_EQ(stats.orphaned_pages, 2U);
    // INV-1: no delete and no free without the reset proof.
    EXPECT_EQ(adapter_->liveJettyCount(), 1U);
    EXPECT_EQ(adapter_->liveLocalSegmentCount(), 2U);
    EXPECT_EQ(adapter_->liveJfcCount(), 1U);
    EXPECT_EQ(provider->portViews().size(), 0U);

    // The retry must be able to finish from the retained state.
    adapter_->allowResets();
    ASSERT_TRUE(provider->shutdown().ok());
    const auto retried = provider->closeStats();
    EXPECT_FALSE(retried.shutdown_incomplete);
    EXPECT_EQ(retried.orphaned_ports, 0U);
    EXPECT_EQ(retried.orphaned_pages, 0U);
    EXPECT_EQ(adapter_->liveJettyCount(), 0U);
    EXPECT_EQ(adapter_->liveJfcCount(), 0U);
    EXPECT_EQ(adapter_->liveLocalSegmentCount(), 0U);
    EXPECT_TRUE(provider->portViews().empty());
}

TEST_F(ProbeCloseTest, DeleteFailureKeepsPageAndRetries) {
    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    auto provider = makeProvider(&directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider->snapshot(graph).ok());

    adapter_->failNextDeleteJetty();
    EXPECT_FALSE(provider->shutdown().ok());

    auto stats = provider->closeStats();
    EXPECT_EQ(stats.orphaned_ports, 1U);
    EXPECT_EQ(stats.orphaned_pages, 2U);
    // The Jetty survived the failed delete, so its handle is retained.
    EXPECT_EQ(adapter_->liveJettyCount(), 1U);
    EXPECT_EQ(adapter_->liveLocalSegmentCount(), 2U);

    ASSERT_TRUE(provider->shutdown().ok());
    stats = provider->closeStats();
    EXPECT_EQ(stats.orphaned_ports, 0U);
    EXPECT_EQ(adapter_->liveJettyCount(), 0U);
    EXPECT_EQ(adapter_->liveLocalSegmentCount(), 0U);
}

TEST_F(ProbeCloseTest, QuarantineIsOnlyReleasedWithProof) {
    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    auto provider = makeProvider(&directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider->snapshot(graph).ok());

    ProbeHandoff handoff;
    handoff.token = 7;
    handoff.lane = LaneKey{0, 99, 1};
    handoff.orphan_reason = OrphanReason::kGenerationRetired;
    provider->handoffUnproven(handoff);
    ASSERT_EQ(provider->quarantineCount(), 1U);

    adapter_->failResets();
    EXPECT_FALSE(provider->shutdown().ok());
    // INV-CLOSE-4: the port is unproven, so the reference stays.
    EXPECT_EQ(provider->quarantineCount(), 1U);

    adapter_->allowResets();
    ASSERT_TRUE(provider->shutdown().ok());
    EXPECT_EQ(provider->quarantineCount(), 0U);
}

TEST_F(ProbeCloseTest, ShutdownIsRepeatableWithoutDoubleFree) {
    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    auto provider = makeProvider(&directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider->snapshot(graph).ok());

    ASSERT_TRUE(provider->shutdown().ok());
    const auto first = provider->closeStats();
    EXPECT_FALSE(first.shutdown_incomplete);
    EXPECT_EQ(first.orphaned_pages, 0U);
    EXPECT_EQ(adapter_->liveLocalSegmentCount(), 0U);

    // Nothing left to release: a second call is a no-op, not a second free.
    ASSERT_TRUE(provider->shutdown().ok());
    const auto second = provider->closeStats();
    EXPECT_EQ(second.orphaned_pages, 0U);
    EXPECT_EQ(second.orphaned_ports, 0U);
    EXPECT_EQ(adapter_->liveJettyCount(), 0U);
    EXPECT_EQ(adapter_->liveLocalSegmentCount(), 0U);
}

// D3: a port orphaned by a failed rollback teardown must be retried on the
// next resource creation instead of silently leaving the probe graph forever.
TEST(UbProbeStandaloneProviderTest, OrphanedPortIsRecoveredOnRetry) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    // An invalid edge makes snapshot() fail *after* the local resources were
    // created, which drives the rollback path.
    StubPeerDirectory directory({ProbeEdgeSupply{}});
    StandaloneProbeResourceProvider provider(
        adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, context, nullptr, 1}}, &directory);

    // The rollback teardown cannot prove destruction: reset fails.
    adapter->failResets();
    ProbeGraph failed;
    EXPECT_FALSE(provider.snapshot(failed).ok());
    auto stats = provider.closeStats();
    EXPECT_EQ(stats.orphaned_ports, 1U);
    EXPECT_EQ(stats.recovered_ports, 0U);

    // Provider recovers and the graph becomes valid: the retry must first
    // destroy the orphan, then re-create the port.
    adapter->allowResets();
    directory.set({makeEdge(0, 42, 0)});
    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    stats = provider.closeStats();
    EXPECT_EQ(stats.orphaned_ports, 0U);
    EXPECT_EQ(stats.recovered_ports, 1U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
