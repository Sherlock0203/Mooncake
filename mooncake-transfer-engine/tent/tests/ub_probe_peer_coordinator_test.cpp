// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// ProbePeerCoordinator handshake/apply/backoff and the full declared-graph
// path: publication -> registry -> source -> provider diff (design §7).

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_registry.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/probe_peer_coordinator.h"
#include "tent/transport/ub/probe_peer_registry.h"
#include "tent/transport/ub/probe_peer_source.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "tent/transport/ub/probe_topology.h"
#include "tent/transport/ub/topology_attrs.h"
#include "ub_fake_urma.h"

namespace mooncake::tent::ub {
namespace {

using namespace fake;

constexpr const char* kLocalEid = "0000:0000:0000:0000:0000:0000:0000:0010";
constexpr const char* kPeerEid0 = "0000:0000:0000:0000:0000:0000:0000:0020";
constexpr const char* kPeerEid1 = "0000:0000:0000:0000:0000:0000:0000:0021";

DeviceInfo makeDevice(const std::string& native_name, const std::string& eid) {
    DeviceInfo device;
    device.native_device_name = native_name;
    device.topology_name = "ub:" + native_name + ":eid0";
    device.native_device_path = "/dev/" + native_name;
    device.eid = eid;
    device.active = true;
    device.kind = DeviceKind::PHYSICAL;
    device.capabilities.transport_modes = 3;
    device.capabilities.max_jfc = 4;
    device.capabilities.max_jetty = 64;
    return device;
}

Topology::NicID addUbNic(Topology& topology, const DeviceInfo& device) {
    Topology::NicEntry nic;
    nic.name = device.topology_name;
    nic.type = Topology::NIC_UB;
    nic.numa_node = 0;
    encodeTopologyDeviceAttributes(device, 0, nic.device_attrs);
    topology.nic_list_.push_back(std::move(nic));
    return static_cast<Topology::NicID>(topology.nic_list_.size() - 1);
}

class StaticRegistry final : public SegmentRegistry {
   public:
    explicit StaticRegistry(SegmentDescRef segment)
        : segment_(std::move(segment)) {}

    Status getSegmentDesc(SegmentDescRef& desc,
                          const std::string& segment_name) override {
        if (segment_name != segment_->name) {
            return Status::InvalidEntry("unknown segment");
        }
        desc = segment_;
        return Status::OK();
    }
    Status putSegmentDesc(SegmentDescRef&) override { return Status::OK(); }
    Status deleteSegmentDesc(const std::string&) override {
        return Status::OK();
    }

   private:
    SegmentDescRef segment_;
};

class RecordingAvailabilitySink final : public ProbeAvailabilitySink {
   public:
    Status onPoolAvailabilityChange(const ProbeEvent& event) override {
        events.push_back(event);
        return Status::OK();
    }
    std::vector<ProbeEvent> events;
};

struct Fixture {
    Topology local;
    DeviceInfo local_device;
    DeviceInfo peer0;
    DeviceInfo peer1;
    ProbeParams params;
    std::shared_ptr<FakeUrmaAdapter> adapter;
    std::unique_ptr<StandaloneProbeResourceProvider> provider;
    std::unique_ptr<ProbePeerRegistry> registry;
    std::unique_ptr<BootstrapProbePeerSource> source;
    std::unique_ptr<HandshakeProbePeerDirectory> directory;
    std::unique_ptr<SegmentManager> segments;
    SegmentDescRef peer_segment;
    RecordingAvailabilitySink sink;

    bool build() {
        Config config;
        if (!config
                 .load(R"({
            "transports": {"ub": {
                "runtime_profile": "rm_legacy", "trans_mode": "rm",
                "probe": {
                    "enabled": true, "provider": "standalone",
                    "local_ports": [{"id": "P0", "bind": "ub:local0:eid0"}],
                    "peers": {"node-b": {"ports": [
                        {"id": "R0", "eid": "0000:0000:0000:0000:0000:0000:0000:0020"},
                        {"id": "R1", "eid": "0000:0000:0000:0000:0000:0000:0000:0021"}
                    ]}},
                    "adjacency": {"node-b": {"P0": ["R0", "R1"]}},
                    "handshake": {"refresh_interval_ms": 30000,
                                  "retry_backoff_ms": 1000,
                                  "timeout_ms": 2000}
                }
            }}
        })")
                 .ok()) {
            return false;
        }
        UbParams ub_params;
        if (!UbParams::FromConfig(config, ub_params).ok()) return false;
        params = ub_params.probe;
        local_device = makeDevice("local0", kLocalEid);
        addUbNic(local, local_device);
        ProbeTopology resolved;
        if (!ProbeTopology::resolve(params, {local_device}, local, resolved)
                 .ok()) {
            return false;
        }
        topology_owner = std::make_unique<ProbeTopology>(std::move(resolved));
        ProbeTopology& topology = *topology_owner;

        adapter = std::make_shared<FakeUrmaAdapter>(local_device);
        if (!adapter->initialize().ok()) return false;
        provider =
            std::make_unique<StandaloneProbeResourceProvider>(adapter, params);
        provider->setNodeIdentity("node-a", 21, 22);
        ProbePortSpec spec;
        spec.port_id = "P0";
        spec.local_nic = 0;
        spec.device = local_device;
        spec.topology_name = local_device.topology_name;
        spec.native_device_name = local_device.native_device_name;
        spec.eid = local_device.eid;
        if (!provider->bindPorts({spec}).ok()) return false;
        if (!provider->prepareLocal().ok()) return false;

        std::map<std::string, std::vector<ProbeDeclaredPeerPort>> declared;
        for (const auto& [peer, decl] : topology.peerDecls()) {
            auto& ports = declared[peer];
            for (const auto& port : decl.ports) {
                ports.push_back(ProbeDeclaredPeerPort{port.id, port.eid});
            }
        }
        registry = std::make_unique<ProbePeerRegistry>(declared, 3);
        source = std::make_unique<BootstrapProbePeerSource>(&topology,
                                                            registry.get());
        directory = std::make_unique<HandshakeProbePeerDirectory>(
            adapter, provider->contexts(), source.get());
        provider->setRemoteDirectory(directory.get());

        ProbeGraph graph;
        if (!provider->snapshot(graph).ok()) return false;
        provider->setAvailabilitySink(&sink);

        peer0 = makeDevice("peer0", kPeerEid0);
        peer1 = makeDevice("peer1", kPeerEid1);
        auto segment = std::make_shared<SegmentDesc>();
        segment->name = "node-b";
        segment->type = SegmentType::Memory;
        segment->rpc_server_addr = "127.0.0.1:19001";
        MemorySegmentDesc memory;
        addUbNic(memory.topology, peer0);
        addUbNic(memory.topology, peer1);
        segment->detail = std::move(memory);
        peer_segment = std::move(segment);
        segments = std::make_unique<SegmentManager>(
            std::make_unique<StaticRegistry>(peer_segment));
        topology.setPeerRegistry(registry.get());
        return true;
    }

    std::unique_ptr<ProbeTopology> topology_owner;

    ProbeBootstrapResponse response(uint64_t incarnation = 11,
                                    uint64_t publication_epoch = 1) {
        ProbeBootstrapResponse result;
        result.responder_segment = "node-b";
        result.node_incarnation = incarnation;
        result.topology_epoch = 7;
        result.publication_epoch = publication_epoch;
        result.transport_mode = "rm";
        result.device_kind = "physical";
        for (const auto& [id, device] :
             std::vector<std::pair<std::string, DeviceInfo>>{{"R0", peer0},
                                                             {"R1", peer1}}) {
            ProbeBootstrapPort port;
            port.port_id = id;
            port.topology_name = device.topology_name;
            port.eid = device.eid;
            port.target_epoch = 3;
            port.jetty = RemoteJettyInfo{device.eid, 101, 0, 0xACFE};
            port.area_descriptor =
                makeDescriptor(0xB00000 + result.ports.size() * 0x1000, 4096);
            port.area_length = 4096;
            port.area_epoch = 1;
            port.area_token = 0xACFE;
            port.node_gate_ok = true;
            port.port_gate_ok = true;
            port.profile_gate_ok = true;
            result.ports.push_back(std::move(port));
        }
        return result;
    }
};

TEST(UbProbePeerCoordinatorTest, AppliesPublicationAndAddsEveryDeclaredEdge) {
    Fixture fixture;
    ASSERT_TRUE(fixture.build());
    int rpc_calls = 0;
    ProbePeerCoordinator::Dependencies deps;
    deps.segment_manager = fixture.segments.get();
    deps.registry = fixture.registry.get();
    deps.provider = fixture.provider.get();
    deps.topology = fixture.topology_owner.get();
    deps.rpc = [&](const std::string& addr,
                   const ProbeBootstrapRequest& request,
                   ProbeBootstrapResponse& response) {
        ++rpc_calls;
        EXPECT_EQ(addr, "127.0.0.1:19001");
        EXPECT_EQ(request.requester_segment, "node-a");
        EXPECT_EQ(request.declared_local_ports.size(), 1U);
        response = fixture.response();
        return Status::OK();
    };
    ProbePeerCoordinator coordinator(fixture.params, deps);

    ASSERT_TRUE(coordinator.handshakeOnce("node-b", 1000).ok());
    EXPECT_EQ(rpc_calls, 1);
    auto binding = fixture.registry->find("node-b");
    ASSERT_TRUE(binding.has_value());
    EXPECT_TRUE(binding->published);
    ASSERT_EQ(binding->ports.size(), 2U);
    EXPECT_EQ(binding->ports[0].id, "R0");
    EXPECT_EQ(binding->ports[1].id, "R1");
    EXPECT_EQ(binding->ports[0].peer_nic_path, "node-b@ub:peer0:eid0");

    // The provider diff emits one kEdgeAdded per declared edge.
    ASSERT_EQ(fixture.sink.events.size(), 2U);
    for (const auto& event : fixture.sink.events) {
        EXPECT_EQ(event.kind, ProbeEventKind::kEdgeAdded);
    }
    const auto stats = coordinator.stats();
    EXPECT_EQ(stats.attempts, 1U);
    EXPECT_EQ(stats.success, 1U);
    EXPECT_EQ(stats.publication_apply, 1U);
}

TEST(UbProbePeerCoordinatorTest, FailureBacksOffAndKeepsLastPublication) {
    Fixture fixture;
    ASSERT_TRUE(fixture.build());
    int calls = 0;
    ProbePeerCoordinator::Dependencies deps;
    deps.segment_manager = fixture.segments.get();
    deps.registry = fixture.registry.get();
    deps.provider = fixture.provider.get();
    deps.topology = fixture.topology_owner.get();
    deps.rpc = [&](const std::string&, const ProbeBootstrapRequest&,
                   ProbeBootstrapResponse& response) {
        ++calls;
        if (calls == 1) {
            response = fixture.response();
            return Status::OK();
        }
        return Status::RpcServiceError("peer restarted");
    };
    ProbePeerCoordinator coordinator(fixture.params, deps);
    ASSERT_TRUE(coordinator.handshakeOnce("node-b", 1000).ok());
    auto before = fixture.registry->find("node-b");
    ASSERT_TRUE(before->published);

    EXPECT_FALSE(coordinator.handshakeOnce("node-b", 2000).ok());
    auto after = fixture.registry->find("node-b");
    EXPECT_TRUE(after->published);
    EXPECT_EQ(after->publication_epoch, before->publication_epoch);
    EXPECT_TRUE(after->stale);
    EXPECT_GT(coordinator.nextAttemptNs("node-b"), 0U);
    EXPECT_EQ(coordinator.stats().failure, 1U);
}

TEST(UbProbePeerCoordinatorTest, UnknownPublicationPortsAreIgnored) {
    Fixture fixture;
    ASSERT_TRUE(fixture.build());
    ProbePeerCoordinator::Dependencies deps;
    deps.segment_manager = fixture.segments.get();
    deps.registry = fixture.registry.get();
    deps.provider = fixture.provider.get();
    deps.topology = fixture.topology_owner.get();
    deps.rpc = [&](const std::string&, const ProbeBootstrapRequest&,
                   ProbeBootstrapResponse& response) {
        response = fixture.response();
        ProbeBootstrapPort extra = response.ports.front();
        extra.eid = "0000:0000:0000:0000:0000:0000:0000:0099";
        extra.port_id = "RX";
        response.ports.push_back(std::move(extra));
        return Status::OK();
    };
    ProbePeerCoordinator coordinator(fixture.params, deps);
    ASSERT_TRUE(coordinator.handshakeOnce("node-b", 1000).ok());
    EXPECT_EQ(coordinator.stats().publication_unknown_port, 1U);
    EXPECT_EQ(fixture.registry->find("node-b")->ports.size(), 2U);
}

}  // namespace
}  // namespace mooncake::tent::ub
