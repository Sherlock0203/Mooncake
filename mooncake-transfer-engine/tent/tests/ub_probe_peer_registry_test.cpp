// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// ProbePeerRegistry withdrawal semantics and the BootstrapProbePeerSource join
// with the declared topology (design §6.3/§7.6).

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "tent/runtime/segment.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/probe_peer_registry.h"
#include "tent/transport/ub/probe_peer_source.h"
#include "tent/transport/ub/probe_topology.h"
#include "tent/transport/ub/topology_attrs.h"

namespace mooncake::tent::ub {
namespace {

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

std::map<std::string, std::vector<ProbeDeclaredPeerPort>> declaredPeers() {
    return {{"node-b", {{"R0", kPeerEid0}, {"R1", kPeerEid1}}}};
}

ProbePeerBinding makePublication(uint64_t incarnation, uint64_t epoch,
                                 bool include_r1, bool gates_ok = true) {
    ProbePeerBinding binding;
    binding.segment = "node-b";
    binding.peer_id = kProbePeerIdBase;
    binding.node_incarnation = incarnation;
    binding.topology_epoch = 7;
    binding.publication_epoch = epoch;
    ProbePeerPortBinding r0;
    r0.id = "R0";
    r0.eid = kPeerEid0;
    r0.remote_nic = 0;
    r0.topology_name = "ub:peer0:eid0";
    r0.peer_nic_path = "node-b@ub:peer0:eid0";
    r0.target_epoch = 3;
    r0.jetty = RemoteJettyInfo{kPeerEid0, 101, 0, 0xACFE};
    r0.area_descriptor.schema_version = 1;
    r0.area_descriptor.hex = "0011";
    r0.area_length = 4096;
    r0.area_epoch = 1;
    r0.area_token = 0xACFE;
    r0.node_gate_ok = gates_ok;
    r0.port_gate_ok = gates_ok;
    r0.profile_gate_ok = gates_ok;
    binding.ports.push_back(r0);
    if (include_r1) {
        ProbePeerPortBinding r1 = r0;
        r1.id = "R1";
        r1.eid = kPeerEid1;
        r1.remote_nic = 1;
        r1.topology_name = "ub:peer1:eid0";
        r1.jetty = RemoteJettyInfo{kPeerEid1, 102, 0, 0xACFE};
        binding.ports.push_back(std::move(r1));
    }
    return binding;
}

TEST(UbProbePeerRegistryTest, AppliesPublicationsAndShortCircuitsUnchanged) {
    ProbePeerRegistry registry(declaredPeers(), 3);
    ASSERT_EQ(registry.peerCount(), 1U);
    ASSERT_TRUE(registry.peerId("node-b").has_value());
    EXPECT_EQ(*registry.peerId("node-b"), kProbePeerIdBase);

    bool applied = false;
    ASSERT_TRUE(
        registry.apply(makePublication(11, 1, true), 100, &applied).ok());
    EXPECT_TRUE(applied);
    auto binding = registry.find("node-b");
    ASSERT_TRUE(binding.has_value());
    EXPECT_TRUE(binding->published);
    EXPECT_FALSE(binding->stale);
    EXPECT_EQ(binding->node_incarnation, 11U);
    EXPECT_EQ(binding->publication_epoch, 1U);
    ASSERT_EQ(binding->ports.size(), 2U);
    EXPECT_EQ(binding->ports[0].id, "R0");
    EXPECT_FALSE(binding->ports[0].withdrawn);

    // Same epoch is acknowledged but does not rebuild the graph.
    applied = true;
    ASSERT_TRUE(
        registry.apply(makePublication(11, 1, true), 200, &applied).ok());
    EXPECT_FALSE(applied);

    // A different node incarnation is a new generation even though the
    // numeric value is smaller (the seed is not monotonic).
    applied = false;
    auto restarted = makePublication(10, 2, true);
    restarted.topology_epoch = 8;
    ASSERT_TRUE(registry.apply(restarted, 300, &applied).ok());
    EXPECT_TRUE(applied);
    EXPECT_EQ(registry.find("node-b")->node_incarnation, 10U);

    // An in-generation older publication is still ignored.
    applied = true;
    auto older = makePublication(10, 1, true);
    older.topology_epoch = 8;
    ASSERT_TRUE(registry.apply(older, 400, &applied).ok());
    EXPECT_FALSE(applied);
    EXPECT_EQ(registry.find("node-b")->publication_epoch, 2U);
}

TEST(UbProbePeerRegistryTest, RejectsAPublicationWithTheWrongPeerId) {
    ProbePeerRegistry registry(declaredPeers(), 3);
    auto publication = makePublication(11, 1, true);
    publication.peer_id = kProbePeerIdBase + 7;
    EXPECT_TRUE(registry.apply(publication, 100).IsInvalidArgument());
    EXPECT_FALSE(registry.find("node-b")->published);
}

TEST(UbProbePeerRegistryTest, ReinstallWithAResetPublicationEpochIsAccepted) {
    ProbePeerRegistry registry(declaredPeers(), 3);
    ASSERT_TRUE(registry.apply(makePublication(11, 9, true), 100).ok());

    // Same process incarnation, new install: topology_epoch changes but the
    // provider publication epoch restarts at 1. This must be accepted.
    auto reinstall = makePublication(11, 1, true);
    reinstall.topology_epoch = 8;
    bool applied = false;
    ASSERT_TRUE(registry.apply(reinstall, 200, &applied).ok());
    EXPECT_TRUE(applied);
    EXPECT_EQ(registry.find("node-b")->publication_epoch, 1U);
    EXPECT_EQ(registry.find("node-b")->topology_epoch, 8U);
}

TEST(UbProbePeerRegistryTest, SamePublicationClearsStale) {
    ProbePeerRegistry registry(declaredPeers(), 3);
    ASSERT_TRUE(registry.apply(makePublication(11, 1, true), 100).ok());
    registry.markStale("node-b", 200);
    ASSERT_TRUE(registry.find("node-b")->stale);

    bool applied = true;
    ASSERT_TRUE(
        registry.apply(makePublication(11, 1, true), 300, &applied).ok());
    EXPECT_FALSE(applied);
    EXPECT_FALSE(registry.find("node-b")->stale);
    EXPECT_EQ(registry.find("node-b")->last_success_ns, 300U);
}

TEST(UbProbePeerRegistryTest, WithdrawsAfterGraceAndReturnsOnReappearance) {
    ProbePeerRegistry registry(declaredPeers(), 3);
    ASSERT_TRUE(registry.apply(makePublication(11, 1, true), 100).ok());

    // R1 is absent from the next publication. A single miss is not enough.
    ASSERT_TRUE(registry.apply(makePublication(11, 2, false), 200).ok());
    auto binding = registry.find("node-b");
    ASSERT_EQ(binding->ports.size(), 2U);
    EXPECT_FALSE(binding->ports[1].withdrawn);

    ASSERT_TRUE(registry.apply(makePublication(11, 3, false), 300).ok());
    ASSERT_TRUE(registry.apply(makePublication(11, 4, false), 400).ok());
    binding = registry.find("node-b");
    ASSERT_EQ(binding->ports.size(), 2U);
    EXPECT_TRUE(binding->ports[1].withdrawn);
    EXPECT_FALSE(binding->ports[0].withdrawn);

    // Reappearance clears the withdrawal immediately.
    ASSERT_TRUE(registry.apply(makePublication(11, 5, true), 500).ok());
    binding = registry.find("node-b");
    EXPECT_FALSE(binding->ports[1].withdrawn);

    // A publication that matches no declared port is rejected and keeps the
    // previous snapshot.
    bool applied = true;
    auto empty = makePublication(11, 6, false);
    empty.ports.clear();
    ASSERT_TRUE(registry.apply(empty, 600, &applied).IsInvalidArgument());
    EXPECT_FALSE(applied);
    EXPECT_EQ(registry.find("node-b")->publication_epoch, 5U);
}

TEST(UbProbePeerRegistryTest, MarkStaleKeepsThePublication) {
    ProbePeerRegistry registry(declaredPeers(), 3);
    ASSERT_TRUE(registry.apply(makePublication(11, 1, true), 100).ok());
    registry.markStale("node-b", 200);
    auto binding = registry.find("node-b");
    EXPECT_TRUE(binding->published);
    EXPECT_TRUE(binding->stale);
    EXPECT_EQ(binding->publication_epoch, 1U);
}

SegmentDesc makePeerSegment(const std::vector<DeviceInfo>& devices) {
    SegmentDesc desc;
    desc.name = "node-b";
    desc.type = SegmentType::Memory;
    MemorySegmentDesc memory;
    for (const auto& device : devices) addUbNic(memory.topology, device);
    desc.detail = std::move(memory);
    return desc;
}

ProbeTopology makeTopology(Topology& local, DeviceInfo& device) {
    Config config;
    EXPECT_TRUE(config
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
                "adjacency": {"node-b": {"P0": ["R0", "R1"]}}
            }
        }}
    })")
                    .ok());
    UbParams params;
    EXPECT_TRUE(UbParams::FromConfig(config, params).ok());
    device = makeDevice("local0", kLocalEid);
    addUbNic(local, device);
    ProbeTopology topology;
    EXPECT_TRUE(
        ProbeTopology::resolve(params.probe, {device}, local, topology).ok());
    return topology;
}

TEST(UbProbePeerSourceTest, EmitsOnlyDeclaredPublishedEdges) {
    Topology local;
    DeviceInfo device;
    auto topology = makeTopology(local, device);
    ProbePeerRegistry registry(declaredPeers(), 3);
    BootstrapProbePeerSource source(&topology, &registry);

    std::vector<ProbePeerDescriptor> descriptors;
    ASSERT_TRUE(source.snapshot(descriptors).ok());
    EXPECT_TRUE(descriptors.empty());

    ASSERT_TRUE(registry.apply(makePublication(11, 1, true), 100).ok());
    ASSERT_TRUE(source.snapshot(descriptors).ok());
    ASSERT_EQ(descriptors.size(), 2U);
    EXPECT_EQ(descriptors[0].local_nic, 0);
    EXPECT_EQ(descriptors[0].peer_segment_id, kProbePeerIdBase);
    EXPECT_EQ(descriptors[0].target_epoch, 3U);
    EXPECT_EQ(descriptors[0].area_length, 4096U);
    EXPECT_EQ(descriptors[0].remote_jetty.id, 101U);
    EXPECT_TRUE(descriptors[0].node_gate_ok);
    EXPECT_EQ(descriptors[1].remote_jetty.id, 102U);

    // Withdrawn R1 is not emitted; the declared R0 edge remains.
    for (int round = 0; round < 3; ++round) {
        ASSERT_TRUE(
            registry.apply(makePublication(11, 2 + round, false), 200 + round)
                .ok());
    }
    descriptors.clear();
    ASSERT_TRUE(source.snapshot(descriptors).ok());
    ASSERT_EQ(descriptors.size(), 1U);
    EXPECT_EQ(descriptors[0].remote_jetty.id, 101U);
    EXPECT_EQ(source.skippedWithdrawn(), 1U);
}

TEST(UbProbePeerRegistryTest, AdjacencyViewTracksPublicationAndGates) {
    Topology local;
    DeviceInfo device;
    auto topology = makeTopology(local, device);
    ProbePeerRegistry registry(declaredPeers(), 3);
    topology.setPeerRegistry(&registry);

    const auto peer0 = makeDevice("peer0", kPeerEid0);
    const auto peer1 = makeDevice("peer1", kPeerEid1);
    const auto segment = makePeerSegment({peer0, peer1});

    // Before the first publication every declared edge stays usable.
    auto view = topology.viewFor("node-b", segment);
    EXPECT_FALSE(view.publication_seen);
    EXPECT_EQ(view.check(0, 0), AdjacencyDecision::kAllowed);
    EXPECT_EQ(view.check(0, 1), AdjacencyDecision::kAllowed);

    // Published with a closed gate: business is blocked, not silently healed.
    ASSERT_TRUE(registry.apply(makePublication(11, 1, true, false), 100).ok());
    view = topology.viewFor("node-b", segment);
    EXPECT_TRUE(view.publication_seen);
    EXPECT_EQ(view.check(0, 0), AdjacencyDecision::kPeerGateBlocked);

    // Re-publish with gates open, then withdraw R1 after the grace period.
    ASSERT_TRUE(registry.apply(makePublication(11, 2, true, true), 200).ok());
    for (int round = 0; round < 3; ++round) {
        ASSERT_TRUE(
            registry.apply(makePublication(11, 3 + round, false), 300 + round)
                .ok());
    }
    view = topology.viewFor("node-b", segment);
    EXPECT_EQ(view.check(0, 0), AdjacencyDecision::kAllowed);
    EXPECT_EQ(view.check(0, 1), AdjacencyDecision::kPortMissing);
}

TEST(UbProbePeerRegistryTest, AdjacencyViewFailsClosedForFileSegments) {
    Topology local;
    DeviceInfo device;
    auto topology = makeTopology(local, device);
    ProbePeerRegistry registry(declaredPeers(), 3);
    topology.setPeerRegistry(&registry);

    SegmentDesc file_segment;
    file_segment.name = "node-b";
    file_segment.type = SegmentType::File;

    const auto view = topology.viewFor("node-b", file_segment);
    EXPECT_TRUE(view.enforced);
    EXPECT_TRUE(view.peer_eid_unmatched);
    EXPECT_EQ(view.check(0, 0), AdjacencyDecision::kPeerEidUnmatched);
}

}  // namespace
}  // namespace mooncake::tent::ub
