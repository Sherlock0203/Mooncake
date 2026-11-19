// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Declared topology resolution and per-slice adjacency views (design
// docs/analyze/tent-ub-probe-wiring-and-adjacency-design.md §4–§5).

#include <gtest/gtest.h>

#include <cctype>
#include <string>
#include <vector>

#include "tent/runtime/segment.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/probe_topology.h"
#include "tent/transport/ub/topology_attrs.h"

namespace mooncake::tent::ub {
namespace {

constexpr const char* kEid0 = "0000:0000:0000:0000:0000:0000:0000:0010";
constexpr const char* kEid1 = "0000:0000:0000:0000:0000:0000:0000:0011";
constexpr const char* kEid2 = "0000:0000:0000:0000:0000:0000:0000:0012";
constexpr const char* kPeerEid0 = "0000:0000:0000:0000:0000:0000:0000:0020";
constexpr const char* kPeerEid1 = "0000:0000:0000:0000:0000:0000:0000:0021";
constexpr const char* kPeerEid2 = "0000:0000:0000:0000:0000:0000:0000:0022";

DeviceInfo makeDevice(const std::string& native_name, const std::string& eid,
                      uint32_t eid_index, bool active = true) {
    DeviceInfo device;
    device.native_device_name = native_name;
    device.topology_name =
        "ub:" + native_name + ":eid" + std::to_string(eid_index);
    device.native_device_path = "/dev/" + native_name;
    device.eid_index = eid_index;
    device.eid = eid;
    device.active = active;
    device.kind = DeviceKind::PHYSICAL;
    device.capabilities.transport_modes = 3;  // RC | RM
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

SegmentDesc makePeerSegment(const std::vector<DeviceInfo>& devices) {
    SegmentDesc desc;
    desc.name = "node-b";
    desc.type = SegmentType::Memory;
    MemorySegmentDesc memory;
    for (const auto& device : devices) addUbNic(memory.topology, device);
    desc.detail = std::move(memory);
    return desc;
}

Status parseProbeParams(const std::string& json_text, ProbeParams& output) {
    Config config;
    const Status loaded = config.load(json_text);
    if (!loaded.ok()) return loaded;
    UbParams params;
    CHECK_STATUS(UbParams::FromConfig(config, params));
    output = params.probe;
    return Status::OK();
}

std::string declaredJson(const std::string& local_ports,
                         const std::string& adjacency) {
    return R"({
        "transports": {"ub": {
            "runtime_profile": "rm_legacy",
            "trans_mode": "rm",
            "probe": {
                "enabled": true,
                "provider": "standalone",
                "local_ports": )" +
           local_ports +
           R"(,
                "peers": {"node-b": {"ports": [
                    {"id": "R0", "eid": "0000:0000:0000:0000:0000:0000:0000:0020"},
                    {"id": "R1", "eid": "0000:0000:0000:0000:0000:0000:0000:0021"}
                ]}},
                "adjacency": )" +
           adjacency + R"(}
        }}
    })";
}

TEST(UbProbeTopologyTest, ResolvesBindingsAndBuildsAdjacencyView) {
    Topology local;
    const auto d0 = makeDevice("udma0", kEid0, 0);
    const auto d1 = makeDevice("udma1", kEid1, 0);
    const auto nic0 = addUbNic(local, d0);
    const auto nic1 = addUbNic(local, d1);
    ASSERT_EQ(nic0, 0);
    ASSERT_EQ(nic1, 1);

    ProbeParams params;
    const Status parsed = parseProbeParams(
        declaredJson(R"([
                {"id": "P0", "bind": "ub:udma0:eid0", "eid": ")" +
                         std::string(kEid0) + R"("},
                {"id": "P1", "bind": "udma1"}
            ])",
                     R"({"node-b": {"P0": ["R0", "R1"], "P1": ["R0"]}})"),
        params);
    ASSERT_TRUE(parsed.ok()) << parsed.ToString();

    ProbeTopology topology;
    ASSERT_TRUE(ProbeTopology::resolve(params, {d0, d1}, local, topology).ok());
    ASSERT_TRUE(topology.declared());
    ASSERT_TRUE(topology.enforces());
    ASSERT_EQ(topology.ports().size(), 2u);
    ASSERT_TRUE(topology.localNic("P0").has_value());
    EXPECT_EQ(*topology.localNic("P0"), nic0);
    EXPECT_EQ(*topology.localNic("P1"), nic1);
    EXPECT_EQ(*topology.localIdForNic(nic1), "P1");

    const auto peer0 = makeDevice("udma0", kPeerEid0, 0);
    const auto peer1 = makeDevice("udma1", kPeerEid1, 0);
    const auto peer2 = makeDevice("udma2", kPeerEid2, 0);
    const auto segment = makePeerSegment({peer0, peer1, peer2});

    const AdjacencyView view = topology.viewFor("node-b", segment);
    EXPECT_TRUE(view.enforced);
    EXPECT_TRUE(view.peer_declared);
    EXPECT_FALSE(view.peer_eid_unmatched);
    ASSERT_EQ(view.local_nics.size(), 2u);
    ASSERT_EQ(view.remote_nics.size(), 2u);
    EXPECT_EQ(view.check(nic0, 0), AdjacencyDecision::kAllowed);
    EXPECT_EQ(view.check(nic0, 1), AdjacencyDecision::kAllowed);
    EXPECT_EQ(view.check(nic1, 0), AdjacencyDecision::kAllowed);
    // P1 -> R1 is not declared even though both ports exist.
    EXPECT_EQ(view.check(nic1, 1), AdjacencyDecision::kUndeclaredRemote);
    // A remote UB NIC that exists in metadata but is not declared.
    EXPECT_EQ(view.check(nic0, 2), AdjacencyDecision::kUndeclaredRemote);
    // A local NIC outside the declaration.
    EXPECT_EQ(view.check(7, 0), AdjacencyDecision::kUndeclaredLocal);
    bool local_declared = true;
    EXPECT_EQ(view.check(7, 0, &local_declared),
              AdjacencyDecision::kUndeclaredLocal);
    EXPECT_FALSE(local_declared);

    // A peer with no adjacency entry is rejected as a whole.
    EXPECT_EQ(topology.viewFor("node-z", segment).check(nic0, 0),
              AdjacencyDecision::kUndeclaredPeer);
}

TEST(UbProbeTopologyTest, PeerEidMissingFromMetadataFailsClosed) {
    Topology local;
    const auto d0 = makeDevice("udma0", kEid0, 0);
    addUbNic(local, d0);

    ProbeParams params;
    ASSERT_TRUE(parseProbeParams(
                    declaredJson(R"([{"id": "P0", "bind": "ub:udma0:eid0"}])",
                                 R"({"node-b": {"P0": ["R0", "R1"]}})"),
                    params)
                    .ok());
    ProbeTopology topology;
    ASSERT_TRUE(ProbeTopology::resolve(params, {d0}, local, topology).ok());

    // The peer only publishes R0; R1 is declared but unknown.
    const auto segment = makePeerSegment({makeDevice("udma0", kPeerEid0, 0)});
    const AdjacencyView view = topology.viewFor("node-b", segment);
    EXPECT_TRUE(view.enforced);
    EXPECT_TRUE(view.peer_eid_unmatched);
    // Every candidate for this peer fails closed, including R0.
    EXPECT_EQ(view.check(0, 0), AdjacencyDecision::kPeerEidUnmatched);
}

TEST(UbProbeTopologyTest, EidsAreMatchedCaseInsensitively) {
    Topology local;
    const auto d0 = makeDevice("udma0", kEid0, 0);
    addUbNic(local, d0);

    ProbeParams params;
    std::string upper = kPeerEid0;
    for (char& c : upper) c = static_cast<char>(std::toupper(c));
    const std::string config = R"({
        "transports": {"ub": {
            "runtime_profile": "rm_legacy", "trans_mode": "rm",
            "probe": {
                "enabled": true, "provider": "standalone",
                "local_ports": [
                    {"id": "P0", "bind": "ub:udma0:eid0"}
                ],
                "peers": {"node-b": {"ports": [{"id": "R0", "eid": ")" +
                               upper + R"("}]}},
                "adjacency": {"node-b": {"P0": ["R0"]}}
            }
        }}
    })";
    ASSERT_TRUE(parseProbeParams(config, params).ok());
    ProbeTopology topology;
    ASSERT_TRUE(ProbeTopology::resolve(params, {d0}, local, topology).ok());
    const auto segment = makePeerSegment({makeDevice("udma0", kPeerEid0, 0)});
    EXPECT_EQ(topology.viewFor("node-b", segment).check(0, 0),
              AdjacencyDecision::kAllowed);
}

TEST(UbProbeTopologyTest, EmptyDeclarationDoesNotEnforce) {
    Topology local;
    ProbeParams params;  // probe off, no declaration
    ProbeTopology topology;
    ASSERT_TRUE(ProbeTopology::resolve(params, {}, local, topology).ok());
    EXPECT_FALSE(topology.declared());
    EXPECT_FALSE(topology.enforces());

    SegmentDesc segment = makePeerSegment({});
    const AdjacencyView view = topology.viewFor("node-b", segment);
    EXPECT_FALSE(view.enforced);
    EXPECT_EQ(view.check(0, 0), AdjacencyDecision::kAllowed);
}

TEST(UbProbeTopologyTest, ResolveRejectsAmbiguousOrMismatchedBindings) {
    Topology local;
    const auto d0 = makeDevice("udma0", kEid0, 0);
    const auto d1 = makeDevice("udma0", kEid1, 1);  // second EID, same device
    addUbNic(local, d0);
    addUbNic(local, d1);

    ProbeParams params;
    // A native-device bind matches two EIDs: ambiguous and rejected.
    ASSERT_TRUE(
        parseProbeParams(declaredJson(R"([{"id": "P0", "bind": "udma0"}])",
                                      R"({"node-b": {"P0": ["R0"]}})"),
                         params)
            .ok());
    ProbeTopology topology;
    EXPECT_TRUE(ProbeTopology::resolve(params, {d0, d1}, local, topology)
                    .IsInvalidArgument());

    // No match at all.
    ASSERT_TRUE(
        parseProbeParams(declaredJson(R"([{"id": "P0", "bind": "missing"}])",
                                      R"({"node-b": {"P0": ["R0"]}})"),
                         params)
            .ok());
    EXPECT_TRUE(ProbeTopology::resolve(params, {d0, d1}, local, topology)
                    .IsInvalidArgument());

    // EID assertion disagrees with the device selected by bind.
    ProbeParams bad_eid;
    ASSERT_TRUE(
        parseProbeParams(
            declaredJson(R"([{"id": "P0", "bind": "udma0:eid0", "eid": ")" +
                             std::string(kEid1) + R"("}])",
                         R"({"node-b": {"P0": ["R0"]}})"),
            bad_eid)
            .ok());
    EXPECT_TRUE(ProbeTopology::resolve(bad_eid, {d0, d1}, local, topology)
                    .IsInvalidArgument());
}

}  // namespace
}  // namespace mooncake::tent::ub
