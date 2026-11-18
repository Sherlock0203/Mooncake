// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3g-c: the bootstrap-driven probe peer source (design §7.2 / §11).
//
// The source is the production replacement for the W1 test-only descriptor
// source. These tests pin the "no guessing" contract: a peer without a probe
// page contributes no edge, an ambiguous page set is rejected, and only the
// latest bootstrap for one directed edge survives.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "tent/transport/ub/bootstrap_peer_source.h"
#include "ub_fake_urma.h"

namespace mooncake::tent::ub {
namespace {

using namespace mooncake::tent::ub::fake;

BootstrapProbePeerInfo makeInfo() {
    BootstrapProbePeerInfo info;
    info.local_nic = 0;
    info.peer_segment_id = 42;
    info.remote_nic = 0;
    info.peer_nic_path = "peer@ub:fake0:eid0";
    info.peer_eid = "0001:0000:0000:0000:0000:0000:0000:0002";
    info.capabilities = {"read", "write", "bootstrap_v2"};
    info.topology_epoch = 3;
    info.target_epoch = 4;
    info.endpoint_generation = 100;
    info.schema_version = kUbBootstrapSchemaV2;
    UbProbeAreaDescriptor area;
    area.descriptor = makeDescriptor(0xA00000, 4096);
    area.address = 0xA00000;
    area.length = 4096;
    area.token = 0xACFE;
    area.area_epoch = 7;
    area.uasid = 501;
    area.jetty_id = 402;
    info.probe_areas = {area};
    return info;
}

TEST(UbBootstrapPeerSourceTest, V2PeerWithAreaBecomesOneProbeDescriptor) {
    BootstrapProbePeerDescriptorSource source;
    ASSERT_TRUE(source.ingest(makeInfo()).ok());
    EXPECT_EQ(source.descriptorCount(), 1U);
    EXPECT_EQ(source.probeAreaMissing(), 0U);
    EXPECT_EQ(source.rejected(), 0U);

    std::vector<ProbePeerDescriptor> descriptors;
    ASSERT_TRUE(source.snapshot(descriptors).ok());
    ASSERT_EQ(descriptors.size(), 1U);
    const auto& descriptor = descriptors.front();
    EXPECT_EQ(descriptor.schema_version, kProbePeerDescriptorSchemaVersion);
    EXPECT_EQ(descriptor.local_nic, 0);
    EXPECT_EQ(descriptor.peer_segment_id, 42U);
    EXPECT_EQ(descriptor.remote_nic, 0);
    EXPECT_EQ(descriptor.peer_nic_path, "peer@ub:fake0:eid0");
    EXPECT_EQ(descriptor.topology_epoch, 3U);
    EXPECT_EQ(descriptor.target_epoch, 4U);
    EXPECT_EQ(descriptor.area_length, 4096U);
    EXPECT_EQ(descriptor.area_epoch, 7U);
    EXPECT_EQ(descriptor.area_token, 0xACFEU);
    EXPECT_EQ(descriptor.area_descriptor.hex, "10485760:4096");
    EXPECT_EQ(descriptor.remote_jetty.id, 402U);
    EXPECT_EQ(descriptor.remote_jetty.uasid, 501U);
    EXPECT_EQ(descriptor.remote_jetty.eid, makeInfo().peer_eid);
    EXPECT_EQ(descriptor.endpoint_generation, 100U);
    EXPECT_TRUE(descriptor.node_gate_ok);
    EXPECT_TRUE(descriptor.port_gate_ok);
    EXPECT_TRUE(descriptor.profile_gate_ok);
}

TEST(UbBootstrapPeerSourceTest, PeerWithoutProbeAreaContributesNoEdge) {
    BootstrapProbePeerDescriptorSource source;
    auto v1 = makeInfo();
    v1.schema_version = kUbBootstrapSchemaV1;
    v1.probe_areas.clear();
    ASSERT_TRUE(source.ingest(v1).ok());

    auto v2_without_area = makeInfo();
    v2_without_area.probe_areas.clear();
    ASSERT_TRUE(source.ingest(v2_without_area).ok());

    EXPECT_EQ(source.descriptorCount(), 0U);
    EXPECT_EQ(source.probeAreaMissing(), 2U);
    EXPECT_EQ(source.rejected(), 0U);
    std::vector<ProbePeerDescriptor> descriptors;
    ASSERT_TRUE(source.snapshot(descriptors).ok());
    EXPECT_TRUE(descriptors.empty());
}

TEST(UbBootstrapPeerSourceTest, AmbiguousOrIncompleteAreasAreRejected) {
    BootstrapProbePeerDescriptorSource source;
    auto two_areas = makeInfo();
    two_areas.probe_areas.push_back(two_areas.probe_areas.front());
    EXPECT_TRUE(source.ingest(two_areas).IsInvalidArgument());
    EXPECT_EQ(source.rejected(), 1U);
    EXPECT_EQ(source.descriptorCount(), 0U);

    auto zero_length = makeInfo();
    zero_length.probe_areas[0].length = 0;
    EXPECT_TRUE(source.ingest(zero_length).IsInvalidArgument());
    EXPECT_EQ(source.rejected(), 2U);

    auto no_jetty = makeInfo();
    no_jetty.probe_areas[0].jetty_id = 0;
    EXPECT_TRUE(source.ingest(no_jetty).IsInvalidArgument());
    EXPECT_EQ(source.rejected(), 3U);

    std::vector<ProbePeerDescriptor> descriptors;
    ASSERT_TRUE(source.snapshot(descriptors).ok());
    EXPECT_TRUE(descriptors.empty());
}

TEST(UbBootstrapPeerSourceTest, DirectedEdgeIdentityIsRequired) {
    BootstrapProbePeerDescriptorSource source;
    auto local_segment = makeInfo();
    local_segment.peer_segment_id = LOCAL_SEGMENT_ID;
    EXPECT_FALSE(source.ingest(local_segment).ok());

    auto missing_path = makeInfo();
    missing_path.peer_nic_path.clear();
    EXPECT_FALSE(source.ingest(missing_path).ok());

    auto bad_port = makeInfo();
    bad_port.remote_nic = -1;
    EXPECT_FALSE(source.ingest(bad_port).ok());
    EXPECT_EQ(source.rejected(), 3U);
}

TEST(UbBootstrapPeerSourceTest, ReBootstrapReplacesTheEdgeAndSnapshotIsStable) {
    BootstrapProbePeerDescriptorSource source;
    ASSERT_TRUE(source.ingest(makeInfo()).ok());
    // A newer bootstrap for the same directed edge (new page epoch) must not
    // create a second descriptor and must win in the snapshot.
    auto newer = makeInfo();
    newer.topology_epoch = 5;
    newer.target_epoch = 6;
    newer.probe_areas[0].area_epoch = 9;
    newer.probe_areas[0].descriptor = makeDescriptor(0xB00000, 4096);
    ASSERT_TRUE(source.ingest(newer).ok());
    EXPECT_EQ(source.descriptorCount(), 1U);

    std::vector<ProbePeerDescriptor> first;
    std::vector<ProbePeerDescriptor> second;
    ASSERT_TRUE(source.snapshot(first).ok());
    ASSERT_TRUE(source.snapshot(second).ok());
    ASSERT_EQ(first.size(), 1U);
    EXPECT_EQ(first[0].area_epoch, 9U);
    EXPECT_EQ(first[0].topology_epoch, 5U);
    EXPECT_EQ(first[0].target_epoch, 6U);
    EXPECT_EQ(first[0].area_descriptor.hex, "11534336:4096");
    EXPECT_EQ(second[0].area_epoch, 9U);
}

}  // namespace
}  // namespace mooncake::tent::ub
