// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3g-b: bootstrap v2 is an additive field block, not a new protocol.
//
// These tests pin the three properties the compatibility matrix depends on:
//   1. a v1 envelope contains no v2 key at all (an old peer never sees them);
//   2. a v2 envelope round-trips every field, and a malformed field is rejected
//      instead of being partially interpreted;
//   3. the endpoint only emits v2 fields when both the local switch and the
//      peer capability allow it, and `rmTarget()` overlays the negotiated
//      profile only when the peer published one.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/transport/ub/bootstrap_v2.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/endpoint.h"
#include "tent/transport/ub/endpoint_store.h"
#include "ub_fake_urma.h"

namespace mooncake::tent {
namespace {

using namespace mooncake::tent;
using namespace mooncake::tent::ub;
using namespace mooncake::tent::ub::fake;

void appendV1Capabilities(UbBootstrapDesc& desc, bool rm) {
    desc.capabilities = {"read", "write", "endpoint_generation"};
    desc.capabilities.push_back(rm ? kUbCapabilityRmTransport
                                   : kUbCapabilityRcTransport);
}

UbBootstrapDesc makeV2PeerDescriptor(bool rm) {
    UbBootstrapDesc desc;
    desc.protocol_version = 1;
    desc.schema_version = kUbBootstrapSchemaV2;
    desc.segment_name = "peer";
    desc.local_eid = "0001:0000:0000:0000:0000:0000:0000:0002";
    desc.endpoint_generation = 7;
    desc.segment_generation = 3;
    desc.jetty_ids = {401};
    desc.jetty_uasids = {501};
    desc.topology_epoch = 11;
    desc.target_epoch = 12;
    appendV1Capabilities(desc, rm);
    desc.capabilities.push_back(kUbCapabilityBootstrapV2);
    ub::UbRemoteTargetProfile profile;
    profile.trans_mode = kUrmaTransportModeRm;
    profile.target_type = kUrmaTargetTypeJetty;
    profile.tp_type = kUrmaTpTypeCtp;
    desc.target_profiles = {profile};
    ub::UbProbeAreaDescriptor area;
    area.descriptor = makeDescriptor(0x900000, 4096);
    area.address = 0x900000;
    area.length = 4096;
    area.token = 0xACFE;
    area.area_epoch = 5;
    area.uasid = 501;
    area.jetty_id = 402;
    desc.probe_areas = {area};
    return desc;
}

TEST(UbBootstrapV2Test, V1EnvelopeContainsNoV2Key) {
    UbBootstrapDesc desc;
    desc.segment_name = "peer";
    appendV1Capabilities(desc, /*rm=*/false);
    const nlohmann::json wire = desc;
    EXPECT_FALSE(wire.contains("schema_version"));
    EXPECT_FALSE(wire.contains("target_profiles"));
    EXPECT_FALSE(wire.contains("probe_areas"));
    EXPECT_FALSE(wire.contains("topology_epoch"));
    EXPECT_FALSE(wire.contains("target_epoch"));
    EXPECT_EQ(wire.at("protocol_version").get<uint32_t>(), 1U);
}

TEST(UbBootstrapV2Test, V2EnvelopeRoundTripsEveryAddedField) {
    const UbBootstrapDesc source = makeV2PeerDescriptor(/*rm=*/true);
    const nlohmann::json wire = source;
    ASSERT_TRUE(wire.contains("schema_version"));
    ASSERT_TRUE(wire.contains("target_profiles"));
    ASSERT_TRUE(wire.contains("probe_areas"));
    EXPECT_EQ(wire.at("protocol_version").get<uint32_t>(), 1U);

    UbBootstrapDesc parsed;
    ASSERT_NO_THROW(parsed = wire.get<UbBootstrapDesc>());
    EXPECT_EQ(parsed.schema_version, kUbBootstrapSchemaV2);
    EXPECT_EQ(parsed.topology_epoch, 11U);
    EXPECT_EQ(parsed.target_epoch, 12U);
    ASSERT_EQ(parsed.target_profiles.size(), 1U);
    EXPECT_EQ(parsed.target_profiles[0].trans_mode, kUrmaTransportModeRm);
    EXPECT_EQ(parsed.target_profiles[0].target_type, kUrmaTargetTypeJetty);
    EXPECT_EQ(parsed.target_profiles[0].tp_type, kUrmaTpTypeCtp);
    ASSERT_EQ(parsed.probe_areas.size(), 1U);
    EXPECT_EQ(parsed.probe_areas[0].address, 0x900000U);
    EXPECT_EQ(parsed.probe_areas[0].length, 4096U);
    EXPECT_EQ(parsed.probe_areas[0].token, 0xACFEU);
    EXPECT_EQ(parsed.probe_areas[0].area_epoch, 5U);
    EXPECT_EQ(parsed.probe_areas[0].jetty_id, 402U);
    EXPECT_EQ(parsed.probe_areas[0].uasid, 501U);
    EXPECT_EQ(parsed.probe_areas[0].descriptor.hex,
              source.probe_areas[0].descriptor.hex);
    EXPECT_TRUE(parsed.advertisesBootstrapV2());
}

TEST(UbBootstrapV2Test, UnknownFieldsAreIgnoredButBadTypesAreRejected) {
    nlohmann::json wire = makeV2PeerDescriptor(/*rm=*/false);
    wire["future_field"] = nlohmann::json{{"anything", 1}};
    EXPECT_NO_THROW((void)wire.get<UbBootstrapDesc>());

    // Malformed types are a contract violation, not an unknown extension.
    nlohmann::json wrong_epoch = makeV2PeerDescriptor(false);
    wrong_epoch["topology_epoch"] = "11";
    EXPECT_THROW((void)wrong_epoch.get<UbBootstrapDesc>(),
                 std::invalid_argument);

    nlohmann::json wrong_schema = makeV2PeerDescriptor(false);
    wrong_schema["schema_version"] = "2";
    EXPECT_THROW((void)wrong_schema.get<UbBootstrapDesc>(),
                 std::invalid_argument);

    nlohmann::json wrong_profiles = makeV2PeerDescriptor(false);
    wrong_profiles["target_profiles"] = "not-an-array";
    EXPECT_THROW((void)wrong_profiles.get<UbBootstrapDesc>(),
                 std::invalid_argument);

    nlohmann::json negative_profile = makeV2PeerDescriptor(false);
    negative_profile["target_profiles"][0]["trans_mode"] = -1;
    EXPECT_THROW((void)negative_profile.get<UbBootstrapDesc>(),
                 std::invalid_argument);

    nlohmann::json bad_area = makeV2PeerDescriptor(false);
    bad_area["probe_areas"][0]["length"] = 0;
    EXPECT_THROW((void)bad_area.get<UbBootstrapDesc>(), std::invalid_argument);
}

TEST(UbBootstrapV2Test, ProfileListMustAlignWithJettyIds) {
    nlohmann::json wire = makeV2PeerDescriptor(/*rm=*/false);
    wire["jetty_ids"] = {401, 402};
    wire["jetty_uasids"] = {501, 502};
    // One profile for two Jetties is rejected (IN-5).
    EXPECT_THROW((void)wire.get<UbBootstrapDesc>(), std::invalid_argument);

    // A missing profile list is the documented v1 fallback and is accepted.
    nlohmann::json without_profiles = makeV2PeerDescriptor(false);
    without_profiles["target_profiles"] = nlohmann::json::array();
    EXPECT_NO_THROW((void)without_profiles.get<UbBootstrapDesc>());
}

TEST(UbBootstrapV2Test, CapabilityStringDecidesTheFieldBlock) {
    UbBootstrapDesc desc;
    appendV1Capabilities(desc, false);
    EXPECT_FALSE(desc.advertisesBootstrapV2());
    desc.capabilities.push_back(kUbCapabilityBootstrapV2);
    EXPECT_TRUE(desc.advertisesBootstrapV2());
    EXPECT_TRUE(ubBootstrapV2Capable(desc.capabilities));
    EXPECT_FALSE(hasUbCapability(desc.capabilities, "bootstrap_v3"));
}

// ---------------------------------------------------------------------------
// Endpoint wiring: the local switch and the peer capability both gate the field
// block, and the negotiated target profile reaches rmTarget().
// ---------------------------------------------------------------------------

class EndpointFixture {
   public:
    explicit EndpointFixture(JettyTransportMode mode) {
        adapter_ = std::make_shared<FakeUrmaAdapter>(fakeDevice());
        (void)adapter_->initialize();
        context_ = std::make_shared<UbContext>(0, fakeDevice(), adapter_);
        (void)context_->initialize(1, JfcOptions{});
        JettyOptions options;
        options.transport_mode = mode;
        store_ = std::make_unique<EndpointStore>(adapter_, 4, 1, options);
    }

    ~EndpointFixture() {
        if (store_) (void)store_->clear();
        store_.reset();
        if (context_) (void)context_->shutdown();
        context_.reset();
        (void)adapter_->shutdown();
    }

    std::shared_ptr<UbEndpoint> createEndpoint() {
        UbEndpointKey key{0, 42, 0, "peer"};
        std::shared_ptr<UbEndpoint> endpoint;
        EXPECT_TRUE(store_->getOrCreate(key, context_, endpoint).ok());
        return endpoint;
    }

    std::shared_ptr<FakeUrmaAdapter> adapter() const { return adapter_; }

   private:
    std::shared_ptr<FakeUrmaAdapter> adapter_;
    std::shared_ptr<UbContext> context_;
    std::unique_ptr<EndpointStore> store_;
};

TEST(UbBootstrapV2Test, RmEndpointCarriesTheNegotiatedTargetProfile) {
    EndpointFixture fixture(JettyTransportMode::kRm);
    auto endpoint = fixture.createEndpoint();
    ASSERT_NE(endpoint, nullptr);
    const UbBootstrapDesc peer = makeV2PeerDescriptor(/*rm=*/true);
    ASSERT_TRUE(endpoint->bind(peer).ok());

    const RemoteJettyInfo target = endpoint->rmTarget(0);
    EXPECT_EQ(target.eid, peer.local_eid);
    EXPECT_EQ(target.id, 401U);
    EXPECT_EQ(target.uasid, 501U);
    EXPECT_EQ(target.trans_mode, kUrmaTransportModeRm);
    EXPECT_EQ(target.target_type, kUrmaTargetTypeJetty);
    EXPECT_EQ(target.tp_type, kUrmaTpTypeCtp);
}

TEST(UbBootstrapV2Test, V1PeerKeepsTheV1TargetPath) {
    EndpointFixture fixture(JettyTransportMode::kRm);
    auto endpoint = fixture.createEndpoint();
    UbBootstrapDesc peer = makeV2PeerDescriptor(/*rm=*/true);
    peer.schema_version = kUbBootstrapSchemaV1;
    peer.target_profiles.clear();
    peer.probe_areas.clear();
    peer.capabilities = {"read", "write", "endpoint_generation",
                         kUbCapabilityRmTransport};
    ASSERT_TRUE(endpoint->bind(peer).ok());
    const RemoteJettyInfo target = endpoint->rmTarget(0);
    EXPECT_EQ(target.eid, peer.local_eid);
    EXPECT_EQ(target.trans_mode, 0U);
    EXPECT_EQ(target.target_type, 0U);
    EXPECT_EQ(target.tp_type, 0U);
}

// A re-bootstrap may upgrade a v1 peer descriptor to v2 (or refresh its
// epochs). That is not a peer-generation conflict: the native identity is
// unchanged and only the advisory v2 block is replaced.
// Major-1: protocol=v1 must ignore a received v2 block, not just stop sending
// one. The profiles/pages are dropped before validation and storage.
TEST(UbBootstrapV2Test, V1ProtocolIgnoresReceivedV2Block) {
    EndpointFixture fixture(JettyTransportMode::kRm);
    auto endpoint = fixture.createEndpoint();
    endpoint->setBootstrapV2Allowed(false);
    const UbBootstrapDesc v2 = makeV2PeerDescriptor(/*rm=*/true);
    ASSERT_TRUE(endpoint->bind(v2).ok());
    EXPECT_EQ(endpoint->bootstrapV2Ignored(), 1U);
    // The negotiated profile did not leak through the rollback switch.
    EXPECT_EQ(endpoint->rmTarget(0).trans_mode, 0U);
    EXPECT_EQ(endpoint->rmTarget(0).target_type, 0U);
    UbBootstrapDesc response;
    ASSERT_TRUE(
        endpoint->makeBootstrapDesc("local", "/local", "/peer", 1, response)
            .ok());
    EXPECT_EQ(response.schema_version, kUbBootstrapSchemaV1);
    EXPECT_TRUE(response.target_profiles.empty());
    EXPECT_TRUE(response.probe_areas.empty());
}

TEST(UbBootstrapV2Test, IdempotentRebindRefreshesTheV2Metadata) {
    EndpointFixture fixture(JettyTransportMode::kRm);
    auto endpoint = fixture.createEndpoint();

    UbBootstrapDesc v1 = makeV2PeerDescriptor(/*rm=*/true);
    v1.schema_version = kUbBootstrapSchemaV1;
    v1.target_profiles.clear();
    v1.probe_areas.clear();
    v1.capabilities = {"read", "write", "endpoint_generation",
                       kUbCapabilityRmTransport};
    ASSERT_TRUE(endpoint->bind(v1).ok());
    EXPECT_EQ(endpoint->rmTarget(0).trans_mode, 0U);

    const UbBootstrapDesc v2 = makeV2PeerDescriptor(/*rm=*/true);
    ASSERT_TRUE(endpoint->bind(v2).ok());
    EXPECT_EQ(endpoint->rmTarget(0).trans_mode, kUrmaTransportModeRm);
    EXPECT_EQ(endpoint->rmTarget(0).target_type, kUrmaTargetTypeJetty);
}

TEST(UbBootstrapV2Test, MisalignedProfileIsRejectedAtBind) {
    EndpointFixture fixture(JettyTransportMode::kRm);
    auto endpoint = fixture.createEndpoint();
    UbBootstrapDesc peer = makeV2PeerDescriptor(/*rm=*/true);
    peer.jetty_ids = {401, 402};
    peer.jetty_uasids = {501, 502};
    // Programmatic descriptors bypass JSON validation, so bind must re-check.
    EXPECT_FALSE(endpoint->bind(peer).ok());
}

TEST(UbBootstrapV2Test, BootstrapResponseSendsV2OnlyWhenBothSidesAllowIt) {
    EndpointFixture fixture(JettyTransportMode::kRm);
    auto endpoint = fixture.createEndpoint();
    endpoint->setBootstrapV2Allowed(true);
    // Before the peer is known (request phase) only capabilities are emitted.
    UbBootstrapDesc request;
    ASSERT_TRUE(
        endpoint->makeBootstrapDesc("local", "/local", "/peer", 1, request)
            .ok());
    EXPECT_EQ(request.schema_version, kUbBootstrapSchemaV1);
    EXPECT_TRUE(
        hasUbCapability(request.capabilities, kUbCapabilityBootstrapV2));
    EXPECT_TRUE(request.target_profiles.empty());
    // One-way boundary (W3g §17.3): the first request cannot publish our own
    // probe pages because the peer capability is still unknown (D-6: never send
    // v2 fields to an unverified peer). A reverse probe edge therefore appears
    // only once the other side initiates its own bootstrap.
    EXPECT_TRUE(request.probe_areas.empty());

    // After binding a v2 peer the response carries the field block.
    const UbBootstrapDesc peer = makeV2PeerDescriptor(/*rm=*/true);
    ASSERT_TRUE(endpoint->bind(peer).ok());
    UbBootstrapDesc response;
    ASSERT_TRUE(
        endpoint->makeBootstrapDesc("local", "/local", "/peer", 1, response)
            .ok());
    EXPECT_EQ(response.schema_version, kUbBootstrapSchemaV2);
    EXPECT_EQ(response.target_profiles.size(), response.jetty_ids.size());
    EXPECT_NE(response.topology_epoch, 0U);  // context incarnation
    EXPECT_NE(response.target_epoch, 0U);    // endpoint generation

    // Local protocol=v1 suppresses the fields even for a capable peer.
    endpoint->setBootstrapV2Allowed(false);
    UbBootstrapDesc v1_response;
    ASSERT_TRUE(
        endpoint->makeBootstrapDesc("local", "/local", "/peer", 1, v1_response)
            .ok());
    EXPECT_EQ(v1_response.schema_version, kUbBootstrapSchemaV1);
    EXPECT_TRUE(v1_response.target_profiles.empty());
}

}  // namespace
}  // namespace mooncake::tent
