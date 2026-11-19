// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// ProbeBootstrapUb wire codec and validation (design §7.2/§7.3).

#include <gtest/gtest.h>

#include <string>

#include "tent/transport/ub/probe_bootstrap.h"

namespace mooncake::tent::ub {
namespace {

SegmentDescriptor descriptor() {
    SegmentDescriptor value;
    value.schema_version = 1;
    value.urma_api_version = 1;
    value.urma_abi_size = 40;
    value.hex = "00112233445566778899aabbccddeeff";
    return value;
}

ProbeBootstrapPort makePort(const std::string& id, const std::string& eid) {
    ProbeBootstrapPort port;
    port.port_id = id;
    port.topology_name = "ub:udma0:eid0";
    port.eid = eid;
    port.target_epoch = 3;
    port.jetty = RemoteJettyRef{eid, 4097, 0, 0xACFE};
    port.area_descriptor = descriptor();
    port.area_length = 4096;
    port.area_epoch = 1;
    port.area_token = 0xACFE;
    port.node_gate_ok = true;
    port.port_gate_ok = true;
    port.profile_gate_ok = true;
    return port;
}

constexpr const char* kEidA = "0000:0000:0000:0000:0000:0000:0000:0011";
constexpr const char* kEidB = "0000:0000:0000:0000:0000:0000:0000:0012";

ProbeBootstrapRequest makeRequest() {
    ProbeBootstrapRequest request;
    request.requester_segment = "node-a";
    request.requester_node_incarnation = 1234;
    request.requester_topology_epoch = 5678;
    request.requester_publication_epoch = 7;
    request.declared_local_ports = {"P0"};
    request.ports = {makePort("P0", kEidA)};
    request.declared_peers = {{"node-b", {{"R0", kEidB}}}};
    return request;
}

ProbeBootstrapResponse makeResponse() {
    ProbeBootstrapResponse response;
    response.responder_segment = "node-b";
    response.node_incarnation = 4321;
    response.topology_epoch = 8765;
    response.publication_epoch = 11;
    response.ports = {makePort("R0", kEidB)};
    return response;
}

TEST(UbProbeBootstrapTest, RequestRoundTripsAndValidates) {
    const auto request = makeRequest();
    ASSERT_TRUE(validateProbeBootstrapRequest(request).ok());

    std::string encoded;
    ASSERT_TRUE(encodeProbeBootstrapRequest(request, encoded).ok());
    ProbeBootstrapRequest decoded;
    ASSERT_TRUE(decodeProbeBootstrapRequest(encoded, decoded).ok());
    EXPECT_EQ(decoded.schema_version, request.schema_version);
    EXPECT_EQ(decoded.requester_segment, request.requester_segment);
    EXPECT_EQ(decoded.requester_node_incarnation,
              request.requester_node_incarnation);
    EXPECT_EQ(decoded.requester_topology_epoch,
              request.requester_topology_epoch);
    EXPECT_EQ(decoded.requester_publication_epoch,
              request.requester_publication_epoch);
    ASSERT_EQ(decoded.declared_local_ports.size(), 1U);
    EXPECT_EQ(decoded.declared_local_ports[0], "P0");
    ASSERT_EQ(decoded.ports.size(), 1U);
    EXPECT_EQ(decoded.ports[0].port_id, "P0");
    EXPECT_EQ(decoded.ports[0].eid, kEidA);
    EXPECT_EQ(decoded.ports[0].target_epoch, 3U);
    EXPECT_EQ(decoded.ports[0].jetty.id, 4097U);
    EXPECT_EQ(decoded.ports[0].area_length, 4096U);
    EXPECT_EQ(decoded.ports[0].area_descriptor.hex, descriptor().hex);
    ASSERT_EQ(decoded.declared_peers.count("node-b"), 1U);
    EXPECT_EQ(decoded.declared_peers.at("node-b")[0].eid, kEidB);
    EXPECT_TRUE(validateProbeBootstrapRequest(decoded).ok());
}

TEST(UbProbeBootstrapTest, ResponseRoundTripsAndValidates) {
    const auto response = makeResponse();
    ASSERT_TRUE(validateProbeBootstrapResponse(response).ok());

    std::string encoded;
    ASSERT_TRUE(encodeProbeBootstrapResponse(response, encoded).ok());
    ProbeBootstrapResponse decoded;
    ASSERT_TRUE(decodeProbeBootstrapResponse(encoded, decoded).ok());
    EXPECT_EQ(decoded.responder_segment, "node-b");
    EXPECT_EQ(decoded.node_incarnation, 4321U);
    EXPECT_EQ(decoded.topology_epoch, 8765U);
    EXPECT_EQ(decoded.publication_epoch, 11U);
    EXPECT_EQ(decoded.transport_mode, "rm");
    EXPECT_EQ(decoded.device_kind, "physical");
    ASSERT_EQ(decoded.ports.size(), 1U);
    EXPECT_EQ(decoded.ports[0].eid, kEidB);
    EXPECT_TRUE(validateProbeBootstrapResponse(decoded).ok());
}

TEST(UbProbeBootstrapTest, RejectsInvalidPortFields) {
    auto request = makeRequest();
    request.ports[0].eid = "not-an-eid";
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.ports[0].target_epoch = 0;
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.ports[0].jetty.id = 0;
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.ports[0].area_token = 0;
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.ports[0].area_length = 0;
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.ports[0].area_epoch = 0;
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.ports[0].area_descriptor.hex.clear();
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());
}

TEST(UbProbeBootstrapTest, RejectsDuplicateIdentityAndBadEpochs) {
    auto request = makeRequest();
    request.ports.push_back(makePort("P0", kEidB));
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.ports.push_back(makePort("P1", kEidA));
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.requester_node_incarnation = 0;
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.schema_version = 2;
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());

    request = makeRequest();
    request.ports.clear();
    EXPECT_FALSE(validateProbeBootstrapRequest(request).ok());
}

TEST(UbProbeBootstrapTest, ResponseRejectsReplyModeAndKind) {
    auto response = makeResponse();
    response.reply_msg = "probe not ready";
    EXPECT_TRUE(validateProbeBootstrapResponse(response).IsRpcServiceError());

    response = makeResponse();
    response.transport_mode = "rc";
    EXPECT_FALSE(validateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.device_kind = "bonding";
    EXPECT_FALSE(validateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.publication_epoch = 0;
    EXPECT_FALSE(validateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.ports.clear();
    EXPECT_FALSE(validateProbeBootstrapResponse(response).ok());
}

TEST(UbProbeBootstrapTest, DecodesMalformedJsonWithoutThrowing) {
    ProbeBootstrapRequest request;
    EXPECT_TRUE(
        decodeProbeBootstrapRequest("{not json", request).IsMalformedJson());

    ProbeBootstrapResponse response;
    EXPECT_FALSE(decodeProbeBootstrapResponse("[]", response).ok());
    EXPECT_TRUE(
        decodeProbeBootstrapResponse("{bad", response).IsMalformedJson());
    EXPECT_TRUE(decodeProbeBootstrapResponse("{}", response).ok());
    // A decoded empty response fails the semantic validator, not the codec.
    EXPECT_FALSE(validateProbeBootstrapResponse(response).ok());
}

}  // namespace
}  // namespace mooncake::tent::ub
