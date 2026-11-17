// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "tent/runtime/probe_bootstrap.h"

namespace mooncake {
namespace tent {
namespace {

ProbeBootstrapResponse makeResponse() {
    ProbeBootstrapResponse response;
    response.node_id = "node-a";
    response.node_epoch = 7;
    response.rpc_server_addr = "127.0.0.1:19090";
    response.topology_epoch = 1;
    ProbeEndpointDesc endpoint;
    endpoint.key = ProbeEndpointKey{"node-a", "udmac0d1e2", 1};
    endpoint.topology_name = "ub:udmac0d1e2:eid1";
    endpoint.eid = "0000:0000:0001:0200:0010:0000:df00:c200";
    endpoint.local_nic = 3;
    endpoint.area.schema_version = kProbeAreaSchemaVersion;
    endpoint.area.urma_api_version = 1;
    endpoint.area.urma_abi_size = 16;
    endpoint.area.hex = "00112233";
    endpoint.area_length = 1;
    endpoint.area_token = 0xACFE;
    endpoint.area_epoch = 2;
    endpoint.probe_jetty = ProbeRemoteJetty{endpoint.eid, 11, 3, 0xACFE};
    endpoint.target_epoch = 4;
    endpoint.endpoint_generation = 4;
    response.endpoints.push_back(endpoint);
    return response;
}

TEST(UbProbeBootstrapTest, RoundTripsWireResponse) {
    const auto response = makeResponse();
    nlohmann::json j = response;
    const auto decoded = j.get<ProbeBootstrapResponse>();
    EXPECT_EQ(decoded.node_id, response.node_id);
    EXPECT_EQ(decoded.node_epoch, response.node_epoch);
    ASSERT_EQ(decoded.endpoints.size(), 1U);
    EXPECT_EQ(decoded.endpoints[0].key, response.endpoints[0].key);
    EXPECT_EQ(decoded.endpoints[0].area.hex, "00112233");
    EXPECT_EQ(decoded.endpoints[0].probe_jetty.id, 11U);
    ASSERT_TRUE(ValidateProbeBootstrapResponse(decoded).ok());
}

TEST(UbProbeBootstrapTest, RejectsMalformedResponses) {
    auto response = makeResponse();
    response.reply_msg = "not available";
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.schema_version = 2;
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.node_epoch = 0;
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.endpoints[0].area_length = 0;
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.endpoints[0].area.schema_version = 99;
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.endpoints[0].target_epoch = 0;
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.topology_epoch = 0;
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.endpoints[0].probe_jetty.token = 0;
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.endpoints[0].area.hex.assign(kProbeMaxAreaHexBytes + 1, 'x');
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());

    response = makeResponse();
    response.endpoints.push_back(response.endpoints[0]);
    EXPECT_FALSE(ValidateProbeBootstrapResponse(response).ok());
}

TEST(UbProbeBootstrapTest, RoundTripsRequest) {
    ProbeBootstrapRequest request;
    request.requester_node = "node-b";
    request.known_node_epoch = 9;
    nlohmann::json j = request;
    const auto decoded = j.get<ProbeBootstrapRequest>();
    EXPECT_EQ(decoded.requester_node, "node-b");
    EXPECT_EQ(decoded.known_node_epoch, 9U);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
