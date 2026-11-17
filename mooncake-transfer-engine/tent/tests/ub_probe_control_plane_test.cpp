// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "tent/transport/ub/probe_control_plane.h"
#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "ub_fake_urma.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace fake;

ProbeEndpointDesc makeEndpoint(const std::string& node,
                               const std::string& native, uint32_t eid_index,
                               int local_nic) {
    ProbeEndpointDesc endpoint;
    endpoint.key = ProbeEndpointKey{node, native, eid_index};
    endpoint.topology_name =
        "ub:" + native + ":eid" + std::to_string(eid_index);
    endpoint.eid = "eid-" + node + "-" + std::to_string(eid_index);
    endpoint.local_nic = local_nic;
    endpoint.area.schema_version = kProbeAreaSchemaVersion;
    endpoint.area.urma_api_version = 1;
    endpoint.area.urma_abi_size = 16;
    endpoint.area.hex =
        "area-" + node + "-" + native + "-" + std::to_string(eid_index);
    endpoint.area_length = 1;
    endpoint.area_token = 0xACFE;
    endpoint.area_epoch = 1;
    endpoint.probe_jetty =
        ProbeRemoteJetty{endpoint.eid, 10 + eid_index, 1, 0xACFE};
    endpoint.target_epoch = 1;
    endpoint.endpoint_generation = 1;
    return endpoint;
}

ProbeBootstrapResponse makeNode(const std::string& node, uint64_t epoch,
                                const std::vector<std::string>& natives) {
    ProbeBootstrapResponse response;
    response.node_id = node;
    response.node_epoch = epoch;
    response.rpc_server_addr = "rpc-" + node;
    response.topology_epoch = 1;
    for (size_t i = 0; i < natives.size(); ++i) {
        response.endpoints.push_back(makeEndpoint(
            node, natives[i], static_cast<uint32_t>(i), static_cast<int>(i)));
    }
    return response;
}

class FakeClient final : public ProbeBootstrapClient {
   public:
    std::map<std::string, ProbeBootstrapResponse> responses;
    int calls{0};

    Status get(const std::string& server_addr,
               const ProbeBootstrapRequest& request,
               ProbeBootstrapResponse& response, uint64_t) override {
        ++calls;
        last_request = request;
        auto it = responses.find(server_addr);
        if (it == responses.end()) {
            return Status::RpcConnectionError("no fake response");
        }
        response = it->second;
        return ValidateProbeBootstrapResponse(response);
    }

    ProbeBootstrapRequest last_request;
};

class FakeResolver final : public PeerAddressResolver {
   public:
    Status resolve(const std::string& peer_segment,
                   std::string& rpc_server_addr,
                   SegmentID& segment_id) override {
        rpc_server_addr = "rpc-" + peer_segment;
        segment_id = static_cast<SegmentID>(++next_id_);
        resolved.push_back(peer_segment);
        return Status::OK();
    }

    int next_id_{0};
    std::vector<std::string> resolved;
};

class FakeLocal final : public LocalEndpointSource {
   public:
    explicit FakeLocal(ProbeBootstrapResponse response)
        : response_(std::move(response)) {}

    Status snapshot(ProbeBootstrapResponse& local) override {
        local = response_;
        return Status::OK();
    }

   private:
    ProbeBootstrapResponse response_;
};

ProbeParams makeParams(std::vector<std::string> peers) {
    ProbeParams params;
    params.peer_segments = std::move(peers);
    params.peer_wait_ms = 100;
    params.peer_rpc_timeout_ms = 100;
    params.descriptor_ttl_ms = 60000;
    params.descriptor_refresh_ms = 60000;
    return params;
}

TEST(UbProbeControlPlaneSourceTest, PairsExactEidPerPeer) {
    auto client = std::make_unique<FakeClient>();
    auto* client_ptr = client.get();
    client_ptr->responses["rpc-b"] = makeNode("b", 1, {"udmac0d1e2"});
    client_ptr->responses["rpc-c"] = makeNode("c", 1, {"udmac0d1e2"});
    auto resolver = std::make_unique<FakeResolver>();
    auto local = std::make_unique<FakeLocal>(makeNode("a", 1, {"udmac0d1e2"}));

    ControlPlaneProbePeerSource source(
        std::move(client), std::move(resolver), std::move(local),
        ProbeLinkGraph{{"b", "c"}}, makeParams({"b", "c"}));

    std::vector<ProbePeerDescriptor> descriptors;
    ASSERT_TRUE(source.snapshot(descriptors).ok());
    ASSERT_EQ(descriptors.size(), 2U);
    std::set<SegmentID> peers;
    for (const auto& descriptor : descriptors) {
        peers.insert(descriptor.peer_segment_id);
        EXPECT_EQ(descriptor.local_nic, 0);
        EXPECT_EQ(descriptor.remote_nic, 0);
        EXPECT_EQ(
            descriptor.area_descriptor.hex,
            "area-" + std::string(descriptor.peer_nic_path) + "-udmac0d1e2-0");
        EXPECT_EQ(descriptor.remote_jetty.id, 10U);
    }
    EXPECT_EQ(peers.size(), 2U);
    EXPECT_EQ(client_ptr->calls, 2);
}

TEST(UbProbeControlPlaneSourceTest, FallsBackToNativeOnlyWhenUnambiguous) {
    auto client = std::make_unique<FakeClient>();
    auto* client_ptr = client.get();
    client_ptr->responses["rpc-b"] = makeNode("b", 1, {"udmac0d1e2"});
    auto local = std::make_unique<FakeLocal>(makeNode("a", 1, {"udmac0d1e2"}));

    ControlPlaneProbePeerSource source(
        std::move(client), std::make_unique<FakeResolver>(), std::move(local),
        ProbeLinkGraph{{"b"}}, makeParams({"b"}));

    std::vector<ProbePeerDescriptor> descriptors;
    ASSERT_TRUE(source.snapshot(descriptors).ok());
    // The local node exposes eid0 and eid1, but the peer exposes only eid0;
    // exact matching must not synthesize an eid1 edge.
    ASSERT_EQ(descriptors.size(), 1U);
    EXPECT_EQ(descriptors[0].area_descriptor.hex, "area-b-udmac0d1e2-0");
}

TEST(UbProbeControlPlaneSourceTest, ReportsRpcFailureWithoutCache) {
    auto client = std::make_unique<FakeClient>();
    auto local = std::make_unique<FakeLocal>(makeNode("a", 1, {"udmac0d1e2"}));
    ProbeParams params = makeParams({"b"});
    params.peer_wait_ms = 1;
    ControlPlaneProbePeerSource source(
        std::move(client), std::make_unique<FakeResolver>(), std::move(local),
        ProbeLinkGraph{{"b"}}, params);

    std::vector<ProbePeerDescriptor> descriptors;
    auto status = source.snapshot(descriptors);
    EXPECT_FALSE(status.ok());
    EXPECT_GT(source.rpcFailureCount(), 0U);
}

TEST(UbProbeControlPlaneSourceTest, RejectsUnchangedResponse) {
    auto client = std::make_unique<FakeClient>();
    auto* client_ptr = client.get();
    auto peer = makeNode("b", 1, {"udmac0d1e2"});
    peer.unchanged = true;
    client_ptr->responses["rpc-b"] = std::move(peer);
    auto local = std::make_unique<FakeLocal>(makeNode("a", 1, {"udmac0d1e2"}));
    ProbeParams params = makeParams({"b"});
    params.peer_wait_ms = 1;
    ControlPlaneProbePeerSource source(
        std::move(client), std::make_unique<FakeResolver>(), std::move(local),
        ProbeLinkGraph{{"b"}}, params);

    std::vector<ProbePeerDescriptor> descriptors;
    EXPECT_FALSE(source.snapshot(descriptors).ok());
}

TEST(UbProbeControlPlaneSourceTest, RejectsLocalOnlyPeerList) {
    auto local = std::make_unique<FakeLocal>(makeNode("a", 1, {"udmac0d1e2"}));
    ControlPlaneProbePeerSource source(
        std::make_unique<FakeClient>(), std::make_unique<FakeResolver>(),
        std::move(local), ProbeLinkGraph{{"a"}}, makeParams({"a"}));

    std::vector<ProbePeerDescriptor> descriptors;
    EXPECT_FALSE(source.snapshot(descriptors).ok());
}

class ProviderBackedLocal final : public LocalEndpointSource {
   public:
    explicit ProviderBackedLocal(StandaloneProbeResourceProvider* provider)
        : provider_(provider) {}

    Status snapshot(ProbeBootstrapResponse& local) override {
        return provider_->exportBootstrap(ProbeBootstrapRequest{}, local);
    }

   private:
    StandaloneProbeResourceProvider* provider_;
};

TEST(UbProbeControlPlaneSourceTest, ProviderSnapshotDoesNotDeadlock) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    ProbeParams params = makeParams({"b"});
    params.enabled = true;
    params.jfc_depth = 64;
    StandaloneProbeResourceProvider provider(
        adapter, params, {ProbeLocalPortConfig{0, context, nullptr, 1}},
        nullptr);
    provider.setNodeIdentity("a", 1);

    auto client = std::make_unique<FakeClient>();
    ProbeBootstrapResponse peer;
    peer.node_id = "b";
    peer.node_epoch = 1;
    peer.topology_epoch = 1;
    peer.rpc_server_addr = "rpc-b";
    ProbeEndpointDesc endpoint;
    endpoint.key = ProbeEndpointKey{"b", "fake0", 0};
    endpoint.topology_name = "ub:fake0:eid0";
    endpoint.eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    endpoint.local_nic = 0;
    auto descriptor = makeDescriptor(0xA00000, 4096);
    endpoint.area.schema_version = descriptor.schema_version;
    endpoint.area.urma_api_version = descriptor.urma_api_version;
    endpoint.area.urma_abi_size = descriptor.urma_abi_size;
    endpoint.area.hex = descriptor.hex;
    endpoint.area_length = 1;
    endpoint.area_token = 0xACFE;
    endpoint.area_epoch = 1;
    endpoint.probe_jetty = ProbeRemoteJetty{endpoint.eid, 401, 1, 0xACFE};
    endpoint.target_epoch = 1;
    peer.endpoints.push_back(endpoint);
    client->responses["rpc-b"] = std::move(peer);

    ControlPlaneProbePeerSource source(
        std::move(client), std::make_unique<FakeResolver>(),
        std::make_unique<ProviderBackedLocal>(&provider), ProbeLinkGraph{{"b"}},
        params);
    HandshakeProbePeerDirectory directory(adapter, provider.contexts(),
                                          &source);
    provider.setRemoteDirectory(&directory);

    ProbeGraph graph;
    // Before the P0 fix this deadlocks: provider snapshot -> directory ->
    // source local export -> provider mutex again.
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    ASSERT_EQ(graph.edges.size(), 1U);

    ASSERT_TRUE(provider.shutdown().ok());
    ASSERT_TRUE(directory.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
