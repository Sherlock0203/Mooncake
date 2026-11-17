// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Route B peer source: fetch per-node probe endpoint descriptors through the
// TENT control plane (GetProbeBootstrap) and pair them by native device/EID.

#ifndef TENT_TRANSPORT_UB_PROBE_CONTROL_PLANE_H_
#define TENT_TRANSPORT_UB_PROBE_CONTROL_PLANE_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tent/runtime/probe_bootstrap.h"
#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "tent/transport/ub/probe_types.h"

namespace mooncake {
namespace tent {
class SegmentManager;
namespace ub {

// Injectable control-plane RPC seam (fake in unit tests).
class ProbeBootstrapClient {
   public:
    virtual ~ProbeBootstrapClient() = default;
    virtual Status get(const std::string& server_addr,
                       const ProbeBootstrapRequest& request,
                       ProbeBootstrapResponse& response,
                       uint64_t timeout_ms) = 0;
};

// Injectable peer address/segment-handle resolver.
class PeerAddressResolver {
   public:
    virtual ~PeerAddressResolver() = default;
    virtual Status resolve(const std::string& peer_segment,
                           std::string& rpc_server_addr,
                           SegmentID& segment_id) = 0;
    virtual void release(SegmentID) {}
};

// Injectable local endpoint source.
class LocalEndpointSource {
   public:
    virtual ~LocalEndpointSource() = default;
    virtual Status snapshot(ProbeBootstrapResponse& local) = 0;
};

class ControlClientProbeBootstrapClient final : public ProbeBootstrapClient {
   public:
    Status get(const std::string& server_addr,
               const ProbeBootstrapRequest& request,
               ProbeBootstrapResponse& response, uint64_t timeout_ms) override;
};

class SegmentManagerAddressResolver final : public PeerAddressResolver {
   public:
    explicit SegmentManagerAddressResolver(SegmentManager* manager)
        : manager_(manager) {}

    Status resolve(const std::string& peer_segment,
                   std::string& rpc_server_addr,
                   SegmentID& segment_id) override;
    void release(SegmentID segment_id) override;

   private:
    SegmentManager* manager_{nullptr};
};

class ProviderLocalEndpointSource final : public LocalEndpointSource {
   public:
    explicit ProviderLocalEndpointSource(
        StandaloneProbeResourceProvider* provider)
        : provider_(provider) {}

    Status snapshot(ProbeBootstrapResponse& local) override;

   private:
    StandaloneProbeResourceProvider* provider_{nullptr};
};

// Peer segment list for Route B. Route C replaces this with explicit links.
struct ProbeLinkGraph {
    std::vector<std::string> peer_segments;
};

class ControlPlaneProbePeerSource final : public ProbePeerDescriptorSource {
   public:
    ControlPlaneProbePeerSource(std::unique_ptr<ProbeBootstrapClient> client,
                                std::unique_ptr<PeerAddressResolver> resolver,
                                std::unique_ptr<LocalEndpointSource> local,
                                ProbeLinkGraph graph, ProbeParams params);
    ~ControlPlaneProbePeerSource() override;

    ControlPlaneProbePeerSource(const ControlPlaneProbePeerSource&) = delete;
    ControlPlaneProbePeerSource& operator=(const ControlPlaneProbePeerSource&) =
        delete;

    Status snapshot(std::vector<ProbePeerDescriptor>& descriptors) override;

    [[nodiscard]] uint64_t rpcFailureCount() const {
        return rpc_failures_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t malformedCount() const {
        return malformed_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t noEndpointCount() const {
        return no_endpoint_.load(std::memory_order_relaxed);
    }

   private:
    struct PeerState {
        std::string node_id;
        uint64_t node_epoch{0};
        uint64_t topology_epoch{0};
        std::string rpc_server_addr;
        SegmentID segment_id{LOCAL_SEGMENT_ID};
        std::vector<ProbeEndpointDesc> endpoints;
        uint64_t fetched_ns{0};
        bool valid{false};
    };

    Status refreshPeer(const std::string& peer, PeerState& state,
                       uint64_t now_ns, bool wait_initial);
    Status buildDescriptors(const ProbeBootstrapResponse& local,
                            std::vector<ProbePeerDescriptor>& descriptors);

    std::mutex mutex_;
    std::unique_ptr<ProbeBootstrapClient> client_;
    std::unique_ptr<PeerAddressResolver> resolver_;
    std::unique_ptr<LocalEndpointSource> local_;
    const ProbeLinkGraph graph_;
    const ProbeParams params_;
    std::map<std::string, PeerState> peers_;
    std::string local_node_id_;
    std::atomic<uint64_t> rpc_failures_{0};
    std::atomic<uint64_t> malformed_{0};
    std::atomic<uint64_t> no_endpoint_{0};
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_CONTROL_PLANE_H_
