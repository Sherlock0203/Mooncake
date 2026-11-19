// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// ProbePeerCoordinator (design §7.4/§8.1): schedules ProbeBootstrapUb
// exchanges, applies the validated peer publication to ProbePeerRegistry and
// asks the provider to refresh its edge set. It owns only control-plane
// threads; native imports/events run in the provider on the same thread, never
// on an RPC handler or a poller.

#ifndef TENT_TRANSPORT_UB_PROBE_PEER_COORDINATOR_H_
#define TENT_TRANSPORT_UB_PROBE_PEER_COORDINATOR_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/segment_manager.h"
#include "tent/transport/ub/control_executor.h"
#include "tent/transport/ub/probe_bootstrap.h"
#include "tent/transport/ub/probe_peer_registry.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "tent/transport/ub/probe_topology.h"

namespace mooncake {
namespace tent {
namespace ub {

struct ProbeCoordinatorStats {
    uint64_t attempts{0};
    uint64_t success{0};
    uint64_t failure{0};
    uint64_t timeout{0};
    uint64_t skipped_inflight{0};
    uint64_t peer_unresolved{0};
    uint64_t unsupported{0};
    uint64_t not_ready{0};
    uint64_t publication_apply{0};
    uint64_t publication_unchanged{0};
    uint64_t publication_stale{0};
    uint64_t publication_unknown_port{0};
    uint64_t publication_identity_mismatch{0};
    uint64_t peer_publication_stale{0};
    uint64_t graph_event_failed{0};
    uint64_t refresh_hint{0};
    uint64_t publication_bump{0};
};

class ProbePeerCoordinator {
   public:
    using Clock = std::function<uint64_t()>;
    // Blocking RPC seam. Tests inject a deterministic implementation; the
    // transport uses ControlClient::probeBootstrapUb. The coordinator only
    // ever schedules this off the caller's thread.
    using HandshakeRpc = std::function<Status(
        const std::string& server_addr, const ProbeBootstrapRequest& request,
        ProbeBootstrapResponse& response)>;

    struct Dependencies {
        SegmentManager* segment_manager{nullptr};
        ProbePeerRegistry* registry{nullptr};
        StandaloneProbeResourceProvider* provider{nullptr};
        const ProbeTopology* topology{nullptr};
        HandshakeRpc rpc;
    };

    ProbePeerCoordinator(ProbeParams params, Dependencies dependencies,
                         Clock clock = {});
    ~ProbePeerCoordinator();

    ProbePeerCoordinator(const ProbePeerCoordinator&) = delete;
    ProbePeerCoordinator& operator=(const ProbePeerCoordinator&) = delete;

    Status start();
    void stop();
    [[nodiscard]] bool running() const;

    // Coalesced trigger. `peer` must be declared; an unknown peer is ignored.
    void requestRefresh(const std::string& peer, const char* reason);
    void onLocalPublicationChanged(uint64_t publication_epoch);
    void noteBusinessPathError(uint64_t peer_id);

    [[nodiscard]] uint64_t localPublicationEpoch() const;
    [[nodiscard]] ProbeCoordinatorStats stats() const;
    [[nodiscard]] uint64_t nextAttemptNs(const std::string& peer) const;

    // Synchronous single attempt, exposed for unit/integration tests. Returns
    // OK when the publication was applied (or was unchanged), and a non-OK
    // status for a transport/validation failure.
    Status handshakeOnce(const std::string& peer, uint64_t now_ns = 0);

   private:
    struct PeerState {
        bool in_flight{false};
        bool refresh_queued{false};
        uint64_t next_attempt_ns{0};
        uint64_t backoff_ns{0};
        uint32_t consecutive_failures{0};
        uint64_t last_hint_ns{0};
    };

    uint64_t nowNs(uint64_t explicit_ns = 0) const;
    void processDue();
    void tickChain();
    void submitAttempt(const std::string& peer, uint64_t now_ns);
    Status runAttempt(const std::string& peer, uint64_t now_ns);
    Status buildRequest(ProbeBootstrapRequest& request) const;
    Status applyResponse(const std::string& peer, const SegmentDesc& segment,
                         const ProbeBootstrapResponse& response,
                         uint64_t now_ns);
    void noteAttemptFinishedLocked(const std::string& peer, uint64_t now_ns,
                                   bool success);
    void scheduleTick(uint64_t delay_ns);

    ProbeParams params_;
    Dependencies deps_;
    Clock clock_;
    mutable std::mutex mutex_;
    std::map<std::string, PeerState> peers_;
    std::vector<std::unique_ptr<ThreadedControlExecutor>> executors_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};
    size_t round_robin_{0};
    uint64_t last_local_publication_epoch_{0};
    ProbeCoordinatorStats stats_;
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_PEER_COORDINATOR_H_
