// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Value types for the TENT UB probe-link subsystem (W1a). The design is
// docs/analyze/tent-ub-probe-link-detail-design.md; section references below
// point into that document.

#ifndef TENT_TRANSPORT_UB_PROBE_TYPES_H_
#define TENT_TRANSPORT_UB_PROBE_TYPES_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "tent/common/types.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
namespace ub {

// ---------------------------------------------------------------------------
// Probe token namespace (design §5.3 / L-5).
//
// Completion records carry a single 64-bit user token. The high 16 bits name
// the kind of work and the low 48 bits are a per-kind sequence, so a poller can
// classify a completion before looking at any status or flag. Token 0 stays
// reserved for native marker records and never names a probe.
constexpr uint64_t kProbeTokenKindShift = 48;
constexpr uint64_t kProbeTokenKindBusiness = 0;
constexpr uint64_t kProbeTokenKindProbe = 1;
constexpr uint64_t kProbeTokenSeqMask = (1ULL << kProbeTokenKindShift) - 1;

constexpr uint64_t makeProbeToken(uint64_t seq) {
    return (kProbeTokenKindProbe << kProbeTokenKindShift) |
           (seq & kProbeTokenSeqMask);
}

constexpr bool isProbeToken(uint64_t token) {
    return (token >> kProbeTokenKindShift) == kProbeTokenKindProbe;
}

// Synthetic peer ids for the probe graph start above every business
// SegmentID allocation domain (design §3/§5.4).
inline constexpr uint64_t kProbePeerIdBase = 1ULL << 63;

// ---------------------------------------------------------------------------
// Identity (design §5.1).
//
// One probe link is one legal adjacency edge: (local port, peer segment,
// remote port). It is deliberately NOT the cartesian product of the ports.
struct ProbePathKey {
    Topology::NicID local_nic{-1};
    SegmentID segment_id{LOCAL_SEGMENT_ID};
    Topology::NicID remote_nic{-1};

    bool operator==(const ProbePathKey&) const = default;

    bool operator<(const ProbePathKey& other) const {
        return std::tie(local_nic, segment_id, remote_nic) <
               std::tie(other.local_nic, other.segment_id, other.remote_nic);
    }

    [[nodiscard]] bool valid() const {
        return local_nic >= 0 && remote_nic >= 0 &&
               segment_id != LOCAL_SEGMENT_ID;
    }
};

struct ProbePathKeyHash {
    size_t operator()(const ProbePathKey& key) const noexcept {
        size_t seed = std::hash<int>{}(key.local_nic);
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<SegmentID>{}(key.segment_id));
        combine(std::hash<int>{}(key.remote_nic));
        return seed;
    }
};

// A campaign is scoped to one peer. peer_nic_path is the provider's stable
// textual identity for the peer NIC and only participates in diagnostics and
// equality, never in path selection.
struct ProbePeerKey {
    SegmentID segment_id{LOCAL_SEGMENT_ID};
    std::string peer_nic_path;

    bool operator==(const ProbePeerKey&) const = default;

    bool operator<(const ProbePeerKey& other) const {
        return std::tie(segment_id, peer_nic_path) <
               std::tie(other.segment_id, other.peer_nic_path);
    }

    [[nodiscard]] bool valid() const { return segment_id != LOCAL_SEGMENT_ID; }
};

struct ProbePeerKeyHash {
    size_t operator()(const ProbePeerKey& key) const noexcept {
        size_t seed = std::hash<SegmentID>{}(key.segment_id);
        seed ^= std::hash<std::string>{}(key.peer_nic_path) +
                0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        return seed;
    }
};

// Health table primary key (design §5.1 / L-6). A topology or remote-target
// epoch change produces a NEW row; the old row is dropped rather than mutated
// in place, so late evidence can never be mistaken for current evidence.
struct PathVersion {
    ProbePathKey path;
    uint64_t topology_epoch{0};
    uint64_t target_epoch{0};

    bool operator==(const PathVersion&) const = default;

    bool operator<(const PathVersion& other) const {
        return std::tie(path, topology_epoch, target_epoch) <
               std::tie(other.path, other.topology_epoch, other.target_epoch);
    }

    [[nodiscard]] bool valid() const {
        return path.valid() && topology_epoch != 0 && target_epoch != 0;
    }
};

struct PathVersionHash {
    size_t operator()(const PathVersion& version) const noexcept {
        size_t seed = ProbePathKeyHash{}(version.path);
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<uint64_t>{}(version.topology_epoch));
        combine(std::hash<uint64_t>{}(version.target_epoch));
        return seed;
    }
};

// Full identity of one probe attempt (design §5.1).
//
// The health decision only reads version/fault_epoch/jetty_generation/
// area_epoch. probe_seq is used to claim the per-path task slot and assert
// in-flight uniqueness, while campaign_id/campaign_epoch only filter campaign
// accounting: a campaign generation mismatch must never be able to publish
// health, and a health-relevant mismatch must never advance campaign progress.
struct ProbeKey {
    PathVersion version;
    uint64_t fault_epoch{0};
    uint64_t campaign_id{0};
    uint64_t campaign_epoch{0};
    uint64_t probe_seq{0};
    uint64_t jetty_generation{0};
    uint64_t area_epoch{0};

    bool operator==(const ProbeKey&) const = default;
};

// ---------------------------------------------------------------------------
// Runtime enums (design §5.2).
enum class LinkHealth : uint8_t {
    kEligibleUnverified,
    kHealthy,
    kIsolated,
    kRecovering,
    kDisabled,
};

enum class TaskState : uint8_t {
    kIdle,
    kQueued,
    kInflight,
};

enum class EntryState : uint8_t {
    kQueued,
    kInflight,
    kOk,
    kFail,
    kCanceled,
};

enum class RecordState : uint8_t {
    kPosting,
    kAwaiting,
    kRejected,
    kSettled,
    kOrphan,
    kHandedOff,
};

enum class OrphanReason : uint8_t {
    kNone,
    kTimeout,
    kGenerationRetired,
    kShutdown,
};

// Pending is a probe-scheduling flag only (L-1). It never affects business
// path selection; only ISOLATED/DISABLED feed the business filter.
enum class PendingReason : uint8_t {
    kNone,
    kLaneRetired,
    kPortRevoked,
    kContextFailure,
    // F12: a replacement event arrived without a drain receipt for the
    // previous generation, so the port must stay unusable.
    kReplacementUnproven,
};

// Why a path was disabled. Recovery needs this: onContextAvailable() may only
// revive paths that the context failure disabled, never a path the graph or
// the pooling side revoked on purpose.
enum class DisableCause : uint8_t {
    kNone,
    kContextFailure,
    kPortRevoked,
    kGraphRemoval,
};

constexpr const char* linkHealthName(LinkHealth health) {
    switch (health) {
        case LinkHealth::kEligibleUnverified:
            return "ELIGIBLE_UNVERIFIED";
        case LinkHealth::kHealthy:
            return "HEALTHY";
        case LinkHealth::kIsolated:
            return "ISOLATED";
        case LinkHealth::kRecovering:
            return "RECOVERING";
        case LinkHealth::kDisabled:
            return "DISABLED";
    }
    return "UNKNOWN";
}

constexpr const char* entryStateName(EntryState state) {
    switch (state) {
        case EntryState::kQueued:
            return "QUEUED";
        case EntryState::kInflight:
            return "INFLIGHT";
        case EntryState::kOk:
            return "OK";
        case EntryState::kFail:
            return "FAIL";
        case EntryState::kCanceled:
            return "CANCELED";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Tuning (design §13.1). Parsing/validation of the transports/ub/probe/* keys
// lives in params.h; these are the locked defaults.
enum class ProbeJfcMode : uint8_t {
    kDedicated,
    kShared,
};

// Who owns the probe resources. kPool preserves the W1 injected-provider seam;
// kStandalone makes the transport construct StandaloneProbeResourceProvider +
// HandshakeProbePeerDirectory + UrmaProbePoster from the local contexts.
enum class ProbeProviderKind : uint8_t {
    kPool,
    kStandalone,
};

// One declared local probe port. `bind` selects exactly one physical EID at
// install time; `eid` is an optional static assertion against the discovered
// identity. A bare string declaration sets id == bind.
struct ProbeLocalPortDecl {
    std::string id;
    std::string bind;
    std::string eid;

    bool operator==(const ProbeLocalPortDecl&) const = default;
};

// One declared remote probe port. `id` is a local alias used only in this
// node's configuration and logs; `eid` is the cross-node identity.
struct ProbePeerPortDecl {
    std::string id;
    std::string eid;

    bool operator==(const ProbePeerPortDecl&) const = default;
};

struct ProbePeerDecl {
    std::string segment;
    std::vector<ProbePeerPortDecl> ports;
};

// Declared adjacency policy (closed-loop design §5.6).
struct ProbeAdjacencyPolicyParams {
    // Reject declared edges whose local/remote EIDs are in different
    // reachability scopes. Disable only for a deployment that really routes
    // across scopes; a warning is logged when disabled.
    bool require_same_eid_scope{true};
    // Consecutive kParamError observations on one declared edge after which
    // the edge is considered invalid for this install. 0 disables.
    uint32_t param_error_invalidate_threshold{3};
};

// Bootstrap handshake scheduling (design §4.1). Distinct from the probe
// scheduler's interval/timeout: this is the control-plane refresh loop.
struct ProbeHandshakeParams {
    uint64_t refresh_interval_ms{30000};
    uint64_t retry_backoff_ms{1000};
    uint64_t timeout_ms{2000};
    uint32_t threads{2};
    bool require_peer_ports{false};
    uint32_t withdraw_grace{3};
};

// Canonical URMA EID text: eight groups of four hex digits separated by ':'.
// Kept dependency-free so configuration parsing and topology resolution share
// one validator without pulling in the native adapter headers.
inline bool isCanonicalEid(std::string_view eid) {
    if (eid.size() != 39) return false;
    for (size_t index = 0; index < eid.size(); ++index) {
        const char value = eid[index];
        if (index % 5 == 4) {
            if (value != ':') return false;
            continue;
        }
        const bool hex = (value >= '0' && value <= '9') ||
                         (value >= 'a' && value <= 'f') ||
                         (value >= 'A' && value <= 'F');
        if (!hex) return false;
    }
    return true;
}

struct ProbeParams {
    static constexpr uint64_t kNsPerMs = 1'000'000;

    // W1 ships disabled: no provider, no PLM, no scheduler thread, and the
    // poller probe slots stay empty.
    bool enabled{false};
    uint64_t interval_ms{1000};
    uint64_t backoff_max_ms{30000};
    uint64_t timeout_ms{1000};
    uint32_t peer_budget{4};
    uint32_t failure_threshold{3};
    // F8: consecutive retryable post rejections after which the verification
    // obligation is abandoned explicitly (and counted) instead of silently
    // dropped.
    uint32_t post_rejection_cap{8};
    bool stop_on_recovery{true};
    uint32_t recover_success_threshold{1};
    ProbeJfcMode jfc_mode{ProbeJfcMode::kDedicated};
    ProbeProviderKind provider{ProbeProviderKind::kPool};
    // Self-owned probe resources (standalone): empty = all active EIDs of all
    // non-bonding physical devices; entries match native_device_name,
    // topology_name, or native:eidN. A native-only entry selects every active
    // EID of that device; an explicit EID entry selects exactly that EID.
    std::vector<std::string> device_filter;
    // 0 = system page size; otherwise a page-aligned multiple.
    uint64_t source_page_size{0};
    uint32_t jfc_depth{64};
    uint32_t credit_reserve{8};
    // Test mode (validation only; production must keep periodic_all=false).
    bool test_periodic_all{false};
    uint64_t test_interval_ms{5000};
    bool test_apply_health{true};

    // Declared probe topology (design §4). These are the operator's
    // declaration of local ports, peer ports and the legal adjacency edges.
    // They are the only source of business routing and probe-graph edges; the
    // handshake fills runtime handles for declared ports only.
    std::vector<ProbeLocalPortDecl> local_ports;
    std::map<std::string, ProbePeerDecl> peers;
    // peer -> (local_id -> remote_id list).
    std::map<std::string, std::map<std::string, std::vector<std::string>>>
        adjacency;
    // When true and a declaration exists, the declared set becomes the first
    // gate of business path selection. Defaults to true (fail-closed); a
    // two-step rollout can start with false and flip it after the probe graph
    // converged.
    bool enforce_adjacency{true};
    // ProbeLinkManager accepts a zero-edge graph at install and adds edges
    // incrementally. Derived from the declaration, never configured directly.
    bool dynamic_edges{false};
    ProbeHandshakeParams handshake;
    ProbeAdjacencyPolicyParams adjacency_policy;

    [[nodiscard]] bool hasDeclaredTopology() const {
        return !local_ports.empty() || !peers.empty() || !adjacency.empty();
    }

    [[nodiscard]] uint64_t intervalNs() const { return interval_ms * kNsPerMs; }
    [[nodiscard]] uint64_t timeoutNs() const { return timeout_ms * kNsPerMs; }
    [[nodiscard]] uint64_t backoffMaxNs() const {
        return backoff_max_ms * kNsPerMs;
    }
    [[nodiscard]] uint64_t testIntervalNs() const {
        return test_interval_ms * kNsPerMs;
    }
    [[nodiscard]] uint64_t handshakeRefreshNs() const {
        return handshake.refresh_interval_ms * kNsPerMs;
    }
    [[nodiscard]] uint64_t handshakeTimeoutNs() const {
        return handshake.timeout_ms * kNsPerMs;
    }

    [[nodiscard]] bool valid() const {
        if (interval_ms == 0 || backoff_max_ms < interval_ms) return false;
        if (timeout_ms == 0 || timeout_ms > interval_ms) return false;
        if (peer_budget == 0 || failure_threshold == 0) return false;
        // stop_on_recovery/recover_success_threshold/recover_success are fixed
        // values in W1; this struct rejects anything else so a later config
        // reader cannot silently accept a divergent value.
        if (!stop_on_recovery || recover_success_threshold != 1) return false;
        if (jfc_depth < 8) return false;
        if (jfc_mode == ProbeJfcMode::kShared && credit_reserve == 0)
            return false;
        // Standalone builds its probe JFC itself; shared mode needs a business
        // JFC injected per port and is not supported by this profile.
        if (provider == ProbeProviderKind::kStandalone &&
            jfc_mode == ProbeJfcMode::kShared) {
            return false;
        }
        if (test_interval_ms == 0) return false;
        if (test_periodic_all && !enabled) return false;
        // Handshake schedule (design §4.1/§4.3 rule 5).
        if (handshake.refresh_interval_ms == 0 ||
            handshake.refresh_interval_ms < 1000 ||
            handshake.refresh_interval_ms > 600000) {
            return false;
        }
        if (handshake.retry_backoff_ms < 100 ||
            handshake.retry_backoff_ms > 60000) {
            return false;
        }
        if (handshake.timeout_ms < 100 || handshake.timeout_ms > 30000 ||
            handshake.timeout_ms > handshake.refresh_interval_ms) {
            return false;
        }
        if (handshake.threads < 1 || handshake.threads > 4) return false;
        if (handshake.withdraw_grace == 0) return false;
        // A declaration is only meaningful together with probing (design
        // §4.3 rules 7 and 8): rejecting half a mapping is safer than
        // silently enforcing an incomplete path set.
        if (hasDeclaredTopology()) {
            if (!enabled) return false;
            if (local_ports.empty() || peers.empty() || adjacency.empty()) {
                return false;
            }
        }
        return true;
    }

    // Enforcement only exists when an adjacency was declared. The default
    // enforce_adjacency=true therefore does not affect the legacy/injected
    // seams, which never declare a topology.
    [[nodiscard]] bool enforcesAdjacency() const {
        return enforce_adjacency && !adjacency.empty();
    }
};

// ---------------------------------------------------------------------------
// Counters (design §13.2). All of them are written under the ProbeLinkManager
// mutex and copied out by stats(); no metrics framework is involved.
//
// A few entries are high-water marks rather than monotonic counters:
// probe_inflight_per_port and probe_post_block_us.
struct ProbeStats {
    uint64_t probe_scheduled{0};
    uint64_t probe_posted{0};
    uint64_t probe_post_rejected{0};
    uint64_t probe_post_unknown{0};
    uint64_t probe_success{0};
    uint64_t probe_failure_path{0};
    uint64_t probe_failure_lane{0};
    uint64_t probe_lane_retire{0};
    uint64_t probe_lane_rebuild{0};
    uint64_t probe_timeout_unknown{0};
    // D2: a deadline passed while the native post was still inside the
    // provider, so the scan refused to fence that Jetty. Sustained growth
    // means a probe port is pinned by a stuck post and has silently left the
    // probe graph (its slot and peer budget stay held).
    uint64_t probe_timeout_deferred_by_post_return{0};
    uint64_t probe_stale{0};
    uint64_t probe_unknown_token{0};
    uint64_t probe_evidence_unmapped{0};
    uint64_t probe_area_config_error{0};
    uint64_t probe_skipped_pending{0};
    uint64_t probe_skipped_credit{0};
    uint64_t probe_skipped_peer_budget{0};
    uint64_t probe_skipped_broken{0};
    uint64_t probe_handle_generation_mismatch{0};
    uint64_t probe_gate_blocked_by_epoch{0};
    uint64_t probe_gate_blocked_by_resource{0};
    uint64_t probe_gate_blocked_by_external{0};
    uint64_t campaign_created{0};
    uint64_t campaign_merged{0};
    uint64_t campaign_exited{0};
    uint64_t campaign_entry_ok{0};
    uint64_t campaign_entry_fail{0};
    uint64_t campaign_entry_canceled{0};
    uint64_t path_state_eligible{0};
    uint64_t path_state_healthy{0};
    uint64_t path_state_isolated{0};
    uint64_t path_state_recovering{0};
    uint64_t path_state_disabled{0};
    // F7: DISABLED paths moved back to RECOVERING by onContextAvailable().
    uint64_t path_revived_on_context_available{0};
    // F12: replacement events rejected because the previous generation had no
    // drain receipt.
    uint64_t probe_replacement_unproven{0};
    // F8: probe posts the provider rejected for a retryable reason (capacity,
    // not-ready, transient device error) and terminal verification give-ups.
    uint64_t probe_post_rejected_retryable{0};
    uint64_t probe_verification_abandoned{0};
    uint64_t probe_inflight_per_port{0};
    uint64_t probe_rr_round{0};
    uint64_t probe_drain_unproven{0};
    uint64_t probe_post_conflict{0};
    uint64_t probe_post_block_us{0};
    uint64_t probe_ack_timeout{0};
    uint64_t business_error_while_isolated{0};
    uint64_t probe_orphan_added{0};
    uint64_t probe_orphan_settled{0};
    uint64_t probe_orphan_handed_off{0};
    uint64_t probe_slot_anomaly{0};
    uint64_t probe_test_post{0};
    uint64_t probe_test_ok{0};
    uint64_t probe_test_fail{0};
    uint64_t probe_test_missed{0};
    uint64_t probe_test_interval_missed{0};
    uint64_t probe_event_malformed{0};
    uint64_t probe_event_gap{0};
    uint64_t probe_event_gap_stuck{0};
    uint64_t campaign_waiting_for_event{0};

    // W1a local diagnostics: business evidence that must not create a path
    // fault, plus interface-7 delivery bookkeeping.
    uint64_t business_param_error{0};
    uint64_t business_lane_error{0};
    uint64_t business_unknown_error{0};
    uint64_t business_error_while_disabled{0};
    uint64_t probe_event_duplicate{0};
    uint64_t probe_event_stale_epoch{0};

    // W3 business-routing gate (design §7.1 / RK8). A path that selection asked
    // about but the probe graph does not model is treated as USABLE, and
    // counted here: a growing value means the adjacency configuration is
    // incomplete rather than that traffic is being filtered.
    uint64_t business_path_not_in_graph{0};
    uint64_t business_path_health_blocked{0};
    uint64_t business_path_gate_blocked{0};
};

}  // namespace ub
}  // namespace tent
// Post acceptance as the probe side observes it. Lives in the leaf types
// header so both the poster interface and the link manager can name it.
enum class ProbePostOutcome : uint8_t {
    kAccepted,
    // Terminal: the version was revoked/superseded or the request itself was
    // invalid; the verification obligation may be dropped.
    kRejected,
    // Retryable: capacity, not-ready, transient provider/device error. The
    // verification obligation is kept and retried with backoff (F8).
    kRejectedRetryable,
    kUnknown,
};

}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_TYPES_H_
