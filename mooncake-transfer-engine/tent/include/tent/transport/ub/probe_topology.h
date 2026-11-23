// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Declared probe topology and business adjacency policy.
//
// The configuration declares local ports (id -> physical EID binding), peer
// ports (id -> real EID) and the legal edges between them. This header turns
// that declaration into:
//   * one resolved local binding per declared port (id -> Topology::NicID),
//   * one read-only adjacency view per (peer, slice) that answers the business
//     routing gate without any string lookup on the hot path.
//
// Topology is never discovered here: a declared binding must match exactly one
// discovered physical EID, and a declared peer EID must match exactly one UB
// NIC in the peer segment metadata. Anything else fails closed.

#ifndef TENT_TRANSPORT_UB_PROBE_TOPOLOGY_H_
#define TENT_TRANSPORT_UB_PROBE_TOPOLOGY_H_

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/probe_stats.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/probe_types.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

class ProbePeerRegistry;

// Provider-neutral reachability scope of one EID. The default derives it from
// the canonical EID prefix (the first seven groups, i.e. everything except the
// interface/port group); config declarations can disable the check when a
// deployment really routes across scopes.
struct EidScope {
    std::string key;
    bool trusted{false};
};

[[nodiscard]] EidScope eidScopeForEid(std::string_view eid);

// True when both EIDs are trusted and share a reachability scope. An EID that
// cannot be parsed is treated as compatible: an older peer may not publish it,
// and refusing to pair would break deployments that work today. Legacy device
// selection uses this to refuse cross-scope local x remote pairs (df00 vs
// df04), which the provider rejects with URMA_CR_LOC_ACCESS_ERR.
[[nodiscard]] bool sameEidScope(std::string_view local_eid,
                                std::string_view remote_eid);

// One declared local port resolved against the discovered devices.
struct ProbeLocalPortBinding {
    std::string id;
    std::string bind;
    std::string eid;
    std::string topology_name;
    std::string native_device_name;
    Topology::NicID local_nic{-1};
};

enum class AdjacencyDecision : uint8_t {
    kAllowed = 0,
    kUndeclaredPeer,
    kUndeclaredLocal,
    kUndeclaredRemote,
    kPeerEidUnmatched,
    kPortMissing,
    kPeerGateBlocked,
};

constexpr const char* adjacencyDecisionName(AdjacencyDecision decision) {
    switch (decision) {
        case AdjacencyDecision::kAllowed:
            return "allowed";
        case AdjacencyDecision::kUndeclaredPeer:
            return "adjacency_undeclared_peer";
        case AdjacencyDecision::kUndeclaredLocal:
            return "adjacency_undeclared_local";
        case AdjacencyDecision::kUndeclaredRemote:
            return "adjacency_undeclared_remote";
        case AdjacencyDecision::kPeerEidUnmatched:
            return "adjacency_peer_eid_unmatched";
        case AdjacencyDecision::kPortMissing:
            return "adjacency_port_missing";
        case AdjacencyDecision::kPeerGateBlocked:
            return "adjacency_peer_gate_blocked";
    }
    return "unknown";
}

// Per-(peer, slice) read-only view. `allowed` is a dense bitmap indexed by
// local slot * remote_nics.size() + remote slot, so a candidate test is one
// integer index lookup plus one byte read.
struct AdjacencyView {
    bool enforced{false};
    bool peer_declared{false};
    // At least one declared peer EID could not be matched to exactly one UB
    // NIC in the peer segment metadata: every candidate for this peer is
    // rejected until the metadata changes (fail-closed).
    bool peer_eid_unmatched{false};
    // True once the peer has successfully published at least one snapshot.
    // Until then the declared edges are allowed as ELIGIBLE_UNVERIFIED.
    bool publication_seen{false};
    std::vector<Topology::NicID> local_nics;
    std::vector<Topology::NicID> remote_nics;
    std::vector<uint8_t> allowed;
    // Per remote slot: 0 = published and gated, 1 = port missing/withdrawn,
    // 2 = published but a node/port/profile gate is closed. Ignored while
    // publication_seen is false.
    std::vector<uint8_t> remote_status;

    [[nodiscard]] bool empty() const {
        return local_nics.empty() || remote_nics.empty();
    }

    // `local_declared` is set when the local NIC is declared for this peer at
    // all, so the caller can distinguish "this port may not talk to that port"
    // from "this port is not part of the declared topology".
    [[nodiscard]] AdjacencyDecision check(Topology::NicID local_nic,
                                          Topology::NicID remote_nic,
                                          bool* local_declared = nullptr) const;
};

// Immutable after resolve(); shared by the workers' read-only routing gate.
class ProbeTopology {
   public:
    ProbeTopology() = default;

    // Validates the declaration and resolves every local binding. The device
    // list is the install-time discovery snapshot and `topology` the business
    // topology the workers use. An empty declaration resolves to an empty,
    // non-enforcing topology.
    static Status resolve(const ProbeParams& params,
                          const std::vector<DeviceInfo>& devices,
                          const Topology& topology, ProbeTopology& output,
                          uint32_t topology_dump_limit = 0);

    [[nodiscard]] bool declared() const { return declared_; }
    [[nodiscard]] bool enforces() const { return enforces_; }
    [[nodiscard]] const std::vector<ProbeLocalPortBinding>& ports() const {
        return ports_;
    }
    [[nodiscard]] const std::map<std::string, ProbePeerDecl>& peers() const {
        return peers_;
    }
    [[nodiscard]] bool hasPeer(const std::string& peer) const {
        return peers_.count(peer) != 0;
    }
    [[nodiscard]] const std::map<std::string, ProbePeerDecl>& peerDecls()
        const {
        return peers_;
    }
    [[nodiscard]] const std::map<std::string, std::vector<std::string>>*
    adjacencyFor(const std::string& peer) const {
        auto found = adjacency_.find(peer);
        return found == adjacency_.end() ? nullptr : &found->second;
    }
    [[nodiscard]] const std::vector<ProbePeerPortDecl>* peerPorts(
        const std::string& peer) const {
        auto found = peers_.find(peer);
        return found == peers_.end() ? nullptr : &found->second.ports;
    }
    [[nodiscard]] const ProbePeerPortDecl* peerPort(
        const std::string& peer, const std::string& remote_id) const {
        auto found = peers_.find(peer);
        if (found == peers_.end()) return nullptr;
        for (const auto& port : found->second.ports) {
            if (port.id == remote_id) return &port;
        }
        return nullptr;
    }
    // Synthetic probe peer id (1<<63 + declaration order); never a business
    // SegmentID. Absent when the peer is not declared.
    [[nodiscard]] std::optional<uint64_t> probePeerId(
        const std::string& peer) const {
        auto found = peers_.find(peer);
        if (found == peers_.end()) return std::nullopt;
        const auto index =
            static_cast<uint64_t>(std::distance(peers_.begin(), found));
        return kProbePeerIdBase + index;
    }
    void setPeerRegistry(const ProbePeerRegistry* registry) {
        peer_registry_ = registry;
    }

    // Observability only (design §11). Safe on the business hot path: one
    // relaxed atomic increment per candidate decision.
    void recordAdjacency(AdjacencyDecision decision, bool unverified) const;
    [[nodiscard]] ProbeTopologyStats stats() const;
    [[nodiscard]] std::optional<Topology::NicID> localNic(
        const std::string& local_id) const {
        auto found = bindings_.find(local_id);
        if (found == bindings_.end()) return std::nullopt;
        return found->second;
    }
    [[nodiscard]] std::optional<std::string> localIdForNic(
        Topology::NicID local_nic) const {
        auto found = local_by_nic_.find(local_nic);
        if (found == local_by_nic_.end()) return std::nullopt;
        return found->second;
    }

    // Builds the per-slice view. Never fails: an unmatched declared EID is
    // reported through AdjacencyView::peer_eid_unmatched so the routing gate
    // can fail closed with a specific reason.
    [[nodiscard]] AdjacencyView viewFor(const std::string& peer,
                                        const SegmentDesc& remote) const;

   private:
    bool declared_{false};
    bool enforces_{false};
    std::vector<ProbeLocalPortBinding> ports_;
    std::unordered_map<std::string, Topology::NicID> bindings_;
    // local NicID -> declared local id (for diagnostics).
    std::unordered_map<Topology::NicID, std::string> local_by_nic_;
    std::map<std::string, ProbePeerDecl> peers_;
    std::map<std::string, std::map<std::string, std::vector<std::string>>>
        adjacency_;
    const ProbePeerRegistry* peer_registry_{nullptr};
    enum DecisionIndex : size_t {
        kAllowedIndex = 0,
        kUndeclaredPeerIndex,
        kUndeclaredLocalIndex,
        kUndeclaredRemoteIndex,
        kPeerEidUnmatchedIndex,
        kPortMissingIndex,
        kPeerGateBlockedIndex,
        kDecisionIndexCount,
    };
    // Atomics live behind a shared pointer so ProbeTopology itself stays
    // movable (resolve() builds a temporary and returns it).
    struct Counters {
        std::array<std::atomic<uint64_t>, kDecisionIndexCount> decision{};
        std::atomic<uint64_t> unverified{0};
    };
    std::shared_ptr<Counters> counters_{std::make_shared<Counters>()};
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_TOPOLOGY_H_
