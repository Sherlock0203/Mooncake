// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Per-peer probe publication cache (design §7.6/§8.1). The registry is the
// only writer of "what the peer published": the coordinator applies validated
// responses here, the descriptor source and the business adjacency view read
// immutable copies. It never creates resources and never calls a provider.

#ifndef TENT_TRANSPORT_UB_PROBE_PEER_REGISTRY_H_
#define TENT_TRANSPORT_UB_PROBE_PEER_REGISTRY_H_

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/probe_resources.h"
#include "tent/transport/ub/probe_types.h"

namespace mooncake {
namespace tent {
namespace ub {

// One declared peer port's current runtime handles. `missing_publications`
// counts consecutive publications in which the port was absent; only after
// withdraw_grace does `withdrawn` become true.
struct ProbePeerPortBinding {
    std::string id;
    std::string eid;
    Topology::NicID remote_nic{-1};
    std::string topology_name;
    std::string peer_nic_path;
    uint64_t target_epoch{0};
    RemoteJettyRef jetty;
    SegmentDescriptor area_descriptor;
    uint64_t area_length{0};
    uint64_t area_epoch{0};
    uint32_t area_token{0};
    bool node_gate_ok{false};
    bool port_gate_ok{false};
    bool profile_gate_ok{false};
    uint32_t missing_publications{0};
    bool withdrawn{false};
};

// Lightweight publication view for the business hot path: no area descriptor
// or Jetty payload, only the per-port gate/withdrawal state.
struct ProbePeerPortView {
    bool withdrawn{false};
    bool gates_ok{false};
};

struct ProbePeerPublicationView {
    bool published{false};
    bool stale{false};
    std::map<std::string, ProbePeerPortView> ports;
};

struct ProbePeerBinding {
    std::string segment;
    uint64_t peer_id{0};
    uint64_t node_incarnation{0};
    uint64_t topology_epoch{0};
    uint64_t publication_epoch{0};
    bool published{false};
    bool stale{false};
    uint64_t last_success_ns{0};
    std::vector<ProbePeerPortBinding> ports;
};

// Declared peer port identity from configuration. The registry allocates a
// synthetic ProbePeerId per declared peer at construction; it is never a
// business SegmentID.
struct ProbeDeclaredPeerPort {
    std::string id;
    std::string eid;
};

class ProbePeerRegistry {
   public:
    // `declared` comes from ProbeTopology and is immutable afterwards.
    explicit ProbePeerRegistry(
        const std::map<std::string, std::vector<ProbeDeclaredPeerPort>>&
            declared,
        uint32_t withdraw_grace = 3,
        const std::map<std::string, uint64_t>& peer_ids = {});

    // Applies a validated publication. A stale incarn/epoch is acknowledged
    // but not applied (`applied=false`). Ports absent from `publication` are
    // carried over until withdraw_grace consecutive misses, then withdrawn.
    Status apply(const ProbePeerBinding& publication, uint64_t now_ns,
                 bool* applied = nullptr, bool* stale = nullptr);

    // Cheap snapshot used by ProbeTopology::viewFor() on every route; it
    // deliberately omits handles and descriptor hex.
    [[nodiscard]] std::optional<ProbePeerPublicationView> publicationView(
        const std::string& peer) const;

    // A failed handshake keeps the last usable publication but marks it stale
    // for observation; it never removes edges or blocks business by itself.
    void markStale(const std::string& peer, uint64_t now_ns);

    [[nodiscard]] std::optional<ProbePeerBinding> find(
        const std::string& peer) const;
    [[nodiscard]] std::vector<ProbePeerBinding> snapshot() const;
    [[nodiscard]] std::optional<uint64_t> peerId(const std::string& peer) const;
    [[nodiscard]] std::vector<std::string> peers() const;
    [[nodiscard]] size_t peerCount() const;

   private:
    mutable std::mutex mutex_;
    std::map<std::string, ProbePeerBinding> bindings_;
    std::map<std::string, std::vector<ProbeDeclaredPeerPort>> declared_;
    uint32_t withdraw_grace_;
    uint64_t next_peer_id_{kProbePeerIdBase};
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_PEER_REGISTRY_H_
