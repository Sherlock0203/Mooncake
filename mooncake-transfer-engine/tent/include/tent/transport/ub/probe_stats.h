// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Lightweight probe observation structs shared by the transport facade and the
// probe components, so the public ub_transport.h does not pull in the provider,
// registry or coordinator headers.

#ifndef TENT_TRANSPORT_UB_PROBE_STATS_H_
#define TENT_TRANSPORT_UB_PROBE_STATS_H_

#include <cstdint>

namespace mooncake::tent::ub {

struct ProbeTopologyStats {
    uint64_t ports{0};
    uint64_t edges{0};
    uint64_t adjacency_allowed{0};
    uint64_t adjacency_undeclared_peer{0};
    uint64_t adjacency_undeclared_local{0};
    uint64_t adjacency_undeclared_remote{0};
    uint64_t adjacency_peer_eid_unmatched{0};
    uint64_t adjacency_port_missing{0};
    uint64_t adjacency_peer_gate_blocked{0};
    uint64_t adjacency_unverified_peer{0};
};

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
    uint64_t graph_event_failed{0};
    uint64_t refresh_hint{0};
    uint64_t publication_bump{0};
    uint64_t publication_ingest{0};
    uint64_t missing_remote_ports{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_PROBE_STATS_H_
