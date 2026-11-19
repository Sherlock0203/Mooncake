// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// BootstrapProbePeerSource joins the declared topology with the peer
// publication registry and produces the descriptor list consumed by
// HandshakeProbePeerDirectory. It creates no resources and never invents an
// edge: every descriptor is one declared adjacency edge whose remote port has
// a current publication.

#ifndef TENT_TRANSPORT_UB_PROBE_PEER_SOURCE_H_
#define TENT_TRANSPORT_UB_PROBE_PEER_SOURCE_H_

#include <cstdint>
#include <vector>

#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/probe_peer_registry.h"
#include "tent/transport/ub/probe_topology.h"

namespace mooncake {
namespace tent {
namespace ub {

class BootstrapProbePeerSource final : public ProbePeerDescriptorSource {
   public:
    BootstrapProbePeerSource(const ProbeTopology* topology,
                             const ProbePeerRegistry* registry);

    Status snapshot(std::vector<ProbePeerDescriptor>& descriptors) override;

    [[nodiscard]] uint64_t skippedWithdrawn() const {
        return skipped_withdrawn_;
    }
    [[nodiscard]] uint64_t emittedEdges() const { return emitted_edges_; }

   private:
    const ProbeTopology* topology_;
    const ProbePeerRegistry* registry_;
    uint64_t skipped_withdrawn_{0};
    uint64_t emitted_edges_{0};
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_PEER_SOURCE_H_
