// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3g-c: the bootstrap-driven ProbePeerDescriptorSource.
//
// W1 shipped the probe assembly with only a test source; production never had
// one, which is why real-machine probing could not be enabled. This source is
// populated by the UB bootstrap exchange: when a business endpoint binds, the
// peer's v2 `probe_areas` block becomes a ProbePeerDescriptor for that
// (local port, remote segment, remote port) edge.
//
// Rules (design §7.2 / IN-4):
//   * no probe area -> no descriptor (the peer is skipped, counted, and probing
//     for it stays off; an address is never guessed);
//   * exactly one area per bootstrap envelope is expected (one envelope already
//     describes one peer port); more than one is ambiguous and rejected;
//   * a re-bootstrap for the same edge replaces the previous descriptor, so a
//     stale page can never be probed twice.
//
// Design: docs/analyze/tent-ub-w3g-pool-claim-and-v2-design.md §7.2.

#ifndef TENT_TRANSPORT_UB_BOOTSTRAP_PEER_SOURCE_H_
#define TENT_TRANSPORT_UB_BOOTSTRAP_PEER_SOURCE_H_

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/bootstrap_v2.h"

namespace mooncake::tent::ub {

// Transport-neutral projection of one bound peer's bootstrap envelope. Keeping
// this DTO instead of taking UbBootstrapDesc keeps the runtime control-plane
// header out of the UB transport leaf.
struct BootstrapProbePeerInfo {
    Topology::NicID local_nic{-1};
    SegmentID peer_segment_id{LOCAL_SEGMENT_ID};
    Topology::NicID remote_nic{-1};
    std::string peer_nic_path;
    std::string peer_eid;
    std::vector<std::string> capabilities;
    std::vector<UbProbeAreaDescriptor> probe_areas;
    uint64_t topology_epoch{0};
    uint64_t target_epoch{0};
    uint64_t endpoint_generation{0};
    uint32_t schema_version{kUbBootstrapSchemaV1};
};

class BootstrapProbePeerDescriptorSource final
    : public ProbePeerDescriptorSource {
   public:
    // Records one bound peer. Returns OK for every well-formed envelope: a peer
    // without probe support is a normal state, not an error (probing stays off
    // for it). `probeAreaMissing()` counts those.
    Status ingest(const BootstrapProbePeerInfo& info);

    Status snapshot(std::vector<ProbePeerDescriptor>& descriptors) override;

    [[nodiscard]] size_t descriptorCount() const;
    [[nodiscard]] uint64_t probeAreaMissing() const;
    [[nodiscard]] uint64_t rejected() const;

   private:
    struct EdgeKey {
        Topology::NicID local_nic{-1};
        SegmentID peer_segment_id{LOCAL_SEGMENT_ID};
        Topology::NicID remote_nic{-1};

        bool operator<(const EdgeKey& other) const {
            if (local_nic != other.local_nic)
                return local_nic < other.local_nic;
            if (peer_segment_id != other.peer_segment_id) {
                return peer_segment_id < other.peer_segment_id;
            }
            return remote_nic < other.remote_nic;
        }
    };

    mutable std::mutex mutex_;
    std::map<EdgeKey, ProbePeerDescriptor> descriptors_;
    uint64_t probe_area_missing_{0};
    uint64_t rejected_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_BOOTSTRAP_PEER_SOURCE_H_
