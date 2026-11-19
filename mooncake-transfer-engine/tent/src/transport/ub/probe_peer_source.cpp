// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_peer_source.h"

#include <algorithm>

namespace mooncake::tent::ub {

BootstrapProbePeerSource::BootstrapProbePeerSource(
    const ProbeTopology* topology, const ProbePeerRegistry* registry)
    : topology_(topology), registry_(registry) {}

Status BootstrapProbePeerSource::snapshot(
    std::vector<ProbePeerDescriptor>& descriptors) {
    descriptors.clear();
    if (topology_ == nullptr || registry_ == nullptr) {
        return Status::InvalidArgument(
            "probe peer source has no topology or registry");
    }
    for (const auto& [peer, decl] : topology_->peerDecls()) {
        auto publication = registry_->find(peer);
        if (!publication || !publication->published) continue;
        const auto* edges = topology_->adjacencyFor(peer);
        if (edges == nullptr) continue;
        for (const auto& [local_id, remote_ids] : *edges) {
            const auto local_nic = topology_->localNic(local_id);
            if (!local_nic.has_value()) continue;
            for (const auto& remote_id : remote_ids) {
                const auto* declared_port =
                    topology_->peerPort(peer, remote_id);
                if (declared_port == nullptr) continue;
                auto port = std::find_if(
                    publication->ports.begin(), publication->ports.end(),
                    [&](const ProbePeerPortBinding& candidate) {
                        return candidate.id == remote_id;
                    });
                if (port == publication->ports.end() || port->withdrawn) {
                    ++skipped_withdrawn_;
                    continue;
                }
                if (port->remote_nic < 0 || port->target_epoch == 0 ||
                    !port->area_length || port->area_epoch == 0 ||
                    port->area_descriptor.hex.empty()) {
                    continue;
                }
                ProbePeerDescriptor descriptor;
                descriptor.local_nic = *local_nic;
                descriptor.peer_segment_id = publication->peer_id;
                descriptor.remote_nic = port->remote_nic;
                descriptor.peer_nic_path = port->peer_nic_path;
                descriptor.topology_epoch = publication->topology_epoch;
                descriptor.target_epoch = port->target_epoch;
                descriptor.area_descriptor = port->area_descriptor;
                descriptor.area_length = port->area_length;
                descriptor.area_epoch = port->area_epoch;
                descriptor.area_token = port->area_token;
                descriptor.remote_jetty = port->jetty;
                descriptor.node_gate_ok = port->node_gate_ok;
                descriptor.port_gate_ok = port->port_gate_ok;
                descriptor.profile_gate_ok = port->profile_gate_ok;
                // The probe graph is decoupled from business endpoint
                // generations; the identity mapping is the ProbePathKey.
                descriptor.endpoint_generation = 0;
                descriptors.push_back(std::move(descriptor));
                ++emitted_edges_;
            }
        }
    }
    return Status::OK();
}

}  // namespace mooncake::tent::ub
