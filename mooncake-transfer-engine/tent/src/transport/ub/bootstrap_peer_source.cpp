// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/bootstrap_peer_source.h"

#include <utility>

namespace mooncake::tent::ub {

Status BootstrapProbePeerDescriptorSource::ingest(
    const BootstrapProbePeerInfo& info) {
    // Only a fully identified directed edge may become a probe descriptor.
    // LOCAL_SEGMENT_ID is the "unknown peer segment" sentinel, and a negative
    // port id cannot address a native entity.
    if (info.local_nic < 0 || info.remote_nic < 0 ||
        info.peer_segment_id == LOCAL_SEGMENT_ID ||
        info.peer_nic_path.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++rejected_;
        return Status::InvalidArgument(
            "UB bootstrap probe peer is missing its directed edge identity");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    // A v1 peer (or any peer that did not publish an area) keeps probing off.
    if (info.schema_version < kUbBootstrapSchemaV2 ||
        info.probe_areas.empty()) {
        ++probe_area_missing_;
        return Status::OK();
    }
    if (info.probe_areas.size() != 1) {
        // One bootstrap envelope describes one peer port, so it must publish
        // exactly one page. More than one is ambiguous; picking one would risk
        // probing a page that belongs to a different port.
        ++rejected_;
        return Status::InvalidArgument(
            "UB bootstrap peer published an ambiguous probe area set");
    }

    const UbProbeAreaDescriptor& area = info.probe_areas.front();
    if (area.length == 0 || area.area_epoch == 0 || area.jetty_id == 0 ||
        area.descriptor.hex.empty()) {
        ++rejected_;
        return Status::InvalidArgument("UB bootstrap probe area is incomplete");
    }

    ProbePeerDescriptor descriptor;
    descriptor.schema_version = kProbePeerDescriptorSchemaVersion;
    descriptor.local_nic = info.local_nic;
    descriptor.peer_segment_id = info.peer_segment_id;
    descriptor.remote_nic = info.remote_nic;
    descriptor.peer_nic_path = info.peer_nic_path;
    // Epochs are part of the directory cache key and must be nonzero; fall back
    // to the bootstrap generation when the peer did not publish a v2 epoch.
    descriptor.topology_epoch =
        info.topology_epoch != 0 ? info.topology_epoch : 1;
    descriptor.target_epoch = info.target_epoch != 0 ? info.target_epoch : 1;
    descriptor.area_descriptor = area.descriptor;
    descriptor.area_length = area.length;
    descriptor.area_epoch = area.area_epoch;
    descriptor.area_token = area.token;
    descriptor.remote_jetty.eid = info.peer_eid;
    descriptor.remote_jetty.id = area.jetty_id;
    descriptor.remote_jetty.uasid = area.uasid;
    descriptor.remote_jetty.token = area.token;
    // Gate bits are not published by bootstrap v2, so they are *proxied* here:
    // a completed handshake proves node/port reachability and the published
    // page proves the probe profile. This is an explicit stand-in until the
    // pooling side publishes real gate fields; do not read it as an
    // independent gate evaluation.
    descriptor.node_gate_ok = hasUbCapability(info.capabilities, "read") &&
                              hasUbCapability(info.capabilities, "write");
    descriptor.port_gate_ok = true;
    descriptor.profile_gate_ok = true;
    descriptor.endpoint_generation = info.endpoint_generation;

    // Latest wins for one directed edge: a re-bootstrap may publish a new page
    // epoch, and the old descriptor must not shadow it in the snapshot.
    descriptors_[EdgeKey{info.local_nic, info.peer_segment_id,
                         info.remote_nic}] = std::move(descriptor);
    return Status::OK();
}

Status BootstrapProbePeerDescriptorSource::snapshot(
    std::vector<ProbePeerDescriptor>& descriptors) {
    std::lock_guard<std::mutex> lock(mutex_);
    descriptors.clear();
    descriptors.reserve(descriptors_.size());
    for (const auto& [_, descriptor] : descriptors_) {
        descriptors.push_back(descriptor);
    }
    return Status::OK();
}

size_t BootstrapProbePeerDescriptorSource::descriptorCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return descriptors_.size();
}

uint64_t BootstrapProbePeerDescriptorSource::probeAreaMissing() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return probe_area_missing_;
}

uint64_t BootstrapProbePeerDescriptorSource::rejected() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return rejected_;
}

}  // namespace mooncake::tent::ub
