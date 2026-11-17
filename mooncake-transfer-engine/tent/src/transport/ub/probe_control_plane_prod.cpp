// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_control_plane.h"

#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment_manager.h"

namespace mooncake {
namespace tent {
namespace ub {

Status ControlClientProbeBootstrapClient::get(
    const std::string& server_addr, const ProbeBootstrapRequest& request,
    ProbeBootstrapResponse& response, uint64_t timeout_ms) {
    return ControlClient::getProbeBootstrap(server_addr, request, response,
                                            timeout_ms);
}

Status SegmentManagerAddressResolver::resolve(const std::string& peer_segment,
                                              std::string& rpc_server_addr,
                                              SegmentID& segment_id) {
    if (manager_ == nullptr) {
        return Status::InvalidArgument("segment manager is null" LOC_MARK);
    }
    segment_id = LOCAL_SEGMENT_ID;
    CHECK_STATUS(manager_->openRemote(segment_id, peer_segment));
    SegmentDescRef desc;
    auto status = manager_->getRemote(desc, peer_segment);
    if (!status.ok() || !desc) return status;
    if (desc->rpc_server_addr.empty()) {
        return Status::InvalidEntry(
            "peer segment has no control-plane address" LOC_MARK);
    }
    rpc_server_addr = desc->rpc_server_addr;
    return Status::OK();
}

void SegmentManagerAddressResolver::release(SegmentID) {
    // openRemote() returns the same handle for an already-open business
    // segment. Closing it here would delete the caller's mapping, so the probe
    // source deliberately does not own/close the handle.
}

Status ProviderLocalEndpointSource::snapshot(ProbeBootstrapResponse& local) {
    if (provider_ == nullptr) {
        return Status::InvalidArgument(
            "standalone probe provider is null" LOC_MARK);
    }
    return provider_->exportBootstrap(ProbeBootstrapRequest{}, local);
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
