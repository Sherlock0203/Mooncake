// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_control_plane.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <thread>
#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace ub {
namespace {

uint64_t nowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace

ControlPlaneProbePeerSource::ControlPlaneProbePeerSource(
    std::unique_ptr<ProbeBootstrapClient> client,
    std::unique_ptr<PeerAddressResolver> resolver,
    std::unique_ptr<LocalEndpointSource> local, ProbeLinkGraph graph,
    ProbeParams params)
    : client_(std::move(client)),
      resolver_(std::move(resolver)),
      local_(std::move(local)),
      graph_(std::move(graph)),
      params_(std::move(params)) {}

ControlPlaneProbePeerSource::~ControlPlaneProbePeerSource() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (resolver_) {
        for (auto& [peer, state] : peers_) {
            if (state.segment_id != LOCAL_SEGMENT_ID) {
                resolver_->release(state.segment_id);
            }
        }
    }
}

Status ControlPlaneProbePeerSource::refreshPeer(const std::string& peer,
                                                PeerState& state,
                                                uint64_t now_ns,
                                                bool wait_initial) {
    if (!resolver_ || !client_) {
        return Status::InvalidArgument(
            "probe control-plane source is incomplete" LOC_MARK);
    }
    Status last_error = Status::InternalError("probe peer was not queried");
    const uint64_t deadline =
        now_ns +
        (wait_initial ? params_.peer_wait_ms * ProbeParams::kNsPerMs : 0);
    for (;;) {
        if (state.rpc_server_addr.empty()) {
            last_error = resolver_->resolve(peer, state.rpc_server_addr,
                                            state.segment_id);
        }
        if (last_error.ok()) {
            ProbeBootstrapRequest request;
            request.requester_node = local_node_id_;
            request.known_node_epoch = state.valid ? state.node_epoch : 0;
            ProbeBootstrapResponse response;
            last_error = client_->get(state.rpc_server_addr, request, response,
                                      params_.peer_rpc_timeout_ms);
            if (last_error.ok()) {
                auto validation = ValidateProbeBootstrapResponse(response);
                if (!validation.ok()) {
                    last_error = validation;
                } else if (response.unchanged) {
                    // The current provider never emits this; accepting it
                    // would pin stale target/area descriptors from an older
                    // server inside the same node_epoch.
                    last_error = Status::InvalidEntry(
                        "peer returned an unsupported unchanged response" +
                        std::string(LOC_MARK));
                } else {
                    state.node_id = response.node_id;
                    state.node_epoch = response.node_epoch;
                    state.topology_epoch = response.topology_epoch;
                    state.endpoints = std::move(response.endpoints);
                    state.valid = true;
                    state.fetched_ns = nowNs();
                    return Status::OK();
                }
            }
        }
        const uint64_t now = nowNs();
        if (now >= deadline) {
            // Count one failed refresh, not one per retry iteration.
            if (!last_error.ok()) {
                if (last_error.IsRpcServiceError() ||
                    last_error.IsRpcConnectionError()) {
                    ++rpc_failures_;
                } else {
                    ++malformed_;
                }
            }
            return last_error;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

Status ControlPlaneProbePeerSource::buildDescriptors(
    const ProbeBootstrapResponse& local,
    std::vector<ProbePeerDescriptor>& descriptors) {
    std::map<std::string, size_t> local_native_count;
    for (const auto& endpoint : local.endpoints) {
        ++local_native_count[endpoint.key.native_device_name];
    }

    descriptors.clear();
    for (const auto& [peer, state] : peers_) {
        if (!state.valid || state.segment_id == LOCAL_SEGMENT_ID) continue;
        // Pairing is per peer: the same (native, eid) may exist on several
        // peers, so a global map would silently pair one peer's endpoint to
        // another peer's edge.
        std::map<std::pair<std::string, uint32_t>, const ProbeEndpointDesc*>
            peer_by_key;
        std::map<std::string, size_t> peer_native_count;
        for (const auto& endpoint : state.endpoints) {
            peer_by_key[{endpoint.key.native_device_name,
                         endpoint.key.eid_index}] = &endpoint;
            ++peer_native_count[endpoint.key.native_device_name];
        }
        for (const auto& local_endpoint : local.endpoints) {
            const auto exact =
                peer_by_key.find({local_endpoint.key.native_device_name,
                                  local_endpoint.key.eid_index});
            const ProbeEndpointDesc* remote = nullptr;
            if (exact != peer_by_key.end()) {
                remote = exact->second;
            } else if (local_native_count[local_endpoint.key
                                              .native_device_name] == 1 &&
                       peer_native_count[local_endpoint.key
                                             .native_device_name] == 1) {
                for (const auto& endpoint : state.endpoints) {
                    if (endpoint.key.native_device_name ==
                        local_endpoint.key.native_device_name) {
                        remote = &endpoint;
                        break;
                    }
                }
            }
            if (remote == nullptr) {
                ++no_endpoint_;
                continue;
            }
            ProbePeerDescriptor descriptor;
            descriptor.local_nic = local_endpoint.local_nic;
            descriptor.peer_segment_id = state.segment_id;
            descriptor.remote_nic = remote->local_nic;
            descriptor.peer_nic_path = peer;
            descriptor.topology_epoch = state.topology_epoch;
            descriptor.target_epoch = remote->target_epoch;
            descriptor.area_descriptor.schema_version =
                remote->area.schema_version;
            descriptor.area_descriptor.urma_api_version =
                remote->area.urma_api_version;
            descriptor.area_descriptor.urma_abi_size =
                remote->area.urma_abi_size;
            descriptor.area_descriptor.hex = remote->area.hex;
            descriptor.area_length = remote->area_length;
            descriptor.area_epoch = remote->area_epoch;
            descriptor.area_token = remote->area_token;
            descriptor.remote_jetty.eid = remote->probe_jetty.eid;
            descriptor.remote_jetty.id = remote->probe_jetty.id;
            descriptor.remote_jetty.uasid = remote->probe_jetty.uasid;
            descriptor.remote_jetty.token = remote->probe_jetty.token;
            descriptor.node_gate_ok = remote->node_gate_ok;
            descriptor.port_gate_ok = remote->port_gate_ok;
            descriptor.profile_gate_ok = remote->profile_gate_ok;
            descriptor.endpoint_generation = remote->endpoint_generation;
            descriptors.push_back(std::move(descriptor));
        }
    }
    return descriptors.empty()
               ? Status::DeviceNotFound(
                     "no probe edge could be paired with any peer" LOC_MARK)
               : Status::OK();
}

Status ControlPlaneProbePeerSource::snapshot(
    std::vector<ProbePeerDescriptor>& descriptors) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!local_ || !resolver_ || !client_) {
        return Status::InvalidArgument(
            "probe control-plane source is incomplete" LOC_MARK);
    }
    ProbeBootstrapResponse local;
    CHECK_STATUS(local_->snapshot(local));
    if (local.node_id.empty() || local.endpoints.empty()) {
        return Status::DeviceNotFound(
            "local probe provider exported no usable endpoint" LOC_MARK);
    }

    const uint64_t now = nowNs();
    local_node_id_ = local.node_id;
    for (const auto& peer : graph_.peer_segments) {
        if (peer.empty()) continue;
        if (peer == local.node_id) {
            LOG_EVERY_N(WARNING, 100)
                << "UB probe: peer_segments contains the local node; ignored";
            continue;
        }
        auto& state = peers_[peer];
        const bool fresh =
            state.valid && now - state.fetched_ns < params_.descriptor_ttl_ms *
                                                        ProbeParams::kNsPerMs;
        if (fresh) continue;
        // Re-resolve every TTL: a peer restart may move its control-plane
        // address while the segment name stays the same.
        state.rpc_server_addr.clear();
        auto status =
            refreshPeer(peer, state, now, /*wait_initial=*/!state.valid);
        if (!status.ok()) {
            if (!state.valid) {
                LOG_EVERY_N(WARNING, 100)
                    << "UB probe: peer bootstrap failed: peer=" << peer
                    << " status=" << status.ToString();
            } else {
                LOG_EVERY_N(WARNING, 100)
                    << "UB probe: keeping stale peer bootstrap: peer=" << peer
                    << " status=" << status.ToString();
            }
        }
    }
    if (peers_.empty()) {
        return Status::DeviceNotFound("probe peer_segments is empty" LOC_MARK);
    }
    return buildDescriptors(local, descriptors);
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
