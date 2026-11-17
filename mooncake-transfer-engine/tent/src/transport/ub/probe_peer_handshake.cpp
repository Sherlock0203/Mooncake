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

#include "tent/transport/ub/probe_peer_handshake.h"

#include <chrono>
#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace ub {
namespace {

// Placeholder until the bootstrap probe-area token is frozen. The peer uses the
// token advertised in its descriptor; a zero token means "default".
constexpr uint32_t kDefaultProbeAreaToken = 0xACFE;

}  // namespace

HandshakeProbePeerDirectory::HandshakeProbePeerDirectory(
    std::shared_ptr<UrmaAdapter> adapter,
    std::map<Topology::NicID, ContextPtr> contexts,
    ProbePeerDescriptorSource* source)
    : adapter_(std::move(adapter)),
      contexts_(std::move(contexts)),
      source_(source) {}

HandshakeProbePeerDirectory::~HandshakeProbePeerDirectory() {
    (void)shutdown();
}

Status HandshakeProbePeerDirectory::snapshotEdges(
    std::vector<ProbeEdgeSupply>& edges) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) {
        return Status::InvalidArgument("probe peer directory is shut down");
    }
    auto status = rebuildLocked(nullptr, /*defer_commit=*/false);
    if (!status.ok()) return status;
    edges.clear();
    edges.reserve(cache_.size());
    for (const auto& [key, cached] : cache_) {
        edges.push_back(cached.edge);
    }
    return Status::OK();
}

Status HandshakeProbePeerDirectory::refresh(std::vector<ProbeEvent>& events) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) {
        return Status::InvalidArgument("probe peer directory is shut down");
    }
    if (has_pending_) {
        // The provider must commit the previous batch before starting a new
        // diff. Silently discarding pending state can desynchronize the PLM
        // from the cache.
        return Status::TooManyRequests("probe refresh is pending commit");
    }
    return rebuildLocked(&events, /*defer_commit=*/true);
}

Status HandshakeProbePeerDirectory::rebuildLocked(
    std::vector<ProbeEvent>* events, bool defer_commit) {
    if (!adapter_ || source_ == nullptr) {
        return Status::InvalidArgument(
            "probe peer directory is not configured");
    }
    std::vector<ProbePeerDescriptor> descriptors;
    auto status = source_->snapshot(descriptors);
    if (!status.ok()) return status;

    // Validate the whole snapshot before mutating the cache: a malformed
    // descriptor must not leave a half-imported cache behind.
    std::vector<const ProbePeerDescriptor*> accepted;
    accepted.reserve(descriptors.size());
    std::vector<EdgeKey> keys;
    keys.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        if (descriptor.schema_version != kProbePeerDescriptorSchemaVersion) {
            ++unsupported_peer_count_;
            continue;
        }
        if (descriptor.local_nic < 0 ||
            descriptor.peer_segment_id == LOCAL_SEGMENT_ID ||
            descriptor.remote_nic < 0 || descriptor.topology_epoch == 0 ||
            descriptor.target_epoch == 0 || descriptor.area_length == 0 ||
            descriptor.area_epoch == 0 ||
            descriptor.area_descriptor.hex.empty()) {
            return Status::InvalidArgument("malformed probe peer descriptor");
        }
        if (contexts_.find(descriptor.local_nic) == contexts_.end() ||
            !contexts_.at(descriptor.local_nic)) {
            return Status::InvalidArgument(
                "probe peer descriptor references an unknown local port");
        }
        const EdgeKey key{descriptor.local_nic, descriptor.peer_segment_id,
                          descriptor.remote_nic, descriptor.topology_epoch};
        for (const auto& seen : keys) {
            if (seen < key || key < seen) continue;
            return Status::InvalidArgument("duplicate probe peer descriptor");
        }
        keys.push_back(key);
        accepted.push_back(&descriptor);
    }

    const uint64_t now_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    std::vector<RemoteSegmentPtr> event_new;
    auto event_for = [&](ProbeEventKind kind, const ProbeEdgeSupply& edge,
                         const RemoteSegmentPtr* new_segment = nullptr) {
        if (events == nullptr) return;
        ProbeEvent event;
        event.kind = kind;
        event.local_nic = edge.version.path.local_nic;
        event.edge = edge;
        events->push_back(std::move(event));
        event_new.push_back(new_segment ? *new_segment : RemoteSegmentPtr{});
    };

    std::vector<RetiredImport> retired;
    std::vector<RemoteSegmentPtr> new_segments;
    std::map<EdgeKey, CachedEdge> next;
    for (const ProbePeerDescriptor* descriptor_ptr : accepted) {
        const auto& descriptor = *descriptor_ptr;
        const auto& context = contexts_.at(descriptor.local_nic);
        const EdgeKey key{descriptor.local_nic, descriptor.peer_segment_id,
                          descriptor.remote_nic, descriptor.topology_epoch};
        auto existing = cache_.find(key);
        if (existing != cache_.end() &&
            existing->second.descriptor_hex == descriptor.area_descriptor.hex) {
            // Same probe page: reuse the import and refresh volatile fields.
            ProbeEdgeSupply edge = existing->second.edge;
            edge.version.target_epoch = descriptor.target_epoch;
            edge.area_length = descriptor.area_length;
            edge.area_epoch = descriptor.area_epoch;
            edge.remote_jetty = descriptor.remote_jetty;
            edge.node_gate_ok = descriptor.node_gate_ok;
            edge.port_gate_ok = descriptor.port_gate_ok;
            edge.profile_gate_ok = descriptor.profile_gate_ok;
            edge.endpoint_generation = descriptor.endpoint_generation;
            if (events != nullptr) {
                if (existing->second.target_epoch != descriptor.target_epoch) {
                    event_for(ProbeEventKind::kTargetEpochChanged, edge);
                } else if (existing->second.area_epoch !=
                           descriptor.area_epoch) {
                    event_for(ProbeEventKind::kAreaReplaced, edge);
                } else if (existing->second.gates.node_ok !=
                               descriptor.node_gate_ok ||
                           existing->second.gates.port_ok !=
                               descriptor.port_gate_ok ||
                           existing->second.gates.profile_ok !=
                               descriptor.profile_gate_ok) {
                    ProbeEvent event;
                    event.kind = ProbeEventKind::kGateChanged;
                    event.local_nic = descriptor.local_nic;
                    event.gate = GateBits{descriptor.node_gate_ok,
                                          descriptor.port_gate_ok,
                                          descriptor.profile_gate_ok};
                    events->push_back(std::move(event));
                    event_new.push_back(RemoteSegmentPtr{});
                }
            }
            next.emplace(
                key, CachedEdge{std::move(edge), descriptor.area_descriptor.hex,
                                descriptor.target_epoch, descriptor.area_epoch,
                                GateBits{descriptor.node_gate_ok,
                                         descriptor.port_gate_ok,
                                         descriptor.profile_gate_ok}});
            continue;
        }

        SegmentOptions options;
        options.access = SEGMENT_ACCESS_WRITE;
        options.token = descriptor.area_token != 0 ? descriptor.area_token
                                                   : kDefaultProbeAreaToken;
        RemoteSegmentPtr segment;
        auto import_status = adapter_->importRemoteSegment(
            context, descriptor.area_descriptor, options, segment);
        if (!import_status.ok() || !segment || segment->address() == 0 ||
            segment->length() == 0) {
            ++import_failure_count_;
            LOG_EVERY_N(WARNING, 100)
                << "UB probe peer area import failed: local_nic="
                << descriptor.local_nic
                << " peer=" << descriptor.peer_segment_id
                << " remote_nic=" << descriptor.remote_nic
                << " status=" << import_status.ToString();
            if (segment) retired.push_back(RetiredImport{std::move(segment)});
            if (existing != cache_.end()) {
                next.emplace(key, existing->second);
            }
            continue;
        }

        new_segments.push_back(segment);
        ProbeEdgeSupply edge;
        edge.version = PathVersion{
            ProbePathKey{descriptor.local_nic, descriptor.peer_segment_id,
                         descriptor.remote_nic},
            descriptor.topology_epoch, descriptor.target_epoch};
        edge.peer =
            ProbePeerKey{descriptor.peer_segment_id, descriptor.peer_nic_path};
        edge.area_segment = segment;
        edge.area_address = segment->address();
        edge.area_length = descriptor.area_length;
        edge.area_epoch = descriptor.area_epoch;
        edge.remote_jetty = descriptor.remote_jetty;
        edge.node_gate_ok = descriptor.node_gate_ok;
        edge.port_gate_ok = descriptor.port_gate_ok;
        edge.profile_gate_ok = descriptor.profile_gate_ok;
        edge.endpoint_generation = descriptor.endpoint_generation;

        if (existing != cache_.end()) {
            if (existing->second.edge.area_segment) {
                retired.push_back(
                    RetiredImport{existing->second.edge.area_segment});
            }
            if (events != nullptr) {
                // Same PathVersion + new area: kAreaReplaced. A target change
                // uses kTargetEpochChanged so the PLM can replace the primary
                // key in one event.
                if (existing->second.target_epoch != descriptor.target_epoch) {
                    event_for(ProbeEventKind::kTargetEpochChanged, edge,
                              &segment);
                } else {
                    event_for(ProbeEventKind::kAreaReplaced, edge, &segment);
                }
            }
        } else if (events != nullptr) {
            event_for(ProbeEventKind::kEdgeAdded, edge, &segment);
        }
        ++import_count_;
        next.emplace(
            key, CachedEdge{
                     std::move(edge), descriptor.area_descriptor.hex,
                     descriptor.target_epoch, descriptor.area_epoch,
                     GateBits{descriptor.node_gate_ok, descriptor.port_gate_ok,
                              descriptor.profile_gate_ok}});
    }

    // Edges that disappeared from the peer snapshot lose their import.
    for (auto& [key, cached] : cache_) {
        if (next.find(key) != next.end()) continue;
        if (events != nullptr) {
            ProbeEvent event;
            event.kind = ProbeEventKind::kEdgeRemoved;
            event.local_nic = key.local_nic;
            event.version = cached.edge.version;
            events->push_back(std::move(event));
            event_new.push_back(RemoteSegmentPtr{});
        }
        if (cached.edge.area_segment) {
            retired.push_back(RetiredImport{cached.edge.area_segment});
        }
    }
    if (defer_commit) {
        pending_cache_ = std::move(next);
        pending_retired_ = std::move(retired);
        pending_new_ = std::move(new_segments);
        pending_event_new_ = std::move(event_new);
        if (events != nullptr) pending_events_ = *events;
        has_pending_ = true;
    } else {
        cache_ = std::move(next);
        retired_.insert(retired_.end(),
                        std::make_move_iterator(retired.begin()),
                        std::make_move_iterator(retired.end()));
        releaseRetiredLocked(false);
    }
    return Status::OK();
}

Status HandshakeProbePeerDirectory::commitRefresh(bool success,
                                                  size_t delivered_events) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!has_pending_) return Status::OK();

    auto next = success ? pending_cache_ : cache_;
    if (!success) {
        const size_t delivered =
            std::min(delivered_events, pending_events_.size());
        for (size_t i = 0; i < delivered; ++i) {
            const auto& event = pending_events_[i];
            if (event.edge.has_value()) {
                const auto& version = event.edge->version;
                const EdgeKey key{
                    version.path.local_nic, version.path.segment_id,
                    version.path.remote_nic, version.topology_epoch};
                auto pending = pending_cache_.find(key);
                if (pending != pending_cache_.end()) {
                    next[key] = pending->second;
                }
            } else if (event.version.has_value()) {
                const auto& version = *event.version;
                next.erase(
                    EdgeKey{version.path.local_nic, version.path.segment_id,
                            version.path.remote_nic, version.topology_epoch});
            } else if (event.gate.has_value()) {
                for (auto& [key, cached] : next) {
                    if (key.local_nic != event.local_nic) continue;
                    cached.gates = *event.gate;
                    cached.edge.node_gate_ok = event.gate->node_ok;
                    cached.edge.port_gate_ok = event.gate->port_ok;
                    cached.edge.profile_gate_ok = event.gate->profile_ok;
                }
            }
        }
    }

    // Retire old imports only for edges whose area was actually replaced or
    // removed in the committed prefix.
    for (const auto& [key, old] : cache_) {
        auto committed = next.find(key);
        if (committed != next.end() &&
            committed->second.edge.area_segment == old.edge.area_segment) {
            continue;
        }
        if (old.edge.area_segment) {
            retired_.push_back(RetiredImport{old.edge.area_segment});
        }
    }
    cache_ = std::move(next);

    // Imports belonging to undelivered events were never applied to the PLM;
    // detach the pending references before unimporting them.
    std::vector<RemoteSegmentPtr> undelivered;
    if (!success) {
        const size_t first_undelivered =
            std::min(delivered_events, pending_event_new_.size());
        for (size_t i = first_undelivered; i < pending_event_new_.size(); ++i) {
            if (pending_event_new_[i]) {
                undelivered.push_back(std::move(pending_event_new_[i]));
            }
        }
    }
    pending_events_.clear();
    pending_new_.clear();
    pending_event_new_.clear();
    pending_cache_.clear();
    pending_retired_.clear();
    has_pending_ = false;

    for (auto& segment : undelivered) {
        auto status = adapter_->unimportRemoteSegment(segment);
        if (!status.ok() && segment) {
            retired_.push_back(RetiredImport{std::move(segment)});
        }
    }
    releaseRetiredLocked(false);
    return Status::OK();
}

void HandshakeProbePeerDirectory::releaseRetiredLocked(bool force) {
    for (auto it = retired_.begin(); it != retired_.end();) {
        // Never unimport a segment that the PLM/record still holds. Shutdown
        // forces the release after posting has drained.
        if (!force && it->segment && it->segment.use_count() > 1) {
            ++it;
            continue;
        }
        if (it->segment) (void)adapter_->unimportRemoteSegment(it->segment);
        it = retired_.erase(it);
    }
}

Status HandshakeProbePeerDirectory::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) return Status::OK();
    Status first_error = Status::OK();
    for (auto& segment : pending_event_new_) {
        if (segment) retired_.push_back(RetiredImport{std::move(segment)});
    }
    pending_events_.clear();
    pending_new_.clear();
    pending_event_new_.clear();
    pending_cache_.clear();
    pending_retired_.clear();
    has_pending_ = false;
    std::vector<RemoteSegmentPtr> released;
    released.reserve(cache_.size());
    for (auto& [key, cached] : cache_) {
        if (cached.edge.area_segment) {
            released.push_back(std::move(cached.edge.area_segment));
        }
    }
    cache_.clear();
    for (auto& segment : released) {
        auto status = adapter_->unimportRemoteSegment(segment);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    for (auto& retired : retired_) {
        if (!retired.segment) continue;
        auto status = adapter_->unimportRemoteSegment(retired.segment);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    retired_.clear();
    shut_down_ = true;
    return first_error;
}

size_t HandshakeProbePeerDirectory::unsupportedPeerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return unsupported_peer_count_;
}

size_t HandshakeProbePeerDirectory::importFailureCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return import_failure_count_;
}

size_t HandshakeProbePeerDirectory::importCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return import_count_;
}

size_t HandshakeProbePeerDirectory::cachedEdgeCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.size();
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
