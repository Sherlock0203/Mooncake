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

#include "tent/transport/ub/probe_link_manager.h"

#include <algorithm>
#include <limits>
#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace ub {
namespace {

// A rejected probe is retried soon, but never in a tight loop: the scheduler
// must not spin on a provider that keeps refusing the WR (design §8.6).
constexpr uint64_t kPostRejectedRetryCapNs = 100ULL * 1000 * 1000;
// Backoff doubling saturates here; interval_ms << 16 already exceeds a day.
constexpr uint64_t kMaxBackoffShift = 16;

EntryState mapCampaignOutcome(CompletionCategory category) {
    switch (category) {
        case CompletionCategory::SUCCESS:
            return EntryState::kOk;
        case CompletionCategory::REMOTE_PATH_ERROR:
        case CompletionCategory::TIMEOUT:
            return EntryState::kFail;
        case CompletionCategory::LOCAL_DEVICE_ERROR:
        case CompletionCategory::ENDPOINT_ERROR:
        case CompletionCategory::MEMORY_ERROR:
        case CompletionCategory::UNKNOWN_ERROR:
        default:
            return EntryState::kCanceled;
    }
}

}  // namespace

ProbeLinkManager::ProbeLinkManager(ProbeParams params, Clock clock)
    : params_(std::move(params)), clock_(std::move(clock)) {}

uint64_t ProbeLinkManager::normalizedNow(uint64_t now_ns) const {
    // 0 means "read the injected clock". A virtual clock must therefore start
    // at a non-zero tick.
    if (now_ns != 0) return now_ns;
    if (clock_) {
        const uint64_t now = clock_();
        if (now != 0) return now;
    }
    return steadyNowNs();
}

bool ProbeLinkManager::initialized() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return initialized_;
}

Status ProbeLinkManager::initialize(const ProbeGraph& graph) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initialized_) {
        return Status::InvalidArgument(
            "probe link manager is already initialized");
    }
    if (!params_.valid()) {
        return Status::InvalidArgument("invalid probe parameters");
    }
    if (graph.graph_epoch == 0) {
        return Status::InvalidArgument("probe graph epoch must be non-zero");
    }
    if (graph.ports.empty()) {
        return Status::InvalidArgument("probe graph supplies no local port");
    }
    if (graph.edges.empty()) {
        // Fail fast instead of silently degrading to "no probing" (design
        // §12.3): enabled probing with an empty adjacency is a supply bug.
        return Status::InvalidArgument(
            "probe graph supplies no adjacency edge");
    }

    for (const auto& supply : graph.ports) {
        if (supply.local_nic < 0) {
            return Status::InvalidArgument(
                "probe port has an invalid local NIC");
        }
        if (ports_.find(supply.local_nic) != ports_.end()) {
            return Status::InvalidArgument("duplicate probe port supply");
        }
        if (supply.jetty_generation == 0) {
            return Status::InvalidArgument(
                "probe port jetty generation must be non-zero");
        }
        if (!supply.probe_jetty || !supply.probe_jetty->valid()) {
            return Status::InvalidArgument("probe port has no probe Jetty");
        }
        if (!supply.source_segment || supply.source_length == 0) {
            return Status::InvalidArgument("probe port has no source segment");
        }
        if (supply.source_generation == 0) {
            return Status::InvalidArgument(
                "probe port source generation must be non-zero");
        }
        if (params_.jfc_mode == ProbeJfcMode::kDedicated && !supply.probe_jfc) {
            return Status::InvalidArgument(
                "dedicated probe mode requires a probe JFC");
        }
        PortRuntime runtime;
        runtime.supply = supply;
        runtime.generation = supply.jetty_generation;
        runtime.slot_generation = supply.jetty_generation;
        runtime.handle_valid = true;
        ports_.emplace(supply.local_nic, std::move(runtime));
    }

    for (const auto& supply : graph.edges) {
        if (!supply.version.valid()) {
            return Status::InvalidArgument(
                "probe edge has an invalid path version");
        }
        if (!supply.peer.valid()) {
            return Status::InvalidArgument("probe edge has an invalid peer");
        }
        auto port = ports_.find(supply.version.path.local_nic);
        if (port == ports_.end()) {
            return Status::InvalidArgument(
                "probe edge references a local port without supply");
        }
        if (!supply.area_segment || supply.area_length == 0 ||
            supply.area_epoch == 0) {
            return Status::InvalidArgument(
                "probe edge has an invalid area supply");
        }
        if (edges_.find(supply.version) != edges_.end()) {
            return Status::InvalidArgument("duplicate probe edge version");
        }
        for (const auto& [version, other] : edges_) {
            if (version.path == supply.version.path) {
                return Status::InvalidArgument(
                    "multiple path versions for one adjacency edge");
            }
        }
        edges_.emplace(supply.version, supply);
        PathRuntime runtime;
        runtime.peer = supply.peer;
        paths_.emplace(supply.version, std::move(runtime));
        port->second.paths.push_back(supply.version);
        ++stats_.path_state_eligible;
    }

    graph_epoch_ = graph.graph_epoch;
    last_applied_seq_ = 0;
    initialized_ = true;
    return Status::OK();
}

ProbeLinkManager::PortRuntime* ProbeLinkManager::findPortLocked(
    Topology::NicID local_nic) {
    auto it = ports_.find(local_nic);
    return it == ports_.end() ? nullptr : &it->second;
}

ProbeLinkManager::PathRuntime* ProbeLinkManager::findPathLocked(
    const PathVersion& version) {
    auto it = paths_.find(version);
    return it == paths_.end() ? nullptr : &it->second;
}

ProbeEdgeSupply* ProbeLinkManager::findEdgeLocked(const PathVersion& version) {
    auto it = edges_.find(version);
    return it == edges_.end() ? nullptr : &it->second;
}

void ProbeLinkManager::emitEffects(const Effects& effects) {
    // Never hold the PLM mutex across a sink call (design §14.1-6).
    if (effects.retire_lane && report_ != nullptr) {
        report_->retireProbeLane(effects.lane, effects.generation,
                                 effects.error_epoch, effects.path,
                                 effects.reason);
    }
    if ((effects.retire_lane || effects.bump) && wake_) wake_();
}

bool ProbeLinkManager::externalGateOk(const ProbeEdgeSupply& edge) {
    return edge.node_gate_ok && edge.port_gate_ok && edge.profile_gate_ok;
}

void ProbeLinkManager::setHealthLocked(PathRuntime& runtime, LinkHealth health,
                                       Effects& effects) {
    if (runtime.health == health) return;
    runtime.health = health;
    switch (health) {
        case LinkHealth::kEligibleUnverified:
            ++stats_.path_state_eligible;
            break;
        case LinkHealth::kHealthy:
            ++stats_.path_state_healthy;
            break;
        case LinkHealth::kIsolated:
            ++stats_.path_state_isolated;
            break;
        case LinkHealth::kRecovering:
            ++stats_.path_state_recovering;
            break;
        case LinkHealth::kDisabled:
            ++stats_.path_state_disabled;
            break;
    }
    // Path-bucket availability changes for health changes that alter business
    // eligibility, plus disable. ELIGIBLE -> HEALTHY does not change
    // eligibility, and ISOLATED -> RECOVERING keeps the path unusable, so
    // neither wakes deferred business slices (design §6.2).
    if (health == LinkHealth::kHealthy || health == LinkHealth::kIsolated ||
        health == LinkHealth::kDisabled) {
        effects.bump = true;
    }
}

void ProbeLinkManager::publishHealthyLocked(PathRuntime& runtime,
                                            Effects& effects) {
    runtime.next_probe_ns = 0;
    setHealthLocked(runtime, LinkHealth::kHealthy, effects);
}

uint64_t ProbeLinkManager::backoffDelayLocked(
    uint64_t consecutive_failures) const {
    const uint64_t base = params_.intervalNs();
    const uint64_t shift =
        consecutive_failures == 0
            ? 0
            : std::min<uint64_t>(consecutive_failures - 1, kMaxBackoffShift);
    uint64_t delay = std::numeric_limits<uint64_t>::max();
    if (base <= (std::numeric_limits<uint64_t>::max() >> shift)) {
        delay = base << shift;
    }
    return std::min(delay, params_.backoffMaxNs());
}

bool ProbeLinkManager::dueLocked(const PathVersion& version,
                                 const PathRuntime& runtime,
                                 uint64_t now_ns) const {
    if (runtime.task == TaskState::kInflight) return false;
    // DISABLED paths are not recovered by probing: they wait for a new graph
    // version (design §6.1).
    if (runtime.health == LinkHealth::kDisabled) return false;
    if (runtime.task == TaskState::kQueued) {
        // A queued campaign entry wins, but only while the campaign itself
        // still expects this version.
        auto campaign = campaigns_.find(runtime.peer);
        if (campaign == campaigns_.end()) return false;
        auto entry = campaign->second.entries.find(version);
        if (entry == campaign->second.entries.end() ||
            entry->second.state != EntryState::kQueued) {
            return false;
        }
    } else if (runtime.health != LinkHealth::kIsolated &&
               runtime.health != LinkHealth::kRecovering) {
        // ELIGIBLE_UNVERIFIED/HEALTHY never run periodic probes; they are only
        // verified as part of a campaign.
        return false;
    }
    return now_ns >= runtime.next_probe_ns;
}

bool ProbeLinkManager::peerBudgetAvailableLocked(
    const ProbePeerKey& peer) const {
    auto it = peer_inflight_.find(peer);
    return it == peer_inflight_.end() || it->second < params_.peer_budget;
}

bool ProbeLinkManager::schedulableLocked(const PathVersion& version,
                                         const PathRuntime& runtime,
                                         uint64_t now_ns) const {
    if (runtime.pending) return false;
    if (!dueLocked(version, runtime, now_ns)) return false;
    return peerBudgetAvailableLocked(runtime.peer);
}

ProbeAction ProbeLinkManager::makeActionLocked(
    const PortRuntime& port, const PathVersion& version,
    const PathRuntime& runtime) const {
    ProbeAction action;
    action.version = version;
    action.peer = runtime.peer;
    action.local_nic = port.supply.local_nic;
    action.jetty_generation = port.generation;
    auto edge = edges_.find(version);
    if (edge != edges_.end()) {
        action.area_epoch = edge->second.area_epoch;
        action.remote_jetty = edge->second.remote_jetty;
        action.area_segment = edge->second.area_segment;
        action.area_address = edge->second.area_address;
        action.area_length = edge->second.area_length;
    }
    action.due_ns = runtime.next_probe_ns;
    action.probe_jetty = port.supply.probe_jetty;
    action.source_address = port.supply.source_address;
    action.source_length = port.supply.source_length;
    return action;
}

std::vector<ProbeAction> ProbeLinkManager::pendingActions(uint64_t now_ns) {
    std::vector<ProbeAction> actions;
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) return actions;
    const uint64_t now = normalizedNow(now_ns);

    for (auto& [local_nic, port] : ports_) {
        if (!port.handle_valid) {
            for (const auto& version : port.paths) {
                auto runtime = paths_.find(version);
                if (runtime != paths_.end() &&
                    dueLocked(version, runtime->second, now)) {
                    ++stats_.probe_skipped_broken;
                    break;
                }
            }
            continue;
        }
        // One probe per port including the "unknown responsibility" window: a
        // non-zero slot means this port already has an outstanding WR.
        if (port.slot_token != 0) continue;
        const size_t count = port.paths.size();
        if (count == 0) continue;
        const size_t start = port.rr_cursor % count;
        for (size_t offset = 0; offset < count; ++offset) {
            const size_t index = (start + offset) % count;
            const PathVersion& version = port.paths[index];
            auto runtime = paths_.find(version);
            if (runtime == paths_.end()) continue;
            if (!dueLocked(version, runtime->second, now)) continue;
            // Due but not schedulable: count why and let the next candidate
            // use the free slot, so one Pending path cannot starve its port.
            if (runtime->second.pending) {
                ++stats_.probe_skipped_pending;
                continue;
            }
            if (!peerBudgetAvailableLocked(runtime->second.peer)) {
                ++stats_.probe_skipped_peer_budget;
                continue;
            }
            port.rr_cursor = index + 1;
            if (port.rr_cursor >= count) ++stats_.probe_rr_round;
            actions.push_back(makeActionLocked(port, version, runtime->second));
            break;
        }
    }
    return actions;
}

std::shared_ptr<ProbeRecord> ProbeLinkManager::beginPost(
    const ProbeAction& action, uint64_t token, uint64_t now_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    const uint64_t now = normalizedNow(now_ns);
    if (!initialized_ || token == 0 || !isProbeToken(token)) return nullptr;
    if (inflight_.find(token) != inflight_.end() ||
        orphans_.find(token) != orphans_.end()) {
        return nullptr;
    }

    PortRuntime* port = findPortLocked(action.local_nic);
    if (port == nullptr) return nullptr;
    if (!port->handle_valid || port->slot_token != 0) return nullptr;
    if (port->generation != action.jetty_generation) return nullptr;

    const PathVersion& version = action.version;
    PathRuntime* runtime = findPathLocked(version);
    ProbeEdgeSupply* edge = findEdgeLocked(version);
    if (runtime == nullptr || edge == nullptr) return nullptr;
    if (edge->area_epoch != action.area_epoch) return nullptr;
    if (!schedulableLocked(version, *runtime, now)) return nullptr;

    // Campaign alignment happens here, under the same lock that publishes the
    // token: no separate "snapshot re-check" is needed afterwards (design
    // §7.3).
    uint64_t campaign_id = 0;
    uint64_t campaign_epoch = 0;
    if (runtime->task == TaskState::kQueued) {
        auto campaign = campaigns_.find(runtime->peer);
        if (campaign == campaigns_.end()) return nullptr;
        auto entry = campaign->second.entries.find(version);
        if (entry == campaign->second.entries.end() ||
            entry->second.state != EntryState::kQueued) {
            return nullptr;
        }
        entry->second.state = EntryState::kInflight;
        entry->second.entry_epoch = campaign->second.epoch;
        campaign_id = campaign->second.id;
        campaign_epoch = campaign->second.epoch;
    }

    ++runtime->probe_seq;
    runtime->task = TaskState::kInflight;
    runtime->inflight_seq = runtime->probe_seq;

    ProbeKey key;
    key.version = version;
    key.fault_epoch = runtime->fault_epoch;
    key.campaign_id = campaign_id;
    key.campaign_epoch = campaign_epoch;
    key.probe_seq = runtime->probe_seq;
    key.jetty_generation = port->generation;
    key.area_epoch = edge->area_epoch;

    auto record = std::make_shared<ProbeRecord>();
    record->token = token;
    record->key = key;
    record->peer = runtime->peer;
    record->posted_ns = now;
    record->deadline_ns = now + params_.timeoutNs();
    record->state = RecordState::kPosting;
    record->source = port->supply.source_segment;
    record->area = edge->area_segment;
    record->credit = 1;

    port->slot_token = token;
    port->slot_generation = port->generation;
    ++port->inflight;
    stats_.probe_inflight_per_port =
        std::max<uint64_t>(stats_.probe_inflight_per_port, port->inflight);
    ++peer_inflight_[runtime->peer];
    inflight_.emplace(token, record);
    ++stats_.probe_scheduled;
    return record;
}

void ProbeLinkManager::onPostResult(const std::shared_ptr<ProbeRecord>& record,
                                    ProbePostOutcome outcome, uint64_t now_ns) {
    if (record == nullptr) return;
    const uint64_t now = normalizedNow(now_ns);
    LocalSegmentPtr source;
    RemoteSegmentPtr area;
    Effects effects;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        switch (outcome) {
            case ProbePostOutcome::kAccepted:
                // The completion may legally have arrived before the post
                // returned; the WR was still accepted, so count it either way.
                ++stats_.probe_posted;
                if (record->settled.load(std::memory_order_acquire)) return;
                record->state = RecordState::kAwaiting;
                record->posted_ns = now;
                record->deadline_ns = now + params_.timeoutNs();
                return;
            case ProbePostOutcome::kUnknown:
                // The provider could not prove non-acceptance: treat it as
                // accepted, never resend (design §8.6).
                ++stats_.probe_posted;
                ++stats_.probe_post_unknown;
                if (record->settled.load(std::memory_order_acquire)) return;
                record->state = RecordState::kAwaiting;
                record->posted_ns = now;
                record->deadline_ns = now + params_.timeoutNs();
                return;
            case ProbePostOutcome::kRejected:
                break;
        }

        if (record->settled.exchange(true)) {
            // A real completion already claimed this record even though the
            // provider reported "not accepted": the resource is
            // self-contradictory, so retire the probe lane without rolling
            // back health (design §8.6).
            ++stats_.probe_post_conflict;
            PortRuntime* port =
                findPortLocked(record->key.version.path.local_nic);
            if (port != nullptr) {
                laneRetireLocked(*port, now, record->key.version,
                                 Status::InternalError(
                                     "probe post rejected after completion"),
                                 effects);
            }
        } else {
            ++stats_.probe_post_rejected;
            record->state = RecordState::kRejected;
            const uint64_t token = record->token;
            inflight_.erase(token);
            PortRuntime* port =
                findPortLocked(record->key.version.path.local_nic);
            if (port != nullptr) releaseSlotLocked(*port, *record);
            releaseBudgetLocked(record->peer);
            PathRuntime* runtime = findPathLocked(record->key.version);
            if (runtime != nullptr) {
                runtime->next_probe_ns =
                    now +
                    std::min(params_.intervalNs(), kPostRejectedRetryCapNs);
            }
            // Rejection is not a path fault: hand the task slot back so a
            // campaign entry returns to kQueued instead of hanging in
            // kInflight.
            releaseTaskSlotLocked(*record);
            source = std::move(record->source);
            area = std::move(record->area);
        }
    }

    source.reset();
    area.reset();
    emitEffects(effects);
}

uint64_t ProbeLinkManager::nextDeadline(uint64_t now_ns) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const uint64_t now = normalizedNow(now_ns);
    uint64_t deadline = 0;
    auto consider = [&deadline](uint64_t candidate) {
        if (deadline == 0 || candidate < deadline) deadline = candidate;
    };
    for (const auto& [token, record] : inflight_) {
        if (record->deadline_ns != 0) consider(record->deadline_ns);
    }
    for (const auto& [local_nic, port] : ports_) {
        if (!port.handle_valid || port.slot_token != 0) continue;
        for (const auto& version : port.paths) {
            auto runtime = paths_.find(version);
            if (runtime == paths_.end()) continue;
            if (!schedulableLocked(version, runtime->second, now)) continue;
            consider(std::max(runtime->second.next_probe_ns, now));
        }
    }
    return deadline;
}

bool ProbeLinkManager::onProbeCompletion(const Completion& completion,
                                         uint64_t now_ns) {
    if (!isProbeToken(completion.token)) return false;
    const uint64_t now = normalizedNow(now_ns);
    LocalSegmentPtr source;
    RemoteSegmentPtr area;
    Effects effects;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto inflight = inflight_.find(completion.token);
        if (inflight == inflight_.end()) {
            // A late completion for an orphan only settles the remaining
            // hardware responsibility: no health change, no campaign progress
            // (design §9.2/§9.4).
            auto orphan = orphans_.find(completion.token);
            if (orphan == orphans_.end()) {
                ++stats_.probe_unknown_token;
                return true;
            }
            auto record = orphan->second;
            if (!record->settled.exchange(true)) {
                orphans_.erase(orphan);
                record->state = RecordState::kSettled;
                ++stats_.probe_orphan_settled;
                source = std::move(record->source);
                area = std::move(record->area);
            }
        } else {
            auto record = inflight->second;
            if (record->settled.exchange(true)) return true;
            inflight_.erase(inflight);
            record->state = RecordState::kSettled;

            PortRuntime* port =
                findPortLocked(record->key.version.path.local_nic);
            if (port != nullptr) releaseSlotLocked(*port, *record);
            releaseBudgetLocked(record->peer);

            PathRuntime* runtime = findPathLocked(record->key.version);
            ProbeEdgeSupply* edge = findEdgeLocked(record->key.version);
            const StaleReason stale = staleReasonLocked(*record);
            const bool current = stale == StaleReason::kNone;
            if (current) {
                applyLocked(completion, record->key.version, *runtime, *edge,
                            *port, now, effects);
            } else {
                ++stats_.probe_stale;
                switch (stale) {
                    case StaleReason::kEpoch:
                        ++stats_.probe_gate_blocked_by_epoch;
                        break;
                    case StaleReason::kGeneration:
                        ++stats_.probe_handle_generation_mismatch;
                        break;
                    case StaleReason::kResource:
                        ++stats_.probe_gate_blocked_by_resource;
                        break;
                    case StaleReason::kNone:
                        break;
                }
            }

            // Campaign accounting is filtered by id/epoch; a stale health
            // result still terminalizes its matching entry so the campaign
            // cannot hang on an in-flight item (design §7.3).
            campaignProgressLocked(
                record->key, current ? mapCampaignOutcome(completion.category)
                                     : EntryState::kCanceled);
            releaseTaskSlotLocked(*record);

            source = std::move(record->source);
            area = std::move(record->area);
        }
    }

    // Handle destructors call into the adapter, so references are released
    // strictly after the PLM mutex is gone (design §9.2 / §14.1-6).
    source.reset();
    area.reset();
    emitEffects(effects);
    return true;
}

ProbeLinkManager::StaleReason ProbeLinkManager::staleReasonLocked(
    const ProbeRecord& record) {
    auto runtime = paths_.find(record.key.version);
    auto edge = edges_.find(record.key.version);
    if (runtime == paths_.end() || edge == edges_.end())
        return StaleReason::kResource;
    auto port = ports_.find(record.key.version.path.local_nic);
    if (port == ports_.end() || !port->second.handle_valid)
        return StaleReason::kResource;
    if (runtime->second.health == LinkHealth::kDisabled)
        return StaleReason::kResource;
    if (record.key.jetty_generation != port->second.generation)
        return StaleReason::kGeneration;
    if (record.key.area_epoch != edge->second.area_epoch)
        return StaleReason::kEpoch;
    if (record.key.fault_epoch != runtime->second.fault_epoch)
        return StaleReason::kEpoch;
    return StaleReason::kNone;
}

void ProbeLinkManager::applyLocked(const Completion& completion,
                                   const PathVersion& version,
                                   PathRuntime& runtime,
                                   const ProbeEdgeSupply& edge,
                                   PortRuntime& port, uint64_t now_ns,
                                   Effects& effects) {
    switch (completion.category) {
        case CompletionCategory::SUCCESS: {
            runtime.consecutive_failures = 0;
            runtime.backoff_ns = 0;
            ++runtime.success_count;
            ++stats_.probe_success;
            if (runtime.health == LinkHealth::kHealthy) {
                // Normal verification on a healthy path: no repeated publish.
                return;
            }
            if (runtime.health == LinkHealth::kIsolated) {
                setHealthLocked(runtime, LinkHealth::kRecovering, effects);
            }
            if (externalGateOk(edge)) {
                publishHealthyLocked(runtime, effects);
            } else {
                // Probe proved liveness, but an external gate is closed:
                // stay RECOVERING and keep proving liveness on the normal
                // interval (design §10.3).
                ++stats_.probe_gate_blocked_by_external;
                runtime.next_probe_ns = now_ns + params_.intervalNs();
            }
            return;
        }
        case CompletionCategory::REMOTE_PATH_ERROR:
        case CompletionCategory::TIMEOUT: {
            // Provider-side ACK timeout carries a completion and is treated as
            // a path fault, while the lane is retired too, so both possible
            // root causes are covered (design §9.3).
            ++stats_.probe_failure_path;
            if (completion.category == CompletionCategory::TIMEOUT) {
                ++stats_.probe_ack_timeout;
            }
            if (runtime.health != LinkHealth::kIsolated) {
                // "Newly discovered bad" bumps the fault epoch; failing again
                // while already isolated is only "not recovered yet".
                ++runtime.fault_epoch;
                setHealthLocked(runtime, LinkHealth::kIsolated, effects);
            }
            ++runtime.consecutive_failures;
            runtime.backoff_ns =
                backoffDelayLocked(runtime.consecutive_failures);
            runtime.next_probe_ns = now_ns + runtime.backoff_ns;
            laneRetireLocked(port, now_ns, version,
                             Status::RdmaError("probe remote path error"),
                             effects);
            return;
        }
        case CompletionCategory::MEMORY_ERROR:
            // Probe area/token mismatch is a lane/configuration problem, never
            // evidence that the path is bad (design §9.3 / L-3).
            ++stats_.probe_area_config_error;
            laneRetireLocked(port, now_ns, version,
                             Status::InternalError("probe area config error"),
                             effects);
            markPortPendingLocked(port, PendingReason::kLaneRetired, now_ns);
            return;
        case CompletionCategory::LOCAL_DEVICE_ERROR:
        case CompletionCategory::ENDPOINT_ERROR:
        case CompletionCategory::UNKNOWN_ERROR:
        default:
            ++stats_.probe_failure_lane;
            laneRetireLocked(port, now_ns, version,
                             Status::InternalError("probe lane error"),
                             effects);
            markPortPendingLocked(port, PendingReason::kLaneRetired, now_ns);
            return;
    }
}

void ProbeLinkManager::releaseSlotLocked(PortRuntime& port,
                                         const ProbeRecord& record) {
    if (port.slot_token == record.token &&
        port.slot_generation == record.key.jetty_generation) {
        port.slot_token = 0;
        if (port.inflight > 0) --port.inflight;
        return;
    }
    // A completion from an older generation must never touch the new
    // generation's slot (invariant 11).
    ++stats_.probe_slot_anomaly;
}

void ProbeLinkManager::releaseBudgetLocked(const ProbePeerKey& peer) {
    auto it = peer_inflight_.find(peer);
    if (it == peer_inflight_.end()) return;
    if (it->second > 0) --it->second;
    if (it->second == 0) peer_inflight_.erase(it);
}

void ProbeLinkManager::releaseTaskSlotLocked(ProbeRecord& record) {
    PathRuntime* runtime = findPathLocked(record.key.version);
    if (runtime == nullptr || runtime->inflight_seq != record.key.probe_seq) {
        // Not the owner of the slot (stale record, or the slot was already
        // reclaimed): leave the runtime untouched.
        return;
    }
    runtime->inflight_seq = 0;
    auto campaign = campaigns_.find(runtime->peer);
    if (campaign != campaigns_.end()) {
        auto entry = campaign->second.entries.find(record.key.version);
        if (entry != campaign->second.entries.end()) {
            if (entry->second.state == EntryState::kInflight) {
                // Post was not accepted or the epoch did not match: the entry
                // waits for another attempt (design §9.2).
                entry->second.state = EntryState::kQueued;
                runtime->task = TaskState::kQueued;
                return;
            }
            if (entry->second.state == EntryState::kQueued) {
                runtime->task = TaskState::kQueued;
                return;
            }
        }
    }
    runtime->task = TaskState::kIdle;
}

void ProbeLinkManager::campaignProgressLocked(const ProbeKey& key,
                                              EntryState outcome) {
    if (key.campaign_id == 0) return;
    for (auto& [peer, campaign] : campaigns_) {
        if (campaign.id != key.campaign_id) continue;
        auto entry = campaign.entries.find(key.version);
        if (entry == campaign.entries.end()) break;
        if (entry->second.state != EntryState::kInflight) break;
        if (entry->second.entry_epoch != key.campaign_epoch) break;
        entry->second.state = outcome;
        switch (outcome) {
            case EntryState::kOk:
                ++stats_.campaign_entry_ok;
                break;
            case EntryState::kFail:
                ++stats_.campaign_entry_fail;
                break;
            default:
                ++stats_.campaign_entry_canceled;
                break;
        }
        break;
    }
    exitFinishedCampaignsLocked();
}

void ProbeLinkManager::exitFinishedCampaignsLocked() {
    // Exit only needs terminal entries. It deliberately does not wait for
    // RECOVERING: a path whose gate stays closed is advanced by its own
    // periodic probe and by interface 7 (design §7.3 / L-2).
    for (auto it = campaigns_.begin(); it != campaigns_.end();) {
        bool active = false;
        for (const auto& [version, entry] : it->second.entries) {
            if (entry.state == EntryState::kQueued ||
                entry.state == EntryState::kInflight) {
                active = true;
                break;
            }
        }
        if (active) {
            ++it;
        } else {
            it = campaigns_.erase(it);
            ++stats_.campaign_exited;
        }
    }
}

void ProbeLinkManager::mergeOrCreateCampaignLocked(const ProbePeerKey& peer,
                                                   uint64_t now_ns) {
    auto it = campaigns_.find(peer);
    Campaign* campaign = nullptr;
    if (it != campaigns_.end()) {
        campaign = &it->second;
        ++campaign->epoch;
        ++stats_.campaign_merged;
    } else {
        Campaign fresh;
        fresh.id = ++campaign_seq_;
        fresh.epoch = 1;
        fresh.peer = peer;
        it = campaigns_.emplace(peer, std::move(fresh)).first;
        campaign = &it->second;
        ++stats_.campaign_created;
    }

    for (auto& [version, runtime] : paths_) {
        if (!(runtime.peer == peer)) continue;
        // A DISABLED path cannot be verified and must not hold a campaign
        // hostage; it waits for a new graph version instead.
        if (runtime.health == LinkHealth::kDisabled) continue;
        auto entry = campaign->entries.find(version);
        if (entry != campaign->entries.end() &&
            entry->second.state == EntryState::kInflight) {
            // In-flight verification settles under its old epoch and is
            // skipped, not restarted (design §7.3).
            continue;
        }
        campaign->entries[version] =
            CampaignEntry{EntryState::kQueued, campaign->epoch};
        runtime.next_probe_ns = now_ns;
        if (runtime.task != TaskState::kInflight) {
            runtime.task = TaskState::kQueued;
        }
    }
    // A peer whose every path is DISABLED gets no entry at all; converge such
    // an empty campaign immediately instead of leaving it stranded.
    exitFinishedCampaignsLocked();
}

void ProbeLinkManager::cancelCampaignEntriesLocked(const PathVersion& version) {
    for (auto& [peer, campaign] : campaigns_) {
        auto entry = campaign.entries.find(version);
        if (entry == campaign.entries.end()) continue;
        if (entry->second.state == EntryState::kQueued ||
            entry->second.state == EntryState::kInflight) {
            entry->second.state = EntryState::kCanceled;
            ++stats_.campaign_entry_canceled;
        }
    }
    exitFinishedCampaignsLocked();
}

std::optional<PathVersion> ProbeLinkManager::resolveVersionLocked(
    const UbPostPath& evidence, bool require_matching_generation) {
    bool found = false;
    std::optional<PathVersion> resolved;
    for (const auto& [version, runtime] : paths_) {
        if (version.path.local_nic != evidence.local_topology_id) continue;
        if (version.path.segment_id != evidence.remote_segment_id) continue;
        if (version.path.remote_nic != evidence.remote_device_id) continue;
        found = true;
        auto edge = edges_.find(version);
        if (edge == edges_.end()) continue;
        const uint64_t anchor = edge->second.endpoint_generation;
        const bool generation_matches =
            anchor == 0 || anchor == evidence.endpoint_generation;
        if (generation_matches) {
            resolved = version;
            break;
        }
        // L-7: an unmappable fault is attributed to the rail's current
        // version anyway, so a real fault is never lost; a success, however,
        // must not promote a path version it does not belong to.
        ++stats_.probe_evidence_unmapped;
        if (require_matching_generation) return std::nullopt;
        resolved = version;
        break;
    }
    if (!found) ++stats_.probe_evidence_unmapped;
    return resolved;
}

void ProbeLinkManager::onBusinessPathSuccess(const UbPostPath& path,
                                             uint64_t now_ns) {
    (void)now_ns;
    Effects effects;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto version =
            resolveVersionLocked(path, /*require_matching_generation=*/true);
        if (!version) return;
        PathRuntime* runtime = findPathLocked(*version);
        if (runtime == nullptr) return;
        if (runtime->health != LinkHealth::kEligibleUnverified) return;
        publishHealthyLocked(*runtime, effects);
        // Business eligibility did not change (both states are usable), so no
        // path-bucket bump is needed (design §6.2).
        effects.bump = false;
    }
    emitEffects(effects);
}

void ProbeLinkManager::onBusinessPathError(const UbPostPath& path,
                                           BusinessErrorKind kind,
                                           uint64_t now_ns) {
    const uint64_t now = normalizedNow(now_ns);

    if (kind == BusinessErrorKind::kParamError) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.business_param_error;
        LOG_EVERY_N(WARNING, 100)
            << "UB probe: local parameter/length/permission business error on "
               "local_nic="
            << path.local_topology_id << " peer=" << path.remote_segment_id
            << " remote_nic=" << path.remote_device_id
            << "; not isolating the path";
        return;
    }
    if (kind == BusinessErrorKind::kContextFailure) {
        // A context failure is a port-level event, not a path fault.
        onContextUnavailable(path.local_topology_id, now);
        return;
    }
    if (kind == BusinessErrorKind::kLaneError ||
        kind == BusinessErrorKind::kUnknown) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (kind == BusinessErrorKind::kLaneError) {
            ++stats_.business_lane_error;
        } else {
            ++stats_.business_unknown_error;
        }
        return;
    }
    if (kind != BusinessErrorKind::kPathFault) return;

    Effects effects;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto version =
            resolveVersionLocked(path, /*require_matching_generation=*/false);
        if (!version) return;
        PathRuntime* runtime = findPathLocked(*version);
        if (runtime == nullptr) return;
        if (runtime->health == LinkHealth::kIsolated) {
            // Idempotent: without business filtering in W1 the same isolated
            // path keeps failing; bumping its epoch would make every in-flight
            // probe stale and the path could never recover (design §6.3-3).
            ++stats_.business_error_while_isolated;
            return;
        }
        if (runtime->health == LinkHealth::kDisabled) {
            // DISABLED is not recovered by probing, so there is nothing to
            // isolate or verify.
            ++stats_.business_error_while_disabled;
            return;
        }
        ++runtime->fault_epoch;
        setHealthLocked(*runtime, LinkHealth::kIsolated, effects);
        runtime->backoff_ns = 0;
        runtime->next_probe_ns = now;
        runtime->consecutive_failures = 0;
        mergeOrCreateCampaignLocked(runtime->peer, now);
    }
    emitEffects(effects);
}

void ProbeLinkManager::onContextUnavailable(Topology::NicID local_nic,
                                            uint64_t now_ns) {
    const uint64_t now = normalizedNow(now_ns);
    Effects effects;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        PortRuntime* port = findPortLocked(local_nic);
        if (port == nullptr) return;
        port->handle_valid = false;
        port->broken_since_ns = now;
        for (const auto& version : port->paths) {
            PathRuntime* runtime = findPathLocked(version);
            if (runtime == nullptr) continue;
            cancelCampaignEntriesLocked(version);
            runtime->task = TaskState::kIdle;
            setHealthLocked(*runtime, LinkHealth::kDisabled, effects);
        }
        markPortPendingLocked(*port, PendingReason::kContextFailure, now);
    }
    effects.bump = true;
    emitEffects(effects);
}

void ProbeLinkManager::markPortPendingLocked(PortRuntime& port,
                                             PendingReason reason,
                                             uint64_t now_ns) {
    for (const auto& version : port.paths) {
        PathRuntime* runtime = findPathLocked(version);
        if (runtime == nullptr) continue;
        if (!runtime->pending) runtime->pending_since_ns = now_ns;
        runtime->pending = true;
        runtime->pending_reason = reason;
    }
}

void ProbeLinkManager::clearPortPendingLocked(PortRuntime& port) {
    for (const auto& version : port.paths) {
        PathRuntime* runtime = findPathLocked(version);
        if (runtime == nullptr) continue;
        runtime->pending = false;
        runtime->pending_reason = PendingReason::kNone;
        runtime->pending_since_ns = 0;
    }
}

void ProbeLinkManager::laneRetireLocked(PortRuntime& port, uint64_t now_ns,
                                        const PathVersion& path,
                                        const Status& reason,
                                        Effects& effects) {
    if (port.handle_valid) {
        // One report per real retirement; repeated reports are merged by the
        // receiver, and a second retire of a broken port is a no-op here.
        port.handle_valid = false;
        port.broken_since_ns = now_ns;
        effects.retire_lane = true;
        effects.lane =
            LaneKey{port.supply.local_nic,
                    port.supply.probe_jetty ? port.supply.probe_jetty->id() : 0,
                    port.generation};
        effects.generation = port.generation;
        effects.error_epoch = ++lane_error_epoch_;
        effects.path = path;
        effects.reason = reason;
    }
    markPortPendingLocked(port, PendingReason::kLaneRetired, now_ns);
}

void ProbeLinkManager::moveToOrphansLocked(
    const std::shared_ptr<ProbeRecord>& record_ref, OrphanReason reason,
    uint64_t now_ns) {
    if (record_ref == nullptr) return;
    // Copy first: callers pass the mapped value of inflight_, and the erase
    // below destroys that node.
    auto record = record_ref;
    const uint64_t token = record->token;
    // Fixed order (design §9.5): campaign accounting, task slot, peer budget,
    // state, orphan table. The port slot is intentionally kept: no resend may
    // ever overlap this WR, and a new generation resets it.
    campaignProgressLocked(record->key, EntryState::kCanceled);
    releaseTaskSlotLocked(*record);
    releaseBudgetLocked(record->peer);
    record->state = RecordState::kOrphan;
    record->orphan_reason = reason;
    inflight_.erase(token);
    orphans_[token] = record;
    ++stats_.probe_orphan_added;
    (void)now_ns;
}

void ProbeLinkManager::removePathLocked(const PathVersion& version) {
    auto it = paths_.find(version);
    if (it == paths_.end()) return;
    cancelCampaignEntriesLocked(version);
    auto port = ports_.find(version.path.local_nic);
    if (port != ports_.end()) {
        auto& list = port->second.paths;
        list.erase(std::remove(list.begin(), list.end(), version), list.end());
        if (port->second.rr_cursor > list.size()) port->second.rr_cursor = 0;
    }
    paths_.erase(it);
}

Status ProbeLinkManager::upsertEdgeLocked(const ProbeEdgeSupply& edge) {
    auto port = ports_.find(edge.version.path.local_nic);
    if (port == ports_.end()) {
        return Status::InvalidArgument(
            "probe edge references a local port without supply");
    }
    auto existing = edges_.find(edge.version);
    if (existing != edges_.end()) {
        // Same PathVersion: an in-place refresh of the area/gates. Health is
        // preserved; in-flight evidence becomes stale via area_epoch.
        existing->second = edge;
        return Status::OK();
    }
    for (const auto& [version, other] : edges_) {
        if (version.path == edge.version.path) {
            return Status::InvalidArgument(
                "multiple path versions for one adjacency edge");
        }
    }
    edges_.emplace(edge.version, edge);
    PathRuntime runtime;
    runtime.peer = edge.peer;
    paths_.emplace(edge.version, std::move(runtime));
    port->second.paths.push_back(edge.version);
    ++stats_.path_state_eligible;
    return Status::OK();
}

bool ProbeLinkManager::validateEventPayloadLocked(
    const ProbeEvent& event) const {
    switch (event.kind) {
        case ProbeEventKind::kProbeHandleReplaced: {
            if (!event.port) return false;
            const ProbePortSupply& port = *event.port;
            if (port.local_nic < 0 || port.jetty_generation == 0 ||
                !port.probe_jetty || !port.probe_jetty->valid() ||
                !port.source_segment || port.source_length == 0 ||
                port.source_generation == 0) {
                return false;
            }
            if (params_.jfc_mode == ProbeJfcMode::kDedicated &&
                !port.probe_jfc) {
                return false;
            }
            if (event.local_nic >= 0 && event.local_nic != port.local_nic) {
                return false;
            }
            return ports_.find(port.local_nic) != ports_.end();
        }
        case ProbeEventKind::kPortRevoked:
            return event.local_nic >= 0;
        case ProbeEventKind::kAreaReplaced:
        case ProbeEventKind::kTargetEpochChanged:
        case ProbeEventKind::kEdgeAdded: {
            if (!event.edge) return false;
            const ProbeEdgeSupply& edge = *event.edge;
            if (!edge.version.valid() || !edge.peer.valid()) return false;
            if (!edge.area_segment || edge.area_length == 0 ||
                edge.area_epoch == 0) {
                return false;
            }
            if (event.local_nic >= 0 &&
                event.local_nic != edge.version.path.local_nic) {
                return false;
            }
            if (ports_.find(edge.version.path.local_nic) == ports_.end()) {
                return false;
            }
            // Reject, rather than partially apply, an event that would leave
            // two live versions for one adjacency. Application is atomic: the
            // handler below deletes old rows before inserting the new one, so
            // this check must happen while validating.
            for (const auto& [version, existing] : edges_) {
                if (version == edge.version) continue;
                if (!(version.path == edge.version.path)) continue;
                if (event.kind == ProbeEventKind::kTargetEpochChanged &&
                    version.topology_epoch == edge.version.topology_epoch) {
                    // The row this event is about to replace.
                    continue;
                }
                return false;
            }
            return true;
        }
        case ProbeEventKind::kGateChanged:
            return event.gate.has_value() && event.local_nic >= 0;
        case ProbeEventKind::kEdgeRemoved:
            return event.version.has_value() && event.version->valid();
    }
    return false;
}

Status ProbeLinkManager::applyEventLocked(const ProbeEvent& event,
                                          uint64_t now_ns, Effects& effects) {
    switch (event.kind) {
        case ProbeEventKind::kProbeHandleReplaced: {
            PortRuntime* port = findPortLocked(event.port->local_nic);
            if (port == nullptr) {
                return Status::InvalidArgument("probe port not in graph");
            }
            // Retire the old generation's in-flight records first: they keep
            // their references in the orphan table and may complete late.
            std::vector<uint64_t> stale_tokens;
            for (const auto& [token, record] : inflight_) {
                if (record->key.version.path.local_nic !=
                    port->supply.local_nic)
                    continue;
                if (record->key.jetty_generation == port->generation) {
                    stale_tokens.push_back(token);
                }
            }
            for (uint64_t token : stale_tokens) {
                auto record = inflight_.find(token);
                if (record != inflight_.end()) {
                    moveToOrphansLocked(record->second,
                                        OrphanReason::kGenerationRetired,
                                        now_ns);
                }
            }

            port->supply = *event.port;
            port->generation = event.port->jetty_generation;
            port->slot_token = 0;
            port->slot_generation = port->generation;
            port->inflight = 0;
            port->handle_valid = true;
            port->broken_since_ns = 0;
            clearPortPendingLocked(*port);
            // Continue from the previous health: an isolated path probes
            // immediately on the new handle, healthy paths stay quiet.
            for (const auto& version : port->paths) {
                PathRuntime* runtime = findPathLocked(version);
                if (runtime == nullptr) continue;
                if (runtime->health == LinkHealth::kIsolated ||
                    runtime->health == LinkHealth::kRecovering) {
                    runtime->next_probe_ns = now_ns;
                }
            }
            effects.bump = true;
            return Status::OK();
        }
        case ProbeEventKind::kPortRevoked: {
            PortRuntime* port = findPortLocked(event.local_nic);
            if (port == nullptr) return Status::OK();
            // The pooling side revoked the resources on purpose: mark broken
            // and wait for kProbeHandleReplaced, do not report interface 2.
            port->handle_valid = false;
            port->broken_since_ns = now_ns;
            markPortPendingLocked(*port, PendingReason::kPortRevoked, now_ns);
            return Status::OK();
        }
        case ProbeEventKind::kAreaReplaced: {
            auto existing = edges_.find(event.edge->version);
            if (existing == edges_.end()) {
                // The version was removed by an earlier event; nothing to
                // refresh. This is not a provider error, so the idempotency
                // key still advances.
                return Status::OK();
            }
            existing->second = *event.edge;
            PathRuntime* runtime = findPathLocked(event.edge->version);
            if (runtime != nullptr &&
                (runtime->health == LinkHealth::kIsolated ||
                 runtime->health == LinkHealth::kRecovering)) {
                // In-flight records for this version are stale via area_epoch;
                // the periodic task resumes immediately on the new page.
                runtime->next_probe_ns = now_ns;
            }
            effects.bump = true;
            return Status::OK();
        }
        case ProbeEventKind::kTargetEpochChanged: {
            // target_epoch is part of the PathVersion primary key, so the old
            // row is located by its (path, topology_epoch) prefix and deleted;
            // the event only carries the new version (design §12.3).
            const PathVersion& next = event.edge->version;
            std::vector<PathVersion> retired;
            for (const auto& [version, runtime] : paths_) {
                if (version == next) continue;
                if (version.path == next.path &&
                    version.topology_epoch == next.topology_epoch) {
                    retired.push_back(version);
                }
            }
            for (const auto& version : retired) {
                removePathLocked(version);
                edges_.erase(version);
            }
            Status status = upsertEdgeLocked(*event.edge);
            effects.bump = true;
            return status;
        }
        case ProbeEventKind::kGateChanged: {
            for (auto& [version, edge] : edges_) {
                if (version.path.local_nic != event.local_nic) continue;
                edge.node_gate_ok = event.gate->node_ok;
                edge.port_gate_ok = event.gate->port_ok;
                edge.profile_gate_ok = event.gate->profile_ok;
            }
            // Re-evaluate RECOVERING paths at the same synchronization point:
            // an already-proven path publishes HEALTHY without new evidence,
            // otherwise it stays RECOVERING (design §10.2/§10.3).
            std::vector<PathVersion> recovering;
            for (const auto& [version, runtime] : paths_) {
                if (version.path.local_nic != event.local_nic) continue;
                if (runtime.health == LinkHealth::kRecovering) {
                    recovering.push_back(version);
                }
            }
            for (const auto& version : recovering) {
                PathRuntime* runtime = findPathLocked(version);
                auto edge = edges_.find(version);
                if (runtime == nullptr || edge == edges_.end()) continue;
                if (externalGateOk(edge->second)) {
                    publishHealthyLocked(*runtime, effects);
                } else {
                    ++stats_.probe_gate_blocked_by_external;
                }
            }
            return Status::OK();
        }
        case ProbeEventKind::kEdgeAdded: {
            Status status = upsertEdgeLocked(*event.edge);
            effects.bump = true;
            return status;
        }
        case ProbeEventKind::kEdgeRemoved: {
            removePathLocked(*event.version);
            edges_.erase(*event.version);
            effects.bump = true;
            return Status::OK();
        }
    }
    return Status::InvalidArgument("unknown probe event kind");
}

Status ProbeLinkManager::onPoolAvailabilityChange(const ProbeEvent& event) {
    const uint64_t now = normalizedNow(0);
    Effects effects;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!initialized_) {
            return Status::InvalidArgument(
                "probe link manager is not initialized");
        }
        if (event.event_seq == 0) {
            ++stats_.probe_event_malformed;
            return Status::InvalidArgument(
                "probe event sequence must start at 1");
        }
        if (event.graph_epoch < graph_epoch_) {
            // A duplicate from an epoch that was already superseded.
            // Acknowledge it so the provider can move on.
            ++stats_.probe_event_stale_epoch;
            return Status::OK();
        }
        if (event.graph_epoch == graph_epoch_) {
            if (event.event_seq <= last_applied_seq_) {
                ++stats_.probe_event_duplicate;
                return Status::OK();
            }
            if (event.event_seq != last_applied_seq_ + 1) {
                // A gap must not advance the key: the provider has to resend
                // from the earliest unconfirmed event (design §12.3).
                ++stats_.probe_event_gap;
                LOG_EVERY_N(WARNING, 100)
                    << "UB probe: interface-7 event gap, expected seq "
                    << (last_applied_seq_ + 1) << " got " << event.event_seq
                    << " at graph epoch " << event.graph_epoch;
                return Status::NeedsRefreshCache("probe event gap");
            }
        } else if (event.event_seq != 1) {
            ++stats_.probe_event_gap;
            return Status::NeedsRefreshCache(
                "probe event gap at new graph epoch");
        }

        if (!validateEventPayloadLocked(event)) {
            ++stats_.probe_event_malformed;
            return Status::InvalidArgument("malformed probe event payload");
        }

        Status status = applyEventLocked(event, now, effects);
        if (!status.ok()) return status;
        graph_epoch_ = event.graph_epoch;
        last_applied_seq_ = event.event_seq;
    }
    emitEffects(effects);
    return Status::OK();
}

std::vector<PathHealthView> ProbeLinkManager::healthView() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PathHealthView> view;
    view.reserve(paths_.size());
    for (const auto& [version, runtime] : paths_) {
        PathHealthView entry;
        entry.version = version;
        entry.peer = runtime.peer;
        entry.health = runtime.health;
        entry.fault_epoch = runtime.fault_epoch;
        entry.consecutive_failures = runtime.consecutive_failures;
        entry.next_probe_ns = runtime.next_probe_ns;
        entry.probe_seq = runtime.probe_seq;
        entry.inflight_seq = runtime.inflight_seq;
        entry.task = runtime.task;
        entry.pending = runtime.pending;
        entry.pending_reason = runtime.pending_reason;
        entry.pending_since_ns = runtime.pending_since_ns;
        view.push_back(std::move(entry));
    }
    return view;
}

std::vector<CampaignView> ProbeLinkManager::campaignView() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<CampaignView> view;
    view.reserve(campaigns_.size());
    for (const auto& [peer, campaign] : campaigns_) {
        CampaignView entry;
        entry.id = campaign.id;
        entry.epoch = campaign.epoch;
        entry.peer = peer;
        entry.entries.reserve(campaign.entries.size());
        for (const auto& [version, state] : campaign.entries) {
            entry.entries.push_back(
                CampaignEntryView{version, state.state, state.entry_epoch});
        }
        view.push_back(std::move(entry));
    }
    return view;
}

ProbeStats ProbeLinkManager::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    ProbeStats snapshot = stats_;
    snapshot.campaign_waiting_for_event = 0;
    for (const auto& [peer, campaign] : campaigns_) {
        bool waiting = false;
        for (const auto& [version, entry] : campaign.entries) {
            if (entry.state != EntryState::kQueued) continue;
            auto port = ports_.find(version.path.local_nic);
            if (port == ports_.end() || !port->second.handle_valid) {
                waiting = true;
                break;
            }
            auto runtime = paths_.find(version);
            if (runtime == paths_.end() || runtime->second.pending) {
                waiting = true;
                break;
            }
        }
        if (waiting) ++snapshot.campaign_waiting_for_event;
    }
    return snapshot;
}

size_t ProbeLinkManager::inflightCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return inflight_.size();
}

size_t ProbeLinkManager::orphanCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return orphans_.size();
}

uint64_t ProbeLinkManager::graphEpoch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return graph_epoch_;
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
