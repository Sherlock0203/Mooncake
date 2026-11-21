// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/jetty_drain_controller.h"

#include <algorithm>
#include <chrono>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace mooncake::tent::ub {
namespace {

uint64_t steadyNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

bool terminalState(DrainState state) noexcept {
    return state == DrainState::kDeleteReady ||
           state == DrainState::kQuarantined;
}

}  // namespace

JettyDrainController::JettyDrainController(std::shared_ptr<UrmaAdapter> adapter,
                                           const TombstoneTable* tombstones,
                                           ControlExecutor* executor,
                                           DrainCallbacks callbacks,
                                           DrainOptions options)
    : adapter_(std::move(adapter)),
      tombstones_(tombstones),
      executor_(executor),
      callbacks_(std::move(callbacks)),
      options_(std::move(options)),
      timeout_ns_(static_cast<uint64_t>(
                      std::max<uint32_t>(1, options_.retire_timeout_ms)) *
                  1'000'000ULL),
      tick_interval_ns_(static_cast<uint64_t>(
                            std::max<uint32_t>(1, options_.tick_interval_ms)) *
                        1'000'000ULL) {}

JettyDrainController::~JettyDrainController() { stop(); }

uint64_t JettyDrainController::nowNs() const {
    if (options_.now_fn) return options_.now_fn();
    return steadyNowNs();
}

bool JettyDrainController::running() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !stop_ && !stopping_;
}

bool JettyDrainController::asyncDrainSupported() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return async_drain_supported_;
}

Status JettyDrainController::beginDrain(const DrainRequest& request) {
    if (request.lane.jetty_id == 0) {
        return Status::InvalidArgument(
            "UB Jetty drain requires a non-zero native id");
    }
    if (!request.post_gate_closed) {
        // The controller is never the first to forbid posting: the pool must
        // have moved the member out of kInUse under its own mutex first.
        return Status::InvalidArgument(
            "UB Jetty drain requires a closed post gate");
    }
    if (adapter_ == nullptr || executor_ == nullptr) {
        return Status::NotImplemented(
            "UB Jetty drain requires an adapter and a control executor");
    }

    const uint64_t key = drainKey(request.jfc_index, request.lane.jetty_id);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_) {
            return Status::TooManyRequests("UB Jetty drain controller stopped");
        }
        if (stopping_) {
            return Status::TooManyRequests(
                "UB Jetty drain controller is draining synchronously");
        }
        if (!async_drain_supported_) {
            ++stats_.async_drain_disabled;
            return Status::NotImplemented(
                "UB provider proved unable to drain Jetties; quarantine "
                "instead");
        }
        auto existing = entries_.find(key);
        if (existing != entries_.end()) {
            if (existing->second->lane == request.lane) {
                // Repeated fence of the same member generation: idempotent.
                return Status::OK();
            }
            // Same native id, different generation: the provider reused an id
            // while the previous drain is unresolved. Refuse rather than let
            // two lanes fight over one marker stream.
            return Status::TooManyRequests(
                "UB native Jetty id is already draining for another lane");
        }

        auto entry = std::make_shared<DrainEntry>();
        entry->lane = request.lane;
        entry->pool_key = request.pool_key;
        entry->error_epoch = request.error_epoch;
        entry->jfc_index = request.jfc_index;
        entry->post_gate_closed = request.post_gate_closed;
        entry->started_ns = nowNs();
        entry->deadline_ns = entry->started_ns + timeout_ns_;
        entry->state = DrainState::kQuiescing;
        entries_.emplace(key, entry);
        ++stats_.drain_begin;
        stats_.drain_pending = entries_.size();
    }

    if (!requestPump(/*immediate=*/true)) {
        // No control task could be armed: fail closed instead of leaving the
        // member in a drain that nobody advances.
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it != entries_.end() && it->second->lane == request.lane) {
            entries_.erase(it);
        }
        stats_.drain_pending = entries_.size();
        ++stats_.drain_begin_failed;
        return Status::TooManyRequests(
            "UB Jetty drain could not be scheduled on the control executor");
    }
    return Status::OK();
}

bool JettyDrainController::onEntityEvent(const Completion& completion,
                                         uint64_t jfc_index) {
    if (completion.token != 0) return false;
    if (adapter_ == nullptr) return false;
    const EntityMarkerKind kind = adapter_->classifyEntityMarker(completion);

    const uint64_t key = drainKey(jfc_index, completion.local_jetty_id);
    bool needs_pump = false;
    if (kind == EntityMarkerKind::kNone) {
        // Not a marker: an async event or an unclassified record. If it names a
        // Jetty with an in-flight drain, the entity's responsibility is not
        // proven and the chain must not reach DELETE_READY (design §6.4). The
        // event itself keeps flowing to the legacy entity-event sink, so this
        // returns false and does not swallow it.
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = entries_.find(key);
            if (it != entries_.end() && !terminalState(it->second->state)) {
                ++stats_.entity_event_on_drain;
                it->second->quarantine_reason =
                    "entity-level event observed on a draining Jetty";
                needs_pump = true;
            }
        }
        if (needs_pump) (void)requestPump(/*immediate=*/true);
        return false;
    }
    {
        // Poller budget (design §3.2 invariant 1): one hash lookup plus flag
        // writes. No provider call, no pool lock.
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            // No drain owns this (jfc, jetty) pair. Attribute it to a retired
            // entity when the tombstone table still records one; otherwise it
            // stays on the legacy unknown-token path (`return false`).
            if (tombstones_ != nullptr && completion.local_jetty_id != 0 &&
                tombstones_
                    ->lookup(EntityKind::kJetty, completion.local_jetty_id)
                    .has_value()) {
                ++stats_.marker_stale;
                return true;
            }
            return false;
        }
        auto& entry = it->second;
        if (terminalState(entry->state)) {
            ++stats_.marker_stale;
            return true;
        }
        if (kind == EntityMarkerKind::kSuspendDone) {
            // Recorded only: a suspend-done marker never advances a drain.
            ++stats_.marker_suspend_done;
            return true;
        }
        ++stats_.marker_flush_done;
        if (entry->marker_seen) {
            // rememberFlushDone() is idempotent; a repeat must not move state.
            ++stats_.marker_dup;
            return true;
        }
        if (!entry->modify_done && !entry->modify_started) {
            // The marker arrived before modifyJettyError() was even requested:
            // either the provider flushes early or a previous epoch's marker
            // survived. Fail closed instead of accepting it as proof (§6.2).
            ++stats_.marker_before_error;
            entry->quarantine_reason =
                "flush-done marker arrived before modify ERROR was requested";
            needs_pump = true;
        } else {
            // Either modify already returned, or it is in flight. The second
            // case is legal and must be accepted: entering ERROR is exactly
            // what makes the provider emit the marker, so the marker can beat
            // the return of modifyJettyError() on another thread. Rejecting it
            // would quarantine a healthy member (§4.1 race).
            entry->marker_seen = true;
            entry->state = DrainState::kDraining;
            needs_pump = true;
        }
    }
    if (needs_pump) (void)requestPump(/*immediate=*/true);
    return true;
}

void JettyDrainController::tick(uint64_t now_ns) {
    std::lock_guard<std::mutex> pump_lock(pump_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // A shutdown drain owns the entries from here on; a queued tick must
        // not race it with a second provider step.
        if (stop_ || stopping_) return;
    }

    std::vector<std::shared_ptr<DrainEntry>> snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot.reserve(entries_.size());
        uint64_t oldest_ns = 0;
        for (const auto& [_, entry] : entries_) {
            snapshot.push_back(entry);
            if (now_ns > entry->started_ns) {
                oldest_ns = std::max(oldest_ns, now_ns - entry->started_ns);
            }
        }
        stats_.drain_pending = entries_.size();
        stats_.drain_oldest_age_ms = oldest_ns / 1'000'000ULL;
    }

    for (auto& entry : snapshot) {
        // Bounded work per tick: a large software SQ must not monopolize the
        // control thread. The re-armed tick continues where this one stopped.
        for (uint32_t round = 0;
             round < std::max<uint32_t>(1, options_.flush_batch); ++round) {
            if (!advanceOne(entry, now_ns)) break;
            bool terminal = false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                terminal = terminalState(entry->state);
            }
            if (terminal) break;
        }
    }
}

bool JettyDrainController::advanceOne(const std::shared_ptr<DrainEntry>& entry,
                                      uint64_t now_ns) {
    enum class Step : uint8_t {
        kIdle,
        kQuarantine,
        kModify,
        kFlush,
        kConvergence,
        kFinish,
        kReady,
    };
    Step step = Step::kIdle;
    std::string reason;
    bool timed_out = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (terminalState(entry->state)) return false;
        if (!entry->quarantine_reason.empty()) {
            reason = entry->quarantine_reason;
            step = Step::kQuarantine;
        } else if (!entry->post_gate_closed) {
            reason = "post gate was not closed before the drain";
            entry->quarantine_reason = reason;
            step = Step::kQuarantine;
        } else if (now_ns >= entry->deadline_ns) {
            timed_out = true;
            reason = "drain deadline exceeded (retire_timeout_ms)";
            entry->quarantine_reason = reason;
            step = Step::kQuarantine;
        } else if (!entry->modify_done) {
            step = Step::kModify;
        } else if (!entry->marker_seen) {
            // Waiting for the poller; nothing to do until the marker arrives.
            step = Step::kIdle;
        } else if (!entry->software_sq_empty) {
            entry->state = DrainState::kDraining;
            step = Step::kFlush;
        } else if (!entry->responsibility_converged) {
            step = Step::kConvergence;
        } else if (!entry->finish_done) {
            step = Step::kFinish;
        } else {
            step = Step::kReady;
        }
    }

    switch (step) {
        case Step::kIdle:
            return false;
        case Step::kQuarantine:
            quarantineEntry(entry, reason, timed_out);
            return false;
        case Step::kModify:
            return doModify(entry);
        case Step::kFlush:
            return doFlush(entry);
        case Step::kConvergence:
            return doConvergence(entry);
        case Step::kFinish:
            return doFinish(entry);
        case Step::kReady:
            markDeleteReady(entry);
            return false;
    }
    return false;
}

bool JettyDrainController::doModify(const std::shared_ptr<DrainEntry>& entry) {
    JettyPtr jetty;
    if (callbacks_.resolveJetty) jetty = callbacks_.resolveJetty(entry->lane);
    if (!jetty || !jetty->valid()) {
        std::lock_guard<std::mutex> lock(mutex_);
        entry->quarantine_reason =
            "native Jetty handle is gone before modify ERROR";
        return true;
    }
    {
        // Claim the step *before* the provider call. From here on a marker that
        // arrives on the poller thread is attributable to this ERROR epoch, so
        // it must not be treated as "before modify" (§4.1 race).
        std::lock_guard<std::mutex> lock(mutex_);
        if (terminalState(entry->state)) return false;
        if (entry->modify_done) return true;
        if (entry->modify_started) return false;
        entry->modify_started = true;
    }
    // The only provider call of this step; never under the entry lock. A
    // successful return also proves post() has left the Jetty (same mutex).
    const Status status = adapter_->modifyJettyError(jetty, entry->error_epoch);
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminalState(entry->state)) return false;
    if (!status.ok()) {
        ++stats_.modify_error_fail;
        entry->quarantine_reason =
            "modifyJettyError failed: " + status.ToString();
        return true;
    }
    ++stats_.modify_error_ok;
    entry->modify_done = true;
    if (!entry->marker_seen) entry->state = DrainState::kFlushing;
    return true;
}

bool JettyDrainController::doFlush(const std::shared_ptr<DrainEntry>& entry) {
    JettyPtr jetty;
    if (callbacks_.resolveJetty) jetty = callbacks_.resolveJetty(entry->lane);
    if (!jetty || !jetty->valid()) {
        std::lock_guard<std::mutex> lock(mutex_);
        entry->quarantine_reason =
            "native Jetty handle is gone during software flush";
        return true;
    }
    std::vector<Completion> drained;
    const Status status = adapter_->flushJettyOnce(jetty, drained);
    size_t wr_count = 0;
    bool marker_in_batch = false;
    for (const auto& completion : drained) {
        if (completion.token == 0) {
            marker_in_batch = true;
        } else {
            ++wr_count;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (terminalState(entry->state)) return false;
        ++stats_.software_flush_calls;
        // Only real work-request completions count as drained WRs; an entity
        // marker in the batch is a contract violation, not a WR.
        stats_.software_flush_wrs += wr_count;
        entry->software_wr_drained += wr_count;
        if (!status.ok()) {
            // The only InternalError flushJettyOnce() produces is the "the
            // software SQ yielded an entity marker" contract violation, which
            // is exactly the unhandled-marker observation and a provider
            // failure for the §9 capability decision.
            if (status.IsInternalError()) {
                ++stats_.software_flush_unhandled;
                entry->provider_failure = true;
            }
            entry->quarantine_reason =
                "flushJettyOnce failed: " + status.ToString();
            return true;
        }
        if (drained.empty()) entry->software_sq_empty = true;
    }
    if (marker_in_batch) {
        // The provider broke the "flush never yields a marker" contract.
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.software_flush_unhandled;
        entry->provider_failure = true;
        entry->quarantine_reason = "software flush returned an entity marker";
        return true;
    }
    for (const auto& completion : drained) {
        // Exactly-once settlement: a token that was already claimed and
        // disposed by the gate is a diagnostic, not a second release.
        const bool settled =
            callbacks_.settleDrainedWr
                ? callbacks_.settleDrainedWr(completion, entry->jfc_index)
                : false;
        if (!settled) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.drained_after_settle;
        }
    }
    // Progress was made either way: a non-empty batch may still have more WRs
    // queued, and an empty one proves the software SQ is drained, which
    // unblocks the convergence/finish stages. The tick round bound keeps the
    // loop finite.
    return true;
}

bool JettyDrainController::doConvergence(
    const std::shared_ptr<DrainEntry>& entry) {
    // DrainRequest already carries the owning pool's context incarnation, so
    // the query needs no new parameter: it just stops relying on the pool-local
    // generation as if it were globally unique.
    const LaneRefKey lane{entry->pool_key.context_incarnation,
                          entry->lane.lane_generation};
    const bool converged = callbacks_.responsibilityConverged
                               ? callbacks_.responsibilityConverged(lane)
                               : false;
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminalState(entry->state)) return false;
    entry->responsibility_converged = converged;
    // When it is not converged yet, stop this tick: the next re-armed tick
    // asks again without burning the control thread.
    return converged;
}

bool JettyDrainController::doFinish(const std::shared_ptr<DrainEntry>& entry) {
    JettyPtr jetty;
    if (callbacks_.resolveJetty) jetty = callbacks_.resolveJetty(entry->lane);
    if (!jetty || !jetty->valid()) {
        std::lock_guard<std::mutex> lock(mutex_);
        entry->quarantine_reason =
            "native Jetty handle is gone before finishDrain";
        return true;
    }
    const Status status = adapter_->finishDrain(jetty);
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminalState(entry->state)) return false;
    if (!status.ok()) {
        entry->quarantine_reason = "finishDrain failed: " + status.ToString();
        return true;
    }
    entry->finish_done = true;
    return true;
}

void JettyDrainController::markDeleteReady(
    const std::shared_ptr<DrainEntry>& entry) {
    PoolLaneKey lane;
    uint64_t error_epoch = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (terminalState(entry->state)) return;
        entry->state = DrainState::kDeleteReady;
        lane = entry->lane;
        error_epoch = entry->error_epoch;
        ++stats_.drain_delete_ready;
        noteDrainSuccessLocked();
        const uint64_t key = drainKey(entry->jfc_index, entry->lane.jetty_id);
        auto it = entries_.find(key);
        if (it != entries_.end() && it->second.get() == entry.get()) {
            entries_.erase(it);
        }
        stats_.drain_pending = entries_.size();
    }
    if (callbacks_.markDeleteReady) {
        callbacks_.markDeleteReady(lane, error_epoch);
    }
}

void JettyDrainController::quarantineEntry(
    const std::shared_ptr<DrainEntry>& entry, const std::string& reason,
    bool timed_out) {
    PoolLaneKey lane;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (terminalState(entry->state)) return;
        entry->state = DrainState::kQuarantined;
        entry->quarantine_reason = reason;
        lane = entry->lane;
        ++stats_.drain_quarantined;
        if (timed_out) ++stats_.drain_timeout;
        // Design §9 only escalates when the *provider* failed to drain: a
        // missing marker (deadline) or a contract violation. Local anomalies
        // (entity event on the Jetty, lost handle, responsibility not
        // converged, rejected scheduling) are quarantined for safety but must
        // never disable the chain for the whole process.
        if (timed_out || entry->provider_failure) noteDrainFailureLocked();
        const uint64_t key = drainKey(entry->jfc_index, entry->lane.jetty_id);
        auto it = entries_.find(key);
        if (it != entries_.end() && it->second.get() == entry.get()) {
            entries_.erase(it);
        }
        stats_.drain_pending = entries_.size();
    }
    LOG_EVERY_N(WARNING, 100) << "UB pooled Jetty drain quarantined: " << reason
                              << " native_id=" << lane.jetty_id
                              << " generation=" << lane.lane_generation;
    if (callbacks_.quarantine) callbacks_.quarantine(lane, reason);
}

void JettyDrainController::drainSynchronously() {
    // Waits for an in-progress control tick (bounded: no provider call inside a
    // tick waits forever), then owns every entry until it returns.
    std::lock_guard<std::mutex> pump_lock(pump_mutex_);
    std::vector<std::shared_ptr<DrainEntry>> snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
        snapshot.reserve(entries_.size());
        for (const auto& [_, entry] : entries_) snapshot.push_back(entry);
    }
    for (auto& entry : snapshot) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (terminalState(entry->state)) continue;
        }
        if (adapter_ == nullptr) {
            quarantineEntry(entry, "synchronous drain: no adapter", false);
            continue;
        }
        JettyPtr jetty;
        if (callbacks_.resolveJetty) {
            jetty = callbacks_.resolveJetty(entry->lane);
        }
        if (!jetty || !jetty->valid()) {
            quarantineEntry(entry, "synchronous drain: native handle is gone",
                            false);
            continue;
        }
        // Shutdown/Context-fatal branch only: this is the design-permitted
        // blocking quiesce, and it must never be used on the error hot path.
        std::vector<Completion> drained;
        const Status status =
            adapter_->quiesceJetty(jetty, options_.retire_timeout_ms, drained);
        // Drop our reference before DELETE_READY: the native delete contract
        // requires the pool to be the sole owner of the handle.
        jetty.reset();
        size_t recovered = 0;
        for (const auto& completion : drained) {
            if (completion.token == 0) continue;
            ++recovered;
            const bool settled =
                callbacks_.settleDrainedWr
                    ? callbacks_.settleDrainedWr(completion, entry->jfc_index)
                    : false;
            if (!settled) {
                std::lock_guard<std::mutex> lock(mutex_);
                ++stats_.drained_after_settle;
            }
        }
        if (!status.ok()) {
            quarantineEntry(
                entry,
                "synchronous drain fallback failed: " + status.ToString(),
                false);
            continue;
        }
        bool converged = false;
        if (callbacks_.responsibilityConverged) {
            converged = callbacks_.responsibilityConverged(
                LaneRefKey{entry->pool_key.context_incarnation,
                           entry->lane.lane_generation});
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (terminalState(entry->state)) continue;
            // quiesceJetty() already ran modify + marker + software flush, so
            // every stage flag is proven by its success.
            entry->modify_done = true;
            entry->marker_seen = true;
            entry->software_sq_empty = true;
            entry->finish_done = true;
            entry->responsibility_converged = converged;
            ++stats_.software_flush_calls;
            stats_.software_flush_wrs += recovered;
            entry->software_wr_drained += recovered;
        }
        if (!converged) {
            // No release without evidence, even during shutdown.
            quarantineEntry(
                entry,
                "synchronous drain: completion responsibility not converged",
                false);
            continue;
        }
        markDeleteReady(entry);
    }
}

void JettyDrainController::stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
}

DrainStats JettyDrainController::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    DrainStats snapshot = stats_;
    snapshot.drain_pending = entries_.size();
    snapshot.async_drain_supported = async_drain_supported_;
    return snapshot;
}

bool JettyDrainController::hasPendingLocked() const {
    for (const auto& [_, entry] : entries_) {
        if (!terminalState(entry->state)) return true;
    }
    return false;
}

bool JettyDrainController::requestPump(bool immediate) {
    std::atomic<bool>& slot = immediate ? urgent_scheduled_ : tick_scheduled_;
    if (slot.exchange(true, std::memory_order_acq_rel)) return true;
    if (executor_ == nullptr) {
        slot.store(false, std::memory_order_release);
        return false;
    }
    Status status;
    if (immediate) {
        status = executor_->submit([this] {
            urgent_scheduled_.store(false, std::memory_order_release);
            runPump();
        });
    } else {
        status = executor_->scheduleAfter(tick_interval_ns_, [this] {
            tick_scheduled_.store(false, std::memory_order_release);
            runPump();
        });
    }
    if (!status.ok()) {
        slot.store(false, std::memory_order_release);
        return false;
    }
    return true;
}

void JettyDrainController::runPump() {
    tick(nowNs());
    armTickIfNeeded();
}

void JettyDrainController::armTickIfNeeded() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_ || stopping_ || !hasPendingLocked()) return;
    }
    (void)requestPump(/*immediate=*/false);
}

void JettyDrainController::noteDrainFailureLocked() {
    ++consecutive_failures_;
    const uint32_t threshold =
        std::max<uint32_t>(1, options_.quarantine_threshold);
    if (async_drain_supported_ && consecutive_failures_ >= threshold) {
        // Design §9: the provider proved unable to drain. New faults are
        // quarantined instead of retried forever; this is a process-level
        // one-way decision, never a silent fallback to the sync fence.
        async_drain_supported_ = false;
        stats_.async_drain_supported = false;
        LOG(WARNING) << "UB asynchronous Jetty drain is disabled after "
                     << consecutive_failures_
                     << " consecutive failed drains; new faults will be "
                        "quarantined";
    }
}

void JettyDrainController::noteDrainSuccessLocked() {
    consecutive_failures_ = 0;
}

}  // namespace mooncake::tent::ub
