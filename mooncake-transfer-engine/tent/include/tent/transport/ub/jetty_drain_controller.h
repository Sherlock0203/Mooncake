// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Asynchronous pooled-Jetty drain chain
// (design docs/analyze/tent-ub-async-drain-design.md §3/§4.2/§5).
//
// A faulted pooled member is no longer fenced with one blocking
// quiesceJetty() call on the error path. The chain is split into independently
// schedulable steps:
//
//   close PostGate -> modifyJettyError -> poller sees the flush-done marker
//   -> flushJettyOnce until the software SQ is empty -> responsibility
//   convergence -> finishDrain -> DELETE_READY -> retire worker
//
// Hard invariants (design §3.2):
//   1. The JFC poller never blocks on a provider call. onEntityEvent() only
//      looks an entry up and records a flag.
//   2. modify ERROR is linearized with post() by the Jetty's own mutex.
//   3. The delete thread never polls a JFC; flushJettyOnce() only runs on the
//      control executor.
//   4. An unknown predicate is quarantine, never delete (fail-closed).
//
// DELETE_READY counter-examples (do not "simplify" them away):
//   - a marker alone is not enough: the software SQ may still hold
//   WR_UNHANDLED;
//   - an empty software flush is not enough: the marker may not have arrived;
//   - a deadline is not a barrier: timeout means responsibility is unknown;
//   - a failed post is not proof: acceptance may be unknown.

#ifndef TENT_TRANSPORT_UB_JETTY_DRAIN_CONTROLLER_H_
#define TENT_TRANSPORT_UB_JETTY_DRAIN_CONTROLLER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/control_executor.h"
#include "tent/transport/ub/pool_key.h"
#include "tent/transport/ub/tombstone.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

// Observable stage of one drain entry (design §5.1). kFlushing means "waiting
// for the current epoch's flush-done marker"; kDraining means "marker seen,
// software flush / convergence in progress".
enum class DrainState : uint8_t {
    kQuiescing = 0,
    kFlushing = 1,
    kDraining = 2,
    kDeleteReady = 3,
    kQuarantined = 4,
};

inline const char* drainStateName(DrainState state) noexcept {
    switch (state) {
        case DrainState::kQuiescing:
            return "quiescing";
        case DrainState::kFlushing:
            return "flushing";
        case DrainState::kDraining:
            return "draining";
        case DrainState::kDeleteReady:
            return "delete_ready";
        case DrainState::kQuarantined:
            return "quarantined";
    }
    return "unknown";
}

struct DrainStats {
    // Every accepted registration, including the ones that then failed to
    // schedule. `drain_begin_failed` is a *subset* of this counter, not an
    // additional event: read "begin" as attempts and "begin_failed" as the
    // backpressured share of those attempts.
    uint64_t drain_begin = 0;
    // A drain that could not even be scheduled (controller stopping / executor
    // queue full). It is a local backpressure fact, never a provider failure.
    uint64_t drain_begin_failed = 0;
    uint64_t drain_delete_ready = 0;
    uint64_t drain_timeout = 0;
    uint64_t drain_quarantined = 0;
    uint64_t modify_error_ok = 0;
    uint64_t modify_error_fail = 0;
    uint64_t marker_flush_done = 0;
    uint64_t marker_suspend_done = 0;
    uint64_t marker_dup = 0;
    uint64_t marker_stale = 0;
    uint64_t marker_before_error = 0;
    // Entity-level events (for example an async event) that named a Jetty with
    // an in-flight drain. They never enter the chain; they force a quarantine
    // because responsibility cannot be proven (design §6.4).
    uint64_t entity_event_on_drain = 0;
    uint64_t software_flush_calls = 0;
    uint64_t software_flush_wrs = 0;
    uint64_t software_flush_unhandled = 0;
    uint64_t drained_after_settle = 0;
    size_t drain_pending = 0;
    uint64_t drain_oldest_age_ms = 0;
    bool async_drain_supported = true;
    uint64_t async_drain_disabled = 0;
};

struct DrainOptions {
    // Total budget from beginDrain() to DELETE_READY (design §10.1). Reaching
    // it is responsibility-unknown, never a delete permission.
    uint32_t retire_timeout_ms = 1000;
    // Bound on flushJettyOnce() rounds per entry per tick.
    uint32_t flush_batch = 64;
    // Re-arm delay of the drain tick while entries are pending.
    uint32_t tick_interval_ms = 1;
    // Consecutive *provider* drain failures that declare the provider unable
    // to drain (design §9). Only a missing marker (deadline) or a provider
    // contract violation counts; a local quarantine (entity event on the Jetty,
    // lost handle, responsibility not converged) never escalates.
    uint32_t quarantine_threshold = 3;
    // Injectable clock (tests use a fake one). Empty means steady_clock.
    std::function<uint64_t()> now_fn;
};

struct DrainRequest {
    PoolLaneKey lane{};
    PoolKey pool_key{};
    // The member's retirement epoch after the pool bumped it.
    uint64_t error_epoch = 0;
    uint64_t jfc_index = 0;
    // The pool closed its post gate (moved the member out of kInUse under the
    // pool mutex) before asking for the drain. The controller asserts it rather
    // than assuming it.
    bool post_gate_closed = false;
};

// One in-flight drain. The controller owns the entry through a shared_ptr and
// the pool owns the member: an entry must never keep a member alive (design
// §7, RC-04 weak-reference convention).
struct DrainEntry {
    PoolLaneKey lane{};
    PoolKey pool_key{};
    uint64_t error_epoch = 0;
    uint64_t jfc_index = 0;
    uint64_t deadline_ns = 0;
    uint64_t started_ns = 0;
    // Responsibility-convergence evidence (§5.3). Every field is derived from
    // an existing facility; no counter is added to PoolMember.
    bool post_gate_closed = false;
    // Set under the entry lock *before* the provider call. The poller uses it
    // to tell "the marker arrived before modify was even requested" (an
    // anomaly) from "modify is in flight and the provider already entered
    // ERROR" (legal, because the ERROR transition is what produces the marker).
    bool modify_started = false;
    bool modify_done = false;
    bool marker_seen = false;
    bool software_sq_empty = false;
    bool responsibility_converged = false;
    bool finish_done = false;
    size_t software_wr_drained = 0;
    // Why this entry was quarantined. Only "failed" reasons may escalate to
    // "the provider cannot drain" (design §9); local anomalies never do.
    bool provider_failure = false;
    DrainState state = DrainState::kQuiescing;
    std::string quarantine_reason;
};

// Pool-side actions injected into the controller, so the controller never
// depends on SendJettyPool (design §4.2).
struct DrainCallbacks {
    // Resolve the live native handle of a lane for one drain step. The
    // controller does not retain the returned handle.
    std::function<JettyPtr(const PoolLaneKey&)> resolveJetty;
    // All DELETE_READY conditions hold: the pool marks the member and enqueues
    // it for native destruction (the delete worker still performs the call).
    std::function<void(const PoolLaneKey&, uint64_t error_epoch)>
        markDeleteReady;
    // Responsibility is unknown: the pool keeps the handle, tombstone and
    // budget, and stops counting the member as serving.
    std::function<void(const PoolLaneKey&, const std::string&)> quarantine;
    // A WR recovered by the software flush must settle exactly once through the
    // normal completion path. Returns false when the token was already settled.
    std::function<bool(const Completion&, uint64_t jfc_index)> settleDrainedWr;
    // "The completion gate has claimed and disposed of every owner of this
    // lane." Convergence is lane-generation scoped: a member owns exactly one
    // WR and a retired generation is never reused, so the lane is a stronger
    // identity than a token that the pool does not know at retire time.
    // The lane ledger is shared by every Context, so the query carries the
    // Context incarnation together with the pool-local generation (INV-ID-3).
    std::function<bool(const LaneRefKey&)> responsibilityConverged;
};

class JettyDrainController {
   public:
    JettyDrainController(std::shared_ptr<UrmaAdapter> adapter,
                         const TombstoneTable* tombstones,
                         ControlExecutor* executor, DrainCallbacks callbacks,
                         DrainOptions options);
    ~JettyDrainController();

    JettyDrainController(const JettyDrainController&) = delete;
    JettyDrainController& operator=(const JettyDrainController&) = delete;

    // Registers one drain and schedules the first control task. Non-blocking:
    // it is called from the error/fault return path. TooManyRequests means the
    // request was not accepted (controller stopping or executor queue full) and
    // the caller must quarantine the member instead of retrying.
    Status beginDrain(const DrainRequest& request);

    // Called by the JFC poller for every token==0 completion. Returns true when
    // the event belongs to this controller (the caller must not count it as an
    // unknown token). Performs no provider call and takes no pool lock.
    bool onEntityEvent(const Completion& completion, uint64_t jfc_index);

    // Advances every pending entry. Runs on the control executor (or directly
    // from a test). Provider calls happen outside the entry lock.
    void tick(uint64_t now_ns);

    // Shutdown fallback: every in-flight entry degrades to the synchronous
    // quiesce fence, and later beginDrain() calls are rejected so the pool
    // quarantines instead of scheduling into a stopping executor.
    void drainSynchronously();

    // Rejects new work and wakes nothing. In-flight entries are left to
    // drainSynchronously(); the executor is owned by the transport.
    void stop();

    [[nodiscard]] DrainStats stats() const;
    [[nodiscard]] bool running() const;
    [[nodiscard]] bool asyncDrainSupported() const;

   private:
    using ClockFn = std::function<uint64_t()>;
    static constexpr uint32_t kDrainKeyShift = 32;

    static uint64_t drainKey(uint64_t jfc_index, uint32_t jetty_id) noexcept {
        return (jfc_index << kDrainKeyShift) | static_cast<uint64_t>(jetty_id);
    }

    uint64_t nowNs() const;
    bool requestPump(bool immediate);
    void runPump();
    void armTickIfNeeded();
    bool hasPendingLocked() const;
    // One step of one entry. Returns true when it made progress that may
    // enable another step immediately.
    bool advanceOne(const std::shared_ptr<DrainEntry>& entry, uint64_t now_ns);
    bool doModify(const std::shared_ptr<DrainEntry>& entry);
    bool doFlush(const std::shared_ptr<DrainEntry>& entry);
    bool doConvergence(const std::shared_ptr<DrainEntry>& entry);
    bool doFinish(const std::shared_ptr<DrainEntry>& entry);
    void quarantineEntry(const std::shared_ptr<DrainEntry>& entry,
                         const std::string& reason, bool timed_out);
    void markDeleteReady(const std::shared_ptr<DrainEntry>& entry);
    void noteDrainFailureLocked();
    void noteDrainSuccessLocked();

    const std::shared_ptr<UrmaAdapter> adapter_;
    const TombstoneTable* tombstones_;
    ControlExecutor* executor_;
    DrainCallbacks callbacks_;
    const DrainOptions options_;
    const uint64_t timeout_ns_;
    const uint64_t tick_interval_ns_;

    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<DrainEntry>> entries_;
    DrainStats stats_;
    // Set once the provider proved unable to drain (§9): new faults go straight
    // to quarantine and the transport may apply its admission brake.
    bool async_drain_supported_ = true;
    uint32_t consecutive_failures_ = 0;
    bool stop_ = false;
    bool stopping_ = false;
    // At most one task of each kind is armed at a time. An urgent event (marker
    // arrival, new request) must never wait for an armed periodic tick, so the
    // two slots are separate; a task clears its own slot before running, and a
    // duplicate pump is harmless because tick() is idempotent.
    std::atomic<bool> urgent_scheduled_{false};
    std::atomic<bool> tick_scheduled_{false};
    // Serializes the control thread's tick with the shutdown fallback, so a
    // timer that is already queued can never run a second step for an entry the
    // synchronous drain is handling.
    std::mutex pump_mutex_;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_JETTY_DRAIN_CONTROLLER_H_
