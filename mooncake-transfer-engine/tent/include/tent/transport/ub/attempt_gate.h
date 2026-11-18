// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_ATTEMPT_GATE_H_
#define TENT_TRANSPORT_UB_ATTEMPT_GATE_H_

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "tent/common/status.h"
#include "tent/transport/ub/business_jetty_pool.h"

namespace mooncake::tent::ub {

// PostAcceptance comes from urma_adapter.h (kAccepted / kNotAccepted /
// kUnknown). The gate consumes the classification normalized at the post
// boundary (D04) and maps it onto resource responsibility.

// Execution verdict produced by the gate (C07).
enum class ExecutionOutcome : uint8_t {
    kNotExecuted = 0,  // definitive zero acceptance, no real CQE
    kSucceeded,        // exactly one real SUCCESS completion, length valid
    kFailed,           // definitive failure without ambiguity
    kUnknown,          // contradiction, duplicate CQE or unknown acceptance
};

inline const char* executionOutcomeName(ExecutionOutcome outcome) noexcept {
    switch (outcome) {
        case ExecutionOutcome::kNotExecuted:
            return "not_executed";
        case ExecutionOutcome::kSucceeded:
            return "succeeded";
        case ExecutionOutcome::kFailed:
            return "failed";
        case ExecutionOutcome::kUnknown:
            return "unknown";
    }
    return "invalid";
}

// Resource safety verdict: only kSafe allows the member to re-enter the
// pool; every other verdict keeps responsibility (retire/drain/quarantine)
// per INV08/INV09.
enum class ResourceSafety : uint8_t {
    kUnsafe = 0,
    kDraining,
    kUnproven,
    kSafe,
};

inline const char* resourceSafetyName(ResourceSafety safety) noexcept {
    switch (safety) {
        case ResourceSafety::kUnsafe:
            return "unsafe";
        case ResourceSafety::kDraining:
            return "draining";
        case ResourceSafety::kUnproven:
            return "unproven";
        case ResourceSafety::kSafe:
            return "safe";
    }
    return "invalid";
}

// Completion gate for one attempt (C07/D04). The gate owns the settlement
// predicate:
//
//   return-to-pool requires ALL of:
//     post_returned && acceptance == Accepted && exactly one real SUCCESS
//     completion && length valid && handler_refs == 0 && no fault &&
//     pool still OPEN (the lease return itself is the single claim)
//
//   definitive zero acceptance (rollback):
//     post_returned && acceptance == NotAccepted && no real CQE && no fault
//     && handler_refs == 0  ->  the attempt is rolled back; the member may
//     be returned after rollback (INV07)
//
//   contradiction (NotAccepted + real CQE, or duplicate real CQE) and
//   unknown acceptance keep responsibility: the member retires and the
//   outcome is Unknown.
//
// Settlement happens exactly once per attempt; late events after settlement
// are counted for diagnostics but never re-settle.
class AttemptGate final {
   public:
    struct Settlement {
        ExecutionOutcome outcome = ExecutionOutcome::kUnknown;
        ResourceSafety safety = ResourceSafety::kUnproven;
        PostAcceptance acceptance = PostAcceptance::kUnknown;
        bool return_to_pool = false;   // normal reuse path
        bool rollback_return = false;  // definitive zero-accept rollback
        bool retire = false;           // permanent retirement
        bool wait = false;             // not settleable yet
    };

    AttemptGate() = default;

    AttemptGate(const AttemptGate&) = delete;
    AttemptGate& operator=(const AttemptGate&) = delete;

    // Registers one attempt; returns the gate token. expected_length is the
    // business length the single real SUCCESS completion must carry.
    uint64_t registerAttempt(const AttemptKey& attempt, const PoolLaneKey& lane,
                             uint64_t expected_length);

    // Post path: the provider call returned exactly once. acceptance must be
    // normalized at this boundary (D04).
    void onPostReturn(uint64_t token, const Status& post_status,
                      PostAcceptance acceptance);

    // Poll path: one real (business) completion was dispatched. Control
    // records (flush-done markers, AE) never count as real completions.
    void onRealCompletion(uint64_t token, bool success, uint64_t length);

    // Handler lifecycle: completion handlers hold a reference while running;
    // settlement waits until all handlers exited (INV05).
    void onHandlerEnter(uint64_t token);
    void onHandlerExit(uint64_t token);

    // Recovery path: a real error was attributed to this attempt's member.
    void reportFault(uint64_t token, const std::string& reason);

    // Attempts to settle. Returns true and fills `out` exactly once when the
    // predicate is decidable; returns false (with out.wait=true when events
    // are still missing) otherwise. pool_open reflects the owning pool at
    // settlement time.
    bool tryFinalize(uint64_t token, bool pool_open, Settlement& out);

    // Drops the record after settlement was acted upon (lease returned /
    // retired). Safe to call multiple times.
    void discard(uint64_t token);

    // Diagnostics.
    [[nodiscard]] bool isFinalized(uint64_t token) const;
    [[nodiscard]] uint64_t lateEventsAfterFinalize(uint64_t token) const;
    [[nodiscard]] size_t openAttempts() const;

   private:
    struct GateRecord {
        AttemptKey attempt{};
        PoolLaneKey lane{};
        uint64_t expected_length = 0;
        bool post_returned = false;
        PostAcceptance acceptance = PostAcceptance::kUnknown;
        std::string post_status_text;
        uint64_t real_completions = 0;
        uint64_t success_completions = 0;
        uint64_t failed_completions = 0;
        uint64_t last_length = 0;
        bool length_valid = false;
        bool fault_reported = false;
        std::string fault_reason;
        int handler_refs = 0;
        bool finalized = false;
        uint64_t late_events = 0;
    };

    static Settlement decide(const GateRecord& record, bool pool_open);

    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, GateRecord> records_;
    uint64_t next_token_ = 1;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_ATTEMPT_GATE_H_
