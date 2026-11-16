// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/attempt_gate.h"

namespace mooncake::tent::ub {

uint64_t AttemptGate::registerAttempt(const AttemptKey& attempt,
                                      const LaneKey& lane,
                                      uint64_t expected_length) {
    std::lock_guard<std::mutex> lock(mutex_);
    const uint64_t token = next_token_++;
    GateRecord record;
    record.attempt = attempt;
    record.lane = lane;
    record.expected_length = expected_length;
    records_.emplace(token, std::move(record));
    return token;
}

void AttemptGate::onPostReturn(uint64_t token, const Status& post_status,
                               PostAcceptance acceptance) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = records_.find(token);
    if (entry == records_.end()) return;
    GateRecord& record = entry->second;
    if (record.finalized) {
        ++record.late_events;
        return;
    }
    // The post path returns exactly once; a second return is a contract bug
    // and must not overwrite the first normalized acceptance.
    if (record.post_returned) return;
    record.post_returned = true;
    record.acceptance = acceptance;
    record.post_status_text = post_status.ToString();
}

void AttemptGate::onRealCompletion(uint64_t token, bool success,
                                   uint64_t length) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = records_.find(token);
    if (entry == records_.end()) return;
    GateRecord& record = entry->second;
    if (record.finalized) {
        ++record.late_events;
        return;
    }
    ++record.real_completions;
    if (success) {
        ++record.success_completions;
        record.last_length = length;
        record.length_valid = length == record.expected_length;
    } else {
        ++record.failed_completions;
    }
}

void AttemptGate::onHandlerEnter(uint64_t token) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = records_.find(token);
    if (entry == records_.end()) return;
    if (entry->second.finalized) {
        ++entry->second.late_events;
        return;
    }
    ++entry->second.handler_refs;
}

void AttemptGate::onHandlerExit(uint64_t token) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = records_.find(token);
    if (entry == records_.end()) return;
    GateRecord& record = entry->second;
    if (record.handler_refs > 0) --record.handler_refs;
}

void AttemptGate::reportFault(uint64_t token, const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = records_.find(token);
    if (entry == records_.end()) return;
    GateRecord& record = entry->second;
    if (record.finalized) {
        ++record.late_events;
        return;
    }
    record.fault_reported = true;
    record.fault_reason = reason;
}

bool AttemptGate::tryFinalize(uint64_t token, bool pool_open,
                              Settlement& out) {
    std::lock_guard<std::mutex> lock(mutex_);
    out = Settlement{};
    auto entry = records_.find(token);
    if (entry == records_.end()) {
        out.wait = true;
        return false;
    }
    GateRecord& record = entry->second;
    if (record.finalized) return false;  // single settlement

    // Responsibility still on the post path, or a handler still runs:
    // nothing may be settled (INV05).
    if (!record.post_returned || record.handler_refs > 0) {
        out.wait = true;
        return false;
    }

    out = decide(record, pool_open);
    if (out.wait) return false;
    record.finalized = true;
    return true;
}

AttemptGate::Settlement AttemptGate::decide(const GateRecord& record,
                                            bool pool_open) {
    Settlement settlement;
    settlement.acceptance = record.acceptance;

    // A real error retires the member permanently; the execution verdict is
    // whatever the completions prove, otherwise Unknown.
    if (record.fault_reported) {
        settlement.retire = true;
        settlement.safety = ResourceSafety::kUnsafe;
        if (record.real_completions == 1 &&
            record.success_completions == 1 && record.length_valid) {
            settlement.outcome = ExecutionOutcome::kSucceeded;
        } else if (record.real_completions == 0) {
            settlement.outcome = ExecutionOutcome::kUnknown;
        } else {
            settlement.outcome = ExecutionOutcome::kFailed;
        }
        return settlement;
    }

    switch (record.acceptance) {
        case PostAcceptance::kAccepted: {
            if (record.real_completions == 1 &&
                record.success_completions == 1 && record.length_valid) {
                settlement.outcome = ExecutionOutcome::kSucceeded;
                settlement.safety = ResourceSafety::kSafe;
                if (pool_open) {
                    settlement.return_to_pool = true;
                } else {
                    // Pool stopped: the attempt succeeded but the member
                    // retires instead of re-entering circulation.
                    settlement.retire = true;
                }
                return settlement;
            }
            if (record.real_completions == 0) {
                // Completion not dispatched yet: wait for it.
                settlement.wait = true;
                return settlement;
            }
            if (record.real_completions == 1 &&
                record.failed_completions == 1) {
                settlement.outcome = ExecutionOutcome::kFailed;
                settlement.safety = ResourceSafety::kUnsafe;
                settlement.retire = true;
                return settlement;
            }
            // Duplicate real CQE or invalid length: contradiction.
            settlement.outcome = ExecutionOutcome::kUnknown;
            settlement.safety = ResourceSafety::kUnsafe;
            settlement.retire = true;
            return settlement;
        }
        case PostAcceptance::kNotAccepted: {
            if (record.real_completions == 0) {
                // Definitive zero acceptance: rollback; the member may be
                // returned after rollback (INV07).
                settlement.outcome = ExecutionOutcome::kNotExecuted;
                settlement.safety = ResourceSafety::kSafe;
                settlement.rollback_return = true;
                return settlement;
            }
            // Provider said NotAccepted but a real CQE exists: unknown
            // responsibility, keep it.
            settlement.outcome = ExecutionOutcome::kUnknown;
            settlement.safety = ResourceSafety::kUnsafe;
            settlement.retire = true;
            return settlement;
        }
        case PostAcceptance::kUnknown: {
            settlement.outcome = ExecutionOutcome::kUnknown;
            if (record.real_completions == 0) {
                // Hold responsibility: drain/quarantine, never reuse.
                settlement.safety = ResourceSafety::kUnproven;
            } else {
                settlement.safety = ResourceSafety::kUnsafe;
            }
            settlement.retire = true;
            return settlement;
        }
    }
    settlement.wait = true;
    return settlement;
}

void AttemptGate::discard(uint64_t token) {
    std::lock_guard<std::mutex> lock(mutex_);
    records_.erase(token);
}

bool AttemptGate::isFinalized(uint64_t token) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = records_.find(token);
    return entry != records_.end() && entry->second.finalized;
}

uint64_t AttemptGate::lateEventsAfterFinalize(uint64_t token) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = records_.find(token);
    return entry == records_.end() ? 0 : entry->second.late_events;
}

size_t AttemptGate::openAttempts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return records_.size();
}

}  // namespace mooncake::tent::ub
