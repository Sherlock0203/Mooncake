// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/admission.h"

#include <limits>
#include <utility>

namespace mooncake::tent::ub {

Status JfcCreditLedger::install(size_t jfc_index, uint64_t capacity) {
    if (capacity == 0) {
        return Status::InvalidArgument(
            "UB JFC credit capacity must be an explicit positive integer"
            LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (jfcs_.count(jfc_index) != 0) {
        return Status::InvalidArgument(
            "UB JFC credit ledger already installed for index " +
            std::to_string(jfc_index) + LOC_MARK);
    }
    JfcCredits credits;
    credits.capacity = capacity;
    jfcs_.emplace(jfc_index, std::move(credits));
    return Status::OK();
}

std::optional<uint64_t> JfcCreditLedger::tryReserve(size_t jfc_index,
                                                    uint64_t credits) {
    if (credits == 0) return std::nullopt;
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    if (jfc == jfcs_.end()) return std::nullopt;

    JfcCredits& record = jfc->second;
    if (credits > record.capacity ||
        record.outstanding > record.capacity - credits) {
        // Overflow-free outstanding + credits > capacity comparison.
        ++record.rejected;
        return std::nullopt;
    }
    const uint64_t token = record.next_token++;
    record.outstanding += credits;
    record.active.emplace(token, credits);
    return token;
}

void JfcCreditLedger::release(size_t jfc_index, uint64_t reservation_token) {
    if (reservation_token == 0) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    if (jfc == jfcs_.end()) return;
    JfcCredits& record = jfc->second;
    auto active = record.active.find(reservation_token);
    if (active == record.active.end()) return;
    const uint64_t credits = active->second;
    record.active.erase(active);
    record.outstanding = credits > record.outstanding ? 0
                                                       : record.outstanding -
                                                             credits;
}

uint64_t JfcCreditLedger::outstanding(size_t jfc_index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    return jfc == jfcs_.end() ? 0 : jfc->second.outstanding;
}

uint64_t JfcCreditLedger::capacity(size_t jfc_index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    return jfc == jfcs_.end() ? 0 : jfc->second.capacity;
}

uint64_t JfcCreditLedger::rejectedReservations(size_t jfc_index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    return jfc == jfcs_.end() ? 0 : jfc->second.rejected;
}

void TaskPermitTicket::release() {
    if (ledger_ != nullptr && permit_id_ != 0) {
        ledger_->releasePermit(permit_id_);
    }
    ledger_ = nullptr;
    permit_id_ = 0;
}

TaskPermitTicket TaskPermitLedger::tryAcquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    // K == 0 is fail-closed by construction: active_.size() >= 0 is always
    // true, so a zero permit budget rejects every acquisition (defer, never
    // a fault) instead of admitting unbounded attempts.
    if (active_.size() >= max_inflight_attempts_) {
        ++rejected_;
        return TaskPermitTicket();
    }
    const uint64_t permit_id = next_permit_id_++;
    active_.emplace(permit_id, true);
    return TaskPermitTicket(this, permit_id);
}

uint64_t TaskPermitLedger::outstanding() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_.size();
}

uint64_t TaskPermitLedger::rejectedAcquisitions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return rejected_;
}

void TaskPermitLedger::releasePermit(uint64_t permit_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    active_.erase(permit_id);
}

}  // namespace mooncake::tent::ub
