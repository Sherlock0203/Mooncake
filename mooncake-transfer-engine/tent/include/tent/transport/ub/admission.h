// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Ported from the pooling side (`930/Mooncake` @ a9edee30, M-1) and trimmed to
// what W3 actually wires.
//
// What survived: `JfcCreditLedger`, the non-blocking per-JFC WR credit ledger a
// lease carries (D-7). Credit is reserved inside `tryLease()` and released
// exactly once by the settlement path; the pool itself never releases caller
// credit.
//
// What was deliberately left behind (D-6 / R-3): the pooling side's
// `TaskPermitLedger` / `TaskPermitTicket`. That ledger is process-wide, so a K
// of 4 would cap the whole process instead of one UbTask, and the design puts K
// on `UbTask` instead (upstream §5.3). Keeping the two implementations apart
// also keeps the compile-time seam that RK3 asks for: nothing here may be
// mistaken for the task-level K gate.

#ifndef TENT_TRANSPORT_UB_ADMISSION_H_
#define TENT_TRANSPORT_UB_ADMISSION_H_

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"

namespace mooncake::tent::ub {

// Non-blocking per-JFC WR credit ledger (C05/C06). Each posted WR consumes
// one credit of its JFC until the completion is dispatched; control records
// (flush-done markers, AE) never consume WR credit (INV/C08). Credits are
// reserved atomically and a failed reservation is released immediately: a
// caller must never hold partially reserved resources while waiting.
class JfcCreditLedger final {
   public:
    JfcCreditLedger() = default;

    JfcCreditLedger(const JfcCreditLedger&) = delete;
    JfcCreditLedger& operator=(const JfcCreditLedger&) = delete;

    // Installs the credit capacity of one JFC: the usable WR depth, i.e. the
    // native JFC depth minus the control reserve. Re-installing an index is
    // rejected so a live ledger can never be silently widened.
    Status install(size_t jfc_index, uint64_t capacity);

    // Reserves one or more credits without blocking. Returns the reservation
    // token (non-zero) on success; std::nullopt when the JFC is unknown or has
    // insufficient credit.
    [[nodiscard]] std::optional<uint64_t> tryReserve(size_t jfc_index,
                                                     uint64_t credits = 1);

    // Releases a reservation exactly once. Releasing an unknown or already
    // released token is a harmless no-op; credit can never underflow.
    void release(size_t jfc_index, uint64_t reservation_token);

    [[nodiscard]] uint64_t outstanding(size_t jfc_index) const;
    [[nodiscard]] uint64_t capacity(size_t jfc_index) const;
    [[nodiscard]] uint64_t rejectedReservations(size_t jfc_index) const;
    [[nodiscard]] uint64_t rejectedReleases(size_t jfc_index) const;

   private:
    struct JfcCredits {
        uint64_t capacity = 0;
        uint64_t outstanding = 0;
        uint64_t rejected = 0;
        uint64_t rejected_releases = 0;
        uint64_t next_token = 1;
        std::unordered_map<uint64_t, uint64_t> active;
    };

    mutable std::mutex mutex_;
    std::unordered_map<size_t, JfcCredits> jfcs_;
};

// Move-only Task K ticket (C06/INV03). One network UbTask may have at most
// K inflight attempts across all contexts; a pool/quota/credit miss defers
// the attempt and never charges this ledger. The ticket is a software
// token: RAII release is safe because it carries no native responsibility.
class TaskPermitTicket {
   public:
    TaskPermitTicket() = default;
    TaskPermitTicket(TaskPermitTicket&& other) noexcept
        : ledger_(other.ledger_), permit_id_(other.permit_id_) {
        other.permit_id_ = 0;
        other.ledger_ = nullptr;
    }
    TaskPermitTicket& operator=(TaskPermitTicket&& other) noexcept {
        if (this != &other) {
            release();
            ledger_ = other.ledger_;
            permit_id_ = other.permit_id_;
            other.permit_id_ = 0;
            other.ledger_ = nullptr;
        }
        return *this;
    }
    ~TaskPermitTicket() { release(); }

    TaskPermitTicket(const TaskPermitTicket&) = delete;
    TaskPermitTicket& operator=(const TaskPermitTicket&) = delete;

    [[nodiscard]] bool valid() const noexcept { return permit_id_ != 0; }
    [[nodiscard]] uint64_t permitId() const noexcept { return permit_id_; }

    void release();

   private:
    friend class TaskPermitLedger;
    TaskPermitTicket(class TaskPermitLedger* ledger, uint64_t permit_id)
        : ledger_(ledger), permit_id_(permit_id) {}

    class TaskPermitLedger* ledger_ = nullptr;
    uint64_t permit_id_ = 0;
};

// Process-wide K ledger. max_inflight_attempts is the explicit positive
// integer configured as transports/ub/send_pool/task_max_inflight_attempts;
// it is never derived from jetty_per_endpoint (C01).
class TaskPermitLedger final {
   public:
    explicit TaskPermitLedger(uint64_t max_inflight_attempts)
        : max_inflight_attempts_(max_inflight_attempts) {}

    TaskPermitLedger(const TaskPermitLedger&) = delete;
    TaskPermitLedger& operator=(const TaskPermitLedger&) = delete;

    // Acquires one attempt permit. Returns an invalid ticket when K is
    // exhausted; the caller must defer instead of retrying or faulting.
    [[nodiscard]] TaskPermitTicket tryAcquire();

    [[nodiscard]] uint64_t maxInflightAttempts() const noexcept {
        return max_inflight_attempts_;
    }
    [[nodiscard]] uint64_t outstanding() const;
    [[nodiscard]] uint64_t rejectedAcquisitions() const;

   private:
    friend class TaskPermitTicket;
    void releasePermit(uint64_t permit_id);

    const uint64_t max_inflight_attempts_;
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, bool> active_;
    uint64_t next_permit_id_ = 1;
    uint64_t rejected_ = 0;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_ADMISSION_H_
