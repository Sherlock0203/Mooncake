// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_NODE_TARGET_QUOTA_H_
#define TENT_TRANSPORT_UB_NODE_TARGET_QUOTA_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "tent/common/status.h"

namespace mooncake::tent::ub {

// Lifecycle state of one node-level target reservation. The four sets are
// mutually exclusive; a ticket occupies exactly one state at a time and
// state transfers never double count (guidance 4.2.1):
//
//   TargetOccupied = CreatingReserved + Active + RetiringDeleting
//                  + QuarantinedUnproven
//   TargetOccupied <= max_target_jetties_per_node   (default 8)
enum class TargetObjectState : uint8_t {
    kCreatingReserved = 0,  // ticket granted, native target not yet published
    kActive,                // published and serving shared inbound traffic
    kRetiringDeleting,      // retired, destruction in progress
    kQuarantinedUnproven,   // destruction unproven/Unknown; keeps its quota
};

inline const char* targetObjectStateName(TargetObjectState state) noexcept {
    switch (state) {
        case TargetObjectState::kCreatingReserved:
            return "creating_reserved";
        case TargetObjectState::kActive:
            return "active";
        case TargetObjectState::kRetiringDeleting:
            return "retiring_deleting";
        case TargetObjectState::kQuarantinedUnproven:
            return "quarantined_unproven";
    }
    return "unknown";
}

class NodeTargetQuota;

// Move-only ticket for exactly one receive-side target native Jetty. The
// holder owns the quota slot until it releases it through a proven-outcome
// gate. Dropping a ticket without a release is a bookkeeping leak: the
// destructor counts it but never frees quota implicitly, because Unknown
// destruction must keep the slot charged (guidance 4.2.1 safe reclaim).
// The ledger is held as a weak reference (RC-04 pattern): if the ledger is
// already gone, the leak notification is a no-op instead of a
// use-after-free.
//
// One target referenced by many peers/business attempts/probes counts once:
// references are owned by the target owner, not by extra tickets. Send-pool
// Jetties, import handles and WR/connection counts stay under the original
// device budget and credit ledgers, not under this quota.
class TargetQuotaTicket {
   public:
    TargetQuotaTicket() = default;
    TargetQuotaTicket(TargetQuotaTicket&& other) noexcept;
    TargetQuotaTicket& operator=(TargetQuotaTicket&& other) noexcept;
    ~TargetQuotaTicket();

    TargetQuotaTicket(const TargetQuotaTicket&) = delete;
    TargetQuotaTicket& operator=(const TargetQuotaTicket&) = delete;

    [[nodiscard]] bool valid() const noexcept { return ticket_id_ != 0; }
    [[nodiscard]] uint64_t ticketId() const noexcept { return ticket_id_; }
    [[nodiscard]] TargetObjectState state() const noexcept { return state_; }

   private:
    friend class NodeTargetQuota;
    TargetQuotaTicket(std::weak_ptr<NodeTargetQuota> quota, uint64_t ticket_id,
                      TargetObjectState state)
        : quota_(std::move(quota)),
          ticket_id_(ticket_id),
          state_(state) {}

    void detach() noexcept {
        quota_.reset();
        ticket_id_ = 0;
        state_ = TargetObjectState::kCreatingReserved;
    }

    std::weak_ptr<NodeTargetQuota> quota_;
    uint64_t ticket_id_ = 0;
    TargetObjectState state_ = TargetObjectState::kCreatingReserved;
};

// Node-level snapshot used by gates, diagnostics and tests. total_occupied
// sums the four mutually exclusive states and never exceeds limit.
struct NodeTargetQuotaSnapshot {
    uint32_t limit = 0;
    uint64_t creating_reserved = 0;
    uint64_t active = 0;
    uint64_t retiring_deleting = 0;
    uint64_t quarantined_unproven = 0;
    uint64_t total_occupied = 0;
    uint64_t rejected_reservations = 0;
    uint64_t leaked_tickets = 0;
};

// Process-authoritative node target quota (guidance 4.2.1). The ledger caps
// the total number of receive-side target native Jetties of one node across
// all senders, Contexts, devices/EIDs, Engines and resource generations. It
// is NOT a per-sender cap and NOT a bound on senders or inflight WRs.
//
// acquire() installs the ledger once per process: every later acquire with
// the same limit returns the same ledger (shared across Contexts/Engines);
// a different limit is rejected because the quota is frozen at node install
// and v1 never hot-updates it. node_incarnation changes, in-process
// restarts and IP aliases never split or zero the old accounting. Multiple
// independent production pooled receiver processes require one authoritative
// allocator / cross-process coordination, which is not implemented in this
// increment; without it, per-process ledgers must not be claimed as a
// node-global guarantee.
//
// Quota is released only by releaseUnallocated() (proven never allocated)
// or releaseAfterProvenDestruction() (proven destroyed). Logical failures,
// timeouts, Unknown deletions and dropped tickets never zero the accounting.
class NodeTargetQuota final
    : public std::enable_shared_from_this<NodeTargetQuota> {
   public:
    NodeTargetQuota(const NodeTargetQuota&) = delete;
    NodeTargetQuota& operator=(const NodeTargetQuota&) = delete;

    // Installs or returns the process-authoritative ledger. limit must be a
    // positive integer (1..UINT32_MAX); 0 is rejected (0 is never
    // "unlimited"). A second acquire with a different limit fails closed
    // with InvalidArgument instead of widening or splitting the ledger.
    static Status acquire(uint32_t limit,
                          std::shared_ptr<NodeTargetQuota>& out);

    // Test-only escape hatch: drops the process registry so the next
    // acquire() installs a fresh ledger. Production code must never call
    // this; it exists because gtest runs many installs in one process.
    static void resetRegistryForTest();

    [[nodiscard]] uint32_t limit() const noexcept { return limit_; }

    // Static capacity plan check, run before any native allocation: the
    // number of targets the selected topology requires (one per port) must
    // not exceed the frozen limit. required > limit is InvalidArgument;
    // the caller must not create, trim ports or raise the limit instead.
    [[nodiscard]] Status validateStaticDemand(
        uint64_t required_targets) const;

    // Atomically reserves one target slot at the linearization point
    // (check-and-increment under one lock; never check-then-increment).
    // Returns std::nullopt when the node is full: the caller defers with a
    // capacity classification (TooManyRequests), never creates, never
    // judges the port bad and never charges a network attempt retry.
    // The ticket starts in kCreatingReserved.
    [[nodiscard]] std::optional<TargetQuotaTicket> tryReserve();

    // Moves a ticket between states. Allowed edges:
    //   kCreatingReserved  -> kActive | kQuarantinedUnproven
    //   kActive            -> kRetiringDeleting | kQuarantinedUnproven
    //   kRetiringDeleting  -> kQuarantinedUnproven
    // Any other edge (backwards, from kQuarantinedUnproven, unknown ticket)
    // is rejected as InvalidArgument and leaves the ledger unchanged.
    Status transition(TargetQuotaTicket& ticket, TargetObjectState next);

    // Releases a reservation that provably never allocated a native target
    // (create was never called, or the frozen provider contract proves the
    // failure left no handle). Only kCreatingReserved tickets may use this
    // rollback path; a partial/Unknown create must transition to
    // kQuarantinedUnproven and keep the slot instead.
    Status releaseUnallocated(TargetQuotaTicket& ticket);

    // Releases quota after the native destruction has been proven. proof
    // must be true; an unproven/Unknown/failed deletion keeps the ticket
    // charged (callers quarantine it instead). Only kRetiringDeleting and
    // kQuarantinedUnproven tickets may be released. A Status failure alone
    // never decrements the accounting.
    Status releaseAfterProvenDestruction(TargetQuotaTicket& ticket,
                                         bool native_destruction_proven);

    // Records a dropped ticket (destructor path). The quota stays charged
    // and the leak counter increases; this is diagnostics, not a release.
    void recordLeakedTicket(const TargetQuotaTicket& ticket);

    [[nodiscard]] NodeTargetQuotaSnapshot snapshot() const;

   private:
    explicit NodeTargetQuota(uint32_t limit) : limit_(limit) {}

    struct TicketEntry {
        TargetObjectState state = TargetObjectState::kCreatingReserved;
    };

    const uint32_t limit_;
    mutable std::mutex mutex_;
    uint64_t next_ticket_id_ = 1;
    uint64_t rejected_reservations_ = 0;
    uint64_t leaked_tickets_ = 0;
    std::unordered_map<uint64_t, TicketEntry> tickets_;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_NODE_TARGET_QUOTA_H_