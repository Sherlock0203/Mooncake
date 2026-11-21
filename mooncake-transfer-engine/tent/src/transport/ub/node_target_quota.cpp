// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/node_target_quota.h"

#include <utility>

namespace mooncake::tent::ub {

namespace {

// One authoritative ledger per process. The registry holds a strong
// reference so in-process restarts/reinstalls keep the old accounting; the
// quota is frozen at the first install and never hot-updated (guidance
// 4.2.1). Cross-process coordination is not implemented in this increment.
std::mutex g_registry_mutex;
std::shared_ptr<NodeTargetQuota> g_registry_quota;

bool isValidTransition(TargetObjectState from, TargetObjectState to) {
    switch (from) {
        case TargetObjectState::kCreatingReserved:
            return to == TargetObjectState::kActive ||
                   to == TargetObjectState::kQuarantinedUnproven;
        case TargetObjectState::kActive:
            return to == TargetObjectState::kRetiringDeleting ||
                   to == TargetObjectState::kQuarantinedUnproven;
        case TargetObjectState::kRetiringDeleting:
            return to == TargetObjectState::kQuarantinedUnproven;
        case TargetObjectState::kQuarantinedUnproven:
            return false;
    }
    return false;
}

}  // namespace

TargetQuotaTicket::TargetQuotaTicket(TargetQuotaTicket&& other) noexcept
    : quota_(std::move(other.quota_)),
      ticket_id_(other.ticket_id_),
      state_(other.state_) {
    other.detach();
}

TargetQuotaTicket& TargetQuotaTicket::operator=(
    TargetQuotaTicket&& other) noexcept {
    if (this != &other) {
        if (valid()) {
            if (auto quota = quota_.lock()) quota->recordLeakedTicket(*this);
        }
        quota_ = std::move(other.quota_);
        ticket_id_ = other.ticket_id_;
        state_ = other.state_;
        other.detach();
    }
    return *this;
}

TargetQuotaTicket::~TargetQuotaTicket() {
    if (!valid()) return;
    // The ledger may already be destroyed (test registry reset); the weak
    // reference turns the leak notification into a no-op instead of a
    // use-after-free. The slot itself stays charged either way.
    if (auto quota = quota_.lock()) quota->recordLeakedTicket(*this);
}

Status NodeTargetQuota::acquire(uint32_t limit,
                                std::shared_ptr<NodeTargetQuota>& out) {
    if (limit == 0) {
        return Status::InvalidArgument(
            "UB node target quota must be a positive integer; 0 is not "
            "unlimited" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    if (g_registry_quota != nullptr) {
        if (g_registry_quota->limit() != limit) {
            return Status::InvalidArgument(
                "UB node target quota is frozen at install with limit " +
                std::to_string(g_registry_quota->limit()) +
                "; a different limit is not allowed in v1" LOC_MARK);
        }
        out = g_registry_quota;
        return Status::OK();
    }
    g_registry_quota =
        std::shared_ptr<NodeTargetQuota>(new NodeTargetQuota(limit));
    out = g_registry_quota;
    return Status::OK();
}

void NodeTargetQuota::resetRegistryForTest() {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_registry_quota.reset();
}

Status NodeTargetQuota::validateStaticDemand(
    uint64_t required_targets) const {
    if (required_targets > limit_) {
        return Status::InvalidArgument(
            "UB static topology requires " + std::to_string(required_targets) +
            " target jetties but max_target_jetties_per_node is " +
            std::to_string(limit_) + LOC_MARK);
    }
    return Status::OK();
}

std::optional<TargetQuotaTicket> NodeTargetQuota::tryReserve() {
    std::lock_guard<std::mutex> lock(mutex_);
    // Linearization point: the occupancy check and the increment happen
    // under the same lock, so concurrent reservers race here instead of
    // check-then-increment.
    if (tickets_.size() >= limit_) {
        ++rejected_reservations_;
        return std::nullopt;
    }
    const uint64_t ticket_id = next_ticket_id_++;
    tickets_.emplace(ticket_id,
                     TicketEntry{TargetObjectState::kCreatingReserved});
    return TargetQuotaTicket(weak_from_this(), ticket_id,
                             TargetObjectState::kCreatingReserved);
}

Status NodeTargetQuota::transition(TargetQuotaTicket& ticket,
                                   TargetObjectState next) {
    if (!ticket.valid()) {
        return Status::InvalidArgument(
            "UB node target transition requires a valid ticket" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = tickets_.find(ticket.ticketId());
    if (entry == tickets_.end()) {
        return Status::InvalidArgument(
            "UB node target ticket was already released" LOC_MARK);
    }
    if (!isValidTransition(entry->second.state, next)) {
        return Status::InvalidArgument(
            std::string("UB node target transition ") +
            targetObjectStateName(entry->second.state) + " -> " +
            targetObjectStateName(next) + " is not allowed" LOC_MARK);
    }
    entry->second.state = next;
    ticket.state_ = next;
    return Status::OK();
}

Status NodeTargetQuota::releaseUnallocated(TargetQuotaTicket& ticket) {
    if (!ticket.valid()) {
        return Status::InvalidArgument(
            "UB node target release requires a valid ticket" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = tickets_.find(ticket.ticketId());
    if (entry == tickets_.end()) {
        return Status::InvalidArgument(
            "UB node target ticket was already released" LOC_MARK);
    }
    if (entry->second.state != TargetObjectState::kCreatingReserved) {
        return Status::InvalidArgument(
            "UB node target unallocated rollback requires a "
            "creating_reserved ticket; a partial/Unknown create must stay "
            "quarantined" LOC_MARK);
    }
    tickets_.erase(entry);
    ticket.detach();
    return Status::OK();
}

Status NodeTargetQuota::releaseAfterProvenDestruction(
    TargetQuotaTicket& ticket, bool native_destruction_proven) {
    if (!ticket.valid()) {
        return Status::InvalidArgument(
            "UB node target release requires a valid ticket" LOC_MARK);
    }
    if (!native_destruction_proven) {
        return Status::InvalidArgument(
            "UB node target release requires proven native destruction; "
            "keep the ticket quarantined instead" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = tickets_.find(ticket.ticketId());
    if (entry == tickets_.end()) {
        return Status::InvalidArgument(
            "UB node target ticket was already released" LOC_MARK);
    }
    if (entry->second.state != TargetObjectState::kRetiringDeleting &&
        entry->second.state != TargetObjectState::kQuarantinedUnproven) {
        return Status::InvalidArgument(
            "UB node target release requires a retiring_deleting or "
            "quarantined_unproven ticket" LOC_MARK);
    }
    tickets_.erase(entry);
    ticket.detach();
    return Status::OK();
}

void NodeTargetQuota::recordLeakedTicket(const TargetQuotaTicket& ticket) {
    if (!ticket.valid()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (tickets_.count(ticket.ticketId()) == 0) return;
    ++leaked_tickets_;
}

NodeTargetQuotaSnapshot NodeTargetQuota::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    NodeTargetQuotaSnapshot result;
    result.limit = limit_;
    result.rejected_reservations = rejected_reservations_;
    result.leaked_tickets = leaked_tickets_;
    for (const auto& entry : tickets_) {
        switch (entry.second.state) {
            case TargetObjectState::kCreatingReserved:
                ++result.creating_reserved;
                break;
            case TargetObjectState::kActive:
                ++result.active;
                break;
            case TargetObjectState::kRetiringDeleting:
                ++result.retiring_deleting;
                break;
            case TargetObjectState::kQuarantinedUnproven:
                ++result.quarantined_unproven;
                break;
        }
    }
    result.total_occupied = result.creating_reserved + result.active +
                            result.retiring_deleting +
                            result.quarantined_unproven;
    return result;
}

}  // namespace mooncake::tent::ub