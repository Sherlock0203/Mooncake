// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/send_jetty_pool.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <system_error>
#include <utility>

namespace mooncake::tent::ub {

SliceJettyLease::SliceJettyLease(SliceJettyLease&& other) noexcept
    : pool_(std::move(other.pool_)),
      member_(std::move(other.member_)),
      attempt_(other.attempt_) {
    other.attempt_ = AttemptKey{};
}

SliceJettyLease& SliceJettyLease::operator=(
    SliceJettyLease&& other) noexcept {
    if (this != &other) {
        if (valid()) {
            if (auto pool = pool_.lock()) pool->onLeaseLeaked(member_);
        }
        pool_ = std::move(other.pool_);
        member_ = std::move(other.member_);
        attempt_ = other.attempt_;
        other.attempt_ = AttemptKey{};
    }
    return *this;
}

SliceJettyLease::~SliceJettyLease() {
    if (!valid()) return;
    // RC-04: the pool may already be destroyed; the weak reference turns
    // the leak notification into a no-op instead of a use-after-free. The
    // member's JettyPtr still releases its handle best-effort.
    if (auto pool = pool_.lock()) pool->onLeaseLeaked(member_);
}

SendJettyPool::SendJettyPool(std::shared_ptr<UrmaAdapter> adapter,
                             ContextPtr context_handle,
                             std::vector<std::shared_ptr<UbJfc>> jfcs,
                             std::shared_ptr<NativeBudget> budget,
                             std::string device_key,
                             std::shared_ptr<JfcCreditLedger> credits,
                             SendJettyPoolOptions options)
    : adapter_(std::move(adapter)),
      context_handle_(std::move(context_handle)),
      jfcs_(std::move(jfcs)),
      budget_(std::move(budget)),
      device_key_(std::move(device_key)),
      credits_(std::move(credits)),
      options_(options) {
    // Executor threads only touch fully initialized members: the initializer
    // list has already stored every dependency before the body runs.
    try {
        if (options_.background_refill) {
            refill_thread_ = std::thread([this] { refillLoop(); });
        }
        if (options_.background_delete) {
            delete_thread_ = std::thread([this] { deleteLoop(); });
        }
    } catch (const std::system_error&) {
        // RC-06: thread creation can fail under resource pressure. Degrade
        // to synchronous refill/delete instead of leaving a joinable thread
        // to std::terminate in the destructor; the stop flags also route
        // every later retirement to the synchronous path.
        stopRefillExecutor();
        stopDeleteExecutor();
    }
}

SendJettyPool::~SendJettyPool() {
    stopRefillExecutor();
    stopDeleteExecutor();
    (void)drainQuarantined();
}

Status SendJettyPool::tryLease(const PoolKey& pool_key,
                               const AttemptKey& attempt,
                               LeaseWithCredit& out) {
    out = LeaseWithCredit{};
    if (!attempt.valid()) {
        return Status::InvalidArgument(
            "UB send pool lease requires a valid attempt key" LOC_MARK);
    }
    if (adapter_ == nullptr || context_handle_ == nullptr ||
        jfcs_.empty() || budget_ == nullptr || credits_ == nullptr) {
        return Status::InvalidArgument(
            "UB send pool is not fully attached" LOC_MARK);
    }

    for (;;) {
        std::shared_ptr<PoolMember> chosen;
        size_t chosen_jfc = 0;
        uint64_t credit_token = 0;
        bool can_create = false;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                ++stop_rejections_;
                return Status::TooManyRequests(
                    "UB send pool is stopping; defer the attempt" LOC_MARK);
            }
            PoolBucket& bucket = buckets_[pool_key];
            // LIFO scan: newest returned member first; JFC credit matching
            // happens inside the LIFO order (non-blocking reservation).
            for (auto it = bucket.idle.begin(); it != bucket.idle.end();
                 ++it) {
                auto token = credits_->tryReserve((*it)->jfc_index, 1);
                if (!token.has_value()) continue;
                chosen = std::move(*it);
                chosen_jfc = chosen->jfc_index;
                credit_token = *token;
                bucket.idle.erase(it);
                break;
            }
            if (chosen != nullptr) {
                chosen->state = PoolMemberState::kInUse;
                ++bucket.in_use;
                ++total_leases_;
            } else {
                const size_t serving = bucket.idle.size() + bucket.in_use;
                can_create = serving + bucket.creating <
                                 options_.desired_serving &&
                             bucket.creating < options_.max_creating;
                if (can_create) ++bucket.creating;
            }
        }

        if (chosen != nullptr) {
            out.lease =
                SliceJettyLease(weak_from_this(), std::move(chosen), attempt);
            out.jfc_index = chosen_jfc;
            out.jfc_credit_token = credit_token;
            return Status::OK();
        }

        if (!can_create) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++pool_misses_;
            return Status::TooManyRequests(
                "UB send pool has no leaseable member; defer the attempt"
                LOC_MARK);
        }

        // Creation happens outside the pool lock; the slot was claimed
        // above so concurrent leases cannot double count it.
        const Status created = createAndPublish(pool_key);
        if (!created.ok()) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++pool_misses_;
            return Status::TooManyRequests(
                "UB send pool could not create a member this round; defer "
                "the attempt" LOC_MARK);
        }
        // Loop back: the published member is at the LIFO tail and the scan
        // will find it (or another concurrent leaser may have taken it, in
        // which case we re-evaluate deficit/miss).
    }
}

Status SendJettyPool::createAndPublish(const PoolKey& pool_key) {
    // Budget first: a member without device budget must never be created.
    BudgetQuantities quantities;
    quantities.jetties = 1;
    auto reservation =
        budget_->tryReserve(device_key_, BudgetRole::kBusiness, quantities);
    if (!reservation.has_value()) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto bucket = buckets_.find(pool_key);
        if (bucket != buckets_.end() && bucket->second.creating > 0) {
            --bucket->second.creating;
        }
        ++create_failures_;
        return Status::TooManyRequests(
            "UB native budget exhausted for device " + device_key_ +
            LOC_MARK);
    }

    size_t jfc_index = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        jfc_index = next_jfc_rotation_++ % jfcs_.size();
    }

    JettyPtr jetty;
    const Status created =
        adapter_->createJetty(context_handle_, jfcs_[jfc_index]->handle(),
                              options_.jetty_options, jetty);

    if (!created.ok()) {
        // Provider contract: a failed createJetty may still hand back a
        // partially allocated handle. It was never posted, so it retires
        // through the RESET-before-delete contract; the budget is released
        // only when destruction is proven.
        retireUnpublished(std::move(jetty), std::move(*reservation), pool_key);
        std::lock_guard<std::mutex> lock(mutex_);
        auto bucket = buckets_.find(pool_key);
        if (bucket != buckets_.end() && bucket->second.creating > 0) {
            --bucket->second.creating;
        }
        ++create_failures_;
        return created;
    }

    // Re-check under the lock: publish only when the pool is still
    // accepting; otherwise the fresh Jetty is retired (stopped result).
    std::shared_ptr<PoolMember> member;
    bool publish = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto bucket = buckets_.find(pool_key);
        if (bucket != buckets_.end() && bucket->second.creating > 0) {
            --bucket->second.creating;
        }
        if (!stopping_) {
            member = std::make_shared<PoolMember>();
            member->lane = LaneKey{jetty->id(), next_lane_generation_++};
            member->pool_key = pool_key;
            member->jetty = std::move(jetty);
            member->jfc_index = jfc_index;
            member->budget = std::move(*reservation);
            member->state = PoolMemberState::kIdle;
            const Status activated = budget_->transition(
                member->budget, BudgetObjectState::kActive);
            if (activated.ok()) {
                buckets_[pool_key].idle.push_back(member);  // LIFO tail
                ++created_members_;
                publish = true;
            }
        }
    }

    if (publish) return Status::OK();

    // Stopped (or activation failed) after creation: retire the fresh
    // object through the RESET-before-delete contract. It never posted a
    // WR; a failed destruction keeps the handle and budget quarantined.
    JettyPtr doomed =
        member != nullptr ? std::move(member->jetty) : std::move(jetty);
    if (member != nullptr) reservation = std::move(member->budget);
    retireUnpublished(std::move(doomed), std::move(*reservation), pool_key);
    return Status::TooManyRequests(
        "UB send pool stopped while creating a member" LOC_MARK);
}

void SendJettyPool::retireUnpublished(JettyPtr jetty,
                                      BudgetReservation reservation,
                                      const PoolKey& pool_key) {
    // Destroys a Jetty that never published (failed or stopped creation).
    // A live handle must pass the RESET-before-delete contract required by
    // the provider; a partially allocated handle without a native resource
    // only needs its wrapper released.
    budget_->transition(reservation, BudgetObjectState::kQuarantined);
    Status destroyed = Status::OK();
    if (jetty != nullptr) {
        if (jetty->valid()) {
            const Status reset = adapter_->resetJetty(jetty);
            destroyed = reset.ok() ? adapter_->deleteJetty(jetty) : reset;
        } else {
            destroyed = adapter_->deleteJetty(jetty);
        }
    }
    if (destroyed.ok()) {
        budget_->releaseAfterProvenDestruction(reservation, true);
        return;
    }
    // Unknown destruction: keep the handle and the budget (INV09/INV10) so
    // a later drainQuarantined can retry the native delete.
    auto member = std::make_shared<PoolMember>();
    member->pool_key = pool_key;
    member->jetty = std::move(jetty);
    member->budget = std::move(reservation);
    member->state = PoolMemberState::kQuarantined;
    member->retire_reason = "unpublished creation retired";
    std::lock_guard<std::mutex> lock(mutex_);
    quarantined_.push_back(std::move(member));
}

Status SendJettyPool::returnOnce(SliceJettyLease lease,
                                 bool completion_gate_passed) {
    if (!lease.valid()) {
        return Status::InvalidArgument(
            "UB send pool return requires a valid lease" LOC_MARK);
    }
    auto member = lease.member();
    lease.detach();

    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (member->state != PoolMemberState::kInUse) {
            ++double_returns_;
            return Status::InvalidArgument(
                "UB send pool lease returned twice" LOC_MARK);
        }
        auto bucket = buckets_.find(member->pool_key);
        if (bucket != buckets_.end() && bucket->second.in_use > 0) {
            --bucket->second.in_use;
        }
        ++total_returns_;
        if (completion_gate_passed && !stopping_) {
            member->state = PoolMemberState::kIdle;
            buckets_[member->pool_key].idle.push_front(member);  // LIFO head
        } else {
            retireMemberLocked(member,
                               stopping_ ? "pool stopping" : "gate failed");
            to_destroy = retired_;
            retired_.clear();
        }
    }

    if (!to_destroy.empty()) destroyRetired(std::move(to_destroy));
    return Status::OK();
}

Status SendJettyPool::retireFaulted(SliceJettyLease lease,
                                    const std::string& reason) {
    if (!lease.valid()) {
        return Status::InvalidArgument(
            "UB send pool retire requires a valid lease" LOC_MARK);
    }
    auto member = lease.member();
    lease.detach();

    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (member->state != PoolMemberState::kInUse) {
            ++double_returns_;
            return Status::InvalidArgument(
                "UB send pool lease retired twice" LOC_MARK);
        }
        auto bucket = buckets_.find(member->pool_key);
        if (bucket != buckets_.end() && bucket->second.in_use > 0) {
            --bucket->second.in_use;
        }
        ++total_returns_;
        retireMemberLocked(member, reason.empty() ? "fault" : reason);
        to_destroy = std::move(retired_);
        retired_.clear();
    }

    if (!to_destroy.empty()) destroyRetired(std::move(to_destroy));
    return Status::OK();
}

void SendJettyPool::retireMemberLocked(
    std::shared_ptr<PoolMember> member, const std::string& reason) {
    member->state = PoolMemberState::kDraining;
    member->retire_reason = reason;
    // First real error retires permanently (INV07): the member never
    // re-enters the idle stack even if the path recovers.
    if (options_.background_delete && !delete_stop_) {
        // The delete executor owns the native destruction from here; the
        // caller returns immediately instead of blocking on the provider.
        member->state = PoolMemberState::kDeleteReady;
        delete_ready_.push_back(std::move(member));
        delete_cv_.notify_one();
        return;
    }
    retired_.push_back(std::move(member));
}

void SendJettyPool::destroyRetired(
    std::vector<std::shared_ptr<PoolMember>> members) {
    // All native calls happen outside the pool lock.
    for (auto& member : members) {
        member->state = PoolMemberState::kDeleting;
        JettyPtr jetty = std::move(member->jetty);
        // The native delete requires a RESET Jetty. A clean member has never
        // been reset, while an errored member is in ERROR after its flush
        // fence. A reset that fails (for example an ERROR Jetty without a
        // trusted barrier) keeps the handle quarantined for a later retry.
        const Status reset = adapter_->resetJetty(jetty);
        const Status deleted =
            reset.ok() ? adapter_->deleteJetty(jetty) : reset;
        if (deleted.ok()) {
            member->state = PoolMemberState::kDestroyed;
            budget_->transition(member->budget,
                                BudgetObjectState::kRetiring);
            budget_->releaseAfterProvenDestruction(member->budget, true);
        } else {
            // Unknown destruction: keep owner and budget (INV09/INV10).
            // The handle goes back to the member so the next drain attempt
            // can retry the native delete; dropping it here would lose the
            // native ownership and leak the budget.
            member->jetty = std::move(jetty);
            member->state = PoolMemberState::kQuarantined;
            budget_->transition(member->budget,
                                BudgetObjectState::kQuarantined);
            std::lock_guard<std::mutex> lock(mutex_);
            quarantined_.push_back(member);
        }
    }
}

void SendJettyPool::onLeaseLeaked(
    const std::shared_ptr<PoolMember>& member) {
    // A lease dropped without returnOnce(): responsibility is unknown, so
    // the member is quarantined with its budget retained; it never returns
    // to circulation.
    std::lock_guard<std::mutex> lock(mutex_);
    ++leaked_leases_;
    if (member == nullptr) return;
    if (member->state == PoolMemberState::kInUse) {
        auto bucket = buckets_.find(member->pool_key);
        if (bucket != buckets_.end() && bucket->second.in_use > 0) {
            --bucket->second.in_use;
        }
    }
    member->state = PoolMemberState::kQuarantined;
    budget_->transition(member->budget, BudgetObjectState::kQuarantined);
    quarantined_.push_back(member);
}

void SendJettyPool::beginStop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    // No creation or native destruction may happen on executor threads after
    // the stop; joining bounds the window in which an executor owns a
    // provider call. Retirements after this point destroy synchronously.
    stopRefillExecutor();
    stopDeleteExecutor();
}

bool SendJettyPool::stopping() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stopping_;
}

Status SendJettyPool::drainIdle() {
    // Shutdown barrier: beginStop joins both executor threads, so after this
    // call no native create/delete runs on an executor thread and the caller
    // may close the parent JFC safely.
    beginStop();
    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& entry : buckets_) {
            for (auto& member : entry.second.idle) {
                member->retire_reason = "drain idle";
                to_destroy.push_back(std::move(member));
            }
            entry.second.idle.clear();
        }
        // RC-03 guard: with every consumer stopped and both executors
        // joined, nothing may still be leased or mid-creation. A non-zero
        // count means the shutdown ordering contract was violated; surface
        // it instead of silently closing the parent JFC under live members.
        size_t in_use = 0;
        size_t creating = 0;
        for (const auto& entry : buckets_) {
            in_use += entry.second.in_use;
            creating += entry.second.creating;
        }
        if (in_use != 0 || creating != 0) {
            LOG(ERROR) << "UB send pool drainIdle found " << in_use
                       << " in-use and " << creating
                       << " creating members after the shutdown barrier; "
                          "stop every posting/polling consumer before the "
                          "context shutdown"
                       << LOC_MARK;
        }
    }
    destroyRetired(std::move(to_destroy));
    return Status::OK();
}

Status SendJettyPool::drainQuarantined() {
    std::vector<std::shared_ptr<PoolMember>> candidates;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        candidates = std::move(quarantined_);
        quarantined_.clear();
    }
    for (auto& member : candidates) {
        if (member->jetty == nullptr) {
            member->state = PoolMemberState::kDestroyed;
            budget_->releaseAfterProvenDestruction(member->budget, true);
            continue;
        }
        JettyPtr jetty = std::move(member->jetty);
        // The native delete requires a RESET Jetty, exactly like the primary
        // destroy path. A reset failure (for example an ERROR Jetty without
        // a trusted flush fence) keeps the handle quarantined for a retry.
        const Status reset = adapter_->resetJetty(jetty);
        const Status deleted =
            reset.ok() ? adapter_->deleteJetty(jetty) : reset;
        if (deleted.ok()) {
            member->state = PoolMemberState::kDestroyed;
            budget_->releaseAfterProvenDestruction(member->budget, true);
        } else {
            // Keep the handle owned by the pool for the next drain attempt.
            member->jetty = std::move(jetty);
            std::lock_guard<std::mutex> lock(mutex_);
            quarantined_.push_back(member);
        }
    }
    return Status::OK();
}

void SendJettyPool::refillLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto interval = std::chrono::milliseconds(
        std::max<uint32_t>(1, options_.refill_interval_ms));
    while (!refill_stop_) {
        refill_cv_.wait_for(lock, interval,
                            [this] { return refill_stop_; });
        if (refill_stop_) break;

        // Claim creation slots under the lock, then call the provider
        // outside it. Every known PoolKey is topped back up to the static
        // Serving size even while no business attempt is in flight.
        std::vector<PoolKey> batch;
        for (auto& entry : buckets_) {
            PoolBucket& bucket = entry.second;
            const size_t serving = bucket.idle.size() + bucket.in_use;
            if (serving + bucket.creating >= options_.desired_serving) {
                continue;
            }
            if (bucket.creating >= options_.max_creating) continue;
            size_t slots =
                options_.desired_serving - serving - bucket.creating;
            slots = std::min(slots,
                             options_.max_creating - bucket.creating);
            bucket.creating += slots;
            for (size_t slot = 0; slot < slots; ++slot) {
                batch.push_back(entry.first);
            }
        }
        lock.unlock();
        for (const auto& pool_key : batch) {
            (void)createAndPublish(pool_key);
        }
        lock.lock();
    }
}

void SendJettyPool::deleteLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
        delete_cv_.wait(lock, [this] {
            return delete_stop_ || !delete_ready_.empty();
        });
        if (delete_ready_.empty()) {
            if (delete_stop_) break;
            continue;
        }
        auto batch = std::move(delete_ready_);
        delete_ready_.clear();
        lock.unlock();
        // reset + delete + budget release happen on this executor, never on
        // the completion/return path.
        destroyRetired(std::move(batch));
        lock.lock();
    }
}

void SendJettyPool::stopRefillExecutor() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        refill_stop_ = true;
    }
    refill_cv_.notify_all();
    if (refill_thread_.joinable()) refill_thread_.join();
}

void SendJettyPool::stopDeleteExecutor() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        delete_stop_ = true;
    }
    delete_cv_.notify_all();
    if (delete_thread_.joinable()) delete_thread_.join();
    // Retirements that raced with the join are drained synchronously so the
    // pool leaves no delete-ready member behind.
    std::vector<std::shared_ptr<PoolMember>> pending;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending = std::move(delete_ready_);
        delete_ready_.clear();
    }
    if (!pending.empty()) destroyRetired(std::move(pending));
}

SendJettyPoolStats SendJettyPool::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    SendJettyPoolStats result{};
    for (const auto& entry : buckets_) {
        result.idle_members += entry.second.idle.size();
        result.in_use_members += entry.second.in_use;
        result.creating_members += entry.second.creating;
    }
    result.retired_members = retired_.size() + delete_ready_.size();
    result.quarantined_members = quarantined_.size();
    result.total_leases = total_leases_;
    result.total_returns = total_returns_;
    result.pool_misses = pool_misses_;
    result.stop_rejections = stop_rejections_;
    result.leaked_leases = leaked_leases_;
    result.created_members = created_members_;
    result.create_failures = create_failures_;
    result.double_returns = double_returns_;
    return result;
}

std::vector<PoolMemberState> SendJettyPool::memberStates() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PoolMemberState> states;
    for (const auto& entry : buckets_) {
        for (const auto& member : entry.second.idle) {
            states.push_back(member->state);
        }
        states.insert(states.end(), entry.second.in_use,
                      PoolMemberState::kInUse);
    }
    for (const auto& member : retired_) states.push_back(member->state);
    // delete_ready_ members are owned by the delete executor, which mutates
    // their state without the pool lock; report the queue state instead.
    states.insert(states.end(), delete_ready_.size(),
                  PoolMemberState::kDeleteReady);
    for (const auto& member : quarantined_) states.push_back(member->state);
    return states;
}

size_t SendJettyPool::servingCount(const PoolKey& pool_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) return 0;
    return bucket->second.idle.size() + bucket->second.in_use;
}

}  // namespace mooncake::tent::ub
