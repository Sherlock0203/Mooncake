// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/send_jetty_pool.h"

#include <algorithm>
#include <chrono>

#include <utility>

namespace mooncake::tent::ub {

SliceJettyLease::SliceJettyLease(SliceJettyLease&& other) noexcept
    : pool_(other.pool_),
      member_(std::move(other.member_)),
      attempt_(other.attempt_) {
    other.pool_ = nullptr;
    other.attempt_ = AttemptKey{};
}

SliceJettyLease& SliceJettyLease::operator=(SliceJettyLease&& other) noexcept {
    if (this != &other) {
        if (valid() && pool_ != nullptr) pool_->onLeaseLeaked(member_);
        pool_ = other.pool_;
        member_ = std::move(other.member_);
        attempt_ = other.attempt_;
        other.pool_ = nullptr;
        other.attempt_ = AttemptKey{};
    }
    return *this;
}

SliceJettyLease::~SliceJettyLease() {
    if (valid() && pool_ != nullptr) pool_->onLeaseLeaked(member_);
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
    // W3f: executor threads only touch fully initialized members; the
    // initializer list stored every dependency before the body runs. A thread
    // creation failure degrades to synchronous refill/delete instead of
    // leaving a joinable thread to std::terminate in the destructor.
    try {
        if (options_.background_refill) {
            refill_thread_ = std::thread([this] { refillLoop(); });
        }
        if (options_.background_delete) {
            delete_thread_ = std::thread([this] { deleteLoop(); });
        }
    } catch (const std::system_error&) {
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
    if (adapter_ == nullptr || context_handle_ == nullptr || jfcs_.empty() ||
        budget_ == nullptr || credits_ == nullptr) {
        return Status::InvalidArgument(
            "UB send pool is not fully attached" LOC_MARK);
    }

    for (;;) {
        std::shared_ptr<PoolMember> chosen;
        size_t chosen_jfc = 0;
        uint64_t credit_token = 0;
        uint64_t chosen_error_epoch = 0;
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
            for (auto it = bucket.idle.begin(); it != bucket.idle.end(); ++it) {
                auto token = credits_->tryReserve((*it)->jfc_index, 1);
                if (!token.has_value()) continue;
                chosen = std::move(*it);
                chosen_jfc = chosen->jfc_index;
                credit_token = *token;
                chosen_error_epoch = chosen->error_epoch;
                bucket.idle.erase(it);
                break;
            }
            if (chosen != nullptr) {
                chosen->state = PoolMemberState::kInUse;
                ++bucket.in_use;
                ++total_leases_;
            } else {
                const size_t serving = bucket.idle.size() + bucket.in_use;
                can_create =
                    serving + bucket.creating < options_.desired_serving &&
                    bucket.creating < options_.max_creating;
                if (can_create) ++bucket.creating;
            }
        }

        if (chosen != nullptr) {
            out.lease = SliceJettyLease(this, std::move(chosen), attempt);
            out.jfc_index = chosen_jfc;
            out.jfc_credit_token = credit_token;
            // W3g: the retirement epoch was snapshotted under the same lock
            // that handed out the member, so the claim cannot see a torn value.
            out.error_epoch = chosen_error_epoch;
            return Status::OK();
        }

        if (!can_create) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++pool_misses_;
            return Status::TooManyRequests(
                "UB send pool has no leaseable member; defer the "
                "attempt" LOC_MARK);
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
            "UB native budget exhausted for device " + device_key_ + LOC_MARK);
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
        // partially allocated handle. It was never posted, so one
        // best-effort delete is the natural cleanup; either way the budget
        // reservation is released because no serving object exists.
        if (jetty) (void)adapter_->deleteJetty(jetty);
        budget_->transition(*reservation, BudgetObjectState::kQuarantined);
        budget_->releaseAfterProvenDestruction(*reservation, true);
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
            member->lane = PoolLaneKey{jetty->id(), next_lane_generation_++};
            member->pool_key = pool_key;
            member->jetty = std::move(jetty);
            member->jfc_index = jfc_index;
            member->budget = std::move(*reservation);
            member->state = PoolMemberState::kIdle;
            const Status activated =
                budget_->transition(member->budget, BudgetObjectState::kActive);
            if (activated.ok()) {
                buckets_[pool_key].idle.push_back(member);  // LIFO tail
                ++created_members_;
                publish = true;
            }
        }
    }

    if (publish) return Status::OK();

    // Stopped (or activation failed) after creation: retire the fresh
    // object. It never posted a WR, so deletion proves destruction and the
    // budget can be released.
    if (member != nullptr) {
        reservation = std::move(member->budget);
        budget_->transition(*reservation, BudgetObjectState::kQuarantined);
        adapter_->deleteJetty(member->jetty);
        budget_->releaseAfterProvenDestruction(*reservation, true);
    } else {
        budget_->transition(*reservation, BudgetObjectState::kQuarantined);
        adapter_->deleteJetty(jetty);
        budget_->releaseAfterProvenDestruction(*reservation, true);
    }
    return Status::TooManyRequests(
        "UB send pool stopped while creating a member" LOC_MARK);
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

void SendJettyPool::retireMemberLocked(std::shared_ptr<PoolMember> member,
                                       const std::string& reason) {
    member->state = PoolMemberState::kDraining;
    member->retire_reason = reason;
    // W3g: every retirement raises the epoch. A lease taken before this point
    // can never claim again, even if the member object is briefly reused for
    // delete/drain bookkeeping.
    ++member->error_epoch;
    // First real error retires permanently (INV07): the member never
    // re-enters the idle stack even if the path recovers.
    if (options_.background_delete && !delete_stop_) {
        // W3f: the delete executor owns native destruction from here, so the
        // return/completion path never blocks on reset+delete. Shutdown makes
        // the hand-off synchronous again (beginStop joins the executor).
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
        // W3f (ported from the pooling side): the provider requires a RESET
        // Jetty before delete. A clean member was never reset, while an
        // errored member is in ERROR after its flush fence; a reset that fails
        // (for example an ERROR Jetty without a trusted barrier) keeps the
        // handle quarantined for a later retry instead of deleting it blindly.
        const Status reset = adapter_->resetJetty(jetty);
        const Status deleted =
            reset.ok() ? adapter_->deleteJetty(jetty) : reset;
        if (deleted.ok()) {
            member->state = PoolMemberState::kDestroyed;
            budget_->transition(member->budget, BudgetObjectState::kRetiring);
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

void SendJettyPool::onLeaseLeaked(const std::shared_ptr<PoolMember>& member) {
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
    // A leaked lease removes the member from circulation just like a
    // retirement; the epoch must move so a stale claim cannot succeed.
    ++member->error_epoch;
    budget_->transition(member->budget, BudgetObjectState::kQuarantined);
    quarantined_.push_back(member);
}

ClaimVerdict SendJettyPool::tryClaimPost(const PoolKey& pool_key,
                                         const AttemptKey& attempt,
                                         const ClaimRequest& request) const {
    // Read-only verdict (IN-1): no member state changes, no counters, no
    // provider call. The fixed check order mirrors the pooling-side design
    // (W3g §6.1): stopping -> bucket -> member state -> generation -> epoch.
    //
    // A null member (or a member that never belonged to this pool key) is
    // reported as retired: the lease is the only proof of identity the caller
    // has, and without it there is nothing this pool can vouch for.
    //
    // Only kPoolStopping is reachable under today's single-lease model; the
    // other verdicts are defensive and covered by unit tests (see the header).
    if (!attempt.valid() || request.member == nullptr) {
        return ClaimVerdict{ClaimClosed::kMemberRetired};
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return ClaimVerdict{ClaimClosed::kPoolStopping};
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) {
        return ClaimVerdict{ClaimClosed::kMemberRetired};
    }
    const PoolMember& member = *request.member;
    if (member.pool_key != pool_key ||
        member.state != PoolMemberState::kInUse) {
        return ClaimVerdict{ClaimClosed::kMemberRetired};
    }
    if (member.lane.lane_generation != request.lane_generation) {
        return ClaimVerdict{ClaimClosed::kGenerationMoved};
    }
    if (member.error_epoch != request.error_epoch_snapshot) {
        return ClaimVerdict{ClaimClosed::kErrorEpochMoved};
    }
    return ClaimVerdict{ClaimClosed::kNone};
}

void SendJettyPool::beginStop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    // W3f: no creation or native destruction may happen on executor threads
    // after the stop; joining bounds the window in which an executor owns a
    // provider call. Retirements after this point destroy synchronously.
    stopRefillExecutor();
    stopDeleteExecutor();
}

bool SendJettyPool::stopping() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stopping_;
}

Status SendJettyPool::drainIdle() {
    // W3f shutdown barrier: beginStop joins both executors, so after this call
    // no native create/delete runs on an executor thread and the caller may
    // close the parent JFC safely.
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
        const Status deleted = adapter_->deleteJetty(jetty);
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
        refill_cv_.wait_for(lock, interval, [this] { return refill_stop_; });
        if (refill_stop_) break;

        // Claim creation slots under the lock, then call the provider outside
        // it. Every known PoolKey is topped back up to the static Serving size
        // even while no business attempt is in flight.
        std::vector<PoolKey> batch;
        for (auto& entry : buckets_) {
            PoolBucket& bucket = entry.second;
            const size_t serving = bucket.idle.size() + bucket.in_use;
            if (serving + bucket.creating >= options_.desired_serving) {
                continue;
            }
            if (bucket.creating >= options_.max_creating) continue;
            size_t slots = options_.desired_serving - serving - bucket.creating;
            slots = std::min(slots, options_.max_creating - bucket.creating);
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
        delete_cv_.wait(
            lock, [this] { return delete_stop_ || !delete_ready_.empty(); });
        if (delete_ready_.empty()) {
            if (delete_stop_) break;
            continue;
        }
        auto batch = std::move(delete_ready_);
        delete_ready_.clear();
        lock.unlock();
        // W3f: reset + delete + budget release happen on this executor, never
        // on the completion/return path.
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
    result.retired_members = retired_.size();
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
    for (const auto& member : quarantined_) states.push_back(member->state);
    return states;
}

size_t SendJettyPool::servingCount(const PoolKey& pool_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) return 0;
    return bucket->second.idle.size() + bucket->second.in_use;
}

size_t SendJettyPool::idleCount(const PoolKey& pool_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) return 0;
    return bucket->second.idle.size();
}

size_t SendJettyPool::creatingCount(const PoolKey& pool_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) return 0;
    return bucket->second.creating;
}

}  // namespace mooncake::tent::ub
