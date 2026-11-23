// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/send_jetty_pool.h"

#include "tent/transport/ub/jetty_drain_controller.h"
#include "tent/transport/ub/jetty_refill_worker.h"
#include "tent/transport/ub/jetty_retire_worker.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <system_error>
#include <utility>

namespace mooncake::tent::ub {
namespace {

uint64_t steadyNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

// Deterministic, bounded exponential backoff for a PoolKey whose creation
// failed (design §7.3 BACKOFF). The per-key phase keeps many failing keys from
// retrying in lockstep without making a single key's schedule unpredictable.
uint64_t createBackoffNs(uint32_t attempts, uint32_t base_ms, uint32_t cap_ms,
                         const PoolKey& key) {
    const uint32_t capped = std::min<uint32_t>(attempts, 10);
    uint64_t delay_ms = std::max<uint32_t>(1, base_ms);
    const uint64_t cap = std::max<uint64_t>(1, cap_ms);
    delay_ms = std::min<uint64_t>(delay_ms << capped, cap);
    const size_t phase = PoolKeyHash{}(key);
    delay_ms += phase % (delay_ms / 4 + 1);
    return delay_ms * 1'000'000ULL;
}

}  // namespace

SliceJettyLease::SliceJettyLease(SliceJettyLease&& other) noexcept
    : pool_(std::move(other.pool_)),
      member_(std::move(other.member_)),
      attempt_(other.attempt_) {
    other.pool_.reset();
    other.attempt_ = AttemptKey{};
}

SliceJettyLease& SliceJettyLease::operator=(SliceJettyLease&& other) noexcept {
    if (this != &other) {
        if (valid()) {
            if (auto pool = pool_.lock()) pool->onLeaseLeaked(member_);
        }
        pool_ = std::move(other.pool_);
        member_ = std::move(other.member_);
        attempt_ = other.attempt_;
        other.pool_.reset();
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
    // list has already stored every dependency before the body runs. A failed
    // thread start degrades to synchronous destruction / demand-driven
    // creation instead of terminating the process later (RC-06).
    startWorkers();
}

SendJettyPool::~SendJettyPool() {
    stopWorkers();
    (void)drainQuarantined();
}

Status SendJettyPool::tryLease(const PoolKey& pool_key,
                               const AttemptKey& attempt,
                               LeaseWithCredit& out) {
    // Never clobber a non-empty output: overwriting it would drop the
    // caller's previous lease (quarantining its member) and lose its JFC
    // credit token without a release.
    if (out.lease.valid() || out.jfc_credit_token != 0) {
        return Status::InvalidArgument(
            "UB send pool lease output still holds an unsettled lease or "
            "JFC credit; settle it before leasing again" LOC_MARK);
    }
    if (!pool_key.valid()) {
        return Status::InvalidArgument(
            "UB send pool lease requires a valid pool key" LOC_MARK);
    }
    if (!attempt.valid()) {
        return Status::InvalidArgument(
            "UB send pool lease requires a valid attempt key" LOC_MARK);
    }
    if (options_.desired_serving == 0 || options_.max_creating == 0) {
        return Status::InvalidArgument(
            "UB send pool requires positive desired_serving and max_creating"
            LOC_MARK);
    }
    if (adapter_ == nullptr || context_handle_ == nullptr || jfcs_.empty() ||
        budget_ == nullptr || credits_ == nullptr) {
        return Status::InvalidArgument(
            "UB send pool is not fully attached" LOC_MARK);
    }
    out = LeaseWithCredit{};

    for (;;) {
        std::shared_ptr<PoolMember> chosen;
        size_t chosen_jfc = 0;
        uint64_t credit_token = 0;
        uint64_t chosen_error_epoch = 0;
        bool can_create = false;
        size_t idle_before = 0;
        size_t in_use_before = 0;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                ++stop_rejections_;
                return Status::TooManyRequests(
                    "UB send pool is stopping; defer the attempt" LOC_MARK);
            }
            if (admissionBlockedLocked()) {
                // The recovery budget is exhausted: new admissions stop
                // instead of shedding or TTL-evicting an unproven owner.
                ++backpressure_rejections_;
                return Status::TooManyRequests(
                    "UB send pool is under recovery backpressure; defer the "
                    "attempt" LOC_MARK);
            }
            PoolBucket& bucket = buckets_[pool_key];
            idle_before = bucket.idle.size();
            in_use_before = bucket.in_use;
            // LIFO scan: newest returned member first; JFC credit matching
            // happens inside the LIFO order (non-blocking reservation).
            for (auto it = bucket.idle.begin(); it != bucket.idle.end(); ++it) {
                auto token = credits_->tryReserve((*it)->jfc_index, 1);
                if (!token.has_value()) continue;
                chosen = std::move(*it);
                chosen_jfc = chosen->jfc_index;
                credit_token = *token;
                // Copied under the pool mutex: a claim compares this snapshot
                // after the lease without re-reading the member.
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
            const uint32_t lane_jetty = chosen->lane.jetty_id;
            const uint64_t lane_generation = chosen->lane.lane_generation;
            const uint64_t pool_generation = pool_key.context_incarnation;
            out.lease =
                SliceJettyLease(weak_from_this(), std::move(chosen), attempt);
            out.jfc_index = chosen_jfc;
            out.jfc_credit_token = credit_token;
            out.error_epoch = chosen_error_epoch;
            if (options_.verbose_events || VLOG_IS_ON(1)) {
                if (options_.verbose_events) {
                    LOG(INFO)
                        << "UB pool lease: device_key=" << device_key_
                        << " pool_gen=" << pool_generation
                        << " lane_gen=" << lane_generation
                        << " jetty=" << lane_jetty
                        << " task=" << attempt.task_id
                        << " slice=" << attempt.slice_id
                        << " attempt=" << attempt.attempt_generation
                        << " jfc=" << chosen_jfc << " credit=" << credit_token
                        << " idle_before=" << idle_before
                        << " in_use_before=" << in_use_before << " status=ok";
                } else {
                    VLOG(1)
                        << "UB pool lease: device_key=" << device_key_
                        << " pool_gen=" << pool_generation
                        << " lane_gen=" << lane_generation
                        << " jetty=" << lane_jetty
                        << " task=" << attempt.task_id
                        << " slice=" << attempt.slice_id
                        << " attempt=" << attempt.attempt_generation
                        << " jfc=" << chosen_jfc << " credit=" << credit_token
                        << " idle_before=" << idle_before
                        << " in_use_before=" << in_use_before << " status=ok";
                }
            } else {
                LOG_EVERY_N(INFO, options_.sample_every == 0
                                      ? 1024
                                      : options_.sample_every)
                    << "UB pool lease: device_key=" << device_key_
                    << " pool_gen=" << pool_generation
                    << " lane_gen=" << lane_generation
                    << " jetty=" << lane_jetty << " task=" << attempt.task_id
                    << " slice=" << attempt.slice_id
                    << " attempt=" << attempt.attempt_generation
                    << " jfc=" << chosen_jfc << " credit=" << credit_token
                    << " idle_before=" << idle_before
                    << " in_use_before=" << in_use_before << " status=ok";
            }
            return Status::OK();
        }

        if (!can_create) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++pool_misses_;
            if (options_.verbose_events || VLOG_IS_ON(1)) {
                if (options_.verbose_events) {
                    LOG(INFO) << "UB pool lease: device_key=" << device_key_
                              << " pool_gen=" << pool_key.context_incarnation
                              << " task=" << attempt.task_id
                              << " slice=" << attempt.slice_id
                              << " attempt=" << attempt.attempt_generation
                              << " idle_before=" << idle_before
                              << " in_use_before=" << in_use_before
                              << " status=miss reason=no_leaseable_member";
                } else {
                    VLOG(1) << "UB pool lease: device_key=" << device_key_
                            << " pool_gen=" << pool_key.context_incarnation
                            << " task=" << attempt.task_id
                            << " slice=" << attempt.slice_id
                            << " attempt=" << attempt.attempt_generation
                            << " idle_before=" << idle_before
                            << " in_use_before=" << in_use_before
                            << " status=miss reason=no_leaseable_member";
                }
            }
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

bool SendJettyPool::admissionBlockedLocked() const {
    if (quarantine_backpressure_) return true;
    // Design §9.2 runtime brake: once the drain controller proved that the
    // provider cannot drain, admitting new slices would only build up members
    // that can never be retired. Stop admission instead of relying on the
    // quarantine budget to notice.
    return drain_controller_ != nullptr &&
           !drain_controller_->asyncDrainSupported();
}

void SendJettyPool::refreshBackpressureLocked() {
    quarantine_backpressure_ =
        options_.quarantine_budget != 0 &&
        quarantined_.size() >= options_.quarantine_budget;
    tombstone_backpressure_ =
        options_.tombstones != nullptr && options_.tombstones->full();
}

void SendJettyPool::noteTombstoneLocked(
    const std::shared_ptr<PoolMember>& member) {
    if (!member || options_.tombstones == nullptr) return;
    if (member->tombstone_recorded) return;
    JettyTombstone stone;
    stone.context_incarnation = member->pool_key.context_incarnation;
    stone.kind = EntityKind::kJetty;
    stone.native_id = member->lane.jetty_id;
    stone.generation = member->lane.lane_generation;
    stone.error_epoch = member->error_epoch;
    stone.responsibility = Responsibility::kUnknown;
    stone.budget_hold = member->budget.quantities.jetties;
    if (options_.tombstones->insert(stone).ok()) {
        member->tombstone_recorded = true;
    } else {
        ++tombstone_insert_failures_;
        refreshBackpressureLocked();
    }
}

bool SendJettyPool::eraseTombstoneForProvenDestruction(PoolMember& member) {
    if (options_.tombstones == nullptr) return true;
    if (!member.tombstone_recorded) {
        // A member can reach destruction without a recorded retirement (for
        // example a creation retired synchronously). Record it now, so the
        // release still has the entry it erases.
        JettyTombstone stone;
        stone.context_incarnation = member.pool_key.context_incarnation;
        stone.kind = EntityKind::kJetty;
        stone.native_id = member.lane.jetty_id;
        stone.generation = member.lane.lane_generation;
        stone.error_epoch = member.error_epoch;
        stone.responsibility = Responsibility::kUnknown;
        stone.budget_hold = member.budget.quantities.jetties;
        if (!options_.tombstones->insert(stone).ok()) {
            ++tombstone_insert_failures_;
            std::lock_guard<std::mutex> lock(mutex_);
            refreshBackpressureLocked();
            return false;
        }
        member.tombstone_recorded = true;
    }
    if (!options_.tombstones->erase(member.lane.lane_generation)) {
        ++tombstone_erase_misses_;
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    refreshBackpressureLocked();
    return true;
}

void SendJettyPool::noteCreateFailureLocked(const PoolKey& pool_key) {
    PoolBucket& bucket = buckets_[pool_key];
    ++bucket.backoff_attempts;
    bucket.backoff_until_ns =
        steadyNowNs() + createBackoffNs(bucket.backoff_attempts,
                                        options_.refill_interval_ms,
                                        options_.cooldown_ms, pool_key);
    ++create_backoffs_;
}

void SendJettyPool::clearCreateBackoffLocked(const PoolKey& pool_key) {
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) return;
    bucket->second.backoff_attempts = 0;
    bucket->second.backoff_until_ns = 0;
}

bool SendJettyPool::inCreateBackoffLocked(const PoolKey& pool_key) const {
    auto bucket = buckets_.find(pool_key);
    return bucket != buckets_.end() && bucket->second.backoff_attempts != 0 &&
           steadyNowNs() < bucket->second.backoff_until_ns;
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
        noteCreateFailureLocked(pool_key);
        LOG_EVERY_N(WARNING, 100)
            << "UB pool member create failed: device_key=" << device_key_
            << " pool_gen=" << pool_key.context_incarnation
            << " reason=budget_exhausted";
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
        noteCreateFailureLocked(pool_key);
        LOG_EVERY_N(WARNING, 100)
            << "UB pool member create failed: device_key=" << device_key_
            << " pool_gen=" << pool_key.context_incarnation
            << " reason=provider status=" << created.ToString();
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
                clearCreateBackoffLocked(pool_key);
                publish = true;
            }
        }
    }

    if (publish) {
        if (VLOG_IS_ON(1)) {
            VLOG(1) << "UB pool member created: device_key=" << device_key_
                    << " pool_gen=" << member->pool_key.context_incarnation
                    << " lane_gen=" << member->lane.lane_generation
                    << " jetty=" << member->lane.jetty_id
                    << " jfc=" << member->jfc_index;
        }
        return Status::OK();
    }

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
    noteTombstoneLocked(member);
    quarantined_.push_back(std::move(member));
    refreshBackpressureLocked();
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
    size_t idle_after = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (member->state != PoolMemberState::kInUse) {
            ++double_returns_;
            LOG(ERROR) << "UB pool double return: device_key=" << device_key_
                       << " pool_gen=" << member->pool_key.context_incarnation
                       << " lane_gen=" << member->lane.lane_generation
                       << " jetty=" << member->lane.jetty_id
                       << " state=" << static_cast<int>(member->state);
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
        idle_after = member->pool_key.context_incarnation == 0
                         ? 0
                         : buckets_[member->pool_key].idle.size();
    }

    if (options_.verbose_events || VLOG_IS_ON(1)) {
        if (options_.verbose_events) {
            LOG(INFO) << "UB pool return: device_key=" << device_key_
                      << " pool_gen=" << member->pool_key.context_incarnation
                      << " lane_gen=" << member->lane.lane_generation
                      << " jetty=" << member->lane.jetty_id
                      << " gate=" << (completion_gate_passed ? "pass" : "fail")
                      << " idle_after=" << idle_after;
        } else {
            VLOG(1) << "UB pool return: device_key=" << device_key_
                    << " pool_gen=" << member->pool_key.context_incarnation
                    << " lane_gen=" << member->lane.lane_generation
                    << " jetty=" << member->lane.jetty_id
                    << " gate=" << (completion_gate_passed ? "pass" : "fail")
                    << " idle_after=" << idle_after;
        }
    } else {
        LOG_EVERY_N(INFO,
                    options_.sample_every == 0 ? 1024 : options_.sample_every)
            << "UB pool return: device_key=" << device_key_
            << " pool_gen=" << member->pool_key.context_incarnation
            << " lane_gen=" << member->lane.lane_generation
            << " jetty=" << member->lane.jetty_id
            << " gate=" << (completion_gate_passed ? "pass" : "fail")
            << " idle_after=" << idle_after;
    }
    if (!to_destroy.empty()) destroyRetired(std::move(to_destroy));
    return Status::OK();
}

Status SendJettyPool::retireFaulted(SliceJettyLease lease,
                                        const std::string& reason) {
    return retireFaulted(std::move(lease), reason,
                         /*fence_hardware=*/false);
}

Status SendJettyPool::retireFaulted(SliceJettyLease lease,
                                        const std::string& reason,
                                        bool fence_hardware) {
    if (!lease.valid()) {
        return Status::InvalidArgument(
            "UB send pool retire requires a valid lease" LOC_MARK);
    }
    auto member = lease.member();
    lease.detach();

    std::shared_ptr<JettyDrainController> controller;
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
        member->retire_reason = reason.empty() ? "fault" : reason;
        // Leave kInUse and raise the epoch *before* the fence: no claim may
        // succeed on a member whose hardware is being retired. This is also the
        // PostGate close the asynchronous chain requires (§4.4/§5.3 ①).
        ++member->error_epoch;
        noteTombstoneLocked(member);
        const bool async = fence_hardware && options_.async_drain &&
                           drain_controller_ != nullptr &&
                           member->jetty != nullptr;
        if (async) {
            member->state = PoolMemberState::kDraining;
            controller = drain_controller_;
            // The budget stays charged for the whole drain; the object merely
            // stops counting as serving (design §4.4).
            (void)budget_->transition(member->budget,
                                      BudgetObjectState::kQuarantined);
            draining_[member->lane.lane_generation] = member;
            ++drain_begin_;
        } else {
            // Legacy/synchronous fence: the member rests in kFlushing while the
            // provider call runs outside the pool lock below.
            member->state = fence_hardware ? PoolMemberState::kFlushing
                                           : PoolMemberState::kDraining;
            if (fence_hardware) ++flushing_;
        }
    }

    LOG(WARNING) << "UB pool retire: device_key=" << device_key_
                 << " pool_gen=" << member->pool_key.context_incarnation
                 << " lane_gen=" << member->lane.lane_generation
                 << " jetty=" << member->lane.jetty_id
                 << " reason=" << member->retire_reason
                 << " state=" << static_cast<int>(member->state)
                 << " epoch=" << member->error_epoch;
    if (controller != nullptr) {
        // Non-blocking hand-off: the chain continues on the control executor.
        DrainRequest request;
        request.lane = member->lane;
        request.pool_key = member->pool_key;
        request.error_epoch = member->error_epoch;
        request.jfc_index = member->jfc_index;
        request.post_gate_closed = true;
        const Status begun = controller->beginDrain(request);
        if (begun.ok()) return Status::OK();
        // The drain could not be scheduled (controller stopping or executor
        // full). Keep responsibility: quarantine, never guess and never block.
        LOG_EVERY_N(WARNING, 100)
            << "UB pooled member drain could not begin; keeping it "
               "quarantined: "
            << begun.ToString();
        return quarantineDrain(member->lane,
                               "drain begin rejected: " + begun.ToString());
    }

    if (fence_hardware && member->jetty != nullptr) {
        // Legacy hardware retirement fence (design §7.2 rule 5 / §5.2): modify
        // ERROR, consume the flush-done marker, flush the software SQ. Lives
        // only behind async_drain=false, shutdown, or direct-pool users; it is
        // exactly the hot-path behavior the asynchronous chain replaces.
        std::vector<Completion> drained;
        const Status fence = adapter_->quiesceJetty(
            member->jetty, options_.retire_timeout_ms, drained);
        size_t with_token = 0;
        for (const auto& completion : drained) {
            if (completion.token != 0) ++with_token;
        }
        if (with_token != 0) {
            std::lock_guard<std::mutex> lock(mutex_);
            fence_drained_completions_ += with_token;
        }
        if (!fence.ok()) {
            // Keep responsibility: the member is quarantined with its budget
            // charged instead of being deleted on an unproven fence.
            LOG_EVERY_N(WARNING, 100)
                << "UB pooled member retirement fence failed; keeping it "
                   "quarantined: "
                << fence.ToString();
            std::lock_guard<std::mutex> lock(mutex_);
            if (fence_hardware && flushing_ > 0) --flushing_;
            member->state = PoolMemberState::kQuarantined;
            budget_->transition(member->budget,
                                BudgetObjectState::kQuarantined);
            quarantined_.push_back(std::move(member));
            refreshBackpressureLocked();
            return fence;
        }
    }

    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (fence_hardware && flushing_ > 0) --flushing_;
        enqueueRetirementLocked(member);
        to_destroy = std::move(retired_);
        retired_.clear();
    }

    if (!to_destroy.empty()) destroyRetired(std::move(to_destroy));
    return Status::OK();
}

void SendJettyPool::attachDrainController(
    std::shared_ptr<JettyDrainController> controller) {
    std::lock_guard<std::mutex> lock(mutex_);
    drain_controller_ = std::move(controller);
}

std::shared_ptr<JettyDrainController> SendJettyPool::drainController()
    const {
    std::lock_guard<std::mutex> lock(mutex_);
    return drain_controller_;
}

JettyPtr SendJettyPool::drainJettyFor(const PoolLaneKey& lane) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = draining_.find(lane.lane_generation);
    if (it == draining_.end()) return nullptr;
    if (it->second->lane != lane) return nullptr;
    return it->second->jetty;
}

Status SendJettyPool::markDrainDeleteReady(const PoolLaneKey& lane,
                                               uint64_t error_epoch) {
    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = draining_.find(lane.lane_generation);
        if (it == draining_.end()) {
            return Status::InvalidArgument(
                "UB drain delete-ready names a lane that is not draining");
        }
        auto member = it->second;
        if (member->lane != lane) {
            return Status::InvalidArgument(
                "UB drain delete-ready lane generation mismatch");
        }
        if (member->error_epoch != error_epoch) {
            // The epoch moved after the drain was registered: the evidence the
            // controller proved belongs to a different generation.
            return Status::InvalidArgument(
                "UB drain delete-ready error epoch mismatch");
        }
        if (member->state != PoolMemberState::kDraining) {
            return Status::InvalidArgument(
                "UB drain delete-ready member is not in the draining state");
        }
        draining_.erase(it);
        ++drain_delete_ready_;
        // Only the pool moves the member and enqueues it; the native delete
        // still runs on the retire executor (or synchronously on this thread
        // when no executor is running).
        enqueueRetirementLocked(member);
        to_destroy = std::move(retired_);
        retired_.clear();
    }
    if (!to_destroy.empty()) destroyRetired(std::move(to_destroy));
    return Status::OK();
}

Status SendJettyPool::quarantineDrain(const PoolLaneKey& lane,
                                          const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = draining_.find(lane.lane_generation);
    if (it == draining_.end()) {
        return Status::InvalidArgument(
            "UB drain quarantine names a lane that is not draining");
    }
    auto member = it->second;
    if (member->lane != lane) {
        return Status::InvalidArgument(
            "UB drain quarantine lane generation mismatch");
    }
    draining_.erase(it);
    ++drain_quarantined_;
    // Keep responsibility: the handle, the tombstone and the budget all stay.
    member->state = PoolMemberState::kQuarantined;
    member->retire_reason = reason.empty() ? "drain quarantined" : reason;
    (void)budget_->transition(member->budget, BudgetObjectState::kQuarantined);
    quarantined_.push_back(std::move(member));
    refreshBackpressureLocked();
    return Status::OK();
}

void SendJettyPool::enqueueRetirementLocked(
    std::shared_ptr<PoolMember> member) {
    // First real error retires permanently (INV07): the member never
    // re-enters the idle stack even if the path recovers.
    if (retire_worker_ && retire_worker_->running()) {
        // The delete executor owns the native reset+delete from here; the
        // caller returns immediately instead of blocking on the provider.
        member->state = PoolMemberState::kDeleteReady;
        retire_worker_->enqueue(std::move(member));
        // A retirement opens a refill deficit; wake the refill pass instead of
        // waiting for its interval.
        if (refill_worker_) refill_worker_->requestWake();
        return;
    }
    retired_.push_back(std::move(member));
}

void SendJettyPool::retireMemberLocked(std::shared_ptr<PoolMember> member,
                                           const std::string& reason) {
    member->state = PoolMemberState::kDraining;
    member->retire_reason = reason;
    // The only retirement-epoch writer besides onLeaseLeaked(). Bumping here
    // is what makes a lease taken before this point unable to claim again. The
    // pool mutex provides the atomicity; the value is never written elsewhere.
    ++member->error_epoch;
    noteTombstoneLocked(member);
    enqueueRetirementLocked(std::move(member));
}

void SendJettyPool::destroyRetired(
    std::vector<std::shared_ptr<PoolMember>> members) {
    // All native calls happen outside the pool lock.
    for (auto& member : members) {
        member->state = PoolMemberState::kDeleting;
        const DeleteOutcome outcome = resetAndDeleteMember(*member);
        finishDelete(member, outcome);
    }
}

DeleteOutcome SendJettyPool::resetAndDeleteMember(PoolMember& member) {
    DeleteOutcome outcome;
    JettyPtr jetty = std::move(member.jetty);
    if (!jetty) {
        // Nothing native is left to reclaim: the handle was already released,
        // so destruction is proven by construction.
        outcome.kind = DeleteOutcome::Kind::kConsumed;
        return outcome;
    }
    // The native delete requires a RESET Jetty in the verified UMDK contract.
    // A reset failure (for example an ERROR Jetty without a trusted barrier)
    // never authorizes the delete: keep the handle for a retry.
    if (options_.reset_before_delete && jetty->valid()) {
        const Status reset = adapter_->resetJetty(jetty);
        if (!reset.ok()) {
            outcome.status = reset;
            outcome.kind = reset.IsTooManyRequests()
                               ? DeleteOutcome::Kind::kRetained
                               : DeleteOutcome::Kind::kUnknown;
            member.jetty = std::move(jetty);
            return outcome;
        }
    }
    outcome = adapter_->deleteJettyWithOutcome(jetty);
    if (outcome.kind != DeleteOutcome::Kind::kConsumed) {
        member.jetty = std::move(jetty);
    }
    return outcome;
}

void SendJettyPool::finishDelete(const std::shared_ptr<PoolMember>& member,
                                     DeleteOutcome outcome) {
    switch (outcome.kind) {
        case DeleteOutcome::Kind::kConsumed: {
            member->state = PoolMemberState::kDestroyed;
            ++delete_consumed_;
            // No release without evidence: the tombstone entry is erased only
            // now that the provider proved destruction.
            if (!eraseTombstoneForProvenDestruction(*member)) {
                LOG_EVERY_N(WARNING, 100)
                    << "UB pooled member was deleted but its tombstone could "
                       "not be cleared; keeping its budget charged";
                return;
            }
            {
                std::lock_guard<std::mutex> lock(mutex_);
                delete_retry_at_ns_.erase(member->lane.lane_generation);
                retained_.erase(member->lane.lane_generation);
            }
            // The reservation may already be in kQuarantined (a drain always
            // moves it there); release accepts both kRetiring and kQuarantined,
            // so the transition is best effort and never gates the release.
            (void)budget_->transition(member->budget,
                                      BudgetObjectState::kRetiring);
            budget_->releaseAfterProvenDestruction(member->budget, true);
            LOG_EVERY_N(INFO, 100)
                << "UB pool member deleted: device_key=" << device_key_
                << " pool_gen=" << member->pool_key.context_incarnation
                << " lane_gen=" << member->lane.lane_generation
                << " jetty=" << member->lane.jetty_id << " outcome=consumed";
            return;
        }
        case DeleteOutcome::Kind::kRetained: {
            // The provider explicitly says the handle is still valid. Retry
            // after an exponential backoff; the owner, tombstone and budget
            // stay exactly where they are.
            ++delete_retained_;
            const uint32_t capped =
                std::min<uint32_t>(++member->delete_retry_attempts, 10);
            uint64_t delay_ms =
                std::max<uint32_t>(1, options_.refill_interval_ms);
            delay_ms =
                std::min<uint64_t>(delay_ms << capped,
                                   std::max<uint32_t>(1, options_.cooldown_ms));
            member->state = PoolMemberState::kDeleteReady;
            std::lock_guard<std::mutex> lock(mutex_);
            delete_retry_at_ns_[member->lane.lane_generation] =
                steadyNowNs() + delay_ms * 1'000'000ULL;
            retained_[member->lane.lane_generation] = member;
            LOG_EVERY_N(WARNING, 100)
                << "UB pool member delete retained: device_key=" << device_key_
                << " pool_gen=" << member->pool_key.context_incarnation
                << " lane_gen=" << member->lane.lane_generation
                << " jetty=" << member->lane.jetty_id
                << " retry_attempts=" << member->delete_retry_attempts;
            return;
        }
        case DeleteOutcome::Kind::kUnknown: {
            // Undecidable destruction: keep the handle owned by the member so
            // a later drain can retry, and keep the budget charged (INV09).
            ++delete_unknown_;
            member->state = PoolMemberState::kQuarantined;
            (void)budget_->transition(member->budget,
                                      BudgetObjectState::kQuarantined);
            std::lock_guard<std::mutex> lock(mutex_);
            delete_retry_at_ns_.erase(member->lane.lane_generation);
            retained_.erase(member->lane.lane_generation);
            quarantined_.push_back(member);
            refreshBackpressureLocked();
            return;
        }
    }
}

size_t SendJettyPool::retryRetainedDeletes() {
    std::vector<std::shared_ptr<PoolMember>> due;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const uint64_t now = steadyNowNs();
        for (auto it = delete_retry_at_ns_.begin();
             it != delete_retry_at_ns_.end();) {
            if (it->second > now) {
                ++it;
                continue;
            }
            auto retained = retained_.find(it->first);
            if (retained != retained_.end()) {
                due.push_back(retained->second);
                retained_.erase(retained);
            }
            it = delete_retry_at_ns_.erase(it);
        }
    }
    if (due.empty()) return 0;
    const size_t count = due.size();
    destroyRetired(std::move(due));
    return count;
}

void SendJettyPool::onLeaseLeaked(
    const std::shared_ptr<PoolMember>& member) {
    // A lease dropped without returnOnce(): responsibility is unknown, so
    // the member is quarantined with its budget retained; it never returns
    // to circulation.
    uint64_t pool_generation = 0;
    uint64_t lane_generation = 0;
    uint32_t jetty_id = 0;
    {
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
        // retirement, so the epoch must move; a stale claim cannot succeed.
        ++member->error_epoch;
        noteTombstoneLocked(member);
        budget_->transition(member->budget, BudgetObjectState::kQuarantined);
        quarantined_.push_back(member);
        refreshBackpressureLocked();
        pool_generation = member->pool_key.context_incarnation;
        lane_generation = member->lane.lane_generation;
        jetty_id = member->lane.jetty_id;
    }
    LOG(WARNING) << "UB pool lease leaked: device_key=" << device_key_
                 << " pool_gen=" << pool_generation
                 << " lane_gen=" << lane_generation << " jetty=" << jetty_id;
}

ClaimVerdict SendJettyPool::tryClaimPost(
    const PoolKey& pool_key, const AttemptKey& attempt,
    const ClaimRequest& request) const {
    // Pure read (IN-1): no state change, no counter, no provider call. The
    // fixed check order is stopping -> bucket -> member state -> generation ->
    // error epoch; diagnostics and tests depend on it.
    //
    // A null member, or a member that never belonged to this pool key, is
    // reported as retired: the lease is the only proof of identity the caller
    // has, and without it the pool cannot vouch for anything.
    //
    // Reachability: only kPoolStopping occurs under today's single-lease
    // model. The other verdicts are defensive and pinned by unit tests.
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
    // No creation or native destruction may happen on executor threads after
    // the stop; joining bounds the window in which an executor owns a
    // provider call. Retirements after this point destroy synchronously.
    stopWorkers();
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
    // Shutdown fallback (design §11.3): every in-flight asynchronous drain
    // degrades to the synchronous quiesce fence, and only a proven fence is
    // allowed to release the member. Leftover entries are quarantined below.
    if (auto controller = drainController()) controller->drainSynchronously();
    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    bool contract_violation = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // A drain that never reached DELETE_READY by the shutdown barrier keeps
        // its responsibility: the member goes to quarantine (handle, tombstone
        // and budget retained) instead of being deleted on unproven evidence.
        for (auto& entry : draining_) {
            auto member = entry.second;
            member->state = PoolMemberState::kQuarantined;
            member->retire_reason = "draining at shutdown";
            (void)budget_->transition(member->budget,
                                      BudgetObjectState::kQuarantined);
            quarantined_.push_back(std::move(member));
        }
        draining_.clear();
        for (auto& entry : retained_) {
            auto member = entry.second;
            member->retire_reason = "retained delete at shutdown";
            to_destroy.push_back(std::move(member));
        }
        retained_.clear();
        delete_retry_at_ns_.clear();
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
            contract_violation = true;
            LOG(ERROR) << "UB send pool drainIdle found " << in_use
                       << " in-use and " << creating
                       << " creating members after the shutdown barrier; "
                          "stop every posting/polling consumer before the "
                          "context shutdown"
                       << LOC_MARK;
        }
    }
    destroyRetired(std::move(to_destroy));
    if (contract_violation) {
        return Status::InternalError(
            "UB send pool drainIdle found outstanding leases or creations "
            "after the shutdown barrier (RC-03 violated)" LOC_MARK);
    }
    return Status::OK();
}

Status SendJettyPool::drainQuarantined() {
    std::vector<std::shared_ptr<PoolMember>> candidates;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        candidates = std::move(quarantined_);
        quarantined_.clear();
        // A shutdown drain is the last chance for a retained delete; it goes
        // through the same classified path, so a provider that still says
        // "valid" simply keeps the member.
        for (auto& entry : retained_) {
            candidates.push_back(std::move(entry.second));
        }
        retained_.clear();
        delete_retry_at_ns_.clear();
    }
    for (auto& member : candidates) {
        member->state = PoolMemberState::kDeleting;
        const DeleteOutcome outcome = resetAndDeleteMember(*member);
        // kRetained during shutdown parks the member in retained_ again, which
        // then still holds the handle and budget: no release without proof.
        finishDelete(member, outcome);
    }
    return Status::OK();
}

void SendJettyPool::refillPass() {
    // A retained native delete is retried on this periodic pass so a busy
    // provider never pins a member forever without a bounded backoff.
    (void)retryRetainedDeletes();
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) return;
    // A full tombstone table pauses refill: creating more members cannot free
    // an unproven owner and would only add pressure (design §7.2).
    if (options_.tombstones != nullptr && options_.tombstones->full()) {
        refreshBackpressureLocked();
        return;
    }
    // The same holds when the provider proved unable to drain (§9): new
    // members could never be retired either.
    if (admissionBlockedLocked()) {
        refreshBackpressureLocked();
        return;
    }
    // Claim creation slots under the lock, then call the provider outside it.
    // Every known PoolKey is topped back up to the static Serving size even
    // while no business attempt is in flight.
    std::vector<PoolKey> batch;
    struct RefillSnapshot {
        PoolKey key;
        size_t desired;
        size_t idle;
        size_t in_use;
        size_t creating;
        size_t deficit;
    };
    std::vector<RefillSnapshot> refill_logs;
    for (auto& entry : buckets_) {
        PoolBucket& bucket = entry.second;
        // BACKOFF: a failed creation must not be mistaken for an immediate
        // deficit, or refill would storm the provider.
        if (inCreateBackoffLocked(entry.first)) continue;
        const size_t serving = bucket.idle.size() + bucket.in_use;
        if (serving + bucket.creating >= options_.desired_serving) {
            continue;
        }
        if (bucket.creating >= options_.max_creating) continue;
        size_t slots = options_.desired_serving - serving - bucket.creating;
        slots = std::min(slots, options_.max_creating - bucket.creating);
        refill_logs.push_back(RefillSnapshot{
            entry.first, options_.desired_serving, bucket.idle.size(),
            bucket.in_use, bucket.creating, slots});
        bucket.creating += slots;
        for (size_t slot = 0; slot < slots; ++slot) {
            batch.push_back(entry.first);
        }
    }
    lock.unlock();
    if (options_.verbose_events || VLOG_IS_ON(1)) {
        for (const auto& log : refill_logs) {
            if (options_.verbose_events) {
                LOG(INFO) << "UB pool refill: device_key=" << device_key_
                          << " pool_gen=" << log.key.context_incarnation
                          << " desired=" << log.desired << " idle=" << log.idle
                          << " in_use=" << log.in_use
                          << " creating=" << log.creating
                          << " deficit=" << log.deficit;
            } else {
                VLOG(1) << "UB pool refill: device_key=" << device_key_
                        << " pool_gen=" << log.key.context_incarnation
                        << " desired=" << log.desired << " idle=" << log.idle
                        << " in_use=" << log.in_use
                        << " creating=" << log.creating
                        << " deficit=" << log.deficit;
            }
        }
    }
    for (const auto& pool_key : batch) {
        (void)createAndPublish(pool_key);
    }
}

void SendJettyPool::startWorkers() {
    if (options_.background_delete) {
        auto worker = std::make_unique<JettyRetireWorker>(
            [this](std::vector<std::shared_ptr<PoolMember>> members) {
                destroyRetired(std::move(members));
            });
        if (worker->start()) {
            retire_worker_ = std::move(worker);
        }
    }
    if (options_.background_refill) {
        auto worker = std::make_unique<JettyRefillWorker>(
            [this] { refillPass(); }, options_.refill_interval_ms);
        if (worker->start()) {
            refill_worker_ = std::move(worker);
        }
    }
}

void SendJettyPool::stopWorkers() {
    if (retire_worker_) {
        retire_worker_->stop();
        retire_worker_.reset();
    }
    if (refill_worker_) {
        refill_worker_->stop();
        refill_worker_.reset();
    }
}

std::optional<BudgetDeviceSnapshot> SendJettyPool::budgetSnapshot() const {
    if (budget_ == nullptr) return std::nullopt;
    return budget_->snapshot(device_key_);
}

SendJettyPoolStats SendJettyPool::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    SendJettyPoolStats result{};
    for (const auto& entry : buckets_) {
        result.idle_members += entry.second.idle.size();
        result.in_use_members += entry.second.in_use;
        result.creating_members += entry.second.creating;
    }
    result.delete_ready_members =
        retire_worker_ ? retire_worker_->pending() : 0;
    result.retired_members =
        retired_.size() + result.delete_ready_members + retained_.size();
    result.flushing_members = flushing_;
    result.draining_members = draining_.size();
    result.quarantined_members = quarantined_.size();
    result.drain_begin = drain_begin_;
    result.drain_delete_ready = drain_delete_ready_;
    result.drain_quarantined = drain_quarantined_;
    result.delete_consumed = delete_consumed_;
    result.delete_retained = delete_retained_;
    result.delete_unknown = delete_unknown_;
    for (const auto& entry : buckets_) {
        if (entry.second.backoff_attempts != 0 &&
            steadyNowNs() < entry.second.backoff_until_ns) {
            ++result.backoff_keys;
        }
    }
    result.total_leases = total_leases_;
    result.total_returns = total_returns_;
    result.pool_misses = pool_misses_;
    result.stop_rejections = stop_rejections_;
    result.leaked_leases = leaked_leases_;
    result.created_members = created_members_;
    result.create_failures = create_failures_;
    result.double_returns = double_returns_;
    result.create_backoffs = create_backoffs_;
    result.fence_drained_completions = fence_drained_completions_;
    result.tombstone_insert_failures = tombstone_insert_failures_;
    result.tombstone_erase_misses = tombstone_erase_misses_;
    result.backpressure_rejections = backpressure_rejections_;
    result.tombstone_backpressure = tombstone_backpressure_;
    result.quarantine_backpressure = quarantine_backpressure_;
    result.drain_admission_stopped = drain_controller_ != nullptr &&
                                     !drain_controller_->asyncDrainSupported();
    if (options_.tombstones != nullptr) {
        result.tombstone_entries = options_.tombstones->size();
        result.tombstone_capacity = options_.tombstones->capacity();
    }
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
    // Members on the delete executor are owned by it and their state/jetty
    // fields mutate without the pool lock; report the queue state instead.
    if (retire_worker_) {
        states.insert(states.end(), retire_worker_->pending(),
                      PoolMemberState::kDeleteReady);
    }
    // Members owned by the asynchronous drain chain are not in a container that
    // memberStates() otherwise walks.
    for (const auto& entry : draining_) states.push_back(entry.second->state);
    for (const auto& entry : retained_) states.push_back(entry.second->state);
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
