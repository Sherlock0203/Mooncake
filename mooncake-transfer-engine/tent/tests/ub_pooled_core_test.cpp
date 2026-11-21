// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Software-only acceptance tests for the pooled rm_pool core components:
//   - NativeBudget (C05): device-level full-role budget ledger
//   - JfcCreditLedger / TaskPermitLedger (C05/C06): admission tickets
//   - SendJettyPool (C06): per-attempt exclusive LIFO pool with JFC credit
//     matching, stop interleaving, fault retirement and leak defence
//   - AttemptGate (C07/D04): completion gate with single settlement
//   - UbParams: pooled configuration keys (C01)
//
// Every test runs against a fake URMA adapter; no provider, NIC or kernel
// resource is required.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/attempt_gate.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/node_target_quota.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/path_registry.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/target_registry.h"
#include "tent/transport/ub/urma_adapter.h"

using mooncake::tent::Config;
using mooncake::tent::Status;
using mooncake::tent::Topology;
using namespace mooncake::tent::ub;

namespace {

// Fake native handles: reference-counted objects behind the adapter boundary.
class PooledFakeContext final : public Context {
   public:
    bool valid() const noexcept override { return true; }
    const DeviceInfo& deviceInfo() const noexcept override { return info_; }
    int asyncFd() const noexcept override { return -1; }

   private:
    DeviceInfo info_{};
};

class PooledFakeJfc final : public Jfc {
   public:
    bool valid() const noexcept override { return true; }
    int eventFd() const noexcept override { return -1; }
};

class PooledFakeJetty final : public Jetty {
   public:
    explicit PooledFakeJetty(uint32_t id) : id_(id) {}
    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return 0; }
    void close() { valid_ = false; }

   private:
    uint32_t id_;
    bool valid_{true};
};

// Minimal fake provider for the pooled core. createJetty/deleteJetty carry
// injectable failures; every other op is a no-op success.
class PooledFakeAdapter final : public UrmaAdapter {
   public:
    bool available() const noexcept override { return true; }
    uint32_t nativeApiVersion() const noexcept override { return 1; }
    size_t nativeSegmentDescriptorSize() const noexcept override {
        return 16;
    }
    Status initialize() override { return Status::OK(); }
    Status shutdown() override { return Status::OK(); }
    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        devices.clear();
        return Status::OK();
    }
    Status openContext(const DeviceInfo&, ContextPtr& context) override {
        context = std::make_shared<PooledFakeContext>();
        return Status::OK();
    }
    Status closeContext(ContextPtr& context) override {
        context.reset();
        return Status::OK();
    }
    Status createJfc(const ContextPtr&, const JfcOptions&,
                     JfcPtr& jfc) override {
        jfc = std::make_shared<PooledFakeJfc>();
        return Status::OK();
    }
    Status deleteJfc(JfcPtr& jfc) override {
        jfc.reset();
        return Status::OK();
    }
    Status registerLocalSegment(const ContextPtr&, uint64_t, size_t,
                                const SegmentOptions&,
                                LocalSegmentPtr& segment) override {
        return Status::NotImplemented("not needed by pooled core tests");
    }
    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        segment.reset();
        return Status::OK();
    }
    Status importRemoteSegment(const ContextPtr&, const SegmentDescriptor&,
                               const SegmentOptions&,
                               RemoteSegmentPtr& segment) override {
        return Status::NotImplemented("not needed by pooled core tests");
    }
    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        segment.reset();
        return Status::OK();
    }
    Status createJetty(const ContextPtr&, const JfcPtr& jfc,
                       const JettyOptions&, JettyPtr& jetty) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!jfc) return Status::InvalidArgument("bad JFC");
        if (create_failures_remaining_ > 0) {
            --create_failures_remaining_;
            jetty.reset();
            return Status::InternalError("injected createJetty failure");
        }
        jetty = std::make_shared<PooledFakeJetty>(next_jetty_id_++);
        ++created_;
        return Status::OK();
    }
    Status deleteJetty(JettyPtr& jetty) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!jetty) return Status::InvalidArgument("jetty is null");
        if (delete_failures_remaining_ > 0) {
            --delete_failures_remaining_;
            return Status::InternalError("injected deleteJetty failure");
        }
        if (auto fake = std::dynamic_pointer_cast<PooledFakeJetty>(jetty)) {
            fake->close();
        }
        jetty.reset();
        ++deleted_;
        return Status::OK();
    }
    Status bindJetty(const JettyPtr&, const RemoteJettyInfo&) override {
        return Status::OK();
    }
    Status unbindJetty(const JettyPtr&) override { return Status::OK(); }
    Status resetJetty(const JettyPtr&) override { return Status::OK(); }
    Status quiesceJetty(const JettyPtr&, uint32_t,
                        std::vector<Completion>& completions) override {
        completions.clear();
        return Status::OK();
    }
    Status post(const JettyPtr&, const std::vector<WorkRequest>& requests,
                size_t& posted_count) override {
        posted_count = requests.size();
        return Status::OK();
    }
    Status poll(const JfcPtr&, size_t,
                std::vector<Completion>& completions) override {
        completions.clear();
        return Status::OK();
    }

    void failNextCreates(int count) {
        std::lock_guard<std::mutex> lock(mutex_);
        create_failures_remaining_ = count;
    }
    void failNextDeletes(int count) {
        std::lock_guard<std::mutex> lock(mutex_);
        delete_failures_remaining_ = count;
    }
    int created() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return created_;
    }
    int deleted() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return deleted_;
    }

   private:
    mutable std::mutex mutex_;
    uint32_t next_jetty_id_ = 1;
    int create_failures_remaining_ = 0;
    int delete_failures_remaining_ = 0;
    int created_ = 0;
    int deleted_ = 0;
};

PoolKey makePoolKey(uint64_t incarnation = 1) {
    PoolKey key;
    key.context_incarnation = incarnation;
    key.trans_mode = 1;
    key.priority = 2;
    key.bind_tp_type_mask = 0x3;
    return key;
}

AttemptKey makeAttemptKey(uint64_t task, uint64_t slice, uint64_t gen) {
    return AttemptKey{task, slice, gen};
}

// Fixture wiring one fake adapter, one budget ledger, one credit ledger and
// one pool with `desired` static Serving per pool key.
struct PoolHarness {
    std::shared_ptr<PooledFakeAdapter> adapter =
        std::make_shared<PooledFakeAdapter>();
    std::shared_ptr<NativeBudget> budget = std::make_shared<NativeBudget>();
    std::shared_ptr<JfcCreditLedger> credits =
        std::make_shared<JfcCreditLedger>();
    std::vector<std::shared_ptr<UbJfc>> jfcs;
    std::shared_ptr<SendJettyPool> pool;
    ContextPtr context = std::make_shared<PooledFakeContext>();

    Status init(size_t desired, size_t max_creating, uint64_t budget_jetties,
                uint64_t reserve, size_t jfc_count, uint64_t jfc_capacity) {
        SendJettyPoolOptions options;
        options.desired_serving = desired;
        options.max_creating = max_creating;
        return initWithOptions(budget_jetties, reserve, jfc_count,
                               jfc_capacity, options);
    }

    // Same wiring with explicit pool options (background executors, refill
    // interval, ...).
    Status initWithOptions(uint64_t budget_jetties, uint64_t reserve,
                           size_t jfc_count, uint64_t jfc_capacity,
                           SendJettyPoolOptions options) {
        CHECK_STATUS(budget->configure("dev0", 0, budget_jetties, reserve));
        for (size_t index = 0; index < jfc_count; ++index) {
            CHECK_STATUS(credits->install(index, jfc_capacity));
            JfcPtr handle;
            CHECK_STATUS(adapter->createJfc(context, JfcOptions{}, handle));
            jfcs.push_back(
                std::make_shared<UbJfc>(index, adapter, std::move(handle)));
        }
        pool = std::make_shared<SendJettyPool>(adapter, context, jfcs, budget,
                                               "dev0", credits, options);
        return Status::OK();
    }
};

}  // namespace

// ---------------------------------------------------------------------------
// NativeBudget (T-08 / C05)
// ---------------------------------------------------------------------------

TEST(UbNativeBudgetTest, ConfigureValidation) {
    NativeBudget budget;
    ASSERT_FALSE(
        budget.configure("", 0, 4, 0).ok());  // empty device key
    ASSERT_FALSE(
        budget.configure("dev0", 0, 0, 0).ok());  // zero budget
    // configured + reserve must not exceed the verified device limit
    ASSERT_FALSE(budget.configure("dev0", 4, 4, 1).ok());
    ASSERT_TRUE(budget.configure("dev0", 4, 4, 0).ok());
    // reconfiguring a device is rejected
    ASSERT_FALSE(budget.configure("dev0", 8, 4, 0).ok());
}

TEST(UbNativeBudgetTest, ReserveTransitionRelease) {
    NativeBudget budget;
    ASSERT_TRUE(budget.configure("dev0", 0, 2, 0).ok());

    BudgetQuantities one;
    one.jetties = 1;
    auto reservation = budget.tryReserve("dev0", BudgetRole::kBusiness, one);
    ASSERT_TRUE(reservation.has_value());
    EXPECT_EQ(reservation->state, BudgetObjectState::kCreating);

    auto snapshot = budget.snapshot("dev0");
    EXPECT_EQ(snapshot.creating_jetties, 1u);
    EXPECT_EQ(snapshot.total_jetties, 1u);

    ASSERT_TRUE(
        budget.transition(*reservation, BudgetObjectState::kActive).ok());
    snapshot = budget.snapshot("dev0");
    EXPECT_EQ(snapshot.active_jetties, 1u);
    EXPECT_EQ(snapshot.creating_jetties, 0u);

    // ACTIVE -> RETIRING -> proven release frees the budget.
    ASSERT_TRUE(
        budget.transition(*reservation, BudgetObjectState::kRetiring).ok());
    ASSERT_TRUE(budget.releaseAfterProvenDestruction(*reservation, true).ok());
    snapshot = budget.snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 0u);
}

TEST(UbNativeBudgetTest, CeilingEnforcedAcrossRoles) {
    NativeBudget budget;
    ASSERT_TRUE(budget.configure("dev0", 0, 2, 1).ok());
    BudgetQuantities one;
    one.jetties = 1;

    // configured(2) + reserve(1) = 3 objects maximum across ALL roles.
    auto business = budget.tryReserve("dev0", BudgetRole::kBusiness, one);
    auto probe = budget.tryReserve("dev0", BudgetRole::kProbe, one);
    auto legacy = budget.tryReserve("dev0", BudgetRole::kLegacy, one);
    ASSERT_TRUE(business.has_value());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(legacy.has_value());
    EXPECT_FALSE(budget.tryReserve("dev0", BudgetRole::kTarget, one)
                     .has_value());

    const auto snapshot = budget.snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 3u);
    EXPECT_EQ(snapshot.rejected_reservations, 1u);
}

TEST(UbNativeBudgetTest, ReplacementReserveAllowsBoundedRebuild) {
    NativeBudget budget;
    ASSERT_TRUE(budget.configure("dev0", 0, 1, 1).ok());
    BudgetQuantities one;
    one.jetties = 1;

    auto old_object = budget.tryReserve("dev0", BudgetRole::kBusiness, one);
    ASSERT_TRUE(old_object.has_value());
    ASSERT_TRUE(
        budget.transition(*old_object, BudgetObjectState::kActive).ok());
    // The old object faults and retires while still holding its budget...
    ASSERT_TRUE(
        budget.transition(*old_object, BudgetObjectState::kRetiring).ok());
    // ...the reserve allows exactly one replacement creation.
    auto replacement = budget.tryReserve("dev0", BudgetRole::kBusiness, one);
    ASSERT_TRUE(replacement.has_value());
    // A second replacement is rejected: no unlimited replacement.
    EXPECT_FALSE(budget.tryReserve("dev0", BudgetRole::kBusiness, one)
                     .has_value());

    ASSERT_TRUE(
        budget.releaseAfterProvenDestruction(*old_object, true).ok());
    auto after_release = budget.tryReserve("dev0", BudgetRole::kBusiness, one);
    EXPECT_TRUE(after_release.has_value());
}

TEST(UbNativeBudgetTest, ReleaseRequiresProofAndTerminalState) {
    NativeBudget budget;
    ASSERT_TRUE(budget.configure("dev0", 0, 2, 0).ok());
    BudgetQuantities one;
    one.jetties = 1;
    auto reservation = budget.tryReserve("dev0", BudgetRole::kBusiness, one);
    ASSERT_TRUE(reservation.has_value());

    // Unproven destruction never releases budget.
    EXPECT_FALSE(budget.releaseAfterProvenDestruction(*reservation, true)
                     .ok());  // creating: not terminal
    EXPECT_FALSE(budget.releaseAfterProvenDestruction(*reservation, false)
                     .ok());  // no proof
    ASSERT_TRUE(
        budget.transition(*reservation, BudgetObjectState::kQuarantined)
            .ok());
    EXPECT_FALSE(budget.releaseAfterProvenDestruction(*reservation, false)
                     .ok());
    EXPECT_TRUE(budget.releaseAfterProvenDestruction(*reservation, true).ok());
    // Double release is rejected.
    EXPECT_FALSE(budget.releaseAfterProvenDestruction(*reservation, true)
                     .ok());
}

TEST(UbNativeBudgetTest, InvalidTransitionsRejected) {
    NativeBudget budget;
    ASSERT_TRUE(budget.configure("dev0", 0, 2, 0).ok());
    BudgetQuantities one;
    one.jetties = 1;
    auto reservation = budget.tryReserve("dev0", BudgetRole::kBusiness, one);
    ASSERT_TRUE(reservation.has_value());

    EXPECT_FALSE(
        budget.transition(*reservation, BudgetObjectState::kRetiring).ok());
    ASSERT_TRUE(
        budget.transition(*reservation, BudgetObjectState::kActive).ok());
    // Backwards transitions are rejected.
    EXPECT_FALSE(
        budget.transition(*reservation, BudgetObjectState::kCreating).ok());
    ASSERT_TRUE(
        budget.transition(*reservation, BudgetObjectState::kQuarantined)
            .ok());
    // Quarantine is terminal except for proven release.
    EXPECT_FALSE(
        budget.transition(*reservation, BudgetObjectState::kActive).ok());
}

TEST(UbNativeBudgetTest, OverflowRejected) {
    NativeBudget budget;
    ASSERT_TRUE(budget.configure("dev0", 0, 4, 0).ok());
    BudgetQuantities huge;
    huge.jetties = UINT64_MAX;
    EXPECT_FALSE(budget.tryReserve("dev0", BudgetRole::kBusiness, huge)
                     .has_value());
    BudgetQuantities zero;
    EXPECT_FALSE(budget.tryReserve("dev0", BudgetRole::kBusiness, zero)
                     .has_value());
    EXPECT_FALSE(budget.tryReserve("unknown-device", BudgetRole::kBusiness,
                                   BudgetQuantities{1, 0, 0})
                     .has_value());
}

TEST(UbNativeBudgetTest, LeakedReservationCounted) {
    NativeBudget budget;
    ASSERT_TRUE(budget.configure("dev0", 0, 2, 0).ok());
    BudgetQuantities one;
    one.jetties = 1;
    auto reservation = budget.tryReserve("dev0", BudgetRole::kBusiness, one);
    ASSERT_TRUE(reservation.has_value());
    budget.recordLeakedReservation(*reservation);
    const auto snapshot = budget.snapshot("dev0");
    EXPECT_EQ(snapshot.leaked_reservations, 1u);
    // The leak keeps the budget charged.
    EXPECT_EQ(snapshot.total_jetties, 1u);
}

// ---------------------------------------------------------------------------
// JfcCreditLedger / TaskPermitLedger (T-10 admission tickets)
// ---------------------------------------------------------------------------

TEST(UbAdmissionTest, JfcCreditRoundTrip) {
    JfcCreditLedger credits;
    ASSERT_TRUE(credits.install(0, 3).ok());
    ASSERT_FALSE(credits.install(0, 3).ok());  // reinstall rejected
    ASSERT_FALSE(credits.install(1, 0).ok());  // zero capacity rejected

    auto first = credits.tryReserve(0, 1);
    auto second = credits.tryReserve(0, 2);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(credits.outstanding(0), 3u);
    // Capacity exhausted: non-blocking defer.
    EXPECT_FALSE(credits.tryReserve(0, 1).has_value());
    EXPECT_EQ(credits.rejectedReservations(0), 1u);

    credits.release(0, *first);
    EXPECT_EQ(credits.outstanding(0), 2u);
    // Double release is a harmless no-op (never underflows).
    credits.release(0, *first);
    EXPECT_EQ(credits.outstanding(0), 2u);
    credits.release(0, *second);
    EXPECT_EQ(credits.outstanding(0), 0u);
}

TEST(UbAdmissionTest, JfcCreditRejectsOversizedReservation) {
    JfcCreditLedger credits;
    ASSERT_TRUE(credits.install(0, 2).ok());
    EXPECT_FALSE(credits.tryReserve(0, 3).has_value());
    EXPECT_FALSE(credits.tryReserve(9, 1).has_value());  // unknown JFC
}

TEST(UbAdmissionTest, TaskPermitEnforcesK) {
    TaskPermitLedger permits(2);
    auto first = permits.tryAcquire();
    auto second = permits.tryAcquire();
    ASSERT_TRUE(first.valid());
    ASSERT_TRUE(second.valid());
    // K exhausted: the attempt defers, never faults.
    EXPECT_FALSE(permits.tryAcquire().valid());
    EXPECT_EQ(permits.rejectedAcquisitions(), 1u);
    EXPECT_EQ(permits.outstanding(), 2u);

    first.release();
    EXPECT_EQ(permits.outstanding(), 1u);
    auto third = permits.tryAcquire();
    EXPECT_TRUE(third.valid());

    // Move ownership: exactly one holder releases.
    TaskPermitTicket moved = std::move(third);
    EXPECT_FALSE(third.valid());
    EXPECT_TRUE(moved.valid());
    moved.release();
    EXPECT_EQ(permits.outstanding(), 1u);
    second.release();
    EXPECT_EQ(permits.outstanding(), 0u);
}

// ---------------------------------------------------------------------------
// SendJettyPool (T-09 / C06)
// ---------------------------------------------------------------------------

TEST(UbSendJettyPoolTest, LeaseCreatesUpToDesiredServing) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(/*desired=*/3, /*max_creating=*/2,
                             /*budget=*/8, /*reserve=*/0, /*jfcs=*/2,
                             /*jfc_capacity=*/16)
                    .ok());
    const auto key = makePoolKey();

    std::vector<LeaseWithCredit> leases;
    for (uint64_t generation = 1; generation <= 3; ++generation) {
        LeaseWithCredit grant;
        ASSERT_TRUE(
            harness.pool
                ->tryLease(key, makeAttemptKey(1, generation, generation),
                           grant)
                .ok());
        ASSERT_TRUE(grant.lease.valid());
        leases.push_back(std::move(grant));
    }

    // Every lease owns a unique lane (exclusive per attempt, INV03/INV04).
    std::set<std::pair<uint32_t, uint64_t>> lanes;
    for (const auto& grant : leases) {
        lanes.emplace(grant.lease.lane().jetty_id,
                      grant.lease.lane().lane_generation);
    }
    EXPECT_EQ(lanes.size(), 3u);
    EXPECT_EQ(harness.adapter->created(), 3);
    EXPECT_EQ(harness.pool->servingCount(key), 3u);

    // All-IN_USE does not expand the pool: the next lease defers.
    LeaseWithCredit deferred;
    const Status status = harness.pool->tryLease(
        key, makeAttemptKey(1, 99, 99), deferred);
    EXPECT_TRUE(status.IsTooManyRequests());
    EXPECT_EQ(harness.adapter->created(), 3);
    EXPECT_EQ(harness.pool->stats().pool_misses, 1u);
}

TEST(UbSendJettyPoolTest, ReturnThenLeaseReusesLifoOrder) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    LeaseWithCredit first;
    LeaseWithCredit second;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), first).ok());
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), second).ok());
    const uint32_t first_id = first.lease.lane().jetty_id;
    const uint32_t second_id = second.lease.lane().jetty_id;
    ASSERT_NE(first_id, second_id);

    // Return first then second: LIFO means the next lease takes `second`.
    ASSERT_TRUE(harness.pool->returnOnce(std::move(first.lease), true).ok());
    ASSERT_TRUE(harness.pool->returnOnce(std::move(second.lease), true).ok());

    LeaseWithCredit third;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 3, 1), third).ok());
    EXPECT_EQ(third.lease.lane().jetty_id, second_id);
    LeaseWithCredit fourth;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 4, 1), fourth).ok());
    EXPECT_EQ(fourth.lease.lane().jetty_id, first_id);
    // Reuse: no additional native creation.
    EXPECT_EQ(harness.adapter->created(), 2);
}

TEST(UbSendJettyPoolTest, JfcCreditMatchingSkipsExhaustedMember) {
    PoolHarness harness;
    // Two JFCs with one WR credit each; two members rotate across them.
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 2, 1).ok());
    const auto key = makePoolKey();

    LeaseWithCredit first;
    LeaseWithCredit second;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), first).ok());
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), second).ok());
    ASSERT_NE(first.jfc_index, second.jfc_index);
    const size_t exhausted_jfc = first.jfc_index;
    const uint32_t other_jetty = second.lease.lane().jetty_id;

    ASSERT_TRUE(harness.pool->returnOnce(std::move(first.lease), true).ok());
    harness.credits->release(first.jfc_index, first.jfc_credit_token);
    ASSERT_TRUE(harness.pool->returnOnce(std::move(second.lease), true).ok());
    harness.credits->release(second.jfc_index, second.jfc_credit_token);

    // Exhaust the credit of the most recently returned member's JFC: the
    // lease must skip it inside the LIFO order and take the other member.
    auto blocker = harness.credits->tryReserve(exhausted_jfc, 1);
    ASSERT_TRUE(blocker.has_value());

    LeaseWithCredit matched;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 3, 1), matched).ok());
    EXPECT_EQ(matched.lease.lane().jetty_id, other_jetty);
    EXPECT_EQ(matched.jfc_index, second.jfc_index);

    // Complete the matched attempt, then exhaust the remaining JFC too:
    // with both JFC credits gone the lease defers (no bypass).
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(matched.lease), true).ok());
    harness.credits->release(matched.jfc_index, matched.jfc_credit_token);
    auto blocker2 = harness.credits->tryReserve(second.jfc_index, 1);
    ASSERT_TRUE(blocker2.has_value());
    LeaseWithCredit deferred;
    EXPECT_TRUE(harness.pool
                    ->tryLease(key, makeAttemptKey(1, 4, 1), deferred)
                    .IsTooManyRequests());

    harness.credits->release(exhausted_jfc, *blocker);
    harness.credits->release(second.jfc_index, *blocker2);
}

TEST(UbSendJettyPoolTest, BudgetExhaustionDefersWithoutCreation) {
    PoolHarness harness;
    // Budget allows 2 Jettys while Serving wants 4.
    ASSERT_TRUE(harness.init(4, 2, 2, 0, 1, 16).ok());
    const auto key = makePoolKey();

    std::vector<LeaseWithCredit> leases;
    for (uint64_t generation = 1; generation <= 2; ++generation) {
        LeaseWithCredit grant;
        ASSERT_TRUE(
            harness.pool
                ->tryLease(key, makeAttemptKey(1, generation, generation),
                           grant)
                .ok());
        leases.push_back(std::move(grant));
    }
    ASSERT_TRUE(harness.pool
                    ->returnOnce(std::move(leases[0].lease), true)
                    .ok());
    ASSERT_TRUE(harness.pool
                    ->returnOnce(std::move(leases[1].lease), true)
                    .ok());

    // Hold both members in use again, then exhaust budget behind them: the
    // pool can never grow past the device budget.
    LeaseWithCredit hold_first;
    LeaseWithCredit hold_second;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(2, 1, 1), hold_first)
            .ok());
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(2, 2, 1), hold_second)
            .ok());
    LeaseWithCredit deferred;
    EXPECT_TRUE(harness.pool
                    ->tryLease(key, makeAttemptKey(2, 3, 1), deferred)
                    .IsTooManyRequests());
    EXPECT_EQ(harness.adapter->created(), 2);

    const auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.active_jetties, 2u);
    EXPECT_LT(0u, snapshot.rejected_reservations);
}

TEST(UbSendJettyPoolTest, CreateFailureDefersAndReleasesBudget) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();
    harness.adapter->failNextCreates(1);

    LeaseWithCredit grant;
    EXPECT_TRUE(harness.pool
                    ->tryLease(key, makeAttemptKey(1, 1, 1), grant)
                    .IsTooManyRequests());
    EXPECT_EQ(harness.pool->stats().create_failures, 1u);
    // The failed creation released its budget reservation.
    const auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 0u);

    // The next round succeeds: the pool recovered without a restart.
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), grant).ok());
    EXPECT_TRUE(grant.lease.valid());
}

TEST(UbSendJettyPoolTest, StopRejectsNewLeasesAndRetiresOutstanding) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());

    harness.pool->beginStop();
    EXPECT_TRUE(harness.pool->stopping());

    LeaseWithCredit rejected;
    EXPECT_TRUE(harness.pool
                    ->tryLease(key, makeAttemptKey(1, 2, 1), rejected)
                    .IsTooManyRequests());
    EXPECT_EQ(harness.pool->stats().stop_rejections, 1u);

    // An outstanding lease still settles; its member retires instead of
    // re-entering the idle stack.
    ASSERT_TRUE(harness.pool->returnOnce(std::move(grant.lease), true).ok());
    EXPECT_EQ(harness.adapter->deleted(), 1);
    EXPECT_EQ(harness.pool->servingCount(key), 0u);
    const auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 0u);
}

TEST(UbSendJettyPoolTest, FaultRetiresPermanently) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
    const uint32_t faulted_id = grant.lease.lane().jetty_id;
    ASSERT_TRUE(
        harness.pool->retireFaulted(std::move(grant.lease), "cqe error").ok());
    EXPECT_EQ(harness.adapter->deleted(), 1);

    // The retired member never returns to circulation: fresh leases create
    // a new native Jetty with a new lane generation.
    LeaseWithCredit next;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), next).ok());
    EXPECT_NE(next.lease.lane().jetty_id, faulted_id);
    EXPECT_EQ(harness.adapter->created(), 2);
    const auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.active_jetties, 1u);
    EXPECT_EQ(snapshot.total_jetties, 1u);
}

TEST(UbSendJettyPoolTest, GateFailedReturnRetiresMember) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
    // completion_gate_passed=false: the member retires even though no fault
    // was reported (Unknown responsibility is never reused, INV08/INV09).
    ASSERT_TRUE(harness.pool->returnOnce(std::move(grant.lease), false).ok());
    EXPECT_EQ(harness.adapter->deleted(), 1);
    EXPECT_EQ(harness.pool->servingCount(key), 0u);
}

TEST(UbSendJettyPoolTest, DoubleReturnRejected) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(1, 1, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
    SliceJettyLease lease = std::move(grant.lease);
    // Fabricate a second lease on the same member to force a double return.
    SliceJettyLease duplicate;
    duplicate.pool_ = harness.pool;
    duplicate.member_ = lease.member();
    duplicate.attempt_ = lease.attempt();

    ASSERT_TRUE(harness.pool->returnOnce(std::move(lease), true).ok());
    EXPECT_TRUE(
        harness.pool->returnOnce(std::move(duplicate), true)
            .IsInvalidArgument());
    EXPECT_EQ(harness.pool->stats().double_returns, 1u);
}

TEST(UbSendJettyPoolTest, DroppedLeaseQuarantinesAndKeepsBudget) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    {
        LeaseWithCredit grant;
        ASSERT_TRUE(
            harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant)
                .ok());
        // The lease drops without returnOnce(): responsibility is unknown.
    }

    const auto stats = harness.pool->stats();
    EXPECT_EQ(stats.leaked_leases, 1u);
    EXPECT_EQ(stats.quarantined_members, 1u);
    EXPECT_EQ(stats.in_use_members, 0u);
    // The budget stays charged while destruction is unproven (INV09).
    auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.quarantined_jetties, 1u);
    EXPECT_EQ(harness.adapter->deleted(), 0);

    // Shutdown drains the quarantine: proven deletion releases the budget.
    ASSERT_TRUE(harness.pool->drainQuarantined().ok());
    EXPECT_EQ(harness.adapter->deleted(), 1);
    snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 0u);
}

TEST(UbSendJettyPoolTest, DeleteFailureQuarantinesRetiredMember) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
    harness.adapter->failNextDeletes(1);
    ASSERT_TRUE(
        harness.pool->retireFaulted(std::move(grant.lease), "cqe error").ok());

    // Unknown destruction keeps owner and budget (INV10).
    EXPECT_EQ(harness.pool->stats().quarantined_members, 1u);
    auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.quarantined_jetties, 1u);

    // A later drain attempt succeeds and releases the budget.
    ASSERT_TRUE(harness.pool->drainQuarantined().ok());
    EXPECT_EQ(harness.adapter->deleted(), 1);
    snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 0u);
}

TEST(UbSendJettyPoolTest, DrainIdleDeletesOnlyIdleMembers) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(3, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    std::vector<LeaseWithCredit> leases;
    for (uint64_t generation = 1; generation <= 3; ++generation) {
        LeaseWithCredit grant;
        ASSERT_TRUE(
            harness.pool
                ->tryLease(key, makeAttemptKey(1, generation, generation),
                           grant)
                .ok());
        leases.push_back(std::move(grant));
    }
    ASSERT_TRUE(harness.pool->returnOnce(std::move(leases[0].lease), true)
                    .ok());
    ASSERT_TRUE(harness.pool->returnOnce(std::move(leases[1].lease), true)
                    .ok());
    // One member remains leased: the RC-03 shutdown ordering contract is
    // violated, so drainIdle still deletes the idle members but surfaces
    // InternalError instead of silently closing the parent JFC.
    const Status drained = harness.pool->drainIdle();
    EXPECT_TRUE(drained.IsInternalError());
    EXPECT_EQ(harness.adapter->deleted(), 2);
    EXPECT_EQ(harness.pool->servingCount(key), 1u);
    ASSERT_TRUE(harness.pool->returnOnce(std::move(leases[2].lease), true)
                    .ok());
}

TEST(UbSendJettyPoolTest, InvalidArgumentsRejected) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(1, 1, 8, 0, 1, 16).ok());
    LeaseWithCredit grant;
    // Invalid attempt key (zero generation).
    EXPECT_TRUE(harness.pool
                    ->tryLease(makePoolKey(), AttemptKey{1, 1, 0}, grant)
                    .IsInvalidArgument());
    // Returning an empty lease.
    EXPECT_TRUE(
        harness.pool->returnOnce(SliceJettyLease{}, true).IsInvalidArgument());
    EXPECT_TRUE(harness.pool
                    ->retireFaulted(SliceJettyLease{}, "x")
                    .IsInvalidArgument());
}

TEST(UbSendJettyPoolTest, DistinctPoolKeysNeverShareMembers) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(1, 1, 8, 0, 1, 16).ok());
    const auto key_a = makePoolKey(1);
    auto key_b = makePoolKey(1);
    key_b.priority = 3;  // different LocalSendProfile

    LeaseWithCredit lease_a;
    ASSERT_TRUE(
        harness.pool->tryLease(key_a, makeAttemptKey(1, 1, 1), lease_a).ok());
    LeaseWithCredit lease_b;
    ASSERT_TRUE(
        harness.pool->tryLease(key_b, makeAttemptKey(2, 1, 1), lease_b).ok());
    EXPECT_NE(lease_a.lease.lane().jetty_id, lease_b.lease.lane().jetty_id);
    EXPECT_EQ(harness.adapter->created(), 2);

    ASSERT_TRUE(harness.pool->returnOnce(std::move(lease_a.lease), true).ok());
    // key_b must not see key_a's returned member.
    LeaseWithCredit second_b;
    EXPECT_TRUE(harness.pool
                    ->tryLease(key_b, makeAttemptKey(2, 2, 1), second_b)
                    .IsTooManyRequests());
}

// ---------------------------------------------------------------------------
// AttemptGate (T-11 / C07 / D04)
// ---------------------------------------------------------------------------

TEST(UbAttemptGateTest, NormalSuccessReturnsToPool) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 4096);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, /*success=*/true, /*length=*/4096);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, /*pool_open=*/true, settlement));
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kSucceeded);
    EXPECT_EQ(settlement.safety, ResourceSafety::kSafe);
    EXPECT_TRUE(settlement.return_to_pool);
    EXPECT_FALSE(settlement.retire);

    // Single settlement: a second finalize never re-settles.
    EXPECT_FALSE(gate.tryFinalize(token, true, settlement));
    // Late events are counted but never re-settle.
    gate.onRealCompletion(token, true, 4096);
    EXPECT_EQ(gate.lateEventsAfterFinalize(token), 1u);
    gate.discard(token);
    EXPECT_EQ(gate.openAttempts(), 0u);
}

TEST(UbAttemptGateTest, EarlyCompletionWaitsForPostReturn) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    // The completion arrives before the post call returns.
    gate.onRealCompletion(token, true, 64);

    AttemptGate::Settlement settlement;
    EXPECT_FALSE(gate.tryFinalize(token, true, settlement));
    EXPECT_TRUE(settlement.wait);

    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_TRUE(settlement.return_to_pool);
}

TEST(UbAttemptGateTest, PostNotReturnedWaits) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    AttemptGate::Settlement settlement;
    EXPECT_FALSE(gate.tryFinalize(token, true, settlement));
    EXPECT_TRUE(settlement.wait);
}

TEST(UbAttemptGateTest, DuplicateCompletionIsContradiction) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, true, 64);
    gate.onRealCompletion(token, true, 64);  // duplicate real CQE

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kUnknown);
    EXPECT_EQ(settlement.safety, ResourceSafety::kUnsafe);
    EXPECT_TRUE(settlement.retire);
}

TEST(UbAttemptGateTest, InvalidLengthIsContradiction) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, true, /*length=*/32);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kUnknown);
    EXPECT_TRUE(settlement.retire);
}

TEST(UbAttemptGateTest, FailedCompletionRetires) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, /*success=*/false, 0);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kFailed);
    EXPECT_TRUE(settlement.retire);
}

TEST(UbAttemptGateTest, DefinitiveZeroAcceptRollsBack) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::TooManyRequests("provider queue full"),
                      PostAcceptance::kNotAccepted);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kNotExecuted);
    EXPECT_EQ(settlement.safety, ResourceSafety::kSafe);
    EXPECT_TRUE(settlement.rollback_return);
    EXPECT_FALSE(settlement.retire);
}

TEST(UbAttemptGateTest, NotAcceptedWithCqeKeepsResponsibility) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::InternalError("provider ambiguous"),
                      PostAcceptance::kNotAccepted);
    gate.onRealCompletion(token, true, 64);  // contradiction

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kUnknown);
    EXPECT_EQ(settlement.safety, ResourceSafety::kUnsafe);
    EXPECT_TRUE(settlement.retire);
}

TEST(UbAttemptGateTest, UnknownAcceptanceHoldsResponsibility) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::InternalError("timeout"),
                      PostAcceptance::kUnknown);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kUnknown);
    EXPECT_EQ(settlement.safety, ResourceSafety::kUnproven);
    EXPECT_TRUE(settlement.retire);
}

TEST(UbAttemptGateTest, HandlerReferencesBlockSettlement) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, true, 64);
    gate.onHandlerEnter(token);

    AttemptGate::Settlement settlement;
    EXPECT_FALSE(gate.tryFinalize(token, true, settlement));
    EXPECT_TRUE(settlement.wait);

    gate.onHandlerExit(token);
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_TRUE(settlement.return_to_pool);
}

TEST(UbAttemptGateTest, ClosedPoolRetiresSuccessfulAttempt) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, true, 64);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, /*pool_open=*/false, settlement));
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kSucceeded);
    EXPECT_FALSE(settlement.return_to_pool);
    EXPECT_TRUE(settlement.retire);
}

TEST(UbAttemptGateTest, FaultRetiresRegardlessOfCompletions) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 1}, 64);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.reportFault(token, "jetty async event");

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    EXPECT_TRUE(settlement.retire);
    EXPECT_EQ(settlement.safety, ResourceSafety::kUnsafe);
    EXPECT_EQ(settlement.outcome, ExecutionOutcome::kUnknown);
}

TEST(UbAttemptGateTest, UnknownTokenWaits) {
    AttemptGate gate;
    AttemptGate::Settlement settlement;
    EXPECT_FALSE(gate.tryFinalize(999, true, settlement));
    EXPECT_TRUE(settlement.wait);
    gate.discard(999);  // harmless
}

// ---------------------------------------------------------------------------
// End-to-end: lease -> post -> gate -> return (normal + degraded paths)
// ---------------------------------------------------------------------------

TEST(UbPooledDataPathTest, NormalAttemptReusesMember) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(1, 1, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();
    AttemptGate gate;

    // Attempt 1: lease, post accepted, one SUCCESS completion, return.
    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
    const uint32_t jetty_id = grant.lease.lane().jetty_id;
    const auto token = gate.registerAttempt(
        makeAttemptKey(1, 1, 1), grant.lease.lane(), /*expected_length=*/128);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, true, 128);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, !harness.pool->stopping(),
                                 settlement));
    ASSERT_TRUE(settlement.return_to_pool);
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(grant.lease), true).ok());
    gate.discard(token);

    // Attempt 2 reuses the same member.
    LeaseWithCredit second;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), second).ok());
    EXPECT_EQ(second.lease.lane().jetty_id, jetty_id);
    EXPECT_EQ(harness.adapter->created(), 1);
}

TEST(UbPooledDataPathTest, DegradedAttemptRetiresAndRebuilds) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(1, 1, 8, 1, 1, 16).ok());
    const auto key = makePoolKey();
    AttemptGate gate;

    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
    const uint32_t first_id = grant.lease.lane().jetty_id;
    const auto token = gate.registerAttempt(
        makeAttemptKey(1, 1, 1), grant.lease.lane(), 128);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, /*success=*/false, 0);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    ASSERT_TRUE(settlement.retire);
    ASSERT_TRUE(harness.pool
                    ->retireFaulted(std::move(grant.lease), "failed cqe")
                    .ok());
    gate.discard(token);

    // The replacement attempt gets a fresh member; budget reserve allowed
    // the rebuild while the retired object was still accounted.
    LeaseWithCredit replacement;
    ASSERT_TRUE(harness.pool
                    ->tryLease(key, makeAttemptKey(1, 2, 1), replacement)
                    .ok());
    EXPECT_NE(replacement.lease.lane().jetty_id, first_id);
    EXPECT_EQ(harness.adapter->created(), 2);
}

// ---------------------------------------------------------------------------
// High-concurrency reuse + leak defence
// ---------------------------------------------------------------------------

TEST(UbSendJettyPoolConcurrencyTest, HammerLeaseReturnKeepsInvariants) {
    PoolHarness harness;
    constexpr size_t kDesired = 4;
    ASSERT_TRUE(harness.init(kDesired, /*max_creating=*/4, /*budget=*/4,
                             /*reserve=*/0, /*jfcs=*/2,
                             /*jfc_capacity=*/1024)
                    .ok());
    const auto key = makePoolKey();

    constexpr int kThreads = 8;
    constexpr int kRounds = 500;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> acquired{0};
    std::atomic<uint64_t> deferred{0};

    // Track exclusive ownership: no lane may be leased twice concurrently.
    std::mutex lane_mutex;
    std::unordered_set<uint32_t> live_lanes;
    std::atomic<bool> ownership_violation{false};

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            for (int round = 0; round < kRounds && !stop.load(); ++round) {
                LeaseWithCredit grant;
                const auto attempt = makeAttemptKey(
                    static_cast<uint64_t>(thread + 1),
                    static_cast<uint64_t>(round + 1), 1);
                const Status status =
                    harness.pool->tryLease(key, attempt, grant);
                if (status.IsTooManyRequests()) {
                    ++deferred;
                    continue;
                }
                if (!status.ok()) {
                    ownership_violation.store(true);
                    stop.store(true);
                    return;
                }
                ++acquired;
                {
                    std::lock_guard<std::mutex> lock(lane_mutex);
                    if (!live_lanes
                             .insert(grant.lease.lane().jetty_id)
                             .second) {
                        ownership_violation.store(true);
                        stop.store(true);
                        return;
                    }
                }
                // Release the JFC credit like a completion dispatch would.
                harness.credits->release(grant.jfc_index,
                                         grant.jfc_credit_token);
                {
                    std::lock_guard<std::mutex> lock(lane_mutex);
                    live_lanes.erase(grant.lease.lane().jetty_id);
                }
                if (!harness.pool->returnOnce(std::move(grant.lease), true)
                         .ok()) {
                    ownership_violation.store(true);
                    stop.store(true);
                    return;
                }
            }
        });
    }
    for (auto& worker : threads) worker.join();

    EXPECT_FALSE(ownership_violation.load());
    EXPECT_GT(acquired.load(), 0u);

    const auto stats = harness.pool->stats();
    EXPECT_EQ(stats.leaked_leases, 0u);
    EXPECT_EQ(stats.double_returns, 0u);
    EXPECT_EQ(stats.in_use_members, 0u);
    EXPECT_EQ(stats.quarantined_members, 0u);
    // Static Serving bound held throughout: no more than `desired` members
    // were ever created.
    EXPECT_LE(stats.created_members, kDesired);
    EXPECT_EQ(stats.created_members,
              static_cast<uint64_t>(harness.adapter->created()));
    EXPECT_EQ(stats.total_leases, acquired.load());
    EXPECT_EQ(stats.total_returns, acquired.load());

    // Every member is reusable after the storm.
    std::vector<LeaseWithCredit> final_leases;
    for (uint64_t generation = 1; generation <= kDesired; ++generation) {
        LeaseWithCredit grant;
        ASSERT_TRUE(
            harness.pool
                ->tryLease(key, makeAttemptKey(999, generation, 1), grant)
                .ok());
        final_leases.push_back(std::move(grant));
    }
    for (auto& grant : final_leases) {
        ASSERT_TRUE(
            harness.pool->returnOnce(std::move(grant.lease), true).ok());
    }
    ASSERT_TRUE(harness.pool->drainIdle().ok());
    EXPECT_EQ(harness.adapter->deleted(), harness.adapter->created());
    const auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 0u);
}

TEST(UbSendJettyPoolConcurrencyTest, HammerWithStopDrainsSafely) {
    PoolHarness harness;
    constexpr size_t kDesired = 3;
    ASSERT_TRUE(harness.init(kDesired, 3, 8, 0, 1, 1024).ok());
    const auto key = makePoolKey();

    constexpr int kThreads = 6;
    constexpr int kRounds = 300;
    std::atomic<bool> stop_requested{false};
    std::atomic<uint64_t> stop_rejected{0};

    std::vector<std::thread> threads;
    threads.reserve(kThreads + 1);
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            for (int round = 0; round < kRounds; ++round) {
                // Signal mid-storm unconditionally: a TooManyRequests defer
                // on this round must not skip the stop request, or the
                // stopper thread below would spin forever (livelock).
                if (round == kRounds / 2 && thread == 0) {
                    stop_requested.store(true);
                }
                LeaseWithCredit grant;
                const auto attempt = makeAttemptKey(
                    static_cast<uint64_t>(thread + 1),
                    static_cast<uint64_t>(round + 1), 1);
                const Status status =
                    harness.pool->tryLease(key, attempt, grant);
                if (status.IsTooManyRequests()) {
                    if (harness.pool->stopping()) ++stop_rejected;
                    continue;
                }
                ASSERT_TRUE(status.ok());
                harness.credits->release(grant.jfc_index,
                                         grant.jfc_credit_token);
                // After stop begins, returns retire instead of reuse.
                ASSERT_TRUE(harness.pool
                                ->returnOnce(std::move(grant.lease), true)
                                .ok());
            }
            // Fail-safe: even if an ASSERT above aborts this worker early,
            // the stopper must not spin forever and mask the failure.
            if (thread == 0) stop_requested.store(true);
        });
    }
    threads.emplace_back([&] {
        while (!stop_requested.load()) {
            std::this_thread::yield();
        }
        harness.pool->beginStop();
    });
    for (auto& worker : threads) worker.join();

    EXPECT_TRUE(harness.pool->stopping());
    const auto stats = harness.pool->stats();
    EXPECT_EQ(stats.leaked_leases, 0u);
    EXPECT_EQ(stats.in_use_members, 0u);
    // Members returned after stop were retired; members already idle wait
    // for the shutdown drain.
    EXPECT_EQ(stats.total_returns, stats.total_leases);

    ASSERT_TRUE(harness.pool->drainIdle().ok());
    EXPECT_EQ(harness.pool->stats().idle_members, 0u);
    // Every created member was retired or drained, and deleted exactly once.
    EXPECT_EQ(harness.adapter->deleted(), harness.adapter->created());
    const auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 0u);
}

// ---------------------------------------------------------------------------
// UbParams pooled keys (C01)
// ---------------------------------------------------------------------------

namespace {

Status parseUbParams(const std::string& json_text, UbParams& params) {
    Config config;
    const Status loaded = config.load(json_text);
    if (!loaded.ok()) return loaded;
    return UbParams::FromConfig(config, params);
}

}  // namespace

TEST(UbParamsPooledTest, DefaultsAreLegacySafe) {
    UbParams params;
    ASSERT_TRUE(parseUbParams("{}", params).ok());
    EXPECT_EQ(params.send_pool.desired_per_context, 6u);
    EXPECT_EQ(params.send_pool.task_max_inflight_attempts, 4u);
    EXPECT_EQ(params.send_pool.max_creating_per_key, 2u);
    EXPECT_EQ(params.send_pool.jfc_credit_reserve, 16u);
    EXPECT_EQ(params.native_budget.max_jetty_per_device, 0u);
    EXPECT_EQ(params.native_budget.replacement_reserve, 2u);
}

TEST(UbParamsPooledTest, ParsesExplicitPoolKeys) {
    const std::string config_json = R"({
        "transports": {
            "ub": {
                "send_pool": {
                    "desired_per_context": 8,
                    "task_max_inflight_attempts": 2,
                    "max_creating_per_key": 1,
                    "jfc_credit_reserve": 4
                },
                "native_budget": {
                    "max_jetty_per_device": 12,
                    "replacement_reserve": 3
                }
            }
        }
    })";
    UbParams params;
    ASSERT_TRUE(parseUbParams(config_json, params).ok());
    EXPECT_EQ(params.send_pool.desired_per_context, 8u);
    EXPECT_EQ(params.send_pool.task_max_inflight_attempts, 2u);
    EXPECT_EQ(params.send_pool.max_creating_per_key, 1u);
    EXPECT_EQ(params.send_pool.jfc_credit_reserve, 4u);
    EXPECT_EQ(params.native_budget.max_jetty_per_device, 12u);
    EXPECT_EQ(params.native_budget.replacement_reserve, 3u);
}

TEST(UbParamsPooledTest, RejectsInvalidPoolKeys) {
    UbParams params;
    // Negative values are rejected (signed parse guard).
    ASSERT_FALSE(parseUbParams(R"({
                         "transports": {
                             "ub": {
                                 "send_pool": {"desired_per_context": -1}
                             }
                         }
                     })",
                               params)
                     .ok());
    // Zero is rejected for the explicit positive knobs.
    ASSERT_FALSE(parseUbParams(R"({
                         "transports": {
                             "ub": {
                                 "send_pool": {
                                     "task_max_inflight_attempts": 0
                                 }
                             }
                         }
                     })",
                               params)
                     .ok());
    // Non-integer values are rejected.
    ASSERT_FALSE(parseUbParams(R"({
                         "transports": {
                             "ub": {
                                 "native_budget": {
                                     "replacement_reserve": "two"
                                 }
                             }
                         }
                     })",
                               params)
                     .ok());
}

TEST(UbParamsPooledTest, PooledProfileOpensForWellFormedConfig) {
    // The rm_pool promotion gate accepts a structurally complete
    // configuration (INV16 opened): provider/capability, native budget and
    // JFC credit gates still run during install before any pooled Jetty is
    // created.
    UbParams params;
    ASSERT_TRUE(parseUbParams(R"({
                        "transports": {
                            "ub": {
                                "runtime_profile": "rm_pool",
                                "trans_mode": "rm",
                                "physical_only": true,
                                "device_identities": [{
                                    "native_device_name": "urma0",
                                    "native_device_path": "/sys/class/urma/urma0",
                                    "eid": "fe80::0001",
                                    "eid_index": 0,
                                    "transport_modes": 3,
                                    "kind": "physical"
                                }],
                                "jetty_pool": {"enable": true},
                                "probe": {"enabled": true}
                            }
                        }
                    })",
                              params)
                    .ok());
    UbRuntimeMode mode;
    const Status status = resolveUbRuntimeMode(params, mode);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(mode, UbRuntimeMode::kPooledRm);
}

// ---------------------------------------------------------------------------
// UbContext pooled attach point (integration seam)
// ---------------------------------------------------------------------------

TEST(UbContextPooledAttachTest, AttachIsDormantForLegacyPaths) {
    auto adapter = std::make_shared<PooledFakeAdapter>();
    DeviceInfo device{};
    device.native_device_name = "fake0";
    device.active = true;
    auto context = std::make_shared<UbContext>(Topology::NicID{0}, device,
                                               adapter);

    // No pooled pool attached: legacy behavior unchanged.
    EXPECT_EQ(context->pooledSendPool(), nullptr);

    auto budget = std::make_shared<NativeBudget>();
    ASSERT_TRUE(budget->configure("dev0", 0, 2, 0).ok());
    auto credits = std::make_shared<JfcCreditLedger>();
    ASSERT_TRUE(credits->install(0, 8).ok());
    ContextPtr handle = std::make_shared<PooledFakeContext>();
    std::vector<std::shared_ptr<UbJfc>> jfcs;
    JfcPtr jfc_handle;
    ASSERT_TRUE(adapter->createJfc(handle, JfcOptions{}, jfc_handle).ok());
    jfcs.push_back(
        std::make_shared<UbJfc>(0, adapter, std::move(jfc_handle)));
    SendJettyPoolOptions options;
    options.desired_serving = 1;
    auto pool = std::make_shared<SendJettyPool>(
        adapter, handle, jfcs, budget, "dev0", credits, options);

    context->attachPooledSendPool(pool);
    EXPECT_EQ(context->pooledSendPool(), pool);
    context->attachPooledSendPool(nullptr);
    EXPECT_EQ(context->pooledSendPool(), nullptr);
}

// ---------------------------------------------------------------------------
// NodeTargetQuota (guidance 4.2.1): process-authoritative node target quota.
// The registry is process-global, so every test isolates it explicitly.
// ---------------------------------------------------------------------------

class UbNodeTargetQuotaTest : public ::testing::Test {
   protected:
    void SetUp() override { NodeTargetQuota::resetRegistryForTest(); }
    void TearDown() override { NodeTargetQuota::resetRegistryForTest(); }
};

TEST_F(UbNodeTargetQuotaTest, AcquireValidationAndRegistryFreeze) {
    std::shared_ptr<NodeTargetQuota> quota;
    // 0 is never "unlimited".
    EXPECT_TRUE(NodeTargetQuota::acquire(0, quota).IsInvalidArgument());
    EXPECT_EQ(quota, nullptr);

    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());
    ASSERT_NE(quota, nullptr);
    EXPECT_EQ(quota->limit(), 8u);

    // Same limit: every later acquire shares the one authoritative ledger.
    std::shared_ptr<NodeTargetQuota> again;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, again).ok());
    EXPECT_EQ(again.get(), quota.get());

    // Different limit: fail closed, never widen or split the ledger.
    std::shared_ptr<NodeTargetQuota> different;
    EXPECT_TRUE(NodeTargetQuota::acquire(4, different).IsInvalidArgument());
    EXPECT_EQ(different, nullptr);
    EXPECT_EQ(quota->limit(), 8u);

    // Test-only reset installs a fresh ledger.
    NodeTargetQuota::resetRegistryForTest();
    std::shared_ptr<NodeTargetQuota> fresh;
    ASSERT_TRUE(NodeTargetQuota::acquire(4, fresh).ok());
    EXPECT_NE(fresh.get(), quota.get());
    EXPECT_EQ(fresh->limit(), 4u);

    // The full uint32 range is a legal limit.
    NodeTargetQuota::resetRegistryForTest();
    std::shared_ptr<NodeTargetQuota> maximal;
    ASSERT_TRUE(NodeTargetQuota::acquire(std::numeric_limits<uint32_t>::max(),
                                         maximal)
                    .ok());
    EXPECT_EQ(maximal->limit(), std::numeric_limits<uint32_t>::max());
}

TEST_F(UbNodeTargetQuotaTest, StaticDemandValidation) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());
    EXPECT_TRUE(quota->validateStaticDemand(0).ok());
    EXPECT_TRUE(quota->validateStaticDemand(8).ok());
    EXPECT_TRUE(quota->validateStaticDemand(9).IsInvalidArgument());
    EXPECT_TRUE(
        quota->validateStaticDemand(std::numeric_limits<uint64_t>::max())
            .IsInvalidArgument());
    // The static check never mutates the occupancy accounting.
    EXPECT_EQ(quota->snapshot().total_occupied, 0u);
}

TEST_F(UbNodeTargetQuotaTest, ReservationBoundaryAndRejectedCounter) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());

    std::vector<TargetQuotaTicket> tickets;
    for (int index = 0; index < 8; ++index) {
        auto reserved = quota->tryReserve();
        ASSERT_TRUE(reserved.has_value()) << index;
        tickets.push_back(std::move(*reserved));
    }
    auto snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.creating_reserved, 8u);
    EXPECT_EQ(snapshot.total_occupied, 8u);
    EXPECT_EQ(snapshot.rejected_reservations, 0u);

    // The 9th reservation defers and is counted; occupancy never exceeds 8.
    EXPECT_FALSE(quota->tryReserve().has_value());
    snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 8u);
    EXPECT_EQ(snapshot.rejected_reservations, 1u);

    // A proven-unallocated rollback frees exactly one slot for reuse.
    ASSERT_TRUE(quota->releaseUnallocated(tickets[0]).ok());
    EXPECT_FALSE(tickets[0].valid());
    auto replacement = quota->tryReserve();
    ASSERT_TRUE(replacement.has_value());
    EXPECT_EQ(quota->snapshot().total_occupied, 8u);

    ASSERT_TRUE(quota->releaseUnallocated(*replacement).ok());
    for (size_t index = 1; index < tickets.size(); ++index) {
        ASSERT_TRUE(quota->releaseUnallocated(tickets[index]).ok());
    }
    snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 0u);
    EXPECT_EQ(snapshot.leaked_tickets, 0u);
}

TEST_F(UbNodeTargetQuotaTest, FullLifecycleReleasesOnlyWithProof) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());
    auto reserved = quota->tryReserve();
    ASSERT_TRUE(reserved.has_value());
    TargetQuotaTicket ticket = std::move(*reserved);
    EXPECT_EQ(ticket.state(), TargetObjectState::kCreatingReserved);

    ASSERT_TRUE(
        quota->transition(ticket, TargetObjectState::kActive).ok());
    EXPECT_EQ(ticket.state(), TargetObjectState::kActive);
    EXPECT_EQ(quota->snapshot().active, 1u);

    ASSERT_TRUE(
        quota->transition(ticket, TargetObjectState::kRetiringDeleting).ok());
    EXPECT_EQ(ticket.state(), TargetObjectState::kRetiringDeleting);
    EXPECT_EQ(quota->snapshot().retiring_deleting, 1u);

    // Unproven destruction never releases the slot.
    EXPECT_TRUE(
        quota->releaseAfterProvenDestruction(ticket, false).IsInvalidArgument());
    EXPECT_TRUE(ticket.valid());
    EXPECT_EQ(quota->snapshot().total_occupied, 1u);

    ASSERT_TRUE(quota->releaseAfterProvenDestruction(ticket, true).ok());
    EXPECT_FALSE(ticket.valid());
    const auto snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 0u);
    EXPECT_EQ(snapshot.leaked_tickets, 0u);

    // A released ticket is detached: every later operation fails closed.
    EXPECT_TRUE(
        quota->transition(ticket, TargetObjectState::kActive).IsInvalidArgument());
    EXPECT_TRUE(quota->releaseUnallocated(ticket).IsInvalidArgument());
    EXPECT_TRUE(
        quota->releaseAfterProvenDestruction(ticket, true).IsInvalidArgument());
}

TEST_F(UbNodeTargetQuotaTest, TransitionEdgeMatrix) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());

    constexpr auto kCreating = TargetObjectState::kCreatingReserved;
    constexpr auto kActive = TargetObjectState::kActive;
    constexpr auto kRetiring = TargetObjectState::kRetiringDeleting;
    constexpr auto kQuarantined = TargetObjectState::kQuarantinedUnproven;
    const TargetObjectState states[] = {kCreating, kActive, kRetiring,
                                        kQuarantined};

    const auto reserveInState = [&](TargetObjectState state) {
        auto reserved = quota->tryReserve();
        EXPECT_TRUE(reserved.has_value());
        TargetQuotaTicket ticket = std::move(*reserved);
        if (state == kActive || state == kRetiring) {
            EXPECT_TRUE(quota->transition(ticket, kActive).ok());
        }
        if (state == kRetiring) {
            EXPECT_TRUE(quota->transition(ticket, kRetiring).ok());
        }
        if (state == kQuarantined) {
            EXPECT_TRUE(quota->transition(ticket, kQuarantined).ok());
        }
        return ticket;
    };
    const auto edgeAllowed = [](TargetObjectState from, TargetObjectState to) {
        switch (from) {
            case kCreating:
                return to == kActive || to == kQuarantined;
            case kActive:
                return to == kRetiring || to == kQuarantined;
            case kRetiring:
                return to == kQuarantined;
            default:
                return false;
        }
    };

    for (const auto from : states) {
        for (const auto to : states) {
            TargetQuotaTicket ticket = reserveInState(from);
            const bool allowed = edgeAllowed(from, to);
            const Status status = quota->transition(ticket, to);
            EXPECT_EQ(status.ok(), allowed)
                << targetObjectStateName(from) << " -> "
                << targetObjectStateName(to);
            EXPECT_EQ(ticket.state(), allowed ? to : from);
            EXPECT_EQ(quota->snapshot().total_occupied, 1u);
            // Settle the ticket so the next iteration reserves from a clean
            // ledger (the limit is 8 and the matrix runs 16 iterations).
            if (ticket.state() == kCreating) {
                EXPECT_TRUE(quota->releaseUnallocated(ticket).ok());
            } else {
                if (ticket.state() == kActive) {
                    EXPECT_TRUE(quota->transition(ticket, kRetiring).ok());
                }
                EXPECT_TRUE(
                    quota->releaseAfterProvenDestruction(ticket, true).ok());
            }
        }
    }
    const auto snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 0u);
    EXPECT_EQ(snapshot.leaked_tickets, 0u);

    // Invalid (default-constructed) tickets are rejected everywhere.
    TargetQuotaTicket invalid;
    EXPECT_FALSE(invalid.valid());
    EXPECT_TRUE(quota->transition(invalid, kActive).IsInvalidArgument());
    EXPECT_TRUE(quota->releaseUnallocated(invalid).IsInvalidArgument());
    EXPECT_TRUE(
        quota->releaseAfterProvenDestruction(invalid, true).IsInvalidArgument());
}

TEST_F(UbNodeTargetQuotaTest, ReleaseGatesRequireProofAndTerminalState) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());
    auto reserved = quota->tryReserve();
    ASSERT_TRUE(reserved.has_value());
    TargetQuotaTicket ticket = std::move(*reserved);

    // proof=false is rejected from every state and keeps the slot charged.
    EXPECT_TRUE(
        quota->releaseAfterProvenDestruction(ticket, false).IsInvalidArgument());
    // kCreatingReserved cannot use the proven-destruction gate.
    EXPECT_TRUE(
        quota->releaseAfterProvenDestruction(ticket, true).IsInvalidArgument());
    EXPECT_EQ(quota->snapshot().total_occupied, 1u);

    ASSERT_TRUE(
        quota->transition(ticket, TargetObjectState::kActive).ok());
    // kActive cannot use the unallocated rollback gate.
    EXPECT_TRUE(quota->releaseUnallocated(ticket).IsInvalidArgument());
    EXPECT_TRUE(
        quota->releaseAfterProvenDestruction(ticket, true).IsInvalidArgument());

    ASSERT_TRUE(
        quota->transition(ticket, TargetObjectState::kRetiringDeleting).ok());
    EXPECT_TRUE(quota->releaseUnallocated(ticket).IsInvalidArgument());
    EXPECT_TRUE(
        quota->releaseAfterProvenDestruction(ticket, false).IsInvalidArgument());
    EXPECT_EQ(quota->snapshot().total_occupied, 1u);

    ASSERT_TRUE(quota->releaseAfterProvenDestruction(ticket, true).ok());
    EXPECT_EQ(quota->snapshot().total_occupied, 0u);

    // The unallocated rollback works from kCreatingReserved and is single-use.
    auto rollback = quota->tryReserve();
    ASSERT_TRUE(rollback.has_value());
    ASSERT_TRUE(quota->releaseUnallocated(*rollback).ok());
    EXPECT_FALSE(rollback->valid());
    EXPECT_TRUE(quota->releaseUnallocated(*rollback).IsInvalidArgument());
    EXPECT_EQ(quota->snapshot().total_occupied, 0u);
}

TEST_F(UbNodeTargetQuotaTest, QuarantinedSlotsAreNeverBypassed) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());

    // Fill the node with quarantined (destruction-unproven) targets.
    std::vector<TargetQuotaTicket> tickets;
    for (int index = 0; index < 8; ++index) {
        auto reserved = quota->tryReserve();
        ASSERT_TRUE(reserved.has_value());
        TargetQuotaTicket ticket = std::move(*reserved);
        ASSERT_TRUE(quota
                        ->transition(ticket,
                                     TargetObjectState::kQuarantinedUnproven)
                        .ok());
        tickets.push_back(std::move(ticket));
    }
    auto snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.quarantined_unproven, 8u);
    EXPECT_EQ(snapshot.total_occupied, 8u);

    // No replacement bypass: a full node defers every new reservation even
    // though all slots are quarantined rather than active.
    for (int attempt = 0; attempt < 5; ++attempt) {
        EXPECT_FALSE(quota->tryReserve().has_value());
    }
    snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.rejected_reservations, 5u);
    EXPECT_EQ(snapshot.total_occupied, 8u);

    // Only proven destruction of a quarantined ticket frees its slot.
    ASSERT_TRUE(quota->releaseAfterProvenDestruction(tickets[0], true).ok());
    auto replacement = quota->tryReserve();
    ASSERT_TRUE(replacement.has_value());
    EXPECT_EQ(quota->snapshot().total_occupied, 8u);

    ASSERT_TRUE(quota->releaseUnallocated(*replacement).ok());
    for (size_t index = 1; index < tickets.size(); ++index) {
        ASSERT_TRUE(
            quota->releaseAfterProvenDestruction(tickets[index], true).ok());
    }
    EXPECT_EQ(quota->snapshot().total_occupied, 0u);
}

TEST_F(UbNodeTargetQuotaTest, MoveSemanticsAndLeakAccounting) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());

    auto first = quota->tryReserve();
    ASSERT_TRUE(first.has_value());
    TargetQuotaTicket moved = std::move(*first);
    EXPECT_FALSE(first->valid());
    EXPECT_TRUE(moved.valid());
    auto snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 1u);
    EXPECT_EQ(snapshot.leaked_tickets, 0u);

    // Move-assigning over a valid ticket leaks the overwritten slot: it stays
    // charged and is counted, never implicitly released.
    auto second = quota->tryReserve();
    ASSERT_TRUE(second.has_value());
    TargetQuotaTicket target = std::move(*second);
    target = std::move(moved);
    EXPECT_FALSE(moved.valid());
    EXPECT_TRUE(target.valid());
    snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 2u);
    EXPECT_EQ(snapshot.leaked_tickets, 1u);

    // Dropping a ticket without release is a bookkeeping leak, not a release.
    {
        TargetQuotaTicket dropped = std::move(target);
        EXPECT_TRUE(dropped.valid());
    }
    EXPECT_FALSE(target.valid());
    snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 2u);
    EXPECT_EQ(snapshot.leaked_tickets, 2u);
}

TEST_F(UbNodeTargetQuotaTest, TicketOutlivesLedgerWithoutUseAfterFree) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());
    auto first = quota->tryReserve();
    auto second = quota->tryReserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    TargetQuotaTicket destroyed_path = std::move(*first);
    TargetQuotaTicket assigned_path = std::move(*second);

    // Destroy the ledger while tickets still exist (test registry reset).
    NodeTargetQuota::resetRegistryForTest();
    quota.reset();

    // Both leak-notification paths must degrade to a no-op through the weak
    // reference instead of dereferencing a dead ledger.
    {
        TargetQuotaTicket sink = std::move(destroyed_path);
        EXPECT_TRUE(sink.valid());
    }
    assigned_path = TargetQuotaTicket{};
    EXPECT_FALSE(assigned_path.valid());

    // A fresh install starts from zero occupancy.
    std::shared_ptr<NodeTargetQuota> fresh;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, fresh).ok());
    EXPECT_EQ(fresh->snapshot().total_occupied, 0u);
}

TEST_F(UbNodeTargetQuotaTest, ConcurrentReservationLinearizes) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, quota).ok());

    constexpr int kThreads = 16;
    std::atomic<bool> go{false};
    std::atomic<int> rejected{0};
    std::mutex granted_mutex;
    std::vector<TargetQuotaTicket> granted;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&] {
            while (!go.load()) std::this_thread::yield();
            auto reserved = quota->tryReserve();
            if (!reserved.has_value()) {
                ++rejected;
                return;
            }
            std::lock_guard<std::mutex> lock(granted_mutex);
            granted.push_back(std::move(*reserved));
        });
    }
    go.store(true);
    for (auto& worker : threads) worker.join();

    // Exactly `limit` reservations succeed; the rest defer and are counted.
    EXPECT_EQ(granted.size(), 8u);
    EXPECT_EQ(rejected.load(), 8);
    auto snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 8u);
    EXPECT_EQ(snapshot.rejected_reservations, 8u);

    for (auto& ticket : granted) {
        ASSERT_TRUE(quota->releaseUnallocated(ticket).ok());
    }
    EXPECT_EQ(quota->snapshot().total_occupied, 0u);
    auto again = quota->tryReserve();
    EXPECT_TRUE(again.has_value());
    ASSERT_TRUE(quota->releaseUnallocated(*again).ok());
}

TEST_F(UbNodeTargetQuotaTest, ConcurrentReserveReleaseChurn) {
    std::shared_ptr<NodeTargetQuota> quota;
    ASSERT_TRUE(NodeTargetQuota::acquire(4, quota).ok());

    constexpr int kThreads = 8;
    constexpr int kRounds = 500;
    std::atomic<uint64_t> acquired{0};
    std::atomic<bool> violation{false};

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            for (int round = 0; round < kRounds; ++round) {
                auto reserved = quota->tryReserve();
                if (!reserved.has_value()) continue;
                ++acquired;
                TargetQuotaTicket ticket = std::move(*reserved);
                Status settled;
                if ((thread + round) % 2 == 0) {
                    // Full lifecycle: publish, retire, proven destruction.
                    if (!quota->transition(ticket, TargetObjectState::kActive)
                             .ok() ||
                        !quota
                             ->transition(ticket,
                                          TargetObjectState::kRetiringDeleting)
                             .ok()) {
                        violation.store(true);
                        return;
                    }
                    settled =
                        quota->releaseAfterProvenDestruction(ticket, true);
                } else {
                    settled = quota->releaseUnallocated(ticket);
                }
                if (!settled.ok()) {
                    violation.store(true);
                    return;
                }
            }
        });
    }
    for (auto& worker : threads) worker.join();

    EXPECT_FALSE(violation.load());
    EXPECT_GT(acquired.load(), 0u);
    const auto snapshot = quota->snapshot();
    EXPECT_EQ(snapshot.total_occupied, 0u);
    EXPECT_EQ(snapshot.leaked_tickets, 0u);
}

// ---------------------------------------------------------------------------
// UbParams: max_target_jetties_per_node strict reader matrix (guidance 4.2.1)
// ---------------------------------------------------------------------------

namespace {

std::string nodeTargetQuotaConfig(const std::string& value) {
    return R"({"transports":{"ub":{"max_target_jetties_per_node":)" + value +
           "}}}";
}

}  // namespace

TEST(UbParamsPooledTest, NodeTargetQuotaLimitDefaultsToEight) {
    UbParams params;
    ASSERT_TRUE(parseUbParams("{}", params).ok());
    EXPECT_EQ(params.max_target_jetties_per_node, 8u);
    // Absent inside a populated ub section keeps the default as well.
    ASSERT_TRUE(
        parseUbParams(R"({"transports":{"ub":{"enable":true}}})", params).ok());
    EXPECT_EQ(params.max_target_jetties_per_node, 8u);
}

TEST(UbParamsPooledTest, NodeTargetQuotaLimitAcceptsValidIntegers) {
    for (const std::string value : {"1", "4", "8", "4294967295"}) {
        UbParams params;
        ASSERT_TRUE(parseUbParams(nodeTargetQuotaConfig(value), params).ok())
            << value;
        EXPECT_EQ(params.max_target_jetties_per_node,
                  static_cast<uint32_t>(std::stoul(value)))
            << value;
    }
}

TEST(UbParamsPooledTest, NodeTargetQuotaLimitRejectsInvalidValues) {
    // 0 is never unlimited; negatives, bools, strings, floats, explicit null
    // and anything beyond UINT32_MAX (including beyond int64) fail closed.
    const std::vector<std::string> invalid = {
        "0", "-1", "true", "\"8\"", "8.5", "null", "4294967296",
        "9221111111111175808"};
    for (const auto& value : invalid) {
        UbParams params;
        EXPECT_FALSE(parseUbParams(nodeTargetQuotaConfig(value), params).ok())
            << value;
    }
}

// ---------------------------------------------------------------------------
// SendJettyPool gap regressions: output clobber guard, zero identities,
// drain/delete failure budget retention, destructor leak accounting.
// ---------------------------------------------------------------------------

TEST(UbSendJettyPoolTest, NonEmptyOutputRejectedWithoutClobber) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    LeaseWithCredit held;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), held).ok());
    const uint32_t held_id = held.lease.lane().jetty_id;
    const uint64_t held_token = held.jfc_credit_token;
    ASSERT_NE(held_token, 0u);

    // A non-empty output is rejected instead of dropping the caller's lease
    // and credit token.
    EXPECT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), held)
            .IsInvalidArgument());
    EXPECT_TRUE(held.lease.valid());
    EXPECT_EQ(held.lease.lane().jetty_id, held_id);
    EXPECT_EQ(held.jfc_credit_token, held_token);
    EXPECT_EQ(harness.pool->stats().total_leases, 1u);
    EXPECT_EQ(harness.pool->stats().leaked_leases, 0u);

    // A token-only output (lease already returned, credit unsettled) is also
    // rejected: the credit must be released before leasing again.
    ASSERT_TRUE(harness.pool->returnOnce(std::move(held.lease), true).ok());
    EXPECT_FALSE(held.lease.valid());
    EXPECT_NE(held.jfc_credit_token, 0u);
    EXPECT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 3, 1), held)
            .IsInvalidArgument());

    // Once fully settled, the same struct leases again.
    harness.credits->release(held.jfc_index, held.jfc_credit_token);
    held.jfc_credit_token = 0;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 4, 1), held).ok());
    EXPECT_TRUE(held.lease.valid());
}

TEST(UbSendJettyPoolTest, ZeroPoolKeyAndZeroOptionsRejected) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(1, 1, 8, 0, 1, 16).ok());
    LeaseWithCredit grant;

    // Zero context incarnation is the unassigned sentinel.
    const PoolKey zero_key{};
    EXPECT_FALSE(zero_key.valid());
    EXPECT_TRUE(
        harness.pool->tryLease(zero_key, makeAttemptKey(1, 1, 1), grant)
            .IsInvalidArgument());
    // Zero task/slice identities are rejected like the zero generation.
    EXPECT_TRUE(
        harness.pool->tryLease(makePoolKey(), AttemptKey{0, 1, 1}, grant)
            .IsInvalidArgument());
    EXPECT_TRUE(
        harness.pool->tryLease(makePoolKey(), AttemptKey{1, 0, 1}, grant)
            .IsInvalidArgument());
    EXPECT_FALSE(grant.lease.valid());
    EXPECT_EQ(harness.adapter->created(), 0);

    // Zero desired_serving / max_creating fail closed at lease time.
    SendJettyPoolOptions bad_options;
    bad_options.desired_serving = 0;
    bad_options.max_creating = 1;
    auto zero_desired = std::make_shared<SendJettyPool>(
        harness.adapter, harness.context, harness.jfcs, harness.budget,
        "dev0", harness.credits, bad_options);
    EXPECT_TRUE(
        zero_desired->tryLease(makePoolKey(), makeAttemptKey(1, 1, 1), grant)
            .IsInvalidArgument());
    bad_options.desired_serving = 1;
    bad_options.max_creating = 0;
    auto zero_creating = std::make_shared<SendJettyPool>(
        harness.adapter, harness.context, harness.jfcs, harness.budget,
        "dev0", harness.credits, bad_options);
    EXPECT_TRUE(
        zero_creating->tryLease(makePoolKey(), makeAttemptKey(1, 1, 1), grant)
            .IsInvalidArgument());
    EXPECT_EQ(harness.adapter->created(), 0);
}

TEST(UbSendJettyPoolTest, DrainIdleDeleteFailureKeepsBudgetForQuarantineDrain) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();

    // Hold both leases at once: sequential lease/return would reuse the
    // same member through the LIFO stack, leaving only one idle member.
    LeaseWithCredit first;
    LeaseWithCredit second;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), first).ok());
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), second).ok());
    ASSERT_TRUE(harness.pool->returnOnce(std::move(first.lease), true).ok());
    harness.credits->release(first.jfc_index, first.jfc_credit_token);
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(second.lease), true).ok());
    harness.credits->release(second.jfc_index, second.jfc_credit_token);
    ASSERT_EQ(harness.pool->stats().idle_members, 2u);

    // One idle deletion fails: its handle and budget stay owned by the pool.
    harness.adapter->failNextDeletes(1);
    ASSERT_TRUE(harness.pool->drainIdle().ok());
    EXPECT_EQ(harness.adapter->deleted(), 1);
    EXPECT_EQ(harness.pool->stats().quarantined_members, 1u);
    auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.quarantined_jetties, 1u);
    EXPECT_EQ(snapshot.total_jetties, 1u);

    // The quarantine drain retries and releases the budget after proof.
    ASSERT_TRUE(harness.pool->drainQuarantined().ok());
    EXPECT_EQ(harness.adapter->deleted(), 2);
    EXPECT_EQ(harness.pool->stats().quarantined_members, 0u);
    snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.total_jetties, 0u);
}

TEST(UbSendJettyPoolTest, DestructorRecordsLeakWithoutNativeRetry) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(1, 1, 8, 0, 1, 16).ok());
    {
        LeaseWithCredit grant;
        ASSERT_TRUE(harness.pool
                        ->tryLease(makePoolKey(), makeAttemptKey(1, 1, 1),
                                   grant)
                        .ok());
        // Dropped lease: the member is quarantined with its budget charged.
    }
    ASSERT_EQ(harness.budget->snapshot("dev0").total_jetties, 1u);
    const int deleted_before = harness.adapter->deleted();

    harness.pool.reset();
    // The destructor never retries native deletion of Unknown handles.
    EXPECT_EQ(harness.adapter->deleted(), deleted_before);
    const auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_GE(snapshot.leaked_reservations, 1u);
    EXPECT_EQ(snapshot.total_jetties, 1u);
}

TEST(UbSendJettyPoolTest, DestructorRecordsIdleReservationsAsLeaked) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(2, 2, 8, 0, 1, 16).ok());
    const auto key = makePoolKey();
    // Hold both leases at once so LIFO reuse cannot collapse them into a
    // single member: two distinct members must be idle at destruction.
    LeaseWithCredit first;
    LeaseWithCredit second;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), first).ok());
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), second).ok());
    ASSERT_TRUE(harness.pool->returnOnce(std::move(first.lease), true).ok());
    harness.credits->release(first.jfc_index, first.jfc_credit_token);
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(second.lease), true).ok());
    harness.credits->release(second.jfc_index, second.jfc_credit_token);
    ASSERT_EQ(harness.pool->stats().idle_members, 2u);

    // Idle members never drained: destruction records them as bookkeeping
    // leaks instead of silently deleting or silently losing the budget.
    harness.pool.reset();
    EXPECT_EQ(harness.adapter->deleted(), 0);
    const auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_GE(snapshot.leaked_reservations, 2u);
    EXPECT_EQ(snapshot.total_jetties, 2u);
}

TEST(UbAdmissionTest, TaskPermitZeroKIsFailClosed) {
    TaskPermitLedger permits(0);
    EXPECT_EQ(permits.maxInflightAttempts(), 0u);
    for (int round = 0; round < 4; ++round) {
        EXPECT_FALSE(permits.tryAcquire().valid());
    }
    EXPECT_EQ(permits.rejectedAcquisitions(), 4u);
    EXPECT_EQ(permits.outstanding(), 0u);
}

TEST(UbAttemptGateTest, ZeroIdentifiersNeverCreateRecords) {
    AttemptGate gate;
    const LaneKey lane{7, 1};
    // Zero attempt identity or lane generation: no record, token 0.
    EXPECT_EQ(gate.registerAttempt(AttemptKey{0, 1, 1}, lane, 64), 0u);
    EXPECT_EQ(gate.registerAttempt(AttemptKey{1, 0, 1}, lane, 64), 0u);
    EXPECT_EQ(gate.registerAttempt(AttemptKey{1, 1, 0}, lane, 64), 0u);
    EXPECT_EQ(
        gate.registerAttempt(makeAttemptKey(1, 1, 1), LaneKey{7, 0}, 64), 0u);
    EXPECT_EQ(gate.openAttempts(), 0u);

    // Token 0 is the fail-closed sentinel: events are no-ops and settlement
    // keeps waiting instead of fabricating a verdict.
    AttemptGate::Settlement settlement;
    EXPECT_FALSE(gate.tryFinalize(0, true, settlement));
    EXPECT_TRUE(settlement.wait);
    gate.onPostReturn(0, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(0, true, 64);
    EXPECT_FALSE(gate.tryFinalize(0, true, settlement));
    EXPECT_TRUE(settlement.wait);
    EXPECT_EQ(gate.openAttempts(), 0u);

    // Valid registration still works afterwards.
    const uint64_t token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), lane, 64);
    EXPECT_NE(token, 0u);
    EXPECT_EQ(gate.openAttempts(), 1u);
    gate.discard(token);
    EXPECT_EQ(gate.openAttempts(), 0u);
}

// ---------------------------------------------------------------------------
// Background executors (P2/P3): refill stop behaviour, asynchronous delete
// accounting and failure quarantine.
// ---------------------------------------------------------------------------

namespace {

// Polls `predicate` until true or the deadline expires; returns the last
// predicate result. Keeps executor tests deterministic without fixed sleeps.
template <typename Predicate>
bool waitUntil(Predicate predicate, int timeout_ms = 5000) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::yield();
    }
    return true;
}

}  // namespace

TEST(UbSendJettyPoolBackgroundTest, RefillTopsUpAndFreezesAfterStop) {
    PoolHarness harness;
    SendJettyPoolOptions options;
    options.desired_serving = 3;
    options.max_creating = 2;
    options.background_refill = true;
    options.refill_interval_ms = 2;
    ASSERT_TRUE(harness.initWithOptions(16, 2, 1, 64, options).ok());
    const auto key = makePoolKey();

    // One business attempt registers the PoolKey; refill owns the rest.
    LeaseWithCredit seed;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), seed).ok());
    ASSERT_TRUE(harness.pool->returnOnce(std::move(seed.lease), true).ok());
    harness.credits->release(seed.jfc_index, seed.jfc_credit_token);

    // Refill tops the key up to the static Serving size without any attempt.
    EXPECT_TRUE(
        waitUntil([&] { return harness.pool->servingCount(key) == 3; }));
    EXPECT_EQ(harness.adapter->created(), 3);

    // After beginStop joins the executor, creation is frozen: no member may
    // appear afterwards, even across several refill intervals.
    harness.pool->beginStop();
    const int created_at_stop = harness.adapter->created();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(harness.adapter->created(), created_at_stop);
    EXPECT_EQ(harness.pool->stats().creating_members, 0u);

    ASSERT_TRUE(harness.pool->drainIdle().ok());
    EXPECT_EQ(harness.pool->servingCount(key), 0u);
    EXPECT_EQ(harness.adapter->deleted(), harness.adapter->created());
    EXPECT_EQ(harness.budget->snapshot("dev0").total_jetties, 0u);
}

TEST(UbSendJettyPoolBackgroundTest, DeleteExecutorRetiresOffReturnPath) {
    PoolHarness harness;
    SendJettyPoolOptions options;
    options.desired_serving = 2;
    options.max_creating = 2;
    options.background_delete = true;
    ASSERT_TRUE(harness.initWithOptions(8, 2, 1, 16, options).ok());
    const auto key = makePoolKey();

    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
    ASSERT_TRUE(
        harness.pool->retireFaulted(std::move(grant.lease), "storm").ok());
    harness.credits->release(grant.jfc_index, grant.jfc_credit_token);

    // The delete executor settles the destruction asynchronously; the
    // deleting counter returns to zero once the member is destroyed.
    EXPECT_TRUE(waitUntil([&] { return harness.adapter->deleted() == 1; }));
    EXPECT_TRUE(waitUntil([&] {
        const auto stats = harness.pool->stats();
        return stats.deleting_members == 0 && stats.retired_members == 0;
    }));
    EXPECT_EQ(harness.pool->stats().quarantined_members, 0u);
    EXPECT_EQ(harness.budget->snapshot("dev0").total_jetties, 0u);

    // The pool keeps serving: a fresh lease creates a replacement member.
    LeaseWithCredit next;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), next).ok());
    ASSERT_TRUE(harness.pool->returnOnce(std::move(next.lease), true).ok());
    harness.credits->release(next.jfc_index, next.jfc_credit_token);
    ASSERT_TRUE(harness.pool->drainIdle().ok());
    EXPECT_EQ(harness.adapter->deleted(), harness.adapter->created());
    EXPECT_EQ(harness.budget->snapshot("dev0").total_jetties, 0u);
}

TEST(UbSendJettyPoolBackgroundTest, DeleteExecutorFailureQuarantinesForRetry) {
    PoolHarness harness;
    SendJettyPoolOptions options;
    options.desired_serving = 2;
    options.max_creating = 2;
    options.background_delete = true;
    ASSERT_TRUE(harness.initWithOptions(8, 2, 1, 16, options).ok());
    const auto key = makePoolKey();

    LeaseWithCredit grant;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
    harness.adapter->failNextDeletes(1);
    ASSERT_TRUE(
        harness.pool->retireFaulted(std::move(grant.lease), "storm").ok());
    harness.credits->release(grant.jfc_index, grant.jfc_credit_token);

    // Unknown destruction on the executor keeps owner and budget charged.
    EXPECT_TRUE(waitUntil(
        [&] { return harness.pool->stats().quarantined_members == 1; }));
    EXPECT_EQ(harness.adapter->deleted(), 0);
    auto snapshot = harness.budget->snapshot("dev0");
    EXPECT_EQ(snapshot.quarantined_jetties, 1u);
    EXPECT_EQ(snapshot.total_jetties, 1u);

    // A later drain retries the native delete and releases the budget.
    ASSERT_TRUE(harness.pool->drainQuarantined().ok());
    EXPECT_EQ(harness.adapter->deleted(), 1);
    EXPECT_EQ(harness.budget->snapshot("dev0").total_jetties, 0u);

    harness.pool->beginStop();
    EXPECT_EQ(harness.pool->stats().retired_members, 0u);
    EXPECT_EQ(harness.pool->stats().deleting_members, 0u);
    EXPECT_EQ(harness.pool->stats().quarantined_members, 0u);
}

// ---------------------------------------------------------------------------
// Concurrency stress with both background executors enabled.
// ---------------------------------------------------------------------------

TEST(UbSendJettyPoolConcurrencyTest, BackgroundExecutorsUnderMixedStorm) {
    PoolHarness harness;
    SendJettyPoolOptions options;
    options.desired_serving = 4;
    options.max_creating = 2;
    options.background_refill = true;
    options.background_delete = true;
    options.refill_interval_ms = 2;
    ASSERT_TRUE(harness.initWithOptions(16, 2, 2, 64, options).ok());
    const auto key = makePoolKey();

    LeaseWithCredit seed;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), seed).ok());
    ASSERT_TRUE(harness.pool->returnOnce(std::move(seed.lease), true).ok());
    harness.credits->release(seed.jfc_index, seed.jfc_credit_token);

    constexpr int kThreads = 6;
    constexpr int kRounds = 400;
    std::atomic<uint64_t> acquired{0};
    std::atomic<uint64_t> deferred{0};
    std::atomic<bool> violation{false};
    std::atomic<bool> stop_requested{false};
    std::mutex lane_mutex;
    std::unordered_set<uint32_t> live_lanes;

    std::vector<std::thread> threads;
    threads.reserve(kThreads + 1);
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            for (int round = 0; round < kRounds; ++round) {
                LeaseWithCredit grant;
                const auto attempt = makeAttemptKey(
                    static_cast<uint64_t>(thread + 1),
                    static_cast<uint64_t>(round + 1), 1);
                const Status status =
                    harness.pool->tryLease(key, attempt, grant);
                if (status.IsTooManyRequests()) {
                    ++deferred;
                    continue;
                }
                if (!status.ok()) {
                    violation.store(true);
                    stop_requested.store(true);
                    return;
                }
                ++acquired;
                {
                    std::lock_guard<std::mutex> lock(lane_mutex);
                    if (!live_lanes
                             .insert(grant.lease.lane().jetty_id)
                             .second) {
                        violation.store(true);
                        stop_requested.store(true);
                        return;
                    }
                }
                harness.credits->release(grant.jfc_index,
                                         grant.jfc_credit_token);
                {
                    std::lock_guard<std::mutex> lock(lane_mutex);
                    live_lanes.erase(grant.lease.lane().jetty_id);
                }
                // Mixed settlement: fault retirement, gate-failed return and
                // normal reuse race with both background executors.
                Status settled;
                if (round % 5 == 0) {
                    settled = harness.pool->retireFaulted(
                        std::move(grant.lease), "storm fault");
                } else {
                    settled = harness.pool->returnOnce(
                        std::move(grant.lease), round % 5 != 1);
                }
                if (!settled.ok()) {
                    violation.store(true);
                    stop_requested.store(true);
                    return;
                }
                if (round == kRounds / 2 && thread == 0) {
                    stop_requested.store(true);
                }
            }
            if (thread == 0) stop_requested.store(true);
        });
    }
    threads.emplace_back([&] {
        while (!stop_requested.load()) std::this_thread::yield();
        harness.pool->beginStop();
    });
    for (auto& worker : threads) worker.join();

    EXPECT_FALSE(violation.load());
    EXPECT_GT(acquired.load(), 0u);
    EXPECT_TRUE(harness.pool->stopping());
    const auto stats = harness.pool->stats();
    EXPECT_EQ(stats.leaked_leases, 0u);
    EXPECT_EQ(stats.double_returns, 0u);
    EXPECT_EQ(stats.in_use_members, 0u);
    // The seed lease/return pair is counted on top of the storm.
    EXPECT_EQ(stats.total_leases, acquired.load() + 1);
    EXPECT_EQ(stats.total_returns, acquired.load() + 1);

    ASSERT_TRUE(harness.pool->drainIdle().ok());
    ASSERT_TRUE(harness.pool->drainQuarantined().ok());
    EXPECT_EQ(harness.adapter->deleted(), harness.adapter->created());
    EXPECT_EQ(harness.pool->stats().retired_members, 0u);
    EXPECT_EQ(harness.pool->stats().deleting_members, 0u);
    EXPECT_EQ(harness.pool->stats().quarantined_members, 0u);
    EXPECT_EQ(harness.budget->snapshot("dev0").total_jetties, 0u);
}

// ---------------------------------------------------------------------------
// T-06 shared target owner / registry (C03/C04): single-flight Context
// import dedup, reference/revocation separation, watermark-on-success, and
// the receive-side LocalPortTarget quota owner.
// ---------------------------------------------------------------------------

namespace {

constexpr const char* kRegistryTestEid =
    "0001:0002:0003:0004:0005:0006:0007:0008";

class RegistryFakeRemoteTarget final : public RemoteTarget {
   public:
    RegistryFakeRemoteTarget(std::string eid, uint32_t uasid, uint32_t id)
        : eid_(std::move(eid)), uasid_(uasid), id_(id) {}

    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return uasid_; }
    const std::string& eid() const noexcept override { return eid_; }

   private:
    std::string eid_;
    uint32_t uasid_;
    uint32_t id_;
    bool valid_{true};
};

class RegistryFakeImportBroker final : public UbTargetImportBroker {
   public:
    std::atomic<uint64_t> import_calls{0};
    std::atomic<uint64_t> unimport_calls{0};
    std::atomic<bool> fail_next_import{false};
    // Optional injection point executed before the import decision; used to
    // block the creator so waiters observe the in-flight window.
    std::function<Status()> hook;

    Status importTarget(const UbImportedTargetKey& key,
                        const std::string& /*target_descriptor*/,
                        RemoteTargetPtr& out) override {
        ++import_calls;
        if (hook) {
            const Status hook_status = hook();
            if (!hook_status.ok()) return hook_status;
        }
        if (fail_next_import.exchange(false)) {
            return Status::InternalError("injected import failure");
        }
        out = std::make_shared<RegistryFakeRemoteTarget>(key.eid, key.uasid,
                                                         key.id);
        return Status::OK();
    }

    Status unimportTarget(const RemoteTargetPtr& target) override {
        if (!target) return Status::InvalidArgument("null target");
        ++unimport_calls;
        return Status::OK();
    }
};

class RegistryFakeLocalTargetFactory final : public UbLocalPortTargetFactory {
   public:
    std::atomic<uint64_t> create_calls{0};
    std::atomic<uint64_t> destroy_calls{0};
    std::atomic<bool> fail_next_create{false};
    std::atomic<bool> unproven_destroy{false};
    std::function<Status()> hook;

    Status createTarget(const UbLocalPortTargetDesc& /*desc*/,
                        JettyPtr& out) override {
        ++create_calls;
        if (hook) {
            const Status hook_status = hook();
            if (!hook_status.ok()) return hook_status;
        }
        if (fail_next_create.exchange(false)) {
            return Status::InternalError("injected create failure");
        }
        out = std::make_shared<PooledFakeJetty>(next_id_++);
        return Status::OK();
    }

    Status destroyTarget(const JettyPtr& target,
                         bool& destruction_proven) override {
        if (!target) return Status::InvalidArgument("null target");
        ++destroy_calls;
        destruction_proven = !unproven_destroy.load();
        return Status::OK();
    }

   private:
    std::atomic<uint32_t> next_id_{100};
};

UbImportedTargetKey makeImportedTargetKey(uint64_t epoch,
                                          uint32_t native_id = 10,
                                          const std::string& port_id = "P0") {
    UbImportedTargetKey key;
    key.context.device_identity = "/dev/udma0#0001";
    key.context.eid = kRegistryTestEid;
    key.context.context_incarnation = 7;
    key.peer.node_incarnation = 42;
    key.peer.port_id = port_id;
    key.peer.target_epoch = epoch;
    key.peer.profile.trans_mode = kUrmaTransportModeRm;
    key.peer.profile.target_type = kUrmaTargetTypeJetty;
    key.peer.profile.tp_type = kUrmaTpTypeCtp;
    key.eid = kRegistryTestEid;
    key.uasid = 0;
    key.id = native_id;
    return key;
}

UbLocalPortTargetDesc makeLocalPortTargetDesc(const std::string& port_id,
                                              uint64_t epoch) {
    UbLocalPortTargetDesc desc;
    desc.port_id = port_id;
    desc.target_epoch = epoch;
    desc.target_identity = "T" + port_id.substr(1);
    desc.profile.trans_mode = kUrmaTransportModeRm;
    desc.profile.target_type = kUrmaTargetTypeJetty;
    desc.profile.tp_type = kUrmaTpTypeCtp;
    return desc;
}

// Installs a fresh process-authoritative node target quota (tests share one
// process, so the frozen ledger must be reset between scenarios).
std::shared_ptr<NodeTargetQuota> installTestQuota(uint32_t limit) {
    NodeTargetQuota::resetRegistryForTest();
    std::shared_ptr<NodeTargetQuota> quota;
    EXPECT_TRUE(NodeTargetQuota::acquire(limit, quota).ok());
    return quota;
}

}  // namespace

TEST(UbImportedTargetRegistryTest, SingleFlightDefersWaitersAndImportsOnce) {
    auto broker = std::make_shared<RegistryFakeImportBroker>();
    std::promise<void> release_creator;
    std::future<void> creator_gate = release_creator.get_future();
    broker->hook = [&creator_gate] {
        creator_gate.wait();
        return Status::OK();
    };

    std::shared_ptr<UbImportedTargetRegistry> registry;
    ASSERT_TRUE(UbImportedTargetRegistry::create(broker, registry).ok());
    const auto key = makeImportedTargetKey(5);

    UbTargetAcquireDisposition disposition =
        UbTargetAcquireDisposition::kDeferred;
    UbImportedTargetRef creator_ref;
    std::thread creator([&] {
        ASSERT_TRUE(registry
                        ->acquire(key, "00aabbcc",
                                  UbTargetReferenceRole::kEndpoint,
                                  creator_ref, disposition)
                        .ok());
    });
    while (broker->import_calls.load() < 1) {
        std::this_thread::yield();
    }

    // While the only creator is inside the broker, a concurrent waiter for
    // the same key defers non-blockingly instead of importing again.
    UbImportedTargetRef waiter_ref;
    UbTargetAcquireDisposition waiter_disposition =
        UbTargetAcquireDisposition::kReady;
    ASSERT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kProbe,
                              waiter_ref, waiter_disposition)
                    .ok());
    EXPECT_EQ(waiter_disposition, UbTargetAcquireDisposition::kDeferred);
    EXPECT_FALSE(waiter_ref.valid());

    release_creator.set_value();
    creator.join();
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    ASSERT_TRUE(creator_ref.valid());

    // The waiter's retry finds the published import and shares it.
    ASSERT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kProbe,
                              waiter_ref, waiter_disposition)
                    .ok());
    EXPECT_EQ(waiter_disposition, UbTargetAcquireDisposition::kReady);
    ASSERT_TRUE(waiter_ref.valid());
    EXPECT_EQ(waiter_ref.target()->id(), creator_ref.target()->id());
    EXPECT_EQ(broker->import_calls.load(), 1u);

    const auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].references, 2u);
    EXPECT_EQ(snapshot.entries[0].endpoint_references, 1u);
    EXPECT_EQ(snapshot.entries[0].probe_references, 1u);
    ASSERT_EQ(snapshot.watermarks.size(), 1u);
    EXPECT_EQ(snapshot.watermarks[0].imported_target_epoch, 5u);
    EXPECT_EQ(snapshot.successful_imports, 1u);
}

TEST(UbImportedTargetRegistryTest,
     EndpointBusinessAndProbeShareOneImportAndReleaseKeepsOthers) {
    auto broker = std::make_shared<RegistryFakeImportBroker>();
    std::shared_ptr<UbImportedTargetRegistry> registry;
    ASSERT_TRUE(UbImportedTargetRegistry::create(broker, registry).ok());
    const auto key = makeImportedTargetKey(5);

    UbImportedTargetRef endpoint_ref, business_ref, probe_ref;
    for (auto [role, ref] :
         {std::pair{UbTargetReferenceRole::kEndpoint, &endpoint_ref},
          std::pair{UbTargetReferenceRole::kBusiness, &business_ref},
          std::pair{UbTargetReferenceRole::kProbe, &probe_ref}}) {
        UbTargetAcquireDisposition disposition =
            UbTargetAcquireDisposition::kDeferred;
        ASSERT_TRUE(
            registry->acquire(key, "00aabbcc", role, *ref, disposition).ok());
        EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    }
    EXPECT_EQ(broker->import_calls.load(), 1u);

    // Eliminating one endpoint only releases its reference: the shared
    // import and every other reference stay alive.
    endpoint_ref = UbImportedTargetRef();
    EXPECT_TRUE(business_ref.valid());
    EXPECT_TRUE(probe_ref.valid());
    EXPECT_EQ(broker->unimport_calls.load(), 0u);
    auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].references, 2u);

    // Dropping every reference keeps the shared import cached (dedup across
    // future endpoints); it is not unimported while not retired.
    business_ref = UbImportedTargetRef();
    probe_ref = UbImportedTargetRef();
    EXPECT_EQ(broker->unimport_calls.load(), 0u);
    snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].references, 0u);
    EXPECT_EQ(snapshot.entries[0].retired, false);
}

TEST(UbImportedTargetRegistryTest,
     NewerEpochRetiresOldImportOnlyAfterLastReference) {
    auto broker = std::make_shared<RegistryFakeImportBroker>();
    std::shared_ptr<UbImportedTargetRegistry> registry;
    ASSERT_TRUE(UbImportedTargetRegistry::create(broker, registry).ok());
    const auto old_key = makeImportedTargetKey(5);
    const auto new_key = makeImportedTargetKey(6);

    UbImportedTargetRef old_business, old_probe, new_ref;
    UbTargetAcquireDisposition disposition;
    ASSERT_TRUE(registry
                    ->acquire(old_key, "00aabbcc",
                              UbTargetReferenceRole::kBusiness, old_business,
                              disposition)
                    .ok());
    ASSERT_TRUE(registry
                    ->acquire(old_key, "00aabbcc",
                              UbTargetReferenceRole::kProbe, old_probe,
                              disposition)
                    .ok());

    // A newer epoch is a new owner; the old import keeps serving its
    // outstanding references.
    ASSERT_TRUE(registry
                    ->acquire(new_key, "00aabbcc",
                              UbTargetReferenceRole::kEndpoint, new_ref,
                              disposition)
                    .ok());
    EXPECT_EQ(broker->import_calls.load(), 2u);
    EXPECT_TRUE(old_business.valid());
    EXPECT_TRUE(old_probe.valid());
    EXPECT_EQ(broker->unimport_calls.load(), 0u);

    old_business = UbImportedTargetRef();
    EXPECT_EQ(broker->unimport_calls.load(), 0u);
    EXPECT_TRUE(old_probe.valid());

    old_probe = UbImportedTargetRef();
    // The retired old generation is unimported only at zero references.
    EXPECT_EQ(broker->unimport_calls.load(), 1u);
    EXPECT_TRUE(new_ref.valid());

    const auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].key.peer.target_epoch, 6u);
    EXPECT_EQ(snapshot.entries[0].references, 1u);
    ASSERT_EQ(snapshot.watermarks.size(), 1u);
    EXPECT_EQ(snapshot.watermarks[0].imported_target_epoch, 6u);
    EXPECT_EQ(snapshot.successful_unimports, 1u);
}

TEST(UbImportedTargetRegistryTest, ImportFailureDoesNotAdvanceWatermark) {
    auto broker = std::make_shared<RegistryFakeImportBroker>();
    std::shared_ptr<UbImportedTargetRegistry> registry;
    ASSERT_TRUE(UbImportedTargetRegistry::create(broker, registry).ok());
    const auto key = makeImportedTargetKey(5);
    const auto newer_key = makeImportedTargetKey(6);

    UbImportedTargetRef incumbent;
    UbTargetAcquireDisposition disposition;
    ASSERT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              incumbent, disposition)
                    .ok());

    broker->fail_next_import = true;
    UbImportedTargetRef failed;
    EXPECT_FALSE(registry
                     ->acquire(newer_key, "00aabbcc",
                               UbTargetReferenceRole::kEndpoint, failed,
                               disposition)
                     .ok());
    EXPECT_FALSE(failed.valid());
    auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.watermarks.size(), 1u);
    EXPECT_EQ(snapshot.watermarks[0].imported_target_epoch, 5u);
    EXPECT_EQ(snapshot.failed_imports, 1u);
    EXPECT_EQ(incumbent.valid(), true);

    // The watermark did not move, so the failed epoch can be retried.
    UbImportedTargetRef retried;
    ASSERT_TRUE(registry
                    ->acquire(newer_key, "00aabbcc",
                              UbTargetReferenceRole::kEndpoint, retried,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    snapshot = registry->snapshot();
    EXPECT_EQ(snapshot.watermarks[0].imported_target_epoch, 6u);
}

TEST(UbImportedTargetRegistryTest, StaleAndConflictingCandidatesKeepIncumbent) {
    auto broker = std::make_shared<RegistryFakeImportBroker>();
    std::shared_ptr<UbImportedTargetRegistry> registry;
    ASSERT_TRUE(UbImportedTargetRegistry::create(broker, registry).ok());

    UbImportedTargetRef incumbent;
    UbTargetAcquireDisposition disposition;
    const auto key = makeImportedTargetKey(5);
    ASSERT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              incumbent, disposition)
                    .ok());

    // An older epoch never overwrites the published scope.
    UbImportedTargetRef stale;
    ASSERT_TRUE(registry
                    ->acquire(makeImportedTargetKey(4), "00aabbcc",
                              UbTargetReferenceRole::kEndpoint, stale,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kStale);
    EXPECT_FALSE(stale.valid());

    // Same generation with different descriptor content conflicts; the
    // healthy incumbent is kept.
    UbImportedTargetRef conflict;
    ASSERT_TRUE(registry
                    ->acquire(key, "00ccbbaa", UbTargetReferenceRole::kEndpoint,
                              conflict, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kConflict);
    EXPECT_FALSE(conflict.valid());

    // A different native identity at the same published epoch is not
    // accepted either.
    UbImportedTargetRef other_native;
    ASSERT_TRUE(registry
                    ->acquire(makeImportedTargetKey(5, /*native_id=*/11),
                              "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              other_native, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kStale);
    EXPECT_FALSE(other_native.valid());

    EXPECT_EQ(broker->import_calls.load(), 1u);
    EXPECT_TRUE(incumbent.valid());
    const auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].references, 1u);
    EXPECT_EQ(snapshot.entries[0].retired, false);
}

TEST(UbImportedTargetRegistryTest,
     RetireImportDeniesNewReferencesAndReclaimsAtZero) {
    auto broker = std::make_shared<RegistryFakeImportBroker>();
    std::shared_ptr<UbImportedTargetRegistry> registry;
    ASSERT_TRUE(UbImportedTargetRegistry::create(broker, registry).ok());
    const auto key = makeImportedTargetKey(5);

    UbImportedTargetRef first, second;
    UbTargetAcquireDisposition disposition;
    ASSERT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              first, disposition)
                    .ok());
    ASSERT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kBusiness,
                              second, disposition)
                    .ok());

    ASSERT_TRUE(registry->retireImport(key).ok());
    // Revoking twice while the entry is alive is idempotent.
    ASSERT_TRUE(registry->retireImport(key).ok());

    // New references on the revoked epoch are denied...
    UbImportedTargetRef denied;
    ASSERT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kProbe,
                              denied, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kStale);
    EXPECT_FALSE(denied.valid());

    // ...but outstanding references keep the native import alive.
    EXPECT_TRUE(first.valid());
    EXPECT_TRUE(second.valid());
    EXPECT_EQ(broker->unimport_calls.load(), 0u);

    first = UbImportedTargetRef();
    EXPECT_EQ(broker->unimport_calls.load(), 0u);
    second = UbImportedTargetRef();
    EXPECT_EQ(broker->unimport_calls.load(), 1u);
    EXPECT_EQ(registry->snapshot().entries.size(), 0u);
    EXPECT_EQ(registry->snapshot().successful_unimports, 1u);
}

TEST(UbImportedTargetRegistryTest, RejectsInvalidKeysAndDescriptors) {
    auto broker = std::make_shared<RegistryFakeImportBroker>();
    std::shared_ptr<UbImportedTargetRegistry> registry;
    ASSERT_TRUE(UbImportedTargetRegistry::create(broker, registry).ok());

    UbImportedTargetRef out;
    UbTargetAcquireDisposition disposition;

    auto key = makeImportedTargetKey(5);
    key.peer.target_epoch = 0;
    EXPECT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              out, disposition)
                    .IsInvalidArgument());

    key = makeImportedTargetKey(5);
    key.peer.port_id.clear();
    EXPECT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              out, disposition)
                    .IsInvalidArgument());

    key = makeImportedTargetKey(5);
    key.id = 0;
    EXPECT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              out, disposition)
                    .IsInvalidArgument());

    key = makeImportedTargetKey(5);
    key.context.context_incarnation = 0;
    EXPECT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              out, disposition)
                    .IsInvalidArgument());

    key = makeImportedTargetKey(5);
    key.peer.profile.tp_type = 2;  // not URMA_CTP in the first version
    EXPECT_TRUE(registry
                    ->acquire(key, "00aabbcc", UbTargetReferenceRole::kEndpoint,
                              out, disposition)
                    .IsInvalidArgument());

    key = makeImportedTargetKey(5);
    EXPECT_TRUE(registry
                    ->acquire(key, "", UbTargetReferenceRole::kEndpoint, out,
                              disposition)
                    .IsInvalidArgument());

    EXPECT_EQ(broker->import_calls.load(), 0u);
    EXPECT_EQ(registry->snapshot().entries.size(), 0u);
}

TEST(UbImportedTargetRegistryTest, ConcurrentAcquireStormImportsOnce) {
    auto broker = std::make_shared<RegistryFakeImportBroker>();
    std::promise<void> release_creator;
    std::future<void> creator_gate = release_creator.get_future();
    broker->hook = [&creator_gate] {
        creator_gate.wait();
        return Status::OK();
    };

    std::shared_ptr<UbImportedTargetRegistry> registry;
    ASSERT_TRUE(UbImportedTargetRegistry::create(broker, registry).ok());
    const auto key = makeImportedTargetKey(5);

    constexpr int kThreads = 8;
    std::atomic<int> finished{0};
    // References outlive the worker threads so the shared import keeps all
    // of them pinned for the final snapshot check.
    std::vector<UbImportedTargetRef> refs(kThreads);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int index = 0; index < kThreads; ++index) {
        threads.emplace_back([&, index] {
            UbImportedTargetRef& slot = refs[index];
            UbTargetAcquireDisposition disposition =
                UbTargetAcquireDisposition::kDeferred;
            const auto role = static_cast<UbTargetReferenceRole>(index % 3);
            ASSERT_TRUE(
                registry->acquire(key, "00aabbcc", role, slot, disposition).ok());
            if (disposition == UbTargetAcquireDisposition::kDeferred) {
                // Deferred waiters retry non-blockingly until the single
                // creator publishes.
                while (true) {
                    ASSERT_TRUE(registry
                                    ->acquire(key, "00aabbcc", role, slot,
                                              disposition)
                                    .ok());
                    if (disposition == UbTargetAcquireDisposition::kReady) {
                        break;
                    }
                    ASSERT_EQ(disposition,
                              UbTargetAcquireDisposition::kDeferred);
                    std::this_thread::yield();
                }
            } else {
                ASSERT_EQ(disposition, UbTargetAcquireDisposition::kReady);
            }
            ++finished;
        });
    }

    bool released = false;
    while (finished.load() < kThreads) {
        // The first thread blocks inside the broker; the rest defer and
        // spin above. Exactly one import call may exist at any time.
        ASSERT_LE(broker->import_calls.load(), 1u);
        if (!released && broker->import_calls.load() == 1) {
            release_creator.set_value();
            released = true;
        }
        std::this_thread::yield();
    }
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(broker->import_calls.load(), 1u);
    const auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].references,
              static_cast<uint64_t>(kThreads));
}

TEST(UbLocalPortTargetRegistryTest, CreatesOneTargetPerPortWithQuotaTickets) {
    const auto quota = installTestQuota(8);
    auto factory = std::make_shared<RegistryFakeLocalTargetFactory>();
    std::shared_ptr<UbLocalPortTargetRegistry> registry;
    ASSERT_TRUE(UbLocalPortTargetRegistry::create(quota, factory, registry)
                    .ok());

    UbLocalPortTargetRef refs[4];
    UbTargetAcquireDisposition disposition;
    for (int index = 0; index < 4; ++index) {
        ASSERT_TRUE(registry
                        ->acquire(makeLocalPortTargetDesc("P" +
                                                              std::to_string(index),
                                                          4),
                                  UbTargetReferenceRole::kEndpoint, refs[index],
                                  disposition)
                        .ok());
        EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    }
    EXPECT_EQ(factory->create_calls.load(), 4u);

    // Another peer referencing an already published port shares the same
    // target: no new creation, no extra quota ticket.
    UbLocalPortTargetRef shared;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 4),
                              UbTargetReferenceRole::kBusiness, shared,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    EXPECT_EQ(factory->create_calls.load(), 4u);
    EXPECT_EQ(shared.jetty()->id(), refs[0].jetty()->id());

    const auto quota_snapshot = quota->snapshot();
    EXPECT_EQ(quota_snapshot.active, 4u);
    EXPECT_EQ(quota_snapshot.total_occupied, 4u);

    const auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 4u);
    EXPECT_EQ(snapshot.entries[0].port_id, "P0");
    EXPECT_EQ(snapshot.entries[0].references, 2u);
    EXPECT_EQ(snapshot.port_watermarks.at("P0"), 4u);
}

TEST(UbLocalPortTargetRegistryTest,
     ExistingTargetHandshakeIgnoresQuotaExhaustion) {
    // A single-slot quota is already fully occupied by the published target.
    const auto quota = installTestQuota(1);
    auto factory = std::make_shared<RegistryFakeLocalTargetFactory>();
    std::shared_ptr<UbLocalPortTargetRegistry> registry;
    ASSERT_TRUE(UbLocalPortTargetRegistry::create(quota, factory, registry)
                    .ok());

    UbLocalPortTargetRef first;
    UbTargetAcquireDisposition disposition;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 3),
                              UbTargetReferenceRole::kEndpoint, first,
                              disposition)
                    .ok());
    ASSERT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    EXPECT_EQ(quota->snapshot().total_occupied, 1u);

    // The shared handshake of an existing target is granted even though the
    // node quota has no free slot: references never consume tickets.
    UbLocalPortTargetRef second, third;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 3),
                              UbTargetReferenceRole::kBusiness, second,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 3),
                              UbTargetReferenceRole::kProbe, third, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    EXPECT_EQ(factory->create_calls.load(), 1u);
    EXPECT_EQ(quota->snapshot().total_occupied, 1u);
}

TEST(UbLocalPortTargetRegistryTest, CapacityDefersWithoutRetiringIncumbent) {
    const auto quota = installTestQuota(1);
    auto factory = std::make_shared<RegistryFakeLocalTargetFactory>();
    std::shared_ptr<UbLocalPortTargetRegistry> registry;
    ASSERT_TRUE(UbLocalPortTargetRegistry::create(quota, factory, registry)
                    .ok());

    UbLocalPortTargetRef incumbent;
    UbTargetAcquireDisposition disposition;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 3),
                              UbTargetReferenceRole::kEndpoint, incumbent,
                              disposition)
                    .ok());

    // A new port cannot be created: the quota is full. The caller defers
    // with a capacity classification and nothing is retired.
    UbLocalPortTargetRef other_port;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P1", 3),
                              UbTargetReferenceRole::kEndpoint, other_port,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kCapacity);
    EXPECT_FALSE(other_port.valid());

    // An epoch replacement also needs a new ticket (new owner); with a full
    // quota it defers and the healthy incumbent stays published.
    UbLocalPortTargetRef replacement;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 4),
                              UbTargetReferenceRole::kEndpoint, replacement,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kCapacity);
    EXPECT_TRUE(incumbent.valid());
    EXPECT_EQ(factory->destroy_calls.load(), 0u);

    const auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].port_id, "P0");
    EXPECT_EQ(snapshot.entries[0].retired, false);
    const auto quota_snapshot = quota->snapshot();
    EXPECT_EQ(quota_snapshot.active, 1u);
    EXPECT_GE(quota_snapshot.rejected_reservations, 2u);
}

TEST(UbLocalPortTargetRegistryTest,
     NewEpochTakesNewTicketAndRetiresOldAfterReferencesDrop) {
    const auto quota = installTestQuota(8);
    auto factory = std::make_shared<RegistryFakeLocalTargetFactory>();
    std::shared_ptr<UbLocalPortTargetRegistry> registry;
    ASSERT_TRUE(UbLocalPortTargetRegistry::create(quota, factory, registry)
                    .ok());

    UbLocalPortTargetRef old_business, old_probe, new_ref;
    UbTargetAcquireDisposition disposition;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 5),
                              UbTargetReferenceRole::kBusiness, old_business,
                              disposition)
                    .ok());
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 5),
                              UbTargetReferenceRole::kProbe, old_probe,
                              disposition)
                    .ok());

    // The replacement reserves a second ticket while the old generation
    // still serves references.
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 6),
                              UbTargetReferenceRole::kEndpoint, new_ref,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    EXPECT_EQ(factory->create_calls.load(), 2u);
    EXPECT_EQ(factory->destroy_calls.load(), 0u);
    EXPECT_EQ(quota->snapshot().total_occupied, 2u);

    old_business = UbLocalPortTargetRef();
    EXPECT_EQ(factory->destroy_calls.load(), 0u);
    EXPECT_TRUE(old_probe.valid());

    // Destruction runs after the last reference drops and is proven before
    // the quota slot is released.
    old_probe = UbLocalPortTargetRef();
    EXPECT_EQ(factory->destroy_calls.load(), 1u);
    EXPECT_TRUE(new_ref.valid());
    EXPECT_EQ(quota->snapshot().active, 1u);
    EXPECT_EQ(quota->snapshot().total_occupied, 1u);

    const auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].target_epoch, 6u);
    EXPECT_EQ(snapshot.port_watermarks.at("P0"), 6u);
    EXPECT_EQ(snapshot.proven_destructions, 1u);
}

TEST(UbLocalPortTargetRegistryTest,
     CreationFailureReleasesTicketAndKeepsWatermark) {
    const auto quota = installTestQuota(8);
    auto factory = std::make_shared<RegistryFakeLocalTargetFactory>();
    std::shared_ptr<UbLocalPortTargetRegistry> registry;
    ASSERT_TRUE(UbLocalPortTargetRegistry::create(quota, factory, registry)
                    .ok());

    factory->fail_next_create = true;
    UbLocalPortTargetRef failed;
    UbTargetAcquireDisposition disposition;
    EXPECT_FALSE(registry
                     ->acquire(makeLocalPortTargetDesc("P0", 5),
                               UbTargetReferenceRole::kEndpoint, failed,
                               disposition)
                     .ok());
    EXPECT_FALSE(failed.valid());
    // The proven-never-allocated rollback released the ticket.
    EXPECT_EQ(quota->snapshot().total_occupied, 0u);
    EXPECT_EQ(registry->snapshot().failed_creations, 1u);
    EXPECT_EQ(registry->snapshot().port_watermarks.count("P0"), 0u);

    // The failure did not advance the watermark, so the same epoch can be
    // retried.
    UbLocalPortTargetRef retried;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 5),
                              UbTargetReferenceRole::kEndpoint, retried,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kReady);
    EXPECT_EQ(registry->snapshot().port_watermarks.at("P0"), 5u);
}

TEST(UbLocalPortTargetRegistryTest, UnprovenDestructionQuarantinesQuota) {
    const auto quota = installTestQuota(8);
    auto factory = std::make_shared<RegistryFakeLocalTargetFactory>();
    factory->unproven_destroy = true;
    std::shared_ptr<UbLocalPortTargetRegistry> registry;
    ASSERT_TRUE(UbLocalPortTargetRegistry::create(quota, factory, registry)
                    .ok());

    UbLocalPortTargetRef old_ref, new_ref;
    UbTargetAcquireDisposition disposition;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 5),
                              UbTargetReferenceRole::kEndpoint, old_ref,
                              disposition)
                    .ok());
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 6),
                              UbTargetReferenceRole::kEndpoint, new_ref,
                              disposition)
                    .ok());

    old_ref = UbLocalPortTargetRef();
    EXPECT_EQ(factory->destroy_calls.load(), 1u);
    // An unproven destruction keeps the slot charged (quarantine) instead
    // of releasing quota that may still hold native resources.
    const auto quota_snapshot = quota->snapshot();
    EXPECT_EQ(quota_snapshot.quarantined_unproven, 1u);
    EXPECT_EQ(quota_snapshot.active, 1u);
    EXPECT_EQ(quota_snapshot.total_occupied, 2u);
    EXPECT_EQ(registry->snapshot().quarantined_destructions, 1u);
}

TEST(UbLocalPortTargetRegistryTest, ConcurrentPeersReferenceOneTarget) {
    const auto quota = installTestQuota(8);
    auto factory = std::make_shared<RegistryFakeLocalTargetFactory>();
    std::promise<void> release_creator;
    std::future<void> creator_gate = release_creator.get_future();
    factory->hook = [&creator_gate] {
        creator_gate.wait();
        return Status::OK();
    };
    std::shared_ptr<UbLocalPortTargetRegistry> registry;
    ASSERT_TRUE(UbLocalPortTargetRegistry::create(quota, factory, registry)
                    .ok());

    UbTargetAcquireDisposition disposition;
    UbLocalPortTargetRef creator_ref;
    std::thread creator([&] {
        ASSERT_TRUE(registry
                        ->acquire(makeLocalPortTargetDesc("P0", 5),
                                  UbTargetReferenceRole::kEndpoint, creator_ref,
                                  disposition)
                        .ok());
    });
    while (factory->create_calls.load() < 1) {
        std::this_thread::yield();
    }

    // Multiple peers converging on the same port during creation defer to
    // the single creator instead of creating a second target.
    UbLocalPortTargetRef peer_ref;
    UbTargetAcquireDisposition peer_disposition =
        UbTargetAcquireDisposition::kReady;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 5),
                              UbTargetReferenceRole::kProbe, peer_ref,
                              peer_disposition)
                    .ok());
    EXPECT_EQ(peer_disposition, UbTargetAcquireDisposition::kDeferred);
    EXPECT_FALSE(peer_ref.valid());

    release_creator.set_value();
    creator.join();
    ASSERT_TRUE(creator_ref.valid());

    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 5),
                              UbTargetReferenceRole::kProbe, peer_ref,
                              peer_disposition)
                    .ok());
    EXPECT_EQ(peer_disposition, UbTargetAcquireDisposition::kReady);
    EXPECT_EQ(peer_ref.jetty()->id(), creator_ref.jetty()->id());
    EXPECT_EQ(factory->create_calls.load(), 1u);

    const auto snapshot = registry->snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].references, 2u);
    EXPECT_EQ(quota->snapshot().active, 1u);
}

TEST(UbLocalPortTargetRegistryTest, RetireAndConflictClassification) {
    const auto quota = installTestQuota(8);
    auto factory = std::make_shared<RegistryFakeLocalTargetFactory>();
    std::shared_ptr<UbLocalPortTargetRegistry> registry;
    ASSERT_TRUE(UbLocalPortTargetRegistry::create(quota, factory, registry)
                    .ok());

    UbLocalPortTargetRef first, second;
    UbTargetAcquireDisposition disposition;
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 5),
                              UbTargetReferenceRole::kEndpoint, first,
                              disposition)
                    .ok());

    // Same epoch with different content conflicts; the incumbent is kept.
    auto conflicting = makeLocalPortTargetDesc("P0", 5);
    conflicting.target_identity = "OTHER";
    ASSERT_TRUE(registry
                    ->acquire(conflicting, UbTargetReferenceRole::kEndpoint,
                              second, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kConflict);
    EXPECT_FALSE(second.valid());
    EXPECT_EQ(factory->create_calls.load(), 1u);

    // An older epoch is stale.
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 4),
                              UbTargetReferenceRole::kEndpoint, second,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kStale);
    EXPECT_FALSE(second.valid());

    // Retiring a live generation pauses new references but keeps serving
    // the outstanding one; reclaim happens at zero references.
    ASSERT_TRUE(registry->retirePortTarget("P0", 5).ok());
    ASSERT_TRUE(registry
                    ->acquire(makeLocalPortTargetDesc("P0", 5),
                              UbTargetReferenceRole::kBusiness, second,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbTargetAcquireDisposition::kStale);
    EXPECT_TRUE(first.valid());
    EXPECT_EQ(factory->destroy_calls.load(), 0u);
    first = UbLocalPortTargetRef();
    EXPECT_EQ(factory->destroy_calls.load(), 1u);
    EXPECT_EQ(quota->snapshot().total_occupied, 0u);

    // Unknown generations cannot be retired.
    EXPECT_TRUE(registry->retirePortTarget("P0", 5).IsInvalidArgument());
}

TEST(UbSendJettyPoolConcurrencyTest, MultiplePoolKeysStormWithExecutors) {
    PoolHarness harness;
    SendJettyPoolOptions options;
    options.desired_serving = 2;
    options.max_creating = 2;
    options.background_refill = true;
    options.background_delete = true;
    options.refill_interval_ms = 2;
    ASSERT_TRUE(harness.initWithOptions(16, 2, 2, 64, options).ok());

    const PoolKey keys[3] = {makePoolKey(1), makePoolKey(2), makePoolKey(3)};
    // Seed every key so refill maintains all three buckets concurrently.
    for (int index = 0; index < 3; ++index) {
        LeaseWithCredit seed;
        ASSERT_TRUE(harness.pool
                        ->tryLease(keys[index],
                                   makeAttemptKey(100 + index, 1, 1), seed)
                        .ok());
        ASSERT_TRUE(
            harness.pool->returnOnce(std::move(seed.lease), true).ok());
        harness.credits->release(seed.jfc_index, seed.jfc_credit_token);
    }

    constexpr int kThreads = 6;
    constexpr int kRounds = 300;
    std::atomic<uint64_t> acquired{0};
    std::atomic<bool> violation{false};
    std::mutex lane_mutex;
    std::unordered_set<uint32_t> live_lanes;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            const PoolKey& key = keys[thread % 3];
            for (int round = 0; round < kRounds; ++round) {
                LeaseWithCredit grant;
                const Status status = harness.pool->tryLease(
                    key,
                    makeAttemptKey(static_cast<uint64_t>(thread + 1),
                                   static_cast<uint64_t>(round + 1), 1),
                    grant);
                if (status.IsTooManyRequests()) continue;
                if (!status.ok()) {
                    violation.store(true);
                    return;
                }
                ++acquired;
                {
                    std::lock_guard<std::mutex> lock(lane_mutex);
                    if (!live_lanes
                             .insert(grant.lease.lane().jetty_id)
                             .second) {
                        violation.store(true);
                        return;
                    }
                }
                harness.credits->release(grant.jfc_index,
                                         grant.jfc_credit_token);
                {
                    std::lock_guard<std::mutex> lock(lane_mutex);
                    live_lanes.erase(grant.lease.lane().jetty_id);
                }
                const Status settled =
                    (round % 7 == 0)
                        ? harness.pool->retireFaulted(
                              std::move(grant.lease), "key storm")
                        : harness.pool->returnOnce(std::move(grant.lease),
                                                   true);
                if (!settled.ok()) {
                    violation.store(true);
                    return;
                }
            }
        });
    }
    for (auto& worker : threads) worker.join();

    EXPECT_FALSE(violation.load());
    EXPECT_GT(acquired.load(), 0u);

    harness.pool->beginStop();
    ASSERT_TRUE(harness.pool->drainIdle().ok());
    ASSERT_TRUE(harness.pool->drainQuarantined().ok());
    const auto stats = harness.pool->stats();
    EXPECT_EQ(stats.leaked_leases, 0u);
    EXPECT_EQ(stats.double_returns, 0u);
    EXPECT_EQ(stats.in_use_members, 0u);
    // Three seed lease/return pairs are counted on top of the storm.
    EXPECT_EQ(stats.total_leases, acquired.load() + 3);
    EXPECT_EQ(stats.total_returns, acquired.load() + 3);
    EXPECT_EQ(stats.created_members,
              static_cast<uint64_t>(harness.adapter->created()));
    EXPECT_EQ(harness.adapter->deleted(), harness.adapter->created());
    for (const auto& key : keys) {
        EXPECT_EQ(harness.pool->servingCount(key), 0u);
    }
    EXPECT_EQ(harness.budget->snapshot("dev0").total_jetties, 0u);
}

// ---------------------------------------------------------------------------
// T-07 path registry (C04): immutable PathVersion snapshots, target
// aggregated load, epoch protection and asynchronous cold-path readiness on
// the control executor (post workers defer without holding any lease).
// ---------------------------------------------------------------------------

namespace {

UbPathVersion makePathVersion(uint64_t pool_incarnation = 1,
                              const std::string& port_id = "P0",
                              uint64_t target_epoch = 5,
                              uint64_t topology_epoch = 2,
                              uint64_t peer_incarnation = 42) {
    UbPathVersion version;
    version.key.pool.context_incarnation = pool_incarnation;
    version.key.pool.trans_mode = kUrmaTransportModeRm;
    version.key.pool.priority = 3;
    version.key.pool.bind_tp_type_mask = 0x3;
    version.key.peer_incarnation = peer_incarnation;
    version.key.remote_port_id = port_id;
    version.key.remote_profile.trans_mode = kUrmaTransportModeRm;
    version.key.remote_profile.target_type = kUrmaTargetTypeJetty;
    version.key.remote_profile.tp_type = kUrmaTpTypeCtp;
    version.topology_epoch = topology_epoch;
    version.target_epoch = target_epoch;
    return version;
}

// Target key coherent with one path version (same Context incarnation, peer,
// port, profile and epoch). `native_id` varies the content fingerprint.
UbImportedTargetKey makePathTargetKey(const UbPathVersion& version,
                                      uint32_t native_id = 10) {
    UbImportedTargetKey key;
    key.context.device_identity = "/dev/udma0#0001";
    key.context.eid = kRegistryTestEid;
    key.context.context_incarnation = version.key.pool.context_incarnation;
    key.peer.node_incarnation = version.key.peer_incarnation;
    key.peer.port_id = version.key.remote_port_id;
    key.peer.target_epoch = version.target_epoch;
    key.peer.profile = version.key.remote_profile;
    key.eid = kRegistryTestEid;
    key.uasid = 0;
    key.id = native_id;
    return key;
}

class FakeColdPathPreparer final : public UbColdPathPreparer {
   public:
    std::atomic<uint64_t> prepare_calls{0};
    std::atomic<bool> fail_next{false};
    std::function<Status()> hook;

    Status prepare(const UbPathVersion& /*path*/,
                   const UbImportedTargetKey& /*target_key*/,
                   const std::string& /*target_descriptor*/) override {
        ++prepare_calls;
        if (hook) {
            const Status hook_status = hook();
            if (!hook_status.ok()) return hook_status;
        }
        if (fail_next.exchange(false)) {
            return Status::InternalError("injected cold prepare failure");
        }
        return Status::OK();
    }
};

// Deterministic control executor: work is captured and only runs when the
// test pumps it, so cold preparation is observed exactly at the control
// linearization points.
class ManualPathExecutor final : public UbPathControlExecutor {
   public:
    explicit ManualPathExecutor(size_t capacity) : capacity_(capacity) {}

    bool enqueue(std::function<void()> work) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= capacity_) return false;
        queue_.push_back(std::move(work));
        return true;
    }

    void runOne() {
        std::function<void()> work;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (queue_.empty()) return;
            work = std::move(queue_.front());
            queue_.pop_front();
        }
        work();
    }

    size_t queued() {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

   private:
    const size_t capacity_;
    mutable std::mutex mutex_;
    std::deque<std::function<void()>> queue_;
};

struct PathRegistryHarness {
    std::shared_ptr<FakeColdPathPreparer> preparer =
        std::make_shared<FakeColdPathPreparer>();
    std::shared_ptr<ManualPathExecutor> executor =
        std::make_shared<ManualPathExecutor>(16);
    std::shared_ptr<UbPathRegistry> registry;

    PathRegistryHarness() {
        const Status status =
            UbPathRegistry::create(preparer, executor, 16, registry);
        assert(status.ok());
    }

    Status publishReady(const UbPathVersion& version, size_t topology_rank,
                        uint32_t native_id = 10,
                        UbPathEligibility eligibility =
                            UbPathEligibility::kHealthy) {
        UbPathPublishDisposition disposition;
        return registry->publish(version, topology_rank, eligibility,
                                 makePathTargetKey(version, native_id),
                                 "desc-" + version.key.remote_port_id, true,
                                 disposition);
    }
};

}  // namespace

TEST(UbPathRegistryTest, ValidationRejectsInvalidPublishRequests) {
    PathRegistryHarness harness;
    UbPathPublishDisposition disposition;

    // Null plumbing and zero bounds are rejected at create time.
    std::shared_ptr<UbPathRegistry> bad;
    EXPECT_FALSE(UbPathRegistry::create(nullptr, harness.executor, 4, bad)
                     .ok());
    EXPECT_FALSE(UbPathRegistry::create(harness.preparer, nullptr, 4, bad)
                     .ok());
    EXPECT_FALSE(
        UbPathRegistry::create(harness.preparer, harness.executor, 0, bad)
            .ok());

    // Key / epoch validation.
    EXPECT_TRUE(harness.registry
                    ->publish(makePathVersion(0), 1,
                              UbPathEligibility::kHealthy,
                              makePathTargetKey(makePathVersion(0)), "d", true,
                              disposition)
                    .IsInvalidArgument());
    auto zero_topology = makePathVersion();
    zero_topology.topology_epoch = 0;
    EXPECT_TRUE(harness.registry
                    ->publish(zero_topology, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(zero_topology), "d", true,
                              disposition)
                    .IsInvalidArgument());
    auto rtp_mode = makePathVersion();
    rtp_mode.key.pool.trans_mode = kUrmaTransportModeRc;
    EXPECT_TRUE(harness.registry
                    ->publish(rtp_mode, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(rtp_mode), "d", true,
                              disposition)
                    .IsInvalidArgument());
    EXPECT_TRUE(harness.registry
                    ->publish(makePathVersion(), 1, UbPathEligibility::kFaulted,
                              makePathTargetKey(makePathVersion()), "d", true,
                              disposition)
                    .IsInvalidArgument());
    EXPECT_TRUE(harness.registry
                    ->publish(makePathVersion(), 1,
                              UbPathEligibility::kHealthy,
                              makePathTargetKey(makePathVersion()), "", true,
                              disposition)
                    .IsInvalidArgument());

    // Path version / target key coherence.
    EXPECT_TRUE(harness.registry
                    ->publish(makePathVersion(), 1,
                              UbPathEligibility::kHealthy,
                              makePathTargetKey(makePathVersion(), 10), "d",
                              true, disposition)
                    .ok());
    auto epoch_mismatch = makePathVersion();
    UbImportedTargetKey mismatched_key =
        makePathTargetKey(epoch_mismatch);
    mismatched_key.peer.target_epoch = 6;
    EXPECT_TRUE(harness.registry
                    ->publish(epoch_mismatch, 1, UbPathEligibility::kHealthy,
                              mismatched_key, "d", true, disposition)
                    .IsInvalidArgument());
    UbImportedTargetKey port_mismatch = makePathTargetKey(epoch_mismatch);
    port_mismatch.peer.port_id = "P9";
    EXPECT_TRUE(harness.registry
                    ->publish(epoch_mismatch, 1, UbPathEligibility::kHealthy,
                              port_mismatch, "d", true, disposition)
                    .IsInvalidArgument());
}

TEST(UbPathRegistryTest, AsymmetricPathCountsPerPeerAndContext) {
    PathRegistryHarness harness;
    // Context 1 reaches two remote ports; context 2 reaches five: the
    // registry never assumes symmetric adjacency.
    ASSERT_TRUE(harness.publishReady(makePathVersion(1, "P0"), 1).ok());
    ASSERT_TRUE(harness.publishReady(makePathVersion(1, "P1"), 2).ok());
    for (int index = 0; index < 5; ++index) {
        ASSERT_TRUE(harness
                        .publishReady(makePathVersion(
                            2, "Q" + std::to_string(index), 5, 2, 42),
                            static_cast<size_t>(index))
                        .ok());
    }

    const auto peer42 = harness.registry->snapshotEligible(42);
    ASSERT_EQ(peer42.size(), 7u);
    for (const auto& snapshot : peer42) {
        EXPECT_EQ(snapshot.version.key.peer_incarnation, 42u);
    }
    // Another peer's paths are invisible here.
    ASSERT_TRUE(
        harness.publishReady(makePathVersion(1, "P0", 5, 2, 7), 1).ok());
    EXPECT_EQ(harness.registry->snapshotEligible(42).size(), 7u);
    ASSERT_EQ(harness.registry->snapshotEligible(7).size(), 1u);

    // Faulting one path removes exactly that entry.
    uint64_t fault_epoch = 0;
    ASSERT_TRUE(
        harness.registry
            ->markFaulted(makePathVersion(1, "P0", 5, 2, 7), fault_epoch)
            .ok());
    EXPECT_EQ(harness.registry->snapshotEligible(7).size(), 0u);
    EXPECT_EQ(harness.registry->snapshotEligible(42).size(), 7u);
}

TEST(UbPathRegistryTest, OlderEpochsNeverOverwriteTheIncumbent) {
    PathRegistryHarness harness;
    UbPathPublishDisposition disposition;

    ASSERT_TRUE(harness.publishReady(makePathVersion(1, "P0", 5, 1), 1).ok());

    // Older target epoch: stale, incumbent kept.
    ASSERT_TRUE(harness.registry
                    ->publish(makePathVersion(1, "P0", 3, 1), 1,
                              UbPathEligibility::kHealthy,
                              makePathTargetKey(makePathVersion(1, "P0", 3, 1)),
                              "d-old", true, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbPathPublishDisposition::kStale);

    // Same generation, same content: idempotent refresh.
    ASSERT_TRUE(harness.registry
                    ->publish(makePathVersion(1, "P0", 5, 1), 9,
                              UbPathEligibility::kEligibleUnverified,
                              makePathTargetKey(makePathVersion(1, "P0", 5, 1)),
                              "desc-P0", true, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbPathPublishDisposition::kPublished);

    // Newer topology epoch supersedes even with an older target epoch.
    ASSERT_TRUE(harness.registry
                    ->publish(makePathVersion(1, "P0", 1, 2), 1,
                              UbPathEligibility::kHealthy,
                              makePathTargetKey(makePathVersion(1, "P0", 1, 2)),
                              "d", true, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbPathPublishDisposition::kPublished);
    auto snapshots = harness.registry->snapshotEligible(42);
    ASSERT_EQ(snapshots.size(), 1u);
    EXPECT_EQ(snapshots[0].version.topology_epoch, 2u);
    EXPECT_EQ(snapshots[0].version.target_epoch, 1u);

    // After the topology bump, the previously newest (1,5) version is stale.
    ASSERT_TRUE(harness.registry
                    ->publish(makePathVersion(1, "P0", 5, 1), 1,
                              UbPathEligibility::kHealthy,
                              makePathTargetKey(makePathVersion(1, "P0", 5, 1)),
                              "d", true, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbPathPublishDisposition::kStale);
}

TEST(UbPathRegistryTest, SameGenerationDifferentContentConflicts) {
    PathRegistryHarness harness;
    UbPathPublishDisposition disposition;
    const auto version = makePathVersion(1, "P0", 5, 2);

    ASSERT_TRUE(harness.publishReady(version, 1, 10).ok());

    // Different native target identity at the same generation.
    ASSERT_TRUE(harness.registry
                    ->publish(version, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(version, 11), "d", true,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbPathPublishDisposition::kConflict);

    // Different descriptor at the same generation.
    ASSERT_TRUE(harness.registry
                    ->publish(version, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(version, 10), "d-other", true,
                              disposition)
                    .ok());
    EXPECT_EQ(disposition, UbPathPublishDisposition::kConflict);

    // The incumbent survives untouched.
    const auto stats = harness.registry->statsSnapshot();
    ASSERT_EQ(stats.entries.size(), 1u);
    EXPECT_EQ(stats.entries[0].version, version);
    EXPECT_FALSE(stats.entries[0].retired);
}

TEST(UbPathRegistryTest, ChargeAndAggregateLoadAcrossSharedTarget) {
    PathRegistryHarness harness;
    // Two pools (different priority) of one Context reference the same
    // imported target: their WR/bytes aggregate on the target cell.
    const auto low = makePathVersion(1, "P0", 5, 2);
    auto high = makePathVersion(1, "P0", 5, 2);
    high.key.pool.priority = 7;
    ASSERT_TRUE(harness.publishReady(low, 1).ok());
    ASSERT_TRUE(harness.publishReady(high, 1).ok());

    UbPathLoadCharge charge_low, charge_high;
    UbPathChargeDisposition disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(low, 1024, 2, charge_low, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbPathChargeDisposition::kCharged);
    ASSERT_TRUE(harness.registry
                    ->tryCharge(high, 512, 1, charge_high, disposition)
                    .ok());

    const auto snapshots = harness.registry->snapshotEligible(42);
    ASSERT_EQ(snapshots.size(), 2u);
    for (const auto& snapshot : snapshots) {
        // Path-local counters stay separate...
        if (snapshot.version.key.pool.priority == 3) {
            EXPECT_EQ(snapshot.path_inflight_wrs, 2u);
            EXPECT_EQ(snapshot.path_inflight_bytes, 1024u);
        } else {
            EXPECT_EQ(snapshot.path_inflight_wrs, 1u);
            EXPECT_EQ(snapshot.path_inflight_bytes, 512u);
        }
        // ...while the shared target aggregates both paths.
        EXPECT_EQ(snapshot.target_inflight_wrs, 3u);
        EXPECT_EQ(snapshot.target_inflight_bytes, 1536u);
    }

    // Endpoint view switch: republishing the same version never resets the
    // aggregated target load.
    UbPathPublishDisposition publish_disposition;
    ASSERT_TRUE(harness.registry
                    ->publish(low, 4, UbPathEligibility::kHealthy,
                              makePathTargetKey(low), "desc-P0", true,
                              publish_disposition)
                    .ok());
    EXPECT_EQ(publish_disposition, UbPathPublishDisposition::kPublished);
    const auto after_republish = harness.registry->snapshotEligible(42);
    ASSERT_EQ(after_republish.size(), 2u);
    EXPECT_EQ(after_republish[0].target_inflight_wrs, 3u);

    charge_low = UbPathLoadCharge();
    charge_high = UbPathLoadCharge();
    const auto drained = harness.registry->snapshotEligible(42);
    ASSERT_EQ(drained.size(), 2u);
    for (const auto& snapshot : drained) {
        EXPECT_EQ(snapshot.path_inflight_wrs, 0u);
        EXPECT_EQ(snapshot.target_inflight_wrs, 0u);
        EXPECT_EQ(snapshot.target_inflight_bytes, 0u);
    }
}

TEST(UbPathRegistryTest, TopologySupersedeKeepsTargetLoadContinuity) {
    PathRegistryHarness harness;
    // Same imported target (same target epoch), new topology generation:
    // the new record must reuse the aggregate cell, never reset it to zero.
    const auto old_version = makePathVersion(1, "P0", 5, 1);
    const auto new_version = makePathVersion(1, "P0", 5, 2);
    const auto target_key = makePathTargetKey(old_version);
    UbPathPublishDisposition disposition;
    ASSERT_TRUE(harness.registry
                    ->publish(old_version, 1, UbPathEligibility::kHealthy,
                              target_key, "d", true, disposition)
                    .ok());

    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(old_version, 4096, 1, charge, charge_disposition)
                    .ok());

    ASSERT_TRUE(harness.registry
                    ->publish(new_version, 1, UbPathEligibility::kHealthy,
                              target_key, "d", true, disposition)
                    .ok());
    EXPECT_EQ(disposition, UbPathPublishDisposition::kPublished);

    const auto snapshots = harness.registry->snapshotEligible(42);
    ASSERT_EQ(snapshots.size(), 1u);
    EXPECT_EQ(snapshots[0].version, new_version);
    EXPECT_EQ(snapshots[0].path_inflight_wrs, 0u);
    // The old generation's charge still shows on the shared target cell.
    EXPECT_EQ(snapshots[0].target_inflight_wrs, 1u);
    EXPECT_EQ(snapshots[0].target_inflight_bytes, 4096u);

    // The retired record drains cleanly through its charge.
    charge = UbPathLoadCharge();
    const auto drained = harness.registry->snapshotEligible(42);
    ASSERT_EQ(drained.size(), 1u);
    EXPECT_EQ(drained[0].target_inflight_wrs, 0u);
}

TEST(UbPathRegistryTest, SelectionOrderPreservesLayering) {
    PathRegistryHarness harness;
    // cold: not ready yet; far: topology rank 3; loaded: rank 1 with in-flight
    // bytes; idle: rank 1 without load.
    const auto cold = makePathVersion(1, "COLD", 5, 2);
    const auto far_ = makePathVersion(1, "FAR", 5, 2);
    const auto loaded = makePathVersion(1, "LOADED", 5, 2);
    const auto idle = makePathVersion(1, "IDLE", 5, 2);
    ASSERT_TRUE(harness.publishReady(far_, 3).ok());
    ASSERT_TRUE(harness.publishReady(loaded, 1).ok());
    ASSERT_TRUE(harness.publishReady(idle, 1).ok());
    UbPathPublishDisposition disposition;
    ASSERT_TRUE(harness.registry
                    ->publish(cold, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(cold), "d", false, disposition)
                    .ok());

    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(loaded, 65536, 1, charge, charge_disposition)
                    .ok());

    const auto snapshots = harness.registry->snapshotEligible(42);
    ASSERT_EQ(snapshots.size(), 4u);
    EXPECT_EQ(snapshots[0].version, idle);
    EXPECT_EQ(snapshots[1].version, loaded);
    EXPECT_EQ(snapshots[2].version, far_);
    EXPECT_EQ(snapshots[3].version, cold);
    EXPECT_FALSE(snapshots[3].target_ready);
}

TEST(UbPathRegistryTest, ColdPathDefersWithoutChargeAndCoalesces) {
    PathRegistryHarness harness;
    const auto cold = makePathVersion(1, "P0", 5, 2);
    UbPathPublishDisposition disposition;
    ASSERT_TRUE(harness.registry
                    ->publish(cold, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(cold), "d", false, disposition)
                    .ok());

    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 1024, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    EXPECT_FALSE(charge.valid());
    EXPECT_EQ(harness.executor->queued(), 1u);

    // The deferred candidate is coalesced: repeated attempts do not spawn a
    // second preparation.
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 1024, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    EXPECT_EQ(harness.executor->queued(), 1u);

    const auto stats = harness.registry->statsSnapshot();
    EXPECT_EQ(stats.total_charge_deferrals, 2u);
    EXPECT_EQ(stats.total_charges, 0u);
    EXPECT_EQ(stats.pending_cold_prepares, 1u);

    // Unknown keys are caller bugs; known keys with stale versions defer
    // without enqueuing anything new.
    EXPECT_TRUE(harness.registry
                    ->tryCharge(makePathVersion(9, "PX", 5, 2), 1, 1, charge,
                                charge_disposition)
                    .IsInvalidArgument());
    EXPECT_TRUE(harness.registry
                    ->tryCharge(makePathVersion(1, "P0", 5, 1), 1, 1, charge,
                                charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kNotEligible);
    EXPECT_EQ(harness.executor->queued(), 1u);

    // The control executor completes the preparation and marks readiness.
    harness.executor->runOne();
    EXPECT_EQ(harness.preparer->prepare_calls.load(), 1u);
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 1024, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kCharged);
    EXPECT_TRUE(charge.valid());
}

TEST(UbPathRegistryTest, NoLeaseHeldDuringColdPrepareRpc) {
    PathRegistryHarness harness;
    const auto cold = makePathVersion(1, "P0", 5, 2);
    UbPathPublishDisposition disposition;
    ASSERT_TRUE(harness.registry
                    ->publish(cold, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(cold), "d", false, disposition)
                    .ok());

    std::promise<void> release_rpc;
    std::future<void> rpc_gate = release_rpc.get_future();
    harness.preparer->hook = [&rpc_gate] {
        rpc_gate.wait();
        return Status::OK();
    };

    // The post worker defers immediately: the call returns before the RPC
    // even starts and holds no charge anywhere.
    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 1024, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    EXPECT_FALSE(charge.valid());
    EXPECT_EQ(harness.executor->queued(), 1u);

    std::thread control([&] { harness.executor->runOne(); });
    while (harness.preparer->prepare_calls.load() < 1) {
        std::this_thread::yield();
    }

    // While the control RPC is blocked inside the preparer, the registry
    // holds zero in-flight load and stays fully responsive.
    {
        const auto stats = harness.registry->statsSnapshot();
        EXPECT_EQ(stats.total_charges, 0u);
        for (const auto& entry : stats.entries) {
            EXPECT_EQ(entry.path_inflight_wrs, 0u);
            EXPECT_EQ(entry.target_inflight_wrs, 0u);
            EXPECT_EQ(entry.target_inflight_bytes, 0u);
        }
    }
    UbPathLoadCharge concurrent;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 64, 1, concurrent, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    EXPECT_FALSE(concurrent.valid());
    EXPECT_EQ(harness.registry->snapshotEligible(42).size(), 1u);

    release_rpc.set_value();
    control.join();
    EXPECT_EQ(harness.preparer->prepare_calls.load(), 1u);
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 1024, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kCharged);
}

TEST(UbPathRegistryTest, ColdPrepareFailureIsRetriedOnNextDefer) {
    PathRegistryHarness harness;
    const auto cold = makePathVersion(1, "P0", 5, 2);
    UbPathPublishDisposition disposition;
    ASSERT_TRUE(harness.registry
                    ->publish(cold, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(cold), "d", false, disposition)
                    .ok());

    harness.preparer->fail_next = true;
    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    harness.executor->runOne();
    EXPECT_EQ(harness.preparer->prepare_calls.load(), 1u);
    EXPECT_EQ(harness.registry->statsSnapshot().total_cold_prepare_failures,
              1u);
    EXPECT_EQ(harness.registry->statsSnapshot().pending_cold_prepares, 0u);

    // Not ready and not pending: the next defer re-enqueues the preparation.
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    EXPECT_EQ(harness.executor->queued(), 1u);
    harness.executor->runOne();
    ASSERT_TRUE(harness.registry
                    ->tryCharge(cold, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kCharged);
}

TEST(UbPathRegistryTest, BoundedPendingColdPreparesNeverBlock) {
    auto preparer = std::make_shared<FakeColdPathPreparer>();
    auto executor = std::make_shared<ManualPathExecutor>(16);
    std::shared_ptr<UbPathRegistry> registry;
    ASSERT_TRUE(UbPathRegistry::create(preparer, executor, 1, registry).ok());

    UbPathPublishDisposition disposition;
    const auto first = makePathVersion(1, "P0", 5, 2);
    const auto second = makePathVersion(1, "P1", 5, 2);
    ASSERT_TRUE(registry
                    ->publish(first, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(first), "d", false, disposition)
                    .ok());
    ASSERT_TRUE(registry
                    ->publish(second, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(second), "d", false, disposition)
                    .ok());

    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(registry->tryCharge(first, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    EXPECT_EQ(executor->queued(), 1u);

    // The bound is full: the second cold path still defers (never blocks,
    // never charges) but is not enqueued.
    ASSERT_TRUE(registry->tryCharge(second, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    EXPECT_FALSE(charge.valid());
    EXPECT_EQ(executor->queued(), 1u);
    EXPECT_EQ(registry->statsSnapshot().cold_prepares_dropped_full, 1u);

    // Completing the first preparation frees the pending slot.
    executor->runOne();
    ASSERT_TRUE(registry->tryCharge(second, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);
    EXPECT_EQ(executor->queued(), 1u);
    executor->runOne();
    ASSERT_TRUE(registry->tryCharge(second, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kCharged);
}

TEST(UbPathRegistryTest, TargetRebuildPausesReferencingPaths) {
    PathRegistryHarness harness;
    // Two pools reference the same target generation.
    const auto business = makePathVersion(1, "P0", 5, 2);
    auto probe_pool = makePathVersion(1, "P0", 5, 2);
    probe_pool.key.pool.priority = 7;
    ASSERT_TRUE(harness.publishReady(business, 1).ok());
    ASSERT_TRUE(harness.publishReady(probe_pool, 1).ok());

    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(business, 2048, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kCharged);

    // A rebuild pause for an older epoch matches nothing.
    UbImportScopeKey scope;
    scope.context = makePathTargetKey(business).context;
    scope.node_incarnation = business.key.peer_incarnation;
    scope.port_id = business.key.remote_port_id;
    scope.profile = business.key.remote_profile;
    uint64_t paused = 123;
    ASSERT_TRUE(
        harness.registry->pauseForTargetRebuild(scope, 4, paused).ok());
    EXPECT_EQ(paused, 0u);

    // Pausing the current generation stops both referencing paths.
    ASSERT_TRUE(
        harness.registry->pauseForTargetRebuild(scope, 5, paused).ok());
    EXPECT_EQ(paused, 2u);
    EXPECT_EQ(harness.registry->snapshotEligible(42).size(), 0u);
    UbPathLoadCharge denied;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(business, 1, 1, denied, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kNotEligible);

    // The outstanding charge on the paused generation still drains safely.
    charge = UbPathLoadCharge();
    const auto stats = harness.registry->statsSnapshot();
    ASSERT_EQ(stats.entries.size(), 2u);
    for (const auto& entry : stats.entries) {
        EXPECT_TRUE(entry.paused);
        EXPECT_EQ(entry.path_inflight_wrs, 0u);
        EXPECT_EQ(entry.target_inflight_wrs, 0u);
    }

    // The rebuilt generation (new target epoch) publishes fresh, eligible
    // versions; markReady is refused on the paused old version.
    EXPECT_TRUE(harness.registry->markReady(business).IsInvalidArgument());
    const auto rebuilt = makePathVersion(1, "P0", 6, 2);
    ASSERT_TRUE(harness.publishReady(rebuilt, 1).ok());
    const auto snapshots = harness.registry->snapshotEligible(42);
    ASSERT_EQ(snapshots.size(), 1u);
    EXPECT_EQ(snapshots[0].version, rebuilt);
    ASSERT_TRUE(harness.registry
                    ->tryCharge(rebuilt, 64, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kCharged);
}

TEST(UbPathRegistryTest, MarkFaultedIncrementsEpochAndExcludes) {
    PathRegistryHarness harness;
    const auto version = makePathVersion(1, "P0", 5, 2);
    ASSERT_TRUE(harness.publishReady(version, 1).ok());

    uint64_t fault_epoch = 0;
    ASSERT_TRUE(harness.registry->markFaulted(version, fault_epoch).ok());
    EXPECT_EQ(fault_epoch, 1u);
    EXPECT_EQ(harness.registry->snapshotEligible(42).size(), 0u);

    ASSERT_TRUE(harness.registry->markFaulted(version, fault_epoch).ok());
    EXPECT_EQ(fault_epoch, 2u);

    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(version, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kNotEligible);

    EXPECT_TRUE(
        harness.registry->markFaulted(makePathVersion(9, "PX"), fault_epoch)
            .IsInvalidArgument());
}

TEST(UbPathRegistryTest, ChargeMoveSemanticsAndSupersededDrain) {
    PathRegistryHarness harness;
    const auto version = makePathVersion(1, "P0", 5, 2);
    ASSERT_TRUE(harness.publishReady(version, 1).ok());

    UbPathLoadCharge first;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(harness.registry
                    ->tryCharge(version, 100, 1, first, charge_disposition)
                    .ok());
    EXPECT_TRUE(first.valid());
    const UbPathVersion& charged_version = first.version();
    EXPECT_EQ(charged_version, version);

    // Charging into an already-held output is a caller bug.
    UbPathLoadCharge second;
    EXPECT_TRUE(harness.registry
                    ->tryCharge(version, 1, 1, first, charge_disposition)
                    .IsInvalidArgument());
    // Zero-WR charges are rejected.
    EXPECT_TRUE(harness.registry
                    ->tryCharge(version, 1, 0, second, charge_disposition)
                    .IsInvalidArgument());

    UbPathLoadCharge moved = std::move(first);
    EXPECT_FALSE(first.valid());
    EXPECT_TRUE(moved.valid());
    EXPECT_EQ(moved.version(), version);

    // Supersede while the charge is outstanding: the new generation takes
    // over selection, and the retired record still drains safely.
    const auto next = makePathVersion(1, "P0", 6, 2);
    ASSERT_TRUE(harness.publishReady(next, 1).ok());
    moved = UbPathLoadCharge();

    const auto stats = harness.registry->statsSnapshot();
    bool saw_retired_drained = false;
    bool saw_incumbent = false;
    for (const auto& entry : stats.entries) {
        if (entry.version == version) {
            EXPECT_TRUE(entry.retired);
            EXPECT_EQ(entry.path_inflight_wrs, 0u);
            saw_retired_drained = true;
        }
        if (entry.version == next) {
            EXPECT_FALSE(entry.retired);
            saw_incumbent = true;
        }
    }
    EXPECT_TRUE(saw_retired_drained);
    EXPECT_TRUE(saw_incumbent);
}

TEST(UbPathRegistryTest, ConcurrentChargeChurnOnSharedTargetAggregates) {
    PathRegistryHarness harness;
    const auto path_a = makePathVersion(1, "P0", 5, 2);
    auto path_b = makePathVersion(1, "P1", 5, 2);
    path_b.key.pool.context_incarnation = 2;
    ASSERT_TRUE(harness.publishReady(path_a, 1).ok());
    ASSERT_TRUE(harness.publishReady(path_b, 1).ok());

    constexpr int kThreads = 4;
    constexpr int kRounds = 200;
    std::atomic<bool> violation{false};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            const UbPathVersion& version =
                (thread % 2 == 0) ? path_a : path_b;
            for (int round = 0; round < kRounds; ++round) {
                UbPathLoadCharge charge;
                UbPathChargeDisposition disposition;
                const Status status = harness.registry->tryCharge(
                    version, 64, 1, charge, disposition);
                if (!status.ok() ||
                    disposition != UbPathChargeDisposition::kCharged) {
                    violation.store(true);
                    return;
                }
                std::this_thread::yield();
            }
        });
    }
    for (auto& worker : threads) worker.join();

    EXPECT_FALSE(violation.load());
    // Every charge was released by scope exit: both path counters and the
    // aggregation return to exactly zero.
    const auto snapshots = harness.registry->snapshotEligible(42);
    ASSERT_EQ(snapshots.size(), 2u);
    for (const auto& snapshot : snapshots) {
        EXPECT_EQ(snapshot.path_inflight_wrs, 0u);
        EXPECT_EQ(snapshot.path_inflight_bytes, 0u);
        EXPECT_EQ(snapshot.target_inflight_wrs, 0u);
        EXPECT_EQ(snapshot.target_inflight_bytes, 0u);
    }
    EXPECT_EQ(harness.registry->statsSnapshot().total_charges,
              static_cast<uint64_t>(kThreads * kRounds));
}

TEST(UbPathRegistryTest, ThreadedControlExecutorDrainsColdPrepares) {
    auto preparer = std::make_shared<FakeColdPathPreparer>();
    std::shared_ptr<UbThreadedPathControlExecutor> executor;
    ASSERT_TRUE(UbThreadedPathControlExecutor::create(4, executor).ok());
    std::shared_ptr<UbPathRegistry> registry;
    ASSERT_TRUE(UbPathRegistry::create(preparer, executor, 4, registry).ok());

    UbPathPublishDisposition disposition;
    const auto cold = makePathVersion(1, "P0", 5, 2);
    ASSERT_TRUE(registry
                    ->publish(cold, 1, UbPathEligibility::kHealthy,
                              makePathTargetKey(cold), "d", false, disposition)
                    .ok());

    UbPathLoadCharge charge;
    UbPathChargeDisposition charge_disposition;
    ASSERT_TRUE(registry->tryCharge(cold, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kDeferred);

    // The dedicated control thread performs the preparation asynchronously.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (registry->statsSnapshot().pending_cold_prepares != 0) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline);
        std::this_thread::yield();
    }
    ASSERT_TRUE(registry->tryCharge(cold, 1, 1, charge, charge_disposition)
                    .ok());
    EXPECT_EQ(charge_disposition, UbPathChargeDisposition::kCharged);

    std::shared_ptr<UbThreadedPathControlExecutor> zero_capacity;
    EXPECT_FALSE(
        UbThreadedPathControlExecutor::create(0, zero_capacity).ok());
}
