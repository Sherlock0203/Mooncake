// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Software-only acceptance tests for the pooled rm_pool core components:
//   - NativeBudget (C05): device-level full-role budget ledger
//   - JfcCreditLedger (C05/C06): admission tickets
//   - SendJettyPool (C06): per-attempt exclusive LIFO pool with JFC credit
//     matching, stop interleaving, fault retirement and leak defence
//   - AttemptGate (C07/D04): completion gate with single settlement
//   - UbParams: pooled configuration keys (C01)
//
// Every test runs against a fake URMA adapter; no provider, NIC or kernel
// resource is required.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
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
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/business_jetty_pool.h"
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
    size_t nativeSegmentDescriptorSize() const noexcept override { return 16; }
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
        CHECK_STATUS(budget->configure("dev0", 0, budget_jetties, reserve));
        for (size_t index = 0; index < jfc_count; ++index) {
            CHECK_STATUS(credits->install(index, jfc_capacity));
            JfcPtr handle;
            CHECK_STATUS(adapter->createJfc(context, JfcOptions{}, handle));
            jfcs.push_back(
                std::make_shared<UbJfc>(index, adapter, std::move(handle)));
        }
        SendJettyPoolOptions options;
        options.desired_serving = desired;
        options.max_creating = max_creating;
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
    ASSERT_FALSE(budget.configure("", 0, 4, 0).ok());      // empty device key
    ASSERT_FALSE(budget.configure("dev0", 0, 0, 0).ok());  // zero budget
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
    EXPECT_FALSE(
        budget.tryReserve("dev0", BudgetRole::kTarget, one).has_value());

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
    EXPECT_FALSE(
        budget.tryReserve("dev0", BudgetRole::kBusiness, one).has_value());

    ASSERT_TRUE(budget.releaseAfterProvenDestruction(*old_object, true).ok());
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
        budget.transition(*reservation, BudgetObjectState::kQuarantined).ok());
    EXPECT_FALSE(
        budget.releaseAfterProvenDestruction(*reservation, false).ok());
    EXPECT_TRUE(budget.releaseAfterProvenDestruction(*reservation, true).ok());
    // Double release is rejected.
    EXPECT_FALSE(budget.releaseAfterProvenDestruction(*reservation, true).ok());
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
        budget.transition(*reservation, BudgetObjectState::kQuarantined).ok());
    // Quarantine is terminal except for proven release.
    EXPECT_FALSE(
        budget.transition(*reservation, BudgetObjectState::kActive).ok());
}

TEST(UbNativeBudgetTest, OverflowRejected) {
    NativeBudget budget;
    ASSERT_TRUE(budget.configure("dev0", 0, 4, 0).ok());
    BudgetQuantities huge;
    huge.jetties = UINT64_MAX;
    EXPECT_FALSE(
        budget.tryReserve("dev0", BudgetRole::kBusiness, huge).has_value());
    BudgetQuantities zero;
    EXPECT_FALSE(
        budget.tryReserve("dev0", BudgetRole::kBusiness, zero).has_value());
    EXPECT_FALSE(budget
                     .tryReserve("unknown-device", BudgetRole::kBusiness,
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
// JfcCreditLedger (T-10 admission tickets). Task-level K lives on
// UbTask and is covered by ub_admission_test.cpp (D-6/R-3).
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

// ---------------------------------------------------------------------------
// SendJettyPool (T-09 / C06)
// ---------------------------------------------------------------------------

TEST(UbSendJettyPoolTest, LeaseCreatesUpToDesiredServing) {
    PoolHarness harness;
    ASSERT_TRUE(harness
                    .init(/*desired=*/3, /*max_creating=*/2,
                          /*budget=*/8, /*reserve=*/0, /*jfcs=*/2,
                          /*jfc_capacity=*/16)
                    .ok());
    const auto key = makePoolKey();

    std::vector<LeaseWithCredit> leases;
    for (uint64_t generation = 1; generation <= 3; ++generation) {
        LeaseWithCredit grant;
        ASSERT_TRUE(harness.pool
                        ->tryLease(key,
                                   makeAttemptKey(1, generation, generation),
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
    const Status status =
        harness.pool->tryLease(key, makeAttemptKey(1, 99, 99), deferred);
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
    ASSERT_TRUE(harness.pool->returnOnce(std::move(matched.lease), true).ok());
    harness.credits->release(matched.jfc_index, matched.jfc_credit_token);
    auto blocker2 = harness.credits->tryReserve(second.jfc_index, 1);
    ASSERT_TRUE(blocker2.has_value());
    LeaseWithCredit deferred;
    EXPECT_TRUE(harness.pool->tryLease(key, makeAttemptKey(1, 4, 1), deferred)
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
        ASSERT_TRUE(harness.pool
                        ->tryLease(key,
                                   makeAttemptKey(1, generation, generation),
                                   grant)
                        .ok());
        leases.push_back(std::move(grant));
    }
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(leases[0].lease), true).ok());
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(leases[1].lease), true).ok());

    // Hold both members in use again, then exhaust budget behind them: the
    // pool can never grow past the device budget.
    LeaseWithCredit hold_first;
    LeaseWithCredit hold_second;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(2, 1, 1), hold_first).ok());
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(2, 2, 1), hold_second).ok());
    LeaseWithCredit deferred;
    EXPECT_TRUE(harness.pool->tryLease(key, makeAttemptKey(2, 3, 1), deferred)
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
    EXPECT_TRUE(harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant)
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
    EXPECT_TRUE(harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), rejected)
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
    EXPECT_TRUE(harness.pool->returnOnce(std::move(duplicate), true)
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
            harness.pool->tryLease(key, makeAttemptKey(1, 1, 1), grant).ok());
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
        ASSERT_TRUE(harness.pool
                        ->tryLease(key,
                                   makeAttemptKey(1, generation, generation),
                                   grant)
                        .ok());
        leases.push_back(std::move(grant));
    }
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(leases[0].lease), true).ok());
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(leases[1].lease), true).ok());
    // One member remains leased.
    ASSERT_TRUE(harness.pool->drainIdle().ok());
    EXPECT_EQ(harness.adapter->deleted(), 2);
    EXPECT_EQ(harness.pool->servingCount(key), 1u);
    ASSERT_TRUE(
        harness.pool->returnOnce(std::move(leases[2].lease), true).ok());
}

TEST(UbSendJettyPoolTest, InvalidArgumentsRejected) {
    PoolHarness harness;
    ASSERT_TRUE(harness.init(1, 1, 8, 0, 1, 16).ok());
    LeaseWithCredit grant;
    // Invalid attempt key (zero generation).
    EXPECT_TRUE(
        harness.pool->tryLease(makePoolKey(), AttemptKey{1, 1, 0}, grant)
            .IsInvalidArgument());
    // Returning an empty lease.
    EXPECT_TRUE(
        harness.pool->returnOnce(SliceJettyLease{}, true).IsInvalidArgument());
    EXPECT_TRUE(harness.pool->retireFaulted(SliceJettyLease{}, "x")
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
    EXPECT_TRUE(harness.pool->tryLease(key_b, makeAttemptKey(2, 2, 1), second_b)
                    .IsTooManyRequests());
}

// ---------------------------------------------------------------------------
// AttemptGate (T-11 / C07 / D04)
// ---------------------------------------------------------------------------

TEST(UbAttemptGateTest, NormalSuccessReturnsToPool) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 4096);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
    AttemptGate::Settlement settlement;
    EXPECT_FALSE(gate.tryFinalize(token, true, settlement));
    EXPECT_TRUE(settlement.wait);
}

TEST(UbAttemptGateTest, DuplicateCompletionIsContradiction) {
    AttemptGate gate;
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
        gate.registerAttempt(makeAttemptKey(1, 1, 1), PoolLaneKey{7, 1}, 64);
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
    ASSERT_TRUE(gate.tryFinalize(token, !harness.pool->stopping(), settlement));
    ASSERT_TRUE(settlement.return_to_pool);
    ASSERT_TRUE(harness.pool->returnOnce(std::move(grant.lease), true).ok());
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
    const auto token =
        gate.registerAttempt(makeAttemptKey(1, 1, 1), grant.lease.lane(), 128);
    gate.onPostReturn(token, Status::OK(), PostAcceptance::kAccepted);
    gate.onRealCompletion(token, /*success=*/false, 0);

    AttemptGate::Settlement settlement;
    ASSERT_TRUE(gate.tryFinalize(token, true, settlement));
    ASSERT_TRUE(settlement.retire);
    ASSERT_TRUE(
        harness.pool->retireFaulted(std::move(grant.lease), "failed cqe").ok());
    gate.discard(token);

    // The replacement attempt gets a fresh member; budget reserve allowed
    // the rebuild while the retired object was still accounted.
    LeaseWithCredit replacement;
    ASSERT_TRUE(
        harness.pool->tryLease(key, makeAttemptKey(1, 2, 1), replacement).ok());
    EXPECT_NE(replacement.lease.lane().jetty_id, first_id);
    EXPECT_EQ(harness.adapter->created(), 2);
}

// ---------------------------------------------------------------------------
// High-concurrency reuse + leak defence
// ---------------------------------------------------------------------------

TEST(UbSendJettyPoolConcurrencyTest, HammerLeaseReturnKeepsInvariants) {
    PoolHarness harness;
    constexpr size_t kDesired = 4;
    ASSERT_TRUE(harness
                    .init(kDesired, /*max_creating=*/4, /*budget=*/4,
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
                const auto attempt =
                    makeAttemptKey(static_cast<uint64_t>(thread + 1),
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
                    if (!live_lanes.insert(grant.lease.lane().jetty_id)
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
                LeaseWithCredit grant;
                const auto attempt =
                    makeAttemptKey(static_cast<uint64_t>(thread + 1),
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
                ASSERT_TRUE(
                    harness.pool->returnOnce(std::move(grant.lease), true)
                        .ok());
                if (round == kRounds / 2 && thread == 0) {
                    stop_requested.store(true);
                }
            }
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
    EXPECT_EQ(params.jetty_pool.business_desired_serving, 6u);
    EXPECT_EQ(params.task_max_inflight_attempts, 4u);
    EXPECT_EQ(params.send_pool.max_creating_per_key, 2u);
    EXPECT_EQ(params.send_pool.jfc_credit_reserve, 16u);
    EXPECT_EQ(params.native_budget.max_jetty_per_device, 0u);
    EXPECT_EQ(params.native_budget.replacement_reserve, 2u);
}

TEST(UbParamsPooledTest, RejectsConflictingRenamedAndLegacyKeys) {
    UbParams params;
    // New and deprecated names both present with different values: fail
    // closed instead of silently preferring one.
    const Status conflict = parseUbParams(R"({
        "transports": {
            "ub": {
                "jetty_pool": {"business_desired_serving": 8},
                "send_pool": {"desired_per_context": 6}
            }
        }
    })",
                                          params);
    EXPECT_TRUE(conflict.IsInvalidArgument());

    // The deprecated name alone is still accepted (migration window).
    ASSERT_TRUE(parseUbParams(R"({
        "transports": {
            "ub": {
                "send_pool": {"desired_per_context": 9}
            }
        }
    })",
                              params)
                    .ok());
    EXPECT_EQ(params.jetty_pool.business_desired_serving, 9u);

    // business_min_size is the compatibility mapping of the legacy
    // jetty_per_endpoint; a conflicting pair is an operator error.
    EXPECT_TRUE(parseUbParams(R"({
        "transports": {
            "ub": {
                "jetty_per_endpoint": 6,
                "jetty_pool": {"business_min_size": 3}
            }
        }
    })",
                              params)
                    .IsInvalidArgument());
    // Both present and equal is accepted.
    ASSERT_TRUE(parseUbParams(R"({
        "transports": {
            "ub": {
                "jetty_per_endpoint": 6,
                "jetty_pool": {
                    "business_desired_serving": 6,
                    "business_min_size": 6,
                    "business_max_size": 6,
                    "business_initial_size": 6
                }
            }
        }
    })",
                              params)
                    .ok());
    EXPECT_EQ(params.jetty_pool.business_min_size, 6u);
}

TEST(UbParamsPooledTest, ParsesDeclaredProbeTopologyAndRejectsBadEdges) {
    UbParams params;
    ASSERT_TRUE(parseUbParams(R"({
        "transports": {
            "ub": {
                "probe": {
                    "local_ports": ["P0", "P1"],
                    "adjacency": {"P0": ["peer-a", "peer-b"], "P1": ["peer-a"]},
                    "target_map": {"P0": ["t0"]}
                }
            }
        }
    })",
                              params)
                    .ok());
    ASSERT_EQ(params.probe.local_ports.size(), 2u);
    ASSERT_EQ(params.probe.adjacency.at("P0").size(), 2u);

    // An edge to an undeclared port is fail-closed.
    EXPECT_TRUE(parseUbParams(R"({
        "transports": {
            "ub": {
                "probe": {
                    "local_ports": ["P0"],
                    "adjacency": {"P9": ["peer-a"]}
                }
            }
        }
    })",
                              params)
                    .IsInvalidArgument());
    // A duplicate edge target is rejected.
    EXPECT_TRUE(parseUbParams(R"({
        "transports": {
            "ub": {
                "probe": {
                    "local_ports": ["P0"],
                    "adjacency": {"P0": ["peer-a", "peer-a"]}
                }
            }
        }
    })",
                              params)
                    .IsInvalidArgument());
}

TEST(UbParamsPooledTest, ParsesExplicitPoolKeys) {
    const std::string config_json = R"({
        "transports": {
            "ub": {
                "jetty_pool": {
                    "business_desired_serving": 8,
                    "business_initial_size": 8,
                    "business_min_size": 8,
                    "business_max_size": 8
                },
                "task_max_inflight_attempts": 2,
                "send_pool": {
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
    EXPECT_EQ(params.jetty_pool.business_desired_serving, 8u);
    EXPECT_EQ(params.task_max_inflight_attempts, 2u);
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
                                 "jetty_pool": {
                                     "business_desired_serving": -1
                                 }
                             }
                         }
                     })",
                               params)
                     .ok());
    // Zero is rejected for the explicit positive knobs.
    ASSERT_FALSE(parseUbParams(R"({
                         "transports": {
                             "ub": {
                                 "task_max_inflight_attempts": 0
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
    auto context =
        std::make_shared<UbContext>(Topology::NicID{0}, device, adapter);

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
    jfcs.push_back(std::make_shared<UbJfc>(0, adapter, std::move(jfc_handle)));
    SendJettyPoolOptions options;
    options.desired_serving = 1;
    auto pool = std::make_shared<SendJettyPool>(adapter, handle, jfcs, budget,
                                                "dev0", credits, options);

    context->attachPooledSendPool(pool);
    EXPECT_EQ(context->pooledSendPool(), pool);
    context->attachPooledSendPool(nullptr);
    EXPECT_EQ(context->pooledSendPool(), nullptr);
}
