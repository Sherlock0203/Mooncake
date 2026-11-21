// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Tombstone table and recovery-budget tests (design §7.2 / §11.4):
//   * an unproven destruction is recorded, looked up by native identity and
//     erased only after the provider proves destruction;
//   * a full table backpressures instead of shedding or TTL-evicting;
//   * native budget is released only after the tombstone is erased;
//   * a full quarantine budget stops admission.

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>
#include <utility>

#include "tent/transport/ub/tombstone.h"
#include "ub_pool_test_support.h"

namespace mooncake::tent::ub {
namespace {

using namespace pool_test;

JettyTombstone makeStone(uint32_t id, uint64_t generation,
                         uint64_t context = 7) {
    JettyTombstone stone;
    stone.context_incarnation = context;
    stone.kind = EntityKind::kJetty;
    stone.native_id = id;
    stone.generation = generation;
    stone.error_epoch = generation * 10;
    stone.responsibility = Responsibility::kUnknown;
    stone.budget_hold = 1;
    return stone;
}

ResourceIdentity idOf(uint32_t native_id, uint64_t generation,
                      uint64_t context = 7) {
    return ResourceIdentity{context, EntityKind::kJetty, native_id, generation};
}

ResourceEntity entityOf(uint32_t native_id, uint64_t context = 7) {
    return ResourceEntity{context, EntityKind::kJetty, native_id};
}

TEST(TombstoneTableTest, InsertLookupAndEraseByGeneration) {
    TombstoneTable table(/*capacity=*/2);
    EXPECT_TRUE(table.insert(makeStone(11, 100)).ok());
    // Re-inserting the same generation is idempotent.
    EXPECT_TRUE(table.insert(makeStone(11, 100)).ok());
    EXPECT_EQ(table.size(), 1U);
    const auto found = table.lookup(EntityKind::kJetty, 11);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->generation, 100U);
    EXPECT_EQ(found->budget_hold, 1U);

    EXPECT_TRUE(table.erase(idOf(11, 100)));
    EXPECT_FALSE(table.erase(idOf(11, 100)));
    EXPECT_EQ(table.size(), 0U);
    EXPECT_FALSE(table.lookup(EntityKind::kJetty, 11).has_value());
    EXPECT_EQ(table.erased(), 1U);
}

TEST(TombstoneTableTest, FullTableBackpressuresWithoutEviction) {
    TombstoneTable table(/*capacity=*/1);
    EXPECT_TRUE(table.insert(makeStone(1, 10)).ok());
    EXPECT_TRUE(table.full());
    // A full table refuses the new owner; the incumbent is never evicted.
    EXPECT_TRUE(table.insert(makeStone(2, 20)).IsTooManyRequests());
    EXPECT_EQ(table.rejectedInserts(), 1U);
    EXPECT_EQ(table.size(), 1U);
    EXPECT_TRUE(table.lookup(EntityKind::kJetty, 1).has_value());
    EXPECT_FALSE(table.lookup(EntityKind::kJetty, 2).has_value());
    // Erasing the incumbent frees the slot for the next owner.
    EXPECT_TRUE(table.erase(idOf(1, 10)));
    EXPECT_TRUE(table.insert(makeStone(2, 20)).ok());
}

TEST(TombstoneTableTest, NewestGenerationWinsForOneNativeIdentity) {
    TombstoneTable table(/*capacity=*/4);
    EXPECT_TRUE(table.insert(makeStone(5, 1)).ok());
    EXPECT_TRUE(table.insert(makeStone(5, 2)).ok());
    const auto found = table.lookup(EntityKind::kJetty, 5);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->generation, 2U);
    // Erasing the older generation leaves the newest entry addressable.
    EXPECT_TRUE(table.erase(idOf(5, 1)));
    EXPECT_EQ(table.lookup(entityOf(5))->generation, 2U);
    EXPECT_TRUE(table.erase(idOf(5, 2)));
    EXPECT_FALSE(table.lookup(entityOf(5)).has_value());
}

TEST(UbTombstonePoolTest, RetirementRecordsAndDestructionErases) {
    auto table = std::make_shared<TombstoneTable>(/*capacity=*/8);
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*quarantine_budget=*/64, table);
    LeaseWithCredit grant;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), grant)
            .ok());
    const uint32_t native_id = grant.lease.lane().jetty_id;
    ASSERT_TRUE(
        fixture.pool().retireFaulted(std::move(grant.lease), "cqe error").ok());
    // The deletion is synchronous in this fixture: the tombstone was recorded
    // and then erased by the proven destruction, and the budget was released.
    EXPECT_EQ(table->size(), 0U);
    EXPECT_EQ(table->erased(), 1U);
    EXPECT_FALSE(table->lookup(EntityKind::kJetty, native_id).has_value());
    EXPECT_EQ(fixture.budget()->snapshot(PoolFixture::kDeviceKey).total_jetties,
              0U);
}

TEST(UbTombstonePoolTest, FailedDestructionKeepsTombstoneAndBudget) {
    auto table = std::make_shared<TombstoneTable>(/*capacity=*/8);
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*quarantine_budget=*/64, table);
    LeaseWithCredit grant;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), grant)
            .ok());
    const uint32_t native_id = grant.lease.lane().jetty_id;
    fixture.adapter()->failNextDeleteJetty();
    ASSERT_TRUE(
        fixture.pool().retireFaulted(std::move(grant.lease), "cqe error").ok());
    // Destruction is unproven: the entry and the budget stay charged.
    EXPECT_EQ(table->size(), 1U);
    EXPECT_TRUE(table->lookup(EntityKind::kJetty, native_id).has_value());
    EXPECT_EQ(fixture.budget()->snapshot(PoolFixture::kDeviceKey).total_jetties,
              1U);
    EXPECT_GE(fixture.pool().stats().quarantined_members, 1U);

    // A later drain proves destruction, erases the entry and releases budget.
    ASSERT_TRUE(fixture.pool().drainQuarantined().ok());
    EXPECT_EQ(table->size(), 0U);
    EXPECT_EQ(fixture.budget()->snapshot(PoolFixture::kDeviceKey).total_jetties,
              0U);
}

TEST(UbTombstonePoolTest, FullQuarantineBudgetStopsAdmission) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*quarantine_budget=*/1);
    LeaseWithCredit grant;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), grant)
            .ok());
    ASSERT_TRUE(
        fixture.pool().retireFaulted(std::move(grant.lease), "cqe error").ok());
    // The member is deleted synchronously, so quarantine is empty again: leak
    // one lease instead to fill the budget.
    {
        LeaseWithCredit leaked;
        ASSERT_TRUE(
            fixture.pool()
                .tryLease(PoolFixture::key(), PoolFixture::attempt(2), leaked)
                .ok());
        // Dropping the lease without returning it quarantines the member.
    }
    const auto stats = fixture.pool().stats();
    EXPECT_TRUE(stats.quarantine_backpressure);
    LeaseWithCredit blocked;
    EXPECT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(3), blocked)
            .IsTooManyRequests());
    EXPECT_GE(fixture.pool().stats().backpressure_rejections, 1U);
}

TEST(UbTombstonePoolTest, RetirementFenceRunsBeforeDelete) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1);
    LeaseWithCredit grant;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), grant)
            .ok());
    ASSERT_TRUE(fixture.pool()
                    .retireFaulted(std::move(grant.lease), "cqe error",
                                   /*fence_hardware=*/true)
                    .ok());
    EXPECT_GE(fixture.adapter()->quiesceCalls(), 1U);
    EXPECT_EQ(fixture.adapter()->liveJettyCount(), 0U);
    EXPECT_EQ(fixture.pool().stats().flushing_members, 0U);
}

// F2/INV-ID-1: two Contexts both start their lane generation at 1. The table
// must keep both entries instead of treating the numeric generation as a
// global key (the old table returned OK on a generation collision, so the
// second Context believed its retirement was recorded).
TEST(TombstoneTableTest, DistinctContextsSameGenerationBothStored) {
    TombstoneTable table(/*capacity=*/8);
    EXPECT_TRUE(table
                    .insert(makeStone(/*id=*/11, /*generation=*/1,
                                      /*context=*/7))
                    .ok());
    EXPECT_TRUE(table
                    .insert(makeStone(/*id=*/12, /*generation=*/1,
                                      /*context=*/8))
                    .ok());
    ASSERT_EQ(table.size(), 2U);
    const auto first = table.lookup(entityOf(11, 7));
    const auto second = table.lookup(entityOf(12, 8));
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->context_incarnation, 7U);
    EXPECT_EQ(second->context_incarnation, 8U);
}

TEST(TombstoneTableTest, EraseOneContextLeavesTheOther) {
    TombstoneTable table(/*capacity=*/8);
    ASSERT_TRUE(table.insert(makeStone(11, 1, 7)).ok());
    ASSERT_TRUE(table.insert(makeStone(12, 1, 8)).ok());
    // INV-ID-2: erase only removes the entry that was inserted, never the
    // other Context's same-numbered generation.
    EXPECT_TRUE(table.erase(idOf(11, 1, 7)));
    EXPECT_EQ(table.size(), 1U);
    EXPECT_FALSE(table.lookup(entityOf(11, 7)).has_value());
    EXPECT_TRUE(table.lookup(entityOf(12, 8)).has_value());
    EXPECT_FALSE(table.erase(idOf(11, 1, 9)));
}

TEST(TombstoneTableTest, InsertWithoutIncarnationIsRejected) {
    TombstoneTable table(/*capacity=*/8);
    auto anonymous = makeStone(11, 1);
    anonymous.context_incarnation = 0;
    EXPECT_TRUE(table.insert(anonymous).IsInvalidArgument());
    EXPECT_EQ(table.size(), 0U);
    EXPECT_EQ(table.rejectedInserts(), 0U);
    EXPECT_EQ(table.invalidIdentityInserts(), 1U);
}

TEST(TombstoneTableTest, SameEntityNewerGenerationSupersedes) {
    TombstoneTable table(/*capacity=*/8);
    ASSERT_TRUE(table.insert(makeStone(5, 3)).ok());
    ASSERT_TRUE(table.insert(makeStone(5, 4)).ok());
    const auto newest = table.lookup(entityOf(5));
    ASSERT_TRUE(newest.has_value());
    EXPECT_EQ(newest->generation, 4U);
    // The older entry is retained until its own proven destruction.
    EXPECT_EQ(table.size(), 2U);
    EXPECT_TRUE(table.erase(idOf(5, 4)));
    EXPECT_EQ(table.lookup(entityOf(5))->generation, 3U);
}

// F3/INV-BUD-1: creation failure has exactly one cleanup path
// (retireUnpublished). A provider that fails but still hands back a partially
// allocated handle must not be pre-deleted by the caller nor have its budget
// released before destruction is proven.
TEST(UbPoolBudgetTest, CreateFailureKeepsBudgetWhenDeleteFails) {
    auto table = std::make_shared<TombstoneTable>(/*capacity=*/8);
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*quarantine_budget=*/64, table);
    fixture.adapter()->failNextCreateJettyWithHandle();
    fixture.adapter()->failNextDeleteJetty();

    LeaseWithCredit grant;
    const Status status = fixture.pool().tryLease(
        PoolFixture::key(), PoolFixture::attempt(1), grant);
    EXPECT_FALSE(status.ok());

    const auto stats = fixture.pool().stats();
    EXPECT_EQ(stats.create_failures, 1U);
    EXPECT_EQ(stats.create_failures_unproven, 1U);
    EXPECT_EQ(stats.budget_release_anomalies, 0U);
    EXPECT_GE(stats.quarantined_members, 1U);
    // The unproven handle is retained, its budget stays charged and the
    // tombstone keeps the generation addressable.
    EXPECT_EQ(fixture.adapter()->liveJettyCount(), 1U);
    EXPECT_EQ(fixture.budget()->snapshot(PoolFixture::kDeviceKey).total_jetties,
              1U);
    EXPECT_EQ(table->size(), 1U);
}

TEST(UbPoolBudgetTest, CreateFailureReleasesExactlyOnceWhenDeleteSucceeds) {
    auto table = std::make_shared<TombstoneTable>(/*capacity=*/8);
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*quarantine_budget=*/64, table);

    // Two consecutive partial-handle failures: each reservation is released
    // by retireUnpublished and never by a second, caller-side path.
    for (int round = 0; round < 2; ++round) {
        fixture.adapter()->failNextCreateJettyWithHandle();
        LeaseWithCredit grant;
        EXPECT_FALSE(fixture.pool()
                         .tryLease(PoolFixture::key(),
                                   PoolFixture::attempt(10 + round), grant)
                         .ok());
    }

    const auto stats = fixture.pool().stats();
    EXPECT_EQ(stats.create_failures, 2U);
    EXPECT_EQ(stats.create_failures_unproven, 0U);
    EXPECT_EQ(stats.budget_release_anomalies, 0U);
    EXPECT_EQ(stats.quarantined_members, 0U);
    EXPECT_EQ(fixture.adapter()->liveJettyCount(), 0U);
    EXPECT_EQ(fixture.budget()->snapshot(PoolFixture::kDeviceKey).total_jetties,
              0U);
    EXPECT_EQ(table->size(), 0U);
}

TEST(UbPoolBudgetTest, StoppedWhileCreatingUsesTheSingleCleanupPath) {
    auto table = std::make_shared<TombstoneTable>(/*capacity=*/8);
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*quarantine_budget=*/64, table);
    fixture.adapter()->holdNextCreateJetty();

    Status status = Status::OK();
    std::thread creator([&] {
        LeaseWithCredit grant;
        status = fixture.pool().tryLease(PoolFixture::key(),
                                         PoolFixture::attempt(1), grant);
    });
    for (int spin = 0; spin < 2000 && !fixture.adapter()->createGateWaiting();
         ++spin) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(fixture.adapter()->createGateWaiting());
    // Stop while createJetty is inside the provider: the fresh handle must be
    // retired through retireUnpublished, not a second caller-side delete.
    fixture.pool().beginStop();
    fixture.adapter()->releaseCreateJetty();
    creator.join();

    EXPECT_TRUE(status.IsTooManyRequests());
    const auto stats = fixture.pool().stats();
    EXPECT_EQ(stats.create_failures_unproven, 0U);
    EXPECT_EQ(stats.budget_release_anomalies, 0U);
    EXPECT_EQ(fixture.adapter()->liveJettyCount(), 0U);
    EXPECT_EQ(fixture.budget()->snapshot(PoolFixture::kDeviceKey).total_jetties,
              0U);
    EXPECT_EQ(table->size(), 0U);
}

}  // namespace
}  // namespace mooncake::tent::ub
