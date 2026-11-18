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

#include <memory>
#include <utility>

#include "tent/transport/ub/tombstone.h"
#include "ub_pool_test_support.h"

namespace mooncake::tent::ub {
namespace {

using namespace pool_test;

JettyTombstone makeStone(uint32_t id, uint64_t generation) {
    JettyTombstone stone;
    stone.context_incarnation = 7;
    stone.kind = EntityKind::kJetty;
    stone.native_id = id;
    stone.generation = generation;
    stone.error_epoch = generation * 10;
    stone.responsibility = Responsibility::kUnknown;
    stone.budget_hold = 1;
    return stone;
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

    EXPECT_TRUE(table.erase(100));
    EXPECT_FALSE(table.erase(100));
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
    EXPECT_TRUE(table.erase(10));
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
    EXPECT_TRUE(table.erase(1));
    EXPECT_EQ(table.lookup(EntityKind::kJetty, 5)->generation, 2U);
    EXPECT_TRUE(table.erase(2));
    EXPECT_FALSE(table.lookup(EntityKind::kJetty, 5).has_value());
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

}  // namespace
}  // namespace mooncake::tent::ub
