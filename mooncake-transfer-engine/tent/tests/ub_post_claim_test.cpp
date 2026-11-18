// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3e/W3g claim/return unit tests against the real SendJettyPool:
//   * the D-5 second verdict ("stopping" vs "no capacity") is derivable,
//   * the claim review's fault-epoch anchor moves only when a path is isolated,
//   * tryClaimPost returns a distinct verdict per refusal reason and is a pure
//     read (design §5.2 rule 3 / §5.7),
//   * a lease is returned exactly once and the credit is released exactly once,
//   * the three mutually exclusive exits (return / retire / leak) are visible
//     in stats() so the tests can assert them without poking internals.
//
// W3-IN-1/W3-IN-2 live here: at most one lease, at most one return, and a
// rollback is only allowed before the native boundary.

#include <gtest/gtest.h>

#include <memory>
#include <utility>
#include <vector>

#include "tent/transport/ub/probe_link_manager.h"
#include "ub_pool_test_support.h"
#include "ub_probe_test_support.h"

namespace mooncake::tent::ub {
namespace {

using namespace pool_test;

// The pooling side reports one TooManyRequests for "stopping", "no idle
// member", "no budget" and "no credit" (D-5/R-2). W3 must therefore re-read
// stopping() after a failure: a stop is a cancellation, a miss is a defer
// (RK2). This pins both halves of that verdict.
TEST(UbPostClaimTest, TooManyRequestsIsDisambiguatedByStopping) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1);

    LeaseWithCredit first;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), first)
            .ok());
    // Capacity miss: the pool is full but running.
    LeaseWithCredit second;
    const Status miss = fixture.pool().tryLease(
        PoolFixture::key(), PoolFixture::attempt(2), second);
    ASSERT_TRUE(miss.IsTooManyRequests());
    EXPECT_FALSE(fixture.pool().stopping());
    EXPECT_FALSE(second.lease.valid());

    // Stopping: the same status, but stopping() distinguishes it.
    fixture.pool().beginStop();
    EXPECT_TRUE(fixture.pool().stopping());
    LeaseWithCredit third;
    const Status stopping = fixture.pool().tryLease(
        PoolFixture::key(), PoolFixture::attempt(3), third);
    ASSERT_TRUE(stopping.IsTooManyRequests());
    EXPECT_TRUE(fixture.pool().stopping());

    const auto stats = fixture.pool().stats();
    EXPECT_EQ(stats.pool_misses, 1U);
    EXPECT_EQ(stats.stop_rejections, 1U);

    // A stopping pool still settles the outstanding lease, and the return is
    // turned into a retirement by the pool itself (the arbiter, design §7.4).
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(first.lease), true).ok());
    EXPECT_EQ(fixture.pool().stats().in_use_members, 0U);
    EXPECT_EQ(fixture.pool().stats().idle_members, 0U);
}

// A successful lease carries one reserved JFC credit, and the caller - not the
// pool - releases it, exactly once (D-7).
TEST(UbPostClaimTest, LeaseCarriesExactlyOneCreditRelease) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/2,
                        /*credit_capacity=*/1);
    EXPECT_EQ(fixture.credits().outstanding(0), 0U);

    LeaseWithCredit lease;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), lease)
            .ok());
    EXPECT_EQ(fixture.credits().outstanding(0), 1U);
    EXPECT_EQ(lease.jfc_index, 0U);
    EXPECT_NE(lease.jfc_credit_token, 0U);

    // Credit exhausted: the second attempt cannot lease even though the pool
    // has room. This is the kCredit-vs-kPoolIdle distinction R-2 would expose;
    // until it lands both defer into the same bucket (§18-2).
    LeaseWithCredit starved;
    const Status miss = fixture.pool().tryLease(
        PoolFixture::key(), PoolFixture::attempt(2), starved);
    ASSERT_TRUE(miss.IsTooManyRequests());
    EXPECT_FALSE(starved.lease.valid());
    EXPECT_EQ(fixture.credits().rejectedReservations(0), 1U);

    // Releasing the token makes the pool leaseable again.
    fixture.credits().release(lease.jfc_index, lease.jfc_credit_token);
    EXPECT_EQ(fixture.credits().outstanding(0), 0U);
    // A duplicate release is harmless for the ledger, but it is a defect and
    // must be observable - otherwise a double release passes every assertion.
    fixture.credits().release(lease.jfc_index, lease.jfc_credit_token);
    EXPECT_EQ(fixture.credits().outstanding(0), 0U);
    EXPECT_EQ(fixture.credits().rejectedReleases(0), 1U);

    LeaseWithCredit again;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(3), again)
            .ok());
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(lease.lease), true).ok());
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(again.lease), true).ok());
    EXPECT_EQ(fixture.pool().stats().double_returns, 0U);
}

// W3-IN-1: one lease, one return. A lease is move-only, so the only way to
// reach a second return is to hand the same value back twice; the moved-from
// value is inert and the pool refuses it without touching its counters. The
// authoritative double-return guard is the gate's claim bit (W2), and the pool
// counts any violation it does see as double_returns.
TEST(UbPostClaimTest, MovedFromLeaseIsInertAndCountersStayClean) {
    PoolFixture fixture;
    LeaseWithCredit lease;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), lease)
            .ok());
    SliceJettyLease moved = std::move(lease.lease);
    EXPECT_FALSE(lease.lease.valid());
    EXPECT_TRUE(moved.valid());

    ASSERT_TRUE(fixture.pool().returnOnce(std::move(moved), true).ok());
    EXPECT_EQ(fixture.pool().stats().total_returns, 1U);
    EXPECT_EQ(fixture.pool().stats().double_returns, 0U);
    EXPECT_EQ(fixture.pool().stats().idle_members, 1U);
    // Returning a moved-from lease is a programming error, reported but never
    // double counted.
    EXPECT_FALSE(fixture.pool().returnOnce(std::move(lease.lease), true).ok());
    EXPECT_EQ(fixture.pool().stats().double_returns, 0U);
    EXPECT_EQ(fixture.pool().stats().total_returns, 1U);
}

// kFinalize: a first real error retires the member permanently, and the budget
// is released only after the native delete succeeded (INV07/INV09).
TEST(UbPostClaimTest, RetireFaultedReleasesTheMemberAndItsBudget) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/2);
    LeaseWithCredit lease;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), lease)
            .ok());
    const auto live_before = fixture.adapter()->liveJettyCount();
    ASSERT_GT(live_before, 0U);

    ASSERT_TRUE(fixture.pool()
                    .retireFaulted(std::move(lease.lease), "remote_path")
                    .ok());
    const auto stats = fixture.pool().stats();
    EXPECT_EQ(stats.in_use_members, 0U);
    // A retired member never returns to the idle stack (INV07).
    EXPECT_EQ(fixture.pool().servingCount(PoolFixture::key()), 0U);
    // The delete went through the adapter, so the native handle count dropped.
    EXPECT_EQ(fixture.adapter()->liveJettyCount(), live_before - 1);
}

// A lease dropped without a return is a responsibility leak: the member is
// quarantined with its budget retained, never silently reused.
TEST(UbPostClaimTest, DroppedLeaseIsQuarantinedNotReused) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1);
    {
        LeaseWithCredit lease;
        ASSERT_TRUE(
            fixture.pool()
                .tryLease(PoolFixture::key(), PoolFixture::attempt(1), lease)
                .ok());
        // `lease` goes out of scope without returnOnce().
    }
    const auto stats = fixture.pool().stats();
    EXPECT_EQ(stats.leaked_leases, 1U);
    EXPECT_EQ(stats.in_use_members, 0U);
    EXPECT_EQ(stats.idle_members, 0U);
    // The reason the leak is a defect and not a crash: the member stayed owned
    // by the pool (quarantined), so shutdown can still drain it.
    EXPECT_GE(stats.quarantined_members, 1U);
}

// §5.7 ClaimAllowsTheLeasedMember + ClaimRefusedOnGenerationMismatch: the
// pool's verdict is the authority on "may this attempt be posted now".
TEST(UbPostClaimTest, ClaimAllowsTheLeasedMember) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1);
    LeaseWithCredit leased;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), leased)
            .ok());
    ClaimRequest request;
    request.attempt = leased.lease.attempt();
    request.lane_generation = leased.lease.lane().lane_generation;
    request.error_epoch_snapshot = leased.error_epoch;
    request.member = leased.lease.member().get();
    EXPECT_TRUE(fixture.pool()
                    .tryClaimPost(PoolFixture::key(), request.attempt, request)
                    .claimed());
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(leased.lease), true).ok());
    EXPECT_EQ(fixture.pool().stats().idle_members, 1U);
}

TEST(UbPostClaimTest, ClaimRefusedOnGenerationMismatch) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1);
    LeaseWithCredit leased;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), leased)
            .ok());
    ClaimRequest request;
    request.attempt = leased.lease.attempt();
    request.lane_generation = leased.lease.lane().lane_generation + 1;
    request.error_epoch_snapshot = leased.error_epoch;
    request.member = leased.lease.member().get();
    EXPECT_EQ(fixture.pool()
                  .tryClaimPost(PoolFixture::key(), request.attempt, request)
                  .closed,
              ClaimClosed::kGenerationMoved);
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(leased.lease), true).ok());
}

// §5.7 ClaimRefusedAfterBeginStop: a denied claim rolls back through
// returnOnce(true), which must put the clean member back at the LIFO head.
TEST(UbPostClaimTest, ClaimRefusedAfterBeginStopReturnsMemberToLifoHead) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1);
    LeaseWithCredit leased;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), leased)
            .ok());
    ClaimRequest request;
    request.attempt = leased.lease.attempt();
    request.lane_generation = leased.lease.lane().lane_generation;
    request.error_epoch_snapshot = leased.error_epoch;
    request.member = leased.lease.member().get();
    fixture.pool().beginStop();
    EXPECT_EQ(fixture.pool()
                  .tryClaimPost(PoolFixture::key(), request.attempt, request)
                  .closed,
              ClaimClosed::kPoolStopping);
    // A stopping pool turns the rollback return into a retirement, which is
    // the pool's decision, not the claimant's (design §7.4).
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(leased.lease), true).ok());
    EXPECT_EQ(fixture.pool().stats().idle_members, 0U);
}

// §5.7 ClaimRefusedAfterRetireBumpsEpoch: a retirement during the lease moves
// the member's epoch, so the pre-lease snapshot can never claim again.
TEST(UbPostClaimTest, ClaimRefusedAfterRetireBumpsEpoch) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1);
    LeaseWithCredit leased;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), leased)
            .ok());
    ClaimRequest request;
    request.attempt = leased.lease.attempt();
    request.lane_generation = leased.lease.lane().lane_generation;
    // The snapshot is one retirement behind: this is the defensive branch that
    // a background-retirement model would take.
    request.error_epoch_snapshot = leased.error_epoch + 1;
    request.member = leased.lease.member().get();
    EXPECT_EQ(fixture.pool()
                  .tryClaimPost(PoolFixture::key(), request.attempt, request)
                  .closed,
              ClaimClosed::kErrorEpochMoved);
    // The member is still exclusively ours; the refusal did not retire it.
    EXPECT_EQ(fixture.pool().stats().in_use_members, 1U);
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(leased.lease), true).ok());
}

// §5.7 ClaimIsPureRead: two consecutive claims change no pool accounting.
TEST(UbPostClaimTest, ClaimIsPureRead) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/1);
    LeaseWithCredit leased;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), leased)
            .ok());
    ClaimRequest request;
    request.attempt = leased.lease.attempt();
    request.lane_generation = leased.lease.lane().lane_generation;
    request.error_epoch_snapshot = leased.error_epoch;
    request.member = leased.lease.member().get();
    const auto before = fixture.pool().stats();
    EXPECT_TRUE(fixture.pool()
                    .tryClaimPost(PoolFixture::key(), request.attempt, request)
                    .claimed());
    EXPECT_TRUE(fixture.pool()
                    .tryClaimPost(PoolFixture::key(), request.attempt, request)
                    .claimed());
    const auto after = fixture.pool().stats();
    EXPECT_EQ(after.idle_members, before.idle_members);
    EXPECT_EQ(after.in_use_members, before.in_use_members);
    EXPECT_EQ(after.total_leases, before.total_leases);
    EXPECT_EQ(after.total_returns, before.total_returns);
    EXPECT_EQ(after.pool_misses, before.pool_misses);
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(leased.lease), true).ok());
}

// §8.2 claim anchor: the fault epoch is the only thing the claim review can
// compare, and it moves on isolation (and only then).
TEST(UbPostClaimTest, ClaimAnchorMovesOnlyOnIsolation) {
    using namespace probe_test;
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const UbPostPath path{0, 10, 0, 5};

    const uint64_t snapshot = plm.businessFaultEpoch(path);
    EXPECT_EQ(snapshot, 0U);
    // The anchor is unchanged while the path is healthy, so a claim taken
    // before the lease still matches after it.
    EXPECT_EQ(plm.businessFaultEpoch(path), snapshot);
    EXPECT_TRUE(plm.businessPathUsable(path));

    plm.onBusinessPathError(path, BusinessErrorKind::kPathFault, clock.now());
    // The path was isolated between the snapshot and the review: the claim is
    // refused and the attempt rolls back (kPathUnavailable).
    EXPECT_NE(plm.businessFaultEpoch(path), snapshot);
    EXPECT_FALSE(plm.businessPathUsable(path));
}

}  // namespace
}  // namespace mooncake::tent::ub
