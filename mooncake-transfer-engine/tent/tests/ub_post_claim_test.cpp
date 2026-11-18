// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3e claim/return unit tests against the real SendJettyPool:
//   * the D-5 second verdict ("stopping" vs "no capacity") is derivable,
//   * the claim review's fault-epoch anchor moves only when a path is isolated,
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

// §8.2 claim anchor: the fault epoch is the only thing the claim review can
// compare, and it moves on isolation (and only then).
// W3g-a: the pool's claim is the authority on "may this attempt be posted".
// A healthy in-use lease claims; a stopping pool, a retired member, a moved
// lane generation and a raised error epoch each get their own verdict.
TEST(UbPostClaimTest, TryClaimPostReportsEachClosedReason) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/2);
    LeaseWithCredit leased;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), leased)
            .ok());
    ASSERT_TRUE(leased.lease.valid());
    const auto lane = leased.lease.lane();

    ClaimRequest request;
    request.attempt = leased.lease.attempt();
    request.lane_generation = lane.lane_generation;
    request.error_epoch_snapshot = leased.error_epoch;
    request.member = leased.lease.member().get();

    // Healthy lease: claimed.
    ClaimVerdict verdict = fixture.pool().tryClaimPost(
        PoolFixture::key(), request.attempt, request);
    EXPECT_TRUE(verdict.claimed());
    EXPECT_EQ(verdict.closed, ClaimClosed::kNone);

    // Wrong generation / wrong epoch are distinguishable and do not mutate the
    // member (IN-1: the claim is read-only).
    ClaimRequest wrong_generation = request;
    wrong_generation.lane_generation = lane.lane_generation + 1;
    EXPECT_EQ(
        fixture.pool()
            .tryClaimPost(PoolFixture::key(), request.attempt, wrong_generation)
            .closed,
        ClaimClosed::kGenerationMoved);
    ClaimRequest wrong_epoch = request;
    wrong_epoch.error_epoch_snapshot = leased.error_epoch + 1;
    EXPECT_EQ(
        fixture.pool()
            .tryClaimPost(PoolFixture::key(), request.attempt, wrong_epoch)
            .closed,
        ClaimClosed::kErrorEpochMoved);
    // Unknown member identity is treated as retired.
    ClaimRequest no_member = request;
    no_member.member = nullptr;
    EXPECT_EQ(fixture.pool()
                  .tryClaimPost(PoolFixture::key(), request.attempt, no_member)
                  .closed,
              ClaimClosed::kMemberRetired);
    // The read-only claim did not change any pool accounting.
    const auto after_claims = fixture.pool().stats();
    EXPECT_EQ(after_claims.in_use_members, 1U);
    EXPECT_EQ(after_claims.total_returns, 0U);

    // Retiring the member (normal return with a failed gate) raises its epoch
    // and makes the old snapshot claim fail as retired.
    ASSERT_TRUE(fixture.pool()
                    .returnOnce(std::move(leased.lease), /*gate_passed=*/false)
                    .ok());
    EXPECT_EQ(fixture.pool()
                  .tryClaimPost(PoolFixture::key(), request.attempt, request)
                  .closed,
              ClaimClosed::kMemberRetired);
}

TEST(UbPostClaimTest, TryClaimPostReportsPoolStoppingFirst) {
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
    // Even a perfect request is refused once the pool is stopping.
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(leased.lease), true).ok());
}

// The epoch is snapshotted into the lease under the pool lock, so a lease taken
// after a member was retired and replaced can never claim with a stale value.
TEST(UbPostClaimTest, LeaseCarriesTheMemberRetirementEpoch) {
    PoolFixture fixture(/*jfc_count=*/1, /*desired_serving=*/2);
    LeaseWithCredit first;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(1), first)
            .ok());
    const uint64_t first_epoch = first.error_epoch;
    ASSERT_TRUE(
        fixture.pool().retireFaulted(std::move(first.lease), "test").ok());

    LeaseWithCredit second;
    ASSERT_TRUE(
        fixture.pool()
            .tryLease(PoolFixture::key(), PoolFixture::attempt(2), second)
            .ok());
    EXPECT_GE(second.error_epoch, first_epoch);
    ClaimRequest request;
    request.attempt = second.lease.attempt();
    request.lane_generation = second.lease.lane().lane_generation;
    request.error_epoch_snapshot = second.error_epoch;
    request.member = second.lease.member().get();
    EXPECT_TRUE(fixture.pool()
                    .tryClaimPost(PoolFixture::key(), request.attempt, request)
                    .claimed());
    ASSERT_TRUE(fixture.pool().returnOnce(std::move(second.lease), true).ok());
}

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
