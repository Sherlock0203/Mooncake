// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "tent/transport/ub/edge_invalid_tracker.h"

namespace mooncake::tent::ub {
namespace {

TEST(UbParamErrorEdgeTrackerTest, InvalidatesAfterConsecutiveErrors) {
    ParamErrorEdgeTracker tracker(3);
    const ProbePathKey key{0, static_cast<SegmentID>(kProbePeerIdBase), 1};
    EXPECT_FALSE(tracker.noteError(key));
    EXPECT_FALSE(tracker.noteError(key));
    EXPECT_FALSE(tracker.invalid(key));
    EXPECT_TRUE(tracker.noteError(key));
    EXPECT_TRUE(tracker.invalid(key));
    EXPECT_EQ(tracker.invalidations(), 1U);
    EXPECT_EQ(tracker.invalidCount(), 1U);
    // A further error is not a second invalidation.
    EXPECT_FALSE(tracker.noteError(key));
    EXPECT_EQ(tracker.invalidations(), 1U);
}

TEST(UbParamErrorEdgeTrackerTest, SuccessBreaksTheConsecutiveStreak) {
    ParamErrorEdgeTracker tracker(3);
    const ProbePathKey key{0, static_cast<SegmentID>(kProbePeerIdBase), 0};
    EXPECT_FALSE(tracker.noteError(key));
    EXPECT_FALSE(tracker.noteError(key));
    tracker.noteSuccess(key);
    EXPECT_FALSE(tracker.noteError(key));
    EXPECT_FALSE(tracker.noteError(key));
    EXPECT_FALSE(tracker.invalid(key));
    EXPECT_TRUE(tracker.noteError(key));
    EXPECT_TRUE(tracker.invalid(key));
}

TEST(UbParamErrorEdgeTrackerTest,
     ZeroThresholdDisablesAndInvalidKeysAreIgnored) {
    ParamErrorEdgeTracker disabled(0);
    const ProbePathKey key{0, static_cast<SegmentID>(kProbePeerIdBase), 0};
    for (int i = 0; i < 10; ++i) EXPECT_FALSE(disabled.noteError(key));
    EXPECT_FALSE(disabled.invalid(key));
    EXPECT_EQ(disabled.invalidations(), 0U);

    ParamErrorEdgeTracker tracker(1);
    const ProbePathKey invalid{0, LOCAL_SEGMENT_ID, 0};
    EXPECT_FALSE(tracker.noteError(invalid));
    EXPECT_FALSE(tracker.invalid(invalid));
}

}  // namespace
}  // namespace mooncake::tent::ub
