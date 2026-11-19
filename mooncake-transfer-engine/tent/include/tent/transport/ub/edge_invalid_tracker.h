// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Consecutive business kParamError tracker for declared edges (closed-loop
// design §5.6.3). A MEMORY_ERROR on a declared edge usually means the edge
// crosses an unreachable EID scope; once an edge exceeds the threshold the
// selector stops retrying it for the rest of the install instead of failing
// every slice.

#ifndef TENT_TRANSPORT_UB_EDGE_INVALID_TRACKER_H_
#define TENT_TRANSPORT_UB_EDGE_INVALID_TRACKER_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>

#include "tent/transport/ub/probe_types.h"

namespace mooncake::tent::ub {

class ParamErrorEdgeTracker {
   public:
    explicit ParamErrorEdgeTracker(uint32_t threshold = 3)
        : threshold_(threshold) {}

    // Returns true exactly once, when this call invalidates the edge.
    bool noteError(const ProbePathKey& key) {
        if (threshold_ == 0 || !key.valid()) return false;
        std::lock_guard<std::mutex> lock(mutex_);
        if (invalid_.count(key) != 0) return false;
        uint32_t& streak = streak_[key];
        if (++streak < threshold_) return false;
        invalid_.insert(key);
        streak_.erase(key);
        invalidations_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // Any non-param evidence for the edge breaks the consecutive streak.
    void noteSuccess(const ProbePathKey& key) {
        if (threshold_ == 0 || !key.valid()) return;
        std::lock_guard<std::mutex> lock(mutex_);
        streak_.erase(key);
    }

    [[nodiscard]] bool invalid(const ProbePathKey& key) const {
        if (threshold_ == 0 || !key.valid()) return false;
        std::lock_guard<std::mutex> lock(mutex_);
        return invalid_.count(key) != 0;
    }

    [[nodiscard]] uint64_t invalidations() const {
        return invalidations_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] size_t invalidCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return invalid_.size();
    }

   private:
    const uint32_t threshold_;
    mutable std::mutex mutex_;
    std::map<ProbePathKey, uint32_t> streak_;
    std::set<ProbePathKey> invalid_;
    std::atomic<uint64_t> invalidations_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_EDGE_INVALID_TRACKER_H_
