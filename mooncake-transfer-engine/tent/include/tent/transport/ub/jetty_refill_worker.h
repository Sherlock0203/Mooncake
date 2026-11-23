// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Dedicated refill executor (design §7.3 / §10.2).
//
// The worker wakes periodically (or on requestWake()) and lets the pool run
// exactly one refill pass. The pass itself owns the deficit math and all
// provider calls; this class only owns the thread, the wake-up edge and the
// stop/join contract, so the pool never needs a second scheduler.

#ifndef TENT_TRANSPORT_UB_JETTY_REFILL_WORKER_H_
#define TENT_TRANSPORT_UB_JETTY_REFILL_WORKER_H_

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>

namespace mooncake::tent::ub {

class JettyRefillWorker {
   public:
    // One refill pass. Runs on the worker thread, never under a caller lock.
    using RefillFn = std::function<void()>;

    JettyRefillWorker(RefillFn refill, uint32_t interval_ms);
    ~JettyRefillWorker();

    JettyRefillWorker(const JettyRefillWorker&) = delete;
    JettyRefillWorker& operator=(const JettyRefillWorker&) = delete;

    // Returns false when the platform refuses a thread; the pool then falls
    // back to demand-driven creation only.
    bool start();
    [[nodiscard]] bool running() const;

    // Wakes the worker before the interval elapses (for example after a
    // member was retired and the Serving target is now below its deficit).
    void requestWake();

    void stop();

   private:
    void loop();

    RefillFn refill_;
    const uint32_t interval_ms_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
    bool stop_{false};
    // Wake edge. notify_one() alone was useless: the wait predicate only
    // checked stop_, so the worker went back to sleep until the original
    // interval elapsed. Every requestWake() now makes exactly one pass run.
    bool dirty_{false};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_JETTY_REFILL_WORKER_H_
