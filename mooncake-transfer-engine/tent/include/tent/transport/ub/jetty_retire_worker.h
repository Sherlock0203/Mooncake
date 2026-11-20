// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Dedicated retirement executor (design §10.2 / §7.2 rule 5).
//
// The worker owns the delete queue and the thread that drains it. It never
// polls a JFC and never touches the pool lock itself: the pool hands it fully
// retired members and supplies a destroy function that performs the provider
// reset+delete outside every pool lock. A member's state/jetty fields mutate
// only on this thread once it is enqueued, so diagnostics must report the
// queue state instead of reading the member.

#ifndef TENT_TRANSPORT_UB_JETTY_RETIRE_WORKER_H_
#define TENT_TRANSPORT_UB_JETTY_RETIRE_WORKER_H_

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "tent/transport/ub/business_jetty_pool.h"

namespace mooncake::tent::ub {

class JettyRetireWorker {
   public:
    // Called on the worker thread with one batch of retired members. It must
    // perform the native destruction and may quarantine a member whose delete
    // failed; it must not poll a JFC.
    using DestroyFn =
        std::function<void(std::vector<std::shared_ptr<PoolMember>>)>;

    explicit JettyRetireWorker(DestroyFn destroy);
    ~JettyRetireWorker();

    JettyRetireWorker(const JettyRetireWorker&) = delete;
    JettyRetireWorker& operator=(const JettyRetireWorker&) = delete;

    // Starts the thread. Returns false when the platform refuses a thread;
    // the pool then keeps destructions synchronous instead of terminating.
    bool start();
    [[nodiscard]] bool running() const;

    void enqueue(std::shared_ptr<PoolMember> member);
    void enqueue(std::vector<std::shared_ptr<PoolMember>> members);

    // Stops the thread and drains every queued member synchronously through
    // the destroy function. Idempotent.
    void stop();

    [[nodiscard]] size_t pending() const;

   private:
    void loop();

    DestroyFn destroy_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::shared_ptr<PoolMember>> pending_;
    std::thread thread_;
    bool stop_{false};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_JETTY_RETIRE_WORKER_H_
