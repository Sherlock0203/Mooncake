// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Control executor for the asynchronous Jetty drain chain
// (design tent-ub-async-drain-design.md §3.1/§4.2).
//
// The chain may not run provider calls on a JFC poller or on the error-return
// path, so every drain step (modify ERROR, software flush, finish, timeout) is
// submitted here and runs on the single control thread. The interface is a
// virtual base so unit tests can drive a deterministic, manual executor with a
// fake clock instead of racing a real thread.

#ifndef TENT_TRANSPORT_UB_CONTROL_EXECUTOR_H_
#define TENT_TRANSPORT_UB_CONTROL_EXECUTOR_H_

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/common/status.h"

namespace mooncake::tent::ub {

class ControlExecutor {
   public:
    using Task = std::function<void()>;

    virtual ~ControlExecutor() = default;

    // Queues `task` for the control thread. Never blocks. Returns
    // TooManyRequests when the queue is full or the executor is stopped; the
    // caller must treat that as "the drain could not be scheduled" and fail
    // closed (quarantine) instead of retrying forever.
    virtual Status submit(Task task) = 0;

    // Queues `task` to run no earlier than `delay_ns` from now. Used to re-arm
    // the drain tick; a controller never polls on its own timer.
    virtual Status scheduleAfter(uint64_t delay_ns, Task task) = 0;

    [[nodiscard]] virtual bool running() const = 0;
};

// Single-thread executor with a bounded queue and a delay wheel. Tasks run in
// due-time order; the thread only exists between start() and stop().
class ThreadedControlExecutor final : public ControlExecutor {
   public:
    ThreadedControlExecutor(std::string name, size_t queue_capacity = 4096);
    ~ThreadedControlExecutor() override;

    ThreadedControlExecutor(const ThreadedControlExecutor&) = delete;
    ThreadedControlExecutor& operator=(const ThreadedControlExecutor&) = delete;

    // Returns false when the platform refuses a thread; the caller then keeps
    // the synchronous retirement path instead of terminating.
    bool start();
    // Drains every queued task on the calling thread, then joins. Idempotent.
    void stop();

    Status submit(Task task) override;
    Status scheduleAfter(uint64_t delay_ns, Task task) override;
    [[nodiscard]] bool running() const override;

    [[nodiscard]] size_t pending() const;

   private:
    struct Entry {
        uint64_t due_ns = 0;
        uint64_t sequence = 0;
        Task task;
    };
    struct Later {
        bool operator()(const Entry& lhs, const Entry& rhs) const noexcept {
            if (lhs.due_ns != rhs.due_ns) return lhs.due_ns > rhs.due_ns;
            return lhs.sequence > rhs.sequence;
        }
    };

    Status enqueue(uint64_t delay_ns, Task task);
    void loop();

    const std::string name_;
    const size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::priority_queue<Entry, std::vector<Entry>, Later> queue_;
    std::thread thread_;
    bool stop_{false};
    bool started_{false};
    uint64_t next_sequence_{1};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_CONTROL_EXECUTOR_H_
