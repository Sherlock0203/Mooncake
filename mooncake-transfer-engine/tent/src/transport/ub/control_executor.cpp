// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/control_executor.h"

#include <algorithm>
#include <chrono>
#include <system_error>
#include <utility>

#include <glog/logging.h>

namespace mooncake::tent::ub {
namespace {

uint64_t steadyNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace

ThreadedControlExecutor::ThreadedControlExecutor(std::string name,
                                                 size_t queue_capacity)
    : name_(std::move(name)), capacity_(std::max<size_t>(1, queue_capacity)) {}

ThreadedControlExecutor::~ThreadedControlExecutor() { stop(); }

bool ThreadedControlExecutor::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) return true;
    try {
        thread_ = std::thread([this] { loop(); });
        started_ = true;
        return true;
    } catch (const std::system_error&) {
        // The caller keeps the synchronous retirement path.
        return false;
    }
}

Status ThreadedControlExecutor::submit(Task task) {
    return enqueue(0, std::move(task));
}

Status ThreadedControlExecutor::scheduleAfter(uint64_t delay_ns, Task task) {
    return enqueue(delay_ns, std::move(task));
}

Status ThreadedControlExecutor::enqueue(uint64_t delay_ns, Task task) {
    if (!task) return Status::InvalidArgument("empty control task");
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_ || !started_) {
            return Status::TooManyRequests("UB control executor " + name_ +
                                           " is not running");
        }
        if (queue_.size() >= capacity_) {
            return Status::TooManyRequests("UB control executor " + name_ +
                                           " queue is full");
        }
        queue_.push(
            Entry{steadyNowNs() + delay_ns, next_sequence_++, std::move(task)});
    }
    cv_.notify_one();
    return Status::OK();
}

bool ThreadedControlExecutor::running() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return started_ && !stop_;
}

size_t ThreadedControlExecutor::pending() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

void ThreadedControlExecutor::loop() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
        if (queue_.empty()) {
            if (stop_) return;
            cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
            continue;
        }
        const uint64_t now = steadyNowNs();
        if (!stop_ && queue_.top().due_ns > now) {
            cv_.wait_until(lock,
                           std::chrono::steady_clock::time_point(
                               std::chrono::nanoseconds(queue_.top().due_ns)));
            continue;
        }
        Entry entry = queue_.top();
        queue_.pop();
        lock.unlock();
        if (entry.task) entry.task();
        lock.lock();
    }
}

void ThreadedControlExecutor::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();

    // A task submitted while the loop was returning must not be dropped: the
    // controller may still owe a quarantine/marker bookkeeping step. The tasks
    // themselves are written to be no-ops once their controller is stopping.
    std::vector<Entry> leftover;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        leftover.reserve(queue_.size());
        while (!queue_.empty()) {
            leftover.push_back(queue_.top());
            queue_.pop();
        }
    }
    std::sort(leftover.begin(), leftover.end(),
              [](const Entry& lhs, const Entry& rhs) {
                  if (lhs.due_ns != rhs.due_ns) return lhs.due_ns < rhs.due_ns;
                  return lhs.sequence < rhs.sequence;
              });
    for (auto& entry : leftover) {
        if (entry.task) entry.task();
    }
}

}  // namespace mooncake::tent::ub
