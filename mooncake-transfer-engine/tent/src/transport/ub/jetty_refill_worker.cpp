// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/jetty_refill_worker.h"

#include <algorithm>
#include <chrono>
#include <system_error>
#include <utility>

namespace mooncake::tent::ub {

JettyRefillWorker::JettyRefillWorker(RefillFn refill, uint32_t interval_ms)
    : refill_(std::move(refill)),
      interval_ms_(std::max<uint32_t>(1, interval_ms)) {}

JettyRefillWorker::~JettyRefillWorker() { stop(); }

bool JettyRefillWorker::start() {
    if (thread_.joinable()) return true;
    try {
        thread_ = std::thread([this] { loop(); });
        return true;
    } catch (const std::system_error&) {
        return false;
    }
}

bool JettyRefillWorker::running() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return thread_.joinable() && !stop_;
}

void JettyRefillWorker::requestWake() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        dirty_ = true;
    }
    cv_.notify_one();
}

void JettyRefillWorker::loop() {
    const auto interval = std::chrono::milliseconds(interval_ms_);
    for (;;) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, interval, [this] { return stop_ || dirty_; });
            if (stop_) return;
            // Consume the wake edge: exactly one refill pass per request.
            dirty_ = false;
        }
        if (refill_) refill_();
    }
}

void JettyRefillWorker::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

}  // namespace mooncake::tent::ub
