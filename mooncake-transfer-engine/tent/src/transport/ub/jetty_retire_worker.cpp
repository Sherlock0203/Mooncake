// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/jetty_retire_worker.h"

#include <system_error>
#include <utility>

namespace mooncake::tent::ub {

JettyRetireWorker::JettyRetireWorker(DestroyFn destroy)
    : destroy_(std::move(destroy)) {}

JettyRetireWorker::~JettyRetireWorker() { stop(); }

bool JettyRetireWorker::start() {
    if (thread_.joinable()) return true;
    try {
        thread_ = std::thread([this] { loop(); });
        return true;
    } catch (const std::system_error&) {
        // Degrade to synchronous destruction: the caller keeps ownership.
        return false;
    }
}

bool JettyRetireWorker::running() const { return thread_.joinable() && !stop_; }

void JettyRetireWorker::enqueue(std::shared_ptr<PoolMember> member) {
    if (!member) return;
    std::vector<std::shared_ptr<PoolMember>> batch;
    batch.push_back(std::move(member));
    enqueue(std::move(batch));
}

void JettyRetireWorker::enqueue(
    std::vector<std::shared_ptr<PoolMember>> members) {
    if (members.empty()) return;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& member : members) pending_.push_back(std::move(member));
    }
    cv_.notify_one();
}

void JettyRetireWorker::loop() {
    for (;;) {
        std::vector<std::shared_ptr<PoolMember>> batch;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stop_ || !pending_.empty(); });
            if (pending_.empty()) {
                if (stop_) return;
                continue;
            }
            batch = std::move(pending_);
            pending_.clear();
        }
        // Native reset+delete and the budget/tombstone accounting run here,
        // never on the completion/return path and never under a pool lock.
        if (destroy_) destroy_(std::move(batch));
    }
}

void JettyRetireWorker::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
    // Anything enqueued while the thread was shutting down drains
    // synchronously, so no retired member is left behind.
    std::vector<std::shared_ptr<PoolMember>> pending;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending = std::move(pending_);
        pending_.clear();
    }
    if (!pending.empty() && destroy_) destroy_(std::move(pending));
}

size_t JettyRetireWorker::pending() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_.size();
}

}  // namespace mooncake::tent::ub
