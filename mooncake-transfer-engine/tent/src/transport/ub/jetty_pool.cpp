// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "jetty_pool.h"

#include <utility>

namespace mooncake::tent::ub {

UbSendJettyPool::UbSendJettyPool(std::shared_ptr<UrmaAdapter> adapter,
                                 const ContextPtr& context_handle,
                                 std::vector<std::shared_ptr<UbJfc>> jfcs,
                                 JettyOptions options)
    : adapter_(std::move(adapter)),
      context_handle_(context_handle),
      jfcs_(std::move(jfcs)),
      options_(options) {}

Status UbSendJettyPool::acquire(size_t count, std::vector<JettyPtr>& jetties,
                                std::vector<size_t>& jfc_indices) {
    jetties.clear();
    jfc_indices.clear();

    // Snapshot the detachable dependencies under the lock so a concurrent
    // detach() during shutdown can never race the native create calls below.
    ContextPtr context_handle;
    std::vector<std::shared_ptr<UbJfc>> jfcs;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return Status::InvalidArgument(
                "UB send Jetty pool is stopping; no new lease" LOC_MARK);
        }
        context_handle = context_handle_;
        jfcs = jfcs_;
    }
    if (!adapter_ || !context_handle || jfcs.empty()) {
        return Status::InvalidArgument(
            "UB send Jetty pool is not attached to a live context" LOC_MARK);
    }

    // Fail-closed (C02 / G-01): drained RESET members are never re-leased, so
    // every acquire creates fresh members outside the pool lock; JFCs rotate
    // round-robin.
    std::vector<std::pair<JettyPtr, size_t>> created;
    created.reserve(count);
    Status first_error = Status::OK();
    while (created.size() < count) {
        size_t jfc_index = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            jfc_index = next_jfc_rotation_++ % jfcs.size();
        }
        JettyPtr jetty;
        auto status = adapter_->createJetty(context_handle,
                                            jfcs[jfc_index]->handle(), options_,
                                            jetty);
        if (!status.ok()) {
            if (jetty) {
                // Provider contract: a failed createJetty may still return a
                // partially allocated handle. It was never posted, so a single
                // best-effort delete is the natural cleanup; it must never be
                // published or kept pending as a reusable member.
                (void)adapter_->deleteJetty(jetty);
            }
            if (first_error.ok()) first_error = std::move(status);
            break;
        }
        created.emplace_back(std::move(jetty), jfc_index);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++created_count_;
        }
    }

    // Re-check stopping after creation: a stop interleaved with an in-flight
    // create must never publish a member (G-02 "创建中 stop").
    bool stopping_now = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_now = stopping_;
    }
    if (!first_error.ok() || stopping_now) {
        // Atomic failure / stop handoff: every clean never-posted member taken
        // in this call is retired (deleted). A failed delete stays pending for
        // drain(); the caller adopts nothing.
        std::vector<std::pair<JettyPtr, size_t>> pending;
        for (auto& entry : created) {
            if (!entry.first) continue;
            auto status = adapter_->deleteJetty(entry.first);
            if (!status.ok()) {
                if (first_error.ok()) first_error = std::move(status);
                pending.push_back(std::move(entry));
            }
        }
        if (!pending.empty()) {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& entry : pending) retired_.push_back(std::move(entry));
        }
        if (first_error.ok()) {
            first_error = Status::InvalidArgument(
                "UB send Jetty pool is stopping; lease rolled back" LOC_MARK);
        }
        return first_error;
    }

    jetties.reserve(created.size());
    jfc_indices.reserve(created.size());
    for (auto& entry : created) {
        jetties.push_back(std::move(entry.first));
        jfc_indices.push_back(entry.second);
    }
    return Status::OK();
}

void UbSendJettyPool::release(std::vector<JettyPtr> jetties,
                              std::vector<size_t> jfc_indices) {
    if (jetties.size() != jfc_indices.size()) return;
    // Fail-closed (C02 / G-01): a used RESET member has no rearm proof, so it
    // is retired (deleted) rather than idled for reuse. A failed delete keeps
    // the member pending for drain(). No native call is held under the lock.
    std::vector<std::pair<JettyPtr, size_t>> pending;
    for (size_t index = 0; index < jetties.size(); ++index) {
        if (!jetties[index]) continue;
        auto status = adapter_->deleteJetty(jetties[index]);
        if (!status.ok()) {
            pending.emplace_back(std::move(jetties[index]), jfc_indices[index]);
        }
    }
    if (!pending.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& entry : pending) retired_.push_back(std::move(entry));
    }
}

void UbSendJettyPool::beginStop() {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
}

Status UbSendJettyPool::drain() {
    std::vector<std::pair<JettyPtr, size_t>> pending;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending = std::move(retired_);
        retired_.clear();
    }
    Status first_error = Status::OK();
    std::vector<std::pair<JettyPtr, size_t>> retained;
    for (auto& entry : pending) {
        if (!entry.first) continue;
        auto status = adapter_->deleteJetty(entry.first);
        if (!status.ok()) {
            // The adapter keeps the handle for a retry on the next drain.
            if (first_error.ok()) first_error = std::move(status);
            retained.push_back(std::move(entry));
        }
    }
    if (!retained.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& entry : retained) retired_.push_back(std::move(entry));
    }
    return first_error;
}

void UbSendJettyPool::detach() {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    context_handle_.reset();
    jfcs_.clear();
}

size_t UbSendJettyPool::idleCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return retired_.size();
}

size_t UbSendJettyPool::createdCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return created_count_;
}

}  // namespace mooncake::tent::ub