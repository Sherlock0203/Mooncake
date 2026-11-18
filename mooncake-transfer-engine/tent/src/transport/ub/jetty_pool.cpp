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
    if (!adapter_ || !context_handle_ || jfcs_.empty()) {
        return Status::InvalidArgument(
            "UB send Jetty pool is not attached to a live context" LOC_MARK);
    }

    std::vector<std::pair<JettyPtr, size_t>> leased;
    leased.reserve(count);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!idle_.empty() && leased.size() < count) {
            leased.push_back(std::move(idle_.back()));
            idle_.pop_back();
        }
    }

    // Create the remainder outside the pool lock; JFCs rotate round-robin.
    Status first_error = Status::OK();
    while (leased.size() < count) {
        size_t jfc_index = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            jfc_index = next_jfc_rotation_++ % jfcs_.size();
        }
        JettyPtr jetty;
        auto status = adapter_->createJetty(context_handle_,
                                            jfcs_[jfc_index]->handle(), options_,
                                            jetty);
        if (!status.ok()) {
            if (jetty) {
                // Provider contract: a failed createJetty may still return a
                // partially allocated handle. It was never posted, so a
                // single best-effort delete is the natural cleanup; a
                // retained handle falls back to the adapter wrapper's own
                // destructor semantics. It must never enter the idle set.
                (void)adapter_->deleteJetty(jetty);
            }
            if (first_error.ok()) first_error = std::move(status);
            break;
        }
        leased.emplace_back(std::move(jetty), jfc_index);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++created_count_;
        }
    }

    if (!first_error.ok()) {
        // Atomic failure: everything taken in this call is clean (never
        // posted) and goes back to the idle set; the caller adopts nothing.
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& entry : leased) {
            idle_.push_back(std::move(entry));
        }
        return first_error;
    }

    jetties.reserve(leased.size());
    jfc_indices.reserve(leased.size());
    for (auto& entry : leased) {
        jetties.push_back(std::move(entry.first));
        jfc_indices.push_back(entry.second);
    }
    return Status::OK();
}

void UbSendJettyPool::release(std::vector<JettyPtr> jetties,
                              std::vector<size_t> jfc_indices) {
    if (jetties.size() != jfc_indices.size()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t index = 0; index < jetties.size(); ++index) {
        if (jetties[index]) {
            idle_.emplace_back(std::move(jetties[index]), jfc_indices[index]);
        }
    }
}

Status UbSendJettyPool::drain() {
    std::vector<std::pair<JettyPtr, size_t>> drained;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        drained = std::move(idle_);
        idle_.clear();
    }
    Status first_error = Status::OK();
    std::vector<std::pair<JettyPtr, size_t>> retained;
    for (auto& entry : drained) {
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
        for (auto& entry : retained) {
            idle_.push_back(std::move(entry));
        }
    }
    return first_error;
}

size_t UbSendJettyPool::idleCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return idle_.size();
}

size_t UbSendJettyPool::createdCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return created_count_;
}

}  // namespace mooncake::tent::ub
