// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/target_registry.h"

#include <utility>

#include <glog/logging.h>

namespace mooncake::tent::ub {

std::string physicalDeviceKey(const DeviceInfo& device) {
    return makePhysicalDeviceIdentity(device.native_device_path, device.eid);
}

ImportedTargetRegistry::ImportedTargetRegistry(
    std::shared_ptr<UrmaAdapter> adapter, ContextPtr context)
    : adapter_(std::move(adapter)), context_(std::move(context)) {}

namespace {

std::string targetKey(const std::string& peer_scope,
                      const UbRmTargetDescriptor& descriptor) {
    return peer_scope + '\x1f' + descriptor.target_identity + '\x1f' +
           std::to_string(descriptor.target_epoch);
}

}  // namespace

Status ImportedTargetRegistry::importLocked(
    const std::string& key, const UbRmTargetDescriptor& descriptor) {
    RemoteTargetPtr handle;
    auto status = adapter_->importRemoteTarget(
        context_, descriptor.eid, descriptor.uasid, descriptor.id,
        descriptor.profile.trans_mode, descriptor.profile.target_type,
        descriptor.profile.tp_type, handle);
    if (!status.ok()) {
        // A provider may hand back a partially allocated handle together with
        // the error. Adopt it and try one unimport; a failure keeps it
        // quarantined here, because dropping it would lose native ownership.
        if (handle) {
            auto released = adapter_->unimportRemoteTarget(handle);
            if (!released.ok()) {
                retained_.push_back(std::move(handle));
            }
        }
        return status;
    }
    Entry entry;
    entry.identity = descriptor.target_identity;
    entry.epoch = descriptor.target_epoch;
    entry.target = std::move(handle);
    entry.refs = 1;
    entries_.emplace(key, std::move(entry));
    return Status::OK();
}

Status ImportedTargetRegistry::importAll(
    const std::string& peer_scope,
    const std::vector<UbRmTargetDescriptor>& descriptors,
    std::vector<RemoteTargetPtr>& out) {
    out.clear();
    out.reserve(descriptors.size());
    std::lock_guard<std::mutex> lock(mutex_);
    Status first_error = Status::OK();
    size_t acquired = 0;
    for (const auto& descriptor : descriptors) {
        const std::string key = targetKey(peer_scope, descriptor);
        auto existing = entries_.find(key);
        if (existing != entries_.end()) {
            // Shared target: one native import, one more reference.
            ++existing->second.refs;
            out.push_back(existing->second.target);
            ++acquired;
            continue;
        }
        const Status status = importLocked(key, descriptor);
        if (!status.ok()) {
            // Keep importing the remaining descriptors: a failed provider call
            // is not a reason to abandon handles that could still be adopted.
            // The whole attempt rolls back below, so nothing leaks.
            if (first_error.ok()) first_error = status;
            continue;
        }
        out.push_back(entries_.at(key).target);
        ++acquired;
    }
    if (first_error.ok()) return Status::OK();

    // Roll back the references this call acquired, keeping the registry at its
    // previous state. Handles that fail to unimport stay retained, not lost.
    for (size_t index = acquired; index > 0; --index) {
        auto& handle = out[index - 1];
        for (auto it = entries_.begin(); it != entries_.end(); ++it) {
            if (it->second.target != handle) continue;
            if (--it->second.refs == 0) {
                auto released =
                    adapter_->unimportRemoteTarget(it->second.target);
                if (!released.ok()) {
                    retained_.push_back(it->second.target);
                }
                entries_.erase(it);
            }
            break;
        }
    }
    out.clear();
    return first_error;
}

Status ImportedTargetRegistry::releaseAll(
    const std::vector<RemoteTargetPtr>& handles) {
    std::lock_guard<std::mutex> lock(mutex_);
    Status first_error = Status::OK();
    for (const auto& handle : handles) {
        if (!handle) continue;
        auto entry = entries_.begin();
        for (; entry != entries_.end(); ++entry) {
            if (entry->second.target == handle) break;
        }
        if (entry == entries_.end()) continue;
        if (entry->second.refs > 0) --entry->second.refs;
        if (entry->second.refs != 0) continue;
        auto target = std::move(entry->second.target);
        entries_.erase(entry);
        const Status status = adapter_->unimportRemoteTarget(target);
        if (!status.ok()) {
            if (first_error.ok()) first_error = status;
            // Never drop a failed unimport: the native ownership is unproven.
            retained_.push_back(std::move(target));
        }
    }
    return first_error;
}

Status ImportedTargetRegistry::drainRetained() {
    std::lock_guard<std::mutex> lock(mutex_);
    Status first_error = Status::OK();
    std::vector<RemoteTargetPtr> still_retained;
    for (auto& target : retained_) {
        const Status status = adapter_->unimportRemoteTarget(target);
        if (!status.ok()) {
            if (first_error.ok()) first_error = status;
            still_retained.push_back(std::move(target));
        }
    }
    retained_ = std::move(still_retained);
    return first_error;
}

size_t ImportedTargetRegistry::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

size_t ImportedTargetRegistry::references() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total = 0;
    for (const auto& [_, entry] : entries_) total += entry.refs;
    return total;
}

size_t ImportedTargetRegistry::retainedCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return retained_.size();
}

}  // namespace mooncake::tent::ub
