// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/identity_import.h"

#include <algorithm>
#include <cctype>
#include <utility>

namespace mooncake::tent::ub {
namespace {

std::string trimCopy(const std::string& value) {
    const auto first = std::find_if_not(value.begin(), value.end(), [](int c) {
        return std::isspace(static_cast<unsigned char>(c)) != 0;
    });
    const auto last =
        std::find_if_not(value.rbegin(), value.rend(), [](int c) {
            return std::isspace(static_cast<unsigned char>(c)) != 0;
        }).base();
    if (first >= last) return {};
    return std::string(first, last);
}

// Removes duplicated and trailing path separators. The root "/" is preserved.
std::string normalizePath(const std::string& value) {
    std::string path = trimCopy(value);
    if (path.empty()) return {};
    std::string normalized;
    normalized.reserve(path.size());
    for (char c : path) {
        if (c == '/' && !normalized.empty() && normalized.back() == '/') {
            continue;
        }
        normalized.push_back(c);
    }
    while (normalized.size() > 1 && normalized.back() == '/') {
        normalized.pop_back();
    }
    return normalized;
}

std::string normalizeEid(const std::string& value) {
    std::string eid = trimCopy(value);
    std::transform(eid.begin(), eid.end(), eid.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return eid;
}

}  // namespace

PhysicalDeviceIdentity makePhysicalDeviceIdentity(
    const std::string& native_device_path, const std::string& eid) {
    PhysicalDeviceIdentity identity;
    identity.native_device_path = normalizePath(native_device_path);
    identity.eid = normalizeEid(eid);
    if (identity.native_device_path.empty() || identity.eid.empty()) {
        // Fail closed: an empty component is an assembly error, and a guessed
        // key would silently merge two ports into one budget ledger.
        return identity;
    }
    identity.device_key = identity.native_device_path + ":" + identity.eid;
    return identity;
}

Status PhysicalDeviceIdentityCache::import(
    const std::string& native_device_path, const std::string& eid,
    PhysicalDeviceIdentity& output) {
    PhysicalDeviceIdentity identity =
        makePhysicalDeviceIdentity(native_device_path, eid);
    if (!identity.valid()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++import_failed_;
        return Status::InvalidArgument(
            "UB physical device identity requires a non-empty path and EID");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto [it, inserted] = by_key_.emplace(identity.device_key, identity);
    ++import_ok_;
    // Re-importing the same normalized key is idempotent. The cached value is
    // returned so every caller shares one canonical spelling.
    output = it->second;
    return Status::OK();
}

Status PhysicalDeviceIdentityCache::import(const DeviceInfo& device,
                                           PhysicalDeviceIdentity& output) {
    return import(device.native_device_path, device.eid, output);
}

size_t PhysicalDeviceIdentityCache::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return by_key_.size();
}

uint64_t PhysicalDeviceIdentityCache::importOk() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return import_ok_;
}

uint64_t PhysicalDeviceIdentityCache::importFailed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return import_failed_;
}

}  // namespace mooncake::tent::ub
