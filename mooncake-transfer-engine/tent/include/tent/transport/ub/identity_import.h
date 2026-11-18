// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3g-d: physical port identity import.
//
// A URMA physical port is addressed by `(native_device_path, eid)`. The same
// port can be described with a trailing slash or a differently-cased EID
// depending on whether the value came from discovery, segment metadata or an
// operator filter. The budget ledger keys native reservations by this identity,
// so two spellings of one port must never produce two ledgers (which would
// double the device's Jetty budget after a restart or an alias change).
//
// Design: docs/analyze/tent-ub-w3g-pool-claim-and-v2-design.md §5.3 / §8.

#ifndef TENT_TRANSPORT_UB_IDENTITY_IMPORT_H_
#define TENT_TRANSPORT_UB_IDENTITY_IMPORT_H_

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "tent/common/status.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

// Canonical identity of one physical port. `device_key` is the stable ledger
// key: `path + ":" + eid`, both normalized. An invalid identity has an empty
// device_key and must never be imported (fail-closed: no guessing).
struct PhysicalDeviceIdentity {
    std::string native_device_path;
    std::string eid;
    std::string device_key;

    [[nodiscard]] bool valid() const noexcept { return !device_key.empty(); }
};

// Pure normalization. Path: trimmed, duplicated/trailing separators removed.
// EID: trimmed and lower-cased. An empty component yields an invalid identity
// (empty device_key), never a synthesized one.
[[nodiscard]] PhysicalDeviceIdentity makePhysicalDeviceIdentity(
    const std::string& native_device_path, const std::string& eid);

// Identity cache used by the pooled assembly. Keyed by the normalized
// device_key, so re-importing the same port is idempotent and two spellings
// collapse to one entry. The counters are observation only.
class PhysicalDeviceIdentityCache {
   public:
    // Returns InvalidArgument for an empty path/EID. Re-importing an existing
    // key succeeds and returns the cached identity.
    Status import(const std::string& native_device_path, const std::string& eid,
                  PhysicalDeviceIdentity& output);
    Status import(const DeviceInfo& device, PhysicalDeviceIdentity& output);

    [[nodiscard]] size_t size() const;
    [[nodiscard]] uint64_t importOk() const;
    [[nodiscard]] uint64_t importFailed() const;

   private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, PhysicalDeviceIdentity> by_key_;
    uint64_t import_ok_{0};
    uint64_t import_failed_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_IDENTITY_IMPORT_H_
