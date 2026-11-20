// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Per-Context imported-target registry (design §5.1 / §10.2).
//
// A remote RM target is a node-wide, per-port resource: several endpoint
// incarnations, business attempts and probes may reference the same imported
// target. Importing it once per reference would multiply native handles and
// make unimport order impossible to reason about. This registry owns the
// native import per (peer scope, target identity, target epoch) inside one
// local Context, reference-counts the users and unimports only on the last
// release. A failed unimport is retained (quarantined) here instead of being
// dropped: native destruction stays unproven until the provider accepts it.

#ifndef TENT_TRANSPORT_UB_TARGET_REGISTRY_H_
#define TENT_TRANSPORT_UB_TARGET_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/bootstrap.h"
#include "tent/transport/ub/bootstrap_types.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

// Canonical physical device key used by the native budget, the bootstrap v2
// identity and the pool. Kept here so every caller derives exactly one key.
[[nodiscard]] std::string physicalDeviceKey(const DeviceInfo& device);

class ImportedTargetRegistry {
   public:
    ImportedTargetRegistry(std::shared_ptr<UrmaAdapter> adapter,
                           ContextPtr context);

    ImportedTargetRegistry(const ImportedTargetRegistry&) = delete;
    ImportedTargetRegistry& operator=(const ImportedTargetRegistry&) = delete;

    // Imports every descriptor of one peer, deduplicating by
    // (peer_scope, target_identity, target_epoch). `out` receives one handle
    // per descriptor, positionally, so a caller can pair them with Jettys.
    // On failure the call rolls back the references it acquired and returns
    // the first error; partial native handles are adopted and quarantined.
    Status importAll(const std::string& peer_scope,
                     const std::vector<UbRmTargetDescriptor>& descriptors,
                     std::vector<RemoteTargetPtr>& out);

    // Releases the handles acquired by importAll (one release per handle). The
    // last release unimports the native target; a failed unimport is retained
    // by the registry and reported through retainedCount().
    Status releaseAll(const std::vector<RemoteTargetPtr>& handles);

    // Retries retained (failed) unimports. Called at context shutdown.
    Status drainRetained();

    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t references() const;
    [[nodiscard]] size_t retainedCount() const;

   private:
    struct Entry {
        std::string identity;
        uint64_t epoch{0};
        RemoteTargetPtr target;
        size_t refs{0};
    };

    Status importLocked(const std::string& key,
                        const UbRmTargetDescriptor& descriptor);

    mutable std::mutex mutex_;
    const std::shared_ptr<UrmaAdapter> adapter_;
    const ContextPtr context_;
    std::map<std::string, Entry> entries_;
    std::vector<RemoteTargetPtr> retained_;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_TARGET_REGISTRY_H_
