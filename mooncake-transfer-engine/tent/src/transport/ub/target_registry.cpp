// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/target_registry.h"

#include <algorithm>
#include <functional>
#include <optional>
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

// ---------------------------------------------------------------------------
// v3 typed keys and shared target registries (tent_rm edf6f9e8; tests only,
// production wiring pending).
// ---------------------------------------------------------------------------

namespace {

Status invalidRegistryArg(const std::string& reason) {
    return Status::InvalidArgument("invalid shared target registry request: " +
                                   reason);
}

Status validateProfile(const UbRemoteTargetProfile& profile,
                       const std::string& context) {
    if (profile.trans_mode != kUrmaTransportModeRm) {
        return invalidRegistryArg(context + " trans_mode must be URMA_TM_RM");
    }
    if (profile.target_type != kUrmaTargetTypeJetty) {
        return invalidRegistryArg(context + " target type must be URMA_JETTY");
    }
    if (profile.tp_type != kUrmaTpTypeCtp) {
        return invalidRegistryArg(context + " tp_type must be URMA_CTP");
    }
    return Status::OK();
}

size_t hashProfile(const UbRemoteTargetProfile& profile, size_t seed) {
    seed = ubHashCombineSeed(seed, std::hash<uint16_t>{}(profile.trans_mode));
    seed = ubHashCombineSeed(seed, std::hash<uint8_t>{}(profile.target_type));
    seed = ubHashCombineSeed(seed, std::hash<uint8_t>{}(profile.tp_type));
    return seed;
}

}  // namespace

size_t ubContextKeyHashValue(const UbContextKey& key) {
    size_t seed = std::hash<std::string>{}(key.device_identity);
    seed = ubHashCombineSeed(seed, std::hash<std::string>{}(key.eid));
    seed = ubHashCombineSeed(seed,
                             std::hash<uint64_t>{}(key.context_incarnation));
    return seed;
}

size_t ubPeerTargetKeyHashValue(const UbPeerTargetKey& key) {
    size_t seed = std::hash<uint64_t>{}(key.node_incarnation);
    seed = ubHashCombineSeed(seed, std::hash<std::string>{}(key.port_id));
    seed = ubHashCombineSeed(seed, std::hash<uint64_t>{}(key.target_epoch));
    return hashProfile(key.profile, seed);
}

size_t ubImportedTargetKeyHashValue(const UbImportedTargetKey& key) {
    size_t seed = ubContextKeyHashValue(key.context);
    seed = ubHashCombineSeed(seed, ubPeerTargetKeyHashValue(key.peer));
    seed = ubHashCombineSeed(seed, std::hash<std::string>{}(key.eid));
    seed = ubHashCombineSeed(seed, std::hash<uint32_t>{}(key.uasid));
    seed = ubHashCombineSeed(seed, std::hash<uint32_t>{}(key.id));
    return seed;
}

size_t ubImportScopeKeyHashValue(const UbImportScopeKey& key) {
    size_t seed = ubContextKeyHashValue(key.context);
    seed = ubHashCombineSeed(seed, std::hash<uint64_t>{}(key.node_incarnation));
    seed = ubHashCombineSeed(seed, std::hash<std::string>{}(key.port_id));
    return hashProfile(key.profile, seed);
}

Status validateUbContextKey(const UbContextKey& key) {
    if (key.device_identity.empty() || key.eid.empty()) {
        return invalidRegistryArg("context key identity is empty");
    }
    if (key.context_incarnation == 0) {
        return invalidRegistryArg("context incarnation must be nonzero");
    }
    return Status::OK();
}

Status validateUbPeerTargetKey(const UbPeerTargetKey& key) {
    if (key.node_incarnation == 0) {
        return invalidRegistryArg("peer node incarnation must be nonzero");
    }
    if (key.port_id.empty()) {
        return invalidRegistryArg("peer port id is empty");
    }
    if (key.target_epoch == 0) {
        return invalidRegistryArg("peer target epoch must be nonzero");
    }
    return validateProfile(key.profile, "peer target profile");
}

Status validateUbImportedTargetKey(const UbImportedTargetKey& key) {
    CHECK_STATUS(validateUbContextKey(key.context));
    CHECK_STATUS(validateUbPeerTargetKey(key.peer));
    if (key.eid.empty()) {
        return invalidRegistryArg("native target EID is empty");
    }
    if (key.id == 0) {
        return invalidRegistryArg("native target ID must be nonzero");
    }
    return Status::OK();
}

const char* ubTargetAcquireDispositionName(
    UbTargetAcquireDisposition disposition) noexcept {
    switch (disposition) {
        case UbTargetAcquireDisposition::kReady:
            return "ready";
        case UbTargetAcquireDisposition::kDeferred:
            return "deferred";
        case UbTargetAcquireDisposition::kStale:
            return "stale";
        case UbTargetAcquireDisposition::kConflict:
            return "conflict";
        case UbTargetAcquireDisposition::kCapacity:
            return "capacity";
    }
    return "unknown";
}

size_t ubTargetReferenceRoleIndex(UbTargetReferenceRole role) noexcept {
    switch (role) {
        case UbTargetReferenceRole::kEndpoint:
            return 0;
        case UbTargetReferenceRole::kBusiness:
            return 1;
        case UbTargetReferenceRole::kProbe:
            return 2;
    }
    return 0;
}

// ---------------------------------------------------------------------------
// UbImportedTargetRef
// ---------------------------------------------------------------------------

UbImportedTargetRef::UbImportedTargetRef(
    std::shared_ptr<UbImportedTargetRegistry> registry, RemoteTargetPtr target,
    const UbImportedTargetKey& key, UbTargetReferenceRole role)
    : registry_(std::move(registry)),
      target_(std::move(target)),
      key_(key),
      role_(role) {}

UbImportedTargetRef::UbImportedTargetRef(UbImportedTargetRef&& other) noexcept
    : registry_(std::move(other.registry_)),
      target_(std::move(other.target_)),
      key_(std::move(other.key_)),
      role_(other.role_) {
    other.role_ = UbTargetReferenceRole::kEndpoint;
}

UbImportedTargetRef& UbImportedTargetRef::operator=(
    UbImportedTargetRef&& other) noexcept {
    if (this != &other) {
        reset();
        registry_ = std::move(other.registry_);
        target_ = std::move(other.target_);
        key_ = std::move(other.key_);
        role_ = other.role_;
        other.role_ = UbTargetReferenceRole::kEndpoint;
    }
    return *this;
}

UbImportedTargetRef::~UbImportedTargetRef() { reset(); }

void UbImportedTargetRef::reset() noexcept {
    if (!target_) {
        registry_.reset();
        return;
    }
    // The registry is kept alive by the reference, so releaseReference is
    // always delivered to the live owner of the import bookkeeping.
    if (registry_) {
        registry_->releaseReference(key_, role_);
    }
    registry_.reset();
    target_.reset();
}

// ---------------------------------------------------------------------------
// UbImportedTargetRegistry
// ---------------------------------------------------------------------------

Status UbImportedTargetRegistry::create(
    std::shared_ptr<UbTargetImportBroker> broker,
    std::shared_ptr<UbImportedTargetRegistry>& out) {
    if (!broker) {
        return invalidRegistryArg("import broker must not be null");
    }
    out = std::shared_ptr<UbImportedTargetRegistry>(
        new UbImportedTargetRegistry(std::move(broker)));
    return Status::OK();
}

UbImportedTargetRegistry::UbImportedTargetRegistry(
    std::shared_ptr<UbTargetImportBroker> broker)
    : broker_(std::move(broker)) {}

Status UbImportedTargetRegistry::acquire(
    const UbImportedTargetKey& key, const std::string& target_descriptor,
    UbTargetReferenceRole role, UbImportedTargetRef& out,
    UbTargetAcquireDisposition& disposition) {
    CHECK_STATUS(validateUbImportedTargetKey(key));
    if (target_descriptor.empty()) {
        return invalidRegistryArg("target descriptor must not be empty");
    }

    const UbImportScopeKey scope{key.context, key.peer.node_incarnation,
                                 key.peer.port_id, key.peer.profile};
    std::unique_lock<std::mutex> lock(mutex_);

    const auto watermark_it = watermarks_.find(scope);
    if (watermark_it != watermarks_.end()) {
        if (key.peer.target_epoch < watermark_it->second) {
            // Older epochs never overwrite the current registry scope.
            disposition = UbTargetAcquireDisposition::kStale;
            return Status::OK();
        }
    }

    const auto entry_it = entries_.find(key);
    if (entry_it != entries_.end()) {
        Entry& entry = entry_it->second;
        if (entry.in_flight) {
            // Single flight: exactly one creator per ImportedTargetKey;
            // waiters defer non-blockingly and retry later.
            disposition = UbTargetAcquireDisposition::kDeferred;
            return Status::OK();
        }
        if (entry.retired) {
            // A revoked epoch is never re-referenced; the replacement
            // carries a newer epoch.
            disposition = UbTargetAcquireDisposition::kStale;
            return Status::OK();
        }
        if (entry.target_descriptor != target_descriptor) {
            // Same generation with different content: keep the healthy
            // incumbent and drop the conflicting candidate.
            disposition = UbTargetAcquireDisposition::kConflict;
            return Status::OK();
        }
        // Idempotent duplicate: share the existing import.
        ++entry.references;
        ++entry.role_references[ubTargetReferenceRoleIndex(role)];
        RemoteTargetPtr shared = entry.target;
        lock.unlock();
        out = UbImportedTargetRef(shared_from_this(), std::move(shared), key,
                                  role);
        disposition = UbTargetAcquireDisposition::kReady;
        return Status::OK();
    }

    if (watermark_it != watermarks_.end() &&
        key.peer.target_epoch == watermark_it->second) {
        // The published epoch exists with a different full key and was
        // retired/reclaimed already; same-epoch re-creation is a conflict
        // against the incumbent scope.
        disposition = UbTargetAcquireDisposition::kStale;
        return Status::OK();
    }

    // Serialize per-scope creation: if any candidate for the same scope is
    // already in flight, defer instead of racing a second creator.
    for (const auto& [entry_key, entry] : entries_) {
        if (entry.in_flight && entry_key.peer.node_incarnation == scope.node_incarnation &&
            entry_key.peer.port_id == scope.port_id &&
            entry_key.peer.profile == scope.profile &&
            entry_key.context == scope.context) {
            disposition = UbTargetAcquireDisposition::kDeferred;
            return Status::OK();
        }
    }

    // New owner candidate: mark in-flight under the lock, import outside.
    Entry& entry = entries_[key];
    entry.key = key;
    entry.target_descriptor = target_descriptor;
    entry.in_flight = true;
    lock.unlock();

    RemoteTargetPtr imported;
    const Status import_status =
        broker_->importTarget(key, target_descriptor, imported);

    lock.lock();
    // Only the creator mutates an in-flight entry, so it is still present.
    auto it = entries_.find(key);
    if (!import_status.ok()) {
        if (it != entries_.end()) {
            entries_.erase(it);
        }
        ++failed_imports_;
        // Import failure never advances the watermark; the caller may retry.
        return import_status;
    }
    Entry& published = it->second;
    published.in_flight = false;
    published.target = imported;
    published.references = 1;
    published.role_references[ubTargetReferenceRoleIndex(role)] = 1;
    ++successful_imports_;

    // Publish linearization point: the watermark advances only here, after
    // the native import succeeded and under the registry lock.
    uint64_t& watermark = watermarks_[scope];
    if (key.peer.target_epoch > watermark) {
        watermark = key.peer.target_epoch;
    }
    // Newer owner committed: retire older imports of the same scope and
    // unimport the ones whose references already dropped.
    std::vector<ReclaimWork> work = supersedeLocked(scope, key.peer.target_epoch);
    lock.unlock();

    reclaimOutsideLock(std::move(work));
    out = UbImportedTargetRef(shared_from_this(), imported, key, role);
    disposition = UbTargetAcquireDisposition::kReady;
    return Status::OK();
}

Status UbImportedTargetRegistry::retireImport(const UbImportedTargetKey& key) {
    CHECK_STATUS(validateUbImportedTargetKey(key));
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return invalidRegistryArg("unknown imported target key");
    }
    Entry& entry = it->second;
    if (entry.in_flight) {
        return invalidRegistryArg("cannot retire an in-flight import");
    }
    if (entry.retired) return Status::OK();  // idempotent
    entry.retired = true;
    if (entry.references > 0) return Status::OK();
    std::vector<ReclaimWork> pending;
    pending.push_back(ReclaimWork{entry.target});
    entries_.erase(it);
    lock.unlock();
    reclaimOutsideLock(std::move(pending));
    return Status::OK();
}

UbImportedTargetRegistrySnapshot UbImportedTargetRegistry::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    UbImportedTargetRegistrySnapshot snapshot;
    snapshot.entries.reserve(entries_.size());
    for (const auto& [key, entry] : entries_) {
        UbImportedTargetEntrySnapshot entry_snapshot;
        entry_snapshot.key = key;
        entry_snapshot.references = entry.references;
        entry_snapshot.endpoint_references = entry.role_references[0];
        entry_snapshot.business_references = entry.role_references[1];
        entry_snapshot.probe_references = entry.role_references[2];
        entry_snapshot.in_flight = entry.in_flight;
        entry_snapshot.retired = entry.retired;
        snapshot.entries.push_back(std::move(entry_snapshot));
    }
    std::sort(snapshot.entries.begin(), snapshot.entries.end(),
              [](const UbImportedTargetEntrySnapshot& lhs,
                 const UbImportedTargetEntrySnapshot& rhs) {
                  return ubImportedTargetKeyHashValue(lhs.key) <
                         ubImportedTargetKeyHashValue(rhs.key);
              });
    for (const auto& [scope, epoch] : watermarks_) {
        snapshot.watermarks.push_back(UbImportScopeWatermarkSnapshot{scope, epoch});
    }
    std::sort(snapshot.watermarks.begin(), snapshot.watermarks.end(),
              [](const UbImportScopeWatermarkSnapshot& lhs,
                 const UbImportScopeWatermarkSnapshot& rhs) {
                  return ubImportScopeKeyHashValue(lhs.scope) <
                         ubImportScopeKeyHashValue(rhs.scope);
              });
    snapshot.successful_imports = successful_imports_;
    snapshot.failed_imports = failed_imports_;
    snapshot.successful_unimports = successful_unimports_;
    return snapshot;
}

void UbImportedTargetRegistry::releaseReference(
    const UbImportedTargetKey& key, UbTargetReferenceRole role) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) return;
    Entry& entry = it->second;
    if (entry.references > 0) {
        --entry.references;
    }
    const size_t role_index = ubTargetReferenceRoleIndex(role);
    if (entry.role_references[role_index] > 0) {
        --entry.role_references[role_index];
    }
    // Endpoint elimination only releases references; the shared import is
    // reclaimed once a retired entry reaches zero references.
    if (entry.references > 0 || !entry.retired) return;
    std::vector<ReclaimWork> pending;
    pending.push_back(ReclaimWork{entry.target});
    entries_.erase(it);
    lock.unlock();
    reclaimOutsideLock(std::move(pending));
}

std::vector<UbImportedTargetRegistry::ReclaimWork>
UbImportedTargetRegistry::supersedeLocked(const UbImportScopeKey& scope,
                                          uint64_t new_epoch) {
    std::vector<ReclaimWork> work;
    for (auto it = entries_.begin(); it != entries_.end();) {
        Entry& entry = it->second;
        const UbImportedTargetKey& entry_key = entry.key;
        const bool same_scope =
            entry_key.context == scope.context &&
            entry_key.peer.node_incarnation == scope.node_incarnation &&
            entry_key.peer.port_id == scope.port_id &&
            entry_key.peer.profile == scope.profile;
        if (!same_scope || entry_key.peer.target_epoch >= new_epoch ||
            entry.retired) {
            ++it;
            continue;
        }
        // The newer owner has committed: revoke the old admission now.
        entry.retired = true;
        if (entry.references == 0) {
            work.push_back(ReclaimWork{entry.target});
            it = entries_.erase(it);
            continue;
        }
        ++it;
    }
    return work;
}

void UbImportedTargetRegistry::reclaimOutsideLock(
    std::vector<ReclaimWork> work) {
    for (auto& item : work) {
        if (!item.target) continue;
        if (broker_->unimportTarget(item.target).ok()) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++successful_unimports_;
        }
        // A failed unimport keeps the handle with the broker; the registry
        // no longer tracks it. Retrying unimport is the broker's recovery
        // responsibility (T-14 recovery owner).
    }
}

// ---------------------------------------------------------------------------
// UbLocalPortTargetRef
// ---------------------------------------------------------------------------

UbLocalPortTargetRef::UbLocalPortTargetRef(
    std::shared_ptr<UbLocalPortTargetRegistry> registry, JettyPtr jetty,
    const std::string& port_id, uint64_t target_epoch,
    UbTargetReferenceRole role)
    : registry_(std::move(registry)),
      jetty_(std::move(jetty)),
      port_id_(port_id),
      target_epoch_(target_epoch),
      role_(role) {}

UbLocalPortTargetRef::UbLocalPortTargetRef(UbLocalPortTargetRef&& other) noexcept
    : registry_(std::move(other.registry_)),
      jetty_(std::move(other.jetty_)),
      port_id_(std::move(other.port_id_)),
      target_epoch_(other.target_epoch_),
      role_(other.role_) {
    other.target_epoch_ = 0;
    other.role_ = UbTargetReferenceRole::kEndpoint;
}

UbLocalPortTargetRef& UbLocalPortTargetRef::operator=(
    UbLocalPortTargetRef&& other) noexcept {
    if (this != &other) {
        reset();
        registry_ = std::move(other.registry_);
        jetty_ = std::move(other.jetty_);
        port_id_ = std::move(other.port_id_);
        target_epoch_ = other.target_epoch_;
        role_ = other.role_;
        other.target_epoch_ = 0;
        other.role_ = UbTargetReferenceRole::kEndpoint;
    }
    return *this;
}

UbLocalPortTargetRef::~UbLocalPortTargetRef() { reset(); }

void UbLocalPortTargetRef::reset() noexcept {
    if (!jetty_) {
        registry_.reset();
        return;
    }
    if (registry_) {
        registry_->releaseReference(port_id_, target_epoch_, role_);
    }
    registry_.reset();
    jetty_.reset();
}

// ---------------------------------------------------------------------------
// UbLocalPortTargetRegistry
// ---------------------------------------------------------------------------

Status validateUbLocalPortTargetDesc(const UbLocalPortTargetDesc& desc) {
    if (desc.port_id.empty()) {
        return invalidRegistryArg("configured port id is empty");
    }
    if (desc.target_epoch == 0) {
        return invalidRegistryArg("local target epoch must be nonzero");
    }
    if (desc.target_identity.empty()) {
        return invalidRegistryArg("local target identity is empty");
    }
    return validateProfile(desc.profile, "local target profile");
}

Status UbLocalPortTargetRegistry::create(
    std::shared_ptr<NodeTargetQuota> quota,
    std::shared_ptr<UbLocalPortTargetFactory> factory,
    std::shared_ptr<UbLocalPortTargetRegistry>& out) {
    if (!quota) {
        return invalidRegistryArg("node target quota must not be null");
    }
    if (!factory) {
        return invalidRegistryArg("local target factory must not be null");
    }
    out = std::shared_ptr<UbLocalPortTargetRegistry>(
        new UbLocalPortTargetRegistry(std::move(quota), std::move(factory)));
    return Status::OK();
}

UbLocalPortTargetRegistry::UbLocalPortTargetRegistry(
    std::shared_ptr<NodeTargetQuota> quota,
    std::shared_ptr<UbLocalPortTargetFactory> factory)
    : quota_(std::move(quota)), factory_(std::move(factory)) {}

Status UbLocalPortTargetRegistry::acquire(
    const UbLocalPortTargetDesc& desc, UbTargetReferenceRole role,
    UbLocalPortTargetRef& out, UbTargetAcquireDisposition& disposition) {
    CHECK_STATUS(validateUbLocalPortTargetDesc(desc));

    std::unique_lock<std::mutex> lock(mutex_);

    const auto watermark_it = watermarks_.find(desc.port_id);
    if (watermark_it != watermarks_.end() &&
        desc.target_epoch < watermark_it->second) {
        disposition = UbTargetAcquireDisposition::kStale;
        return Status::OK();
    }

    const PortEpochKey port_epoch{desc.port_id, desc.target_epoch};
    const auto entry_it = entries_.find(port_epoch);
    if (entry_it != entries_.end()) {
        Entry& entry = entry_it->second;
        if (entry.in_flight) {
            disposition = UbTargetAcquireDisposition::kDeferred;
            return Status::OK();
        }
        if (entry.retired) {
            disposition = UbTargetAcquireDisposition::kStale;
            return Status::OK();
        }
        if (entry.desc.target_identity != desc.target_identity ||
            !(entry.desc.profile == desc.profile)) {
            disposition = UbTargetAcquireDisposition::kConflict;
            return Status::OK();
        }
        // Existing shared target: grant the reference without touching the
        // node quota, so a shared handshake is never rejected because the
        // quota is exhausted.
        ++entry.references;
        ++entry.role_references[ubTargetReferenceRoleIndex(role)];
        JettyPtr jetty = entry.jetty;
        lock.unlock();
        out = UbLocalPortTargetRef(shared_from_this(), std::move(jetty),
                                   desc.port_id, desc.target_epoch, role);
        disposition = UbTargetAcquireDisposition::kReady;
        return Status::OK();
    }

    if (watermark_it != watermarks_.end() &&
        desc.target_epoch == watermark_it->second) {
        // The published epoch was retired and reclaimed; the same epoch is
        // never re-created (new owner = new epoch).
        disposition = UbTargetAcquireDisposition::kStale;
        return Status::OK();
    }

    // Serialize per-port creation: one creator at a time.
    for (const auto& [key, entry] : entries_) {
        if (key.port_id == desc.port_id && entry.in_flight) {
            disposition = UbTargetAcquireDisposition::kDeferred;
            return Status::OK();
        }
    }

    // Every initial creation and every epoch replacement reserves one
    // ticket from the node-authoritative quota. A full quota defers with a
    // capacity classification and never retires a healthy incumbent.
    std::optional<TargetQuotaTicket> ticket = quota_->tryReserve();
    if (!ticket.has_value()) {
        disposition = UbTargetAcquireDisposition::kCapacity;
        return Status::OK();
    }

    Entry& entry = entries_[port_epoch];
    entry.desc = desc;
    entry.ticket = std::move(*ticket);
    entry.in_flight = true;
    lock.unlock();

    JettyPtr jetty;
    const Status create_status = factory_->createTarget(desc, jetty);

    lock.lock();
    auto it = entries_.find(port_epoch);
    if (!create_status.ok()) {
        if (it != entries_.end()) {
            // Proven-never-allocated rollback path: creation failed before
            // any native handle was published.
            Status release_status =
                quota_->releaseUnallocated(it->second.ticket);
            if (!release_status.ok()) {
                // The ticket keeps its slot charged; quarantine instead of
                // double-releasing quota.
                quota_->transition(it->second.ticket,
                                   TargetObjectState::kQuarantinedUnproven);
            }
            entries_.erase(it);
        }
        ++failed_creations_;
        // Creation failure never advances the port watermark.
        return create_status;
    }

    Entry& published = it->second;
    published.in_flight = false;
    published.jetty = jetty;
    published.references = 1;
    published.role_references[ubTargetReferenceRoleIndex(role)] = 1;
    ++successful_creations_;
    quota_->transition(published.ticket, TargetObjectState::kActive);

    uint64_t& watermark = watermarks_[desc.port_id];
    if (desc.target_epoch > watermark) {
        watermark = desc.target_epoch;
    }
    std::vector<ReclaimWork> work = supersedeLocked(desc.port_id, desc.target_epoch);
    lock.unlock();

    reclaimOutsideLock(std::move(work));
    out = UbLocalPortTargetRef(shared_from_this(), jetty, desc.port_id,
                               desc.target_epoch, role);
    disposition = UbTargetAcquireDisposition::kReady;
    return Status::OK();
}

Status UbLocalPortTargetRegistry::retirePortTarget(const std::string& port_id,
                                                   uint64_t target_epoch) {
    if (port_id.empty() || target_epoch == 0) {
        return invalidRegistryArg("port id and target epoch must be non-empty");
    }
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = entries_.find(PortEpochKey{port_id, target_epoch});
    if (it == entries_.end()) {
        return invalidRegistryArg("unknown port target generation");
    }
    Entry& entry = it->second;
    if (entry.in_flight) {
        return invalidRegistryArg("cannot retire an in-flight creation");
    }
    if (entry.retired) return Status::OK();  // idempotent
    entry.retired = true;
    if (entry.references > 0) return Status::OK();
    std::vector<ReclaimWork> pending;
    pending.push_back(ReclaimWork{entry.jetty, std::move(entry.ticket)});
    entries_.erase(it);
    lock.unlock();
    reclaimOutsideLock(std::move(pending));
    return Status::OK();
}

UbLocalPortTargetRegistrySnapshot UbLocalPortTargetRegistry::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    UbLocalPortTargetRegistrySnapshot snapshot;
    snapshot.entries.reserve(entries_.size());
    for (const auto& [key, entry] : entries_) {
        UbLocalPortTargetEntrySnapshot entry_snapshot;
        entry_snapshot.port_id = key.port_id;
        entry_snapshot.target_epoch = key.target_epoch;
        entry_snapshot.target_identity = entry.desc.target_identity;
        entry_snapshot.profile = entry.desc.profile;
        entry_snapshot.references = entry.references;
        entry_snapshot.endpoint_references = entry.role_references[0];
        entry_snapshot.business_references = entry.role_references[1];
        entry_snapshot.probe_references = entry.role_references[2];
        entry_snapshot.in_flight = entry.in_flight;
        entry_snapshot.retired = entry.retired;
        snapshot.entries.push_back(std::move(entry_snapshot));
    }
    std::sort(snapshot.entries.begin(), snapshot.entries.end(),
              [](const UbLocalPortTargetEntrySnapshot& lhs,
                 const UbLocalPortTargetEntrySnapshot& rhs) {
                  if (lhs.port_id != rhs.port_id) return lhs.port_id < rhs.port_id;
                  return lhs.target_epoch < rhs.target_epoch;
              });
    snapshot.port_watermarks = {watermarks_.begin(), watermarks_.end()};
    snapshot.successful_creations = successful_creations_;
    snapshot.failed_creations = failed_creations_;
    snapshot.proven_destructions = proven_destructions_;
    snapshot.quarantined_destructions = quarantined_destructions_;
    return snapshot;
}

void UbLocalPortTargetRegistry::releaseReference(
    const std::string& port_id, uint64_t target_epoch,
    UbTargetReferenceRole role) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = entries_.find(PortEpochKey{port_id, target_epoch});
    if (it == entries_.end()) return;
    Entry& entry = it->second;
    if (entry.references > 0) {
        --entry.references;
    }
    const size_t role_index = ubTargetReferenceRoleIndex(role);
    if (entry.role_references[role_index] > 0) {
        --entry.role_references[role_index];
    }
    if (entry.references > 0 || !entry.retired) return;
    std::vector<ReclaimWork> pending;
    pending.push_back(ReclaimWork{entry.jetty, std::move(entry.ticket)});
    entries_.erase(it);
    lock.unlock();
    reclaimOutsideLock(std::move(pending));
}

std::vector<UbLocalPortTargetRegistry::ReclaimWork>
UbLocalPortTargetRegistry::supersedeLocked(const std::string& port_id,
                                           uint64_t new_epoch) {
    std::vector<ReclaimWork> work;
    for (auto it = entries_.begin(); it != entries_.end();) {
        Entry& entry = it->second;
        if (it->first.port_id != port_id ||
            it->first.target_epoch >= new_epoch || entry.retired) {
            ++it;
            continue;
        }
        entry.retired = true;
        if (entry.references == 0) {
            work.push_back(ReclaimWork{entry.jetty, std::move(entry.ticket)});
            it = entries_.erase(it);
            continue;
        }
        ++it;
    }
    return work;
}

void UbLocalPortTargetRegistry::reclaimOutsideLock(
    std::vector<ReclaimWork> work) {
    for (auto& item : work) {
        if (!item.jetty) continue;
        bool destruction_proven = false;
        const Status destroy_status =
            factory_->destroyTarget(item.jetty, destruction_proven);
        if (destroy_status.ok() && destruction_proven) {
            quota_->transition(item.ticket,
                               TargetObjectState::kRetiringDeleting);
            if (quota_
                    ->releaseAfterProvenDestruction(item.ticket, true)
                    .ok()) {
                std::lock_guard<std::mutex> lock(mutex_);
                ++proven_destructions_;
                continue;
            }
        }
        // Unproven or failed destruction keeps the quota slot charged
        // (quarantine); the recovery owner (T-14) retries later.
        quota_->transition(item.ticket,
                           TargetObjectState::kQuarantinedUnproven);
        std::lock_guard<std::mutex> lock(mutex_);
        ++quarantined_destructions_;
    }
}

}  // namespace mooncake::tent::ub

