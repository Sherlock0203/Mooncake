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
//
// The sections below the classic registry add the v3 typed-key shared target
// registries (send-side UbImportedTargetRegistry and receive-side
// UbLocalPortTargetRegistry, from tent_rm edf6f9e8). They are currently
// exercised by tests only; production wiring is pending.

#ifndef TENT_TRANSPORT_UB_TARGET_REGISTRY_H_
#define TENT_TRANSPORT_UB_TARGET_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/bootstrap.h"
#include "tent/transport/ub/bootstrap_types.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/node_target_quota.h"
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

// ---------------------------------------------------------------------------
// v3 typed keys and shared target registries (tent_rm edf6f9e8; tests only,
// production wiring pending).
// ---------------------------------------------------------------------------

// Order-stable hash combiner for typed keys (guidance 3.3).
[[nodiscard]] inline size_t ubHashCombineSeed(size_t seed, size_t value) {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    return seed;
}

// ---------------------------------------------------------------------------
// Typed keys (guidance 3.3 / V5 section 2 key table). Keys use typed fields
// with full equality and hash; no truncated EIDs or raw-address mixing.
// ---------------------------------------------------------------------------

// Local Context identity that owns the native import (V5 ContextKey):
// device identity + EID + context incarnation.
struct UbContextKey {
    std::string device_identity;
    std::string eid;
    uint64_t context_incarnation = 0;

    bool operator==(const UbContextKey&) const = default;
};

[[nodiscard]] size_t ubContextKeyHashValue(const UbContextKey& key);

struct UbContextKeyHash {
    [[nodiscard]] size_t operator()(const UbContextKey& key) const {
        return ubContextKeyHashValue(key);
    }
};

// Peer-side shared target identity (V5 LocalTargetKey): the receiving
// node incarnation, the configured port, the target epoch and the frozen
// remote target profile.
struct UbPeerTargetKey {
    uint64_t node_incarnation = 0;
    std::string port_id;
    uint64_t target_epoch = 0;
    UbRemoteTargetProfile profile{};

    bool operator==(const UbPeerTargetKey&) const = default;
};

[[nodiscard]] size_t ubPeerTargetKeyHashValue(const UbPeerTargetKey& key);

struct UbPeerTargetKeyHash {
    [[nodiscard]] size_t operator()(const UbPeerTargetKey& key) const {
        return ubPeerTargetKeyHashValue(key);
    }
};

// Full import key (V5 ImportedTargetKey): the local ContextKey plus the peer
// LocalTargetKey plus the native EID/UASID/ID triple. One key identifies
// exactly one native import owned by the ImportedTargetRegistry.
struct UbImportedTargetKey {
    UbContextKey context{};
    UbPeerTargetKey peer{};
    std::string eid;
    uint32_t uasid = 0;
    uint32_t id = 0;

    bool operator==(const UbImportedTargetKey&) const = default;
};

[[nodiscard]] size_t ubImportedTargetKeyHashValue(const UbImportedTargetKey& key);

struct UbImportedTargetKeyHash {
    [[nodiscard]] size_t operator()(const UbImportedTargetKey& key) const {
        return ubImportedTargetKeyHashValue(key);
    }
};

// Watermark scope for imports: one watermark per (local Context, peer node,
// port, profile). The watermark is the latest successfully imported target
// epoch in that scope; older epochs are stale and never re-imported.
struct UbImportScopeKey {
    UbContextKey context{};
    uint64_t node_incarnation = 0;
    std::string port_id;
    UbRemoteTargetProfile profile{};

    bool operator==(const UbImportScopeKey&) const = default;
};

[[nodiscard]] size_t ubImportScopeKeyHashValue(const UbImportScopeKey& key);

struct UbImportScopeKeyHash {
    [[nodiscard]] size_t operator()(const UbImportScopeKey& key) const {
        return ubImportScopeKeyHashValue(key);
    }
};

// Strict key validation: nonzero incarnations/epochs/ids, non-empty
// identities and the frozen first-version profile (RM / JETTY / CTP).
Status validateUbContextKey(const UbContextKey& key);
Status validateUbPeerTargetKey(const UbPeerTargetKey& key);
Status validateUbImportedTargetKey(const UbImportedTargetKey& key);

// ---------------------------------------------------------------------------
// Shared dispositions and reference roles (T-06).
// ---------------------------------------------------------------------------

enum class UbTargetAcquireDisposition : uint8_t {
    // A reference was acquired on an existing or newly published target.
    kReady = 0,
    // A creation/import with the same key (or the same scope) is already in
    // flight; the caller defers non-blockingly and retries later.
    kDeferred,
    // The requested epoch is not acceptable (older than the published
    // watermark, or the entry was retired). The incumbent is kept.
    kStale,
    // The same generation carries different content; the healthy incumbent
    // is kept and the conflicting candidate is dropped.
    kConflict,
    // The receive-side node target quota is full. The caller defers with a
    // capacity classification; no healthy incumbent is retired.
    kCapacity,
};

[[nodiscard]] const char* ubTargetAcquireDispositionName(
    UbTargetAcquireDisposition disposition) noexcept;

// Who holds a reference on a shared target. Recorded for diagnostics and
// later probe budgeting; references are equivalent regardless of role.
enum class UbTargetReferenceRole : uint8_t {
    kEndpoint = 0,
    kBusiness,
    kProbe,
};

[[nodiscard]] size_t ubTargetReferenceRoleIndex(
    UbTargetReferenceRole role) noexcept;

// ---------------------------------------------------------------------------
// Send side: ImportedTargetRegistry (V5 5.1.3 / 23.2).
//
// The registry is the unique owner of native imports for one Context.
// Same ImportedTargetKey -> exactly one creator; concurrent waiters defer
// non-blockingly. The watermark of successfully imported target epochs
// advances only at the publish linearization point, never on failure.
// A newer epoch is a new owner: the old import keeps serving outstanding
// references and is unimported only after its last reference drops.
// ---------------------------------------------------------------------------

// Native import boundary. Production wires this to the URMA adapter (the
// concrete broker closes over the owning Context); tests inject fakes.
// The registry never calls these while holding its lock, and the broker
// must fully retain or clean up partial handles on failure.
class UbTargetImportBroker {
   public:
    virtual ~UbTargetImportBroker() = default;

    virtual Status importTarget(const UbImportedTargetKey& key,
                                const std::string& target_descriptor,
                                RemoteTargetPtr& out) = 0;

    virtual Status unimportTarget(const RemoteTargetPtr& target) = 0;
};

class UbImportedTargetRef;
class UbImportedTargetRegistry;

struct UbImportedTargetEntrySnapshot {
    UbImportedTargetKey key{};
    uint64_t references = 0;
    uint64_t endpoint_references = 0;
    uint64_t business_references = 0;
    uint64_t probe_references = 0;
    bool in_flight = false;
    bool retired = false;
};

struct UbImportScopeWatermarkSnapshot {
    UbImportScopeKey scope{};
    uint64_t imported_target_epoch = 0;
};

struct UbImportedTargetRegistrySnapshot {
    std::vector<UbImportedTargetEntrySnapshot> entries;  // sorted by key
    std::vector<UbImportScopeWatermarkSnapshot> watermarks;  // sorted by scope
    uint64_t successful_imports = 0;
    uint64_t failed_imports = 0;
    uint64_t successful_unimports = 0;
};

// Move-only reference on one imported shared target. Dropping the reference
// never unimports by itself: the import is reclaimed only when the entry is
// retired (superseded by a newer committed epoch, or explicitly revoked)
// AND its reference count reaches zero.
class UbImportedTargetRef {
   public:
    UbImportedTargetRef() = default;
    UbImportedTargetRef(UbImportedTargetRef&& other) noexcept;
    UbImportedTargetRef& operator=(UbImportedTargetRef&& other) noexcept;
    ~UbImportedTargetRef();

    UbImportedTargetRef(const UbImportedTargetRef&) = delete;
    UbImportedTargetRef& operator=(const UbImportedTargetRef&) = delete;

    [[nodiscard]] bool valid() const noexcept { return target_ != nullptr; }
    [[nodiscard]] const UbImportedTargetKey& key() const noexcept {
        return key_;
    }
    [[nodiscard]] UbTargetReferenceRole role() const noexcept {
        return role_;
    }
    // Native imported handle for WRs; null when invalid.
    [[nodiscard]] RemoteTargetPtr target() const noexcept { return target_; }

   private:
    friend class UbImportedTargetRegistry;
    UbImportedTargetRef(std::shared_ptr<UbImportedTargetRegistry> registry,
                        RemoteTargetPtr target, const UbImportedTargetKey& key,
                        UbTargetReferenceRole role);

    void reset() noexcept;

    std::shared_ptr<UbImportedTargetRegistry> registry_;
    RemoteTargetPtr target_;
    UbImportedTargetKey key_{};
    UbTargetReferenceRole role_ = UbTargetReferenceRole::kEndpoint;
};

class UbImportedTargetRegistry final
    : public std::enable_shared_from_this<UbImportedTargetRegistry> {
   public:
    UbImportedTargetRegistry(const UbImportedTargetRegistry&) = delete;
    UbImportedTargetRegistry& operator=(const UbImportedTargetRegistry&) =
        delete;

    static Status create(
        std::shared_ptr<UbTargetImportBroker> broker,
        std::shared_ptr<UbImportedTargetRegistry>& out);

    // Acquires a reference on the shared import identified by `key`.
    // `target_descriptor` is the opaque validated descriptor bytes from the
    // pooled v3 bootstrap; at equal published epochs it doubles as the
    // content fingerprint for conflict detection.
    //
    // On success (status OK) `disposition` is one of kReady / kDeferred /
    // kStale / kConflict and `out` is set only for kReady. On a broker
    // failure the entry is removed, the watermark is NOT advanced and the
    // failure status is returned; `disposition` is only meaningful when the
    // status is OK.
    Status acquire(const UbImportedTargetKey& key,
                   const std::string& target_descriptor,
                   UbTargetReferenceRole role, UbImportedTargetRef& out,
                   UbTargetAcquireDisposition& disposition);

    // Explicitly revokes one import (fault isolation / teardown). New
    // references on the revoked epoch are denied as stale; the native
    // unimport happens once the last outstanding reference drops. Unknown
    // or in-flight keys are rejected; revoking twice is idempotent.
    Status retireImport(const UbImportedTargetKey& key);

    [[nodiscard]] UbImportedTargetRegistrySnapshot snapshot() const;

   private:
    explicit UbImportedTargetRegistry(
        std::shared_ptr<UbTargetImportBroker> broker);

    struct Entry {
        UbImportedTargetKey key{};
        std::string target_descriptor;
        RemoteTargetPtr target;
        uint64_t references = 0;
        uint64_t role_references[3] = {0, 0, 0};
        bool in_flight = false;
        bool retired = false;
    };

    struct ReclaimWork {
        RemoteTargetPtr target;
    };

    friend class UbImportedTargetRef;

    // Called from UbImportedTargetRef destruction.
    void releaseReference(const UbImportedTargetKey& key,
                          UbTargetReferenceRole role);

    // Marks same-scope entries older than `new_epoch` retired and collects
    // zero-reference ones for immediate reclaim. Returns the targets that
    // must be unimported outside the lock.
    std::vector<ReclaimWork> supersedeLocked(const UbImportScopeKey& scope,
                                             uint64_t new_epoch);
    void reclaimOutsideLock(std::vector<ReclaimWork> work);

    const std::shared_ptr<UbTargetImportBroker> broker_;
    mutable std::mutex mutex_;
    std::unordered_map<UbImportedTargetKey, Entry, UbImportedTargetKeyHash>
        entries_;
    std::unordered_map<UbImportScopeKey, uint64_t, UbImportScopeKeyHash>
        watermarks_;
    uint64_t successful_imports_ = 0;
    uint64_t failed_imports_ = 0;
    uint64_t successful_unimports_ = 0;
};

// ---------------------------------------------------------------------------
// Receive side: LocalPortTargetRegistry (V5 5.1.1 / T-06 step 3).
//
// One LocalPortTarget per configured port, referenced by many peers,
// endpoints, business attempts and probes. Every initial creation and every
// epoch replacement reserves a ticket from the node-authoritative
// NodeTargetQuota (never per peer / Context / Engine / process). Referencing
// an existing published target never touches the quota, so a shared
// handshake is never rejected because the quota is full, and a capacity
// error never retires a healthy incumbent. Destruction of a retired target
// happens only after its last reference drops and must be proven before the
// quota slot is released.
//
// Lock order (T-15): registry mutex -> NodeTargetQuota mutex. The quota
// ledger never calls back into the registry, so the order is
// one-directional and deadlock-free.
// ---------------------------------------------------------------------------

// Descriptor of one receive-side shared target generation.
struct UbLocalPortTargetDesc {
    std::string port_id;          // configured port identity
    uint64_t target_epoch = 0;    // nonzero; new epoch = new owner
    std::string target_identity;  // published target identity (e.g. "T0")
    UbRemoteTargetProfile profile{};
};

Status validateUbLocalPortTargetDesc(const UbLocalPortTargetDesc& desc);

// Native receive-target boundary. Production wires this to the URMA adapter
// (creating the JFR jetty and associated resources from the PortBinding
// config); tests inject fakes. createTarget is called without registry
// locks. destroyTarget must set destruction_proven according to the
// provider contract; an unproven destruction keeps the node quota charged.
class UbLocalPortTargetFactory {
   public:
    virtual ~UbLocalPortTargetFactory() = default;

    virtual Status createTarget(const UbLocalPortTargetDesc& desc,
                                JettyPtr& out) = 0;

    virtual Status destroyTarget(const JettyPtr& target,
                                 bool& destruction_proven) = 0;
};

class UbLocalPortTargetRef;
class UbLocalPortTargetRegistry;

struct UbLocalPortTargetEntrySnapshot {
    std::string port_id;
    uint64_t target_epoch = 0;
    std::string target_identity;
    UbRemoteTargetProfile profile{};
    uint64_t references = 0;
    uint64_t endpoint_references = 0;
    uint64_t business_references = 0;
    uint64_t probe_references = 0;
    bool in_flight = false;
    bool retired = false;
};

struct UbLocalPortTargetRegistrySnapshot {
    std::vector<UbLocalPortTargetEntrySnapshot> entries;  // sorted by port
    std::map<std::string, uint64_t> port_watermarks;
    uint64_t successful_creations = 0;
    uint64_t failed_creations = 0;
    uint64_t proven_destructions = 0;
    uint64_t quarantined_destructions = 0;
};

// Move-only reference on one receive-side shared target generation.
// Dropping the reference never destroys the target: a retired generation is
// destroyed (and its node quota ticket released) only after the last
// reference drops and destruction is proven.
class UbLocalPortTargetRef {
   public:
    UbLocalPortTargetRef() = default;
    UbLocalPortTargetRef(UbLocalPortTargetRef&& other) noexcept;
    UbLocalPortTargetRef& operator=(UbLocalPortTargetRef&& other) noexcept;
    ~UbLocalPortTargetRef();

    UbLocalPortTargetRef(const UbLocalPortTargetRef&) = delete;
    UbLocalPortTargetRef& operator=(const UbLocalPortTargetRef&) = delete;

    [[nodiscard]] bool valid() const noexcept { return jetty_ != nullptr; }
    [[nodiscard]] const std::string& portId() const noexcept {
        return port_id_;
    }
    [[nodiscard]] uint64_t targetEpoch() const noexcept {
        return target_epoch_;
    }
    [[nodiscard]] UbTargetReferenceRole role() const noexcept {
        return role_;
    }
    // Native receive-side target handle; null when invalid.
    [[nodiscard]] JettyPtr jetty() const noexcept { return jetty_; }

   private:
    friend class UbLocalPortTargetRegistry;
    UbLocalPortTargetRef(
        std::shared_ptr<UbLocalPortTargetRegistry> registry, JettyPtr jetty,
        const std::string& port_id, uint64_t target_epoch,
        UbTargetReferenceRole role);

    void reset() noexcept;

    std::shared_ptr<UbLocalPortTargetRegistry> registry_;
    JettyPtr jetty_;
    std::string port_id_;
    uint64_t target_epoch_ = 0;
    UbTargetReferenceRole role_ = UbTargetReferenceRole::kEndpoint;
};

class UbLocalPortTargetRegistry final
    : public std::enable_shared_from_this<UbLocalPortTargetRegistry> {
   public:
    UbLocalPortTargetRegistry(const UbLocalPortTargetRegistry&) = delete;
    UbLocalPortTargetRegistry& operator=(const UbLocalPortTargetRegistry&) =
        delete;

    static Status create(
        std::shared_ptr<NodeTargetQuota> quota,
        std::shared_ptr<UbLocalPortTargetFactory> factory,
        std::shared_ptr<UbLocalPortTargetRegistry>& out);

    // Acquires a reference on the shared target of one configured port. If
    // a published target exists at the same epoch with the same content the
    // reference is granted without touching the node quota (shared
    // handshake never blocked by capacity). Otherwise a new generation is
    // created single-flight after reserving a node quota ticket; a full
    // quota defers with kCapacity and keeps every healthy incumbent.
    Status acquire(const UbLocalPortTargetDesc& desc,
                   UbTargetReferenceRole role, UbLocalPortTargetRef& out,
                   UbTargetAcquireDisposition& disposition);

    // Explicitly revokes the current generation of one port (fault
    // isolation / teardown). New references at that epoch are denied as
    // stale; destruction runs after the last reference drops. Unknown ports
    // or in-flight creations are rejected; revoking twice is idempotent.
    Status retirePortTarget(const std::string& port_id,
                            uint64_t target_epoch);

    [[nodiscard]] UbLocalPortTargetRegistrySnapshot snapshot() const;

   private:
    explicit UbLocalPortTargetRegistry(
        std::shared_ptr<NodeTargetQuota> quota,
        std::shared_ptr<UbLocalPortTargetFactory> factory);

    struct PortEpochKey {
        std::string port_id;
        uint64_t target_epoch = 0;

        bool operator==(const PortEpochKey&) const = default;
    };

    struct PortEpochKeyHash {
        [[nodiscard]] size_t operator()(const PortEpochKey& key) const {
            return ubHashCombineSeed(std::hash<std::string>{}(key.port_id),
                                     std::hash<uint64_t>{}(key.target_epoch));
        }
    };

    struct Entry {
        UbLocalPortTargetDesc desc;
        JettyPtr jetty;
        TargetQuotaTicket ticket;
        uint64_t references = 0;
        uint64_t role_references[3] = {0, 0, 0};
        bool in_flight = false;
        bool retired = false;
    };

    struct ReclaimWork {
        JettyPtr jetty;
        TargetQuotaTicket ticket;
    };

    friend class UbLocalPortTargetRef;

    void releaseReference(const std::string& port_id, uint64_t target_epoch,
                          UbTargetReferenceRole role);

    // Marks older generations of `port_id` retired and collects
    // zero-reference ones for immediate reclaim.
    std::vector<ReclaimWork> supersedeLocked(const std::string& port_id,
                                             uint64_t new_epoch);
    void reclaimOutsideLock(std::vector<ReclaimWork> work);

    const std::shared_ptr<NodeTargetQuota> quota_;
    const std::shared_ptr<UbLocalPortTargetFactory> factory_;
    mutable std::mutex mutex_;
    std::unordered_map<PortEpochKey, Entry, PortEpochKeyHash> entries_;
    std::unordered_map<std::string, uint64_t> watermarks_;
    uint64_t successful_creations_ = 0;
    uint64_t failed_creations_ = 0;
    uint64_t proven_destructions_ = 0;
    uint64_t quarantined_destructions_ = 0;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_TARGET_REGISTRY_H_
