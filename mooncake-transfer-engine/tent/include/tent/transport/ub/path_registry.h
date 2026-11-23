// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_PATH_REGISTRY_H_
#define TENT_TRANSPORT_UB_PATH_REGISTRY_H_

// ---------------------------------------------------------------------------
// NOT YET WIRED (audit A3).
//
// This file and path_registry.cpp arrived with the v3 control-plane import
// (tent/tent_rm @ edf6f9e8) and are compiled only because the UB library globs
// *.cpp. No translation unit outside this pair references any of these types,
// so a green build says nothing about v3 being active.
//
// Expected consumers once the v3 line lands: UbWorkers path selection and
// SendJettyPool's per-target load cell. Until then:
//   * do not read this header as an interface contract;
//   * do not "clean up" the types as dead code - they are the incoming
//     branch's destination, and removing them here would conflict with the
//     next sync from that line.
// If the v3 line is dropped instead, delete the pair in a dedicated change
// owned by that line, not as part of an unrelated cleanup.
// ---------------------------------------------------------------------------

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/bootstrap_types.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/target_registry.h"

namespace mooncake::tent::ub {

// ---------------------------------------------------------------------------
// Path identity (V5 section 3.2 key table, M04).
//
// PathKey = PoolKey + peer_incarnation + remote_port_id + profile.
// PathVersion = PathKey + topology_epoch + target_epoch. A version snapshot
// is immutable once published; publishing a newer version supersedes the
// incumbent (which keeps serving outstanding load charges until they drain)
// and an older epoch never overwrites a newer published incumbent.
// ---------------------------------------------------------------------------

struct UbPathKey {
    PoolKey pool{};
    uint64_t peer_incarnation = 0;
    std::string remote_port_id;
    UbRemoteTargetProfile remote_profile{};

    bool operator==(const UbPathKey&) const = default;
};

[[nodiscard]] size_t ubPathKeyHashValue(const UbPathKey& key);

struct UbPathKeyHash {
    [[nodiscard]] size_t operator()(const UbPathKey& key) const noexcept {
        return ubPathKeyHashValue(key);
    }
};

struct UbPathVersion {
    UbPathKey key{};
    uint64_t topology_epoch = 0;
    uint64_t target_epoch = 0;

    bool operator==(const UbPathVersion&) const = default;

    // Generation order within one PathKey: topology_epoch dominates, then
    // target_epoch. A version is newer only if strictly greater on this
    // lexicographic order; equal versions differ only by content.
    [[nodiscard]] bool newerThan(const UbPathVersion& other) const noexcept {
        if (topology_epoch != other.topology_epoch) {
            return topology_epoch > other.topology_epoch;
        }
        return target_epoch > other.target_epoch;
    }
};

[[nodiscard]] size_t ubPathVersionHashValue(const UbPathVersion& version);

struct UbPathVersionHash {
    [[nodiscard]] size_t operator()(const UbPathVersion& version) const
        noexcept {
        return ubPathVersionHashValue(version);
    }
};

Status validateUbPathKey(const UbPathKey& key);
Status validateUbPathVersion(const UbPathVersion& version);

// Business eligibility of one path version (V5 7.1). INVALID paths are never
// published; FAULTED is reached only via markFaulted. HEALTHY and
// ELIGIBLE_UNVERIFIED are both eligible for business selection.
enum class UbPathEligibility : uint8_t {
    kInvalid = 0,
    kEligibleUnverified,
    kHealthy,
    kFaulted,
};

[[nodiscard]] const char* ubPathEligibilityName(
    UbPathEligibility eligibility) noexcept;

enum class UbPathPublishDisposition : uint8_t {
    // The version was installed (new generation, or an idempotent
    // same-content republish that refreshes rank/eligibility and keeps every
    // load counter).
    kPublished = 0,
    // The candidate is older than the published incumbent; the incumbent is
    // kept and nothing changes.
    kStale,
    // The candidate carries the same generation but different content
    // (target key / descriptor); the healthy incumbent is kept.
    kConflict,
};

enum class UbPathChargeDisposition : uint8_t {
    // A move-only load charge was granted on a ready path version.
    kCharged = 0,
    // The path version is eligible but its target is not ready yet: cold
    // preparation is coalesced onto the control executor and the caller
    // defers without holding any lease or charge.
    kDeferred,
    // The version is paused (target rebuild), retired, faulted or superseded;
    // charging is denied and the caller selects another candidate.
    kNotEligible,
};

// ---------------------------------------------------------------------------
// Immutable selection snapshot (V5 7.1: snapshotEligible).
//
// Counters are a lock-consistent view of the path's own in-flight load plus
// the aggregated load of every path in this process referencing the same
// ImportedTargetKey. The endpoint view switch (republishing the same version)
// never resets these counters.
// ---------------------------------------------------------------------------

struct UbPathSnapshot {
    UbPathVersion version{};
    UbPathEligibility eligibility = UbPathEligibility::kInvalid;
    uint64_t fault_epoch = 0;
    size_t topology_rank = 0;
    bool target_ready = false;
    uint64_t path_inflight_wrs = 0;
    uint64_t path_inflight_bytes = 0;
    // Aggregated across all paths sharing the same ImportedTargetKey.
    uint64_t target_inflight_wrs = 0;
    uint64_t target_inflight_bytes = 0;
};

// Preserved selection layering (C04): admission (ready target) first, then
// topology rank, path in-flight pressure, per-imported-target aggregate
// pressure, and finally the stable key tail. Quota availability, bandwidth
// samples and retry_order remain caller-owned layers applied on top of this
// snapshot, exactly as in UbPathSelectionScore today.
[[nodiscard]] bool betterUbPathSnapshot(const UbPathSnapshot& lhs,
                                        const UbPathSnapshot& rhs) noexcept;

// Internal record; defined in path_registry.cpp. Declared here so load
// charges can hold shared ownership without depending on the registry.
struct UbPathRecord;

// Aggregate load cell shared by every path version in this process that
// references the same ImportedTargetKey (V5 7.1). Counting is atomic so
// charge/release never takes the registry lock. The cell survives path
// republication and endpoint view switches; it is reclaimed only when the
// last referencing record and charge drop.
struct UbTargetLoadCell {
    std::atomic<uint64_t> inflight_wrs{0};
    std::atomic<uint64_t> inflight_bytes{0};
};

// Move-only in-flight load charge on one published path version. Release is
// lock-free (atomics on the path record and its shared target cell) and
// stays valid after the version is retired or the registry is destroyed:
// the record stays alive through shared ownership until the last charge
// drops. A charge is never granted on a cold (not-ready) path, so a cold
// preparation RPC can never run while a lease is held.
class UbPathLoadCharge {
   public:
    UbPathLoadCharge() = default;
    UbPathLoadCharge(UbPathLoadCharge&& other) noexcept;
    UbPathLoadCharge& operator=(UbPathLoadCharge&& other) noexcept;
    ~UbPathLoadCharge();

    UbPathLoadCharge(const UbPathLoadCharge&) = delete;
    UbPathLoadCharge& operator=(const UbPathLoadCharge&) = delete;

    [[nodiscard]] bool valid() const noexcept { return record_ != nullptr; }
    // Defined in path_registry.cpp (UbPathRecord is incomplete here).
    [[nodiscard]] const UbPathVersion& version() const noexcept;
    [[nodiscard]] uint64_t bytes() const noexcept { return bytes_; }
    [[nodiscard]] uint64_t wrs() const noexcept { return wrs_; }

   private:
    friend class UbPathRegistry;
    UbPathLoadCharge(std::shared_ptr<UbPathRecord> record, uint64_t bytes,
                     uint64_t wrs);

    void reset() noexcept;

    std::shared_ptr<UbPathRecord> record_;
    uint64_t bytes_ = 0;
    uint64_t wrs_ = 0;
};

// ---------------------------------------------------------------------------
// Cold path preparation boundary (V5 7.1 / guidance 4.3).
//
// The control executor performs the async handshake/import for a path whose
// target is not ready. Production wires this to the bootstrap + import
// broker; tests inject fakes. prepare() is invoked only on the control
// executor thread: never on a post worker, never with the registry lock
// held, and never while any load charge or lease is held (charges are only
// granted on ready paths, and a cold-pending version is never ready).
// ---------------------------------------------------------------------------

class UbColdPathPreparer {
   public:
    virtual ~UbColdPathPreparer() = default;

    virtual Status prepare(const UbPathVersion& path,
                           const UbImportedTargetKey& target_key,
                           const std::string& target_descriptor) = 0;
};

// Bounded control-executor abstraction (guidance 3.3): cold preparation is
// queued and coalesced, never executed inline on the calling post worker.
// enqueue() is non-blocking and returns false when the bounded queue is
// full; the caller keeps its pending state and retries later.
class UbPathControlExecutor {
   public:
    virtual ~UbPathControlExecutor() = default;

    virtual bool enqueue(std::function<void()> work) = 0;
};

// Single-threaded bounded FIFO control executor. Work runs on a dedicated
// thread; the queue never grows past `capacity`. Destruction stops accepting
// new work, drains what is queued and joins the thread.
class UbThreadedPathControlExecutor final : public UbPathControlExecutor {
   public:
    UbThreadedPathControlExecutor(const UbThreadedPathControlExecutor&) =
        delete;
    UbThreadedPathControlExecutor& operator=(
        const UbThreadedPathControlExecutor&) = delete;

    static Status create(
        size_t capacity,
        std::shared_ptr<UbThreadedPathControlExecutor>& out);
    ~UbThreadedPathControlExecutor() override;

    bool enqueue(std::function<void()> work) override;

   private:
    explicit UbThreadedPathControlExecutor(size_t capacity);
    void run();

    const size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> queue_;
    bool stopping_ = false;
    std::thread thread_;
};

// ---------------------------------------------------------------------------
// PathRegistry (V5 3.1 / 7.1, M04): legal paths, eligibility, epochs and
// load. The data plane reads immutable snapshots and takes lock-free load
// charges; the control plane (publish / pause / markReady / markFaulted)
// runs under one mutex and never performs provider or RPC work inside it.
//
// Lock order: registry mutex -> control-executor mutex. The executor never
// calls back into the registry while holding its own lock, so the order is
// one-directional and deadlock-free.
// ---------------------------------------------------------------------------

struct UbPathEntrySnapshot {
    UbPathVersion version{};
    UbPathEligibility eligibility = UbPathEligibility::kInvalid;
    uint64_t fault_epoch = 0;
    size_t topology_rank = 0;
    bool target_ready = false;
    bool paused = false;
    bool retired = false;
    bool cold_pending = false;
    uint64_t path_inflight_wrs = 0;
    uint64_t path_inflight_bytes = 0;
    uint64_t target_inflight_wrs = 0;
    uint64_t target_inflight_bytes = 0;
};

struct UbPathRegistrySnapshot {
    std::vector<UbPathEntrySnapshot> entries;  // incumbents + retired, sorted
    size_t pending_cold_prepares = 0;
    uint64_t total_charges = 0;
    uint64_t total_charge_deferrals = 0;
    uint64_t total_cold_prepares_enqueued = 0;
    uint64_t total_cold_prepare_failures = 0;
    uint64_t cold_prepares_dropped_full = 0;
};

class UbPathRegistry final : public std::enable_shared_from_this<UbPathRegistry> {
   public:
    UbPathRegistry(const UbPathRegistry&) = delete;
    UbPathRegistry& operator=(const UbPathRegistry&) = delete;

    // `max_pending_cold_prepares` bounds the coalesced cold-preparation set;
    // a full set keeps deferring without enqueuing (never blocks, never
    // drops the pending state of paths already queued).
    static Status create(
        std::shared_ptr<UbColdPathPreparer> preparer,
        std::shared_ptr<UbPathControlExecutor> executor,
        size_t max_pending_cold_prepares,
        std::shared_ptr<UbPathRegistry>& out);

    // Publishes or replaces one path version for its PathKey.
    //  - newer generation: the incumbent is retired (it stops being
    //    eligible; outstanding charges drain through their atomics) and the
    //    new version is installed;
    //  - same generation + same content (target key and descriptor): an
    //    idempotent refresh of topology rank / eligibility; every load
    //    counter and the ready state are preserved (endpoint view switches
    //    never reset target load);
    //  - same generation + different content: kConflict, incumbent kept;
    //  - older generation: kStale, incumbent kept.
    Status publish(const UbPathVersion& version, size_t topology_rank,
                   UbPathEligibility eligibility,
                   const UbImportedTargetKey& target_key,
                   const std::string& target_descriptor, bool target_ready,
                   UbPathPublishDisposition& disposition);

    // Target rebuild pause (V5 18.3): every published or retired version
    // whose imported target matches `scope` and whose target epoch is not
    // newer than `target_epoch` is paused. Paused paths are excluded from
    // snapshotEligible and denied new charges; outstanding charges keep
    // draining normally. `paused_count` reports how many versions paused.
    Status pauseForTargetRebuild(const UbImportScopeKey& scope,
                                 uint64_t target_epoch,
                                 uint64_t& paused_count);

    // Marks a published cold version ready (control executor success path,
    // or a ready descriptor arriving from bootstrap). Unknown versions are
    // rejected; retired or paused versions are rejected without changes.
    Status markReady(const UbPathVersion& version);

    // Faults one published version: eligibility becomes kFaulted, the fault
    // epoch increments (starting at 1) and the version is excluded from
    // eligible snapshots until probe recovery (M08) re-publishes it.
    Status markFaulted(const UbPathVersion& version, uint64_t& fault_epoch);

    // Eligible (HEALTHY / ELIGIBLE_UNVERIFIED, not paused, not retired)
    // versions of one peer, sorted by the preserved selection layering.
    // Cold (not-ready) versions are included with target_ready = false so
    // post workers can defer them and try the next candidate.
    [[nodiscard]] std::vector<UbPathSnapshot> snapshotEligible(
        uint64_t peer_incarnation) const;

    // Non-blocking load charge on one published version.
    //  - ready: kCharged with a move-only charge (lock-free release);
    //  - eligible but cold: kDeferred, no charge held, cold preparation
    //    coalesced onto the control executor (bounded);
    //  - paused / retired / faulted / superseded: kNotEligible.
    // An unknown PathKey is a caller bug (InvalidArgument); a known key with
    // a stale version is a selection race and defers with kNotEligible.
    Status tryCharge(const UbPathVersion& version, uint64_t bytes,
                     uint64_t wrs, UbPathLoadCharge& charge,
                     UbPathChargeDisposition& disposition);

    [[nodiscard]] UbPathRegistrySnapshot statsSnapshot() const;

   private:
    explicit UbPathRegistry(std::shared_ptr<UbColdPathPreparer> preparer,
                            std::shared_ptr<UbPathControlExecutor> executor,
                            size_t max_pending_cold_prepares);

    // Runs on the control executor thread only. Never holds the registry
    // lock across prepare() and never runs while a charge exists on the
    // record (cold-pending versions are never ready).
    void runColdPrepare(const std::shared_ptr<UbPathRecord>& record);

    // Coalescing + bound, called with the registry mutex held. The executor
    // enqueue is non-blocking, satisfying the no-RPC-under-lock rule.
    void maybeEnqueueColdLocked(const std::shared_ptr<UbPathRecord>& record);

    // Finds or creates the shared aggregate cell for one imported target so
    // load survives republication. Called with the registry mutex held.
    std::shared_ptr<UbTargetLoadCell> cellForTargetLocked(
        const UbImportedTargetKey& target_key);

    // Drops retired records whose charges have fully drained. Called with
    // the registry mutex held.
    void pruneRetiredLocked();

    const std::shared_ptr<UbColdPathPreparer> preparer_;
    const std::shared_ptr<UbPathControlExecutor> executor_;
    const size_t max_pending_cold_prepares_;

    mutable std::mutex mutex_;
    // One incumbent per PathKey.
    std::unordered_map<UbPathKey, std::shared_ptr<UbPathRecord>, UbPathKeyHash>
        by_key_;
    // Retired generations kept alive until their charges drain.
    std::vector<std::shared_ptr<UbPathRecord>> retired_;
    // Live aggregate cells keyed by imported target identity. Weak so a cell
    // is reclaimed once no record or charge references it.
    std::unordered_map<UbImportedTargetKey, std::weak_ptr<UbTargetLoadCell>,
                       UbImportedTargetKeyHash>
        target_cells_;
    // Coalesced cold-preparation set, keyed by path version.
    std::unordered_set<UbPathVersion, UbPathVersionHash> pending_cold_;
    uint64_t total_charges_ = 0;
    uint64_t total_charge_deferrals_ = 0;
    uint64_t total_cold_prepares_enqueued_ = 0;
    uint64_t total_cold_prepare_failures_ = 0;
    uint64_t cold_prepares_dropped_full_ = 0;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_PATH_REGISTRY_H_
