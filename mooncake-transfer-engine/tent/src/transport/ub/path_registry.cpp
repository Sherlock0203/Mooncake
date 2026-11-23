// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/path_registry.h"

#include <algorithm>
#include <condition_variable>
#include <utility>

namespace mooncake::tent::ub {
namespace {

Status invalidPathRegistryArg(const std::string& reason) {
    return Status::InvalidArgument("invalid path registry request: " + reason);
}

Status validateRemoteProfile(const UbRemoteTargetProfile& profile,
                             const std::string& context) {
    if (profile.trans_mode != kUrmaTransportModeRm) {
        return invalidPathRegistryArg(context + " trans_mode must be URMA_TM_RM");
    }
    if (profile.target_type != kUrmaTargetTypeJetty) {
        return invalidPathRegistryArg(context + " target type must be URMA_JETTY");
    }
    if (profile.tp_type != kUrmaTpTypeCtp) {
        return invalidPathRegistryArg(context + " tp_type must be URMA_CTP");
    }
    return Status::OK();
}

// Total order on path versions used to sort diagnostics snapshots. Selection
// ordering is betterUbPathSnapshot, never this function.
int comparePathVersions(const UbPathVersion& lhs, const UbPathVersion& rhs) {
    const auto& lk = lhs.key.pool;
    const auto& rk = rhs.key.pool;
    if (lk.context_incarnation != rk.context_incarnation)
        return lk.context_incarnation < rk.context_incarnation ? -1 : 1;
    if (lk.trans_mode != rk.trans_mode)
        return lk.trans_mode < rk.trans_mode ? -1 : 1;
    if (lk.priority != rk.priority) return lk.priority < rk.priority ? -1 : 1;
    if (lk.bind_tp_type_mask != rk.bind_tp_type_mask)
        return lk.bind_tp_type_mask < rk.bind_tp_type_mask ? -1 : 1;
    if (lhs.key.peer_incarnation != rhs.key.peer_incarnation)
        return lhs.key.peer_incarnation < rhs.key.peer_incarnation ? -1 : 1;
    if (lhs.key.remote_port_id != rhs.key.remote_port_id)
        return lhs.key.remote_port_id < rhs.key.remote_port_id ? -1 : 1;
    if (lhs.topology_epoch != rhs.topology_epoch)
        return lhs.topology_epoch < rhs.topology_epoch ? -1 : 1;
    if (lhs.target_epoch != rhs.target_epoch)
        return lhs.target_epoch < rhs.target_epoch ? -1 : 1;
    return 0;
}

}  // namespace

// ---------------------------------------------------------------------------
// Internal record. Control-plane fields are guarded by the registry mutex;
// in-flight counters and the shared target cell are atomic so the data plane
// (tryCharge / charge release) never takes the registry lock.
// ---------------------------------------------------------------------------

struct UbPathRecord {
    UbPathVersion version{};
    UbImportedTargetKey target_key{};
    std::string target_descriptor;
    size_t topology_rank = 0;
    UbPathEligibility eligibility = UbPathEligibility::kEligibleUnverified;
    uint64_t fault_epoch = 0;
    bool target_ready = false;
    bool paused = false;
    bool retired = false;
    std::atomic<uint64_t> inflight_wrs{0};
    std::atomic<uint64_t> inflight_bytes{0};
    std::shared_ptr<UbTargetLoadCell> target_cell;

    UbPathSnapshot selectionSnapshotLocked() const {
        UbPathSnapshot snapshot;
        snapshot.version = version;
        snapshot.eligibility = eligibility;
        snapshot.fault_epoch = fault_epoch;
        snapshot.topology_rank = topology_rank;
        snapshot.target_ready = target_ready;
        snapshot.path_inflight_wrs =
            inflight_wrs.load(std::memory_order_relaxed);
        snapshot.path_inflight_bytes =
            inflight_bytes.load(std::memory_order_relaxed);
        snapshot.target_inflight_wrs =
            target_cell->inflight_wrs.load(std::memory_order_relaxed);
        snapshot.target_inflight_bytes =
            target_cell->inflight_bytes.load(std::memory_order_relaxed);
        return snapshot;
    }

    UbPathEntrySnapshot entrySnapshotLocked() const {
        UbPathEntrySnapshot snapshot;
        snapshot.version = version;
        snapshot.eligibility = eligibility;
        snapshot.fault_epoch = fault_epoch;
        snapshot.topology_rank = topology_rank;
        snapshot.target_ready = target_ready;
        snapshot.paused = paused;
        snapshot.retired = retired;
        snapshot.path_inflight_wrs =
            inflight_wrs.load(std::memory_order_relaxed);
        snapshot.path_inflight_bytes =
            inflight_bytes.load(std::memory_order_relaxed);
        snapshot.target_inflight_wrs =
            target_cell->inflight_wrs.load(std::memory_order_relaxed);
        snapshot.target_inflight_bytes =
            target_cell->inflight_bytes.load(std::memory_order_relaxed);
        return snapshot;
    }
};

// ---------------------------------------------------------------------------
// Keys: hash / validation.
// ---------------------------------------------------------------------------

size_t ubPathKeyHashValue(const UbPathKey& key) {
    size_t seed = std::hash<uint64_t>{}(key.pool.context_incarnation);
    seed = ubHashCombineSeed(seed, std::hash<uint16_t>{}(key.pool.trans_mode));
    seed = ubHashCombineSeed(seed, std::hash<uint8_t>{}(key.pool.priority));
    seed = ubHashCombineSeed(seed,
                             std::hash<uint32_t>{}(key.pool.bind_tp_type_mask));
    seed = ubHashCombineSeed(seed,
                             std::hash<uint64_t>{}(key.peer_incarnation));
    seed = ubHashCombineSeed(seed,
                             std::hash<std::string>{}(key.remote_port_id));
    seed = ubHashCombineSeed(
        seed, std::hash<uint16_t>{}(key.remote_profile.trans_mode));
    seed = ubHashCombineSeed(
        seed, std::hash<uint8_t>{}(key.remote_profile.target_type));
    seed =
        ubHashCombineSeed(seed, std::hash<uint8_t>{}(key.remote_profile.tp_type));
    return seed;
}

size_t ubPathVersionHashValue(const UbPathVersion& version) {
    size_t seed = ubPathKeyHashValue(version.key);
    seed =
        ubHashCombineSeed(seed, std::hash<uint64_t>{}(version.topology_epoch));
    seed = ubHashCombineSeed(seed, std::hash<uint64_t>{}(version.target_epoch));
    return seed;
}

Status validateUbPathKey(const UbPathKey& key) {
    if (!key.pool.valid()) {
        return invalidPathRegistryArg("path pool key is unassigned");
    }
    if (key.pool.trans_mode != kUrmaTransportModeRm) {
        return invalidPathRegistryArg("path pool trans_mode must be URMA_TM_RM");
    }
    if (key.peer_incarnation == 0) {
        return invalidPathRegistryArg("peer incarnation must be nonzero");
    }
    if (key.remote_port_id.empty()) {
        return invalidPathRegistryArg("remote port id is empty");
    }
    return validateRemoteProfile(key.remote_profile, "path remote profile");
}

Status validateUbPathVersion(const UbPathVersion& version) {
    CHECK_STATUS(validateUbPathKey(version.key));
    if (version.topology_epoch == 0) {
        return invalidPathRegistryArg("topology epoch must be nonzero");
    }
    if (version.target_epoch == 0) {
        return invalidPathRegistryArg("target epoch must be nonzero");
    }
    return Status::OK();
}

const char* ubPathEligibilityName(UbPathEligibility eligibility) noexcept {
    switch (eligibility) {
        case UbPathEligibility::kInvalid:
            return "invalid";
        case UbPathEligibility::kEligibleUnverified:
            return "eligible_unverified";
        case UbPathEligibility::kHealthy:
            return "healthy";
        case UbPathEligibility::kFaulted:
            return "faulted";
    }
    return "unknown";
}

// ---------------------------------------------------------------------------
// Selection snapshot ordering (C04 layering).
// ---------------------------------------------------------------------------

bool betterUbPathSnapshot(const UbPathSnapshot& lhs,
                          const UbPathSnapshot& rhs) noexcept {
    // Admission first: a ready target outranks a cold one.
    if (lhs.target_ready != rhs.target_ready) return lhs.target_ready;
    // Topology locality remains the primary policy choice.
    if (lhs.topology_rank != rhs.topology_rank)
        return lhs.topology_rank < rhs.topology_rank;
    // Live path pressure: bytes before WRs, mirroring the legacy
    // normalized_inflight -> normalized_quota_wrs layering.
    if (lhs.path_inflight_bytes != rhs.path_inflight_bytes)
        return lhs.path_inflight_bytes < rhs.path_inflight_bytes;
    if (lhs.path_inflight_wrs != rhs.path_inflight_wrs)
        return lhs.path_inflight_wrs < rhs.path_inflight_wrs;
    // Aggregated pressure of every path sharing the same imported target.
    if (lhs.target_inflight_wrs != rhs.target_inflight_wrs)
        return lhs.target_inflight_wrs < rhs.target_inflight_wrs;
    if (lhs.target_inflight_bytes != rhs.target_inflight_bytes)
        return lhs.target_inflight_bytes < rhs.target_inflight_bytes;
    // Deterministic tail so concurrent workers agree on an order.
    return comparePathVersions(lhs.version, rhs.version) < 0;
}

// ---------------------------------------------------------------------------
// UbPathLoadCharge: lock-free move-only load ticket.
// ---------------------------------------------------------------------------

UbPathLoadCharge::UbPathLoadCharge(std::shared_ptr<UbPathRecord> record,
                                   uint64_t bytes, uint64_t wrs)
    : record_(std::move(record)), bytes_(bytes), wrs_(wrs) {}

UbPathLoadCharge::UbPathLoadCharge(UbPathLoadCharge&& other) noexcept
    : record_(std::move(other.record_)),
      bytes_(std::exchange(other.bytes_, 0)),
      wrs_(std::exchange(other.wrs_, 0)) {}

UbPathLoadCharge& UbPathLoadCharge::operator=(
    UbPathLoadCharge&& other) noexcept {
    if (this != &other) {
        reset();
        record_ = std::move(other.record_);
        bytes_ = std::exchange(other.bytes_, 0);
        wrs_ = std::exchange(other.wrs_, 0);
    }
    return *this;
}

UbPathLoadCharge::~UbPathLoadCharge() { reset(); }

const UbPathVersion& UbPathLoadCharge::version() const noexcept {
    return record_->version;
}

void UbPathLoadCharge::reset() noexcept {
    if (!record_) return;
    record_->inflight_wrs.fetch_sub(wrs_, std::memory_order_relaxed);
    record_->inflight_bytes.fetch_sub(bytes_, std::memory_order_relaxed);
    record_->target_cell->inflight_wrs.fetch_sub(wrs_,
                                                 std::memory_order_relaxed);
    record_->target_cell->inflight_bytes.fetch_sub(bytes_,
                                                   std::memory_order_relaxed);
    record_.reset();
    bytes_ = 0;
    wrs_ = 0;
}

// ---------------------------------------------------------------------------
// UbThreadedPathControlExecutor.
// ---------------------------------------------------------------------------

Status UbThreadedPathControlExecutor::create(
    size_t capacity, std::shared_ptr<UbThreadedPathControlExecutor>& out) {
    if (capacity == 0) {
        return Status::InvalidArgument(
            "control executor capacity must be nonzero");
    }
    out = std::shared_ptr<UbThreadedPathControlExecutor>(
        new UbThreadedPathControlExecutor(capacity));
    return Status::OK();
}

UbThreadedPathControlExecutor::UbThreadedPathControlExecutor(size_t capacity)
    : capacity_(capacity) {
    thread_ = std::thread([this] { run(); });
}

UbThreadedPathControlExecutor::~UbThreadedPathControlExecutor() {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

bool UbThreadedPathControlExecutor::enqueue(std::function<void()> work) {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (stopping_) return false;
        if (queue_.size() >= capacity_) return false;
        queue_.push_back(std::move(work));
    }
    cv_.notify_one();
    return true;
}

void UbThreadedPathControlExecutor::run() {
    for (;;) {
        std::function<void()> work;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock,
                     [this] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) return;  // stopping and fully drained
            work = std::move(queue_.front());
            queue_.pop_front();
        }
        work();
    }
}

// ---------------------------------------------------------------------------
// UbPathRegistry.
// ---------------------------------------------------------------------------

UbPathRegistry::UbPathRegistry(std::shared_ptr<UbColdPathPreparer> preparer,
                               std::shared_ptr<UbPathControlExecutor> executor,
                               size_t max_pending_cold_prepares)
    : preparer_(std::move(preparer)),
      executor_(std::move(executor)),
      max_pending_cold_prepares_(max_pending_cold_prepares) {}

Status UbPathRegistry::create(std::shared_ptr<UbColdPathPreparer> preparer,
                              std::shared_ptr<UbPathControlExecutor> executor,
                              size_t max_pending_cold_prepares,
                              std::shared_ptr<UbPathRegistry>& out) {
    if (!preparer) {
        return Status::InvalidArgument("cold path preparer is null");
    }
    if (!executor) {
        return Status::InvalidArgument("control executor is null");
    }
    if (max_pending_cold_prepares == 0) {
        return Status::InvalidArgument(
            "cold prepare bound must be nonzero");
    }
    out = std::shared_ptr<UbPathRegistry>(
        new UbPathRegistry(std::move(preparer), std::move(executor),
                           max_pending_cold_prepares));
    return Status::OK();
}

Status UbPathRegistry::publish(
    const UbPathVersion& version, size_t topology_rank,
    UbPathEligibility eligibility, const UbImportedTargetKey& target_key,
    const std::string& target_descriptor, bool target_ready,
    UbPathPublishDisposition& disposition) {
    CHECK_STATUS(validateUbPathVersion(version));
    if (eligibility != UbPathEligibility::kEligibleUnverified &&
        eligibility != UbPathEligibility::kHealthy) {
        return invalidPathRegistryArg(
            "published paths must be HEALTHY or ELIGIBLE_UNVERIFIED");
    }
    CHECK_STATUS(validateUbImportedTargetKey(target_key));
    if (target_descriptor.empty()) {
        return invalidPathRegistryArg("target descriptor is empty");
    }
    // The path version and the imported target must describe the same
    // generation: same local Context, same peer incarnation / port /
    // profile and the same target epoch.
    if (version.key.pool.context_incarnation !=
        target_key.context.context_incarnation) {
        return invalidPathRegistryArg(
            "path pool incarnation does not match the target context");
    }
    if (version.key.peer_incarnation != target_key.peer.node_incarnation) {
        return invalidPathRegistryArg(
            "path peer incarnation does not match the target peer");
    }
    if (version.key.remote_port_id != target_key.peer.port_id) {
        return invalidPathRegistryArg(
            "path remote port does not match the target port");
    }
    if (!(version.key.remote_profile == target_key.peer.profile)) {
        return invalidPathRegistryArg(
            "path remote profile does not match the target profile");
    }
    if (version.target_epoch != target_key.peer.target_epoch) {
        return invalidPathRegistryArg(
            "path target epoch does not match the target key epoch");
    }

    std::lock_guard<std::mutex> guard(mutex_);
    pruneRetiredLocked();
    const auto incumbent_it = by_key_.find(version.key);
    if (incumbent_it != by_key_.end()) {
        const std::shared_ptr<UbPathRecord>& incumbent = incumbent_it->second;
        if (incumbent->version == version) {
            if (incumbent->target_key == target_key &&
                incumbent->target_descriptor == target_descriptor) {
                // Idempotent refresh (e.g. endpoint view switch): keep every
                // load counter and never downgrade readiness.
                incumbent->topology_rank = topology_rank;
                incumbent->eligibility = eligibility;
                incumbent->target_ready =
                    incumbent->target_ready || target_ready;
                disposition = UbPathPublishDisposition::kPublished;
                return Status::OK();
            }
            disposition = UbPathPublishDisposition::kConflict;
            return Status::OK();
        }
        if (incumbent->version.newerThan(version)) {
            disposition = UbPathPublishDisposition::kStale;
            return Status::OK();
        }
        // Strictly newer generation: retire the incumbent. Outstanding
        // charges keep draining through their atomics; a new generation of
        // the same imported target reuses the aggregate cell below so the
        // target load is never reset to zero.
        incumbent->retired = true;
        pending_cold_.erase(incumbent->version);
        retired_.push_back(incumbent);
    }

    auto record = std::make_shared<UbPathRecord>();
    record->version = version;
    record->target_key = target_key;
    record->target_descriptor = target_descriptor;
    record->topology_rank = topology_rank;
    record->eligibility = eligibility;
    record->target_ready = target_ready;
    record->target_cell = cellForTargetLocked(target_key);
    by_key_[version.key] = std::move(record);
    disposition = UbPathPublishDisposition::kPublished;
    return Status::OK();
}

Status UbPathRegistry::pauseForTargetRebuild(const UbImportScopeKey& scope,
                                             uint64_t target_epoch,
                                             uint64_t& paused_count) {
    paused_count = 0;
    CHECK_STATUS(validateUbContextKey(scope.context));
    if (scope.node_incarnation == 0) {
        return invalidPathRegistryArg("scope node incarnation must be nonzero");
    }
    if (scope.port_id.empty()) {
        return invalidPathRegistryArg("scope port id is empty");
    }
    CHECK_STATUS(
        validateRemoteProfile(scope.profile, "scope remote profile"));
    if (target_epoch == 0) {
        return invalidPathRegistryArg("rebuild target epoch must be nonzero");
    }

    const auto matches = [&](const UbPathRecord& record) {
        return record.target_key.context == scope.context &&
               record.target_key.peer.node_incarnation ==
                   scope.node_incarnation &&
               record.target_key.peer.port_id == scope.port_id &&
               record.target_key.peer.profile == scope.profile &&
               record.version.target_epoch <= target_epoch;
    };

    std::lock_guard<std::mutex> guard(mutex_);
    pruneRetiredLocked();
    for (auto& [key, record] : by_key_) {
        if (matches(*record)) {
            record->paused = true;
            pending_cold_.erase(record->version);
            ++paused_count;
        }
    }
    for (const auto& record : retired_) {
        if (matches(*record) && !record->paused) {
            record->paused = true;
            ++paused_count;
        }
    }
    return Status::OK();
}

Status UbPathRegistry::markReady(const UbPathVersion& version) {
    CHECK_STATUS(validateUbPathVersion(version));
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = by_key_.find(version.key);
    if (it == by_key_.end() || !(it->second->version == version)) {
        return invalidPathRegistryArg("unknown path version");
    }
    const std::shared_ptr<UbPathRecord>& record = it->second;
    if (record->retired || record->paused) {
        return invalidPathRegistryArg(
            "path version is retired or paused; a new epoch must be published");
    }
    record->target_ready = true;
    return Status::OK();
}

Status UbPathRegistry::markFaulted(const UbPathVersion& version,
                                   uint64_t& fault_epoch) {
    fault_epoch = 0;
    CHECK_STATUS(validateUbPathVersion(version));
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = by_key_.find(version.key);
    if (it == by_key_.end() || !(it->second->version == version)) {
        return invalidPathRegistryArg("unknown path version");
    }
    const std::shared_ptr<UbPathRecord>& record = it->second;
    if (record->retired) {
        return invalidPathRegistryArg("cannot fault a retired path version");
    }
    if (record->fault_epoch == UINT64_MAX) {
        return Status::InternalError("path fault epoch exhausted");
    }
    ++record->fault_epoch;
    record->eligibility = UbPathEligibility::kFaulted;
    fault_epoch = record->fault_epoch;
    return Status::OK();
}

std::vector<UbPathSnapshot> UbPathRegistry::snapshotEligible(
    uint64_t peer_incarnation) const {
    std::lock_guard<std::mutex> guard(mutex_);
    std::vector<UbPathSnapshot> snapshots;
    for (const auto& [key, record] : by_key_) {
        if (key.peer_incarnation != peer_incarnation) continue;
        if (record->retired || record->paused) continue;
        if (record->eligibility == UbPathEligibility::kFaulted ||
            record->eligibility == UbPathEligibility::kInvalid) {
            continue;
        }
        snapshots.push_back(record->selectionSnapshotLocked());
    }
    std::sort(snapshots.begin(), snapshots.end(),
              [](const UbPathSnapshot& lhs, const UbPathSnapshot& rhs) {
                  return betterUbPathSnapshot(lhs, rhs);
              });
    return snapshots;
}

Status UbPathRegistry::tryCharge(const UbPathVersion& version, uint64_t bytes,
                                 uint64_t wrs, UbPathLoadCharge& charge,
                                 UbPathChargeDisposition& disposition) {
    if (charge.valid()) {
        return invalidPathRegistryArg("charge output is already held");
    }
    if (wrs == 0) {
        return invalidPathRegistryArg("charge requires nonzero WRs");
    }
    CHECK_STATUS(validateUbPathVersion(version));

    std::shared_ptr<UbPathRecord> record;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        const auto key_it = by_key_.find(version.key);
        if (key_it == by_key_.end()) {
            return invalidPathRegistryArg("unknown path key");
        }
        const std::shared_ptr<UbPathRecord>& candidate = key_it->second;
        if (!(candidate->version == version) || candidate->retired ||
            candidate->paused ||
            candidate->eligibility == UbPathEligibility::kFaulted) {
            // The selection snapshot raced with a supersede / rebuild pause /
            // fault: defer to another candidate, no retry, no fault.
            disposition = UbPathChargeDisposition::kNotEligible;
            return Status::OK();
        }
        if (!candidate->target_ready) {
            disposition = UbPathChargeDisposition::kDeferred;
            ++total_charge_deferrals_;
            maybeEnqueueColdLocked(candidate);
            return Status::OK();
        }
        ++total_charges_;
        record = candidate;
    }

    // Lock-free charge: the record stays alive through shared ownership even
    // if a concurrent publish retires it between here and the increments.
    record->inflight_wrs.fetch_add(wrs, std::memory_order_relaxed);
    record->inflight_bytes.fetch_add(bytes, std::memory_order_relaxed);
    record->target_cell->inflight_wrs.fetch_add(wrs, std::memory_order_relaxed);
    record->target_cell->inflight_bytes.fetch_add(bytes,
                                                  std::memory_order_relaxed);
    charge = UbPathLoadCharge(std::move(record), bytes, wrs);
    disposition = UbPathChargeDisposition::kCharged;
    return Status::OK();
}

UbPathRegistrySnapshot UbPathRegistry::statsSnapshot() const {
    std::lock_guard<std::mutex> guard(mutex_);
    UbPathRegistrySnapshot snapshot;
    for (const auto& [key, record] : by_key_) {
        snapshot.entries.push_back(record->entrySnapshotLocked());
    }
    for (const auto& record : retired_) {
        snapshot.entries.push_back(record->entrySnapshotLocked());
    }
    std::sort(snapshot.entries.begin(), snapshot.entries.end(),
              [](const UbPathEntrySnapshot& lhs,
                 const UbPathEntrySnapshot& rhs) {
                  return comparePathVersions(lhs.version, rhs.version) < 0;
              });
    snapshot.pending_cold_prepares = pending_cold_.size();
    snapshot.total_charges = total_charges_;
    snapshot.total_charge_deferrals = total_charge_deferrals_;
    snapshot.total_cold_prepares_enqueued = total_cold_prepares_enqueued_;
    snapshot.total_cold_prepare_failures = total_cold_prepare_failures_;
    snapshot.cold_prepares_dropped_full = cold_prepares_dropped_full_;
    return snapshot;
}

void UbPathRegistry::runColdPrepare(
    const std::shared_ptr<UbPathRecord>& record) {
    // Control executor thread only: the registry lock is not held across
    // prepare(), and no load charge or lease can exist on this record
    // because charges are granted exclusively on ready versions while a
    // cold-pending version is never ready.
    const Status status = preparer_->prepare(record->version,
                                             record->target_key,
                                             record->target_descriptor);
    std::lock_guard<std::mutex> guard(mutex_);
    pending_cold_.erase(record->version);
    if (!status.ok()) {
        ++total_cold_prepare_failures_;
        return;
    }
    const auto it = by_key_.find(record->version.key);
    if (it == by_key_.end() || it->second != record) return;  // superseded
    if (record->retired || record->paused) return;            // rebuild won
    record->target_ready = true;
}

void UbPathRegistry::maybeEnqueueColdLocked(
    const std::shared_ptr<UbPathRecord>& record) {
    if (record->retired || record->paused) return;
    if (pending_cold_.count(record->version) != 0) return;  // coalesced
    if (pending_cold_.size() >= max_pending_cold_prepares_) {
        ++cold_prepares_dropped_full_;
        return;
    }
    pending_cold_.insert(record->version);
    // The closure keeps the registry and the record alive. enqueue() is
    // non-blocking (bounded try-push), so no RPC or wait happens under the
    // registry lock; the lock order registry -> executor is one-directional
    // because the executor thread only re-enters the registry without
    // holding its own lock.
    const auto self = shared_from_this();
    const std::shared_ptr<UbPathRecord> captured = record;
    if (!executor_->enqueue(
            [self, captured] { self->runColdPrepare(captured); })) {
        pending_cold_.erase(record->version);
        ++cold_prepares_dropped_full_;
        return;
    }
    ++total_cold_prepares_enqueued_;
}

std::shared_ptr<UbTargetLoadCell> UbPathRegistry::cellForTargetLocked(
    const UbImportedTargetKey& target_key) {
    const auto it = target_cells_.find(target_key);
    if (it != target_cells_.end()) {
        if (auto cell = it->second.lock()) {
            return cell;
        }
        target_cells_.erase(it);
    }
    auto cell = std::make_shared<UbTargetLoadCell>();
    target_cells_[target_key] = cell;
    return cell;
}

void UbPathRegistry::pruneRetiredLocked() {
    retired_.erase(
        std::remove_if(retired_.begin(), retired_.end(),
                       [](const std::shared_ptr<UbPathRecord>& record) {
                           return record->inflight_wrs.load(
                                      std::memory_order_relaxed) == 0 &&
                                  record->inflight_bytes.load(
                                      std::memory_order_relaxed) == 0;
                       }),
        retired_.end());
}

}  // namespace mooncake::tent::ub
