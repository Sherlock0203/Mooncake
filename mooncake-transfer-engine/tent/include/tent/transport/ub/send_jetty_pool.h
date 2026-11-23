// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Per-context business send-Jetty pool (design §10.2).
//
// This file owns the native send resource of one attempt: the lease. The
// business path holds it, posts exactly one WR through it and hands it back
// exactly once (design §8/§10). The parking/refill executor split lives in
// jetty_refill_worker.h and jetty_retire_worker.h; both run provider calls
// outside the pool lock.
//
// Unified naming (merge of business_jetty_pool.h and the former tent_rm
// send_jetty_pool.h): the class is canonically `SendJettyPool` and lives in
// send_jetty_pool.h. The implementation is the superset evolution (tombstone
// bookkeeping, asynchronous drain controller, claim gate, creation backoff,
// retained-delete retry) that previously lived under the interim spelling
// `BusinessJettyPool`; that file and alias are gone.

#ifndef TENT_TRANSPORT_UB_SEND_JETTY_POOL_H_
#define TENT_TRANSPORT_UB_SEND_JETTY_POOL_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/pool_key.h"
#include "tent/transport/ub/tombstone.h"
#include "tent/transport/ub/urma_adapter.h"

// Regression-test friendship for the double-return guard (INV04).
class UbSendJettyPoolTest_DoubleReturnRejected_Test;

namespace mooncake::tent::ub {

// PoolKey / AttemptKey / PoolLaneKey live in pool_key.h so the asynchronous
// drain controller can share the exact identities without including this
// header (this header includes the controller's forward declaration below).

class JettyDrainController;

enum class PoolMemberState : uint8_t {
    kCreating = 0,
    // Creation failed without leaving native responsibility. A failed creation
    // never publishes a member, so BACKOFF is tracked per PoolKey (see
    // PoolBucket::backoff_until_ns and SendJettyPoolStats::backoff_keys) and
    // this value is only used by diagnostics that want one state vocabulary.
    kBackoff,
    kIdle,
    kInUse,
    kFaulted,
    // The member left kInUse and no claim can succeed on it any more: the
    // post gate is closed (state + error_epoch under the pool mutex). In the
    // asynchronous chain this is the observable "drain registered, modify not
    // yet done" stage (design §5.1: kDraining); the synchronous legacy fence
    // never rests here.
    kDraining,
    // The member's native Jetty is in ERROR and the drain chain is waiting for
    // the current epoch's flush-done marker. No native destruction may start
    // before marker + software-SQ-empty + responsibility convergence are all
    // proven (design §5.3).
    kFlushing,
    kDeleteReady,
    kDeleting,
    kQuarantined,
    kDestroyed,
};

inline const char* poolMemberStateName(PoolMemberState state) noexcept {
    switch (state) {
        case PoolMemberState::kCreating:
            return "creating";
        case PoolMemberState::kBackoff:
            return "backoff";
        case PoolMemberState::kIdle:
            return "idle";
        case PoolMemberState::kInUse:
            return "in_use";
        case PoolMemberState::kFaulted:
            return "faulted";
        case PoolMemberState::kDraining:
            return "draining";
        case PoolMemberState::kFlushing:
            return "flushing";
        case PoolMemberState::kDeleteReady:
            return "delete_ready";
        case PoolMemberState::kDeleting:
            return "deleting";
        case PoolMemberState::kQuarantined:
            return "quarantined";
        case PoolMemberState::kDestroyed:
            return "destroyed";
    }
    return "unknown";
}

// One pooled send Jetty. The member keeps its budget reservation until the
// native destruction is proven; state follows the C08 lifecycle
// (IDLE/IN_USE are the static Serving set).
struct PoolMember {
    PoolLaneKey lane{};
    PoolKey pool_key{};
    JettyPtr jetty;
    size_t jfc_index = 0;
    BudgetReservation budget{};
    // Written by the retire/delete path, read by memberStates()/stats() and
    // the return path. Atomic so a delete-thread transition cannot be a torn
    // read on the pool side (INV-3 / F9.3).
    std::atomic<PoolMemberState> state{PoolMemberState::kCreating};
    uint64_t error_epoch = 0;
    // True once a tombstone entry records this generation's unproven
    // destruction. Native budget may only be released after that entry is
    // erased (design §5.3/§7.2).
    bool tombstone_recorded = false;
    std::string retire_reason;
    // Native delete attempts that the provider answered with "still valid"
    // (DeleteOutcome::kRetained). Only drives the retry backoff; it is not a
    // responsibility counter.
    uint32_t delete_retry_attempts = 0;
};

class SendJettyPool;
class JettyRetireWorker;
class JettyRefillWorker;

// Move-only exclusive lease for one attempt (D03). The lease is returned
// exactly once through SendJettyPool::returnOnce() (normal or retire path).
// The destructor never calls native delete: a dropped lease is a
// responsibility leak, so the pool keeps the member out of circulation,
// retains its budget and counts the leak for diagnostics. The pool is held
// as a weak reference (RC-04): if the pool is destroyed first, the leak
// notification is skipped instead of dereferencing a dead pool; the member's
// JettyPtr still releases its handle best-effort.
class SliceJettyLease {
   public:
    SliceJettyLease() = default;
    SliceJettyLease(SliceJettyLease&& other) noexcept;
    SliceJettyLease& operator=(SliceJettyLease&& other) noexcept;
    ~SliceJettyLease();

    SliceJettyLease(const SliceJettyLease&) = delete;
    SliceJettyLease& operator=(const SliceJettyLease&) = delete;

    [[nodiscard]] bool valid() const noexcept { return member_ != nullptr; }
    // Safe on an invalid lease: a diagnostic path must never dereference a
    // dropped member (review minor).
    [[nodiscard]] const PoolLaneKey& lane() const noexcept {
        static const PoolLaneKey kNoLane{};
        return member_ ? member_->lane : kNoLane;
    }
    [[nodiscard]] const AttemptKey& attempt() const noexcept {
        return attempt_;
    }
    [[nodiscard]] std::shared_ptr<PoolMember> member() const noexcept {
        return member_;
    }

   private:
    friend class SendJettyPool;
    class JettyRetireWorker;
    class JettyRefillWorker;
    friend class ::UbSendJettyPoolTest_DoubleReturnRejected_Test;
    SliceJettyLease(std::weak_ptr<SendJettyPool> pool,
                    std::shared_ptr<PoolMember> member, AttemptKey attempt)
        : pool_(std::move(pool)),
          member_(std::move(member)),
          attempt_(attempt) {}

    void detach() noexcept {
        pool_.reset();
        member_ = nullptr;
        attempt_ = AttemptKey{};
    }

    // SendJettyPool is held as a weak reference (RC-04).
    std::weak_ptr<SendJettyPool> pool_;
    std::shared_ptr<PoolMember> member_;
    AttemptKey attempt_{};
};

// Lease plus the non-blocking JFC credit reservation for the WR the caller
// is about to post. The credit token must be released on the completion or
// rollback path, exactly once.
struct LeaseWithCredit {
    SliceJettyLease lease;
    size_t jfc_index = 0;
    uint64_t jfc_credit_token = 0;
    // The member's retirement epoch observed under the pool mutex while the
    // lease was taken. A claim compares it without re-locking for the member,
    // so a retirement that raced the lease can never be followed by a post
    // through the retired member.
    uint64_t error_epoch = 0;
};

// Why the pool refused the post right (design §5.2 rule 3). The order of the
// checks is fixed in tryClaimPost(): stopping -> bucket -> member state ->
// generation -> error epoch. Tests and diagnostics depend on that order.
//
// Reachability today: with the single-lease ownership model only
// kPoolStopping occurs in practice, because a leased member can only be
// retired by the lease holder itself, which is the caller. kMemberRetired /
// kGenerationMoved / kErrorEpochMoved are defensive assertions: they make the
// check total for a future multi-owner / background-retirement model and pin
// the intended verdicts in unit tests. They must not be read as evidence of a
// current race, and no extra lock is needed on their account.
enum class ClaimClosed : uint8_t {
    kNone = 0,
    kPoolStopping,     // beginStop() happened after the lease was taken
    kMemberRetired,    // member is no longer kInUse (retired/quarantined)
    kGenerationMoved,  // lane_generation no longer matches the lease snapshot
    kErrorEpochMoved,  // the member was retired after the lease was taken
};

inline const char* claimClosedName(ClaimClosed closed) noexcept {
    switch (closed) {
        case ClaimClosed::kNone:
            return "claimed";
        case ClaimClosed::kPoolStopping:
            return "pool_stopping";
        case ClaimClosed::kMemberRetired:
            return "member_retired";
        case ClaimClosed::kGenerationMoved:
            return "generation_moved";
        case ClaimClosed::kErrorEpochMoved:
            return "error_epoch_moved";
    }
    return "unknown";
}

// Input for tryClaimPost(). `member` is the leased member itself: the pool does
// not keep an in-use index, and the lease is the only owner of that identity,
// so passing it keeps the check O(1). The pointer stays valid because the
// caller holds the lease for the whole call.
struct ClaimRequest {
    AttemptKey attempt;
    uint64_t lane_generation = 0;
    uint64_t error_epoch_snapshot = 0;
    const PoolMember* member = nullptr;
};

struct ClaimVerdict {
    ClaimClosed closed = ClaimClosed::kNone;

    [[nodiscard]] bool claimed() const noexcept {
        return closed == ClaimClosed::kNone;
    }
};

struct SendJettyPoolOptions {
    // Static Serving (IDLE + IN_USE) per PoolKey. The pool never grows
    // beyond it; all-IN_USE therefore defers instead of expanding (C05).
    size_t desired_serving = 0;
    // Bound on concurrent creations per PoolKey so refill cannot storm the
    // provider.
    size_t max_creating = 2;
    // Background refill executor: periodically tops every known PoolKey back
    // up to desired_serving while idle and replaces retired members without
    // waiting for the next business attempt. Disabled by default so
    // deterministic direct-pool users keep synchronous accounting.
    bool background_refill = false;
    // Background delete executor: reset+delete of retired members happens off
    // the completion/return path. Disabled by default for the same reason.
    bool background_delete = false;
    uint32_t refill_interval_ms = 50;
    // Upper bound for one drain step: the hardware retirement fence (modify
    // ERROR + flush-done + software flush) in synchronous mode, and the total
    // from beginDrain() to DELETE_READY in asynchronous mode (design §10.1).
    uint32_t retire_timeout_ms = 1000;
    // Asynchronous drain chain (design §4.4). Enabled only when the transport
    // attaches a JettyDrainController; a pool without one always retires
    // synchronously, so direct-pool users keep the pre-drain behavior.
    bool async_drain = false;
    // Bound on flushJettyOnce() rounds the control executor runs before it
    // yields to its next task (design §10.1 drain_flush_batch).
    uint32_t drain_flush_batch = 64;
    // Consecutive failed drains after which the controller reports the
    // provider as unable to drain (design §9 fail-closed escalation).
    uint32_t drain_quarantine_threshold = 3;
    // UMDK requires a reset before a deleted Jetty in the current contract, so
    // the default keeps the shipped behavior. The knob exists because the
    // provider contract may relax it (source design §1.4) and a future
    // "handles delete an ERROR Jetty" provider must not be forced to reset.
    bool reset_before_delete = true;
    // Cap for the per-PoolKey creation backoff (design §7.3 BACKOFF).
    uint32_t cooldown_ms = 1000;
    // Bound on quarantined members. Reaching it stops admission instead of
    // dropping responsibility (design §11.4).
    size_t quarantine_budget = 64;
    // Shared tombstone table for unproven destructions. Null disables the
    // table (direct-pool users) and keeps the pre-tombstone budget behavior.
    std::shared_ptr<TombstoneTable> tombstones;
    JettyOptions jetty_options;
    // Event-level observability (verification guide O-1/O-2). Logging only.
    bool verbose_events = false;
    uint32_t sample_every = 1024;
};

struct SendJettyPoolStats {
    size_t idle_members = 0;
    size_t in_use_members = 0;
    size_t creating_members = 0;
    size_t retired_members = 0;
    size_t flushing_members = 0;
    size_t draining_members = 0;
    size_t delete_ready_members = 0;
    size_t quarantined_members = 0;
    // PoolKeys currently in creation backoff (bucket-level, not members).
    size_t backoff_keys = 0;
    uint64_t total_leases = 0;
    uint64_t total_returns = 0;
    uint64_t pool_misses = 0;
    uint64_t stop_rejections = 0;
    uint64_t leaked_leases = 0;
    uint64_t created_members = 0;
    uint64_t create_failures = 0;
    // Create failures whose cleanup could not prove destruction (the handle
    // and the budget stay in quarantined_). Observability for INV-BUD-1.
    uint64_t create_failures_unproven = 0;
    // A proven-destruction budget release that the ledger rejected. Must stay
    // zero; a non-zero value means the single-release invariant was violated.
    uint64_t budget_release_anomalies = 0;
    uint64_t double_returns = 0;
    uint64_t create_backoffs = 0;
    // Completions returned by a retirement fence. A pooled member serves one
    // WR, so a non-zero value is diagnostic, never a responsibility path.
    uint64_t fence_drained_completions = 0;
    // Asynchronous drain chain counters (design §10.2). The controller owns the
    // per-stage numbers; these are the ones the pool advances.
    uint64_t drain_begin = 0;
    uint64_t drain_delete_ready = 0;
    uint64_t drain_quarantined = 0;
    // Native delete outcomes (design §8.1). A retained or unknown outcome
    // never releases the member's budget.
    uint64_t delete_consumed = 0;
    uint64_t delete_retained = 0;
    uint64_t delete_unknown = 0;
    // Tombstone / recovery-budget observability (design §12.1).
    size_t tombstone_entries = 0;
    size_t tombstone_capacity = 0;
    uint64_t tombstone_insert_failures = 0;
    uint64_t tombstone_erase_misses = 0;
    uint64_t backpressure_rejections = 0;
    bool tombstone_backpressure = false;
    bool quarantine_backpressure = false;
    // Design §9 admission brake: the drain controller proved that the provider
    // cannot drain this device, so new slices are no longer admitted. It is a
    // process-lifetime decision and has no automatic reset.
    bool drain_admission_stopped = false;
};

// Per-attempt business send-Jetty pool (C06, INV03/INV04/INV06). One pool
// serves every PoolKey of one native Context. Members are maintained as a
// LIFO stack per PoolKey: returns push_front, leases pop the front (most
// recently returned first), freshly created members are appended at the
// back. JFC credit matching happens inside the LIFO order: the lease scans
// from the front for the newest member whose JFC credit is available this
// round. All provider calls happen outside the pool lock.
//
// Shutdown ordering contract (RC-03): the transport must stop every
// posting/polling consumer (workers->stop()) before the owning Context runs
// beginStop()/drainIdle(); drainIdle() verifies that no lease is outstanding
// and logs an error when the contract is violated. The pool must be owned by
// a shared_ptr so leases can hold a weak reference (RC-04).
class SendJettyPool final
    : public std::enable_shared_from_this<SendJettyPool> {
   public:
    SendJettyPool(std::shared_ptr<UrmaAdapter> adapter,
                      ContextPtr context_handle,
                      std::vector<std::shared_ptr<UbJfc>> jfcs,
                      std::shared_ptr<NativeBudget> budget,
                      std::string device_key,
                      std::shared_ptr<JfcCreditLedger> credits,
                      SendJettyPoolOptions options);

    SendJettyPool(const SendJettyPool&) = delete;
    SendJettyPool& operator=(const SendJettyPool&) = delete;

    // Stops both executors (when enabled), drains any pending asynchronous
    // deletes and gives quarantined members one last delete attempt. The
    // context normally calls beginStop()/drainIdle()/drainQuarantined()
    // first; the destructor is the final safety net.
    ~SendJettyPool();

    // Non-blocking lease for one attempt. Status codes:
    //   OK               lease (and JFC credit) acquired
    //   TooManyRequests  defer: pool/quota/credit insufficient (never a
    //                    retry charge, never a path fault, never a bypass)
    //   InvalidArgument  bad key/attempt or pool not attached
    Status tryLease(const PoolKey& pool_key, const AttemptKey& attempt,
                    LeaseWithCredit& out);

    // The pool's authority on "may this attempt be posted now" (design §5.2
    // rule 3). Pure read: no member state change, no counter, no provider
    // call (IN-1). It runs entirely under the pool mutex, so the verdict is
    // linearized with beginStop() and retirement, and it is safe between the
    // last rollback point and the native post.
    [[nodiscard]] ClaimVerdict tryClaimPost(const PoolKey& pool_key,
                                            const AttemptKey& attempt,
                                            const ClaimRequest& request) const;

    // Returns a lease exactly once. completion_gate_passed=true (the C07
    // normal gate) pushes the member back to the idle front; false or a
    // stopping pool retires the member permanently. Returns
    // InvalidArgument on a double return.
    Status returnOnce(SliceJettyLease lease, bool completion_gate_passed);

    // Retires the leased member after the first real error (INV07: the
    // member never returns to the pool, even if the path recovers).
    Status retireFaulted(SliceJettyLease lease, const std::string& reason);

    // Same, but runs the hardware retirement fence first when `fence_hardware`
    // is true. With an attached JettyDrainController and async_drain enabled,
    // the fence is registered with the controller and completes on the control
    // executor (`beginDrain` -> modify ERROR -> poller marker -> software flush
    // -> DELETE_READY). Without one, the legacy synchronous fence runs here:
    // the member moves to kFlushing, the adapter modifies the Jetty to ERROR,
    // consumes its flush-done marker and flushes the software SQ. Only a proven
    // fence hands the handle to reset+delete; a failed fence leaves the member
    // quarantined with its budget charged. The caller passes true only for a
    // real error completion (a proven timeout fence and the shutdown drain have
    // already fenced the member).
    Status retireFaulted(SliceJettyLease lease, const std::string& reason,
                         bool fence_hardware);

    // Asynchronous drain chain hooks (design §4.4). The transport attaches the
    // per-context controller after the pool and the workers exist; a null
    // controller (or async_drain=false) keeps every retirement synchronous.
    void attachDrainController(
        std::shared_ptr<JettyDrainController> controller);
    [[nodiscard]] std::shared_ptr<JettyDrainController> drainController() const;

    // Controller -> pool verdicts. They run on the control executor thread and
    // are never called under the controller lock.
    //
    // markDrainDeleteReady: every DELETE_READY condition holds (post gate
    // closed, modify done, current-epoch marker seen, software SQ empty, gate
    // responsibility converged). Only the pool moves the member and enqueues
    // it for native destruction; the native delete still runs on the retire
    // executor or on this thread when no executor is running.
    //
    // quarantineDrain: responsibility is unknown (timeout, unowned marker,
    // provider capability missing). The member keeps its handle, tombstone and
    // budget and no longer counts as serving.
    Status markDrainDeleteReady(const PoolLaneKey& lane, uint64_t error_epoch);
    Status quarantineDrain(const PoolLaneKey& lane, const std::string& reason);
    // Resolves the live native handle of one draining lane for a drain step.
    // The controller never retains it: ownership stays with the member so the
    // eventual delete still sees use_count()==1.
    [[nodiscard]] JettyPtr drainJettyFor(const PoolLaneKey& lane) const;

    // Stops new leases/creations and joins both background executors (when
    // enabled), draining any pending asynchronous deletes synchronously.
    // Outstanding leases still settle through returnOnce()/retireFaulted();
    // their members are retired synchronously, never reused.
    void beginStop();
    [[nodiscard]] bool stopping() const;

    // Deletes every idle member and acts as the shutdown barrier for the
    // background executors: the refill and delete threads are joined before
    // any native destruction here, so the caller may close the parent JFC
    // afterwards. Precondition (RC-03): every posting/polling consumer must
    // already be stopped (workers->stop()); drainIdle verifies that no lease
    // is outstanding or mid-creation and logs an error on violation. Handles
    // whose delete fails stay owned by the pool for the next drain attempt;
    // their budget stays charged until destruction is proven.
    Status drainIdle();

    // Attempts to delete quarantined members (e.g. during shutdown after
    // their responsibilities drained). Successful deletes release budget.
    Status drainQuarantined();

    // Retries every kRetained native delete whose backoff elapsed. Returns the
    // number of members retried. Driven by the refill pass and by the shutdown
    // drains; it never blocks and never drops responsibility.
    size_t retryRetainedDeletes();

    [[nodiscard]] SendJettyPoolStats stats() const;
    // Native budget snapshot for this pool's device (diagnostics only).
    [[nodiscard]] std::optional<BudgetDeviceSnapshot> budgetSnapshot() const;
    [[nodiscard]] std::vector<PoolMemberState> memberStates() const;
    [[nodiscard]] size_t servingCount(const PoolKey& pool_key) const;

    // W3 R-2: non-blocking pre-screen queries. `servingCount()` alone cannot
    // tell "every member is busy" from "the pool is empty and still creating",
    // so path selection can only pre-filter with these. The authority stays
    // with tryLease(): a positive idleCount still races other attempts, so a
    // miss remains a normal kPoolIdle defer (design §7.1).
    //
    // A richer LeaseMissReason (stopping/idle/credit/budget) is R-2 on the
    // pooling side and is deliberately not invented here; until it lands,
    // credit and pool-idle misses share the kPoolIdle defer bucket (§18-2).
    [[nodiscard]] size_t idleCount(const PoolKey& pool_key) const;
    [[nodiscard]] size_t creatingCount(const PoolKey& pool_key) const;

    // The JFC credit ledger shared by every lease of this pool. Credits are
    // released by the caller (completion or rollback path), never by the pool.
    [[nodiscard]] const std::shared_ptr<JfcCreditLedger>& creditLedger() const {
        return credits_;
    }

   private:
    friend class SliceJettyLease;  // reports dropped leases

    struct PoolBucket {
        std::deque<std::shared_ptr<PoolMember>> idle;  // front = newest
        size_t in_use = 0;
        size_t creating = 0;
        // Creation backoff (design §7.3 BACKOFF). The refill executor must not
        // treat a failed creation as an immediate deficit, or it storms the
        // provider. A demand-driven tryLease stays bounded by max_creating and
        // is allowed to retry, so a transient failure still recovers at once.
        uint64_t backoff_until_ns = 0;
        uint32_t backoff_attempts = 0;
    };

    Status createAndPublish(const PoolKey& pool_key);
    void retireMemberLocked(std::shared_ptr<PoolMember> member,
                            const std::string& reason);
    void enqueueRetirementLocked(std::shared_ptr<PoolMember> member);
    void noteCreateFailureLocked(const PoolKey& pool_key);
    void clearCreateBackoffLocked(const PoolKey& pool_key);
    [[nodiscard]] bool inCreateBackoffLocked(const PoolKey& pool_key) const;
    void destroyRetired(std::vector<std::shared_ptr<PoolMember>> members);
    // Applies one classified native delete outcome (design §8.2). Only
    // kConsumed releases the budget and erases the tombstone.
    void finishDelete(const std::shared_ptr<PoolMember>& member,
                      DeleteOutcome outcome);
    // Native reset+delete of one member through deleteJettyWithOutcome. Returns
    // the outcome; the caller owns the member either way.
    [[nodiscard]] DeleteOutcome resetAndDeleteMember(PoolMember& member);
    void onLeaseLeaked(const std::shared_ptr<PoolMember>& member);
    // Records unproven destruction for one member. Called whenever a member
    // leaves circulation; a full table raises tombstone backpressure instead of
    // evicting an unproven owner.
    void noteTombstoneLocked(const std::shared_ptr<PoolMember>& member);
    // True only when native budget may be released: a tombstone was recorded
    // (and is erased now) or no table is attached.
    bool eraseTombstoneForProvenDestruction(PoolMember& member);
    // Destroys a Jetty that never published. The single cleanup point for
    // failed/stopped creation: reset -> delete -> release only when the
    // destruction is proven; otherwise the handle and budget stay quarantined.
    // Returns the cause status when destruction is unproven.
    Status retireUnpublished(JettyPtr jetty, BudgetReservation reservation,
                             const PoolKey& pool_key);
    void refreshBackpressureLocked();
    [[nodiscard]] bool admissionBlockedLocked() const;

    // Background executors (design §10.2). The pool keeps the deficit/tombstone
    // logic and hands the thread ownership to the two workers, so no provider
    // call ever runs under the pool lock.
    void refillPass();
    void startWorkers();
    void stopWorkers();

    const std::shared_ptr<UrmaAdapter> adapter_;
    const ContextPtr context_handle_;
    const std::vector<std::shared_ptr<UbJfc>> jfcs_;
    const std::shared_ptr<NativeBudget> budget_;
    const std::string device_key_;
    const std::shared_ptr<JfcCreditLedger> credits_;
    const SendJettyPoolOptions options_;

    mutable std::mutex mutex_;  // never held across provider calls
    std::unordered_map<PoolKey, PoolBucket, PoolKeyHash> buckets_;
    std::vector<std::shared_ptr<PoolMember>> retired_;
    std::vector<std::shared_ptr<PoolMember>> quarantined_;
    // NOTE (INV-ID-2): every container below is *pool-private* and may keep
    // its uint64_t lane-generation key. A generation is unique within one pool
    // (next_lane_generation_ is monotonic here), so no ambiguity exists. Only
    // tables shared across Contexts (TombstoneTable, the workers' lane
    // reference ledger) must use the global ResourceIdentity/LaneRefKey; do not
    // "simplify" those back to a bare generation.
    // Members owned by the asynchronous drain chain, keyed by lane generation.
    // The pool retains them (not the controller: an entry must not keep a
    // member alive, design §7) until markDrainDeleteReady/quarantineDrain
    // hands them to the retirement path.
    std::unordered_map<uint64_t, std::shared_ptr<PoolMember>> draining_;
    // Members whose native delete returned kRetained, keyed by lane
    // generation, with the earliest retry time for each.
    std::unordered_map<uint64_t, std::shared_ptr<PoolMember>> retained_;
    std::unordered_map<uint64_t, uint64_t> delete_retry_at_ns_;
    uint64_t next_lane_generation_ = 1;
    size_t next_jfc_rotation_ = 0;
    bool stopping_ = false;
    // Members currently in kFlushing; the struct is not in a container while
    // its provider fence runs, so the count is kept explicitly.
    size_t flushing_ = 0;

    // Owned executors; null means "not started" and the pool degrades to
    // synchronous destruction / demand-driven creation.
    std::unique_ptr<JettyRetireWorker> retire_worker_;
    std::unique_ptr<JettyRefillWorker> refill_worker_;
    // Null means "not attached": every retirement uses the synchronous fence.
    std::shared_ptr<JettyDrainController> drain_controller_;

    // Diagnostics counters. Atomic rather than mutex-protected: finishDelete()
    // increments several of them on the retire thread and must not take the
    // pool lock for bookkeeping (nested-lock risk), while stats() may read them
    // at any time.
    std::atomic<uint64_t> total_leases_{0};
    std::atomic<uint64_t> total_returns_{0};
    std::atomic<uint64_t> pool_misses_{0};
    std::atomic<uint64_t> stop_rejections_{0};
    std::atomic<uint64_t> leaked_leases_{0};
    std::atomic<uint64_t> created_members_{0};
    std::atomic<uint64_t> create_failures_{0};
    std::atomic<uint64_t> create_failures_unproven_{0};
    std::atomic<uint64_t> budget_release_anomalies_{0};
    std::atomic<uint64_t> double_returns_{0};
    std::atomic<uint64_t> create_backoffs_{0};
    std::atomic<uint64_t> fence_drained_completions_{0};
    std::atomic<uint64_t> drain_begin_{0};
    std::atomic<uint64_t> drain_delete_ready_{0};
    std::atomic<uint64_t> drain_quarantined_{0};
    std::atomic<uint64_t> delete_consumed_{0};
    std::atomic<uint64_t> delete_retained_{0};
    std::atomic<uint64_t> delete_unknown_{0};
    std::atomic<uint64_t> tombstone_insert_failures_{0};
    std::atomic<uint64_t> tombstone_erase_misses_{0};
    std::atomic<uint64_t> backpressure_rejections_{0};
    bool tombstone_backpressure_ = false;
    bool quarantine_backpressure_ = false;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_SEND_JETTY_POOL_H_
