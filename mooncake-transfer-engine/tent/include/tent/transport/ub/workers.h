// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_WORKERS_H_
#define TENT_TRANSPORT_UB_WORKERS_H_

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/segment_manager.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/completion_record.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/edge_invalid_tracker.h"
#include "tent/transport/ub/entity_event.h"
#include "tent/transport/ub/jetty_drain_controller.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/path_load_ledger.h"
#include "tent/transport/ub/post_scheduler.h"
#include "tent/transport/ub/probe_topology.h"
#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/slice.h"
#include "tent/transport/ub/tombstone.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

class UbEndpoint;

struct EndpointResolveRequest {
    UbContextPtr local_context;
    SegmentID remote_segment_id{LOCAL_SEGMENT_ID};
    const SegmentDesc* remote_segment{nullptr};
    Topology::NicID remote_topology_id{-1};
    uint64_t segment_generation{0};
};

using EndpointResolver = std::function<Status(const EndpointResolveRequest&,
                                              std::shared_ptr<UbEndpoint>&)>;
using EndpointRetirer = std::function<void(const std::shared_ptr<UbEndpoint>&)>;
using LocalDeviceFailureHandler =
    std::function<Status(Topology::NicID local_topology_id)>;

// Lexicographic path score. Capacity is a hard preference, topology locality
// remains the primary policy choice, and live pressure breaks ties before
// learned bandwidth. The deterministic tail makes concurrent workers agree on
// an order while retry_order rotates otherwise-equivalent rails on retry.
struct UbPathSelectionScore {
    bool quota_available{false};
    size_t topology_rank{Topology::DevicePriorityRanks};
    double normalized_inflight{1.0};
    double normalized_quota_wrs{1.0};
    // W3 D-11 renamed these from endpoint_outstanding_{wrs,bytes}: the value no
    // longer comes from the endpoint (which resets on every generation change)
    // but from the per-path in-flight ledger. The rename makes the source
    // change impossible to miss in review.
    uint64_t path_inflight_wrs{0};
    uint64_t path_inflight_bytes{0};
    bool has_bandwidth_sample{false};
    double ewma_bandwidth_bytes_per_second{-1.0};
    size_t retry_order{0};
    Topology::NicID local_topology_id{-1};
    int remote_device_id{-1};
};

[[nodiscard]] inline bool betterUbPathScore(
    const UbPathSelectionScore& lhs, const UbPathSelectionScore& rhs) noexcept {
    if (lhs.quota_available != rhs.quota_available) return lhs.quota_available;
    if (lhs.topology_rank != rhs.topology_rank)
        return lhs.topology_rank < rhs.topology_rank;
    if (lhs.normalized_inflight != rhs.normalized_inflight)
        return lhs.normalized_inflight < rhs.normalized_inflight;
    if (lhs.normalized_quota_wrs != rhs.normalized_quota_wrs)
        return lhs.normalized_quota_wrs < rhs.normalized_quota_wrs;
    if (lhs.path_inflight_wrs != rhs.path_inflight_wrs) {
        return lhs.path_inflight_wrs < rhs.path_inflight_wrs;
    }
    if (lhs.path_inflight_bytes != rhs.path_inflight_bytes) {
        return lhs.path_inflight_bytes < rhs.path_inflight_bytes;
    }
    if (lhs.has_bandwidth_sample != rhs.has_bandwidth_sample)
        return lhs.has_bandwidth_sample;
    if (lhs.has_bandwidth_sample && lhs.ewma_bandwidth_bytes_per_second !=
                                        rhs.ewma_bandwidth_bytes_per_second) {
        return lhs.ewma_bandwidth_bytes_per_second >
               rhs.ewma_bandwidth_bytes_per_second;
    }
    if (lhs.retry_order != rhs.retry_order)
        return lhs.retry_order < rhs.retry_order;
    if (lhs.local_topology_id != rhs.local_topology_id)
        return lhs.local_topology_id < rhs.local_topology_id;
    return lhs.remote_device_id < rhs.remote_device_id;
}

// Orders pre-scored rails for lazy endpoint resolution. `local_topology_ids`
// holds each rail's local device in pre-score order, and the returned values
// are indices into it.
//
// Pass one takes the highest-scoring rail of every local device, so the
// candidates that actually get resolved span devices instead of piling onto
// whichever device ranks best. Pass two appends the remaining rails, so a
// caller that still needs candidates - a single-device topology, or a rail that
// failed to resolve - can fill its floor without attempting a rail twice.
[[nodiscard]] inline std::vector<size_t> ubResolutionAttemptOrder(
    const std::vector<Topology::NicID>& local_topology_ids) {
    const size_t count = local_topology_ids.size();
    std::vector<bool> taken(count, false);
    std::vector<size_t> order;
    order.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        const auto id = local_topology_ids[index];
        bool known_device = false;
        for (size_t earlier : order) {
            if (local_topology_ids[earlier] == id) {
                known_device = true;
                break;
            }
        }
        if (known_device) continue;
        order.push_back(index);
        taken[index] = true;
    }
    for (size_t index = 0; index < count; ++index) {
        if (!taken[index]) order.push_back(index);
    }
    return order;
}

// W3 business-path counters (design §13). The values are cumulative except
// queue_depth/parked_depth, which are sampled at snapshot() time.
struct UbBusinessStatsSnapshot {
    uint64_t queue_depth{0};
    uint64_t parked_depth{0};
    uint64_t submit_backpressure{0};
    uint64_t submit_accepted{0};
    std::array<uint64_t, kDeferReasonCount> defer_pending{};
    std::array<uint64_t, kDeferReasonCount> defer_epoch_wakeup{};
    uint64_t defer_reflow_repark{0};
    uint64_t defer_bucket_snapshot_retry{0};
    uint64_t reflow_requeued{0};
    uint64_t reflow_cancelled{0};
    uint64_t task_K_wait{0};
    uint64_t pool_lease_attempts{0};
    uint64_t pool_lease_misses{0};
    uint64_t pool_stop_rejections{0};
    uint64_t pool_hard_errors{0};
    uint64_t business_param_error_invalid_edge{0};
    uint64_t business_deadline_timeouts{0};
    // F6a/F6a2: control-executor scheduling of deadline fences and device
    // retirement. Rejections leave the responsibility in place for a retry.
    // A pooled record without a Context incarnation cannot be keyed globally;
    // it is counted instead of aliasing every Context's lane 0 (D5).
    uint64_t pooled_lane_ref_unmapped{0};
    // Legacy local x remote candidates refused because the two EIDs are in
    // different reachability scopes (require_same_eid_scope).
    uint64_t eid_scope_denied{0};
    uint64_t timeout_fence_submitted{0};
    uint64_t timeout_fence_rejected{0};
    uint64_t device_retire_rejected{0};
    // F6c: requests terminated because Request::deadline_ns had passed. All
    // four stay zero when deadline_ns == 0 or the switch is off.
    uint64_t expired_in_queue{0};
    uint64_t expired_parked{0};
    uint64_t expired_pre_post{0};
    uint64_t expired_inflight{0};
    uint64_t declared_candidates_total{0};
    uint64_t declared_candidates_denied{0};
    // W3g claim (R-1): rejections by pool-authority reason, the pre-W3g
    // rollback path (claim/enabled=false) and the surviving PLM epoch check.
    uint64_t claim_rejected_stopping{0};
    uint64_t claim_rejected_member{0};
    uint64_t claim_rejected_generation{0};
    uint64_t claim_rejected_error_epoch{0};
    uint64_t claim_disabled{0};
    uint64_t claim_after_isolation{0};
    // Unknown-token completions attributed to a tombstoned (retired) native
    // entity. `unknown_token` still counts every unattributed token.
    uint64_t unknown_token_tombstoned{0};
    uint64_t post_on_isolated_path{0};
    uint64_t pooled_return{0};
    uint64_t pooled_retire{0};
    uint64_t pool_return_double{0};
    uint64_t path_inflight_peak{0};
    uint64_t path_load_imbalance{0};
};

// One dedicated probe completion queue handed over by the pooling side. The
// handle stays owned by the provider; workers only polls it (design §11.1).
struct ProbeJfcSlot {
    Topology::NicID local_nic{-1};
    JfcPtr jfc;
};

// Native UB scheduler and owner of the business half of every completion
// record. Posting lanes own request selection and URMA post; poller lanes own
// completion dispatch. A monotonic numeric token, never a raw UbSlice pointer,
// crosses the adapter boundary.
//
// `gate::Sink` is the completion gate's side-effect outlet (design D-11): the
// gate decides *when* each action may run, this class performs the resource
// release, the health reporting and the slice delivery.
class UbWorkers final : public gate::Sink {
   public:
    UbWorkers(std::shared_ptr<UrmaAdapter> adapter,
              std::vector<UbContextPtr> contexts,
              std::shared_ptr<Topology> local_topology,
              SegmentManager* segment_manager, UbBufferManager* buffers,
              RailMonitor* rail_monitor, QuotaManager* quota, UbParams params,
              EndpointResolver endpoint_resolver,
              EndpointRetirer endpoint_retirer = {},
              LocalDeviceFailureHandler local_device_failure_handler = {},
              ProbeLinkManager* probe_manager = nullptr,
              std::vector<ProbeJfcSlot> probe_jfcs = {},
              std::shared_ptr<TombstoneTable> tombstones = nullptr,
              const ProbeTopology* probe_topology = nullptr,
              // F6a: inject a deterministic control executor (tests) or a
              // dedicated one owned by the transport. Null makes start()
              // create its own; an executor passed here is never stopped by
              // the workers.
              std::shared_ptr<ControlExecutor> control_executor = nullptr);
    ~UbWorkers();

    UbWorkers(const UbWorkers&) = delete;
    UbWorkers& operator=(const UbWorkers&) = delete;

    Status start();
    Status stop();
    Status submitBatch(const std::vector<UbTask::Ptr>& tasks,
                       uint64_t device_mask = ~0ULL);
    Status submit(const UbTask::Ptr& task, uint64_t device_mask = ~0ULL);
    Status cancel(const UbTask::Ptr& task);

    [[nodiscard]] bool running() const noexcept {
        return accepting_.load(std::memory_order_acquire);
    }
    [[nodiscard]] size_t queuedCount() const;
    // Deferred (parked) slices: part of the same bounded budget as the queue
    // (design §6.1), reported separately for observation.
    [[nodiscard]] size_t parkedCount() const;
    // Number of records still registered (in-flight owner accounting).
    [[nodiscard]] size_t inflightCount() const;
    // W3 admission/defer/pool counters (design §13).
    [[nodiscard]] UbBusinessStatsSnapshot businessStats() const;
    [[nodiscard]] uint64_t queueBudget() const noexcept {
        return params_.queue_budget;
    }
    // Completion gate counters (design §9).
    [[nodiscard]] CompletionStatsSnapshot completionStats() const {
        return gate_ctx_.stats.snapshot();
    }
    [[nodiscard]] const CountingEntityEventSink& entityEvents() const {
        return entity_events_;
    }

    // Asynchronous drain chain entry points (design §4.3/§6.3). Both run on
    // the control executor, never on a poller.
    //
    // settleDrainedWr(): a WR recovered by the software flush settles exactly
    // once through the normal completion path. Returns true when this call
    // settled it, false when the token was already claimed and disposed.
    bool settleDrainedWr(const Completion& completion, uint64_t jfc_index);
    // True once no registered record still holds the lane's lease, i.e. the
    // completion gate claimed and disposed of every owner. This is the
    // evidence DELETE_READY condition ⑤ asks for, derived from the existing
    // record table (no counter on PoolMember).
    //
    // INV-ID-3: the query is keyed by (context incarnation, lane generation).
    // A pool-local generation alone is not unique across Contexts: every pool
    // starts at 1, so a bare generation could report convergence while another
    // Context's WR is still in flight and delete its Jetty too early.
    [[nodiscard]] bool pooledLaneConverged(const LaneRefKey& lane) const;

    // Business path-error hint used by the probe coordinator to refresh the
    // affected peer early. Called outside the PLM lock; nullptr disables it.
    void setProbeRefreshHint(std::function<void(uint64_t)> hint) {
        std::lock_guard<std::mutex> lock(probe_hint_mutex_);
        probe_refresh_hint_ = std::move(hint);
    }

    // F6a: executor for deadline fences and device retirement. Must be set
    // before start(); when it is null (tests, or a refused thread) the work
    // runs inline and the poller keeps the legacy blocking behavior.
    void setControlExecutor(std::shared_ptr<ControlExecutor> executor);

   private:
    friend class UbWorkersTestPeer;
    // Scoped to the submitting thread so transport tests can exercise the
    // real workers without changing their lifecycle or racing posting lanes.
    static thread_local bool fail_second_enqueue_for_test_;

    struct PendingSlice {
        // Keep the task alive until every queued/in-flight slice reaches a
        // terminal state. Callers may release the sub-batch handle before the
        // asynchronous data path has drained.
        UbTask::Ptr task;
        UbSlice::Ptr slice;
        uint64_t device_mask{~0ULL};
        int priority{PRIO_HIGH};
        SegmentID target_id{LOCAL_SEGMENT_ID};
        Request::OpCode opcode{Request::READ};
        // How often this slice has been parked so far. Used only to count
        // defer_reflow_repark: a reflowed slice that has to wait again means a
        // wake-up source and its condition disagree (design §13).
        uint32_t deferrals{0};
        // F6c: steady-clock time the batch was accepted, for queue-latency
        // diagnostics against Request::deadline_ns.
        uint64_t submit_ns{0};
    };
    struct Route;
    struct BusinessRecord;
    // Availability epochs of every defer bucket, sampled once before the
    // admission conditions are evaluated (design §7.4).
    struct DeferEpochs {
        uint64_t task_permit{0};
        uint64_t quota{0};
        uint64_t pool_idle{0};
        uint64_t path{0};
    };
    // Why selection failed and when to re-evaluate it.
    struct DeferPlan {
        DeferReason reason{DeferReason::kQuota};
        // Non-zero for conditions that only time can resolve (a device in
        // recovery cooldown). Every other reason is bumped by its release site.
        uint64_t expire_ns{0};
    };
    struct LocalDeviceCandidate {
        Topology::NicID id{-1};
        size_t topology_rank{Topology::DevicePriorityRanks};
    };

    // popPending distinguishes "took an item", "the deferred set has wake-ups
    // to reflow" and "posting stopped" so the posting loop never sleeps on a
    // resource change it could act on (design §7.4).
    enum class PopResult : uint8_t {
        kPending,
        kReflow,
        kStopped,
    };

    void postingLoop(size_t worker_index);
    void pollingLoop(size_t poller_index);
    // tent_merge's tri-state pop is authoritative: kStopped must exit the
    // posting thread, kReflow must run the defer reflow, kPending may still
    // carry an empty PendingSlice ("woke up, nothing to do").
    PopResult popPending(PendingSlice& pending);

    // ---- idle spin (yuanhao-1113, performance) ---------------------------
    // Purely a scheduling hint. spinForPending() never consumes work and never
    // bypasses the locked predicate: the caller always falls through to
    // queue_cv_, which rechecks posting_ and every queue under queue_mutex_,
    // so an early or late exit can never lose a wakeup.
    void spinForPending();
    [[nodiscard]] bool withinIdleSpin(uint64_t now_ns, uint64_t last_ns,
                                      uint32_t window_us) const;
    void enqueueRetry(const PendingSlice& pending);
    // Parks one slice against the availability epoch that was read *before*
    // the failing condition was evaluated (design §7.4). Never blocks and never
    // sleeps: the bucket bump is the only wake-up source.
    void deferPending(const PendingSlice& pending, DeferReason reason,
                      uint64_t wait_epoch, uint64_t expire_ns = 0);
    // Moves stale parked slices back to the business queue. Returns the number
    // of reflowed items.
    size_t reflowDeferred();
    // Must be called with queue_mutex_ held.
    void bumpLocked(DeferReason reason);
    void processPending(const PendingSlice& pending, size_t worker_index);
    // Returns a copy of the deferred slice with its deferral counter advanced
    // (and the repark observation recorded).
    [[nodiscard]] PendingSlice withDeferralCount(const PendingSlice& pending);
    // Periodic, rate-limited business summary (production observation outlet).
    void logBusinessSummary(uint64_t now_ns);
    // W3 pooled tail of processPending: K -> lease(+credit) -> claim snapshot
    // -> claim review -> commit -> publish -> tristate post (design §8.1).
    void processPendingPooled(
        const PendingSlice& pending, size_t worker_index, const Route& route,
        const std::shared_ptr<UbEndpoint>& endpoint, const UbPostPath& path,
        const ProbePathKey& probe_key, const QuotaReservation& reservation,
        const LocalSegmentRef& local, const ImportedSegmentRef& remote,
        const UbAttemptToken& attempt);
    // W2 tail of processPending, kept byte-for-byte for pool == nullptr.
    void postLegacy(const PendingSlice& pending, size_t worker_index,
                    const ProbePathKey& probe_key,
                    const std::shared_ptr<UbEndpoint>& endpoint,
                    const UbPostPath& path, const QuotaReservation& reservation,
                    const LocalSegmentRef& local,
                    const ImportedSegmentRef& remote,
                    const UbAttemptToken& attempt);
    std::shared_ptr<BusinessRecord> makeRecord(
        const PendingSlice& pending,
        const std::shared_ptr<UbEndpoint>& endpoint, const UbPostPath& path,
        const ProbePathKey& probe_key, const QuotaReservation& reservation,
        const LocalSegmentRef& local, const ImportedSegmentRef& remote,
        const UbAttemptToken& attempt);
    // Maps a business path to the synthetic probe identity when its peer is
    // declared. An invalid key means "not modelled by the probe graph".
    [[nodiscard]] ProbePathKey probeKeyFor(const Route& route,
                                           const UbPostPath& path) const;
    // W3g-a claim decision. `closed` is the pool authority's verdict (R-1);
    // `path_epoch_changed` is the pre-W3g PLM scalar check, which stays a
    // separate input (design D-2: sampling is kept, authority moves).
    struct ClaimDecision {
        ClaimClosed closed{ClaimClosed::kNone};
        bool path_epoch_changed{false};

        [[nodiscard]] bool claimed() const noexcept {
            return closed == ClaimClosed::kNone && !path_epoch_changed;
        }
    };

    // The W3g claim (design §8.2/R-1): the pool's synchronized verdict plus the
    // surviving fault-epoch comparison. Runs immediately before the native
    // post, never blocks and never calls a provider (W3-IN-8, W3g IN-1).
    [[nodiscard]] ClaimDecision claimPost(
        const std::shared_ptr<SendJettyPool>& pool, const PoolKey& pool_key,
        const AttemptKey& attempt, const LeaseWithCredit& leased,
        const UbPostPath& path, const ProbePathKey& probe_key,
        uint64_t fault_epoch_snapshot);
    [[nodiscard]] static PoolKey poolKeyFor(
        const std::shared_ptr<UbEndpoint>& endpoint);
    [[nodiscard]] DeferEpochs readDeferEpochs() const;
    // The W3 mode gate (D-2): non-null only when the transport attached a pool
    // to this endpoint's context.
    [[nodiscard]] std::shared_ptr<SendJettyPool> pooledPoolFor(
        const std::shared_ptr<UbEndpoint>& endpoint) const;
    // Cheap pool pre-filter for path selection (design §7.1 rule 2). A false
    // answer only means "try another candidate"; tryLease stays authoritative.
    [[nodiscard]] bool poolCandidateUsable(
        const std::shared_ptr<UbEndpoint>& endpoint) const;
    [[nodiscard]] static WorkRequest makeWorkRequest(
        const PendingSlice& pending, uint64_t token,
        const LocalSegmentRef& local, const ImportedSegmentRef& remote);
    Status buildRoute(const PendingSlice& pending, Route& route);
    Status chooseAndResolveEndpoint(const PendingSlice& pending, Route& route,
                                    std::shared_ptr<UbEndpoint>& endpoint,
                                    UbPostPath& path,
                                    QuotaReservation& reservation,
                                    DeferPlan* plan = nullptr);
    // Namespace first: probe tokens are routed to the PLM before the business
    // table can silently drop them (design §11.2). `local_nic`/`jfc_index`
    // identify the polling slot for the interface-8 reports; the fence/stop
    // drains pass the record's local NIC and no JFC index.
    bool dispatchCompletion(const Completion& completion, int local_nic = -1,
                            uint64_t jfc_index = 0);
    bool pollBusinessSlot(size_t slot_index);
    bool pollProbeSlot(size_t slot_index);
    void handleLocalDeviceFailure(const UbContextPtr& context);
    void progressLocalDeviceFailure(const UbContextPtr& context);
    void scanTimeouts();
    // F6a: one discovered deadline fence. The poller only claims and enqueues;
    // the native quiesce runs on the control executor.
    struct TimeoutFenceJob {
        uint64_t token{0};
        std::shared_ptr<BusinessRecord> record;
        // Attempt generation for log correlation only; the exactly-once
        // guarantee still comes from the gate claim and eraseRecord()'s
        // pointer identity check.
        uint32_t attempt{0};
    };
    void enqueueTimeoutFence(const std::shared_ptr<BusinessRecord>& record);
    // Locks endpoint_drain_mutex_ and executes the fence.
    void runTimeoutFence(const TimeoutFenceJob& job);
    // Same work with endpoint_drain_mutex_ already held by the caller. The
    // degraded (no control executor) path calls this from scanTimeouts, which
    // owns the lock; taking it again would self-deadlock.
    void runTimeoutFenceLocked(const TimeoutFenceJob& job);
    // F6c helpers. requestDeadlineNs() returns 0 when the request has no
    // deadline or the switch is off, so every caller collapses to the legacy
    // path in that case.
    [[nodiscard]] uint64_t requestDeadlineNs(const PendingSlice& pending) const;
    [[nodiscard]] bool requestExpired(const PendingSlice& pending,
                                      uint64_t now_ns) const;
    void expirePending(const PendingSlice& pending,
                       std::atomic<uint64_t>& counter);
    // F6a2: only the native endpoint barrier moves off the poller; the context
    // state transition and the PLM notification stay synchronous.
    void enqueueDeviceRetire(const UbContextPtr& context);
    void noteControlJobFinished();
    void drainPendingControlJobs();
    void startControlExecutor();
    void stopControlExecutor();
    static WrFacts makeWrFacts(const Completion& completion);
    [[nodiscard]] std::shared_ptr<BusinessRecord> findRecord(
        uint64_t token) const;
    void releaseOwnerResources(BusinessRecord& record,
                               OwnerDisposition disposition);
    void eraseRecord(uint64_t token, const BusinessRecord* record);
    void deliverSettlement(BusinessRecord& record);
    void recordSuccessEvidence(BusinessRecord& record);

    // gate::Sink. All of them run on the thread that won the corresponding
    // claim, outside the registry lock.
    void onSettle(CompletionRecord& record, LogicalOutcome outcome) override;
    void onOwners(CompletionRecord& record,
                  OwnerDisposition disposition) override;
    void onFault(CompletionRecord& record, FaultKind kind) override;
    void onBlocked(CompletionRecord& record, BlockReason reason) override;
    void onDuplicate(CompletionRecord& record) override;
    void rememberEndpointDrain(const std::shared_ptr<UbEndpoint>& endpoint);
    void forgetEndpointDrain(const std::shared_ptr<UbEndpoint>& endpoint);
    void failUnposted(const PendingSlice& pending,
                      TransferStatusEnum outcome = FAILED);
    uint64_t nextCompletionToken();
    // `location` is the local buffer's location as resolved by
    // localLocationOf().
    std::vector<LocalDeviceCandidate> orderedLocalDevices(
        const PendingSlice& pending, const std::string& location) const;
    std::string localLocationOf(const PendingSlice& pending) const;
    static std::vector<Topology::NicID> orderedRemoteDevices(
        const SegmentDesc& segment, const BufferDesc& buffer,
        const UbBufferMetadata& metadata);

    std::shared_ptr<UrmaAdapter> adapter_;
    std::vector<UbContextPtr> contexts_;
    std::unordered_map<Topology::NicID, UbContextPtr> context_by_topology_id_;
    std::shared_ptr<Topology> local_topology_;
    SegmentManager* segment_manager_;
    UbBufferManager* buffers_;
    RailMonitor* rail_monitor_;
    QuotaManager* quota_;
    const UbParams params_;
    EndpointResolver endpoint_resolver_;
    EndpointRetirer endpoint_retirer_;
    LocalDeviceFailureHandler local_device_failure_handler_;
    // Serializes endpoint-store retirement progress across JFC pollers. The
    // handler temporarily moves quarantined endpoints out of the store while
    // retrying cleanup, so a second poller must not observe a false empty
    // barrier and reactivate the context early.
    mutable std::mutex local_device_failure_mutex_;
    // F6a/F6a2 control-executor bookkeeping. `control_jobs_pending_` counts
    // both fence and device-retire jobs so stop() can wait for every queued job
    // before it starts its own shutdown fences.
    std::shared_ptr<ControlExecutor> control_executor_;
    bool control_executor_owned_{false};
    std::mutex control_jobs_mutex_;
    std::condition_variable control_jobs_cv_;
    uint64_t control_jobs_pending_{0};
    std::atomic<uint64_t> pooled_lane_ref_unmapped_{0};
    std::atomic<uint64_t> eid_scope_denied_{0};
    std::atomic<uint64_t> expired_in_queue_{0};
    std::atomic<uint64_t> expired_parked_{0};
    std::atomic<uint64_t> expired_pre_post_{0};
    std::atomic<uint64_t> expired_inflight_{0};
    std::atomic<uint64_t> timeout_fence_submitted_{0};
    std::atomic<uint64_t> timeout_fence_rejected_{0};
    std::atomic<uint64_t> device_retire_rejected_{0};
    // Deduplicates device-retirement jobs per local NIC: the poller sees the
    // failed context repeatedly and must not queue one job per poll.
    std::mutex device_retire_mutex_;
    std::unordered_set<Topology::NicID> device_retire_queued_;

    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::array<std::deque<PendingSlice>, PRIO_LOW + 1> queues_;
    // Slices currently in `queues_`, kept incrementally so submit() can apply
    // the all-or-nothing budget check without walking every queue (design
    // §6.1/§6.3). This stays the authoritative count.
    size_t queued_{0};
    // W3 defer buckets. Always mutated under queue_mutex_ together with the
    // queue, so "park" and "bump" are ordered (see post_scheduler.h).
    DeferRegistry<PendingSlice> defer_;
    // F9.1: last time a parked-item reflow was forced by the bounded cadence.
    // Atomic because every posting worker runs popPending().
    std::atomic<uint64_t> last_reflow_ns_{0};

    // ---- idle spin state (yuanhao-1113, performance) ---------------------
    // Lock-free mirror of queued_, maintained under queue_mutex_ at every site
    // that mutates queued_. Only the idle posting spinner reads it, as a hint;
    // the queues stay authoritative and are rechecked under the lock. Parked
    // (deferred) slices are deliberately NOT counted: while everything is
    // parked the posting thread must fall through to the bounded wait so the
    // defer bucket wakeups drive it.
    std::atomic<size_t> queued_hint_{0};
    // At most one posting worker spins at a time.
    std::atomic<bool> post_spinner_active_{false};
    // steadyNowNs() of the most recent post attempt; anchors the idle-spin
    // windows of pollers and the posting spinner.
    std::atomic<uint64_t> last_post_ns_{0};

    mutable std::mutex records_mutex_;
    mutable std::mutex endpoint_drain_mutex_;
    // Kept under its original name: stop() still waits on it for the registry
    // to drain.
    std::condition_variable inflight_cv_;
    // Keyed by completion token. Renamed from `inflight_` because W5 adds a
    // second, deadline-ordered index next to it.
    std::unordered_map<uint64_t, std::shared_ptr<BusinessRecord>> records_;
    // Live records per pooled lane, keyed globally (Context incarnation +
    // lane generation) because this single ledger spans every Context.
    // Maintained under records_mutex_ so the drain controller can ask "is this
    // lane fully claimed?" in O(1) instead of scanning every in-flight record
    // on each tick.
    std::unordered_map<LaneRefKey, size_t, LaneRefKeyHash> pooled_lane_refs_;
    // Endpoints whose ERROR transition has not yet reached its native flush
    // fence remain owned here even if every logical token completes naturally.
    std::unordered_map<uint64_t, std::shared_ptr<UbEndpoint>>
        draining_endpoints_;

    struct BusinessSlot {
        std::shared_ptr<UbJfc> jfc;
        UbContextPtr context;
        // Drain controller of the slot's context (design §4.3). Kept per slot
        // so a marker never has to look it up on the poll path.
        std::shared_ptr<JettyDrainController> drain;
    };
    struct ProbeSlot {
        JfcPtr jfc;
        UbContextPtr context;
    };
    std::vector<BusinessSlot> business_slots_;
    std::vector<ProbeSlot> probe_slots_;
    ProbeLinkManager* probe_manager_{nullptr};
    mutable std::mutex probe_hint_mutex_;
    std::function<void(uint64_t)> probe_refresh_hint_;
    // Read-only adjacency policy; nullptr when no topology was declared.
    const ProbeTopology* probe_topology_{nullptr};
    // Declared-edge kParamError invalidation (closed-loop design §5.6.3).
    ParamErrorEdgeTracker param_error_edges_;
    std::atomic<uint64_t> business_deadline_timeouts_{0};
    std::atomic<uint64_t> last_rail_log_ns_{0};
    std::atomic<uint64_t> declared_candidates_total_{0};
    std::atomic<uint64_t> declared_candidates_denied_{0};
    // Bounded record of unproven destructions, used to attribute a completion
    // whose token no longer maps to a live record (design §7.1/§7.2).
    std::shared_ptr<TombstoneTable> tombstones_;
    // W3 per-path in-flight ledger (scoring only, D-11).
    PathLoadLedger path_load_;

    // W3 business counters. Cumulative except the two sampled depths.
    std::atomic<uint64_t> submit_backpressure_{0};
    // Slices that were admissible when the batch capacity verdict was taken but
    // could not be marked because a cancellation won the race. Not an error:
    // the slice is terminal already. Counted so the gap stays observable.
    std::atomic<uint64_t> submit_partial_cancelled_{0};
    std::atomic<uint64_t> submit_accepted_{0};
    std::array<std::atomic<uint64_t>, kDeferReasonCount> defer_pending_{};
    std::array<std::atomic<uint64_t>, kDeferReasonCount> defer_epoch_wakeup_{};
    std::atomic<uint64_t> defer_reflow_repark_{0};
    std::atomic<uint64_t> defer_bucket_snapshot_retry_{0};
    std::atomic<uint64_t> reflow_requeued_{0};
    std::atomic<uint64_t> reflow_cancelled_{0};
    std::atomic<uint64_t> task_K_wait_{0};
    std::atomic<uint64_t> pool_lease_attempts_{0};
    std::atomic<uint64_t> pool_lease_misses_{0};
    std::atomic<uint64_t> pool_stop_rejections_{0};
    std::atomic<uint64_t> pool_hard_errors_{0};
    std::atomic<uint64_t> claim_rejected_stopping_{0};
    std::atomic<uint64_t> claim_rejected_member_{0};
    std::atomic<uint64_t> claim_rejected_generation_{0};
    std::atomic<uint64_t> claim_rejected_error_epoch_{0};
    std::atomic<uint64_t> claim_disabled_{0};
    std::atomic<uint64_t> claim_after_isolation_{0};
    std::atomic<uint64_t> unknown_token_tombstoned_{0};
    std::atomic<uint64_t> post_on_isolated_path_{0};
    std::atomic<uint64_t> pooled_return_{0};
    std::atomic<uint64_t> pooled_retire_{0};
    // Only one poller per interval emits the periodic business summary.
    std::atomic<uint64_t> last_business_log_ns_{0};

    std::vector<std::thread> posting_threads_;
    std::vector<std::thread> polling_threads_;
    std::atomic<bool> accepting_{false};
    std::atomic<bool> posting_{false};
    std::atomic<bool> polling_{false};
    std::atomic<bool> timeout_scans_enabled_{false};
    std::atomic<uint64_t> next_token_{1};

    // Completion gate plumbing. Declared last so every member it touches during
    // construction is already initialized.
    GateContext gate_ctx_;
    CountingEntityEventSink entity_events_;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_WORKERS_H_
