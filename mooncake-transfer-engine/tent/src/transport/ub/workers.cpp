// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/workers.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <new>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <utility>

#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include <glog/logging.h>

#include "tent/runtime/platform.h"
#include "tent/transport/ub/endpoint.h"

namespace mooncake::tent::ub {
namespace {

uint64_t deadlineAfter(uint64_t now_ns, uint64_t timeout_ns) {
    return timeout_ns > std::numeric_limits<uint64_t>::max() - now_ns
               ? std::numeric_limits<uint64_t>::max()
               : now_ns + timeout_ns;
}

bool retryableStatus(const Status& status) {
    return status.IsDeviceNotFound() || status.IsNeedsRefreshCache() ||
           status.IsRpcServiceError() || status.IsInternalError() ||
           status.IsRdmaError();
}

// Business-side identity mapping (design §5.4). The conversion lives on the
// owner side, never in the shared kernel, so a P0-D change to UbPostPath cannot
// drag the probe side along with it.
RecordPath makeBusinessRecordPath(const UbPostPath& path) {
    return RecordPath{path.local_topology_id, path.remote_segment_id,
                      path.remote_device_id};
}

// Rail health and quota are keyed by the physical rail, which deliberately
// excludes the endpoint generation (see UbRailKey). These read-only probes
// therefore only need a generation that satisfies UbPostPath::valid(); real
// endpoint generations are monotonic and never below this value.
// W3g-c: pre-resolution health queries are path-level, not generation-level.
// Zero means "no generation claim" to the PLM, so a bootstrap-published edge
// with a real endpoint-generation anchor does not report every candidate as
// `probe_evidence_unmapped`. The claim-time check still uses the real
// generation and remains strict.
constexpr uint64_t kRailProbeEndpointGeneration = 0;

// Number of candidates always taken in pre-score order, regardless of which
// local device they belong to. Two is the floor that keeps tryAcquireFirst from
// deferring a slice when its top pick is preempted by another worker, including
// on a single-local-device topology where per-device coverage alone would leave
// only one candidate.
constexpr size_t kMinResolvedCandidates = 2;

// Re-evaluation floor for deferred items whose bucket has no reliable bump
// source (device recovery cooldown, PLM health without a business wake seam,
// pool creation repeatedly failing). 10ms keeps the retry cheap while staying
// far below any user-visible timeout; the epoch bumps remain the fast path.
constexpr uint64_t kTimedDeferNs = 10'000'000ULL;

// Idle-spin pause (yuanhao-1113, performance). Cheaper than a scheduler
// handoff while a poller or the posting spinner is waiting for work.
inline void cpuRelax() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
}

}  // namespace

struct UbWorkers::Route {
    SegmentDescRef pin;
    BufferDesc* remote_buffer{nullptr};
    // Decoded once per slice and reused by device ordering and the import.
    UbBufferMetadata metadata;
    std::vector<Topology::NicID> remote_devices;
    // Declaration gate for this peer, computed once per route.
    AdjacencyView adjacency;
    // Synthetic peer id for the probe graph; 0 when the peer is not declared.
    uint64_t probe_peer_id{LOCAL_SEGMENT_ID};
    // Local buffer location, resolved once per slice (yuanhao-1113). A buffer
    // whose registration resolved to one concrete location makes this a cached
    // string, so the posting path issues no NUMA probe per slice.
    std::string local_location{kWildcardLocation};
};

// Owner-side half of a completion record (design §5.3). The shared kernel
// holds identity and one-shot facts; everything below belongs to the business
// data path and never leaks into the gate.
struct UbWorkers::BusinessRecord {
    CompletionRecord core;
    PendingSlice pending;
    // Full business identity: slice settlement needs it, and the gate's
    // attempt_key is only an observability/claiming scalar.
    UbAttemptToken attempt;
    UbPostPath path;
    ProbePathKey probe_key;
    std::shared_ptr<UbEndpoint> endpoint;
    LocalSegmentPtr local_segment;
    RemoteSegmentPtr remote_segment;
    QuotaReservation quota;
    uint64_t posted_ns{0};
    uint64_t deadline_ns{0};
    // F6c: the deadline above came from Request::deadline_ns (it was earlier
    // than posted + slice_timeout), so the fence is executing the request
    // deadline rather than a WR budget overrun.
    bool deadline_from_request{false};

    // ---- W3 pooled ownership (design §5.1) --------------------------------
    // nullptr means the W2 legacy path: nothing below is touched, and the
    // member-return code never runs (D-2 mode gate).
    std::shared_ptr<SendJettyPool> pool;
    // Handed over exactly once to returnOnce()/retireFaulted() from
    // onOwners(). A lease still valid at destruction is a responsibility leak
    // and the pool quarantines the member (diagnostic, not a release path).
    SliceJettyLease lease;
    // Identity of the leased member for the lane-scoped drain query. The lease
    // itself is handed over on the finalize path, so the lane is snapshotted
    // here while the lease is still valid.
    PoolLaneKey pooled_lane{};
    // Context incarnation of the leased member. The lane ledger spans all
    // Contexts, so pooled_lane.lane_generation alone is ambiguous (INV-ID-3).
    uint64_t context_incarnation{0};
    size_t credit_jfc_index{0};
    uint64_t credit_token{0};
    // The claim anchor: snapshotted before the lease, compared after it, and
    // written into core.fault_epoch so business evidence is generation-mapped
    // (design §5.1/§10.4).
    uint64_t fault_epoch_snapshot{0};
    // True while this record owns one task-level K slot (D-6).
    bool attempt_slot_held{false};
};

UbWorkers::UbWorkers(std::shared_ptr<UrmaAdapter> adapter,
                     std::vector<UbContextPtr> contexts,
                     std::shared_ptr<Topology> local_topology,
                     SegmentManager* segment_manager, UbBufferManager* buffers,
                     RailMonitor* rail_monitor, QuotaManager* quota,
                     UbParams params, EndpointResolver endpoint_resolver,
                     EndpointRetirer endpoint_retirer,
                     LocalDeviceFailureHandler local_device_failure_handler,
                     ProbeLinkManager* probe_manager,
                     std::vector<ProbeJfcSlot> probe_jfcs,
                     std::shared_ptr<TombstoneTable> tombstones,
                     const ProbeTopology* probe_topology,
                     std::shared_ptr<ControlExecutor> control_executor)
    : adapter_(std::move(adapter)),
      contexts_(std::move(contexts)),
      local_topology_(std::move(local_topology)),
      segment_manager_(segment_manager),
      buffers_(buffers),
      rail_monitor_(rail_monitor),
      quota_(quota),
      params_(std::move(params)),
      endpoint_resolver_(std::move(endpoint_resolver)),
      endpoint_retirer_(std::move(endpoint_retirer)),
      local_device_failure_handler_(std::move(local_device_failure_handler)),
      probe_manager_(probe_manager),
      probe_topology_(probe_topology),
      param_error_edges_(
          params_.probe.adjacency_policy.param_error_invalidate_threshold),
      tombstones_(std::move(tombstones)),
      control_executor_(std::move(control_executor)) {
    gate_ctx_.retry_timeout_write = params_.retry_timeout_write;
    for (const auto& context : contexts_) {
        if (!context) continue;
        context_by_topology_id_[context->topologyId()] = context;
        auto drain = context->drainController();
        for (const auto& jfc : context->jfcs()) {
            if (jfc) {
                business_slots_.push_back(BusinessSlot{jfc, context, drain});
            }
        }
    }
    for (auto& slot : probe_jfcs) {
        if (!slot.jfc) continue;
        auto context = context_by_topology_id_.find(slot.local_nic);
        if (context == context_by_topology_id_.end() || !context->second) {
            LOG(WARNING) << "UB probe JFC for local NIC " << slot.local_nic
                         << " has no matching context; its completions will "
                            "not be polled";
            continue;
        }
        probe_slots_.push_back(ProbeSlot{std::move(slot.jfc), context->second});
    }
}

UbWorkers::~UbWorkers() { (void)stop(); }

Status UbWorkers::start() {
    bool expected = false;
    if (!accepting_.compare_exchange_strong(expected, true,
                                            std::memory_order_acq_rel)) {
        return Status::InvalidArgument("UB workers already started" LOC_MARK);
    }
    if (!adapter_ || contexts_.empty() || business_slots_.empty() ||
        !segment_manager_ || !buffers_ || !rail_monitor_ || !quota_ ||
        !endpoint_resolver_ || params_.worker_count == 0 ||
        params_.poller_count == 0) {
        accepting_.store(false, std::memory_order_release);
        return Status::InvalidArgument(
            "UB workers have incomplete dependencies" LOC_MARK);
    }

    // F6a: the poller only discovers and enqueues deadline fences. A refused
    // thread degrades to the inline path instead of failing the transport.
    startControlExecutor();
    posting_.store(true, std::memory_order_release);
    polling_.store(true, std::memory_order_release);
    timeout_scans_enabled_.store(true, std::memory_order_release);
    try {
        posting_threads_.reserve(params_.worker_count);
        for (uint32_t i = 0; i < params_.worker_count; ++i) {
            posting_threads_.emplace_back(&UbWorkers::postingLoop, this, i);
        }
        const size_t poller_count =
            std::min<size_t>(params_.poller_count, business_slots_.size());
        polling_threads_.reserve(poller_count);
        for (size_t i = 0; i < poller_count; ++i) {
            polling_threads_.emplace_back(&UbWorkers::pollingLoop, this, i);
        }
    } catch (const std::exception& error) {
        accepting_.store(false, std::memory_order_release);
        posting_.store(false, std::memory_order_release);
        polling_.store(false, std::memory_order_release);
        timeout_scans_enabled_.store(false, std::memory_order_release);
        queue_cv_.notify_all();
        for (auto& thread : posting_threads_) {
            if (thread.joinable()) thread.join();
        }
        for (auto& thread : polling_threads_) {
            if (thread.joinable()) thread.join();
        }
        posting_threads_.clear();
        polling_threads_.clear();
        return Status::InternalError(std::string("Cannot start UB workers: ") +
                                     error.what() + LOC_MARK);
    }
    return Status::OK();
}

Status UbWorkers::stop() {
    accepting_.store(false, std::memory_order_release);
    // Fence retry enqueue before draining the queues. A completion racing
    // with shutdown will turn its retry-pending slice into CANCELED instead of
    // leaving a new queue entry behind the drain pass.
    posting_.store(false, std::memory_order_release);
    timeout_scans_enabled_.store(false, std::memory_order_release);
    queue_cv_.notify_all();
    // No new probe may be posted once the posting side is closed (design
    // §11.4). Timeout scans continue until the scheduler is joined below.
    if (probe_manager_ != nullptr) probe_manager_->stopPosting();

    std::vector<PendingSlice> abandoned;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& queue : queues_) {
            while (!queue.empty()) {
                abandoned.push_back(std::move(queue.front()));
                queue.pop_front();
            }
        }
        queued_ = 0;
        queued_hint_.store(0, std::memory_order_release);
        // Nothing may stay parked across a stop: every deferred slice is pulled
        // out and canceled below, so a parked item can never outlive the
        // posting threads that would have reflowed it (design §11.2).
        while (defer_.drainAll(abandoned, params_.defer_reflow_batch) != 0) {
        }
    }
    for (auto& pending : abandoned) {
        if (pending.slice) pending.slice->requestCancellation();
    }

    for (auto& thread : posting_threads_) {
        if (thread.joinable()) thread.join();
    }
    posting_threads_.clear();

    // Pool stop ordering (design §11.2): beginStop() runs after the posting
    // threads are joined, so no new lease can appear, but *before* the business
    // quiesce below, which still settles in-flight records through
    // returnOnce()/retireFaulted(). A stopping pool turns every later return
    // into a retirement, which is exactly the wanted shutdown semantic.
    for (const auto& context : contexts_) {
        if (!context) continue;
        if (auto pool = context->pooledSendPool()) pool->beginStop();
    }

    // Wait for a timeout scan that began before the flag change. scanTimeouts
    // rechecks under this mutex, so no new endpoint can enter the drain set
    // after this barrier and escape the shutdown snapshot.
    {
        std::lock_guard<std::mutex> lock(endpoint_drain_mutex_);
    }
    // F6a/F6a2: scans are disabled and the barrier above proves no scan is in
    // its claim section any more, so every job that can still touch a native
    // endpoint is already queued. Let them finish before this function starts
    // its own shutdown fences, then retire the executor.
    drainPendingControlJobs();
    stopControlExecutor();

    // Snapshot all native work after posting threads have joined; no new WR
    // can cross the adapter boundary beyond this point. Quiesce each endpoint
    // before stopping pollers. A failed fence leaves pollers, tokens, native
    // segments, quota, and endpoints intact so uninstall can be retried
    // safely; fixed sleeps are never treated as proof that DMA stopped.
    std::vector<std::shared_ptr<BusinessRecord>> still_inflight;
    std::vector<std::shared_ptr<UbEndpoint>> endpoints_to_fence;
    // W3 D-14/W3-IN-10: a pooled record's WR rode a leased member, so its
    // fence target is that member's Jetty. Fencing the endpoint instead would
    // prove nothing about the WR that is actually outstanding.
    std::vector<std::pair<JettyPtr, int>> pooled_jetties_to_fence;
    {
        std::lock_guard<std::mutex> lock(records_mutex_);
        still_inflight.reserve(records_.size());
        for (const auto& [_, record] : records_) {
            still_inflight.push_back(record);
            if (!record) continue;
            if (record->pool && record->lease.valid()) {
                const auto member = record->lease.member();
                if (member && member->jetty) {
                    pooled_jetties_to_fence.emplace_back(
                        member->jetty, record->core.path.local_nic);
                }
                continue;
            }
            if (record->endpoint) {
                endpoints_to_fence.push_back(record->endpoint);
            }
        }
        for (const auto& [_, endpoint] : draining_endpoints_) {
            if (endpoint) endpoints_to_fence.push_back(endpoint);
        }
    }
    for (const auto& record : still_inflight) {
        if (record && record->pending.slice) {
            record->pending.slice->requestCancellation();
        }
    }

    Status fence_error = Status::OK();
    std::unordered_set<UbEndpoint*> fenced;
    for (const auto& endpoint : endpoints_to_fence) {
        if (!endpoint || !fenced.insert(endpoint.get()).second) {
            continue;
        }
        std::vector<Completion> drained;
        auto status = endpoint->quiesce(params_.slice_timeout_ms, drained);
        for (const auto& completion : drained) {
            dispatchCompletion(completion, endpoint->key().local_topology_id);
        }
        if (!status.ok()) {
            rememberEndpointDrain(endpoint);
            if (fence_error.ok()) fence_error = status;
            continue;
        }
        forgetEndpointDrain(endpoint);
        if (endpoint_retirer_) endpoint_retirer_(endpoint);
    }
    if (!fence_error.ok()) {
        return fence_error;
    }
    // Pooled fences run after the endpoint fences and before the pollers stop.
    // A failed fence means responsibility is unproven, so the member must NOT
    // be deleted: every record keeps its lease, and the failure is reported so
    // uninstall can be retried (same contract as the endpoint fence above, and
    // the same reason the bottom-out below is not reached in that case).
    for (const auto& [jetty, local_nic] : pooled_jetties_to_fence) {
        std::vector<Completion> drained;
        auto status =
            adapter_->quiesceJetty(jetty, params_.slice_timeout_ms, drained);
        for (const auto& completion : drained) {
            dispatchCompletion(completion, local_nic);
        }
        if (!status.ok()) {
            LOG_EVERY_N(WARNING, 100)
                << "Cannot establish the pooled UB shutdown fence: "
                << status.ToString();
            if (fence_error.ok()) fence_error = status;
        }
    }
    if (!fence_error.ok()) {
        // Deliberately before the probe drain, the poller join and the
        // onUnposted bottom-out: the records still own their leases, so no
        // member is retired on unproven evidence.
        return fence_error;
    }
    {
        std::lock_guard<std::mutex> lock(records_mutex_);
        if (!draining_endpoints_.empty()) {
            return Status::InternalError(
                "UB endpoint drain set changed during shutdown" LOC_MARK);
        }
    }

    // Business quiesce is complete and the pollers are still running: join the
    // probe scheduler, let outstanding probe completions settle, and hand off
    // whatever remains (design §9.6/§11.4). The scheduler is joined before the
    // pollers stop so no timeout scan or interface-2 report can race them.
    if (probe_manager_ != nullptr) {
        const auto drained = probe_manager_->drainBeforePollStop(0);
        if (!drained.drained) {
            LOG(WARNING) << "UB probe shutdown handed off " << drained.unproven
                         << " unproven probe record(s) after draining "
                         << drained.settled;
        }
    }

    // Successful endpoint fences prove that any token still missing from the
    // JFC can no longer touch memory. Let pollers consume already-queued CRs,
    // then stop them and safely resolve any provider-lost token.
    if (!still_inflight.empty()) {
        std::unique_lock<std::mutex> lock(records_mutex_);
        (void)inflight_cv_.wait_for(lock, std::chrono::milliseconds(100),
                                    [this] { return records_.empty(); });
    }
    polling_.store(false, std::memory_order_release);
    for (auto& thread : polling_threads_) {
        if (thread.joinable()) thread.join();
    }
    polling_threads_.clear();

    still_inflight.clear();
    {
        std::lock_guard<std::mutex> lock(records_mutex_);
        for (auto& [_, record] : records_) {
            still_inflight.push_back(std::move(record));
        }
        records_.clear();
        pooled_lane_refs_.clear();
    }
    for (const auto& record : still_inflight) {
        // The bottom-out shares every claim bit with the CQE and timeout exits:
        // if any of them already disposed of the record, this call only
        // supplies the missing settlement, so a double release is impossible
        // (design §6.9). In pooled mode this also settles the lease: an
        // unposted record finalizes, retires its member and releases its
        // credit exactly once.
        gate::onUnposted(record->core, RecordDropReason::kShutdown, *this,
                         gate_ctx_);
    }
    inflight_cv_.notify_all();

    // Idle members are deleted only after the pollers are joined: an idle
    // member's JFC may still have marker/flush completions to consume
    // (design §11.2, RK6). Quarantined members are retried last; a delete that
    // still fails stays quarantined and keeps its budget.
    for (const auto& context : contexts_) {
        if (!context) continue;
        auto pool = context->pooledSendPool();
        if (!pool) continue;
        (void)pool->drainIdle();
        (void)pool->drainQuarantined();
    }
    return Status::OK();
}

Status UbWorkers::submit(const UbTask::Ptr& task, uint64_t device_mask) {
    if (!task) {
        return Status::InvalidArgument("Cannot submit a null UB task" LOC_MARK);
    }
    return submitBatch({task}, device_mask);
}

thread_local bool UbWorkers::fail_second_enqueue_for_test_ = false;

Status UbWorkers::submitBatch(const std::vector<UbTask::Ptr>& tasks,
                              uint64_t device_mask) {
    const bool fail_second_enqueue =
        std::exchange(fail_second_enqueue_for_test_, false);
    if (!accepting_.load(std::memory_order_acquire)) {
        return Status::InvalidArgument(
            "UB workers are not accepting work" LOC_MARK);
    }

    // ---- Phase 1: validate the whole batch before any slice state changes ---
    // yuanhao-1113 introduced the batch entry point; tent_merge's W3a rule is
    // that every verdict which can reject the work is taken *before*
    // markQueued(), because kQueued has no unmark path. Running the structural
    // checks for the entire batch first preserves that rule for a batch: a
    // null, duplicated or unsealed task can never leave an earlier task of the
    // same batch half-submitted.
    std::unordered_set<const UbTask*> unique_tasks;
    for (const auto& task : tasks) {
        if (!task) {
            return Status::InvalidArgument(
                "Cannot submit a null UB task" LOC_MARK);
        }
        if (!unique_tasks.insert(task.get()).second) {
            return Status::InvalidArgument(
                "Cannot submit the same UB task twice in one batch" LOC_MARK);
        }
        if (!task->snapshot().sealed) {
            return Status::InvalidArgument(
                "UB task must be sealed before submission" LOC_MARK);
        }
    }

    // ---- Phase 2: one capacity verdict for the batch, then mark and insert --
    // `need` counts only slices that can actually be queued: a slice that is
    // already terminal or canceled must not inflate the estimate, or a
    // cancellation storm would backpressure healthy submissions. Parked slices
    // count against the same budget: a slice waiting for a lease is just as
    // much queued work as one waiting for a lane.
    size_t need = 0;
    for (const auto& task : tasks) {
        for (const auto& slice : task->slices()) {
            if (slice && slice->queuedEligible()) ++need;
        }
    }

    bool stopped_during_submit = false;
    bool budget_exhausted = false;
    bool enqueue_failed = false;
    size_t marked_count = 0;
    size_t inserted_count = 0;
    std::array<size_t, PRIO_LOW + 1> inserted_counts{};
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!accepting_.load(std::memory_order_relaxed)) {
            stopped_during_submit = true;
        } else if (!businessQueueAdmits(
                       params_.queue_budget, static_cast<uint64_t>(queued_),
                       static_cast<uint64_t>(defer_.parkedTotal()),
                       static_cast<uint64_t>(need))) {
            budget_exhausted = true;
        } else {
            try {
                for (const auto& task : tasks) {
                    const int priority = std::clamp(task->request().priority,
                                                    static_cast<int>(PRIO_HIGH),
                                                    static_cast<int>(PRIO_LOW));
                    for (const auto& slice : task->slices()) {
                        if (!slice || !slice->markQueued()) continue;
                        ++marked_count;
                        if (fail_second_enqueue && inserted_count == 1) {
                            throw std::length_error(
                                "Injected UB enqueue failure");
                        }
                        PendingSlice entry{task,
                                           slice,
                                           device_mask,
                                           priority,
                                           task->request().target_id,
                                           task->request().opcode};
                        entry.submit_ns = steadyNowNs();
                        queues_[priority].push_back(std::move(entry));
                        ++inserted_counts[priority];
                        ++inserted_count;
                        ++queued_;
                    }
                }
            } catch (const std::bad_alloc&) {
                enqueue_failed = true;
            } catch (const std::length_error&) {
                enqueue_failed = true;
            }
            if (enqueue_failed) {
                // Roll the queues back to their pre-batch shape. The slices
                // stay marked kQueued because there is no unmark path, so the
                // whole batch is canceled below instead of left unqueueable.
                for (size_t priority = 0; priority < queues_.size();
                     ++priority) {
                    while (inserted_counts[priority] != 0) {
                        queues_[priority].pop_back();
                        --queued_;
                        --inserted_counts[priority];
                    }
                }
            }
            // Single write under queue_mutex_: the hint can never drift from
            // the authoritative count.
            queued_hint_.store(queued_, std::memory_order_release);
        }
    }
    if (enqueue_failed) {
        // A markQueued() failure or enqueue exception may occur after earlier
        // slices changed state. None of them are visible to posting workers;
        // cancel every task of the batch so no slice remains orphaned in
        // kQueued and the queue budget stays honest.
        for (const auto& task : tasks) {
            (void)task->requestCancellation();
        }
        queue_cv_.notify_all();
        return Status::InternalError(
            "Unable to enqueue UB task batch" LOC_MARK);
    }
    if (stopped_during_submit) {
        return Status::InvalidArgument(
            "UB workers stopped during submission" LOC_MARK);
    }
    // A short mark is NOT an error. `need` is sampled before the lock and a
    // concurrent cancellation can make a slice non-queueable in between; such a
    // slice is already terminal, so skipping it leaves nothing to do. Failing
    // here would return an error while the remaining slices are already in
    // queues_, i.e. the caller would see "rejected" for work that will run, and
    // a retry would submit the prefix twice.
    if (marked_count < need) {
        submit_partial_cancelled_.fetch_add(need - marked_count,
                                            std::memory_order_relaxed);
    }
    if (budget_exhausted) {
        // Backpressure, not an error: the caller may resubmit the whole batch
        // and no slice was touched.
        submit_backpressure_.fetch_add(1, std::memory_order_relaxed);
        return Status::TooManyRequests(
            "UB business queue budget exhausted" LOC_MARK);
    }
    submit_accepted_.fetch_add(1, std::memory_order_relaxed);
    if (need != 0) queue_cv_.notify_all();
    return Status::OK();
}

Status UbWorkers::cancel(const UbTask::Ptr& task) {
    if (!task) {
        return Status::InvalidArgument("Cannot cancel a null UB task" LOC_MARK);
    }
    (void)task->requestCancellation();
    // A canceled slice that is currently parked would otherwise keep its queue
    // budget until some unrelated release bumps its bucket. Bumping every
    // bucket makes the next reflow visit it: the reflow cancels it (it is
    // already terminal) and frees the budget (design §6.1).
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (size_t index = 0; index < kDeferReasonCount; ++index) {
            bumpLocked(static_cast<DeferReason>(index));
        }
    }
    queue_cv_.notify_all();
    return Status::OK();
}

size_t UbWorkers::queuedCount() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return queued_;
}

size_t UbWorkers::parkedCount() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return defer_.parkedTotal();
}

UbBusinessStatsSnapshot UbWorkers::businessStats() const {
    UbBusinessStatsSnapshot out;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        out.queue_depth = queued_;
        out.parked_depth = defer_.parkedTotal();
        for (size_t i = 0; i < kDeferReasonCount; ++i) {
            out.defer_pending[i] =
                defer_pending_[i].load(std::memory_order_relaxed);
            out.defer_epoch_wakeup[i] =
                defer_epoch_wakeup_[i].load(std::memory_order_relaxed);
        }
    }
    out.submit_backpressure =
        submit_backpressure_.load(std::memory_order_relaxed);
    out.submit_accepted = submit_accepted_.load(std::memory_order_relaxed);
    out.defer_reflow_repark =
        defer_reflow_repark_.load(std::memory_order_relaxed);
    out.defer_bucket_snapshot_retry =
        defer_bucket_snapshot_retry_.load(std::memory_order_relaxed);
    out.reflow_requeued = reflow_requeued_.load(std::memory_order_relaxed);
    out.reflow_cancelled = reflow_cancelled_.load(std::memory_order_relaxed);
    out.task_K_wait = task_K_wait_.load(std::memory_order_relaxed);
    out.pool_lease_attempts =
        pool_lease_attempts_.load(std::memory_order_relaxed);
    out.pool_lease_misses = pool_lease_misses_.load(std::memory_order_relaxed);
    out.pool_stop_rejections =
        pool_stop_rejections_.load(std::memory_order_relaxed);
    out.pool_hard_errors = pool_hard_errors_.load(std::memory_order_relaxed);
    out.business_param_error_invalid_edge = param_error_edges_.invalidations();
    out.business_deadline_timeouts =
        business_deadline_timeouts_.load(std::memory_order_relaxed);
    out.expired_in_queue = expired_in_queue_.load(std::memory_order_relaxed);
    out.expired_parked = expired_parked_.load(std::memory_order_relaxed);
    out.expired_pre_post = expired_pre_post_.load(std::memory_order_relaxed);
    out.expired_inflight = expired_inflight_.load(std::memory_order_relaxed);
    out.pooled_lane_ref_unmapped =
        pooled_lane_ref_unmapped_.load(std::memory_order_relaxed);
    out.timeout_fence_submitted =
        timeout_fence_submitted_.load(std::memory_order_relaxed);
    out.timeout_fence_rejected =
        timeout_fence_rejected_.load(std::memory_order_relaxed);
    out.device_retire_rejected =
        device_retire_rejected_.load(std::memory_order_relaxed);
    out.declared_candidates_total =
        declared_candidates_total_.load(std::memory_order_relaxed);
    out.declared_candidates_denied =
        declared_candidates_denied_.load(std::memory_order_relaxed);
    out.claim_rejected_stopping =
        claim_rejected_stopping_.load(std::memory_order_relaxed);
    out.claim_rejected_member =
        claim_rejected_member_.load(std::memory_order_relaxed);
    out.claim_rejected_generation =
        claim_rejected_generation_.load(std::memory_order_relaxed);
    out.claim_rejected_error_epoch =
        claim_rejected_error_epoch_.load(std::memory_order_relaxed);
    out.claim_disabled = claim_disabled_.load(std::memory_order_relaxed);
    out.claim_after_isolation =
        claim_after_isolation_.load(std::memory_order_relaxed);
    out.unknown_token_tombstoned =
        unknown_token_tombstoned_.load(std::memory_order_relaxed);
    out.post_on_isolated_path =
        post_on_isolated_path_.load(std::memory_order_relaxed);
    out.pooled_return = pooled_return_.load(std::memory_order_relaxed);
    out.pooled_retire = pooled_retire_.load(std::memory_order_relaxed);
    out.path_inflight_peak = path_load_.peakWrs();
    // Saturating: an imbalance below zero is a bookkeeping defect that must
    // surface as zero rather than as a huge unsigned value.
    const uint64_t published = path_load_.publishedTotal();
    const uint64_t finalized = path_load_.finalizedTotal();
    out.path_load_imbalance = finalized > published ? 0 : published - finalized;
    // Pool-side double returns are the external counterpart of the gate's
    // one_shot_violations: both must stay at zero.
    uint64_t double_returns = 0;
    for (const auto& context : contexts_) {
        if (!context) continue;
        const auto pool = context->pooledSendPool();
        if (!pool) continue;
        double_returns += pool->stats().double_returns;
    }
    out.pool_return_double = double_returns;
    return out;
}

size_t UbWorkers::inflightCount() const {
    std::lock_guard<std::mutex> lock(records_mutex_);
    return records_.size();
}

bool UbWorkers::withinIdleSpin(uint64_t now_ns, uint64_t last_ns,
                               uint32_t window_us) const {
    if (window_us == 0 || last_ns == 0) return false;
    // last_ns may be published by another thread after now_ns was sampled.
    if (last_ns >= now_ns) return true;
    return now_ns - last_ns < static_cast<uint64_t>(window_us) * 1000ULL;
}

void UbWorkers::spinForPending() {
    if (params_.post_idle_spin_us == 0) return;
    if (queued_hint_.load(std::memory_order_acquire) != 0) return;
    bool expected = false;
    if (!post_spinner_active_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
        return;
    }
    // Spin only while the previous post is recent. The caller always falls
    // through to the locked condition-variable wait, which rechecks the
    // queues, so leaving the spin early or late can never lose a wakeup.
    while (posting_.load(std::memory_order_acquire) &&
           queued_hint_.load(std::memory_order_acquire) == 0 &&
           withinIdleSpin(steadyNowNs(),
                          last_post_ns_.load(std::memory_order_relaxed),
                          params_.post_idle_spin_us)) {
        cpuRelax();
    }
    post_spinner_active_.store(false, std::memory_order_release);
}

UbWorkers::PopResult UbWorkers::popPending(PendingSlice& pending) {
    // Idle spin runs before the lock: it only shortens the handoff of a
    // pending queue, never replaces the locked predicate below.
    spinForPending();
    std::unique_lock<std::mutex> lock(queue_mutex_);
    // The wait is bounded by one millisecond instead of the W2 fixed 50us sleep
    // (design §7.4). Two things need it: a timed defer (a device in recovery
    // cooldown) has no release site that could bump a bucket, and a PLM health
    // transition may land without the scheduler being the bumper. The bound is
    // idle-only: as soon as any queue item or actionable parked item exists the
    // wait returns immediately.
    (void)queue_cv_.wait_for(lock, std::chrono::milliseconds(1), [this] {
        if (!posting_.load(std::memory_order_acquire)) return true;
        for (const auto& queue : queues_) {
            if (!queue.empty()) return true;
        }
        return defer_.hasActionable(steadyNowNs());
    });
    if (!posting_.load(std::memory_order_acquire)) return PopResult::kStopped;
    // F9.1: kReflow used to be reachable only when every queue was empty, so a
    // steady ready-queue supply starved parked items forever. Force one bounded
    // reflow opportunity per gap. Nothing is popped here yet, so the ready item
    // is only re-queued by the reflow pass: no work is lost, only one pop's
    // worth of latency is yielded every `defer_reflow_max_gap_ms`.
    const uint64_t reflow_gap_ns =
        static_cast<uint64_t>(params_.defer_reflow_max_gap_ms) * 1'000'000ULL;
    if (reflow_gap_ns != 0 && defer_.hasDirty()) {
        const uint64_t now = steadyNowNs();
        uint64_t last = last_reflow_ns_.load(std::memory_order_relaxed);
        if (now - last >= reflow_gap_ns &&
            last_reflow_ns_.compare_exchange_strong(
                last, now, std::memory_order_acq_rel)) {
            return PopResult::kReflow;
        }
    }
    for (auto& queue : queues_) {
        if (!queue.empty()) {
            pending = std::move(queue.front());
            queue.pop_front();
            if (queued_ != 0) --queued_;
            // Single write under queue_mutex_: the hint can never drift.
            queued_hint_.store(queued_, std::memory_order_relaxed);
            return PopResult::kPending;
        }
    }
    // Nothing runnable in the queue: hand the loop back so it can reflow the
    // stale/timed-out parked items. This replaces the W2 blind requeue.
    return PopResult::kReflow;
}

void UbWorkers::postingLoop(size_t worker_index) {
    while (posting_.load(std::memory_order_acquire)) {
        PendingSlice pending;
        switch (popPending(pending)) {
            case PopResult::kStopped:
                return;
            case PopResult::kReflow:
                (void)reflowDeferred();
                continue;
            case PopResult::kPending:
                // An empty `pending` means "woke up, nothing to do"; treating
                // it as work would dereference a null slice below.
                if (pending.slice) processPending(pending, worker_index);
                continue;
        }
    }
}

void UbWorkers::bumpLocked(DeferReason reason) { defer_.bump(reason); }

size_t UbWorkers::reflowDeferred() {
    std::vector<PendingSlice> ready;
    size_t drained = 0;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        std::array<uint64_t, kDeferReasonCount> wakeups{};
        drained = defer_.drainDirty(ready, params_.defer_reflow_batch,
                                    steadyNowNs(), &wakeups);
        for (size_t i = 0; i < kDeferReasonCount; ++i) {
            if (wakeups[i] != 0) {
                defer_epoch_wakeup_[i].fetch_add(wakeups[i],
                                                 std::memory_order_relaxed);
            }
        }
        const uint64_t reflow_now = steadyNowNs();
        for (auto& item : ready) {
            // F6c stage 2: waking up because the request deadline passed means
            // "cancel", never "requeue".
            if (requestExpired(item, reflow_now)) {
                expirePending(item, expired_parked_);
                continue;
            }
            // requeueAfterDefer(), not markQueued(): a slice deferred before
            // beginAttempt() is still kQueued and markQueued() would refuse it,
            // which used to cancel a healthy slice on every reflow (W3 review
            // B1).
            if (!posting_.load(std::memory_order_relaxed) || !item.slice ||
                item.slice->cancellationRequested() ||
                !item.slice->requeueAfterDefer()) {
                if (item.slice) item.slice->requestCancellation();
                reflow_cancelled_.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            queues_[std::clamp(item.priority, static_cast<int>(PRIO_HIGH),
                               static_cast<int>(PRIO_LOW))]
                .push_back(std::move(item));
            ++queued_;
            queued_hint_.store(queued_, std::memory_order_release);
            reflow_requeued_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    if (drained != 0) queue_cv_.notify_all();
    return drained;
}

void UbWorkers::pollingLoop(size_t poller_index) {
    // The poller count is unchanged by probe support: probe slots are
    // distributed across the existing poller set (design §3.3 / §11.1).
#if defined(__linux__)
    // The default 50us timer slack would stretch the 20us idle sleep below
    // to as much as 70us of completion latency (yuanhao-1113).
    (void)prctl(PR_SET_TIMERSLACK, 1UL, 0UL, 0UL, 0UL);
#endif
    const size_t poller_count = std::max<size_t>(
        1, std::min<size_t>(params_.poller_count, business_slots_.size()));
    uint64_t last_timeout_scan = 0;
    // Idle-spin anchor for this poller (yuanhao-1113): the 20us sleep below is
    // the fallback only when neither a completion landed nor a post is recent.
    uint64_t last_progress_ns = 0;
    while (polling_.load(std::memory_order_acquire)) {
        bool progressed = false;
        // Pass one: probe completion queues first, so a business flood cannot
        // delay probe completions (design §11.1).
        for (size_t index = poller_index; index < probe_slots_.size();
             index += poller_count) {
            progressed = pollProbeSlot(index) || progressed;
        }
        // Pass two: business queues by stride, exactly as before.
        for (size_t index = poller_index; index < business_slots_.size();
             index += poller_count) {
            progressed = pollBusinessSlot(index) || progressed;
        }
        const uint64_t now = steadyNowNs();
        if (timeout_scans_enabled_.load(std::memory_order_acquire) &&
            now - last_timeout_scan >= 1'000'000ULL) {
            scanTimeouts();
            last_timeout_scan = now;
        }
        logBusinessSummary(now);
        if (progressed) {
            last_progress_ns = now;
            continue;
        }
        // Keep polling while a post or completion is recent so a sparse
        // foreground transfer does not wait out a sleep after its WR lands.
        const uint64_t last_activity_ns = std::max(
            last_progress_ns, last_post_ns_.load(std::memory_order_relaxed));
        if (withinIdleSpin(now, last_activity_ns, params_.poll_idle_spin_us)) {
            cpuRelax();
            continue;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(20));
    }
}

void UbWorkers::logBusinessSummary(uint64_t now_ns) {
    // One line every 5s, emitted by whichever poller wins the CAS. Only while
    // there is something to say, so an idle transport stays silent. This is the
    // production outlet for the W3 counters: without it the queue/defer/pool
    // numbers would only exist for tests.
    uint64_t last = last_business_log_ns_.load(std::memory_order_acquire);
    if (now_ns - last < 5'000'000'000ULL) return;
    if (!last_business_log_ns_.compare_exchange_strong(
            last, now_ns, std::memory_order_acq_rel)) {
        return;
    }
    const auto stats = businessStats();
    // Drain-chain summary (design §10.2). The controller owns the stage
    // counters; this is the periodic outlet that makes them observable without
    // a per-WR log line.
    DrainStats drain{};
    for (const auto& context : contexts_) {
        if (!context) continue;
        auto controller = context->drainController();
        if (!controller) continue;
        const DrainStats one = controller->stats();
        drain.drain_begin += one.drain_begin;
        drain.drain_begin_failed += one.drain_begin_failed;
        drain.drain_delete_ready += one.drain_delete_ready;
        drain.drain_timeout += one.drain_timeout;
        drain.drain_quarantined += one.drain_quarantined;
        drain.modify_error_ok += one.modify_error_ok;
        drain.modify_error_fail += one.modify_error_fail;
        drain.marker_flush_done += one.marker_flush_done;
        drain.marker_suspend_done += one.marker_suspend_done;
        drain.marker_dup += one.marker_dup;
        drain.marker_stale += one.marker_stale;
        drain.marker_before_error += one.marker_before_error;
        drain.entity_event_on_drain += one.entity_event_on_drain;
        drain.software_flush_calls += one.software_flush_calls;
        drain.software_flush_wrs += one.software_flush_wrs;
        drain.software_flush_unhandled += one.software_flush_unhandled;
        drain.drained_after_settle += one.drained_after_settle;
        drain.drain_pending += one.drain_pending;
        drain.drain_oldest_age_ms =
            std::max(drain.drain_oldest_age_ms, one.drain_oldest_age_ms);
        drain.async_drain_supported =
            drain.async_drain_supported && one.async_drain_supported;
        drain.async_drain_disabled += one.async_drain_disabled;
    }
    for (const auto& context : contexts_) {
        if (!context) continue;
        const auto pool = context->pooledSendPool();
        if (!pool) continue;
        const auto budget = pool->budgetSnapshot();
        if (!budget || budget->total_jetties == 0) continue;
        LOG(INFO) << "UB native budget: device_key=" << budget->device_key
                  << " configured=" << budget->configured_budget_jetties
                  << " reserve=" << budget->replacement_reserve_jetties
                  << " verified_limit=" << budget->verified_device_limit_jetties
                  << " total=" << budget->total_jetties
                  << " active=" << budget->active_jetties
                  << " creating=" << budget->creating_jetties
                  << " retiring=" << budget->retiring_jetties
                  << " quarantined=" << budget->quarantined_jetties
                  << " rejected=" << budget->rejected_reservations
                  << " leaked=" << budget->leaked_reservations;
    }
    if (stats.queue_depth == 0 && stats.parked_depth == 0 &&
        stats.pool_lease_attempts == 0 && drain.drain_begin == 0) {
        return;
    }
    uint64_t last_rail = last_rail_log_ns_.load(std::memory_order_acquire);
    if (now_ns - last_rail >= 30'000'000'000ULL &&
        last_rail_log_ns_.compare_exchange_strong(last_rail, now_ns,
                                                  std::memory_order_acq_rel) &&
        rail_monitor_ != nullptr) {
        for (const auto& rail : rail_monitor_->allStats(now_ns)) {
            LOG(INFO) << "UB rail business: local_topology_id="
                      << rail.key.local_topology_id
                      << " remote_device_id=" << rail.key.remote_device_id
                      << " remote_segment_id=" << rail.key.remote_segment_id
                      << " paused=" << (rail.paused ? 1 : 0)
                      << " ok=" << rail.successful_completions
                      << " err=" << rail.completion_errors
                      << " timeout=" << rail.timeouts
                      << " recoveries=" << rail.recoveries
                      << " rebuilds=" << rail.endpoint_rebuilds
                      << " pauses=" << rail.pauses << " ewma_lat_us="
                      << (rail.ewma_latency_ns < 0
                              ? -1.0
                              : rail.ewma_latency_ns / 1000.0)
                      << " ewma_bw_mbps="
                      << (rail.ewma_bandwidth_bytes_per_second < 0
                              ? -1.0
                              : rail.ewma_bandwidth_bytes_per_second * 8.0 /
                                    1e6);
        }
    }
    const auto gate = completionStats();
    LOG(INFO) << "UB business: queued=" << stats.queue_depth
              << " parked=" << stats.parked_depth
              << " submit_rejected=" << stats.submit_backpressure
              << " defer[k=" << stats.defer_pending[0]
              << ",quota=" << stats.defer_pending[1]
              << ",pool=" << stats.defer_pending[2]
              << ",path=" << stats.defer_pending[3]
              << "] wakeup[k=" << stats.defer_epoch_wakeup[0]
              << ",quota=" << stats.defer_epoch_wakeup[1]
              << ",pool=" << stats.defer_epoch_wakeup[2]
              << ",path=" << stats.defer_epoch_wakeup[3]
              << "] repark=" << stats.defer_reflow_repark
              << " k_wait=" << stats.task_K_wait
              << " lease{try=" << stats.pool_lease_attempts
              << ",miss=" << stats.pool_lease_misses
              << ",stopped=" << stats.pool_stop_rejections
              << ",hard=" << stats.pool_hard_errors
              << "} claim{stopping=" << stats.claim_rejected_stopping
              << ",member=" << stats.claim_rejected_member
              << ",gen=" << stats.claim_rejected_generation
              << ",epoch=" << stats.claim_rejected_error_epoch
              << ",disabled=" << stats.claim_disabled
              << ",isolated=" << stats.claim_after_isolation << "}"
              << " post_isolated=" << stats.post_on_isolated_path
              << " param_invalid_edge="
              << stats.business_param_error_invalid_edge
              << " declared_candidates=" << stats.declared_candidates_total
              << "/" << stats.declared_candidates_denied
              << " retire=" << stats.pooled_retire
              << " deadline_timeout=" << stats.business_deadline_timeouts
              << " path_peak=" << stats.path_inflight_peak
              << " imbalance=" << stats.path_load_imbalance
              << " double_return=" << stats.pool_return_double
              << " unknown_tombstoned=" << stats.unknown_token_tombstoned
              << " drain{begin=" << drain.drain_begin
              << ",begin_failed=" << drain.drain_begin_failed
              << ",ready=" << drain.drain_delete_ready
              << ",pending=" << drain.drain_pending
              << ",oldest_ms=" << drain.drain_oldest_age_ms
              << ",timeout=" << drain.drain_timeout
              << ",quarantine=" << drain.drain_quarantined
              << ",modify_ok=" << drain.modify_error_ok
              << ",modify_fail=" << drain.modify_error_fail
              << ",marker=" << drain.marker_flush_done
              << ",marker_stale=" << drain.marker_stale
              << ",marker_dup=" << drain.marker_dup
              << ",marker_early=" << drain.marker_before_error
              << ",event_on_drain=" << drain.entity_event_on_drain
              << ",flush=" << drain.software_flush_calls
              << ",flush_wrs=" << drain.software_flush_wrs
              << ",flush_marker=" << drain.software_flush_unhandled
              << ",settled_late=" << drain.drained_after_settle
              << ",supported=" << (drain.async_drain_supported ? 1 : 0)
              << ",disabled=" << drain.async_drain_disabled << "}"
              << " gate{post_return=" << gate.gate_blocked_by_post_return
              << ",cqe=" << gate.gate_blocked_by_cqe
              << ",responsibility=" << gate.gate_blocked_by_responsibility
              << ",refs=" << gate.gate_blocked_by_handler_refs
              << ",fault=" << gate.gate_blocked_by_fault
              << ",not_accepted=" << gate.gate_blocked_by_not_accepted
              << ",acceptance=" << gate.gate_blocked_by_acceptance << "}";
    if (probe_topology_ != nullptr) {
        const auto probe = probe_topology_->stats();
        LOG(INFO) << "UB probe topology: ports=" << probe.ports
                  << " edges=" << probe.edges
                  << " adjacency_allowed=" << probe.adjacency_allowed
                  << " adjacency_undeclared_peer="
                  << probe.adjacency_undeclared_peer
                  << " adjacency_undeclared_local="
                  << probe.adjacency_undeclared_local
                  << " adjacency_undeclared_remote="
                  << probe.adjacency_undeclared_remote
                  << " adjacency_peer_eid_unmatched="
                  << probe.adjacency_peer_eid_unmatched
                  << " adjacency_port_missing=" << probe.adjacency_port_missing
                  << " adjacency_peer_gate_blocked="
                  << probe.adjacency_peer_gate_blocked
                  << " adjacency_unverified_peer="
                  << probe.adjacency_unverified_peer;
    }
}

bool UbWorkers::pollBusinessSlot(size_t slot_index) {
    auto& slot = business_slots_[slot_index];
    std::vector<Completion> completions;
    auto status = slot.jfc->poll(64, completions);
    if (!status.ok()) {
        if (slot.context) handleLocalDeviceFailure(slot.context);
        LOG_EVERY_N(WARNING, 1000)
            << "UB JFC poll failed: " << status.ToString();
        return false;
    }
    if (slot.context) {
        // Cleanup retries are native barriers too: keep them off the poller.
        enqueueDeviceRetire(slot.context);
        if (slot.context->recordPollSuccess(
                slot.jfc->index(),
                static_cast<uint64_t>(params_.endpoint_cooldown_ms) *
                    1'000'000ULL)) {
            LOG(INFO) << "Recovered UB device "
                      << slot.context->deviceInfo().topology_name
                      << " after endpoint retirement and an error-free JFC "
                         "cooldown";
            // F7: propagate the availability edge to the probe side. Without
            // it the paths DISABLED by onContextUnavailable() would stay
            // disabled forever, because probe recovery waits for a new graph
            // version that only a rebuild emits.
            if (probe_manager_ != nullptr) {
                const Status revived = probe_manager_->onContextAvailable(
                    slot.context->topologyId());
                if (!revived.ok()) {
                    LOG_EVERY_N(WARNING, 100)
                        << "UB probe context-available event was rejected: "
                        << revived.ToString();
                }
            }
        }
    }
    const int local_nic = slot.context ? slot.context->topologyId() : -1;
    const uint64_t jfc_index =
        slot.jfc ? static_cast<uint64_t>(slot.jfc->index()) : 0;
    for (const auto& completion : completions) {
        dispatchCompletion(completion, local_nic, jfc_index);
    }
    return !completions.empty();
}

bool UbWorkers::pollProbeSlot(size_t slot_index) {
    auto& slot = probe_slots_[slot_index];
    std::vector<Completion> completions;
    auto status = adapter_->poll(slot.jfc, 64, completions);
    if (!status.ok()) {
        // Probe JFC failures are treated exactly like business JFC failures
        // (design §11.1) and also disable the probe paths on that context.
        if (slot.context) handleLocalDeviceFailure(slot.context);
        LOG_EVERY_N(WARNING, 1000)
            << "UB probe JFC poll failed: " << status.ToString();
        return false;
    }
    if (slot.context) enqueueDeviceRetire(slot.context);
    // The probe JFC is provider-owned and not part of UbContext::jfcs(), so it
    // deliberately does not participate in the business JFC recovery epoch.
    const int local_nic = slot.context ? slot.context->topologyId() : -1;
    for (const auto& completion : completions) {
        dispatchCompletion(completion, local_nic);
    }
    return !completions.empty();
}

bool UbWorkers::dispatchCompletion(const Completion& completion, int local_nic,
                                   uint64_t jfc_index) {
    // Step 1: token 0 is reserved for a native marker / async event. W2 only
    // accounts for it: telling a marker from an AE needs the typed CR that
    // P0-B supplies, so the W4 classification hangs off this single call site
    // (IN-9: token 0 never reaches the business registry).
    if (completion.token == 0) {
        // W4 classification (design tent-ub-async-drain-design §4.3): a
        // token==0 record may be a provider entity marker owned by an
        // in-flight pooled drain. The controller claims it first; only an
        // unowned record falls through to the legacy accounting. The poller
        // performs one hash lookup and flag writes, never a provider call.
        const BusinessSlot* slot = nullptr;
        for (const auto& candidate : business_slots_) {
            if (candidate.jfc && candidate.jfc->index() == jfc_index &&
                candidate.context &&
                candidate.context->topologyId() == local_nic) {
                slot = &candidate;
                break;
            }
        }
        if (slot != nullptr && slot->drain != nullptr &&
            slot->drain->onEntityEvent(completion, jfc_index)) {
            return true;
        }
        // Classification is observation only here: a marker with no drain
        // owner still goes through the unknown-token path below (the controller
        // already counted an owned-but-unusable marker).
        EntityEventKind kind = EntityEventKind::kUnclassified;
        switch (adapter_ ? adapter_->classifyEntityMarker(completion)
                         : EntityMarkerKind::kNone) {
            case EntityMarkerKind::kFlushDone:
                kind = EntityEventKind::kMarkerFlushDone;
                break;
            case EntityMarkerKind::kSuspendDone:
                kind = EntityEventKind::kMarkerSuspendDone;
                break;
            case EntityMarkerKind::kNone:
                break;
        }
        EntityEvidence evidence;
        evidence.token = completion.token;
        evidence.jetty_id = completion.local_jetty_id;
        evidence.native_status = completion.native_status;
        evidence.completed_bytes = completion.completed_bytes;
        entity_events_.onEntityEvent(local_nic, EntityType::kJfc, jfc_index,
                                     kind, evidence);
        return false;
    }
    // Step 2: namespace first (probe design §11.2). A probe completion may
    // carry an error status that the business table would silently drop.
    if (probe_manager_ != nullptr && isProbeToken(completion.token)) {
        (void)probe_manager_->onProbeCompletion(completion);
        return true;
    }
    // Step 3: business registry. The handler reference brackets the settle
    // scope so the gate cannot dispose of the record while a poller still
    // touches reusable state (design §6.5).
    auto record = findRecord(completion.token);
    if (record) {
        // W3 D-15: books only. A completion whose byte count differs from the
        // submitted length is recorded and never changes the settlement
        // predicate or the pool return predicate.
        if (record->pending.slice &&
            completion.category == CompletionCategory::SUCCESS &&
            completion.completed_bytes != 0 &&
            completion.completed_bytes !=
                record->pending.slice->spec().length) {
            gate_ctx_.stats.completion_length_mismatch.fetch_add(
                1, std::memory_order_relaxed);
        }
        gate::HandlerRef handler(record->core, *this, gate_ctx_);
        gate::onRealCr(record->core, makeWrFacts(completion), *this, gate_ctx_);
        return true;
    }
    // Step 4: unknown / late / already disposed. Count it, report it, and keep
    // polling: one stray token never affects any other responsibility (IN-8).
    gate_ctx_.stats.unknown_token.fetch_add(1, std::memory_order_relaxed);
    // Attribute the token to a retired native entity when the tombstone table
    // still owns it. The entry proves the Jetty left service, so this is a
    // late completion of an unproven retirement, not a topology-wide fault.
    if (tombstones_ != nullptr && completion.local_jetty_id != 0) {
        const auto stone =
            tombstones_->lookup(EntityKind::kJetty, completion.local_jetty_id);
        if (stone.has_value()) {
            unknown_token_tombstoned_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    EntityEvidence evidence;
    evidence.token = completion.token;
    evidence.jetty_id = completion.local_jetty_id;
    evidence.native_status = completion.native_status;
    evidence.completed_bytes = completion.completed_bytes;
    entity_events_.onEntityEvent(local_nic, EntityType::kJfc, jfc_index,
                                 EntityEventKind::kUnknownToken, evidence);
    return false;
}

void UbWorkers::enqueueRetry(const PendingSlice& pending) {
    if (!pending.slice) return;
    // Deliberately NOT subject to queue_budget: a retry is already
    // bounded by the slice's own bounded retry budget, and rejecting it here
    // would turn "the queue is momentarily full" into a transfer failure. The
    // budget exists for submit() backpressure (review minor).
    if (!posting_.load(std::memory_order_acquire) ||
        !pending.slice->markQueued()) {
        pending.slice->requestCancellation();
        return;
    }
    bool stopped = false;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!posting_.load(std::memory_order_relaxed)) {
            stopped = true;
        } else {
            queues_[std::clamp(pending.priority, static_cast<int>(PRIO_HIGH),
                               static_cast<int>(PRIO_LOW))]
                .push_back(pending);
            ++queued_;
            queued_hint_.store(queued_, std::memory_order_release);
        }
    }
    if (stopped) {
        pending.slice->requestCancellation();
        return;
    }
    queue_cv_.notify_one();
}

UbWorkers::PendingSlice UbWorkers::withDeferralCount(
    const PendingSlice& pending) {
    PendingSlice parked = pending;
    // A slice that is parked again after a reflow means the wake-up source and
    // the admitted condition disagreed; count it (defer_reflow_repark).
    if (parked.deferrals > 0) {
        defer_reflow_repark_.fetch_add(1, std::memory_order_relaxed);
    }
    if (parked.deferrals != UINT32_MAX) ++parked.deferrals;
    return parked;
}

void UbWorkers::deferPending(const PendingSlice& pending, DeferReason reason,
                             uint64_t wait_epoch, uint64_t expire_ns) {
    if (!pending.slice) return;
    // F6c stage 2: a parked slice still owes its request deadline. Cap the wake
    // time (and give previously untimed defers a timer) so an expired request
    // is cancelled instead of being re-queued forever.
    const uint64_t request_deadline = requestDeadlineNs(pending);
    if (request_deadline != 0 &&
        (expire_ns == 0 || request_deadline < expire_ns)) {
        expire_ns = request_deadline;
    }
    // Diagnostics only (yuanhao-1113): a traced slice counts its deferrals so
    // the task trace can separate queue latency from device latency.
    if (pending.task && pending.task->traceEnabled()) {
        pending.slice->noteDeferred();
    }
    bool cancel = false;
    bool requeue = false;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!posting_.load(std::memory_order_relaxed) ||
            pending.slice->cancellationRequested()) {
            // Shutdown or cancellation while the attempt was being evaluated:
            // nothing may stay parked across a stop (design §8.3).
            cancel = true;
        } else if (!defer_.park(DeferRegistry<PendingSlice>::Item{
                       withDeferralCount(pending), reason, wait_epoch,
                       expire_ns})) {
            // The bucket advanced between "read epoch" and "park": re-queue
            // immediately instead of registering a wait that nobody will ever
            // wake (design §7.4, defer_bucket_snapshot_retry).
            defer_bucket_snapshot_retry_.fetch_add(1,
                                                   std::memory_order_relaxed);
            requeue = true;
        } else {
            defer_pending_[static_cast<size_t>(reason)].fetch_add(
                1, std::memory_order_relaxed);
        }
        if (requeue) {
            // Same state machine as the reflow above: the deferred slice may
            // still be kQueued (defer before beginAttempt), in which case it is
            // already admissible and must not be canceled.
            if (pending.slice->requeueAfterDefer()) {
                queues_[std::clamp(pending.priority,
                                   static_cast<int>(PRIO_HIGH),
                                   static_cast<int>(PRIO_LOW))]
                    .push_back(pending);
                ++queued_;
                queued_hint_.store(queued_, std::memory_order_release);
            } else {
                cancel = true;
            }
        }
    }
    if (cancel) pending.slice->requestCancellation();
    if (requeue) queue_cv_.notify_all();
}

Status UbWorkers::buildRoute(const PendingSlice& pending, Route& route) {
    return segment_manager_->withCachedSegment(
        pending.target_id, route.pin, [&](SegmentDesc* segment) -> Status {
            if (!segment || segment->type != SegmentType::Memory) {
                return Status::InvalidMetadataType(
                    "UB target is not a memory segment" LOC_MARK);
            }
            auto* buffer =
                segment->findBuffer(pending.slice->spec().remote_address,
                                    pending.slice->spec().length);
            if (!buffer) {
                return Status::NeedsRefreshCache(
                    "UB target range is not registered" LOC_MARK);
            }
            auto attr = buffer->transport_attrs.find(TransportType::UB);
            if (attr == buffer->transport_attrs.end()) {
                return Status::NeedsRefreshCache(
                    "UB target buffer has no transport metadata" LOC_MARK);
            }
            UbBufferMetadata metadata;
            auto status = decodeBufferMetadata(attr->second, metadata);
            if (!status.ok()) return status;
            route.remote_buffer = buffer;
            route.metadata = std::move(metadata);
            // Remote metadata is decoded once in this function and reused by
            // device ordering and by importRemote (yuanhao-1113).
            route.remote_devices =
                orderedRemoteDevices(*segment, *buffer, route.metadata);
            if (probe_topology_ != nullptr) {
                route.adjacency =
                    probe_topology_->viewFor(segment->name, *segment);
                route.probe_peer_id =
                    probe_topology_->probePeerId(segment->name)
                        .value_or(LOCAL_SEGMENT_ID);
            }
            if (route.remote_devices.empty() &&
                !(route.adjacency.enforced &&
                  route.adjacency.peer_eid_unmatched)) {
                return Status::DeviceNotFound(
                    "UB target has no active advertised device" LOC_MARK);
            }
            return Status::OK();
        });
}

std::string UbWorkers::localLocationOf(const PendingSlice& pending) const {
    // The registration already probed the buffer; reuse that unless it
    // spanned several locations, so the common path makes no NUMA syscall.
    auto location = buffers_->localLocation(
        reinterpret_cast<uint64_t>(pending.slice->spec().local_address));
    if (!location.empty()) return location;
    auto locations = Platform::getLoader().getLocation(
        pending.slice->spec().local_address, 1, true);
    if (!locations.empty()) return locations.front().location;
    return kWildcardLocation;
}

std::vector<UbWorkers::LocalDeviceCandidate> UbWorkers::orderedLocalDevices(
    const PendingSlice& pending, const std::string& location) const {
    std::vector<LocalDeviceCandidate> ordered;
    std::unordered_set<Topology::NicID> seen;
    std::unordered_map<Topology::NicID, size_t> device_rank;

    auto append = [&](Topology::NicID id) {
        auto found = context_by_topology_id_.find(id);
        if (found == context_by_topology_id_.end() || !found->second ||
            !found->second->active()) {
            return;
        }
        const bool allowed = pending.device_mask == ~0ULL ||
                             (id >= 0 && id < 64 &&
                              (pending.device_mask & (uint64_t{1} << id)) != 0);
        if (allowed && seen.insert(id).second) {
            const auto rank = device_rank.find(id);
            ordered.push_back({id, rank == device_rank.end()
                                       ? Topology::DevicePriorityRanks
                                       : rank->second});
        }
    };
    if (local_topology_) {
        if (const auto* memory = local_topology_->getMemEntry(location)) {
            for (size_t rank = 0; rank < Topology::DevicePriorityRanks;
                 ++rank) {
                for (auto id : memory->device_list[rank]) {
                    auto [it, inserted] = device_rank.emplace(id, rank);
                    if (!inserted) it->second = std::min(it->second, rank);
                    append(id);
                }
            }
        }
    }
    for (const auto& context : contexts_) {
        if (context) {
            device_rank.try_emplace(context->topologyId(),
                                    Topology::DevicePriorityRanks);
            append(context->topologyId());
        }
    }
    // Snapshot inflight bytes before sorting. The live counters change on
    // other posting/polling threads; reading them inside the comparator makes
    // the sorting non-deterministic, which violates the strict weak ordering
    // requirements of std::stable_sort and can drive it out of bounds.
    std::unordered_map<Topology::NicID, uint64_t> inflight_snapshot;
    for (const auto& candidate : ordered) {
        auto found = context_by_topology_id_.find(candidate.id);
        if (found != context_by_topology_id_.end() && found->second) {
            inflight_snapshot[candidate.id] = found->second->inflightBytes();
        }
    }
    std::stable_sort(
        ordered.begin(), ordered.end(), [&](const auto& lhs, const auto& rhs) {
            if (lhs.topology_rank != rhs.topology_rank)
                return lhs.topology_rank < rhs.topology_rank;
            auto lhs_inflight = inflight_snapshot.find(lhs.id);
            auto rhs_inflight = inflight_snapshot.find(rhs.id);
            const uint64_t lhs_inflight_bytes =
                lhs_inflight == inflight_snapshot.end() ? 0
                                                        : lhs_inflight->second;
            const uint64_t rhs_inflight_bytes =
                rhs_inflight == inflight_snapshot.end() ? 0
                                                        : rhs_inflight->second;
            return lhs_inflight_bytes < rhs_inflight_bytes;
        });
    return ordered;
}

std::vector<Topology::NicID> UbWorkers::orderedRemoteDevices(
    const SegmentDesc& segment, const BufferDesc& buffer,
    const UbBufferMetadata& metadata) {
    std::vector<Topology::NicID> ordered;
    std::unordered_set<Topology::NicID> advertised;
    for (const auto& item : metadata.segments) {
        advertised.insert(item.topology_id);
    }
    std::unordered_set<Topology::NicID> seen;
    const auto& topology = segment.getMemory().topology;
    auto append = [&](Topology::NicID id) {
        const auto* nic = topology.getNicEntry(id);
        if (nic && nic->type == Topology::NIC_UB && advertised.count(id) &&
            seen.insert(id).second) {
            ordered.push_back(id);
        }
    };
    if (const auto* memory = topology.getMemEntry(buffer.location)) {
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            for (auto id : memory->device_list[rank]) append(id);
        }
    }
    for (const auto& item : metadata.segments) append(item.topology_id);
    return ordered;
}

Status UbWorkers::chooseAndResolveEndpoint(
    const PendingSlice& pending, Route& route,
    std::shared_ptr<UbEndpoint>& endpoint, UbPostPath& path,
    QuotaReservation& reservation, DeferPlan* plan) {
    struct Candidate {
        std::shared_ptr<UbEndpoint> endpoint;
        UbPostPath path;
        UbPathSelectionScore score;
    };

    // One local x remote combination, scored from rail-level state before any
    // endpoint is resolved. Endpoint-level load is deliberately absent: it is
    // only available after resolution and is applied to the finalists below.
    struct RailCandidate {
        Topology::NicID local_id;
        int remote_id;
        size_t topology_rank;
        size_t retry_order;
        UbPostPath probe_path;
        RailStats rail_stats;
        QuotaAvailability capacity;
    };

    // The local buffer's location is resolved once per slice (yuanhao-1113);
    // a single-location buffer therefore costs no NUMA probe here.
    auto local_devices = orderedLocalDevices(pending, route.local_location);
    auto remote_devices = route.remote_devices;
    // INV-1/INV-2: in declared mode the candidate set is exactly the resolved
    // bindings x declared adjacency, not the full local x remote discovery
    // product. Unpublished/withdrawn/gated remote ports stay in the view so
    // the gate can classify them as transient.
    if (route.adjacency.enforced) {
        if (route.adjacency.peer_eid_unmatched || remote_devices.empty()) {
            if (plan != nullptr) {
                plan->reason = DeferReason::kPathUnavailable;
                plan->expire_ns = deadlineAfter(steadyNowNs(), kTimedDeferNs);
            }
            return Status::TooManyRequests(
                "Declared peer has no reachable probe port yet" LOC_MARK);
        }
        std::unordered_set<Topology::NicID> declared_locals(
            route.adjacency.local_nics.begin(),
            route.adjacency.local_nics.end());
        local_devices.erase(
            std::remove_if(local_devices.begin(), local_devices.end(),
                           [&](const LocalDeviceCandidate& candidate) {
                               return declared_locals.count(candidate.id) == 0;
                           }),
            local_devices.end());
        remote_devices = route.adjacency.remote_nics;
        if (remote_devices.empty()) {
            if (plan != nullptr) {
                plan->reason = DeferReason::kPathUnavailable;
                plan->expire_ns = deadlineAfter(steadyNowNs(), kTimedDeferNs);
            }
            return Status::TooManyRequests(
                "Declared peer has no mapped remote port" LOC_MARK);
        }
    }
    if (remote_devices.empty()) {
        return Status::DeviceNotFound("No usable UB posting path" LOC_MARK);
    }
    const auto snapshot = pending.slice->snapshot();
    if (local_devices.empty()) {
        const uint64_t now_ns = steadyNowNs();
        const uint64_t cooldown_ns =
            static_cast<uint64_t>(params_.endpoint_cooldown_ms) * 1'000'000ULL;
        const uint64_t recovery_grace_ns =
            static_cast<uint64_t>(params_.slice_timeout_ms) * 1'000'000ULL;
        uint64_t recovery_deadline_ns = 0;
        for (const auto& context : contexts_) {
            if (!context || context->state() != UbContext::State::kFailed) {
                continue;
            }
            const uint64_t failed_ns = context->failureStartedNs();
            if (failed_ns == 0) continue;
            recovery_deadline_ns =
                std::max(recovery_deadline_ns,
                         deadlineAfter(deadlineAfter(failed_ns, cooldown_ns),
                                       recovery_grace_ns));
        }
        if (pending.task && pending.task->request().deadline_ns != 0 &&
            recovery_deadline_ns != 0) {
            recovery_deadline_ns = std::min(
                recovery_deadline_ns, pending.task->request().deadline_ns);
        }
        if (recovery_deadline_ns != 0 && now_ns < recovery_deadline_ns) {
            if (plan != nullptr) {
                // Only time resolves a recovery cooldown: no release site can
                // bump a bucket for it, so this defer carries a deadline. The
                // posting loop re-evaluates it on its bounded tick instead of
                // sleeping on the W2 50us backoff.
                plan->reason = DeferReason::kQuota;
                plan->expire_ns = deadlineAfter(now_ns, kTimedDeferNs);
            }
            return Status::TooManyRequests(
                "All local UB devices are in recovery cooldown" LOC_MARK);
        }
        return Status::DeviceNotFound(
            "All local UB devices remained unavailable" LOC_MARK);
    }
    struct DeclaredPair {
        LocalDeviceCandidate local;
        Topology::NicID remote_id;
    };
    std::vector<DeclaredPair> declared_pairs;
    if (route.adjacency.enforced) {
        for (const auto& local : local_devices) {
            const auto local_it =
                std::find(route.adjacency.local_nics.begin(),
                          route.adjacency.local_nics.end(), local.id);
            if (local_it == route.adjacency.local_nics.end()) continue;
            const size_t local_slot = static_cast<size_t>(
                std::distance(route.adjacency.local_nics.begin(), local_it));
            for (size_t remote_slot = 0;
                 remote_slot < route.adjacency.remote_nics.size();
                 ++remote_slot) {
                const size_t index =
                    local_slot * route.adjacency.remote_nics.size() +
                    remote_slot;
                if (index >= route.adjacency.allowed.size() ||
                    route.adjacency.allowed[index] == 0) {
                    continue;
                }
                declared_pairs.push_back(DeclaredPair{
                    local, route.adjacency.remote_nics[remote_slot]});
            }
        }
        if (declared_pairs.empty()) {
            return Status::InvalidArgument(
                "declared adjacency has no usable local/remote pair" LOC_MARK);
        }
    }
    const size_t combinations =
        route.adjacency.enforced ? declared_pairs.size()
                                 : local_devices.size() * remote_devices.size();
    const size_t start =
        combinations == 0 ? 0 : snapshot.retry_count % combinations;
    Status first_error = Status::DeviceNotFound(
        "No ready UB endpoint for any posting path" LOC_MARK);

    auto railScore = [](const RailCandidate& candidate) {
        return UbPathSelectionScore{
            candidate.capacity.can_acquire,
            candidate.topology_rank,
            candidate.capacity.normalized_inflight,
            candidate.capacity.normalized_outstanding_wrs,
            0,
            0,
            candidate.rail_stats.ewma_bandwidth_bytes_per_second >= 0.0,
            candidate.rail_stats.ewma_bandwidth_bytes_per_second,
            candidate.retry_order,
            candidate.local_id,
            candidate.remote_id};
    };

    // Phase 1: rank every combination from rail-level state only. Resolving an
    // uncached endpoint costs a bootstrap RPC to the peer, so this pass must
    // not resolve anything.
    std::vector<RailCandidate> rails;
    rails.reserve(combinations);
    bool health_screened_out = false;
    // Declared adjacency is a static configuration fence and runs before any
    // health/load judgement (design §5.1 step 2). The first denial is kept for
    // a precise error when no candidate survives.
    bool adjacency_screened_out = false;
    size_t adjacency_denied = 0;
    size_t adjacency_hard_denied = 0;
    size_t adjacency_transient_denied = 0;
    size_t invalid_edge_denied = 0;
    AdjacencyDecision first_adjacency_denial = AdjacencyDecision::kAllowed;
    Topology::NicID first_denied_local = -1;
    Topology::NicID first_denied_remote = -1;
    if (route.adjacency.enforced) {
        declared_candidates_total_.fetch_add(combinations,
                                             std::memory_order_relaxed);
    }
    for (size_t flat = 0; flat < combinations; ++flat) {
        LocalDeviceCandidate local;
        Topology::NicID remote_id = -1;
        if (route.adjacency.enforced) {
            local = declared_pairs[flat].local;
            remote_id = declared_pairs[flat].remote_id;
            const ProbePathKey candidate_key{
                local.id, static_cast<SegmentID>(route.probe_peer_id),
                remote_id};
            if (param_error_edges_.invalid(candidate_key)) {
                ++invalid_edge_denied;
                continue;
            }
        } else {
            local = local_devices[flat / remote_devices.size()];
            remote_id = remote_devices[flat % remote_devices.size()];
        }
        if (route.adjacency.enforced) {
            bool local_declared = false;
            const auto decision =
                route.adjacency.check(local.id, remote_id, &local_declared);
            if (probe_topology_ != nullptr) {
                probe_topology_->recordAdjacency(
                    decision, decision == AdjacencyDecision::kAllowed &&
                                  !route.adjacency.publication_seen);
            }
            if (decision != AdjacencyDecision::kAllowed) {
                const bool hard =
                    decision == AdjacencyDecision::kUndeclaredPeer ||
                    decision == AdjacencyDecision::kUndeclaredLocal ||
                    decision == AdjacencyDecision::kUndeclaredRemote;
                if (!adjacency_screened_out || hard) {
                    first_adjacency_denial = decision;
                    first_denied_local = local.id;
                    first_denied_remote = remote_id;
                }
                ++adjacency_denied;
                if (hard) {
                    ++adjacency_hard_denied;
                } else {
                    ++adjacency_transient_denied;
                }
                adjacency_screened_out = true;
                continue;
            }
        }
        const UbPostPath probe_path{local.id, pending.target_id, remote_id,
                                    kRailProbeEndpointGeneration};
        // W3 health gate (design §7.1 rule 1 / D-10). Health is a pure
        // subtraction: only an explicitly unusable (ISOLATED/RECOVERING/
        // DISABLED) path is dropped. A PLM-less deployment passes everything,
        // and so does a path the probe graph does not model (W3-IN-6), so an
        // incomplete adjacency configuration can never starve business traffic.
        //
        // Rail pause is GONE here (design §7.2): the pause state was a local
        // substitute for isolation, and keeping both would double-penalize a
        // path that PLM already excludes.
        if (probe_manager_ != nullptr) {
            const bool usable =
                route.probe_peer_id != LOCAL_SEGMENT_ID
                    ? probe_manager_->businessPathUsable(ProbePathKey{
                          local.id, static_cast<SegmentID>(route.probe_peer_id),
                          remote_id})
                    : probe_manager_->businessPathUsable(probe_path);
            if (!usable) {
                health_screened_out = true;
                continue;
            }
        }
        // A non-inserting lookup: an unknown rail scores as "no health history
        // yet" without being added to the map, so probing every local x remote
        // combination cannot grow the rail map or seed a generation for a rail
        // that is never posted to.
        auto rail_stats = rail_monitor_->statsIfPresent(probe_path);
        rails.push_back(RailCandidate{
            local.id, remote_id, local.topology_rank,
            (flat + combinations - start) % combinations, probe_path,
            std::move(rail_stats),
            quota_->availability(probe_path, pending.slice->spec().length)});
    }
    const size_t counted_denied = adjacency_denied + invalid_edge_denied;
    const size_t health_denied =
        combinations > counted_denied + rails.size()
            ? combinations - counted_denied - rails.size()
            : 0;
    if (route.adjacency.enforced) {
        declared_candidates_denied_.fetch_add(counted_denied,
                                              std::memory_order_relaxed);
    }
    if (rails.empty() && invalid_edge_denied == combinations) {
        LOG_EVERY_N(WARNING, params_.observability.leaseSampleEvery())
            << "UB endpoint select denied: task="
            << reinterpret_cast<uint64_t>(pending.task.get())
            << " slice=" << pending.slice->snapshot().id
            << " policy=" << (route.adjacency.enforced ? "declared" : "legacy")
            << " candidates=" << combinations << " healthy=" << rails.size()
            << " health_denied=" << health_denied
            << " denied{adjacency=" << adjacency_denied
            << ",invalid_edge=" << invalid_edge_denied << ",quota=0}";
        return Status::InvalidArgument(
            "Every declared edge was invalidated after repeated "
            "parameter/memory errors" LOC_MARK);
    }
    if (rails.empty() && adjacency_screened_out &&
        adjacency_denied == combinations) {
        if (adjacency_hard_denied > 0) {
            LOG_EVERY_N(WARNING, params_.observability.leaseSampleEvery())
                << "UB endpoint select denied: task="
                << reinterpret_cast<uint64_t>(pending.task.get())
                << " slice=" << pending.slice->snapshot().id
                << " candidates=" << combinations
                << " denied{adjacency=" << adjacency_denied
                << ",invalid_edge=" << invalid_edge_denied << ",quota=0}"
                << " first=" << adjacencyDecisionName(first_adjacency_denial)
                << " local=" << first_denied_local
                << " remote=" << first_denied_remote;
            return Status::InvalidArgument(
                "UB adjacency rejects every posting candidate: peer=" +
                (route.pin ? route.pin->name : std::string("<unknown>")) +
                " local_nic=" + std::to_string(first_denied_local) +
                " remote_nic=" + std::to_string(first_denied_remote) +
                " reason=" + adjacencyDecisionName(first_adjacency_denial) +
                LOC_MARK);
        }
        if (adjacency_transient_denied > 0) {
            // A peer EID that is not in the current metadata, a port inside
            // its withdraw grace, or a peer gate that is temporarily closed
            // are all recoverable: defer instead of failing the slice.
            if (plan != nullptr) {
                plan->reason = DeferReason::kPathUnavailable;
                plan->expire_ns = deadlineAfter(steadyNowNs(), kTimedDeferNs);
            }
            return Status::TooManyRequests(
                "Every UB posting path is transiently unavailable (adjacency) "
                "reason=" +
                std::string(adjacencyDecisionName(first_adjacency_denial)) +
                LOC_MARK);
        }
    }
    std::stable_sort(rails.begin(), rails.end(),
                     [&](const RailCandidate& lhs, const RailCandidate& rhs) {
                         return betterUbPathScore(railScore(lhs),
                                                  railScore(rhs));
                     });

    // Phase 2: resolve lazily in the order produced by
    // ubResolutionAttemptOrder(), which takes the best rail of each local
    // device first and the remaining rails only after that. So the candidates
    // span devices instead of piling onto whichever device ranks best, and a
    // rail that cannot be resolved simply yields to the next attempt -
    // including the remaining rails on its own device from pass two. The
    // bootstrap fan-out is therefore bounded by the number of local devices -
    // one endpoint per device on a cold peer - rather than by the local x
    // remote combination count.
    const size_t resolve_limit =
        std::max(kMinResolvedCandidates, local_devices.size());
    std::vector<Topology::NicID> rail_local_ids;
    rail_local_ids.reserve(rails.size());
    for (const auto& rail : rails) rail_local_ids.push_back(rail.local_id);
    std::vector<Candidate> candidates;
    candidates.reserve(std::min(rails.size(), resolve_limit));
    bool pool_screened_out = false;
    for (size_t attempt : ubResolutionAttemptOrder(rail_local_ids)) {
        if (candidates.size() >= resolve_limit) break;
        const auto& rail = rails[attempt];
        auto context = context_by_topology_id_.at(rail.local_id);
        EndpointResolveRequest request{context, pending.target_id,
                                       route.pin.get(), rail.remote_id,
                                       route.metadata.generation};
        std::shared_ptr<UbEndpoint> candidate_endpoint;
        auto status = endpoint_resolver_(request, candidate_endpoint);
        if (!status.ok() || !candidate_endpoint ||
            !candidate_endpoint->ready()) {
            if (first_error.IsDeviceNotFound() && !status.ok())
                first_error = status;
            continue;
        }
        UbPostPath candidate_path{rail.local_id, pending.target_id,
                                  rail.remote_id,
                                  candidate_endpoint->generation()};
        // W3 pooled pre-filter (design §7.1 rule 2). It only avoids collecting
        // a candidate that tryLease would reject anyway; it never admits work
        // on its own, because a positive answer still races other attempts.
        if (!poolCandidateUsable(candidate_endpoint)) {
            pool_screened_out = true;
            continue;
        }
        // Re-read rail state under the real generation: the rail may only have
        // been created by the resolution above.
        const auto rail_stats = rail_monitor_->stats(candidate_path);
        const auto capacity =
            quota_->availability(candidate_path, pending.slice->spec().length);
        // W3 D-11: score from the per-path ledger, not from the endpoint
        // counters. The endpoint counter resets with the generation, which
        // used to make a fresh endpoint look like an idle path.
        const auto load = path_load_.load(candidate_path);
        candidates.push_back(Candidate{
            std::move(candidate_endpoint), candidate_path,
            UbPathSelectionScore{
                capacity.can_acquire, rail.topology_rank,
                capacity.normalized_inflight,
                capacity.normalized_outstanding_wrs, load.wrs, load.bytes,
                rail_stats.ewma_bandwidth_bytes_per_second >= 0.0,
                rail_stats.ewma_bandwidth_bytes_per_second, rail.retry_order,
                rail.local_id, rail.remote_id}});
    }
    if (candidates.empty()) {
        if (health_screened_out) {
            // Every candidate was excluded by health. That is capacity
            // pressure, not a failure: failing the slice here would turn a
            // probe-driven isolation into a business outage, which is exactly
            // what RK8 warns about. The write path defers instead.
            //
            // No PLM-side wake exists for this bucket yet (W1's single wake
            // callback belongs to the probe scheduler), so the defer carries a
            // 1 ms deadline: recovery is re-evaluated on the posting loop's
            // bounded tick instead of waiting forever.
            if (plan != nullptr) {
                plan->reason = DeferReason::kPathUnavailable;
                plan->expire_ns = deadlineAfter(steadyNowNs(), kTimedDeferNs);
            }
            return Status::TooManyRequests(
                "Every ready UB posting path is unhealthy" LOC_MARK);
        }
        if (pool_screened_out) {
            // Every resolved path had no leaseable pool capacity. This is
            // capacity pressure on the pool, so it parks in its own bucket and
            // is woken by returnOnce() rather than by a quota release.
            if (plan != nullptr) {
                plan->reason = DeferReason::kPoolIdle;
                plan->expire_ns = deadlineAfter(steadyNowNs(), kTimedDeferNs);
            }
            return Status::TooManyRequests(
                "Every ready UB posting path has no leaseable send "
                "member" LOC_MARK);
        }
        if (first_error.IsTooManyRequests()) {
            // Endpoint-cache/handshake pressure is different from live path
            // quota pressure below. Consume the slice's bounded retry budget
            // so a persistently quarantined native endpoint can eventually
            // fail this transport and let TENT select a fallback instead of
            // remaining in deferPending forever.
            return Status::InternalError(
                "UB endpoint resolution remained unavailable: " +
                first_error.ToString());
        }
        return first_error;
    }

    std::stable_sort(candidates.begin(), candidates.end(),
                     [](const Candidate& lhs, const Candidate& rhs) {
                         return betterUbPathScore(lhs.score, rhs.score);
                     });
    std::vector<UbPostPath> ordered_paths;
    ordered_paths.reserve(candidates.size());
    for (const auto& candidate : candidates) {
        ordered_paths.push_back(candidate.path);
    }

    auto acquired =
        quota_->tryAcquireFirst(ordered_paths, pending.slice->spec().length);
    if (!acquired) {
        if (plan != nullptr) plan->reason = DeferReason::kQuota;
        return Status::TooManyRequests(
            "Every ready UB posting path is at quota" LOC_MARK);
    }
    const auto selected = std::find_if(
        candidates.begin(), candidates.end(), [&](const Candidate& candidate) {
            return candidate.path == acquired->path;
        });
    if (selected == candidates.end()) {
        (void)quota_->release(*acquired);
        return Status::InternalError(
            "UB quota selected an unknown posting path" LOC_MARK);
    }
    endpoint = selected->endpoint;
    path = selected->path;
    reservation = *acquired;
    {
        const auto slice = pending.slice->snapshot();
        const uint64_t task_id = reinterpret_cast<uint64_t>(pending.task.get());
        const bool declared = route.adjacency.enforced;
        auto healthy_text = [&]() {
            std::string out;
            const size_t limit = std::min<size_t>(rails.size(), 8);
            for (size_t i = 0; i < limit; ++i) {
                if (i != 0) out += ",";
                out += std::to_string(rails[i].local_id);
                out += ":";
                out += std::to_string(rails[i].remote_id);
            }
            if (rails.size() > limit) out += ",...";
            return out;
        };
        if (params_.observability.verbose_events) {
            LOG(INFO) << "UB endpoint select: task=" << task_id
                      << " slice=" << slice.id << " attempt=" << slice.attempt
                      << " policy=" << (declared ? "declared" : "legacy")
                      << " candidates=" << combinations << " start=" << start
                      << " chosen=" << selected->score.local_topology_id << ":"
                      << selected->score.remote_device_id
                      << " rank=" << selected->score.topology_rank
                      << " probe_path=" << (route.probe_peer_id != 0 ? 1 : 0)
                      << " denied{adjacency=" << adjacency_denied
                      << ",invalid_edge=" << invalid_edge_denied << ",quota=0}"
                      << " healthy=" << rails.size()
                      << " resolved=" << candidates.size()
                      << " health_denied=" << health_denied
                      << " healthy_paths=[" << healthy_text() << "]";
        } else if (VLOG_IS_ON(1)) {
            VLOG(1) << "UB endpoint select: task=" << task_id
                    << " slice=" << slice.id << " attempt=" << slice.attempt
                    << " policy=" << (declared ? "declared" : "legacy")
                    << " candidates=" << combinations << " start=" << start
                    << " chosen=" << selected->score.local_topology_id << ":"
                    << selected->score.remote_device_id
                    << " rank=" << selected->score.topology_rank
                    << " probe_path=" << (route.probe_peer_id != 0 ? 1 : 0)
                    << " denied{adjacency=" << adjacency_denied
                    << ",invalid_edge=" << invalid_edge_denied << ",quota=0}"
                    << " healthy=" << rails.size()
                    << " resolved=" << candidates.size()
                    << " health_denied=" << health_denied << " healthy_paths=["
                    << healthy_text() << "]";
        } else {
            LOG_EVERY_N(INFO, params_.observability.leaseSampleEvery())
                << "UB endpoint select: task=" << task_id
                << " slice=" << slice.id << " attempt=" << slice.attempt
                << " policy=" << (declared ? "declared" : "legacy")
                << " candidates=" << combinations << " healthy=" << rails.size()
                << " resolved=" << candidates.size()
                << " health_denied=" << health_denied << " start=" << start
                << " chosen=" << selected->score.local_topology_id << ":"
                << selected->score.remote_device_id
                << " rank=" << selected->score.topology_rank
                << " probe_path=" << (route.probe_peer_id != 0 ? 1 : 0)
                << " denied{adjacency=" << adjacency_denied
                << ",invalid_edge=" << invalid_edge_denied << ",quota=0}";
        }
    }
    return Status::OK();
}

UbWorkers::DeferEpochs UbWorkers::readDeferEpochs() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return DeferEpochs{defer_.epoch(DeferReason::kTaskPermit),
                       defer_.epoch(DeferReason::kQuota),
                       defer_.epoch(DeferReason::kPoolIdle),
                       defer_.epoch(DeferReason::kPathUnavailable)};
}

std::shared_ptr<SendJettyPool> UbWorkers::pooledPoolFor(
    const std::shared_ptr<UbEndpoint>& endpoint) const {
    if (!endpoint) return nullptr;
    const auto& context = endpoint->context();
    return context ? context->pooledSendPool() : nullptr;
}

bool UbWorkers::poolCandidateUsable(
    const std::shared_ptr<UbEndpoint>& endpoint) const {
    const auto pool = pooledPoolFor(endpoint);
    // Not pooled: the legacy path has no pool resource to pre-filter.
    if (!pool) return true;
    if (pool->stopping()) return false;
    const PoolKey key = poolKeyFor(endpoint);
    if (pool->idleCount(key) > 0) return true;
    if (pool->creatingCount(key) > 0) return true;
    // Below the static serving target a member will be created on demand, so
    // the pool is not "full" yet. Without this clause a cold pool (serving 0)
    // would reject every candidate and business traffic could never start,
    // which is why the literal `servingCount() > 0` pre-screen of design §7.1
    // is relaxed here; tryLease remains the authority.
    return pool->servingCount(key) <
           params_.jetty_pool.business_desired_serving;
}

PoolKey UbWorkers::poolKeyFor(const std::shared_ptr<UbEndpoint>& endpoint) {
    PoolKey key;
    if (!endpoint) return key;
    if (const auto& context = endpoint->context()) {
        key.context_incarnation = context->incarnation();
    }
    const auto& options = endpoint->jettyOptions();
    key.trans_mode = options.transport_mode == JettyTransportMode::kRm
                         ? kUrmaTransportModeRm
                         : kUrmaTransportModeRc;
    key.priority = options.priority;
    // This baseline has no CTP bind mask (it derives the transport mode from
    // JettyOptions instead), so the field stays zero and the PoolKey keeps its
    // shape for a later LocalSendProfile.
    key.bind_tp_type_mask = 0;
    return key;
}

UbWorkers::ClaimDecision UbWorkers::claimPost(
    const std::shared_ptr<SendJettyPool>& pool, const PoolKey& pool_key,
    const AttemptKey& attempt, const LeaseWithCredit& leased,
    const UbPostPath& path, const ProbePathKey& probe_key,
    uint64_t fault_epoch_snapshot) {
    // The PLM fault epoch compared here is part of the claim *decision*, not an
    // observation: a path isolated after the pre-lease snapshot must reject
    // this attempt (the caller rolls back and re-plans).
    // `post_on_isolated_path_` only observes the inherent window between this
    // claim and the native post call; it is not the isolation check itself.
    //
    // W3g R-1. Two independent inputs decide whether the WR may be posted:
    //   1. the pool's synchronized verdict (stopping / member / generation /
    //      error epoch), authoritative for the leased resource;
    //   2. the PLM fault-epoch scalar compared against the pre-lease sample,
    //      which stays a separate read because the pool must not call the PLM
    //      while holding its lock (W3g D-4).
    // Both are lock-free from this thread's perspective and allocation-free;
    // this runs between the last rollback point and the native post.
    ClaimDecision decision;
    if (probe_manager_ != nullptr) {
        const uint64_t current =
            probe_key.valid() ? probe_manager_->businessFaultEpoch(probe_key)
                              : probe_manager_->businessFaultEpoch(path);
        if (current != fault_epoch_snapshot) {
            decision.path_epoch_changed = true;
        }
    }
    if (!params_.claim.enabled) {
        // D-9 rollback lever: the pre-W3g recheck only. Counted so a config
        // accidentally left off is visible in the periodic summary.
        claim_disabled_.fetch_add(1, std::memory_order_relaxed);
        if (pool && pool->stopping()) {
            decision.closed = ClaimClosed::kPoolStopping;
        }
        return decision;
    }
    if (!pool) return decision;
    ClaimRequest request;
    request.attempt = attempt;
    request.lane_generation = leased.lease.lane().lane_generation;
    request.error_epoch_snapshot = leased.error_epoch;
    request.member = leased.lease.member().get();
    decision.closed = pool->tryClaimPost(pool_key, attempt, request).closed;
    return decision;
}

WorkRequest UbWorkers::makeWorkRequest(const PendingSlice& pending,
                                       uint64_t token,
                                       const LocalSegmentRef& local,
                                       const ImportedSegmentRef& remote) {
    WorkRequest work;
    work.operation =
        pending.opcode == Request::READ ? Operation::READ : Operation::WRITE;
    work.local_address =
        reinterpret_cast<uint64_t>(pending.slice->spec().local_address);
    work.remote_address = pending.slice->spec().remote_address;
    work.length = pending.slice->spec().length;
    work.token = token;
    work.local_segment = local.segment;
    work.remote_segment = remote.segment;
    return work;
}

ProbePathKey UbWorkers::probeKeyFor(const Route& route,
                                    const UbPostPath& path) const {
    ProbePathKey key;
    if (route.probe_peer_id == LOCAL_SEGMENT_ID) return key;
    key.local_nic = path.local_topology_id;
    key.segment_id = static_cast<SegmentID>(route.probe_peer_id);
    key.remote_nic = path.remote_device_id;
    return key;
}

std::shared_ptr<UbWorkers::BusinessRecord> UbWorkers::makeRecord(
    const PendingSlice& pending, const std::shared_ptr<UbEndpoint>& endpoint,
    const UbPostPath& path, const ProbePathKey& probe_key,
    const QuotaReservation& reservation, const LocalSegmentRef& local,
    const ImportedSegmentRef& remote, const UbAttemptToken& attempt) {
    const uint64_t completion_token = nextCompletionToken();
    auto record = std::make_shared<BusinessRecord>();
    record->core.token = completion_token;
    record->core.kind = RecordKind::kBusiness;
    record->core.attempt_id = attempt.slice_id;
    record->core.attempt_serial = attempt.attempt;
    record->core.path = makeBusinessRecordPath(path);
    record->core.jetty_generation = path.endpoint_generation;
    record->core.owner = record.get();
    record->core.write_opcode = pending.opcode == Request::WRITE;
    record->pending = pending;
    record->attempt = attempt;
    record->path = path;
    record->probe_key = probe_key;
    record->endpoint = endpoint;
    record->local_segment = local.segment;
    record->remote_segment = remote.segment;
    record->quota = reservation;
    record->context_incarnation = endpoint && endpoint->context()
                                      ? endpoint->context()->incarnation()
                                      : 0;
    record->posted_ns = steadyNowNs();
    const uint64_t slice_deadline = deadlineAfter(
        record->posted_ns,
        static_cast<uint64_t>(params_.slice_timeout_ms) * 1'000'000ULL);
    // F6c stage 4: an in-flight attempt must honour the earlier of the two
    // deadlines. When the request deadline wins, the timeout fence (F6a) is
    // what enforces it, so no extra timer is needed.
    const uint64_t request_deadline = requestDeadlineNs(pending);
    if (request_deadline != 0 && request_deadline < slice_deadline) {
        record->deadline_ns = request_deadline;
        record->deadline_from_request = true;
    } else {
        record->deadline_ns = slice_deadline;
    }
    return record;
}

void UbWorkers::processPending(const PendingSlice& pending,
                               size_t worker_index) {
    if (!pending.slice) return;
    if (pending.slice->cancellationRequested()) {
        pending.slice->requestCancellation();
        return;
    }
    // F6c stage 1: the request was already past its deadline while it sat in a
    // ready queue. Terminate it here, before any route/lease/quota is touched,
    // and without charging backpressure.
    if (requestExpired(pending, steadyNowNs())) {
        expirePending(pending, expired_in_queue_);
        return;
    }
    // Sample every availability epoch before evaluating anything: a bump that
    // happens after this read is guaranteed to be visible to park(), which
    // then requeues immediately instead of dropping the wake-up (design §7.4).
    const DeferEpochs epochs = readDeferEpochs();

    // Diagnostics only (yuanhao-1113): stamp the dequeue so the task trace can
    // report queue_us separately from device_us.
    const bool traced = pending.task && pending.task->traceEnabled();
    if (traced) pending.slice->noteDequeued(steadyNowNs());
    Route route;
    auto status = buildRoute(pending, route);
    if (!status.ok()) {
        const auto resolution = pending.slice->resolveBeforePost(
            FAILED, 0, retryableStatus(status));
        if (resolution == UbAttemptResolution::kRetryScheduled) {
            enqueueRetry(pending);
        }
        return;
    }
    route.local_location = localLocationOf(pending);
    if (traced && pending.slice->spec().request_offset == 0) {
        pending.task->noteTraceLocations(
            route.local_location == kWildcardLocation ? std::string()
                                                      : route.local_location,
            route.remote_buffer->location);
    }
    const uint64_t route_ready_ns = traced ? steadyNowNs() : 0;

    std::shared_ptr<UbEndpoint> endpoint;
    UbPostPath path;
    QuotaReservation reservation;
    DeferPlan plan;
    status = chooseAndResolveEndpoint(pending, route, endpoint, path,
                                      reservation, &plan);
    if (!status.ok()) {
        if (status.IsTooManyRequests()) {
            deferPending(pending, plan.reason,
                         plan.reason == DeferReason::kPoolIdle
                             ? epochs.pool_idle
                             : epochs.quota,
                         plan.expire_ns);
            return;
        }
        const auto resolution = pending.slice->resolveBeforePost(
            FAILED, 0, retryableStatus(status));
        if (resolution == UbAttemptResolution::kRetryScheduled) {
            enqueueRetry(pending);
        }
        return;
    }
    const uint64_t path_selected_ns = traced ? steadyNowNs() : 0;

    LocalSegmentRef local;
    status = buffers_->findLocal(
        reinterpret_cast<uint64_t>(pending.slice->spec().local_address),
        pending.slice->spec().length, path.local_topology_id, local);
    if (!status.ok()) {
        (void)quota_->release(reservation);
        (void)pending.slice->resolveBeforePost(FAILED, 0, false);
        return;
    }
    ImportedSegmentRef remote;
    status = buffers_->importRemote(pending.target_id, path.local_topology_id,
                                    path.remote_device_id, *route.remote_buffer,
                                    route.metadata, pending.opcode,
                                    pending.slice->spec().remote_address,
                                    pending.slice->spec().length, remote);
    if (!status.ok()) {
        (void)quota_->release(reservation);
        const bool retryable = status.IsNeedsRefreshCache();
        if (retryable && pending.target_id != LOCAL_SEGMENT_ID) {
            (void)segment_manager_->invalidateRemote(pending.target_id);
        }
        const auto resolution =
            pending.slice->resolveBeforePost(FAILED, 0, retryable);
        if (resolution == UbAttemptResolution::kRetryScheduled) {
            enqueueRetry(pending);
        }
        return;
    }
    const uint64_t segments_ready_ns = traced ? steadyNowNs() : 0;

    // F6c stage 3: route resolution and segment import can consume the whole
    // remaining budget. Do not burn a lease/quota/K slot on a request that is
    // already late.
    if (requestExpired(pending, steadyNowNs())) {
        (void)quota_->release(reservation);
        expirePending(pending, expired_pre_post_);
        return;
    }

    if (!endpoint->tryAcquireOutstanding(pending.slice->spec().length)) {
        (void)quota_->release(reservation);
        // Outstanding pressure is released together with the quota in onOwners,
        // so it parks in the quota bucket rather than inventing a wake-up.
        deferPending(pending, DeferReason::kQuota, epochs.quota);
        return;
    }

    auto attempt = pending.slice->beginAttempt(path);
    if (!attempt) {
        endpoint->releaseOutstanding(pending.slice->spec().length);
        (void)quota_->release(reservation);
        return;
    }

    // ---- D-2 mode gate ----------------------------------------------------
    // `pooledSendPool() != nullptr` selects the W3 pool-aware admission below;
    // otherwise the W2 path runs byte for byte, touching no lease, credit or K
    // slot (RK10).
    // Diagnostics only (yuanhao-1113): the prepare phases are stamped right
    // before the commit, so prep_commit_us is the pool-claim plus post time.
    if (traced) {
        pending.slice->notePrepareCheckpoints(route_ready_ns, path_selected_ns,
                                              segments_ready_ns);
    }
    const ProbePathKey probe_key = probeKeyFor(route, path);
    if (pooledPoolFor(endpoint)) {
        processPendingPooled(pending, worker_index, route, endpoint, path,
                             probe_key, reservation, local, remote, *attempt);
        return;
    }
    postLegacy(pending, worker_index, probe_key, endpoint, path, reservation,
               local, remote, *attempt);
}

void UbWorkers::postLegacy(const PendingSlice& pending, size_t worker_index,
                           const ProbePathKey& probe_key,
                           const std::shared_ptr<UbEndpoint>& endpoint,
                           const UbPostPath& path,
                           const QuotaReservation& reservation,
                           const LocalSegmentRef& local,
                           const ImportedSegmentRef& remote,
                           const UbAttemptToken& attempt) {
    auto record = makeRecord(pending, endpoint, path, probe_key, reservation,
                             local, remote, attempt);
    const size_t length = pending.slice->spec().length;

    if (!pending.slice->tryCommitPost(attempt, record->posted_ns)) {
        endpoint->releaseOutstanding(length);
        (void)quota_->release(reservation);
        return;
    }
    // §5.1 status bit, not a gate: it records that this side took the attempt's
    // single post right, so a future local double post stays detectable.
    record->core.post_claimed.store(true, std::memory_order_release);

    WorkRequest work =
        makeWorkRequest(pending, record->core.token, local, remote);
    // RM Legacy: one slice attempt is one native WR that must reference the
    // imported target positional with this Jetty; the target is never
    // substituted by a bound remote Jetty.
    const bool rm_endpoint = endpoint->rmMode();
    if (rm_endpoint) {
        work.target = endpoint->rmTarget(
            worker_index % std::max<size_t>(1, endpoint->jettyCount()));
    }
    {
        // The post-stack reference spans the blocking native call, and it is
        // taken *before* the record becomes visible to the timeout scan: a
        // published record with zero references would let a scan fence a post
        // that has not even started (D-7). The guard releases the reference on
        // every exit path; when it is the last one the gate advances with both
        // facts available.
        gate::HandlerRef post_handler(record->core, *this, gate_ctx_);
        {
            std::lock_guard<std::mutex> lock(records_mutex_);
            records_.emplace(record->core.token, record);
            if (record->pool && record->pooled_lane.lane_generation != 0) {
                // D5: a zero incarnation would create a key shared by every
                // Context. It is unreachable for a pooled record (the endpoint
                // always owns a Context), so count it rather than alias it.
                if (record->context_incarnation == 0) {
                    pooled_lane_ref_unmapped_.fetch_add(
                        1, std::memory_order_relaxed);
                } else {
                    ++pooled_lane_refs_[LaneRefKey{
                        record->context_incarnation,
                        record->pooled_lane.lane_generation}];
                }
            }
        }
        gate_ctx_.stats.record_published.fetch_add(1,
                                                   std::memory_order_relaxed);
        path_load_.onPublish(path, length);
        auto jetty = endpoint->jetty(worker_index);
        size_t posted_count = 0;
        PostAcceptance acceptance = PostAcceptance::kNotAccepted;
        Status status;
        if (!jetty) {
            status = Status::InternalError("UB endpoint has no Jetty" LOC_MARK);
        } else if (rm_endpoint && !work.target) {
            status = Status::InternalError(
                "RM endpoint has no imported target for this Jetty" LOC_MARK);
        } else {
            // Anchor the pollers' idle-spin window (yuanhao-1113).
            last_post_ns_.store(record->posted_ns, std::memory_order_relaxed);
            status = adapter_->post(jetty, {work}, posted_count, acceptance);
            if (record->pending.task && record->pending.task->traceEnabled()) {
                record->pending.slice->notePostReturned(steadyNowNs());
            }
        }
        if (posted_count > 1) {
            // One WR can only produce one record, so more than one accepted WR
            // is a contradictory adapter result. It is counted, and it must
            // never be used as a reason to roll back (D-12).
            LOG_EVERY_N(WARNING, 100)
                << "UB adapter reported an impossible posted_count "
                << posted_count << " for one WR; token " << record->core.token
                << " drains until a fence proves responsibility";
        }
        if (acceptance == PostAcceptance::kUnknown) {
            // Nothing is released on a guess: the record drains until the
            // timeout fence proves the old DMA stopped (upstream §1.2.4).
            LOG_EVERY_N(WARNING, 100)
                << "UB post acceptance is unknown; retaining WR owners until a "
                   "CQE or fence settles token "
                << record->core.token;
        }
        if (acceptance == PostAcceptance::kAccepted) {
            rail_monitor_->registerPath(path);
            if (!status.ok()) {
                LOG_EVERY_N(WARNING, 1000)
                    << "UB post returned an error after accepting the WR: "
                    << status.ToString();
            }
        }
        gate::onPostReturn(
            record->core,
            PostReturn{acceptance,
                       acceptance == PostAcceptance::kAccepted ? length : 0,
                       retryableStatus(status)},
            gate_ctx_);
        // A rejected post needs no extra work here: the reject disposition
        // performs the legacy rollback exactly once when the guard above
        // releases the last reference.
    }
}

void UbWorkers::processPendingPooled(
    const PendingSlice& pending, size_t worker_index, const Route& route,
    const std::shared_ptr<UbEndpoint>& endpoint, const UbPostPath& path,
    const ProbePathKey& probe_key, const QuotaReservation& reservation,
    const LocalSegmentRef& local, const ImportedSegmentRef& remote,
    const UbAttemptToken& attempt) {
    (void)route;
    const size_t length = pending.slice->spec().length;
    auto pool = pooledPoolFor(endpoint);
    if (!pool) {
        // The gate above only calls this with a pool; defensive rollback.
        endpoint->releaseOutstanding(length);
        (void)quota_->release(reservation);
        return;
    }
    const DeferEpochs epochs = readDeferEpochs();

    // ④ K (D-6): the task-level concurrency ticket. K exhausted is capacity
    // pressure, never a failure: no retry is consumed, no path fault is
    // recorded and no slice is marked failed (W3-IN-3).
    if (!pending.task->tryAcquireAttemptSlot()) {
        task_K_wait_.fetch_add(1, std::memory_order_relaxed);
        endpoint->releaseOutstanding(length);
        (void)quota_->release(reservation);
        (void)pending.slice->deferAttempt(attempt);
        deferPending(pending, DeferReason::kTaskPermit, epochs.task_permit);
        return;
    }

    // ⑤ The claim anchor is sampled *before* the lease. Comparing it after the
    // lease covers the longest window in the attempt (selection -> lease), see
    // design §8.1 note 2.
    const uint64_t fault_epoch_snapshot =
        probe_manager_ == nullptr
            ? 0
            : (probe_key.valid() ? probe_manager_->businessFaultEpoch(probe_key)
                                 : probe_manager_->businessFaultEpoch(path));

    // ⑥ Lease (+ JFC credit). tryLease is non-blocking and already performs the
    // LIFO scan, the credit reservation and the budget gate, so this side never
    // keeps a second credit ledger.
    const PoolKey pool_key = poolKeyFor(endpoint);
    const AttemptKey attempt_key{reinterpret_cast<uint64_t>(pending.task.get()),
                                 attempt.slice_id, attempt.attempt};
    LeaseWithCredit leased;
    pool_lease_attempts_.fetch_add(1, std::memory_order_relaxed);
    const Status lease_status = pool->tryLease(pool_key, attempt_key, leased);
    if (!lease_status.ok()) {
        // Quota, endpoint counter and the K slot are already held: undo them in
        // reverse order before deciding what the attempt becomes.
        endpoint->releaseOutstanding(length);
        (void)quota_->release(reservation);
        pending.task->releaseAttemptSlot();
        if (lease_status.IsTooManyRequests()) {
            // D-5: the pooling side returns the same status for "stopping" and
            // "no capacity", so the verdict is re-read here. A stopped pool
            // must cancel instead of parking, otherwise the slice would wait
            // for a wake-up that can never come (RK2).
            if (pool->stopping()) {
                pool_stop_rejections_.fetch_add(1, std::memory_order_relaxed);
                pending.slice->requestCancellation();
                return;
            }
            pool_lease_misses_.fetch_add(1, std::memory_order_relaxed);
            (void)pending.slice->deferAttempt(attempt);
            // A return/retire normally bumps this bucket, but a pool whose
            // member creation keeps failing has no release site at all, so the
            // wait is bounded as well.
            deferPending(pending, DeferReason::kPoolIdle, epochs.pool_idle,
                         deadlineAfter(steadyNowNs(), kTimedDeferNs));
            return;
        }
        // A hard lease error is a real failure: it consumes retry budget like
        // every other native error.
        pool_hard_errors_.fetch_add(1, std::memory_order_relaxed);
        const auto resolution = pending.slice->resolveAttempt(
            attempt, FAILED, 0, retryableStatus(lease_status));
        if (resolution == UbAttemptResolution::kRetryScheduled) {
            enqueueRetry(pending);
        }
        return;
    }

    // ⑦ W3g claim (R-1). The pool synchronizes the verdict with retirement and
    // stop; the PLM fault epoch remains a separate scalar input.
    const ClaimDecision claim =
        claimPost(pool, pool_key, attempt_key, leased, path, probe_key,
                  fault_epoch_snapshot);
    if (!claim.claimed()) {
        switch (claim.closed) {
            case ClaimClosed::kPoolStopping:
                claim_rejected_stopping_.fetch_add(1,
                                                   std::memory_order_relaxed);
                break;
            case ClaimClosed::kMemberRetired:
                claim_rejected_member_.fetch_add(1, std::memory_order_relaxed);
                break;
            case ClaimClosed::kGenerationMoved:
                claim_rejected_generation_.fetch_add(1,
                                                     std::memory_order_relaxed);
                break;
            case ClaimClosed::kErrorEpochMoved:
                claim_rejected_error_epoch_.fetch_add(
                    1, std::memory_order_relaxed);
                break;
            case ClaimClosed::kNone:
                break;
        }
        if (claim.path_epoch_changed) {
            claim_after_isolation_.fetch_add(1, std::memory_order_relaxed);
        }
        // Reverse order again: credit (bound to the member's JFC) after the
        // member, then quota, outstanding counter and the K slot. No WR was
        // posted, so the member is clean and goes straight back to the LIFO
        // head (design §8.4).
        if (auto credits = pool->creditLedger()) {
            credits->release(leased.jfc_index, leased.jfc_credit_token);
        }
        (void)pool->returnOnce(std::move(leased.lease), /*gate_passed=*/true);
        pooled_return_.fetch_add(1, std::memory_order_relaxed);
        endpoint->releaseOutstanding(length);
        (void)quota_->release(reservation);
        pending.task->releaseAttemptSlot();
        (void)pending.slice->deferAttempt(attempt);
        // Bounded re-evaluation, see the health-screen comment above: the PLM
        // cannot bump this bucket without a second wake seam (R-1 territory).
        deferPending(pending, DeferReason::kPathUnavailable, epochs.path,
                     deadlineAfter(steadyNowNs(), kTimedDeferNs));
        return;
    }

    // ⑧ Record + commit. The record owns the lease and the credit token from
    // here on, so every exit below (including a late CQE, a timeout fence and
    // shutdown) releases them through onOwners() exactly once.
    auto record = makeRecord(pending, endpoint, path, probe_key, reservation,
                             local, remote, attempt);
    record->pool = pool;
    record->pooled_lane = leased.lease.lane();
    record->lease = std::move(leased.lease);
    record->credit_jfc_index = leased.jfc_index;
    record->credit_token = leased.jfc_credit_token;
    record->fault_epoch_snapshot = fault_epoch_snapshot;
    record->attempt_slot_held = true;
    // Business evidence becomes generation-mapped, which is what removes
    // probe_evidence_unmapped (design §10.4).
    record->core.fault_epoch = fault_epoch_snapshot;

    if (!pending.slice->tryCommitPost(attempt, record->posted_ns)) {
        // Cancellation won the linearization point: no WR crossed, so every
        // owner is released and the slice is already terminal. No defer here.
        SliceJettyLease lease = std::move(record->lease);
        if (auto credits = pool->creditLedger()) {
            credits->release(record->credit_jfc_index, record->credit_token);
        }
        (void)pool->returnOnce(std::move(lease), /*gate_passed=*/true);
        pooled_return_.fetch_add(1, std::memory_order_relaxed);
        endpoint->releaseOutstanding(length);
        (void)quota_->release(reservation);
        pending.task->releaseAttemptSlot();
        return;
    }
    record->core.post_claimed.store(true, std::memory_order_release);

    // ⑨ publish + ⑩ post. The record is visible before the native call because
    // a completion may legally arrive first.
    WorkRequest work =
        makeWorkRequest(pending, record->core.token, local, remote);
    {
        gate::HandlerRef post_handler(record->core, *this, gate_ctx_);
        {
            std::lock_guard<std::mutex> lock(records_mutex_);
            records_.emplace(record->core.token, record);
            if (record->pool && record->pooled_lane.lane_generation != 0) {
                // D5: a zero incarnation would create a key shared by every
                // Context. It is unreachable for a pooled record (the endpoint
                // always owns a Context), so count it rather than alias it.
                if (record->context_incarnation == 0) {
                    pooled_lane_ref_unmapped_.fetch_add(
                        1, std::memory_order_relaxed);
                } else {
                    ++pooled_lane_refs_[LaneRefKey{
                        record->context_incarnation,
                        record->pooled_lane.lane_generation}];
                }
            }
        }
        gate_ctx_.stats.record_published.fetch_add(1,
                                                   std::memory_order_relaxed);
        path_load_.onPublish(path, length);

        const auto member = record->lease.member();
        JettyPtr jetty = member ? member->jetty : nullptr;
        size_t posted_count = 0;
        PostAcceptance acceptance = PostAcceptance::kNotAccepted;
        Status status;
        // Anchor the pollers' idle-spin window (yuanhao-1113).
        last_post_ns_.store(record->posted_ns, std::memory_order_relaxed);
        if (!jetty) {
            status = Status::InternalError(
                "UB pooled send Jetty is unavailable" LOC_MARK);
        } else if (endpoint->rmMode()) {
            // RM: the WR carries the peer target explicitly; the leased member
            // is never bound to a remote Jetty. The target slot follows the
            // posting lane, not the member's JFC rotation index - the rotation
            // index is a pool-internal detail unrelated to the peer's Jetty
            // topology (review M6).
            const size_t target_slot =
                worker_index % std::max<size_t>(1, endpoint->jettyCount());
            work.target = endpoint->rmTarget(target_slot);
            if (!work.target) {
                // Never post an RM WR without a valid target: the adapter would
                // otherwise build a WR with no remote Jetty.
                status = Status::InternalError(
                    "UB RM endpoint has no imported target for this "
                    "slot" LOC_MARK);
            } else {
                status =
                    adapter_->post(jetty, {work}, posted_count, acceptance);
            }
        } else {
            status = adapter_->post(jetty, {work}, posted_count, acceptance);
        }
        if (record->pending.task && record->pending.task->traceEnabled()) {
            record->pending.slice->notePostReturned(steadyNowNs());
        }
        if (posted_count > 1) {
            LOG_EVERY_N(WARNING, 100)
                << "UB adapter reported an impossible posted_count "
                << posted_count << " for one WR; token " << record->core.token
                << " drains until a fence proves responsibility";
        }
        if (acceptance == PostAcceptance::kAccepted) {
            rail_monitor_->registerPath(path);
            if (!status.ok()) {
                LOG_EVERY_N(WARNING, 1000)
                    << "UB post returned an error after accepting the WR: "
                    << status.ToString();
            }
        }
        // Observation only: how often the claim window actually leaked a post
        // onto a path that had just been isolated (design §8.2, RK1 urgency).
        if (probe_manager_ != nullptr) {
            const uint64_t current =
                probe_key.valid()
                    ? probe_manager_->businessFaultEpoch(probe_key)
                    : probe_manager_->businessFaultEpoch(path);
            if (current != fault_epoch_snapshot) {
                post_on_isolated_path_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        gate::onPostReturn(
            record->core,
            PostReturn{acceptance,
                       acceptance == PostAcceptance::kAccepted ? length : 0,
                       retryableStatus(status)},
            gate_ctx_);
    }
}

void UbWorkers::setControlExecutor(std::shared_ptr<ControlExecutor> executor) {
    control_executor_ = std::move(executor);
    control_executor_owned_ = false;
}

void UbWorkers::startControlExecutor() {
    if (control_executor_) return;
    // Dedicated executor: draining a Jetty is a long-tail operation, so sharing
    // the queue with the drain chain would let a burst of drains starve the
    // deadline fences (and vice versa).
    auto executor =
        std::make_shared<ThreadedControlExecutor>("ub-control-latency", 4096);
    if (!executor->start()) {
        LOG(WARNING) << "UB cannot start the deadline-fence control executor; "
                        "fences will run on the poller";
        return;
    }
    control_executor_ = std::move(executor);
    control_executor_owned_ = true;
}

void UbWorkers::stopControlExecutor() {
    if (!control_executor_ || !control_executor_owned_) return;
    if (auto threaded = std::dynamic_pointer_cast<ThreadedControlExecutor>(
            control_executor_)) {
        threaded->stop();
    }
    control_executor_.reset();
    control_executor_owned_ = false;
}

void UbWorkers::noteControlJobFinished() {
    {
        std::lock_guard<std::mutex> lock(control_jobs_mutex_);
        if (control_jobs_pending_ != 0) --control_jobs_pending_;
    }
    control_jobs_cv_.notify_all();
}

void UbWorkers::drainPendingControlJobs() {
    std::unique_lock<std::mutex> lock(control_jobs_mutex_);
    // Not an unbounded silent wait: a provider quiesce that overruns its own
    // timeout must be visible in the log. The wait itself is deliberately not
    // abandoned on timeout: proceeding would let a queued fence run natively
    // while stop() is already tearing the same endpoints down, which the drain
    // contract (and INV-1's ordering) forbids. So: warn periodically, keep the
    // safety ordering.
    while (control_jobs_pending_ != 0) {
        const uint64_t pending = control_jobs_pending_;
        if (!control_jobs_cv_.wait_for(lock, std::chrono::seconds(5), [this] {
                return control_jobs_pending_ == 0;
            })) {
            LOG(WARNING) << "UB shutdown is waiting for " << pending
                         << " control job(s) (timeout fence / device retire); "
                            "a provider call is overrunning its timeout";
        }
    }
}

uint64_t UbWorkers::requestDeadlineNs(const PendingSlice& pending) const {
    if (!params_.honor_request_deadline || !pending.task) return 0;
    // Contract (tent/common/types.h): absolute steady_clock timestamp, 0 means
    // "no deadline"; both the engine and this file use steadyNowNs().
    return pending.task->request().deadline_ns;
}

bool UbWorkers::requestExpired(const PendingSlice& pending,
                               uint64_t now_ns) const {
    const uint64_t deadline = requestDeadlineNs(pending);
    return deadline != 0 && now_ns >= deadline;
}

void UbWorkers::expirePending(const PendingSlice& pending,
                              std::atomic<uint64_t>& counter) {
    if (!pending.slice) return;
    // Resolve as TIMEOUT without a cancellation request: requestCancellation()
    // would terminalize the slice as CANCELED, losing why it was dropped.
    if (pending.slice->tryResolveBeforePost(TIMEOUT)) {
        counter.fetch_add(1, std::memory_order_relaxed);
    }
}

void UbWorkers::enqueueTimeoutFence(
    const std::shared_ptr<BusinessRecord>& record) {
    if (!record) return;
    TimeoutFenceJob job{record->core.token, record, record->attempt.attempt};
    if (!control_executor_) {
        // Degraded path (no control thread). The only caller is scanTimeouts(),
        // which already holds endpoint_drain_mutex_ for its claim+enqueue
        // section, so this MUST use the locked variant: taking the mutex here
        // would re-enter a non-recursive std::mutex on the poller thread and
        // deadlock the first deadline timeout (exactly the legacy inline shape,
        // which had one lock for both steps).
        runTimeoutFenceLocked(job);
        return;
    }
    {
        std::lock_guard<std::mutex> lock(control_jobs_mutex_);
        ++control_jobs_pending_;
    }
    const Status submitted = control_executor_->submit([this, job] {
        runTimeoutFence(job);
        noteControlJobFinished();
    });
    if (!submitted.ok()) {
        noteControlJobFinished();
        timeout_fence_rejected_.fetch_add(1, std::memory_order_relaxed);
        // The record stays registered, but the provisional timeout mark must
        // be revoked or the next scan would skip it forever (the scan filters
        // timed_out): a scheduling failure is not fence evidence.
        bool still_registered = false;
        {
            std::lock_guard<std::mutex> lock(records_mutex_);
            auto it = records_.find(record->core.token);
            still_registered = it != records_.end() && it->second == record;
        }
        if (still_registered) gate::onFenceFailed(record->core);
        LOG_EVERY_N(WARNING, 100)
            << "UB timeout fence was not scheduled (" << submitted.ToString()
            << "); the record keeps its responsibility for the next scan";
        return;
    }
    timeout_fence_submitted_.fetch_add(1, std::memory_order_relaxed);
}

void UbWorkers::enqueueDeviceRetire(const UbContextPtr& context) {
    if (!context || !local_device_failure_handler_) return;
    // Only a genuinely failed context whose barrier has not run yet needs the
    // native cleanup. Without this guard the success-path call would enqueue
    // one job per successful poll (during the recovery cooldown as well).
    if (context->state() != UbContext::State::kFailed ||
        context->failureCleanupComplete()) {
        return;
    }
    const Topology::NicID local_nic = context->topologyId();
    {
        std::lock_guard<std::mutex> lock(device_retire_mutex_);
        // The poller observes a failed context on every poll; queueing one job
        // per observation would flood the executor.
        if (!device_retire_queued_.insert(local_nic).second) return;
    }
    auto finish = [this, local_nic] {
        {
            std::lock_guard<std::mutex> lock(device_retire_mutex_);
            device_retire_queued_.erase(local_nic);
        }
        noteControlJobFinished();
    };
    if (!control_executor_) {
        progressLocalDeviceFailure(context);
        finish();
        return;
    }
    {
        std::lock_guard<std::mutex> lock(control_jobs_mutex_);
        ++control_jobs_pending_;
    }
    const Status submitted = control_executor_->submit([this, context, finish] {
        progressLocalDeviceFailure(context);
        finish();
    });
    if (!submitted.ok()) {
        device_retire_rejected_.fetch_add(1, std::memory_order_relaxed);
        // Let a later poll try again instead of leaving the context failed
        // forever behind a dedupe entry.
        {
            std::lock_guard<std::mutex> lock(device_retire_mutex_);
            device_retire_queued_.erase(local_nic);
        }
        noteControlJobFinished();
        LOG_EVERY_N(WARNING, 100)
            << "UB device retirement was not scheduled ("
            << submitted.ToString() << "); the next poll retries it";
    }
}

void UbWorkers::handleLocalDeviceFailure(const UbContextPtr& context) {
    if (!context) return;
    const bool newly_failed = context->markUnavailable();
    if (!newly_failed) return;

    // The probe subsystem must stop probing this port as soon as its context
    // fails, regardless of whether a business cleanup handler is installed.
    if (probe_manager_ != nullptr) {
        probe_manager_->onContextUnavailable(context->topologyId());
    }

    if (!local_device_failure_handler_) {
        LOG(WARNING) << "UB device " << context->deviceInfo().topology_name
                     << " has no endpoint cleanup handler; automatic "
                        "reactivation is disabled";
        return;
    }
    // F6a2: only the native endpoint barrier moves off the poller. The state
    // transition and the PLM notification above stay synchronous so no new
    // post can target the failed device.
    enqueueDeviceRetire(context);
}

void UbWorkers::progressLocalDeviceFailure(const UbContextPtr& context) {
    if (!context || context->state() != UbContext::State::kFailed ||
        !local_device_failure_handler_) {
        return;
    }
    std::lock_guard<std::mutex> lock(local_device_failure_mutex_);
    if (context->state() != UbContext::State::kFailed) return;

    auto status = local_device_failure_handler_(context->topologyId());
    if (status.ok()) {
        // EndpointStore reports success only after every old endpoint has
        // reached Destroyed. This is the native-resource barrier required
        // before JFC health can reactivate the context.
        context->completeFailureCleanup();
        return;
    }
    LOG_EVERY_N(WARNING, 100)
        << "UB endpoint retirement after local device failure is pending: "
        << status.ToString();
}

WrFacts UbWorkers::makeWrFacts(const Completion& completion) {
    WrFacts facts;
    facts.completion_ns = steadyNowNs();
    facts.native_status = completion.native_status;
    facts.completed_bytes = completion.completed_bytes;
    switch (completion.category) {
        case CompletionCategory::SUCCESS:
            facts.outcome = CompletionOutcome::kSuccess;
            break;
        case CompletionCategory::TIMEOUT:
            facts.outcome = CompletionOutcome::kTimeout;
            break;
        case CompletionCategory::LOCAL_DEVICE_ERROR:
            facts.outcome = CompletionOutcome::kLocalDeviceError;
            break;
        case CompletionCategory::REMOTE_PATH_ERROR:
            facts.outcome = CompletionOutcome::kRemotePathError;
            break;
        case CompletionCategory::ENDPOINT_ERROR:
            facts.outcome = CompletionOutcome::kEndpointError;
            break;
        case CompletionCategory::MEMORY_ERROR:
            facts.outcome = CompletionOutcome::kMemoryError;
            break;
        case CompletionCategory::LOCAL_ACCESS_ERROR:
            facts.outcome = CompletionOutcome::kLocalAccessError;
            break;
        case CompletionCategory::UNKNOWN_ERROR:
            facts.outcome = CompletionOutcome::kUnknownError;
            break;
    }
    return facts;
}

void UbWorkers::onSettle(CompletionRecord& record, LogicalOutcome outcome) {
    // The verdict is already published in the record. Delivery deliberately
    // waits for onOwners(): a retry must never be queued before the old owners
    // were released (D-8 / IN-7).
    LOG_EVERY_N(INFO, 1000)
        << "UB completion settled: token=" << record.token
        << " kind=" << recordKindName(record.kind)
        << " path=" << recordPathText(record.path)
        << " generation=" << record.jetty_generation << " acceptance="
        << postAcceptanceName(record.acceptance.load(std::memory_order_relaxed))
        << " terminal=" << completionOutcomeName(record.terminal.outcome)
        << " outcome=" << logicalOutcomeName(outcome);
}

void UbWorkers::onFault(CompletionRecord& record, FaultKind kind) {
    // Called at most once per record; whichever exit claims fault_reported
    // first owns the health side effects (design §6.3).
    auto* business = static_cast<BusinessRecord*>(record.owner);
    if (business == nullptr) return;
    const uint64_t now = steadyNowNs();
    const auto report_error = [&](BusinessErrorKind kind) {
        if (business->probe_key.valid()) {
            if (kind == BusinessErrorKind::kParamError) {
                if (param_error_edges_.noteError(business->probe_key)) {
                    LOG(WARNING)
                        << "UB declared edge invalidated after repeated "
                           "parameter/memory errors: peer="
                        << business->probe_key.segment_id
                        << " local_nic=" << business->probe_key.local_nic
                        << " remote_nic=" << business->probe_key.remote_nic;
                }
            } else {
                param_error_edges_.noteSuccess(business->probe_key);
            }
        }
        if (probe_manager_ == nullptr) return;
        if (business->probe_key.valid()) {
            probe_manager_->onBusinessPathError(business->probe_key, kind, now);
        } else {
            probe_manager_->onBusinessPathError(business->path, kind, now);
        }
        if (kind == BusinessErrorKind::kPathFault &&
            business->probe_key.valid()) {
            std::function<void(uint64_t)> hint;
            {
                std::lock_guard<std::mutex> lock(probe_hint_mutex_);
                hint = probe_refresh_hint_;
            }
            if (hint) hint(business->probe_key.segment_id);
        }
    };
    if (kind == FaultKind::kTimeout) {
        // Deadline evidence without a placeable completion. A late completion
        // resolves the same one-shot, so the rail sees exactly one timeout.
        business_deadline_timeouts_.fetch_add(1, std::memory_order_relaxed);
        if (business->deadline_from_request) {
            expired_inflight_.fetch_add(1, std::memory_order_relaxed);
        }
        rail_monitor_->recordTimeout(business->path, now);
        // A deadline timeout is the strongest path-fault evidence there is:
        // nothing completed and there is no native error code to attribute.
        // Feed the same isolation/probe chain as an adapter-level TIMEOUT,
        // otherwise "suspended with no CQE" - the one case that most needs
        // probe recovery - would never start a probe campaign.
        //
        // Deliberately NOT `endpoint_retirer_`: unlike the adapter-level
        // TIMEOUT, the timeout fence already fenced this specific Jetty, so
        // retiring the whole endpoint would amplify one budget overrun to a
        // device-wide retirement. Keep that difference instead of "aligning"
        // the two paths.
        report_error(BusinessErrorKind::kPathFault);
        const uint64_t age_us = now > business->posted_ns
                                    ? (now - business->posted_ns) / 1000ULL
                                    : 0ULL;
        LOG_EVERY_N(WARNING, 100)
            << "UB business deadline timeout: token=" << record.token
            << " task="
            << reinterpret_cast<uint64_t>(business->pending.task.get())
            << " slice=" << business->attempt.slice_id
            << " attempt=" << business->attempt.attempt
            << " local_nic=" << record.path.local_nic
            << " remote_device=" << record.path.remote_nic
            << " remote_segment=" << record.path.segment_id
            << " jetty_gen=" << record.jetty_generation
            << " lane_gen=" << business->pooled_lane.lane_generation
            << " posted_age_us=" << age_us
            << " deadline_us=" << params_.slice_timeout_ms * 1000ULL;
        return;
    }
    const char* category = "unknown";
    switch (record.terminal.outcome) {
        case CompletionOutcome::kRemotePathError:
            category = "path_fault";
            break;
        case CompletionOutcome::kTimeout:
            category = "timeout";
            break;
        case CompletionOutcome::kMemoryError:
            category = "param_error";
            break;
        case CompletionOutcome::kLocalAccessError:
            category = "path_fault";
            break;
        case CompletionOutcome::kLocalDeviceError:
            category = "context_failure";
            break;
        case CompletionOutcome::kEndpointError:
            category = "lane_error";
            break;
        case CompletionOutcome::kUnknownError:
            category = "unknown";
            break;
        case CompletionOutcome::kSuccess:
            category = "success";
            break;
    }
    LOG_EVERY_N(WARNING, 20)
        << "UB business error: token=" << record.token
        << " task=" << reinterpret_cast<uint64_t>(business->pending.task.get())
        << " slice=" << business->attempt.slice_id
        << " attempt=" << business->attempt.attempt
        << " kind=" << recordKindName(record.kind)
        << " local_nic=" << record.path.local_nic
        << " remote_device=" << record.path.remote_nic
        << " remote_segment=" << record.path.segment_id << " acceptance="
        << postAcceptanceName(record.acceptance.load(std::memory_order_relaxed))
        << " terminal=" << completionOutcomeName(record.terminal.outcome)
        << " category=" << category << " jetty_gen=" << record.jetty_generation
        << " lane_gen=" << business->pooled_lane.lane_generation << " pool_gen="
        << (business->endpoint && business->endpoint->context()
                ? business->endpoint->context()->incarnation()
                : 0);
    switch (record.terminal.outcome) {
        case CompletionOutcome::kLocalDeviceError:
            if (business->endpoint && business->endpoint->context()) {
                handleLocalDeviceFailure(business->endpoint->context());
            }
            if (probe_manager_ != nullptr) {
                // kContextFailure disables every path on the port; the PLM fans
                // it out to onContextUnavailable().
                report_error(BusinessErrorKind::kContextFailure);
            }
            rail_monitor_->recordError(business->path, now);
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            return;
        case CompletionOutcome::kRemotePathError:
            if (probe_manager_ != nullptr) {
                report_error(BusinessErrorKind::kPathFault);
            }
            rail_monitor_->recordError(business->path, now);
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            return;
        case CompletionOutcome::kEndpointError:
            if (probe_manager_ != nullptr) {
                report_error(BusinessErrorKind::kLaneError);
            }
            rail_monitor_->recordError(business->path, now);
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            return;
        case CompletionOutcome::kLocalAccessError:
            // A link-down can surface as a local access error. Treat it like a
            // remote path fault: isolate the path and let the slice retry on
            // another one. A genuine token/permission error exhausts the
            // bounded retry budget instead of failing on the first attempt.
            if (probe_manager_ != nullptr) {
                report_error(BusinessErrorKind::kPathFault);
            }
            rail_monitor_->recordError(business->path, now);
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            return;
        case CompletionOutcome::kMemoryError:
            if (probe_manager_ != nullptr) {
                report_error(BusinessErrorKind::kParamError);
            }
            rail_monitor_->recordError(business->path, now);
            return;
        case CompletionOutcome::kUnknownError:
            if (probe_manager_ != nullptr) {
                report_error(BusinessErrorKind::kUnknown);
            }
            rail_monitor_->recordError(business->path, now);
            return;
        case CompletionOutcome::kTimeout:
            // Adapter-level TIMEOUT category (not the deadline scan).
            if (probe_manager_ != nullptr) {
                report_error(BusinessErrorKind::kPathFault);
            }
            rail_monitor_->recordTimeout(business->path, now);
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            return;
        case CompletionOutcome::kSuccess:
            return;
    }
}

void UbWorkers::onOwners(CompletionRecord& record,
                         OwnerDisposition disposition) {
    auto* business = static_cast<BusinessRecord*>(record.owner);
    if (business == nullptr) return;
    switch (disposition) {
        case OwnerDisposition::kReturn:
            // §6.8 order: release the lease/outstanding accounting first, then
            // the quota, then the segment references, and only then deliver the
            // logical result. Anything else could admit a new attempt while the
            // old one still holds capacity.
            releaseOwnerResources(*business, disposition);
            recordSuccessEvidence(*business);
            break;
        case OwnerDisposition::kFinalize:
            releaseOwnerResources(*business, disposition);
            break;
        case OwnerDisposition::kReject:
            // The legacy not-accepted rollback: free the accounting, bill the
            // rail, retire the endpoint, then deliver FAILED/retryable.
            releaseOwnerResources(*business, disposition);
            rail_monitor_->recordError(business->path, steadyNowNs());
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            break;
    }
    // §6.8 steps 3-4: drop the target/segment/buffer references after the quota
    // accounting and before the slice may be resubmitted.
    business->local_segment.reset();
    business->remote_segment.reset();
    business->endpoint.reset();
    // Registry erase belongs to the completion claimant, never to the post side
    // (D-3 / IN-4). Duplicate completions that arrive after this point are
    // accounted as unknown tokens.
    eraseRecord(record.token, business);
    deliverSettlement(*business);
}

void UbWorkers::onBlocked(CompletionRecord& record, BlockReason reason) {
    // Observational only: the gate already maintains gate_blocked_by_*.
    if (params_.observability.verbose_events || VLOG_IS_ON(1)) {
        if (params_.observability.verbose_events) {
            LOG(INFO)
                << "UB completion gate blocked: token=" << record.token
                << " kind=" << recordKindName(record.kind)
                << " path=" << recordPathText(record.path)
                << " generation=" << record.jetty_generation << " acceptance="
                << postAcceptanceName(
                       record.acceptance.load(std::memory_order_relaxed))
                << " blocked_by=" << blockReasonName(reason)
                << " post_returned="
                << (record.post_returned.load(std::memory_order_acquire) ? 1
                                                                         : 0)
                << " early_cqe="
                << ((record.wr_terminal.load(std::memory_order_acquire) &&
                     !record.post_returned.load(std::memory_order_acquire))
                        ? 1
                        : 0)
                << " handler_refs="
                << record.handler_refs.load(std::memory_order_relaxed)
                << " fault="
                << (record.fault_reported.load(std::memory_order_relaxed) ? 1
                                                                          : 0);
        } else {
            VLOG(1) << "UB completion gate blocked: token=" << record.token
                    << " kind=" << recordKindName(record.kind)
                    << " path=" << recordPathText(record.path)
                    << " generation=" << record.jetty_generation
                    << " acceptance="
                    << postAcceptanceName(
                           record.acceptance.load(std::memory_order_relaxed))
                    << " blocked_by=" << blockReasonName(reason)
                    << " post_returned="
                    << (record.post_returned.load(std::memory_order_acquire)
                            ? 1
                            : 0)
                    << " early_cqe="
                    << ((record.wr_terminal.load(std::memory_order_acquire) &&
                         !record.post_returned.load(std::memory_order_acquire))
                            ? 1
                            : 0)
                    << " handler_refs="
                    << record.handler_refs.load(std::memory_order_relaxed)
                    << " fault="
                    << (record.fault_reported.load(std::memory_order_relaxed)
                            ? 1
                            : 0);
        }
    }
}

void UbWorkers::onDuplicate(CompletionRecord& record) {
    LOG_EVERY_N(WARNING, 1000)
        << "UB duplicate completion ignored: token=" << record.token
        << " path=" << recordPathText(record.path)
        << " generation=" << record.jetty_generation;
}

void UbWorkers::scanTimeouts() {
    // F6a: this function runs on a JFC poller, so it must never call into the
    // provider. It snapshots, claims once and enqueues; runTimeoutFence() does
    // the native fence on the control executor. The drain mutex is still held
    // for the claim+enqueue section so stop()'s barrier proves no fence job can
    // appear after it.
    std::lock_guard<std::mutex> drain_lock(endpoint_drain_mutex_);
    if (!timeout_scans_enabled_.load(std::memory_order_acquire)) return;
    const uint64_t now = steadyNowNs();
    std::vector<std::shared_ptr<BusinessRecord>> expired;
    {
        std::lock_guard<std::mutex> lock(records_mutex_);
        for (const auto& [_, record] : records_) {
            if (record->deadline_ns <= now &&
                !record->core.timed_out.load(std::memory_order_acquire)) {
                expired.push_back(record);
            }
        }
    }
    for (const auto& record : expired) {
        // D-7: a record whose native post has not returned is not fenced yet.
        // Marking it now would let the scan race a post call that is still
        // using the record's segments; the next scan picks it up.
        if (record->core.handler_refs.load(std::memory_order_acquire) != 0) {
            gate_ctx_.stats.timeout_deferred_by_post_return.fetch_add(
                1, std::memory_order_relaxed);
            continue;
        }
        // One-shot mark. The fault side effects stay unclaimed until there is
        // evidence: a revoked timeout must not swallow the real fault (IN-11).
        if (!gate::onTimeout(record->core)) continue;
        enqueueTimeoutFence(record);
    }
}

void UbWorkers::runTimeoutFence(const TimeoutFenceJob& job) {
    std::lock_guard<std::mutex> drain_lock(endpoint_drain_mutex_);
    runTimeoutFenceLocked(job);
}

void UbWorkers::runTimeoutFenceLocked(const TimeoutFenceJob& job) {
    // Precondition: endpoint_drain_mutex_ is held by the caller (either the
    // control executor's runTimeoutFence wrapper or scanTimeouts' inline
    // degraded path).
    const std::shared_ptr<BusinessRecord>& record = job.record;
    if (!record) return;
    std::vector<Completion> drained;
    Status status;
    JettyPtr pooled_jetty;
    if (record->pool && record->lease.valid()) {
        // D-14 / W3-IN-10: in pooled mode the fence target is the leased
        // member's Jetty. `endpoint->jetty(worker_index)` belongs to another,
        // unused Jetty set and fencing it would prove nothing.
        const auto member = record->lease.member();
        pooled_jetty = member ? member->jetty : nullptr;
    }
    if (pooled_jetty) {
        status = adapter_->quiesceJetty(pooled_jetty, params_.slice_timeout_ms,
                                        drained);
    } else if (record->pool) {
        status = Status::InvalidArgument(
            "Timed-out pooled UB WR has no leased Jetty" LOC_MARK);
    } else {
        status =
            record->endpoint
                ? record->endpoint->quiesce(params_.slice_timeout_ms, drained)
                : Status::InvalidArgument(
                      "Timed-out UB WR has no endpoint" LOC_MARK);
    }
    // Even a partial/failed fence may have returned valid WR completions; they
    // must never be lost and must be settled by the same dispatcher.
    for (const auto& completion : drained) {
        dispatchCompletion(completion, record->core.path.local_nic);
    }
    if (!status.ok()) {
        // The pooled path keeps the endpoint alive: the member is the only
        // resource that can be at fault, and the record stays registered so a
        // later scan retries the fence.
        if (!record->pool) rememberEndpointDrain(record->endpoint);
        LOG_EVERY_N(ERROR, 100)
            << "Cannot establish UB timeout drain fence: " << status.ToString();
        // Leave the token and all resource references alive. A natural
        // completion can still resolve it safely; otherwise a later scan
        // retries the native fence.
        bool still_registered = false;
        {
            std::lock_guard<std::mutex> lock(records_mutex_);
            auto it = records_.find(record->core.token);
            still_registered = it != records_.end() && it->second == record;
        }
        if (still_registered) gate::onFenceFailed(record->core);
        return;
    }
    if (!record->pool) {
        forgetEndpointDrain(record->endpoint);
        if (endpoint_retirer_) endpoint_retirer_(record->endpoint);
    }
    // The erase and the "is this record still mine?" check happen in one
    // critical section: if a drained completion already claimed and disposed of
    // the record, this fence must not release the owners again (IN-4).
    eraseRecord(record->core.token, record.get());
    // quiesce() is the proof that retry cannot overlap old DMA. The fence
    // evidence then settles TIMEOUT and finalizes once (§6.7).
    gate::onFenceProven(record->core, *this, gate_ctx_);
}

bool UbWorkers::settleDrainedWr(const Completion& completion,
                                uint64_t jfc_index) {
    (void)jfc_index;
    if (completion.token == 0) return false;
    // A software-flush WR is a WR the provider accepted but never completed. It
    // settles through the same gate as a real CQE, exactly once. Looking the
    // record up here and settling it directly keeps two properties that routing
    // through dispatchCompletion() could not:
    //   - a token that was already claimed and disposed is reported as "not
    //     settled" instead of being counted as an unknown token (it is a
    //     duplicate of a settled WR, not a topology fault);
    //   - the one-shot gate claim (cr_claimed / finalize_claimed) is still the
    //     exactly-once authority, so a concurrent disposal cannot double
    //     release even if it happens after the lookup.
    auto record = findRecord(completion.token);
    if (!record) return false;
    if (probe_manager_ != nullptr && isProbeToken(completion.token)) {
        // Namespace first, exactly like dispatchCompletion().
        (void)probe_manager_->onProbeCompletion(completion);
        return true;
    }
    gate::HandlerRef handler(record->core, *this, gate_ctx_);
    gate::onRealCr(record->core, makeWrFacts(completion), *this, gate_ctx_);
    return true;
}

bool UbWorkers::pooledLaneConverged(const LaneRefKey& lane) const {
    if (lane.lane_generation == 0) return true;
    std::lock_guard<std::mutex> lock(records_mutex_);
    auto refs = pooled_lane_refs_.find(lane);
    return refs == pooled_lane_refs_.end() || refs->second == 0;
}

std::shared_ptr<UbWorkers::BusinessRecord> UbWorkers::findRecord(
    uint64_t token) const {
    // The registry lock is only held to take a shared_ptr; every consumer works
    // on the reference outside the lock (design §5.6 rule 2).
    std::lock_guard<std::mutex> lock(records_mutex_);
    auto it = records_.find(token);
    return it == records_.end() ? nullptr : it->second;
}

void UbWorkers::releaseOwnerResources(BusinessRecord& record,
                                      OwnerDisposition disposition) {
    // Exactly one call per record: the gate's claim bit already guaranteed it
    // (W3-IN-1). Order is fixed: endpoint accounting, quota, pool member,
    // credit, K slot (design §10.1).
    const size_t length =
        record.pending.slice ? record.pending.slice->spec().length : 0;
    if (record.endpoint) record.endpoint->releaseOutstanding(length);
    if (record.quota.valid()) (void)quota_->release(record.quota);
    bool pool_woken = false;
    if (record.pool && record.lease.valid()) {
        if (disposition == OwnerDisposition::kFinalize) {
            // A first real error, a proven timeout or a shutdown bottom-out:
            // the member is retired permanently and never re-enters the LIFO
            // stack (INV07). The pool is the arbiter, so a pool that is already
            // stopping also ends here.
            const RecordDropReason drop =
                record.core.orphan_reason.load(std::memory_order_acquire);
            const std::string reason =
                drop != RecordDropReason::kNone
                    ? recordDropReasonName(drop)
                    : completionOutcomeName(record.core.terminal.outcome);
            // A real error completion must pass the hardware retirement fence
            // (modify ERROR -> flush-done -> software flush) before the member
            // may be reset and deleted (design §7.2 rule 5). A proven timeout
            // fence and the shutdown bottom-out already fenced the member, so
            // they must not pay for a second, blocking provider call.
            const bool hardware_fault =
                drop == RecordDropReason::kCompletionError ||
                (drop == RecordDropReason::kNone &&
                 record.core.terminal.outcome != CompletionOutcome::kSuccess &&
                 record.core.terminal.outcome != CompletionOutcome::kTimeout);
            (void)record.pool->retireFaulted(std::move(record.lease), reason,
                                             hardware_fault);
            pooled_retire_.fetch_add(1, std::memory_order_relaxed);
        } else {
            // kReturn and kReject both hand the member back: a rejected post
            // never crossed the native boundary, so the member is clean
            // (W3-IN-2). The pool turns a return into a retirement by itself
            // when it is stopping.
            (void)record.pool->returnOnce(std::move(record.lease),
                                          /*gate_passed=*/true);
            pooled_return_.fetch_add(1, std::memory_order_relaxed);
        }
        // Credit is released after the member: the token is bound to the
        // member's JFC, so releasing it first would let the ledger admit a new
        // WR for a JFC whose previous member is still not back (design §10.1).
        if (auto credits = record.pool->creditLedger()) {
            credits->release(record.credit_jfc_index, record.credit_token);
        }
        pool_woken = true;
    }
    // W3-IN-5: the task-level K slot ends exactly here, i.e. with the end of
    // native responsibility for this attempt - never before onOwners.
    if (record.attempt_slot_held) {
        record.attempt_slot_held = false;
        if (record.pending.task) record.pending.task->releaseAttemptSlot();
    }
    // W3-IN-7: the ledger is decremented at the same point as the quota
    // release, with the same path the publish used, so the pair always
    // balances.
    if (record.path.valid()) path_load_.onFinalize(record.path, length);
    // Wake exactly the buckets this release unblocked (design §7.4). The quota
    // release and the K release happen for every record; a pool return wakes
    // the pool bucket only on the pooled path.
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        bumpLocked(DeferReason::kTaskPermit);
        bumpLocked(DeferReason::kQuota);
        if (pool_woken) bumpLocked(DeferReason::kPoolIdle);
    }
    queue_cv_.notify_all();
}

void UbWorkers::eraseRecord(uint64_t token, const BusinessRecord* record) {
    bool erased = false;
    {
        std::lock_guard<std::mutex> lock(records_mutex_);
        auto it = records_.find(token);
        if (it != records_.end() && it->second.get() == record) {
            records_.erase(it);
            const uint64_t generation = record->pooled_lane.lane_generation;
            if (generation != 0 && record->context_incarnation != 0) {
                auto refs = pooled_lane_refs_.find(
                    LaneRefKey{record->context_incarnation, generation});
                if (refs != pooled_lane_refs_.end()) {
                    if (refs->second <= 1) {
                        pooled_lane_refs_.erase(refs);
                    } else {
                        --refs->second;
                    }
                }
            }
            erased = true;
        }
    }
    if (erased) inflight_cv_.notify_all();
}

void UbWorkers::deliverSettlement(BusinessRecord& record) {
    // The verdict was decided under the settle claim; this is delivery only
    // (D-8 / IN-7), so a retry can never be queued before the memory is safe.
    TransferStatusEnum outcome = FAILED;
    size_t bytes = 0;
    bool retryable = false;
    switch (record.core.logical_outcome) {
        case LogicalOutcome::kCompleted:
            outcome = COMPLETED;
            bytes = record.pending.slice->spec().length;
            break;
        case LogicalOutcome::kTimeout:
            outcome = TIMEOUT;
            retryable = record.core.logical_retryable;
            break;
        case LogicalOutcome::kFailed:
        case LogicalOutcome::kCanceled:
            // A provably unposted WR keeps the legacy wire result: upstream §8
            // freezes "post definitely not accepted" as FAILED/NotExecuted, so
            // W2 does not turn it into CANCELED even though the gate classifies
            // the logical result as not executed.
            outcome = FAILED;
            retryable = record.core.logical_retryable;
            break;
        case LogicalOutcome::kPending:
            return;
    }
    const auto resolution = record.pending.slice->resolveAttempt(
        record.attempt, outcome, bytes, retryable);
    if (resolution == UbAttemptResolution::kRetryScheduled) {
        enqueueRetry(record.pending);
    }
}

void UbWorkers::recordSuccessEvidence(BusinessRecord& record) {
    // Success evidence feeds the probe health state machine and the rail EWMA.
    // It is recorded once, under the normal-return claim, in the same order the
    // baseline used (owners released first, then resolve).
    const uint64_t now = steadyNowNs();
    const uint64_t latency =
        now >= record.posted_ns ? now - record.posted_ns : 0;
    if (record.probe_key.valid()) {
        param_error_edges_.noteSuccess(record.probe_key);
    }
    if (probe_manager_ != nullptr) {
        if (record.probe_key.valid()) {
            probe_manager_->onBusinessPathSuccess(record.probe_key, now);
        } else {
            probe_manager_->onBusinessPathSuccess(record.path, now);
        }
    }
    rail_monitor_->recordSuccess(
        record.path, record.pending.slice->spec().length, latency, now);
}

void UbWorkers::rememberEndpointDrain(
    const std::shared_ptr<UbEndpoint>& endpoint) {
    if (!endpoint) return;
    std::lock_guard<std::mutex> lock(records_mutex_);
    draining_endpoints_[endpoint->generation()] = endpoint;
}

void UbWorkers::forgetEndpointDrain(
    const std::shared_ptr<UbEndpoint>& endpoint) {
    if (!endpoint) return;
    std::lock_guard<std::mutex> lock(records_mutex_);
    auto it = draining_endpoints_.find(endpoint->generation());
    if (it != draining_endpoints_.end() && it->second == endpoint) {
        draining_endpoints_.erase(it);
    }
}

void UbWorkers::failUnposted(const PendingSlice& pending,
                             TransferStatusEnum outcome) {
    if (pending.slice) (void)pending.slice->tryResolveBeforePost(outcome);
}

uint64_t UbWorkers::nextCompletionToken() {
    uint64_t token = next_token_.fetch_add(1, std::memory_order_relaxed);
    if (token == 0) token = next_token_.fetch_add(1, std::memory_order_relaxed);
    return token;
}

}  // namespace mooncake::tent::ub
