// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/workers.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <unordered_set>
#include <utility>

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

// Rail health and quota are keyed by the physical rail, which deliberately
// excludes the endpoint generation (see UbRailKey). These read-only probes
// therefore only need a generation that satisfies UbPostPath::valid(); real
// endpoint generations are monotonic and never below this value.
constexpr uint64_t kRailProbeEndpointGeneration = 1;

// Number of candidates always taken in pre-score order, regardless of which
// local device they belong to. Two is the floor that keeps tryAcquireFirst from
// deferring a slice when its top pick is preempted by another worker, including
// on a single-local-device topology where per-device coverage alone would leave
// only one candidate.
constexpr size_t kMinResolvedCandidates = 2;

}  // namespace

struct UbWorkers::Route {
    SegmentDescRef pin;
    BufferDesc* remote_buffer{nullptr};
    UbBufferMetadata metadata;
    std::vector<Topology::NicID> remote_devices;
};

struct UbWorkers::Inflight {
    uint64_t completion_token{0};
    PendingSlice pending;
    UbAttemptToken attempt;
    UbPostPath path;
    std::shared_ptr<UbEndpoint> endpoint;
    LocalSegmentPtr local_segment;
    RemoteSegmentPtr remote_segment;
    QuotaReservation quota;
    uint64_t posted_ns{0};
    uint64_t deadline_ns{0};
    std::atomic<bool> timed_out{false};
    std::atomic<bool> timeout_recorded{false};
    std::atomic<bool> resources_released{false};
    // rm_pool data-path owners. pooled_pool is null on legacy paths; the
    // lease/permit/gate/credit fields are only meaningful when it is set.
    // The lease is settled exactly once through settlePooledAttempt(); a
    // dropped lease would be counted as a leak by the pool, so every exit
    // path must route through the settlement helper.
    std::shared_ptr<SendJettyPool> pooled_pool;
    SliceJettyLease pooled_lease;
    TaskPermitTicket pooled_permit;
    uint64_t gate_token{0};
    uint64_t jfc_credit_token{0};
    size_t pooled_jfc_index{0};
    std::atomic<bool> pooled_settled{false};
};

UbWorkers::UbWorkers(std::shared_ptr<UrmaAdapter> adapter,
                     std::vector<UbContextPtr> contexts,
                     std::shared_ptr<Topology> local_topology,
                     SegmentManager* segment_manager, UbBufferManager* buffers,
                     RailMonitor* rail_monitor, QuotaManager* quota,
                     UbParams params, EndpointResolver endpoint_resolver,
                     EndpointRetirer endpoint_retirer,
                     LocalDeviceFailureHandler local_device_failure_handler,
                     std::shared_ptr<TaskPermitLedger> task_permits,
                     std::shared_ptr<AttemptGate> attempt_gate)
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
      task_permits_(std::move(task_permits)),
      attempt_gate_(std::move(attempt_gate)) {
    for (const auto& context : contexts_) {
        if (!context) continue;
        context_by_topology_id_[context->topologyId()] = context;
        for (const auto& jfc : context->jfcs()) {
            if (jfc) {
                all_jfcs_.push_back(jfc);
                context_by_jfc_[jfc.get()] = context;
            }
        }
    }
}

UbWorkers::~UbWorkers() { (void)stop(); }

Status UbWorkers::start() {
    bool expected = false;
    if (!accepting_.compare_exchange_strong(expected, true,
                                            std::memory_order_acq_rel)) {
        return Status::InvalidArgument("UB workers already started" LOC_MARK);
    }
    if (!adapter_ || contexts_.empty() || all_jfcs_.empty() ||
        !segment_manager_ || !buffers_ || !rail_monitor_ || !quota_ ||
        !endpoint_resolver_ || params_.worker_count == 0 ||
        params_.poller_count == 0) {
        accepting_.store(false, std::memory_order_release);
        return Status::InvalidArgument(
            "UB workers have incomplete dependencies" LOC_MARK);
    }

    posting_.store(true, std::memory_order_release);
    polling_.store(true, std::memory_order_release);
    timeout_scans_enabled_.store(true, std::memory_order_release);
    try {
        posting_threads_.reserve(params_.worker_count);
        for (uint32_t i = 0; i < params_.worker_count; ++i) {
            posting_threads_.emplace_back(&UbWorkers::postingLoop, this, i);
        }
        const size_t poller_count =
            std::min<size_t>(params_.poller_count, all_jfcs_.size());
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

    std::vector<PendingSlice> abandoned;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& queue : queues_) {
            while (!queue.empty()) {
                abandoned.push_back(std::move(queue.front()));
                queue.pop_front();
            }
        }
    }
    for (auto& pending : abandoned) {
        if (pending.slice) pending.slice->requestCancellation();
    }

    for (auto& thread : posting_threads_) {
        if (thread.joinable()) thread.join();
    }
    posting_threads_.clear();

    // Wait for a timeout scan that began before the flag change. scanTimeouts
    // rechecks under this mutex, so no new endpoint can enter the drain set
    // after this barrier and escape the shutdown snapshot.
    {
        std::lock_guard<std::mutex> lock(endpoint_drain_mutex_);
    }

    // Snapshot all native work after posting threads have joined; no new WR
    // can cross the adapter boundary beyond this point. Quiesce each endpoint
    // before stopping pollers. A failed fence leaves pollers, tokens, native
    // segments, quota, and endpoints intact so uninstall can be retried
    // safely; fixed sleeps are never treated as proof that DMA stopped.
    std::vector<std::shared_ptr<Inflight>> still_inflight;
    std::vector<std::shared_ptr<UbEndpoint>> endpoints_to_fence;
    std::vector<std::shared_ptr<Inflight>> pooled_to_fence;
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        still_inflight.reserve(inflight_.size());
        for (const auto& [_, inflight] : inflight_) {
            still_inflight.push_back(inflight);
            if (!inflight) continue;
            if (inflight->pooled_pool && inflight->pooled_lease.valid() &&
                inflight->pooled_lease.member()) {
                // The WR rode a pooled member; the fence must quiesce the
                // member's Jetty, not the endpoint's Jetty set.
                pooled_to_fence.push_back(inflight);
            } else if (inflight->endpoint) {
                endpoints_to_fence.push_back(inflight->endpoint);
            }
        }
        for (const auto& [_, endpoint] : draining_endpoints_) {
            if (endpoint) endpoints_to_fence.push_back(endpoint);
        }
    }
    for (const auto& inflight : still_inflight) {
        if (inflight && inflight->pending.slice) {
            inflight->pending.slice->requestCancellation();
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
            if (completion.token != 0) handleCompletion(completion);
        }
        if (!status.ok()) {
            rememberEndpointDrain(endpoint);
            if (fence_error.ok()) fence_error = status;
            continue;
        }
        forgetEndpointDrain(endpoint);
        if (endpoint_retirer_) endpoint_retirer_(endpoint);
    }
    for (const auto& inflight : pooled_to_fence) {
        // The shutdown fence proves no more DMA from the pooled member before
        // context teardown destroys native resources.
        std::vector<Completion> drained;
        auto status = adapter_->quiesceJetty(
            inflight->pooled_lease.member()->jetty, params_.slice_timeout_ms,
            drained);
        for (const auto& completion : drained) {
            if (completion.token != 0) handleCompletion(completion);
        }
        if (!status.ok()) {
            if (fence_error.ok()) fence_error = status;
            continue;
        }
        settlePooledAttempt(inflight, PooledSettleKind::kFence, 0,
                            "shutdown fence");
    }
    if (!fence_error.ok()) {
        return fence_error;
    }
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        if (!draining_endpoints_.empty()) {
            return Status::InternalError(
                "UB endpoint drain set changed during shutdown" LOC_MARK);
        }
    }

    // Successful endpoint fences prove that any token still missing from the
    // JFC can no longer touch memory. Let pollers consume already-queued CRs,
    // then stop them and safely resolve any provider-lost token.
    if (!still_inflight.empty()) {
        std::unique_lock<std::mutex> lock(inflight_mutex_);
        (void)inflight_cv_.wait_for(lock, std::chrono::milliseconds(100),
                                    [this] { return inflight_.empty(); });
    }
    polling_.store(false, std::memory_order_release);
    for (auto& thread : polling_threads_) {
        if (thread.joinable()) thread.join();
    }
    polling_threads_.clear();

    still_inflight.clear();
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        for (auto& [_, inflight] : inflight_) {
            still_inflight.push_back(std::move(inflight));
        }
        inflight_.clear();
    }
    for (const auto& inflight : still_inflight) {
        // Idempotent: a fence-drained completion may have settled the pooled
        // attempt already; this only covers still-unsettled leases.
        settlePooledAttempt(inflight, PooledSettleKind::kFence, 0,
                            "provider-lost token");
        releaseInflight(inflight);
        (void)inflight->pending.slice->resolveAttempt(inflight->attempt, FAILED,
                                                      0, false);
    }
    inflight_cv_.notify_all();
    return Status::OK();
}

Status UbWorkers::submit(const UbTask::Ptr& task, uint64_t device_mask) {
    if (!task) {
        return Status::InvalidArgument("Cannot submit a null UB task" LOC_MARK);
    }
    if (!accepting_.load(std::memory_order_acquire)) {
        return Status::InvalidArgument(
            "UB workers are not accepting work" LOC_MARK);
    }
    const auto snapshot = task->snapshot();
    if (!snapshot.sealed) {
        return Status::InvalidArgument(
            "UB task must be sealed before submission" LOC_MARK);
    }
    const int priority =
        std::clamp(task->request().priority, static_cast<int>(PRIO_HIGH),
                   static_cast<int>(PRIO_LOW));

    std::vector<PendingSlice> pending;
    for (const auto& slice : task->slices()) {
        if (slice && slice->markQueued()) {
            pending.push_back(PendingSlice{task, slice, device_mask, priority,
                                           task->request().target_id,
                                           task->request().opcode});
        }
    }
    bool stopped_during_submit = false;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!accepting_.load(std::memory_order_relaxed)) {
            stopped_during_submit = true;
        } else {
            for (auto& item : pending) {
                queues_[item.priority].push_back(std::move(item));
            }
        }
    }
    if (stopped_during_submit) {
        for (auto& item : pending) item.slice->requestCancellation();
        return Status::InvalidArgument(
            "UB workers stopped during submission" LOC_MARK);
    }
    if (!pending.empty()) queue_cv_.notify_all();
    return Status::OK();
}

Status UbWorkers::cancel(const UbTask::Ptr& task) {
    if (!task) {
        return Status::InvalidArgument("Cannot cancel a null UB task" LOC_MARK);
    }
    (void)task->requestCancellation();
    queue_cv_.notify_all();
    return Status::OK();
}

size_t UbWorkers::queuedCount() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    size_t count = 0;
    for (const auto& queue : queues_) count += queue.size();
    return count;
}

size_t UbWorkers::inflightCount() const {
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    return inflight_.size();
}

bool UbWorkers::popPending(PendingSlice& pending) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [this] {
        if (!posting_.load(std::memory_order_acquire)) return true;
        for (const auto& queue : queues_) {
            if (!queue.empty()) return true;
        }
        return false;
    });
    if (!posting_.load(std::memory_order_acquire)) return false;
    for (auto& queue : queues_) {
        if (!queue.empty()) {
            pending = std::move(queue.front());
            queue.pop_front();
            return true;
        }
    }
    return false;
}

void UbWorkers::postingLoop(size_t worker_index) {
    while (posting_.load(std::memory_order_acquire)) {
        PendingSlice pending;
        if (!popPending(pending)) continue;
        if (pending.slice) processPending(pending, worker_index);
    }
}

void UbWorkers::pollingLoop(size_t poller_index) {
    const size_t poller_count = std::max<size_t>(
        1, std::min<size_t>(params_.poller_count, all_jfcs_.size()));
    uint64_t last_timeout_scan = 0;
    while (polling_.load(std::memory_order_acquire)) {
        bool progressed = false;
        for (size_t index = poller_index; index < all_jfcs_.size();
             index += poller_count) {
            std::vector<Completion> completions;
            auto status = all_jfcs_[index]->poll(64, completions);
            auto context = context_by_jfc_.find(all_jfcs_[index].get());
            if (!status.ok()) {
                if (context != context_by_jfc_.end() && context->second) {
                    handleLocalDeviceFailure(context->second);
                }
                LOG_EVERY_N(WARNING, 1000)
                    << "UB JFC poll failed: " << status.ToString();
                continue;
            }
            if (context != context_by_jfc_.end() && context->second) {
                progressLocalDeviceFailure(context->second);
                if (context->second->recordPollSuccess(
                        all_jfcs_[index]->index(),
                        static_cast<uint64_t>(params_.endpoint_cooldown_ms) *
                            1'000'000ULL)) {
                    LOG(INFO) << "Recovered UB device "
                              << context->second->deviceInfo().topology_name
                              << " after endpoint retirement and an "
                                 "error-free JFC cooldown";
                }
            }
            progressed = progressed || !completions.empty();
            for (const auto& completion : completions) {
                if (completion.token != 0) handleCompletion(completion);
            }
        }
        const uint64_t now = steadyNowNs();
        if (timeout_scans_enabled_.load(std::memory_order_acquire) &&
            now - last_timeout_scan >= 1'000'000ULL) {
            scanTimeouts();
            last_timeout_scan = now;
        }
        if (!progressed)
            std::this_thread::sleep_for(std::chrono::microseconds(20));
    }
}

void UbWorkers::enqueueRetry(const PendingSlice& pending) {
    if (!pending.slice) return;
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
        }
    }
    if (stopped) {
        pending.slice->requestCancellation();
        return;
    }
    queue_cv_.notify_one();
}

void UbWorkers::deferPending(const PendingSlice& pending) {
    if (!pending.slice || pending.slice->cancellationRequested() ||
        !posting_.load(std::memory_order_acquire)) {
        if (pending.slice) pending.slice->requestCancellation();
        return;
    }
    // Capacity pressure is not a transfer failure. A short bounded backoff
    // prevents a posting lane from exhausting CPU while completions release
    // quota; releaseInflight() also wakes the queue condition variable.
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        (void)queue_cv_.wait_for(lock, std::chrono::microseconds(50), [&] {
            return !posting_.load(std::memory_order_relaxed) ||
                   pending.slice->cancellationRequested();
        });
        if (posting_.load(std::memory_order_relaxed) &&
            !pending.slice->cancellationRequested()) {
            queues_[std::clamp(pending.priority, static_cast<int>(PRIO_HIGH),
                               static_cast<int>(PRIO_LOW))]
                .push_back(pending);
            lock.unlock();
            queue_cv_.notify_one();
            return;
        }
    }
    pending.slice->requestCancellation();
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
            route.remote_devices = orderedRemoteDevices(*segment, *buffer);
            if (route.remote_devices.empty()) {
                return Status::DeviceNotFound(
                    "UB target has no active advertised device" LOC_MARK);
            }
            return Status::OK();
        });
}

std::vector<UbWorkers::LocalDeviceCandidate> UbWorkers::orderedLocalDevices(
    const PendingSlice& pending) const {
    std::vector<LocalDeviceCandidate> ordered;
    std::unordered_set<Topology::NicID> seen;
    std::unordered_map<Topology::NicID, size_t> device_rank;
    std::string location = kWildcardLocation;
    auto locations = Platform::getLoader().getLocation(
        pending.slice->spec().local_address, 1, true);
    if (!locations.empty()) location = locations.front().location;

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
                lhs_inflight == inflight_snapshot.end() ? 0 : lhs_inflight->second;
            const uint64_t rhs_inflight_bytes =
                rhs_inflight == inflight_snapshot.end() ? 0 : rhs_inflight->second;
            return lhs_inflight_bytes < rhs_inflight_bytes;
        });
    return ordered;
}

std::vector<Topology::NicID> UbWorkers::orderedRemoteDevices(
    const SegmentDesc& segment, const BufferDesc& buffer) {
    std::vector<Topology::NicID> ordered;
    auto attr = buffer.transport_attrs.find(TransportType::UB);
    if (attr == buffer.transport_attrs.end()) return ordered;
    UbBufferMetadata metadata;
    if (!decodeBufferMetadata(attr->second, metadata).ok()) return ordered;
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
    QuotaReservation& reservation) {
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

    const auto local_devices = orderedLocalDevices(pending);
    if (route.remote_devices.empty()) {
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
            return Status::TooManyRequests(
                "All local UB devices are in recovery cooldown" LOC_MARK);
        }
        return Status::DeviceNotFound(
            "All local UB devices remained unavailable" LOC_MARK);
    }
    const size_t combinations =
        local_devices.size() * route.remote_devices.size();
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
    for (size_t flat = 0; flat < combinations; ++flat) {
        const auto& local = local_devices[flat / route.remote_devices.size()];
        const auto remote_id =
            route.remote_devices[flat % route.remote_devices.size()];
        const UbPostPath probe_path{local.id, pending.target_id, remote_id,
                                    kRailProbeEndpointGeneration};
        // A non-inserting lookup: an unknown rail scores as "no health history
        // yet" without being added to the map, so probing every local x remote
        // combination cannot grow the rail map or seed a generation for a rail
        // that is never posted to.
        auto rail_stats = rail_monitor_->statsIfPresent(probe_path);
        if (rail_stats.paused) continue;
        rails.push_back(RailCandidate{
            local.id, remote_id, local.topology_rank,
            (flat + combinations - start) % combinations, probe_path,
            std::move(rail_stats),
            quota_->availability(probe_path, pending.slice->spec().length)});
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
        // Re-read rail state under the real generation: the rail may only have
        // been created by the resolution above.
        const auto rail_stats = rail_monitor_->stats(candidate_path);
        if (rail_stats.paused) continue;
        const auto capacity =
            quota_->availability(candidate_path, pending.slice->spec().length);
        const uint64_t endpoint_wrs = candidate_endpoint->outstandingWrs();
        const uint64_t endpoint_bytes = candidate_endpoint->outstandingBytes();
        candidates.push_back(
            Candidate{std::move(candidate_endpoint), candidate_path,
                      UbPathSelectionScore{
                          capacity.can_acquire, rail.topology_rank,
                          capacity.normalized_inflight,
                          capacity.normalized_outstanding_wrs, endpoint_wrs,
                          endpoint_bytes,
                          rail_stats.ewma_bandwidth_bytes_per_second >= 0.0,
                          rail_stats.ewma_bandwidth_bytes_per_second,
                          rail.retry_order, rail.local_id, rail.remote_id}});
    }
    if (candidates.empty()) {
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
    return Status::OK();
}

void UbWorkers::processPending(const PendingSlice& pending,
                               size_t worker_index) {
    if (pending.slice->cancellationRequested()) {
        pending.slice->requestCancellation();
        return;
    }
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

    std::shared_ptr<UbEndpoint> endpoint;
    UbPostPath path;
    QuotaReservation reservation;
    status =
        chooseAndResolveEndpoint(pending, route, endpoint, path, reservation);
    if (!status.ok()) {
        if (status.IsTooManyRequests()) {
            deferPending(pending);
            return;
        }
        const auto resolution = pending.slice->resolveBeforePost(
            FAILED, 0, retryableStatus(status));
        if (resolution == UbAttemptResolution::kRetryScheduled) {
            enqueueRetry(pending);
        }
        return;
    }

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
                                    pending.opcode,
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

    if (!endpoint->tryAcquireOutstanding(pending.slice->spec().length)) {
        (void)quota_->release(reservation);
        deferPending(pending);
        return;
    }

    auto attempt = pending.slice->beginAttempt(path);
    if (!attempt) {
        endpoint->releaseOutstanding(pending.slice->spec().length);
        (void)quota_->release(reservation);
        return;
    }

    // rm_pool data path (C06): admission runs before any inflight state is
    // published. A permit/lease miss is capacity pressure - the attempt is
    // deferred without charging retry budget and every owner acquired so far
    // is released. A hard lease error fails the attempt through the normal
    // retryable resolution instead.
    std::shared_ptr<SendJettyPool> pooled_pool =
        endpoint->context() ? endpoint->context()->pooledSendPool() : nullptr;
    TaskPermitTicket pooled_permit;
    LeaseWithCredit pooled_lease;
    uint64_t gate_token = 0;
    if (pooled_pool) {
        auto defer_pooled_attempt = [&] {
            (void)pending.slice->deferAttempt(*attempt);
            endpoint->releaseOutstanding(pending.slice->spec().length);
            (void)quota_->release(reservation);
            deferPending(pending);
        };
        if (task_permits_) {
            pooled_permit = task_permits_->tryAcquire();
            if (!pooled_permit.valid()) {
                defer_pooled_attempt();
                return;
            }
        }
        const JettyOptions profile = endpoint->jettyOptions();
        const PoolKey pool_key{endpoint->context()->incarnation(),
                               profile.trans_mode,
                               static_cast<uint8_t>(profile.priority),
                               profile.bind_tp_type_mask};
        const AttemptKey attempt_key{
            reinterpret_cast<uint64_t>(pending.task.get()), attempt->slice_id,
            attempt->attempt};
        const auto lease_status =
            pooled_pool->tryLease(pool_key, attempt_key, pooled_lease);
        if (lease_status.IsTooManyRequests()) {
            defer_pooled_attempt();
            return;
        }
        if (!lease_status.ok()) {
            endpoint->releaseOutstanding(pending.slice->spec().length);
            (void)quota_->release(reservation);
            const auto resolution = pending.slice->resolveAttempt(
                *attempt, FAILED, 0, retryableStatus(lease_status));
            if (resolution == UbAttemptResolution::kRetryScheduled) {
                enqueueRetry(pending);
            }
            return;
        }
        if (attempt_gate_) {
            gate_token = attempt_gate_->registerAttempt(
                attempt_key, pooled_lease.lease.lane(),
                pending.slice->spec().length);
        }
    }

    const uint64_t completion_token = nextCompletionToken();
    auto inflight = std::make_shared<Inflight>();
    inflight->completion_token = completion_token;
    inflight->pending = pending;
    inflight->attempt = *attempt;
    inflight->path = path;
    inflight->endpoint = endpoint;
    inflight->local_segment = local.segment;
    inflight->remote_segment = remote.segment;
    inflight->quota = reservation;
    inflight->posted_ns = steadyNowNs();
    inflight->deadline_ns = deadlineAfter(
        inflight->posted_ns,
        static_cast<uint64_t>(params_.slice_timeout_ms) * 1'000'000ULL);
    inflight->pooled_pool = pooled_pool;
    inflight->pooled_permit = std::move(pooled_permit);
    inflight->gate_token = gate_token;

    if (!pending.slice->tryCommitPost(*attempt, inflight->posted_ns)) {
        endpoint->releaseOutstanding(pending.slice->spec().length);
        (void)quota_->release(reservation);
        if (pooled_pool) {
            // Cancellation won before the native boundary: no WR crossed, so
            // the member is clean and returns to the pool. The gate record
            // never saw a post and is dropped; the credit is released here
            // because the pool never releases caller credits.
            if (attempt_gate_ && gate_token != 0) {
                attempt_gate_->discard(gate_token);
            }
            if (auto credits = pooled_pool->creditLedger()) {
                credits->release(pooled_lease.jfc_index,
                                 pooled_lease.jfc_credit_token);
            }
            (void)pooled_pool->returnOnce(std::move(pooled_lease.lease), true);
        }
        return;
    }
    if (pooled_pool) {
        inflight->pooled_jfc_index = pooled_lease.jfc_index;
        inflight->jfc_credit_token = pooled_lease.jfc_credit_token;
        inflight->pooled_lease = std::move(pooled_lease.lease);
    }
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        inflight_.emplace(completion_token, inflight);
    }

    WorkRequest work;
    work.operation =
        pending.opcode == Request::READ ? Operation::READ : Operation::WRITE;
    work.local_address =
        reinterpret_cast<uint64_t>(pending.slice->spec().local_address);
    work.remote_address = pending.slice->spec().remote_address;
    work.length = pending.slice->spec().length;
    work.token = completion_token;
    work.local_segment = local.segment;
    work.remote_segment = remote.segment;
    // RM Legacy: one slice attempt is one native WR that must reference the
    // imported target positional with this Jetty; the target is never
    // substituted by a bound remote Jetty.
    const bool rm_endpoint = endpoint->rmMode();
    if (rm_endpoint) work.target = endpoint->rmTarget(worker_index);
    JettyPtr jetty;
    if (inflight->pooled_pool) {
        // rm_pool data path: the WR rides the leased pool member, never an
        // endpoint-owned Jetty. The imported target still comes from the
        // endpoint incarnation that owns the peer relationship.
        const auto& member = inflight->pooled_lease.member();
        jetty = member ? member->jetty : nullptr;
    } else {
        jetty = endpoint->jetty(worker_index);
    }
    size_t posted_count = 0;
    PostAcceptance acceptance = PostAcceptance::kNotAccepted;
    if (!jetty) {
        status = Status::InternalError(
            inflight->pooled_pool ? "Pooled send Jetty is unavailable" LOC_MARK
                                  : "UB endpoint has no Jetty" LOC_MARK);
    } else if (rm_endpoint && !work.target) {
        status = Status::InternalError(
            "RM endpoint has no imported target for this Jetty" LOC_MARK);
    } else {
        status = adapter_->post(jetty, {work}, posted_count, acceptance);
    }
    if (inflight->pooled_pool && attempt_gate_ && inflight->gate_token != 0) {
        attempt_gate_->onPostReturn(inflight->gate_token, status, acceptance);
    }
    if (posted_count == 1) {
        rail_monitor_->registerPath(path);
        if (acceptance == PostAcceptance::kUnknown) {
            // A contradictory native result cannot prove the WR was rejected:
            // never roll back or replay; the CQE or the timeout fence settles
            // the pre-registered owners.
            LOG(WARNING)
                << "UB post acceptance is unknown; retaining WR owners until "
                   "a CQE or fence settles: "
                << status.ToString();
        } else if (!status.ok()) {
            LOG_EVERY_N(WARNING, 1000)
                << "UB post returned an error after accepting the WR: "
                << status.ToString();
        }
        return;
    }
    if (acceptance == PostAcceptance::kUnknown) {
        // Zero-count but contradictory: the WR may still have crossed. Keep
        // the token, buffer, segment and endpoint owners; the timeout fence
        // settles responsibility. Never replay here.
        LOG(WARNING)
            << "UB post acceptance is unknown with no posted WR; retaining "
               "WR owners until a fence settles: "
            << status.ToString();
        rail_monitor_->registerPath(path);
        return;
    }

    if (status.ok()) {
        status = Status::InternalError(
            "URMA adapter accepted an unexpected WR count" LOC_MARK);
    }

    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        auto it = inflight_.find(completion_token);
        if (it != inflight_.end() && it->second == inflight)
            inflight_.erase(it);
    }
    inflight_cv_.notify_all();
    releaseInflight(inflight);
    rail_monitor_->recordError(path);
    if (inflight->pooled_pool) {
        // Definitive zero acceptance: the attempt gate decides whether the
        // member returns to the pool (INV07 rollback) or retires on
        // contradiction; the endpoint incarnation is not at fault.
        settlePooledAttempt(inflight, PooledSettleKind::kZeroAccept, 0,
                            "post rejected: " + status.ToString());
    } else {
        if (endpoint_retirer_) endpoint_retirer_(endpoint);
    }
    const auto resolution = pending.slice->resolveAttempt(
        *attempt, FAILED, 0, retryableStatus(status));
    if (resolution == UbAttemptResolution::kRetryScheduled) {
        enqueueRetry(pending);
    }
}

void UbWorkers::handleLocalDeviceFailure(const UbContextPtr& context) {
    if (!context) return;
    const bool newly_failed = context->markUnavailable();
    if (!newly_failed) return;

    if (!local_device_failure_handler_) {
        LOG(WARNING) << "UB device " << context->deviceInfo().topology_name
                     << " has no endpoint cleanup handler; automatic "
                        "reactivation is disabled";
        return;
    }
    progressLocalDeviceFailure(context);
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

void UbWorkers::handleCompletion(const Completion& completion) {
    std::shared_ptr<Inflight> inflight;
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        auto it = inflight_.find(completion.token);
        if (it == inflight_.end()) return;
        inflight = std::move(it->second);
        inflight_.erase(it);
    }
    inflight_cv_.notify_all();
    releaseInflight(inflight);
    if (inflight->timed_out.load(std::memory_order_acquire)) {
        // A natural or flush completion proves that this particular WR can no
        // longer touch memory. Whichever side wins the timeout/drain race may
        // advance the logical attempt; attempt matching makes the other call
        // an idempotent no-op.
        const uint64_t now = steadyNowNs();
        recordTimeoutOnce(inflight, now);
        resolveInflight(inflight, TIMEOUT, 0, true);
        return;
    }

    const uint64_t now = steadyNowNs();
    const uint64_t latency =
        now >= inflight->posted_ns ? now - inflight->posted_ns : 0;
    switch (completion.category) {
        case CompletionCategory::SUCCESS:
            rail_monitor_->recordSuccess(inflight->path,
                                         inflight->pending.slice->spec().length,
                                         latency, now);
            settlePooledAttempt(inflight, PooledSettleKind::kRealSuccess,
                                inflight->pending.slice->spec().length, "");
            resolveInflight(inflight, COMPLETED,
                            inflight->pending.slice->spec().length, false);
            break;
        case CompletionCategory::TIMEOUT:
            rail_monitor_->recordTimeout(inflight->path, now);
            settlePooledAttempt(inflight, PooledSettleKind::kRealError, 0,
                                "native completion timeout");
            if (!inflight->pooled_pool && endpoint_retirer_) {
                endpoint_retirer_(inflight->endpoint);
            }
            resolveInflight(inflight, TIMEOUT, 0, true);
            break;
        case CompletionCategory::LOCAL_DEVICE_ERROR:
            if (inflight->endpoint && inflight->endpoint->context()) {
                handleLocalDeviceFailure(inflight->endpoint->context());
            }
            rail_monitor_->recordError(inflight->path, now);
            settlePooledAttempt(inflight, PooledSettleKind::kRealError, 0,
                                "local device completion error");
            if (endpoint_retirer_) endpoint_retirer_(inflight->endpoint);
            resolveInflight(inflight, FAILED, 0, true);
            break;
        case CompletionCategory::REMOTE_PATH_ERROR:
        case CompletionCategory::ENDPOINT_ERROR:
            rail_monitor_->recordError(inflight->path, now);
            settlePooledAttempt(inflight, PooledSettleKind::kRealError, 0,
                                completion.category ==
                                        CompletionCategory::ENDPOINT_ERROR
                                    ? "endpoint completion error"
                                    : "remote path completion error");
            if (endpoint_retirer_) endpoint_retirer_(inflight->endpoint);
            resolveInflight(inflight, FAILED, 0, true);
            break;
        case CompletionCategory::MEMORY_ERROR:
        case CompletionCategory::UNKNOWN_ERROR:
            rail_monitor_->recordError(inflight->path, now);
            settlePooledAttempt(inflight, PooledSettleKind::kRealError, 0,
                                completion.category ==
                                        CompletionCategory::MEMORY_ERROR
                                    ? "memory completion error"
                                    : "unknown completion error");
            resolveInflight(inflight, FAILED, 0, false);
            break;
    }
}

void UbWorkers::scanTimeouts() {
    std::lock_guard<std::mutex> drain_lock(endpoint_drain_mutex_);
    if (!timeout_scans_enabled_.load(std::memory_order_acquire)) return;
    const uint64_t now = steadyNowNs();
    std::vector<std::shared_ptr<Inflight>> expired;
    {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        for (const auto& [_, inflight] : inflight_) {
            bool expected = false;
            if (inflight->deadline_ns <= now &&
                inflight->timed_out.compare_exchange_strong(
                    expected, true, std::memory_order_acq_rel)) {
                expired.push_back(inflight);
            }
        }
    }
    for (const auto& inflight : expired) {
        std::vector<Completion> drained;
        const bool pooled = inflight->pooled_pool &&
                            inflight->pooled_lease.valid() &&
                            inflight->pooled_lease.member();
        Status status;
        if (pooled) {
            // The WR rode a pooled member, not an endpoint Jetty: the fence
            // must quiesce the member's Jetty to prove no more DMA.
            status = adapter_->quiesceJetty(
                inflight->pooled_lease.member()->jetty,
                params_.slice_timeout_ms, drained);
        } else {
            status = inflight->endpoint
                         ? inflight->endpoint->quiesce(params_.slice_timeout_ms,
                                                       drained)
                         : Status::InvalidArgument(
                               "Timed-out UB WR has no endpoint" LOC_MARK);
        }
        // Even a partial/failed fence may have returned valid WR completions;
        // they must never be lost.
        for (const auto& completion : drained) {
            if (completion.token != 0) handleCompletion(completion);
        }
        if (!status.ok()) {
            if (!pooled) rememberEndpointDrain(inflight->endpoint);
            LOG_EVERY_N(ERROR, 100)
                << "Cannot establish UB timeout drain fence: "
                << status.ToString();
            // Leave the token and all resource references alive. A natural
            // completion can still resolve it safely; otherwise a later scan
            // retries the native fence.
            bool still_inflight = false;
            {
                std::lock_guard<std::mutex> lock(inflight_mutex_);
                auto it = inflight_.find(inflight->completion_token);
                still_inflight =
                    it != inflight_.end() && it->second == inflight;
            }
            if (still_inflight) {
                inflight->timed_out.store(false, std::memory_order_release);
            }
            continue;
        }
        if (pooled) {
            // The fence proved no more DMA from the member. If a drained
            // completion already settled the attempt this is a no-op;
            // otherwise the unsettled attempt retires the member (INV07).
            settlePooledAttempt(inflight, PooledSettleKind::kFence, 0,
                                "slice timeout fence");
        } else {
            forgetEndpointDrain(inflight->endpoint);
            if (endpoint_retirer_) endpoint_retirer_(inflight->endpoint);
        }
        recordTimeoutOnce(inflight, now);
        // quiesce() is the proof that retry cannot overlap old DMA. Reclaim a
        // provider-lost token here: the successful fence proves the old WR can
        // no longer touch memory, so releasing outstanding/quota accounting is
        // safe. releaseInflight() is idempotent against a racing completion.
        bool erased = false;
        {
            std::lock_guard<std::mutex> lock(inflight_mutex_);
            auto it = inflight_.find(inflight->completion_token);
            if (it != inflight_.end() && it->second == inflight) {
                inflight_.erase(it);
                erased = true;
            }
        }
        if (erased) {
            inflight_cv_.notify_all();
            releaseInflight(inflight);
        }
        resolveInflight(inflight, TIMEOUT, 0, true);
    }
}

void UbWorkers::releaseInflight(const std::shared_ptr<Inflight>& inflight) {
    bool expected = false;
    if (!inflight || !inflight->resources_released.compare_exchange_strong(
                         expected, true, std::memory_order_acq_rel)) {
        return;
    }
    if (inflight->endpoint) {
        inflight->endpoint->releaseOutstanding(
            inflight->pending.slice->spec().length);
    }
    if (inflight->quota.valid()) (void)quota_->release(inflight->quota);
    queue_cv_.notify_all();
}

void UbWorkers::recordTimeoutOnce(const std::shared_ptr<Inflight>& inflight,
                                  uint64_t now_ns) {
    bool expected = false;
    if (inflight && inflight->timeout_recorded.compare_exchange_strong(
                        expected, true, std::memory_order_acq_rel)) {
        rail_monitor_->recordTimeout(inflight->path, now_ns);
    }
}

void UbWorkers::rememberEndpointDrain(
    const std::shared_ptr<UbEndpoint>& endpoint) {
    if (!endpoint) return;
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    draining_endpoints_[endpoint->generation()] = endpoint;
}

void UbWorkers::forgetEndpointDrain(
    const std::shared_ptr<UbEndpoint>& endpoint) {
    if (!endpoint) return;
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    auto it = draining_endpoints_.find(endpoint->generation());
    if (it != draining_endpoints_.end() && it->second == endpoint) {
        draining_endpoints_.erase(it);
    }
}

void UbWorkers::resolveInflight(const std::shared_ptr<Inflight>& inflight,
                                TransferStatusEnum outcome, size_t bytes,
                                bool retryable) {
    const auto resolution = inflight->pending.slice->resolveAttempt(
        inflight->attempt, outcome, bytes, retryable);
    if (resolution == UbAttemptResolution::kRetryScheduled) {
        enqueueRetry(inflight->pending);
    }
}

void UbWorkers::failUnposted(const PendingSlice& pending,
                             TransferStatusEnum outcome) {
    if (pending.slice) (void)pending.slice->tryResolveBeforePost(outcome);
}

void UbWorkers::settlePooledAttempt(const std::shared_ptr<Inflight>& inflight,
                                    PooledSettleKind kind,
                                    uint64_t completed_length,
                                    const std::string& reason) {
    if (!inflight || !inflight->pooled_pool ||
        !inflight->pooled_lease.valid()) {
        return;
    }
    bool expected = false;
    if (!inflight->pooled_settled.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
        return;
    }
    // Hardware retirement fence (C08): a real error completion, including the
    // pinned provider's native status 9 class, takes the member out of
    // hardware service before it is retired. quiesceJetty moves the Jetty to
    // URMA_JETTY_STATE_ERROR (urma_modify_jetty), waits for that ERROR
    // epoch's flush-done marker and drains the remaining WRs with
    // urma_flush_jetty. Only then does the pool reset and delete the member.
    // A failed fence leaves the member in ERROR without a trusted barrier, so
    // the pool's reset is rejected and the handle stays quarantined instead
    // of re-entering circulation.
    if (kind == PooledSettleKind::kRealError) {
        const auto& member = inflight->pooled_lease.member();
        if (member && member->jetty) {
            std::vector<Completion> drained;
            const Status fence = adapter_->quiesceJetty(
                member->jetty, params_.slice_timeout_ms, drained);
            for (const auto& completion : drained) {
                if (completion.token != 0) handleCompletion(completion);
            }
            if (!fence.ok()) {
                LOG_EVERY_N(WARNING, 100)
                    << "UB pooled member could not be fenced before retirement; "
                       "it stays quarantined: "
                    << fence.ToString();
            }
        }
    }
    const auto& pool = inflight->pooled_pool;
    AttemptGate::Settlement settlement;
    bool settled = false;
    if (attempt_gate_ && inflight->gate_token != 0) {
        switch (kind) {
            case PooledSettleKind::kRealSuccess:
                attempt_gate_->onRealCompletion(inflight->gate_token, true,
                                                completed_length);
                break;
            case PooledSettleKind::kRealError:
                attempt_gate_->onRealCompletion(inflight->gate_token, false,
                                                0);
                attempt_gate_->reportFault(inflight->gate_token, reason);
                break;
            case PooledSettleKind::kZeroAccept:
                // onPostReturn already recorded the definitive rejection; a
                // rollback must not add a fault or the member would retire.
                break;
            case PooledSettleKind::kFence:
                attempt_gate_->reportFault(inflight->gate_token, reason);
                break;
        }
        settled = attempt_gate_->tryFinalize(inflight->gate_token,
                                             !pool->stopping(), settlement);
    }
    if (settled &&
        (settlement.return_to_pool || settlement.rollback_return)) {
        (void)pool->returnOnce(std::move(inflight->pooled_lease), true);
    } else {
        const char* fallback = "pooled attempt unsettled";
        switch (kind) {
            case PooledSettleKind::kRealSuccess:
                fallback = "pooled member retired before reuse";
                break;
            case PooledSettleKind::kRealError:
                fallback = "pooled attempt completion error";
                break;
            case PooledSettleKind::kZeroAccept:
                fallback = "pooled rollback rejected by gate";
                break;
            case PooledSettleKind::kFence:
                fallback = "pooled attempt fenced";
                break;
        }
        (void)pool->retireFaulted(std::move(inflight->pooled_lease),
                                  reason.empty() ? std::string(fallback)
                                                 : reason);
    }
    if (attempt_gate_ && inflight->gate_token != 0) {
        attempt_gate_->discard(inflight->gate_token);
    }
    // The pool never releases caller credits; the settlement path owns the
    // exactly-once release.
    if (auto credits = pool->creditLedger()) {
        credits->release(inflight->pooled_jfc_index,
                         inflight->jfc_credit_token);
    }
}

uint64_t UbWorkers::nextCompletionToken() {
    uint64_t token = next_token_.fetch_add(1, std::memory_order_relaxed);
    if (token == 0) token = next_token_.fetch_add(1, std::memory_order_relaxed);
    return token;
}

}  // namespace mooncake::tent::ub
