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
    std::shared_ptr<UbEndpoint> endpoint;
    LocalSegmentPtr local_segment;
    RemoteSegmentPtr remote_segment;
    QuotaReservation quota;
    uint64_t posted_ns{0};
    uint64_t deadline_ns{0};
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
                     std::vector<ProbeJfcSlot> probe_jfcs)
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
      probe_manager_(probe_manager) {
    for (const auto& context : contexts_) {
        if (!context) continue;
        context_by_topology_id_[context->topologyId()] = context;
        for (const auto& jfc : context->jfcs()) {
            if (jfc) business_slots_.push_back(BusinessSlot{jfc, context});
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
    std::vector<std::shared_ptr<BusinessRecord>> still_inflight;
    std::vector<std::shared_ptr<UbEndpoint>> endpoints_to_fence;
    {
        std::lock_guard<std::mutex> lock(records_mutex_);
        still_inflight.reserve(records_.size());
        for (const auto& [_, record] : records_) {
            still_inflight.push_back(record);
            if (record && record->endpoint) {
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
    }
    for (const auto& record : still_inflight) {
        // The bottom-out shares every claim bit with the CQE and timeout exits:
        // if any of them already disposed of the record, this call only
        // supplies the missing settlement, so a double release is impossible
        // (design §6.9).
        gate::onUnposted(record->core, RecordDropReason::kShutdown, *this,
                         gate_ctx_);
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
    std::lock_guard<std::mutex> lock(records_mutex_);
    return records_.size();
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
    // The poller count is unchanged by probe support: probe slots are
    // distributed across the existing poller set (design §3.3 / §11.1).
    const size_t poller_count = std::max<size_t>(
        1, std::min<size_t>(params_.poller_count, business_slots_.size()));
    uint64_t last_timeout_scan = 0;
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
        if (!progressed)
            std::this_thread::sleep_for(std::chrono::microseconds(20));
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
        progressLocalDeviceFailure(slot.context);
        if (slot.context->recordPollSuccess(
                slot.jfc->index(),
                static_cast<uint64_t>(params_.endpoint_cooldown_ms) *
                    1'000'000ULL)) {
            LOG(INFO) << "Recovered UB device "
                      << slot.context->deviceInfo().topology_name
                      << " after endpoint retirement and an error-free JFC "
                         "cooldown";
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
    if (slot.context) progressLocalDeviceFailure(slot.context);
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
        EntityEvidence evidence;
        evidence.token = completion.token;
        evidence.jetty_id = completion.local_jetty_id;
        evidence.native_status = completion.native_status;
        evidence.completed_bytes = completion.completed_bytes;
        entity_events_.onEntityEvent(local_nic, EntityType::kJfc, jfc_index,
                                     EntityEventKind::kUnclassified, evidence);
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
        gate::HandlerRef handler(record->core, *this, gate_ctx_);
        gate::onRealCr(record->core, makeWrFacts(completion), *this, gate_ctx_);
        return true;
    }
    // Step 4: unknown / late / already disposed. Count it, report it, and keep
    // polling: one stray token never affects any other responsibility (IN-8).
    gate_ctx_.stats.unknown_token.fetch_add(1, std::memory_order_relaxed);
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
    // quota; releaseOwnerResources() also wakes the queue condition variable.
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
    std::stable_sort(
        ordered.begin(), ordered.end(), [&](const auto& lhs, const auto& rhs) {
            if (lhs.topology_rank != rhs.topology_rank)
                return lhs.topology_rank < rhs.topology_rank;
            return context_by_topology_id_.at(lhs.id)->inflightBytes() <
                   context_by_topology_id_.at(rhs.id)->inflightBytes();
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

    // t0 publish: the token enters the registry and this call stack owns one
    // handler reference until the native post returns (design §2.2/§6.5).
    const uint64_t completion_token = nextCompletionToken();
    auto record = std::make_shared<BusinessRecord>();
    record->core.token = completion_token;
    record->core.kind = RecordKind::kBusiness;
    record->core.attempt_id = attempt->slice_id;
    record->core.attempt_serial = attempt->attempt;
    record->core.path = makeBusinessRecordPath(path);
    record->core.jetty_generation = path.endpoint_generation;
    record->core.owner = record.get();
    record->pending = pending;
    record->attempt = *attempt;
    record->path = path;
    record->endpoint = endpoint;
    record->local_segment = local.segment;
    record->remote_segment = remote.segment;
    record->quota = reservation;
    record->posted_ns = steadyNowNs();
    record->deadline_ns = deadlineAfter(
        record->posted_ns,
        static_cast<uint64_t>(params_.slice_timeout_ms) * 1'000'000ULL);

    if (!pending.slice->tryCommitPost(*attempt, record->posted_ns)) {
        endpoint->releaseOutstanding(pending.slice->spec().length);
        (void)quota_->release(reservation);
        return;
    }
    // §5.1 status bit, not a gate: it records that this side took the attempt's
    // single post right, so a future local double post stays detectable. The
    // authority for "may we post at all" belongs to the pooling-side
    // tryClaimPost (W3), which is why nothing reads it back in W2. Published to
    // the registry under the lock below.
    record->core.post_claimed.store(true, std::memory_order_release);

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
    {
        // The post-stack reference spans the blocking native call, and it is
        // taken *before* the record becomes visible to the timeout scan: a
        // published record with zero references would let a scan fence a post
        // that has not even started (D-7). The guard releases the reference on
        // every exit path; when it is the last one the gate advances with both
        // facts available.
        gate::HandlerRef post_handler(record->core, *this, gate_ctx_);
        // Publish before the native post: a completion may legally arrive
        // before post() returns. Only the completion claimant ever erases
        // (D-3), so a CQE racing with this insert still finds its record.
        {
            std::lock_guard<std::mutex> lock(records_mutex_);
            records_.emplace(completion_token, record);
        }
        gate_ctx_.stats.record_published.fetch_add(1,
                                                   std::memory_order_relaxed);
        auto jetty = endpoint->jetty(worker_index);
        size_t posted_count = 0;
        status =
            jetty ? adapter_->post(jetty, {work}, posted_count)
                  : Status::InternalError("UB endpoint has no Jetty" LOC_MARK);
        if (posted_count != 1 && status.ok()) {
            // The adapter claimed a count it cannot have for one WR.
            status = Status::InternalError(
                "URMA adapter accepted an unexpected WR count" LOC_MARK);
        }
        const PostAcceptance acceptance =
            legacyAcceptance(status, posted_count);
        if (acceptance == PostAcceptance::kUnknown) {
            // The adapter claims more WRs than were submitted, so this side
            // cannot prove what is in flight. Nothing is released on a guess:
            // the record drains until the timeout fence proves the old DMA
            // stopped (upstream §1.2.4), and every blocked pass is counted as
            // gate_blocked_by_acceptance.
            LOG_EVERY_N(WARNING, 100)
                << "UB adapter reported an impossible posted_count "
                << posted_count << " for one WR; token " << completion_token
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
        gate::onPostReturn(record->core,
                           PostReturn{acceptance,
                                      acceptance == PostAcceptance::kAccepted
                                          ? pending.slice->spec().length
                                          : 0,
                                      retryableStatus(status)},
                           gate_ctx_);
        // A rejected post needs no extra work here: the reject disposition
        // performs the legacy rollback exactly once when the guard above
        // releases the last reference.
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
    if (kind == FaultKind::kTimeout) {
        // Deadline evidence without a placeable completion. A late completion
        // resolves the same one-shot, so the rail sees exactly one timeout.
        rail_monitor_->recordTimeout(business->path, now);
        return;
    }
    switch (record.terminal.outcome) {
        case CompletionOutcome::kLocalDeviceError:
            if (business->endpoint && business->endpoint->context()) {
                handleLocalDeviceFailure(business->endpoint->context());
            }
            if (probe_manager_ != nullptr) {
                // kContextFailure disables every path on the port; the PLM fans
                // it out to onContextUnavailable().
                probe_manager_->onBusinessPathError(
                    business->path, BusinessErrorKind::kContextFailure, now);
            }
            rail_monitor_->recordError(business->path, now);
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            return;
        case CompletionOutcome::kRemotePathError:
            if (probe_manager_ != nullptr) {
                probe_manager_->onBusinessPathError(
                    business->path, BusinessErrorKind::kPathFault, now);
            }
            rail_monitor_->recordError(business->path, now);
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            return;
        case CompletionOutcome::kEndpointError:
            if (probe_manager_ != nullptr) {
                probe_manager_->onBusinessPathError(
                    business->path, BusinessErrorKind::kLaneError, now);
            }
            rail_monitor_->recordError(business->path, now);
            if (endpoint_retirer_) endpoint_retirer_(business->endpoint);
            return;
        case CompletionOutcome::kMemoryError:
            if (probe_manager_ != nullptr) {
                probe_manager_->onBusinessPathError(
                    business->path, BusinessErrorKind::kParamError, now);
            }
            rail_monitor_->recordError(business->path, now);
            return;
        case CompletionOutcome::kUnknownError:
            if (probe_manager_ != nullptr) {
                probe_manager_->onBusinessPathError(
                    business->path, BusinessErrorKind::kUnknown, now);
            }
            rail_monitor_->recordError(business->path, now);
            return;
        case CompletionOutcome::kTimeout:
            // Adapter-level TIMEOUT category (not the deadline scan).
            if (probe_manager_ != nullptr) {
                probe_manager_->onBusinessPathError(
                    business->path, BusinessErrorKind::kPathFault, now);
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
            releaseOwnerResources(*business);
            recordSuccessEvidence(*business);
            break;
        case OwnerDisposition::kFinalize:
            releaseOwnerResources(*business);
            break;
        case OwnerDisposition::kReject:
            // The legacy not-accepted rollback: free the accounting, bill the
            // rail, retire the endpoint, then deliver FAILED/retryable.
            releaseOwnerResources(*business);
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
    // W3 hangs the defer/backpressure wake-up here, W4 the classified logging.
    (void)record;
    (void)reason;
}

void UbWorkers::onDuplicate(CompletionRecord& record) {
    LOG_EVERY_N(WARNING, 1000)
        << "UB duplicate completion ignored: token=" << record.token
        << " path=" << recordPathText(record.path)
        << " generation=" << record.jetty_generation;
}

void UbWorkers::scanTimeouts() {
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
        std::vector<Completion> drained;
        auto status =
            record->endpoint
                ? record->endpoint->quiesce(params_.slice_timeout_ms, drained)
                : Status::InvalidArgument(
                      "Timed-out UB WR has no endpoint" LOC_MARK);
        // Even a partial/failed fence may have returned valid WR completions;
        // they must never be lost and must be settled by the same dispatcher.
        for (const auto& completion : drained) {
            dispatchCompletion(completion, record->core.path.local_nic);
        }
        if (!status.ok()) {
            rememberEndpointDrain(record->endpoint);
            LOG_EVERY_N(ERROR, 100)
                << "Cannot establish UB timeout drain fence: "
                << status.ToString();
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
            continue;
        }
        forgetEndpointDrain(record->endpoint);
        if (endpoint_retirer_) endpoint_retirer_(record->endpoint);
        // The erase and the "is this record still mine?" check happen in one
        // critical section: if a drained completion already claimed and
        // disposed of the record, the scan must not release the owners again
        // (IN-4).
        eraseRecord(record->core.token, record.get());
        // quiesce() is the proof that retry cannot overlap old DMA. The fence
        // evidence then settles TIMEOUT and finalizes once (§6.7).
        gate::onFenceProven(record->core, *this, gate_ctx_);
    }
}

std::shared_ptr<UbWorkers::BusinessRecord> UbWorkers::findRecord(
    uint64_t token) const {
    // The registry lock is only held to take a shared_ptr; every consumer works
    // on the reference outside the lock (design §5.6 rule 2).
    std::lock_guard<std::mutex> lock(records_mutex_);
    auto it = records_.find(token);
    return it == records_.end() ? nullptr : it->second;
}

void UbWorkers::releaseOwnerResources(BusinessRecord& record) {
    // Exactly one call per record: the gate's claim bit already guaranteed it.
    // §6.8 steps 1-2, in this order: the endpoint accounting first (W3 replaces
    // this with pool->release(lease)), then the quota, both before the logical
    // result is delivered.
    if (record.endpoint) {
        record.endpoint->releaseOutstanding(
            record.pending.slice->spec().length);
    }
    if (record.quota.valid()) (void)quota_->release(record.quota);
    // A posting lane may be backing off on capacity; wake it (§5.4).
    queue_cv_.notify_all();
}

void UbWorkers::eraseRecord(uint64_t token, const BusinessRecord* record) {
    bool erased = false;
    {
        std::lock_guard<std::mutex> lock(records_mutex_);
        auto it = records_.find(token);
        if (it != records_.end() && it->second.get() == record) {
            records_.erase(it);
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
    if (probe_manager_ != nullptr) {
        probe_manager_->onBusinessPathSuccess(record.path, now);
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
