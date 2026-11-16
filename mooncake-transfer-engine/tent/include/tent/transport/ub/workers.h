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
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/segment_manager.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/completion_record.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/entity_event.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/slice.h"
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
    uint64_t endpoint_outstanding_wrs{0};
    uint64_t endpoint_outstanding_bytes{0};
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
    if (lhs.endpoint_outstanding_wrs != rhs.endpoint_outstanding_wrs) {
        return lhs.endpoint_outstanding_wrs < rhs.endpoint_outstanding_wrs;
    }
    if (lhs.endpoint_outstanding_bytes != rhs.endpoint_outstanding_bytes) {
        return lhs.endpoint_outstanding_bytes < rhs.endpoint_outstanding_bytes;
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
              std::vector<ProbeJfcSlot> probe_jfcs = {});
    ~UbWorkers();

    UbWorkers(const UbWorkers&) = delete;
    UbWorkers& operator=(const UbWorkers&) = delete;

    Status start();
    Status stop();
    Status submit(const UbTask::Ptr& task, uint64_t device_mask = ~0ULL);
    Status cancel(const UbTask::Ptr& task);

    [[nodiscard]] bool running() const noexcept {
        return accepting_.load(std::memory_order_acquire);
    }
    [[nodiscard]] size_t queuedCount() const;
    // Number of records still registered (in-flight owner accounting).
    [[nodiscard]] size_t inflightCount() const;
    // Completion gate counters (design §9).
    [[nodiscard]] CompletionStatsSnapshot completionStats() const {
        return gate_ctx_.stats.snapshot();
    }
    [[nodiscard]] const CountingEntityEventSink& entityEvents() const {
        return entity_events_;
    }

   private:
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
    };
    struct Route;
    struct BusinessRecord;
    struct LocalDeviceCandidate {
        Topology::NicID id{-1};
        size_t topology_rank{Topology::DevicePriorityRanks};
    };

    void postingLoop(size_t worker_index);
    void pollingLoop(size_t poller_index);
    bool popPending(PendingSlice& pending);
    void enqueueRetry(const PendingSlice& pending);
    void deferPending(const PendingSlice& pending);
    void processPending(const PendingSlice& pending, size_t worker_index);
    Status buildRoute(const PendingSlice& pending, Route& route);
    Status chooseAndResolveEndpoint(const PendingSlice& pending, Route& route,
                                    std::shared_ptr<UbEndpoint>& endpoint,
                                    UbPostPath& path,
                                    QuotaReservation& reservation);
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
    static WrFacts makeWrFacts(const Completion& completion);
    [[nodiscard]] std::shared_ptr<BusinessRecord> findRecord(
        uint64_t token) const;
    void releaseOwnerResources(BusinessRecord& record);
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
    std::vector<LocalDeviceCandidate> orderedLocalDevices(
        const PendingSlice& pending) const;
    static std::vector<Topology::NicID> orderedRemoteDevices(
        const SegmentDesc& segment, const BufferDesc& buffer);

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

    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::array<std::deque<PendingSlice>, PRIO_LOW + 1> queues_;

    mutable std::mutex records_mutex_;
    mutable std::mutex endpoint_drain_mutex_;
    // Kept under its original name: stop() still waits on it for the registry
    // to drain.
    std::condition_variable inflight_cv_;
    // Keyed by completion token. Renamed from `inflight_` because W5 adds a
    // second, deadline-ordered index next to it.
    std::unordered_map<uint64_t, std::shared_ptr<BusinessRecord>> records_;
    // Endpoints whose ERROR transition has not yet reached its native flush
    // fence remain owned here even if every logical token completes naturally.
    std::unordered_map<uint64_t, std::shared_ptr<UbEndpoint>>
        draining_endpoints_;

    struct BusinessSlot {
        std::shared_ptr<UbJfc> jfc;
        UbContextPtr context;
    };
    struct ProbeSlot {
        JfcPtr jfc;
        UbContextPtr context;
    };
    std::vector<BusinessSlot> business_slots_;
    std::vector<ProbeSlot> probe_slots_;
    ProbeLinkManager* probe_manager_{nullptr};

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
