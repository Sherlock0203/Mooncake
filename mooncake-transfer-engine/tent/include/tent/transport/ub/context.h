// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_CONTEXT_H_
#define TENT_TRANSPORT_UB_CONTEXT_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/target_registry.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

class UbSendJettyPool;
class JettyDrainController;

class UbContext final : public std::enable_shared_from_this<UbContext> {
   public:
    enum class State : uint8_t {
        kUninitialized,
        kActive,
        kFailed,
        kDraining,
        kClosed,
    };

    UbContext(Topology::NicID topology_id, DeviceInfo device,
              std::shared_ptr<UrmaAdapter> adapter);
    ~UbContext();

    UbContext(const UbContext&) = delete;
    UbContext& operator=(const UbContext&) = delete;

    Status initialize(uint32_t jfc_count, const JfcOptions& options);
    Status shutdown();
    // Returns true only for the Active -> Failed transition. Every subsequent
    // poll error advances the failure epoch so stale successes from another
    // JFC can never reactivate the device.
    [[nodiscard]] bool markUnavailable() noexcept;
    // The failure owner calls this only after every endpoint backed by this
    // device has been unpublished and has reached Destroyed. Pending native
    // cleanup remains quarantined and keeps this recovery barrier incomplete.
    void completeFailureCleanup() noexcept;
    // True once the cleanup barrier finished and the context is only waiting
    // for its error-free cooldown. Pollers use it so the native barrier is not
    // re-scheduled on every successful poll during that window.
    [[nodiscard]] bool failureCleanupComplete() const noexcept;
    // Pollers continue probing every JFC. A transiently failed context becomes
    // active only after all JFCs have succeeded in the current failure epoch,
    // the error-free cooldown has elapsed, old WRs have drained, and endpoint
    // unpublication has completed.
    [[nodiscard]] bool recordPollSuccess(size_t jfc_index,
                                         uint64_t cooldown_ns) noexcept;

    [[nodiscard]] Topology::NicID topologyId() const noexcept {
        return topology_id_;
    }
    // Process-unique identity for bootstrap v2 context_incarnation. It is
    // allocated once per constructed context and never reused.
    [[nodiscard]] uint64_t incarnation() const noexcept { return incarnation_; }
    [[nodiscard]] const DeviceInfo& deviceInfo() const noexcept {
        return device_;
    }
    [[nodiscard]] const ContextPtr& handle() const noexcept { return handle_; }
    [[nodiscard]] State state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool active() const noexcept {
        return state() == State::kActive;
    }
    [[nodiscard]] const std::vector<std::shared_ptr<UbJfc>>& jfcs() const {
        return jfcs_;
    }

    // Per-Context send JettyOptions derived from this Context's own trusted
    // LocalSendProfile at install time (C01 / G-03). Heterogeneous devices each
    // keep their own priority/bind mask instead of sharing a last-iteration
    // value. Unset for contexts installed without an explicit profile, so the
    // EndpointStore falls back to its shared base options (RC baseline).
    void setJettyOptions(JettyOptions options);
    [[nodiscard]] std::optional<JettyOptions> jettyOptions() const;

    // Simple send-Jetty reuse (RM Legacy only). Null unless the transport
    // installed a pool for this context; endpoints fall back to per-incarnation
    // createJetty when absent, so RC behavior is unchanged.
    void attachJettyPool(std::shared_ptr<UbSendJettyPool> pool);
    [[nodiscard]] std::shared_ptr<UbSendJettyPool> jettyPool() const {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        return jetty_pool_;
    }

    // Pooled rm_pool data path (C06). Null unless the transport installed a
    // per-attempt pool for this context; the legacy send path never touches
    // it, so RC/RM-legacy behavior is unchanged while the rm_pool gate
    // matrix remains fail-closed.
    void attachPooledSendPool(std::shared_ptr<SendJettyPool> pool);
    [[nodiscard]] std::shared_ptr<SendJettyPool> pooledSendPool() const {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        return pooled_send_pool_;
    }

    // Asynchronous drain chain owner of this context (design §3.1). Null unless
    // the transport installed the rm_pool drain chain; the workers then deliver
    // every token==0 entity marker to it before the legacy accounting.
    void attachDrainController(
        std::shared_ptr<JettyDrainController> controller);
    [[nodiscard]] std::shared_ptr<JettyDrainController> drainController()
        const {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        return drain_controller_;
    }
    [[nodiscard]] std::shared_ptr<UbJfc> jfc(size_t index) const;

    // Imported RM targets owned by this Context (design §5.1). Endpoints,
    // business attempts and probes reference the same native import instead of
    // creating one per reference. Null before initialize() succeeds.
    [[nodiscard]] std::shared_ptr<ImportedTargetRegistry> targetRegistry()
        const {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        return target_registry_;
    }

    void addInflight(uint64_t bytes) noexcept;
    void removeInflight(uint64_t bytes) noexcept;
    [[nodiscard]] uint64_t inflightBytes() const noexcept {
        return inflight_bytes_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t outstandingWrs() const noexcept {
        return outstanding_wrs_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t recoveryCount() const noexcept {
        return recovery_count_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t failureStartedNs() const noexcept {
        return failure_started_ns_.load(std::memory_order_acquire);
    }

   private:
    Status shutdownLocked();
    static uint64_t allocateIncarnation() noexcept;

    const Topology::NicID topology_id_;
    const DeviceInfo device_;
    std::shared_ptr<UrmaAdapter> adapter_;
    const uint64_t incarnation_;
    ContextPtr handle_;
    std::vector<std::shared_ptr<UbJfc>> jfcs_;
    std::optional<JettyOptions> jetty_options_;
    std::shared_ptr<UbSendJettyPool> jetty_pool_;
    std::shared_ptr<SendJettyPool> pooled_send_pool_;
    std::shared_ptr<JettyDrainController> drain_controller_;
    std::shared_ptr<ImportedTargetRegistry> target_registry_;
    mutable std::mutex lifecycle_mutex_;
    std::atomic<State> state_{State::kUninitialized};
    std::atomic<uint64_t> inflight_bytes_{0};
    std::atomic<uint64_t> outstanding_wrs_{0};
    std::atomic<uint64_t> failure_started_ns_{0};
    std::atomic<uint64_t> last_failure_ns_{0};
    std::atomic<uint64_t> recovery_count_{0};
    uint64_t failure_epoch_{0};
    std::vector<uint64_t> jfc_success_epochs_;
    bool failure_cleanup_complete_{false};
};

using UbContextPtr = std::shared_ptr<UbContext>;

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_CONTEXT_H_
