// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_TRANSPORT_UB_PARAMS_H_
#define TENT_TRANSPORT_UB_PARAMS_H_

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/transport/ub/bootstrap.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/probe_types.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

// Runtime selection is explicit and fail-closed. RC legacy is the default.
// RM legacy must be requested explicitly (runtime_profile=rm_legacy,
// trans_mode=rm, physical_only=true) and passes the config gate only when
// every structural constraint holds; provider/capability gates still run at
// install time before any native resource is created. Pooled RM
// (runtime_profile=rm_pool) additionally requires jetty_pool.enable and
// probe.enabled, stays fail-closed against jetty_pool.force_disable, and
// runs its capability/budget gates during install.
enum class UbRuntimeProfile : uint8_t {
    kRcLegacy,
    kRmLegacy,
    kRmPool,
};

enum class UbTransportMode : uint8_t {
    kRc,
    kRm,
};

// Pooled send-path switches. `rm_pool` additionally requires jetty_pool/enable
// and the RM transport mode (M-3); force_disable is the runtime brake and wins
// over every other key.
struct UbJettyPoolParams {
    bool enable = false;
    bool force_disable = false;
    // Static Serving target per PoolKey (IDLE + IN_USE). The first pooled
    // version is static, so initial/min/max are accepted for the forward
    // compatible schema but never trigger adaptive resizing.
    uint32_t business_desired_serving = 6;
    uint32_t business_initial_size = 6;
    uint32_t business_min_size = 6;
    uint32_t business_max_size = 6;
    uint32_t refill_interval_ms = 50;
    // Upper bound for the hardware retirement fence. In asynchronous mode it is
    // the total budget from beginDrain() to DELETE_READY.
    uint32_t retire_timeout_ms = 1000;
    // Asynchronous drain chain (design §10.1). true is the design default;
    // false is a bisection lever that restores the synchronous quiesce on the
    // error path, never a production fallback.
    bool async_drain = true;
    // Bound on flushJettyOnce() rounds per control tick.
    uint32_t drain_flush_batch = 64;
    // Consecutive failed drains after which the provider is declared unable to
    // drain and new faults are quarantined (design §9).
    uint32_t drain_quarantine_threshold = 3;
    // Creation-backoff cap (design §7.3 BACKOFF). Distinct from
    // endpoint_cooldown_ms, which throttles endpoint re-resolution.
    uint32_t cooldown_ms = 1000;
};

// Simple send-Jetty reuse for RM Legacy: endpoints lease their Jetty set
// from a per-context idle pool and retired incarnations return drained
// Jettys to it (LIFO), avoiding createJetty/deleteJetty churn. This is a
// lightweight reuse switch, distinct from the full rm_pool profile: it never
// shares a Jetty across concurrent endpoint incarnations and leaves the
// imported-target ownership model unchanged.
struct UbJettyReuseParams {
    bool enable = false;
};

// Pool claim review (design §5.2 rule 3 / §5.6). When disabled, the pooled
// post path falls back to the pre-claim "pool->stopping() only" recheck and
// counts `claim_disabled`, which is the documented bisection lever. It is not
// a way to skip the verdict silently: the counter makes a left-off switch
// visible in the periodic summary.
struct UbClaimParams {
    bool enabled = true;
};

// Pooled send-path parameters (C01). Every knob is an explicit value and is
// never derived from jetty_per_endpoint. The business serving target and the
// refill interval now live in UbJettyPoolParams, because their config keys are
// normalized to transports/ub/jetty_pool/* (design §9.1).
struct UbSendPoolParams {
    // Bound on concurrent Jetty creations per pool key so refill cannot
    // storm the provider.
    uint32_t max_creating_per_key = 2;
    // Per-JFC WR credits reserved for control records (flush-done markers,
    // AE); business WRs only use the remaining capacity.
    uint32_t jfc_credit_reserve = 16;
    // Background executors (P2/P3). Refill keeps every known PoolKey at the
    // desired Serving size while idle and replaces retired members without
    // waiting for the next attempt; delete performs reset+delete and budget
    // release off the completion path.
    bool background_refill = true;
    bool background_delete = true;
};

// Device-level native budget parameters (C01/C05).
struct UbNativeBudgetParams {
    // Configured Jetty budget per physical device. 0 derives the budget
    // from the verified device limit at attach time; an explicit value must
    // be positive and must not exceed the verified limit.
    uint32_t max_jetty_per_device = 0;
    // Non-negative reserve allowing bounded replacement creation while
    // retired/quarantined predecessors still hold their budget.
    uint32_t replacement_reserve = 2;
};

// The mode a caller may actually enter after gate evaluation.
enum class UbRuntimeMode : uint8_t {
    kRcLegacy,
    kRmLegacy,
    kPooledRm,
};

// Stable spelling for logs and diagnostics. Never derive a runtime decision
// from this string; it exists so human-facing summaries do not change shape.
inline const char* ubRuntimeModeName(UbRuntimeMode mode) noexcept {
    switch (mode) {
        case UbRuntimeMode::kRcLegacy:
            return "rc_legacy";
        case UbRuntimeMode::kRmLegacy:
            return "rm_legacy";
        case UbRuntimeMode::kPooledRm:
            return "rm_pool";
    }
    return "unknown";
}

inline bool parseUbRuntimeProfile(std::string_view value,
                                  UbRuntimeProfile& profile) {
    if (value == "rc_legacy" || value == "RC_LEGACY") {
        profile = UbRuntimeProfile::kRcLegacy;
    } else if (value == "rm_legacy" || value == "RM_LEGACY") {
        profile = UbRuntimeProfile::kRmLegacy;
    } else if (value == "rm_pool" || value == "RM_POOL") {
        profile = UbRuntimeProfile::kRmPool;
    } else {
        return false;
    }
    return true;
}

inline bool parseUbTransportMode(std::string_view value,
                                 UbTransportMode& mode) {
    if (value == "rc" || value == "RC") {
        mode = UbTransportMode::kRc;
    } else if (value == "rm" || value == "RM") {
        mode = UbTransportMode::kRm;
    } else {
        return false;
    }
    return true;
}

inline const char* ubRuntimeProfileName(UbRuntimeProfile profile) noexcept {
    switch (profile) {
        case UbRuntimeProfile::kRcLegacy:
            return "rc_legacy";
        case UbRuntimeProfile::kRmLegacy:
            return "rm_legacy";
        case UbRuntimeProfile::kRmPool:
            return "rm_pool";
    }
    return "unknown";
}

inline const char* ubTransportModeName(UbTransportMode mode) noexcept {
    return mode == UbTransportMode::kRm ? "rm" : "rc";
}

// Event-level observability (closed-loop verification guide). All switches
// only gate logging; they never change a verdict or a state machine.
struct UbObservabilityParams {
    bool verbose_events = false;
    // 0 keeps the component default (lease/return 1024, probe/health 100).
    uint32_t sample_every = 0;
    // 0 means no limit on the install-time declared-topology dump.
    uint32_t topology_dump_limit = 0;

    [[nodiscard]] uint32_t leaseSampleEvery() const {
        return sample_every == 0 ? 1024 : sample_every;
    }
    [[nodiscard]] uint32_t probeSampleEvery() const {
        return sample_every == 0 ? 100 : sample_every;
    }
};

struct UbParams {
    // UB is opt-in until a real URMA provider and NIC topology are present.
    bool enable = false;
    std::vector<std::string> device_filter;
    // Legacy selection remains the default. physical_only requires explicit
    // trusted identities and never falls back to name-based classification.
    bool physical_only = false;
    std::vector<DeviceIdentity> device_identities;
    // yuanhao-1113 carried bonding_mode/bonding_level here. This deployment
    // does not use bonding devices: the physical-device requirement above is
    // the only selection policy, and the adapter keeps its own hardcoded
    // STANDALONE/IODIE bonding control behind an isBondingDevice() guard.

    uint32_t worker_count = 6;
    uint32_t poller_count = 2;
    uint32_t jfc_per_context = 6;
    uint32_t jetty_per_endpoint = 6;
    uint32_t max_endpoints = 65536;
    size_t slice_size = 64 * 1024;
    size_t max_slices_per_task = 64 * 1024;
    uint32_t max_retries = 8;
    uint32_t slice_timeout_ms = 5000;
    uint32_t endpoint_cooldown_ms = 1000;
    // After the most recent post, JFC pollers keep polling instead of
    // sleeping, and one posting worker spins on the queue instead of waiting
    // on the condition variable, for this long. Sparse foreground transfers
    // then skip a scheduler wakeup on both the post and the completion side.
    // 0 restores the sleep/wait-only behaviour.
    uint32_t poll_idle_spin_us = 1000;
    uint32_t post_idle_spin_us = 1000;

    // Defaults are the verified RC legacy baseline; pool and probe stay off.
    UbRuntimeProfile runtime_profile = UbRuntimeProfile::kRcLegacy;
    UbTransportMode trans_mode = UbTransportMode::kRc;
    // Explicit bootstrap protocol selector (design §6). kAuto derives the
    // version from the runtime mode; v1/v2 force one version and reject the
    // other instead of silently switching.
    UbBootstrapProtocol bootstrap_protocol = UbBootstrapProtocol::kAuto;
    UbJettyPoolParams jetty_pool;
    UbClaimParams claim;
    UbJettyReuseParams jetty_reuse;
    // Pooled data-path knobs; dormant until the rm_pool gate matrix passes.
    UbSendPoolParams send_pool;
    UbNativeBudgetParams native_budget;
    // Node-level cap on receive-side target native Jetties (guidance 4.2.1):
    // total across all senders, Contexts, devices/EIDs, Engines and resource
    // generations of one node. Default 8 when absent; 0 is never unlimited.
    // Frozen at node install for rm_pool; RC/RM legacy semantics are
    // unchanged by this key.
    uint32_t max_target_jetties_per_node = 8;

    bool enable_bandwidth_estimation = true;
    bool enable_notifications = false;

    // ---- W3 business queue / defer / K (design §12) ----
    // Capacity of the bounded business queue in slices: queued + parked.
    // submit() is all-or-nothing per Task, so this must admit one maximal Task.
    uint64_t queue_budget = 65536;
    // Bound on quarantined business members. Reaching it stops admission
    // instead of dropping responsibility (design §11.4).
    uint32_t quarantine_budget = 64;
    // Bound on unerased tombstones. Reaching it backpressures refill and tags
    // every new retirement; entries are never TTL-evicted (design §7.2).
    uint32_t tombstone_budget = 1024;
    // K: the maximum inflight attempts of one UbTask across all contexts
    // (D-6). Named differently from send_pool/task_max_inflight_attempts on
    // purpose: that key configures the unused process-wide ledger.
    uint32_t task_max_inflight_attempts = 4;
    // Bound on how many parked slices one reflow pass moves back to the queue.
    uint32_t defer_reflow_batch = 256;
    // F9.1: while the ready queues stay non-empty a parked slice would never be
    // reflowed (kReflow is only reachable on an empty pop). Force a reflow at
    // least this often. 0 disables the cadence (legacy behavior).
    uint32_t defer_reflow_max_gap_ms = 10;
    // A local device may only pair with a remote device in the same EID
    // reachability scope (df00 <-> df00, df04 <-> df04). Cross-scope WRs come
    // back as URMA_CR_LOC_ACCESS_ERR, which surfaces as a retryable path fault
    // and collapses the transfer onto a single rail. Declared adjacency has
    // its own probe/adjacency_policy/require_same_eid_scope; this switch
    // covers the legacy local x remote combination path.
    bool require_same_eid_scope = true;
    // F6d: allow replaying a timed-out WRITE. Fixed-length byte moves are
    // idempotent while the source is not concurrently rewritten, which is an
    // existing assumption of the whole design; this switch lets a deployment
    // that needs strict write-once semantics forbid it. READ replay stays
    // allowed either way.
    bool retry_timeout_write = true;
    // F6c: honor Request::deadline_ns (absolute steady_clock, 0 = none) at
    // every stage. UB does no predictive cancellation (that is the admission
    // queue's job); it only terminates requests whose deadline already passed.
    // false restores byte-for-byte legacy behavior for A/B runs.
    bool honor_request_deadline = true;

    // Probe-link parameters, read from transports/ub/probe/* below.
    ProbeParams probe;
    UbObservabilityParams observability;

    // Reads every UB setting from transports/ub/*. Numeric settings are
    // deliberately read as signed JSON integers first so a negative value is
    // rejected rather than silently converted to an unsigned default.
    static Status FromConfig(const Config& config, UbParams& output) {
        UbParams parsed;

        CHECK_STATUS(readBool(config, "transports/ub/enable", parsed.enable,
                              parsed.enable));
        CHECK_STATUS(readDeviceFilter(config, parsed.device_filter));
        CHECK_STATUS(readBool(config, "transports/ub/physical_only",
                              parsed.physical_only, parsed.physical_only));
        CHECK_STATUS(readDeviceIdentities(config, parsed.device_identities));
        if (parsed.physical_only && parsed.device_identities.empty()) {
            return Status::InvalidArgument(
                "transports/ub/physical_only requires device_identities");
        }
        CHECK_STATUS(readTransportMode(config, parsed.trans_mode));
        CHECK_STATUS(readPositive(config, "transports/ub/worker_count",
                                  parsed.worker_count, parsed.worker_count));
        CHECK_STATUS(readPositive(config, "transports/ub/poller_count",
                                  parsed.poller_count, parsed.poller_count));
        CHECK_STATUS(readPositive(config, "transports/ub/jfc_per_context",
                                  parsed.jfc_per_context,
                                  parsed.jfc_per_context));
        CHECK_STATUS(readPositive(config, "transports/ub/jetty_per_endpoint",
                                  parsed.jetty_per_endpoint,
                                  parsed.jetty_per_endpoint));
        CHECK_STATUS(readPositive(config, "transports/ub/max_endpoints",
                                  parsed.max_endpoints, parsed.max_endpoints));

        uint64_t slice_size = parsed.slice_size;
        CHECK_STATUS(readPositive(config, "transports/ub/slice_size",
                                  slice_size, slice_size));
        if (slice_size > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument(
                "transports/ub/slice_size exceeds the URMA SGE length limit");
        }
        parsed.slice_size = static_cast<size_t>(slice_size);

        uint64_t max_slices_per_task = parsed.max_slices_per_task;
        CHECK_STATUS(readPositive(config, "transports/ub/max_slices_per_task",
                                  max_slices_per_task, max_slices_per_task));
        if (max_slices_per_task > std::numeric_limits<size_t>::max()) {
            return Status::InvalidArgument(
                "transports/ub/max_slices_per_task exceeds size_t");
        }
        parsed.max_slices_per_task = static_cast<size_t>(max_slices_per_task);

        CHECK_STATUS(readNonNegative(config, "transports/ub/max_retries",
                                     parsed.max_retries, parsed.max_retries));
        CHECK_STATUS(readPositive(config, "transports/ub/slice_timeout_ms",
                                  parsed.slice_timeout_ms,
                                  parsed.slice_timeout_ms));
        CHECK_STATUS(readPositive(config, "transports/ub/endpoint_cooldown_ms",
                                  parsed.endpoint_cooldown_ms,
                                  parsed.endpoint_cooldown_ms));
        CHECK_STATUS(readNonNegative(config, "transports/ub/poll_idle_spin_us",
                                     parsed.poll_idle_spin_us,
                                     parsed.poll_idle_spin_us));
        CHECK_STATUS(readNonNegative(config, "transports/ub/post_idle_spin_us",
                                     parsed.post_idle_spin_us,
                                     parsed.post_idle_spin_us));
        if (parsed.poll_idle_spin_us > kMaxIdleSpinUs ||
            parsed.post_idle_spin_us > kMaxIdleSpinUs) {
            return Status::InvalidArgument(
                "transports/ub/*_idle_spin_us must not exceed 1000000");
        }
        CHECK_STATUS(readBool(config,
                              "transports/ub/enable_bandwidth_estimation",
                              parsed.enable_bandwidth_estimation,
                              parsed.enable_bandwidth_estimation));
        CHECK_STATUS(readBool(config, "transports/ub/enable_notifications",
                              parsed.enable_notifications,
                              parsed.enable_notifications));
        CHECK_STATUS(readRuntimeProfile(config, parsed.runtime_profile));
        CHECK_STATUS(readTransportMode(config, parsed.trans_mode));
        CHECK_STATUS(readBool(config, "transports/ub/jetty_pool/enable",
                              parsed.jetty_pool.enable,
                              parsed.jetty_pool.enable));
        CHECK_STATUS(readBool(config, "transports/ub/jetty_pool/force_disable",
                              parsed.jetty_pool.force_disable,
                              parsed.jetty_pool.force_disable));
        CHECK_STATUS(readBool(config, "transports/ub/jetty_reuse/enable",
                              parsed.jetty_reuse.enable,
                              parsed.jetty_reuse.enable));
        // Explicit bootstrap protocol selector. Unset keeps the historical
        // mode-derived behavior.
        std::string bootstrap_protocol;
        CHECK_STATUS(readStringEnum(config, "transports/ub/bootstrap/protocol",
                                    "auto", {"auto", "v1", "v2"},
                                    bootstrap_protocol));
        parsed.bootstrap_protocol =
            bootstrap_protocol == "v1"
                ? UbBootstrapProtocol::kV1
                : (bootstrap_protocol == "v2" ? UbBootstrapProtocol::kV2
                                              : UbBootstrapProtocol::kAuto);

        // ---- Pooled business pool (design §9.1) ---------------------------
        // Keys are normalized to jetty_pool/*. Legacy names stay readable with
        // a deprecation warning, and a conflicting old/new pair is rejected
        // instead of silently picking one (no silent compatibility).
        uint32_t desired_serving = parsed.jetty_pool.business_desired_serving;
        CHECK_STATUS(readRenamedPositive(
            config, "transports/ub/jetty_pool/business_desired_serving",
            "transports/ub/send_pool/desired_per_context", desired_serving));
        parsed.jetty_pool.business_desired_serving = desired_serving;

        uint32_t initial_size = desired_serving;
        CHECK_STATUS(readPositive(
            config, "transports/ub/jetty_pool/business_initial_size",
            initial_size, initial_size));
        parsed.jetty_pool.business_initial_size = initial_size;

        // business_min_size is the compatibility mapping of the legacy
        // jetty_per_endpoint. When both are present they must agree; a
        // conflicting pair is an operator error, not a preference.
        uint32_t min_size = desired_serving;
        const json min_json = config.get<json>(
            "transports/ub/jetty_pool/business_min_size", json());
        const json legacy_endpoints_json =
            config.get<json>("transports/ub/jetty_per_endpoint", json());
        if (!min_json.is_null()) {
            CHECK_STATUS(readPositive(
                config, "transports/ub/jetty_pool/business_min_size", min_size,
                min_size));
            if (!legacy_endpoints_json.is_null() &&
                legacy_endpoints_json.is_number_integer() &&
                legacy_endpoints_json.get<int64_t>() >= 0 &&
                static_cast<uint64_t>(legacy_endpoints_json.get<int64_t>()) !=
                    static_cast<uint64_t>(min_size)) {
                return Status::InvalidArgument(
                    "transports/ub/jetty_pool/business_min_size conflicts with "
                    "the legacy transports/ub/jetty_per_endpoint");
            }
        } else if (!legacy_endpoints_json.is_null()) {
            // jetty_per_endpoint is only a compatibility input: the first
            // pooled version never uses it to size a lease set.
            min_size = parsed.jetty_per_endpoint;
        }
        parsed.jetty_pool.business_min_size = min_size;

        uint32_t max_size = desired_serving;
        CHECK_STATUS(readPositive(config,
                                  "transports/ub/jetty_pool/business_max_size",
                                  max_size, max_size));
        parsed.jetty_pool.business_max_size = max_size;

        CHECK_STATUS(readPositive(config,
                                  "transports/ub/jetty_pool/retire_timeout_ms",
                                  parsed.jetty_pool.retire_timeout_ms,
                                  parsed.jetty_pool.retire_timeout_ms));
        CHECK_STATUS(readBool(config, "transports/ub/jetty_pool/async_drain",
                              parsed.jetty_pool.async_drain,
                              parsed.jetty_pool.async_drain));
        CHECK_STATUS(readPositive(config,
                                  "transports/ub/jetty_pool/drain_flush_batch",
                                  parsed.jetty_pool.drain_flush_batch,
                                  parsed.jetty_pool.drain_flush_batch));
        CHECK_STATUS(readPositive(
            config, "transports/ub/jetty_pool/drain_quarantine_threshold",
            parsed.jetty_pool.drain_quarantine_threshold,
            parsed.jetty_pool.drain_quarantine_threshold));
        CHECK_STATUS(readPositive(
            config, "transports/ub/jetty_pool/cooldown_ms",
            parsed.jetty_pool.cooldown_ms, parsed.jetty_pool.cooldown_ms));
        CHECK_STATUS(readRenamedPositive(
            config, "transports/ub/jetty_pool/refill_interval_ms",
            "transports/ub/send_pool/refill_interval_ms",
            parsed.jetty_pool.refill_interval_ms));

        // Non-renamed pooled knobs: explicit positive integers, never derived
        // from jetty_per_endpoint (C01).
        CHECK_STATUS(
            readPositive(config, "transports/ub/send_pool/max_creating_per_key",
                         parsed.send_pool.max_creating_per_key,
                         parsed.send_pool.max_creating_per_key));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/send_pool/jfc_credit_reserve",
            parsed.send_pool.jfc_credit_reserve,
            parsed.send_pool.jfc_credit_reserve));
        CHECK_STATUS(readBool(config,
                              "transports/ub/send_pool/background_refill",
                              parsed.send_pool.background_refill,
                              parsed.send_pool.background_refill));
        CHECK_STATUS(readBool(config,
                              "transports/ub/send_pool/background_delete",
                              parsed.send_pool.background_delete,
                              parsed.send_pool.background_delete));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/native_budget/max_jetty_per_device",
            parsed.native_budget.max_jetty_per_device,
            parsed.native_budget.max_jetty_per_device));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/native_budget/replacement_reserve",
            parsed.native_budget.replacement_reserve,
            parsed.native_budget.replacement_reserve));
        CHECK_STATUS(readNodeTargetQuotaLimit(
            config, parsed.max_target_jetties_per_node));

        // Pooled RM runtime selection (W3, §12). Every key is optional and the
        // defaults reproduce the RC legacy baseline byte for byte.
        std::string runtime_profile;
        CHECK_STATUS(readStringEnum(
            config, "transports/ub/runtime_profile", "rc_legacy",
            {"rc_legacy", "rm_legacy", "rm_pool"}, runtime_profile));
        if (!parseUbRuntimeProfile(runtime_profile, parsed.runtime_profile)) {
            return Status::InvalidArgument(
                "transports/ub/runtime_profile must be rc_legacy, rm_legacy or "
                "rm_pool");
        }
        std::string trans_mode;
        CHECK_STATUS(readStringEnum(config, "transports/ub/trans_mode", "rc",
                                    {"rc", "rm"}, trans_mode));
        if (!parseUbTransportMode(trans_mode, parsed.trans_mode)) {
            return Status::InvalidArgument(
                "transports/ub/trans_mode must be rc or rm");
        }
        CHECK_STATUS(readBool(config, "transports/ub/jetty_pool/enable",
                              parsed.jetty_pool.enable,
                              parsed.jetty_pool.enable));
        CHECK_STATUS(readBool(config, "transports/ub/jetty_pool/force_disable",
                              parsed.jetty_pool.force_disable,
                              parsed.jetty_pool.force_disable));
        CHECK_STATUS(readBool(config, "transports/ub/jetty_pool/claim/enabled",
                              parsed.claim.enabled, parsed.claim.enabled));
        // Queue and recovery budgets (design §9.1/§11.4).
        CHECK_STATUS(readRenamedPositive(config, "transports/ub/queue_budget",
                                         "transports/ub/business_queue_budget",
                                         parsed.queue_budget));
        CHECK_STATUS(readPositive(config, "transports/ub/quarantine_budget",
                                  parsed.quarantine_budget,
                                  parsed.quarantine_budget));
        CHECK_STATUS(readPositive(config, "transports/ub/tombstone_budget",
                                  parsed.tombstone_budget,
                                  parsed.tombstone_budget));
        CHECK_STATUS(readProbeTopology(config, parsed.probe));

        // K belongs to UbTask (D-6). The pooling side's process-wide
        // send_pool/task_max_inflight_attempts is removed (design §7.1): it is
        // ignored, with a warning, instead of being silently reinterpreted.
        if (!config
                 .get<json>(
                     "transports/ub/send_pool/task_max_inflight_attempts",
                     json())
                 .is_null()) {
            LOG(WARNING)
                << "deprecated UB config key "
                   "transports/ub/send_pool/task_max_inflight_attempts is "
                   "ignored; use transports/ub/task_max_inflight_attempts";
        }
        CHECK_STATUS(readPositive(config,
                                  "transports/ub/task_max_inflight_attempts",
                                  parsed.task_max_inflight_attempts,
                                  parsed.task_max_inflight_attempts));
        CHECK_STATUS(readPositive(config, "transports/ub/defer_reflow_batch",
                                  parsed.defer_reflow_batch,
                                  parsed.defer_reflow_batch));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/defer_reflow_max_gap_ms",
            parsed.defer_reflow_max_gap_ms, parsed.defer_reflow_max_gap_ms));
        CHECK_STATUS(readBool(config, "transports/ub/require_same_eid_scope",
                              parsed.require_same_eid_scope,
                              parsed.require_same_eid_scope));
        CHECK_STATUS(readBool(config, "transports/ub/retry_timeout_write",
                              parsed.retry_timeout_write,
                              parsed.retry_timeout_write));
        CHECK_STATUS(readBool(config, "transports/ub/honor_request_deadline",
                              parsed.honor_request_deadline,
                              parsed.honor_request_deadline));

        CHECK_STATUS(readBool(config, "transports/ub/probe/enabled",
                              parsed.probe.enabled, parsed.probe.enabled));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/interval_ms",
                                  parsed.probe.interval_ms,
                                  parsed.probe.interval_ms));
        CHECK_STATUS(readPositive(
            config, "transports/ub/probe/post_rejection_cap",
            parsed.probe.post_rejection_cap, parsed.probe.post_rejection_cap));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/backoff_max_ms",
                                  parsed.probe.backoff_max_ms,
                                  parsed.probe.backoff_max_ms));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/timeout_ms",
                                  parsed.probe.timeout_ms,
                                  parsed.probe.timeout_ms));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/peer_budget",
                                  parsed.probe.peer_budget,
                                  parsed.probe.peer_budget));
        CHECK_STATUS(readPositive(
            config, "transports/ub/probe/failure_threshold",
            parsed.probe.failure_threshold, parsed.probe.failure_threshold));
        // stop_on_recovery and recover_success_threshold are fixed values in
        // W1: any other value is rejected instead of being silently ignored.
        CHECK_STATUS(readFixedBool(
            config, "transports/ub/probe/stop_on_recovery", true));
        CHECK_STATUS(readFixedU32(
            config, "transports/ub/probe/recover_success_threshold", 1));
        std::string jfc_mode;
        CHECK_STATUS(readStringEnum(config, "transports/ub/probe/jfc_mode",
                                    "dedicated", {"dedicated", "shared"},
                                    jfc_mode));
        parsed.probe.jfc_mode = jfc_mode == "shared" ? ProbeJfcMode::kShared
                                                     : ProbeJfcMode::kDedicated;
        std::string provider;
        CHECK_STATUS(readStringEnum(config, "transports/ub/probe/provider",
                                    "pool", {"pool", "standalone"}, provider));
        parsed.probe.provider = provider == "standalone"
                                    ? ProbeProviderKind::kStandalone
                                    : ProbeProviderKind::kPool;
        CHECK_STATUS(readPositive(config, "transports/ub/probe/jfc_depth",
                                  parsed.probe.jfc_depth,
                                  parsed.probe.jfc_depth));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/probe/credit_reserve",
            parsed.probe.credit_reserve, parsed.probe.credit_reserve));
        CHECK_STATUS(readStringList(config, "transports/ub/probe/device_filter",
                                    parsed.probe.device_filter));
        uint64_t source_page_size = parsed.probe.source_page_size;
        CHECK_STATUS(readNonNegative(config,
                                     "transports/ub/probe/source_page_size",
                                     source_page_size, source_page_size));
        if (source_page_size > std::numeric_limits<size_t>::max()) {
            return Status::InvalidArgument(
                "transports/ub/probe/source_page_size exceeds size_t");
        }
        parsed.probe.source_page_size = static_cast<size_t>(source_page_size);
        CHECK_STATUS(readBool(config, "transports/ub/probe/test/periodic_all",
                              parsed.probe.test_periodic_all,
                              parsed.probe.test_periodic_all));
        CHECK_STATUS(readPositive(
            config, "transports/ub/probe/test/interval_ms",
            parsed.probe.test_interval_ms, parsed.probe.test_interval_ms));
        CHECK_STATUS(readBool(config, "transports/ub/probe/test/apply_health",
                              parsed.probe.test_apply_health,
                              parsed.probe.test_apply_health));
        CHECK_STATUS(readBool(config,
                              "transports/ub/observability/verbose_events",
                              parsed.observability.verbose_events,
                              parsed.observability.verbose_events));
        CHECK_STATUS(readNonNegative(config,
                                     "transports/ub/observability/sample_every",
                                     parsed.observability.sample_every,
                                     parsed.observability.sample_every));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/observability/topology_dump_limit",
            parsed.observability.topology_dump_limit,
            parsed.observability.topology_dump_limit));

        // Declared topology cross-checks (design §4.3/§4.5). A declaration is
        // only meaningful in standalone mode with binding-based selection:
        // an injected provider decides its own resource mapping, and a
        // device_filter would make the declared bind ambiguous.
        if (parsed.probe.hasDeclaredTopology()) {
            if (parsed.probe.provider != ProbeProviderKind::kStandalone) {
                return Status::InvalidArgument(
                    "declared probe topology requires "
                    "transports/ub/probe/provider=standalone");
            }
            if (!parsed.probe.device_filter.empty() ||
                !parsed.device_filter.empty()) {
                return Status::InvalidArgument(
                    "declared probe topology cannot be combined with "
                    "transports/ub/device_filter or "
                    "transports/ub/probe/device_filter; local_ports bind "
                    "selects the probe devices");
            }
            if (parsed.runtime_profile == UbRuntimeProfile::kRcLegacy) {
                return Status::InvalidArgument(
                    "declared probe topology requires an RM runtime "
                    "(runtime_profile=rm_legacy or rm_pool); RC legacy has no "
                    "probe transport");
            }
            if (!config.get<json>("transports/ub/probe/enabled", json())
                     .is_null() &&
                !parsed.probe.enabled) {
                return Status::InvalidArgument(
                    "declared probe topology requires "
                    "transports/ub/probe/enabled=true");
            }
        }

        // Cross-key constraints (design §13.1). Each one has its own message
        // so an operator can tell which pair is inconsistent.
        if (parsed.probe.backoff_max_ms < parsed.probe.interval_ms) {
            return Status::InvalidArgument(
                "transports/ub/probe/backoff_max_ms must be >= "
                "transports/ub/probe/interval_ms");
        }
        if (parsed.probe.timeout_ms > parsed.probe.interval_ms) {
            return Status::InvalidArgument(
                "transports/ub/probe/timeout_ms must be <= "
                "transports/ub/probe/interval_ms");
        }
        if (parsed.probe.jfc_depth < 8) {
            return Status::InvalidArgument(
                "transports/ub/probe/jfc_depth must be >= 8");
        }
        if (parsed.probe.jfc_mode == ProbeJfcMode::kShared &&
            parsed.probe.credit_reserve == 0) {
            return Status::InvalidArgument(
                "transports/ub/probe/credit_reserve must be > 0 in shared "
                "mode");
        }
        if (parsed.probe.provider == ProbeProviderKind::kStandalone &&
            parsed.probe.jfc_mode == ProbeJfcMode::kShared) {
            return Status::InvalidArgument(
                "transports/ub/probe/provider=standalone requires "
                "transports/ub/probe/jfc_mode=dedicated");
        }
        if (parsed.probe.test_periodic_all && !parsed.probe.enabled) {
            return Status::InvalidArgument(
                "transports/ub/probe/test/periodic_all requires "
                "transports/ub/probe/enabled=true");
        }
        if (!parsed.probe.valid()) {
            return Status::InvalidArgument("invalid probe configuration");
        }

        // W3 runtime and queue cross-checks (§12). All of them run once at
        // install time and fail closed; an inconsistent pair is never
        // downgraded to a warning.
        //
        // The queue budget must admit one maximal Task, because submit() is
        // all-or-nothing: a smaller budget would reject every large Task
        // forever (design §5.1 and its note).
        if (parsed.queue_budget <
            static_cast<uint64_t>(parsed.max_slices_per_task)) {
            return Status::InvalidArgument(
                "transports/ub/queue_budget must be >= "
                "transports/ub/max_slices_per_task");
        }
        // The first pooled version is static: initial/min/max must agree with
        // the serving target instead of implying an unsupported resize path.
        if (parsed.jetty_pool.business_min_size >
                parsed.jetty_pool.business_desired_serving ||
            parsed.jetty_pool.business_desired_serving >
                parsed.jetty_pool.business_max_size) {
            return Status::InvalidArgument(
                "transports/ub/jetty_pool business sizes must satisfy "
                "min_size <= desired_serving <= max_size");
        }
        if (parsed.jetty_pool.business_initial_size <
                parsed.jetty_pool.business_min_size ||
            parsed.jetty_pool.business_initial_size >
                parsed.jetty_pool.business_max_size) {
            return Status::InvalidArgument(
                "transports/ub/jetty_pool/business_initial_size must be within "
                "[business_min_size, business_max_size]");
        }
        if (parsed.task_max_inflight_attempts < 1) {
            return Status::InvalidArgument(
                "transports/ub/task_max_inflight_attempts must be >= 1");
        }
        if (parsed.runtime_profile == UbRuntimeProfile::kRcLegacy) {
            if (parsed.trans_mode != UbTransportMode::kRc) {
                return Status::InvalidArgument(
                    "runtime_profile=rc_legacy requires trans_mode=rc");
            }
            if (parsed.jetty_pool.enable) {
                return Status::InvalidArgument(
                    "transports/ub/jetty_pool/enable requires "
                    "runtime_profile=rm_pool");
            }
        } else if (parsed.runtime_profile == UbRuntimeProfile::kRmLegacy) {
            if (parsed.trans_mode != UbTransportMode::kRm) {
                return Status::InvalidArgument(
                    "runtime_profile=rm_legacy requires trans_mode=rm");
            }
            if (parsed.jetty_pool.enable) {
                return Status::InvalidArgument(
                    "runtime_profile=rm_legacy must not enable jetty_pool");
            }
        } else if (parsed.runtime_profile == UbRuntimeProfile::kRmPool) {
            // M-3: rm_pool does NOT force probe/enabled. Probing stays an
            // independent v1.7 decision; the pooled data path only requires
            // the RM transport, the pool switch and at least one serving slot.
            if (parsed.trans_mode != UbTransportMode::kRm) {
                return Status::InvalidArgument(
                    "runtime_profile=rm_pool requires trans_mode=rm");
            }
            if (!parsed.jetty_pool.enable) {
                return Status::InvalidArgument(
                    "runtime_profile=rm_pool requires jetty_pool/enable=true");
            }
            if (parsed.jetty_pool.force_disable) {
                return Status::InvalidArgument(
                    "runtime_profile=rm_pool is rejected while "
                    "jetty_pool/force_disable=true");
            }
            if (parsed.jetty_pool.business_desired_serving < 1) {
                return Status::InvalidArgument(
                    "runtime_profile=rm_pool requires "
                    "jetty_pool/business_desired_serving >= 1");
            }
        } else if (parsed.jetty_pool.enable) {
            return Status::InvalidArgument(
                "transports/ub/jetty_pool/enable requires "
                "runtime_profile=rm_pool");
        } else if (parsed.trans_mode == UbTransportMode::kRm &&
                   parsed.runtime_profile != UbRuntimeProfile::kRmLegacy) {
            return Status::InvalidArgument(
                "transports/ub/trans_mode=rm requires runtime_profile="
                "rm_legacy or rm_pool");
        }

        output = std::move(parsed);
        return Status::OK();
    }

    // W3 data-path gate (D-2): the pooled admission path is entered if and
    // only if the transport attached a SendJettyPool to the context. This
    // keeps both behaviors inside one binary and makes the regression
    // boundary a pointer comparison.
    [[nodiscard]] bool pooledSendPoolRequested() const {
        return runtime_profile == UbRuntimeProfile::kRmPool &&
               jetty_pool.enable && !jetty_pool.force_disable;
    }

   private:
    static Status readRuntimeProfile(const Config& config,
                                     UbRuntimeProfile& output) {
        static constexpr const char* kKey = "transports/ub/runtime_profile";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) return Status::OK();
        if (!value.is_string()) {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be a string");
        }
        const auto text = value.get<std::string>();
        if (!parseUbRuntimeProfile(text, output)) {
            return Status::InvalidArgument(
                std::string(kKey) + " must be rc_legacy, rm_legacy or rm_pool");
        }
        return Status::OK();
    }

    static Status readTransportMode(const Config& config,
                                    UbTransportMode& output) {
        static constexpr const char* kKey = "transports/ub/trans_mode";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) return Status::OK();
        if (!value.is_string()) {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be a string");
        }
        const auto text = value.get<std::string>();
        if (!parseUbTransportMode(text, output)) {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be rc or rm");
        }
        return Status::OK();
    }

    // Upper bound for the idle-spin windows (yuanhao-1113). Anything larger is
    // a configuration mistake, not a tuning choice.
    static constexpr uint32_t kMaxIdleSpinUs = 1'000'000;
    // Strict reader for transports/ub/max_target_jetties_per_node (guidance
    // 4.2.1): absent keeps the default 8, but an explicit null is rejected
    // just like 0, negatives, bools, strings, floats and overflow. get()
    // maps both absent and explicit null to null, so paths() (which lists
    // null leaves) distinguishes the two cases.
    static Status readNodeTargetQuotaLimit(const Config& config,
                                           uint32_t& output) {
        static constexpr const char* kKey =
            "transports/ub/max_target_jetties_per_node";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) {
            const auto paths = config.paths();
            if (std::find(paths.begin(), paths.end(), kKey) != paths.end()) {
                return Status::InvalidArgument(std::string(kKey) +
                                               " must not be null");
            }
            return Status::OK();
        }
        if (!value.is_number_integer()) {
            return Status::InvalidArgument(
                std::string(kKey) +
                " must be an integer in 1..UINT32_MAX");
        }
        int64_t signed_value = 0;
        try {
            signed_value = value.get<int64_t>();
        } catch (...) {
            return Status::InvalidArgument(std::string(kKey) +
                                           " is outside the supported range");
        }
        if (signed_value <= 0 ||
            static_cast<uint64_t>(signed_value) >
                std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument(
                std::string(kKey) +
                " must be an integer in 1..UINT32_MAX");
        }
        output = static_cast<uint32_t>(signed_value);
        return Status::OK();
    }

    template <typename T>
    static Status readPositive(const Config& config, const char* key,
                               T default_value, T& output) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) {
            output = default_value;
            return Status::OK();
        }
        if (!value.is_number_integer()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a positive integer");
        }

        int64_t signed_value = 0;
        try {
            signed_value = value.get<int64_t>();
        } catch (...) {
            return Status::InvalidArgument(std::string(key) +
                                           " is outside the supported range");
        }
        if (signed_value <= 0 ||
            static_cast<uint64_t>(signed_value) >
                static_cast<uint64_t>(std::numeric_limits<T>::max())) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a positive integer in "
                                           "the supported range");
        }
        output = static_cast<T>(signed_value);
        return Status::OK();
    }

    template <typename T>
    static Status readNonNegative(const Config& config, const char* key,
                                  T default_value, T& output) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) {
            output = default_value;
            return Status::OK();
        }
        if (!value.is_number_integer()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a non-negative integer");
        }
        int64_t signed_value = 0;
        try {
            signed_value = value.get<int64_t>();
        } catch (...) {
            return Status::InvalidArgument(std::string(key) +
                                           " is outside the supported range");
        }
        if (signed_value < 0 ||
            static_cast<uint64_t>(signed_value) >
                static_cast<uint64_t>(std::numeric_limits<T>::max())) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a non-negative integer "
                                           "in the supported range");
        }
        output = static_cast<T>(signed_value);
        return Status::OK();
    }

    // A fixed-value key may be omitted (the default is the fixed value) but
    // must never carry a different value.
    static Status readFixedBool(const Config& config, const char* key,
                                bool required) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) return Status::OK();
        if (!value.is_boolean()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a boolean");
        }
        if (value.get<bool>() != required) {
            return Status::InvalidArgument(std::string(key) + " is fixed to " +
                                           (required ? "true" : "false"));
        }
        return Status::OK();
    }

    static Status readFixedU32(const Config& config, const char* key,
                               uint32_t required) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) return Status::OK();
        if (!value.is_number_integer()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be an integer");
        }
        int64_t signed_value = 0;
        try {
            signed_value = value.get<int64_t>();
        } catch (...) {
            return Status::InvalidArgument(std::string(key) +
                                           " is outside the supported range");
        }
        if (signed_value < 0 || static_cast<uint64_t>(signed_value) !=
                                    static_cast<uint64_t>(required)) {
            return Status::InvalidArgument(std::string(key) + " is fixed to " +
                                           std::to_string(required));
        }
        return Status::OK();
    }

    // String enumerations are read as JSON strings and matched exactly; an
    // unknown spelling is rejected rather than defaulted.
    static Status readStringEnum(const Config& config, const char* key,
                                 const char* default_value,
                                 std::initializer_list<const char*> allowed,
                                 std::string& output) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) {
            output = default_value;
            return Status::OK();
        }
        if (!value.is_string()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a string");
        }
        const std::string parsed = value.get<std::string>();
        for (const char* candidate : allowed) {
            if (parsed == candidate) {
                output = parsed;
                return Status::OK();
            }
        }
        std::string message = std::string(key) + " must be one of:";
        for (const char* candidate : allowed) {
            message += " ";
            message += candidate;
        }
        return Status::InvalidArgument(message);
    }

    static Status readBool(const Config& config, const char* key,
                           bool default_value, bool& output) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) {
            output = default_value;
            return Status::OK();
        }
        if (!value.is_boolean()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a boolean");
        }
        output = value.get<bool>();
        return Status::OK();
    }

    static Status readStringList(const Config& config, const std::string& key,
                                 std::vector<std::string>& output) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) {
            output.clear();
            return Status::OK();
        }

        std::vector<std::string> parsed;
        if (value.is_string()) {
            parsed.push_back(value.get<std::string>());
        } else if (value.is_array()) {
            for (const auto& entry : value) {
                if (!entry.is_string()) {
                    return Status::InvalidArgument(key +
                                                   " entries must be strings");
                }
                parsed.push_back(entry.get<std::string>());
            }
        } else {
            return Status::InvalidArgument(key +
                                           " must be a string or string array");
        }

        for (const auto& entry : parsed) {
            if (entry.empty()) {
                return Status::InvalidArgument(key +
                                               " entries must not be empty");
            }
        }
        output = std::move(parsed);
        return Status::OK();
    }

    static Status readDeviceIdentities(const Config& config,
                                       std::vector<DeviceIdentity>& output) {
        static constexpr const char* kKey = "transports/ub/device_identities";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) {
            output.clear();
            return Status::OK();
        }
        if (!value.is_array()) {
            return Status::InvalidArgument(
                "transports/ub/device_identities must be an array");
        }

        std::vector<DeviceIdentity> parsed;
        for (size_t i = 0; i < value.size(); ++i) {
            try {
                const auto& entry = value[i];
                DeviceIdentity identity;
                identity.native_device_name =
                    entry.at("native_device_name").get<std::string>();
                identity.native_device_path =
                    entry.at("native_device_path").get<std::string>();
                identity.eid = entry.at("eid").get<std::string>();

                const auto& eid_index = entry.at("eid_index");
                const auto& transport_modes = entry.at("transport_modes");
                if (!eid_index.is_number_integer() ||
                    !transport_modes.is_number_integer()) {
                    return Status::InvalidArgument(
                        "UB device identity indices and transport_modes must "
                        "be integers");
                }
                const int64_t parsed_eid_index = eid_index.get<int64_t>();
                const int64_t parsed_modes = transport_modes.get<int64_t>();
                if (parsed_eid_index < 0 ||
                    static_cast<uint64_t>(parsed_eid_index) >
                        std::numeric_limits<uint32_t>::max() ||
                    parsed_modes <= 0 ||
                    static_cast<uint64_t>(parsed_modes) >
                        std::numeric_limits<uint16_t>::max()) {
                    return Status::InvalidArgument(
                        "UB device identity index or transport_modes is out "
                        "of range");
                }
                identity.eid_index = static_cast<uint32_t>(parsed_eid_index);
                identity.transport_modes = static_cast<uint16_t>(parsed_modes);

                std::string kind;
                entry.at("kind").get_to(kind);
                if (!parseDeviceKind(kind, identity.kind)) {
                    return Status::InvalidArgument(
                        "unknown UB device kind in device_identities");
                }
                CHECK_STATUS(validateDeviceIdentityRecord(identity));
                for (const auto& existing : parsed) {
                    if (sameDeviceLocator(identity, existing)) {
                        return Status::InvalidArgument(
                            "duplicate UB device identity configuration");
                    }
                }
                parsed.push_back(std::move(identity));
            } catch (const std::exception& error) {
                return Status::InvalidArgument(
                    "invalid transports/ub/device_identities entry " +
                    std::to_string(i) + ": " + error.what());
            }
        }
        output = std::move(parsed);
        return Status::OK();
    }

    static Status readDeviceFilter(const Config& config,
                                   std::vector<std::string>& output) {
        return readStringList(config, "transports/ub/device_filter", output);
    }

    // Renamed keys follow "new key wins, old key warns, conflicting pair is
    // rejected". Never silently pick one value.
    template <typename T>
    static Status readRenamedPositive(const Config& config, const char* new_key,
                                      const char* old_key, T& output) {
        const json new_value = config.get<json>(new_key, json());
        const json old_value = config.get<json>(old_key, json());
        if (new_value.is_null() && old_value.is_null()) return Status::OK();
        if (!new_value.is_null() && !old_value.is_null() &&
            new_value != old_value) {
            return Status::InvalidArgument(
                std::string("conflicting UB config: ") + new_key +
                " and the "
                "deprecated " +
                old_key + " disagree");
        }
        if (!new_value.is_null()) {
            return readPositive(config, new_key, output, output);
        }
        LOG(WARNING) << "deprecated UB config key " << old_key << ", use "
                     << new_key;
        return readPositive(config, old_key, output, output);
    }

    // Declared probe topology (design §4): fail-closed validation, because a
    // malformed adjacency becomes a silently wrong path set at runtime. The
    // old shape (adjacency: {local: [peer...]}, target_map) is rejected with a
    // migration example instead of being reinterpreted.
    static Status readProbeTopology(const Config& config, ProbeParams& probe) {
        const json target_map =
            config.get<json>("transports/ub/probe/target_map", json());
        if (!target_map.is_null()) {
            return Status::InvalidArgument(
                "transports/ub/probe/target_map was removed; declare remote "
                "port EIDs under transports/ub/probe/peers/.../ports[] and "
                "edges under transports/ub/probe/adjacency/{peer}/{local_id}");
        }

        std::unordered_set<std::string> unique_ports;
        const json ports_json =
            config.get<json>("transports/ub/probe/local_ports", json());
        if (!ports_json.is_null()) {
            if (!ports_json.is_array()) {
                return Status::InvalidArgument(
                    "transports/ub/probe/local_ports must be an array");
            }
            for (const auto& entry : ports_json) {
                ProbeLocalPortDecl port;
                if (entry.is_string()) {
                    port.id = entry.get<std::string>();
                    port.bind = port.id;
                } else if (entry.is_object()) {
                    if (!entry.contains("id") || !entry["id"].is_string() ||
                        !entry.contains("bind") || !entry["bind"].is_string()) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/local_ports entries need a "
                            "string id and bind");
                    }
                    port.id = entry["id"].get<std::string>();
                    port.bind = entry["bind"].get<std::string>();
                    if (entry.contains("eid")) {
                        if (!entry["eid"].is_string()) {
                            return Status::InvalidArgument(
                                "transports/ub/probe/local_ports eid must be "
                                "a string");
                        }
                        port.eid = entry["eid"].get<std::string>();
                    }
                } else {
                    return Status::InvalidArgument(
                        "transports/ub/probe/local_ports entries must be a "
                        "string or an object");
                }
                if (port.id.empty() || port.id.size() > 64) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/local_ports id must be non-empty "
                        "and at most 64 bytes");
                }
                if (port.bind.empty()) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/local_ports bind must be "
                        "non-empty");
                }
                if (!port.eid.empty() && !isCanonicalEid(port.eid)) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/local_ports eid must be a "
                        "canonical URMA EID");
                }
                if (!unique_ports.insert(port.id).second) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/local_ports contains a duplicate "
                        "port id");
                }
                probe.local_ports.push_back(std::move(port));
            }
        }

        const json peers_json =
            config.get<json>("transports/ub/probe/peers", json());
        if (!peers_json.is_null()) {
            if (!peers_json.is_object()) {
                return Status::InvalidArgument(
                    "transports/ub/probe/peers must be an object");
            }
            for (const auto& item : peers_json.items()) {
                if (item.key().empty()) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/peers has an empty peer name");
                }
                const json& peer_value = item.value();
                if (!peer_value.is_object() || !peer_value.contains("ports") ||
                    !peer_value["ports"].is_array()) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/peers/" + item.key() +
                        " must be an object with a ports array");
                }
                ProbePeerDecl peer;
                peer.segment = item.key();
                std::unordered_set<std::string> port_ids;
                std::unordered_set<std::string> port_eids;
                for (const auto& port_entry : peer_value["ports"]) {
                    if (!port_entry.is_object() || !port_entry.contains("id") ||
                        !port_entry["id"].is_string() ||
                        !port_entry.contains("eid") ||
                        !port_entry["eid"].is_string()) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/peers/" + item.key() +
                            "/ports entries need string id and eid");
                    }
                    ProbePeerPortDecl port;
                    port.id = port_entry["id"].get<std::string>();
                    port.eid = port_entry["eid"].get<std::string>();
                    if (port.id.empty()) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/peers/" + item.key() +
                            "/ports has an empty id");
                    }
                    if (!isCanonicalEid(port.eid)) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/peers/" + item.key() +
                            "/ports eid must be a canonical URMA EID");
                    }
                    if (!port_ids.insert(port.id).second) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/peers/" + item.key() +
                            "/ports has a duplicate id");
                    }
                    if (!port_eids.insert(port.eid).second) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/peers/" + item.key() +
                            "/ports has a duplicate eid");
                    }
                    peer.ports.push_back(std::move(port));
                }
                if (peer.ports.empty()) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/peers/" + item.key() +
                        "/ports must not be empty");
                }
                probe.peers.emplace(peer.segment, std::move(peer));
            }
        }

        const json adjacency_json =
            config.get<json>("transports/ub/probe/adjacency", json());
        if (!adjacency_json.is_null()) {
            if (!adjacency_json.is_object()) {
                return Status::InvalidArgument(
                    "transports/ub/probe/adjacency must be an object");
            }
            for (const auto& peer_item : adjacency_json.items()) {
                const std::string& peer_name = peer_item.key();
                const json& peer_value = peer_item.value();
                if (peer_value.is_array()) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/adjacency has the removed shape "
                        "{local: [peer...]}; use {peer: {local_id: "
                        "[remote_id...]}} and declare peer ports under "
                        "transports/ub/probe/peers");
                }
                auto peer = probe.peers.find(peer_name);
                if (peer == probe.peers.end()) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/adjacency references an "
                        "undeclared peer '" +
                        peer_name + "'");
                }
                if (!peer_value.is_object()) {
                    return Status::InvalidArgument(
                        "transports/ub/probe/adjacency/" + peer_name +
                        " must be an object of remote-id arrays");
                }
                std::map<std::string, std::vector<std::string>> edges;
                // A peer with no declared local edge still needs an entry so
                // the declared set can express "this peer is blocked
                // entirely"; an empty map is valid but the top-level
                // adjacency must be non-empty.
                for (const auto& local_item : peer_value.items()) {
                    const std::string& local_id = local_item.key();
                    if (unique_ports.count(local_id) == 0) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/adjacency/" + peer_name +
                            " references an undeclared local port '" +
                            local_id + "'");
                    }
                    if (!local_item.value().is_array()) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/adjacency/" + peer_name + "/" +
                            local_id + " must be an array of remote ids");
                    }
                    std::vector<std::string> remote_ids;
                    std::unordered_set<std::string> unique_remotes;
                    for (const auto& remote : local_item.value()) {
                        if (!remote.is_string()) {
                            return Status::InvalidArgument(
                                "transports/ub/probe/adjacency/" + peer_name +
                                "/" + local_id +
                                " must contain only remote-id strings");
                        }
                        const std::string remote_id = remote.get<std::string>();
                        if (remote_id.empty() ||
                            !unique_remotes.insert(remote_id).second) {
                            return Status::InvalidArgument(
                                "transports/ub/probe/adjacency/" + peer_name +
                                "/" + local_id +
                                " has an empty or duplicate remote id");
                        }
                        const bool declared =
                            std::any_of(peer->second.ports.begin(),
                                        peer->second.ports.end(),
                                        [&](const ProbePeerPortDecl& port) {
                                            return port.id == remote_id;
                                        });
                        if (!declared) {
                            return Status::InvalidArgument(
                                "transports/ub/probe/adjacency/" + peer_name +
                                "/" + local_id +
                                " references undeclared "
                                "remote id '" +
                                remote_id + "'");
                        }
                        remote_ids.push_back(remote_id);
                    }
                    if (remote_ids.empty()) {
                        return Status::InvalidArgument(
                            "transports/ub/probe/adjacency/" + peer_name + "/" +
                            local_id + " must not be empty");
                    }
                    edges.emplace(local_id, std::move(remote_ids));
                }
                probe.adjacency.emplace(peer_name, std::move(edges));
            }
        }

        // enforce_adjacency defaults to true, but a declaration-less install
        // (injected seam or probe off) simply has nothing to enforce. Only an
        // explicit true without a declaration is an operator error.
        const json enforce_json =
            config.get<json>("transports/ub/probe/enforce_adjacency", json());
        if (!enforce_json.is_null()) {
            if (!enforce_json.is_boolean()) {
                return Status::InvalidArgument(
                    "transports/ub/probe/enforce_adjacency must be a boolean");
            }
            probe.enforce_adjacency = enforce_json.get<bool>();
            if (probe.enforce_adjacency && probe.adjacency.empty()) {
                return Status::InvalidArgument(
                    "transports/ub/probe/enforce_adjacency=true requires a "
                    "non-empty transports/ub/probe/adjacency");
            }
        }
        probe.dynamic_edges = !probe.adjacency.empty();

        CHECK_STATUS(readPositive(config,
                                  "transports/ub/probe/handshake/"
                                  "refresh_interval_ms",
                                  probe.handshake.refresh_interval_ms,
                                  probe.handshake.refresh_interval_ms));
        CHECK_STATUS(readPositive(config,
                                  "transports/ub/probe/handshake/"
                                  "retry_backoff_ms",
                                  probe.handshake.retry_backoff_ms,
                                  probe.handshake.retry_backoff_ms));
        CHECK_STATUS(readPositive(
            config, "transports/ub/probe/handshake/timeout_ms",
            probe.handshake.timeout_ms, probe.handshake.timeout_ms));
        CHECK_STATUS(
            readPositive(config, "transports/ub/probe/handshake/threads",
                         probe.handshake.threads, probe.handshake.threads));
        CHECK_STATUS(readBool(config,
                              "transports/ub/probe/handshake/"
                              "require_peer_ports",
                              probe.handshake.require_peer_ports,
                              probe.handshake.require_peer_ports));
        CHECK_STATUS(readBool(
            config,
            "transports/ub/probe/adjacency_policy/require_same_eid_scope",
            probe.adjacency_policy.require_same_eid_scope,
            probe.adjacency_policy.require_same_eid_scope));
        CHECK_STATUS(readNonNegative(
            config,
            "transports/ub/probe/adjacency_policy/"
            "param_error_invalidate_threshold",
            probe.adjacency_policy.param_error_invalidate_threshold,
            probe.adjacency_policy.param_error_invalidate_threshold));
        CHECK_STATUS(readPositive(config,
                                  "transports/ub/probe/handshake/"
                                  "withdraw_grace",
                                  probe.handshake.withdraw_grace,
                                  probe.handshake.withdraw_grace));
        if (probe.handshake.refresh_interval_ms < 1000 ||
            probe.handshake.refresh_interval_ms > 600000) {
            return Status::InvalidArgument(
                "transports/ub/probe/handshake/refresh_interval_ms must be in "
                "[1000, 600000]");
        }
        if (probe.handshake.retry_backoff_ms < 100 ||
            probe.handshake.retry_backoff_ms > 60000) {
            return Status::InvalidArgument(
                "transports/ub/probe/handshake/retry_backoff_ms must be in "
                "[100, 60000]");
        }
        if (probe.handshake.timeout_ms < 100 ||
            probe.handshake.timeout_ms > 30000 ||
            probe.handshake.timeout_ms > probe.handshake.refresh_interval_ms) {
            return Status::InvalidArgument(
                "transports/ub/probe/handshake/timeout_ms must be in "
                "[100, 30000] and <= refresh_interval_ms");
        }
        if (probe.handshake.threads < 1 || probe.handshake.threads > 4) {
            return Status::InvalidArgument(
                "transports/ub/probe/handshake/threads must be in [1, 4]");
        }
        return Status::OK();
    }
};

// Evaluates the default-off matrix before any adapter or native resource is
// created. Structural conflicts are rejected as invalid configuration; every
// accepted mode still runs its provider/capability gates during install
// before any native resource is created, and those gates fail closed instead
// of silently running RC under an RM label. `jetty_pool/enable=false` alone
// is not a failure: it simply selects the RC legacy baseline.
inline Status resolveUbRuntimeMode(const UbParams& params,
                                   UbRuntimeMode& mode) {
    const bool pool = params.jetty_pool.enable;
    const bool reuse = params.jetty_reuse.enable;
    const bool probe = params.probe.enabled;

    // One snapshot feeds every branch, so a CI failure prints the complete
    // selection instead of only the first field that happened to disagree.
    const auto snapshot = [&] {
        std::ostringstream out;
        out << "profile=" << ubRuntimeProfileName(params.runtime_profile)
            << " trans_mode=" << ubTransportModeName(params.trans_mode)
            << " jetty_pool=" << (pool ? "on" : "off") << " force_disable="
            << (params.jetty_pool.force_disable ? "true" : "false")
            << " jetty_reuse=" << (reuse ? "on" : "off")
            << " probe=" << (probe ? "on" : "off")
            << " physical_only=" << (params.physical_only ? "true" : "false");
        return out.str();
    };
    const auto reject = [&](Status status) {
        LOG(WARNING) << "UB runtime selection rejected: " << snapshot()
                     << " status=" << status.ToString();
        return status;
    };

    if (params.runtime_profile == UbRuntimeProfile::kRcLegacy) {
        if (params.trans_mode != UbTransportMode::kRc) {
            return reject(Status::InvalidArgument(
                "runtime_profile=rc_legacy requires trans_mode=rc"));
        }
        if (pool) {
            return reject(Status::InvalidArgument(
                "jetty_pool.enable requires runtime_profile=rm_pool"));
        }
        if (reuse) {
            return reject(Status::InvalidArgument(
                "jetty_reuse.enable requires runtime_profile=rm_legacy"));
        }
        mode = UbRuntimeMode::kRcLegacy;
        VLOG(1) << "UB runtime selection accepted: mode=rc_legacy "
                << snapshot();
        return Status::OK();
    }

    if (params.trans_mode != UbTransportMode::kRm) {
        return reject(Status::InvalidArgument(
            "RM runtime profiles require trans_mode=rm"));
    }
    if (params.runtime_profile == UbRuntimeProfile::kRmLegacy) {
        if (pool) {
            return reject(Status::InvalidArgument(
                "runtime_profile=rm_legacy must not enable jetty_pool"));
        }
        if (!params.physical_only) {
            return reject(Status::InvalidArgument(
                "runtime_profile=rm_legacy requires physical_only=true"));
        }
        // A well-formed RM Legacy configuration passes the config gate. The
        // provider/capability gates (trusted PHYSICAL identity, RM/RTP
        // priority masks, bootstrap v2) run during install before any native
        // resource is created and fail closed with NotImplemented or
        // InvalidArgument; they never fall back to RC or bonding.
        mode = UbRuntimeMode::kRmLegacy;
        VLOG(1) << "UB runtime selection accepted: mode=rm_legacy "
                << snapshot();
        return Status::OK();
    }

    if (!pool) {
        return reject(Status::InvalidArgument(
            "runtime_profile=rm_pool requires jetty_pool.enable=true"));
    }
    if (reuse) {
        return reject(Status::InvalidArgument(
            "jetty_reuse.enable requires runtime_profile=rm_legacy and is "
            "not part of rm_pool"));
    }
    if (!params.physical_only) {
        return reject(Status::InvalidArgument(
            "runtime_profile=rm_pool requires physical_only=true"));
    }
    if (params.jetty_pool.force_disable) {
        // The runtime brake stays fail-closed: an operator that forced the
        // pool off must not be overridden by a profile selection.
        return reject(
            Status::InvalidArgument("runtime_profile=rm_pool is rejected while "
                                    "jetty_pool.force_disable=true"));
    }
    // A well-formed pooled RM configuration passes the config gate. The
    // provider/capability gate (trusted PHYSICAL identity, RM/RTP priority
    // masks), the device-level native budget and the JFC credit ledger are
    // installed during transport install before any pooled Jetty is created,
    // and every one of them fails closed.
    mode = UbRuntimeMode::kPooledRm;
    VLOG(1) << "UB runtime selection accepted: mode=rm_pool " << snapshot();
    return Status::OK();
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PARAMS_H_
