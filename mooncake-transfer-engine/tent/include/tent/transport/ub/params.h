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

#include <cstddef>
#include <cstdint>
#include <glog/logging.h>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/transport/ub/device_selection.h"

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

// Pooled resources stay off unless the full gate matrix passes. force_disable
// is a runtime brake for pooled attempts and never switches the native mode.
struct UbJettyPoolParams {
    bool enable = false;
    bool force_disable = false;
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

// Probe keys are schema-complete but dormant until the P4 health scheduler
// exists. `enabled` only declares resource capability; it never starts a
// periodic WR on its own.
struct UbProbeParams {
    bool enabled = false;
    uint32_t interval = 1000;
    uint32_t timeout = 500;
    uint32_t backoff_max = 30000;
    uint32_t peer_budget = 1;
    uint32_t recover_success_threshold = 1;
    uint32_t failure_threshold = 3;
    bool stop_on_recovery = true;
};

// Pooled send-path parameters (C01). Both knobs are explicit positive
// integers and are never derived from jetty_per_endpoint:
//  - desired_per_context is the static Serving (IDLE + IN_USE) target per
//    Context/LocalSendProfile pool key;
//  - task_max_inflight_attempts is K, the maximum inflight attempts of one
//    network UbTask across all contexts.
struct UbSendPoolParams {
    uint32_t desired_per_context = 6;
    uint32_t task_max_inflight_attempts = 4;
    // Bound on concurrent Jetty creations per pool key so refill cannot
    // storm the provider.
    uint32_t max_creating_per_key = 2;
    // Per-JFC WR credits reserved for control records (flush-done markers,
    // AE); business WRs only use the remaining capacity.
    uint32_t jfc_credit_reserve = 16;
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

struct UbParams {
    // UB is opt-in until a real URMA provider and NIC topology are present.
    bool enable = false;
    std::vector<std::string> device_filter;
    // Legacy selection remains the default. physical_only requires explicit
    // trusted identities and never falls back to name-based classification.
    bool physical_only = false;
    std::vector<DeviceIdentity> device_identities;

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

    // Defaults are the verified RC legacy baseline; pool and probe stay off.
    UbRuntimeProfile runtime_profile = UbRuntimeProfile::kRcLegacy;
    UbTransportMode trans_mode = UbTransportMode::kRc;
    UbJettyPoolParams jetty_pool;
    UbJettyReuseParams jetty_reuse;
    UbProbeParams probe;
    // Pooled data-path knobs; dormant until the rm_pool gate matrix passes.
    UbSendPoolParams send_pool;
    UbNativeBudgetParams native_budget;

    bool enable_bandwidth_estimation = true;
    bool enable_notifications = false;

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
        CHECK_STATUS(readBool(config, "transports/ub/probe/enabled",
                              parsed.probe.enabled, parsed.probe.enabled));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/interval",
                                  parsed.probe.interval,
                                  parsed.probe.interval));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/timeout",
                                  parsed.probe.timeout, parsed.probe.timeout));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/backoff_max",
                                  parsed.probe.backoff_max,
                                  parsed.probe.backoff_max));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/peer_budget",
                                  parsed.probe.peer_budget,
                                  parsed.probe.peer_budget));
        CHECK_STATUS(readPositive(
            config, "transports/ub/probe/recover_success_threshold",
            parsed.probe.recover_success_threshold,
            parsed.probe.recover_success_threshold));
        CHECK_STATUS(readPositive(config,
                                  "transports/ub/probe/failure_threshold",
                                  parsed.probe.failure_threshold,
                                  parsed.probe.failure_threshold));
        CHECK_STATUS(readBool(config, "transports/ub/probe/stop_on_recovery",
                              parsed.probe.stop_on_recovery,
                              parsed.probe.stop_on_recovery));
        if (!parsed.probe.stop_on_recovery) {
            return Status::InvalidArgument(
                "transports/ub/probe/stop_on_recovery must remain true in v1");
        }

        // Pooled send-path knobs: explicit positive integers, never derived
        // from jetty_per_endpoint (C01).
        CHECK_STATUS(readPositive(config,
                                  "transports/ub/send_pool/desired_per_context",
                                  parsed.send_pool.desired_per_context,
                                  parsed.send_pool.desired_per_context));
        CHECK_STATUS(readPositive(
            config, "transports/ub/send_pool/task_max_inflight_attempts",
            parsed.send_pool.task_max_inflight_attempts,
            parsed.send_pool.task_max_inflight_attempts));
        CHECK_STATUS(readPositive(
            config, "transports/ub/send_pool/max_creating_per_key",
            parsed.send_pool.max_creating_per_key,
            parsed.send_pool.max_creating_per_key));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/send_pool/jfc_credit_reserve",
            parsed.send_pool.jfc_credit_reserve,
            parsed.send_pool.jfc_credit_reserve));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/native_budget/max_jetty_per_device",
            parsed.native_budget.max_jetty_per_device,
            parsed.native_budget.max_jetty_per_device));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/native_budget/replacement_reserve",
            parsed.native_budget.replacement_reserve,
            parsed.native_budget.replacement_reserve));

        output = std::move(parsed);
        return Status::OK();
    }

   private:
    static Status readRuntimeProfile(const Config& config,
                                     UbRuntimeProfile& output) {
        static constexpr const char* kKey =
            "transports/ub/runtime_profile";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) return Status::OK();
        if (!value.is_string()) {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be a string");
        }
        const auto text = value.get<std::string>();
        if (!parseUbRuntimeProfile(text, output)) {
            return Status::InvalidArgument(
                std::string(kKey) +
                " must be rc_legacy, rm_legacy or rm_pool");
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

    static Status readDeviceIdentities(
        const Config& config, std::vector<DeviceIdentity>& output) {
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
                identity.eid_index =
                    static_cast<uint32_t>(parsed_eid_index);
                identity.transport_modes =
                    static_cast<uint16_t>(parsed_modes);

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
        static constexpr const char* kKey = "transports/ub/device_filter";
        const json value = config.get<json>(kKey, json());
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
                    return Status::InvalidArgument(
                        "transports/ub/device_filter entries must be strings");
                }
                parsed.push_back(entry.get<std::string>());
            }
        } else {
            return Status::InvalidArgument(
                "transports/ub/device_filter must be a string or string "
                "array");
        }

        for (const auto& entry : parsed) {
            if (entry.empty()) {
                return Status::InvalidArgument(
                    "transports/ub/device_filter entries must not be empty");
            }
        }
        output = std::move(parsed);
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
            << " jetty_pool=" << (pool ? "on" : "off")
            << " force_disable="
            << (params.jetty_pool.force_disable ? "true" : "false")
            << " jetty_reuse=" << (reuse ? "on" : "off")
            << " probe=" << (probe ? "on" : "off")
            << " physical_only="
            << (params.physical_only ? "true" : "false");
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
        if (probe) {
            return reject(Status::InvalidArgument(
                "probe.enabled requires runtime_profile=rm_pool"));
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
        if (probe) {
            return reject(Status::InvalidArgument(
                "probe.enabled requires runtime_profile=rm_pool"));
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
    if (!probe) {
        return reject(Status::InvalidArgument(
            "runtime_profile=rm_pool requires probe.enabled=true"));
    }
    if (!params.physical_only) {
        return reject(Status::InvalidArgument(
            "runtime_profile=rm_pool requires physical_only=true"));
    }
    if (params.jetty_pool.force_disable) {
        // The runtime brake stays fail-closed: an operator that forced the
        // pool off must not be overridden by a profile selection.
        return reject(Status::InvalidArgument(
            "runtime_profile=rm_pool is rejected while "
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
