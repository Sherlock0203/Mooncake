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
#include <initializer_list>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/transport/ub/probe_types.h"

namespace mooncake {
namespace tent {
namespace ub {

struct UbParams {
    // UB is opt-in until a real URMA provider and NIC topology are present.
    bool enable = false;
    std::vector<std::string> device_filter;

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

    bool enable_bandwidth_estimation = true;
    bool enable_notifications = false;

    // Probe-link parameters, read from transports/ub/probe/* below.
    ProbeParams probe;

    // Reads every UB setting from transports/ub/*. Numeric settings are
    // deliberately read as signed JSON integers first so a negative value is
    // rejected rather than silently converted to an unsigned default.
    static Status FromConfig(const Config& config, UbParams& output) {
        UbParams parsed;

        CHECK_STATUS(readBool(config, "transports/ub/enable", parsed.enable,
                              parsed.enable));
        CHECK_STATUS(readDeviceFilter(config, parsed.device_filter));
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

        CHECK_STATUS(readBool(config, "transports/ub/probe/enabled",
                              parsed.probe.enabled, parsed.probe.enabled));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/interval_ms",
                                  parsed.probe.interval_ms,
                                  parsed.probe.interval_ms));
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
        CHECK_STATUS(readStringList(config,
                                    "transports/ub/probe/device_filter",
                                    parsed.probe.device_filter));
        uint64_t source_page_size = parsed.probe.source_page_size;
        CHECK_STATUS(readNonNegative(config,
                                     "transports/ub/probe/source_page_size",
                                     source_page_size, source_page_size));
        if (source_page_size > std::numeric_limits<size_t>::max()) {
            return Status::InvalidArgument(
                "transports/ub/probe/source_page_size exceeds size_t");
        }
        parsed.probe.source_page_size =
            static_cast<size_t>(source_page_size);
        CHECK_STATUS(readStringList(config,
                                    "transports/ub/probe/peer_segments",
                                    parsed.probe.peer_segments));
        CHECK_STATUS(readPositive(config, "transports/ub/probe/peer_wait_ms",
                                  parsed.probe.peer_wait_ms,
                                  parsed.probe.peer_wait_ms));
        CHECK_STATUS(readPositive(
            config, "transports/ub/probe/descriptor_ttl_ms",
            parsed.probe.descriptor_ttl_ms,
            parsed.probe.descriptor_ttl_ms));
        CHECK_STATUS(readNonNegative(
            config, "transports/ub/probe/descriptor_refresh_ms",
            parsed.probe.descriptor_refresh_ms,
            parsed.probe.descriptor_refresh_ms));
        CHECK_STATUS(readPositive(
            config, "transports/ub/probe/peer_rpc_timeout_ms",
            parsed.probe.peer_rpc_timeout_ms,
            parsed.probe.peer_rpc_timeout_ms));
        if (parsed.probe.descriptor_ttl_ms <
            parsed.probe.peer_rpc_timeout_ms) {
            return Status::InvalidArgument(
                "transports/ub/probe/descriptor_ttl_ms must be >= "
                "transports/ub/probe/peer_rpc_timeout_ms");
        }
        CHECK_STATUS(readBool(config, "transports/ub/probe/test/periodic_all",
                              parsed.probe.test_periodic_all,
                              parsed.probe.test_periodic_all));
        CHECK_STATUS(readPositive(config,
                                  "transports/ub/probe/test/interval_ms",
                                  parsed.probe.test_interval_ms,
                                  parsed.probe.test_interval_ms));
        CHECK_STATUS(readBool(config, "transports/ub/probe/test/apply_health",
                              parsed.probe.test_apply_health,
                              parsed.probe.test_apply_health));

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

        output = std::move(parsed);
        return Status::OK();
    }

   private:
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

    static Status readStringList(const Config& config,
                                 const std::string& key,
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
                    return Status::InvalidArgument(
                        key + " entries must be strings");
                }
                parsed.push_back(entry.get<std::string>());
            }
        } else {
            return Status::InvalidArgument(
                key + " must be a string or string array");
        }

        for (const auto& entry : parsed) {
            if (entry.empty()) {
                return Status::InvalidArgument(
                    key + " entries must not be empty");
            }
        }
        output = std::move(parsed);
        return Status::OK();
    }

    static Status readDeviceFilter(const Config& config,
                                   std::vector<std::string>& output) {
        return readStringList(config, "transports/ub/device_filter", output);
    }
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PARAMS_H_
