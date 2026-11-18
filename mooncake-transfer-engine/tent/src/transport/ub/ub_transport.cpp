// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/ub_transport.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <new>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <future>

#include <glog/logging.h>

#include "tent/common/utils/string_builder.h"
#include "tent/runtime/segment.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/bootstrap.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"

#include "jetty_pool.h"
#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/attempt_gate.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/endpoint.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/identity_import.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/slice.h"
#include "tent/transport/ub/urma_adapter.h"
#include "tent/transport/ub/workers.h"

namespace mooncake::tent {
namespace {

bool filterAllows(const std::vector<std::string>& filter,
                  const ub::DeviceInfo& device) {
    if (filter.empty()) return true;
    return std::find(filter.begin(), filter.end(), device.topology_name) !=
               filter.end() ||
           std::find(filter.begin(), filter.end(), device.native_device_name) !=
               filter.end();
}

uint64_t generationSeed() {
    const auto wall = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const auto steady = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    const uint64_t seed =
        (wall ^ (steady << 19) ^ (steady >> 5)) & 0x7fffffffffffffffULL;
    return seed == 0 ? 1 : seed;
}

}  // namespace

struct UbTransport::Impl {
    explicit Impl(std::shared_ptr<ub::UrmaAdapter> injected)
        : adapter(injected ? std::move(injected)
                           : ub::createDefaultUrmaAdapter()) {}

    // Automatically collects local device identities from the provider and
    // injects them into transports/ub/device_identities when the config
    // requests physical-only selection (the RM family gate) without explicit
    // identities. Explicit configuration always wins; RC installs and
    // disabled transports are never touched. On success the discovery
    // snapshot is cached so install verifies the injected records against
    // the exact data they were collected from.
    Status autoImportDeviceIdentities(Config& config) {
        const auto existing = config.get<nlohmann::json>(
            "transports/ub/device_identities", nlohmann::json());
        if (existing.is_array() && !existing.empty()) {
            return Status::OK();
        }
        if (!config.get("transports/ub/enable", false)) {
            return Status::OK();
        }
        if (!config.get("transports/ub/physical_only", false)) {
            return Status::OK();
        }

        auto status = adapter->initialize();
        if (!status.ok()) return status;
        adapter_initialized = true;

        std::vector<ub::DeviceInfo> discovered;
        status = adapter->discoverDevices(discovered);
        if (!status.ok()) return status;

        std::vector<ub::DeviceIdentity> identities;
        status = ub::collectLocalDeviceIdentities(discovered, identities);
        if (!status.ok()) return status;

        config.set("transports/ub/device_identities",
                   ub::deviceIdentitiesToJson(identities));
        auto_import_snapshot = discovered;
        auto_import_snapshot_valid = true;
        LOG(INFO) << "UB auto-imported " << identities.size()
                  << " device identities from provider discovery; manual "
                     "transports/ub/device_identities configuration is no "
                     "longer required";
        persistAutoImportedIdentities(config);
        return Status::OK();
    }

    // Best-effort write-back so the collected identities survive restarts
    // and stay visible to external tooling using the exact manual schema.
    // Only configs loaded from a file (MC_TENT_CONF path) are persisted;
    // programmatic configs (for example the benchmark's in-memory config)
    // rely on the injected in-memory records alone. A write failure never
    // fails the install: the injected records already remove the need for
    // manual editing in this process.
    void persistAutoImportedIdentities(const Config& config) {
        const auto source_file =
            config.get<std::string>("config_source_file", "");
        if (source_file.empty()) return;
        auto status = config.saveFile(source_file);
        if (!status.ok()) {
            LOG(WARNING) << "UB auto-import could not persist device "
                            "identities to "
                         << source_file << ": " << status.ToString();
        } else {
            LOG(INFO) << "UB auto-import persisted device identities to "
                      << source_file;
        }
    }

    Status install(const std::string& segment_name,
                   std::shared_ptr<ControlService> control,
                   std::shared_ptr<Topology> topology,
                   std::shared_ptr<Config> config) {
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        if (installed.load(std::memory_order_acquire)) {
            return Status::InvalidArgument(
                "UB transport has already been installed" LOC_MARK);
        }
        if (shutting_down.load(std::memory_order_acquire) || workers ||
            adapter_initialized) {
            return Status::InvalidArgument(
                "A previous UB uninstall has not drained yet" LOC_MARK);
        }
        if (segment_name.empty() || !control || !topology) {
            return Status::InvalidArgument(
                "UB install requires segment, control service and "
                "topology" LOC_MARK);
        }
        if (!adapter || !adapter->available()) {
            return Status::DeviceNotFound(
                "A real or injected URMA adapter is unavailable" LOC_MARK);
        }
        if (!config) config = std::make_shared<Config>();

        // RM auto-import pre-pass: when physical-only selection is requested
        // without explicit identities, collect them from the provider and
        // inject them into the config before the FromConfig gate, so RM
        // installs no longer require hand-edited device_identities. The
        // pre-pass may initialize the adapter; every early failure below
        // must roll that back because failInstall() is not wired yet.
        auto abort_after_auto_import = [this](Status failure) {
            if (adapter_initialized) {
                auto shutdown_status = adapter->shutdown();
                adapter_initialized = false;
                if (!shutdown_status.ok()) {
                    LOG(WARNING)
                        << "UB adapter shutdown after a failed auto-import "
                           "install returned "
                        << shutdown_status.ToString();
                }
            }
            return failure;
        };
        auto import_status = autoImportDeviceIdentities(*config);
        if (!import_status.ok()) {
            return abort_after_auto_import(import_status);
        }

        ub::UbParams parsed;
        auto parse_status = ub::UbParams::FromConfig(*config, parsed);
        if (!parse_status.ok()) return abort_after_auto_import(parse_status);
        if (!parsed.enable) {
            return abort_after_auto_import(Status::InvalidArgument(
                "UB transport is disabled by configuration" LOC_MARK));
        }
        if (parsed.enable_notifications) {
            return abort_after_auto_import(Status::NotImplemented(
                "UB notifications are not supported by protocol version "
                "1" LOC_MARK));
        }
        ub::UbRuntimeMode selected_mode = ub::UbRuntimeMode::kRcLegacy;
        auto mode_status = ub::resolveUbRuntimeMode(parsed, selected_mode);
        if (!mode_status.ok()) return abort_after_auto_import(mode_status);
        LOG(INFO) << "UB runtime selection: mode="
                  << ub::ubRuntimeModeName(selected_mode)
                  << " profile="
                  << ub::ubRuntimeProfileName(parsed.runtime_profile)
                  << " trans_mode="
                  << ub::ubTransportModeName(parsed.trans_mode)
                  << " jetty_pool="
                  << (parsed.jetty_pool.enable ? "on" : "off")
                  << " probe=" << (parsed.probe.enabled ? "on" : "off")
                  << " force_disable="
                  << (parsed.jetty_pool.force_disable ? "true" : "false");

        shutting_down.store(false, std::memory_order_release);
        local_segment_name = segment_name;
        metadata = std::move(control);
        local_topology = std::move(topology);
        conf = std::move(config);
        params = parsed;
        runtime_mode = selected_mode;

        if (!adapter_initialized) {
            auto init_status = adapter->initialize();
            if (!init_status.ok()) return failInstall(init_status);
            adapter_initialized = true;
        }

        // The auto-import pre-pass already took a discovery snapshot when it
        // injected identities; reuse it so the injected records are verified
        // against exactly the same snapshot they were collected from.
        std::vector<ub::DeviceInfo> discovered;
        Status status;
        if (auto_import_snapshot_valid) {
            discovered = auto_import_snapshot;
        } else {
            status = adapter->discoverDevices(discovered);
            if (!status.ok()) return failInstall(status);
        }
        status = ub::resolveDeviceKindIdentities(params.device_identities,
                                                 discovered);
        if (!status.ok()) return failInstall(status);

        const bool explicit_filter = !params.device_filter.empty();
        const auto preferred_devices =
            params.physical_only
                ? ub::physicalDevices(discovered)
                : ub::preferBondingDevicesIfPresent(discovered,
                                                    explicit_filter);
        const bool prefer_bonding =
            !params.physical_only && !explicit_filter &&
            std::any_of(discovered.begin(), discovered.end(),
                        [](const ub::DeviceInfo& device) {
                            return ub::isBondingDevice(device);
                        });
        std::unordered_set<std::string> preferred_topology_names;
        preferred_topology_names.reserve(preferred_devices.size());
        for (const auto& device : preferred_devices) {
            preferred_topology_names.insert(device.topology_name);
        }

        std::unordered_map<std::string, ub::DeviceInfo> by_topology_name;
        std::unordered_map<Topology::NicID, uint32_t> jfc_depth_by_device;
        for (auto& device : discovered) {
            by_topology_name.emplace(device.topology_name, std::move(device));
        }

        // RM capability gate (legacy and pooled): every selectable device
        // must be a trusted PHYSICAL device whose provider capabilities pass
        // the LocalSendProfile derivation (RM transport mode, RM/CTP support,
        // priority bind mask). The gate runs before any native resource
        // (context, JFC, Jetty) is created and fails closed; there is never a
        // silent fallback to RC or bonding.
        const bool rm_runtime =
            runtime_mode == ub::UbRuntimeMode::kRmLegacy ||
            runtime_mode == ub::UbRuntimeMode::kPooledRm;
        uint32_t rm_capability_bind_tp_type_mask = 0;
        uint8_t rm_capability_priority = ub::JettyOptions{}.priority;
        if (rm_runtime) {
            for (const auto& [name, device] : by_topology_name) {
                if (preferred_topology_names.find(device.topology_name) ==
                    preferred_topology_names.end()) {
                    continue;
                }
                if (!filterAllows(params.device_filter, device)) continue;
                if (device.kind != ub::DeviceKind::PHYSICAL) {
                    return failInstall(Status::NotImplemented(
                        "RM runtime requires trusted PHYSICAL devices; "
                        "device " +
                        device.native_device_name + " is not physical"
                        LOC_MARK));
                }
                ub::LocalSendProfile profile;
                const auto profile_status = ub::buildLocalSendProfile(
                    device,
                    static_cast<uint8_t>(ub::JettyOptions{}.priority),
                    profile);
                if (!profile_status.ok()) {
                    return failInstall(Status::NotImplemented(
                        "RM capability gate failed for device " +
                        device.native_device_name + ": " +
                        profile_status.ToString() + LOC_MARK));
                }
                rm_capability_bind_tp_type_mask = profile.bind_tp_type_mask;
                rm_capability_priority = profile.priority;
            }
        }

        std::vector<std::string> preferred_names;
        std::vector<std::string> skipped_names;

        for (size_t id = 0; id < local_topology->getNicCount(); ++id) {
            const auto* nic = local_topology->getNicEntry(static_cast<int>(id));
            if (!nic || nic->type != Topology::NIC_UB) continue;
            auto found = by_topology_name.find(nic->name);
            if (found == by_topology_name.end() || !found->second.active) {
                continue;
            }
            if (!filterAllows(params.device_filter, found->second)) continue;
            if (preferred_topology_names.find(found->second.topology_name) ==
                preferred_topology_names.end()) {
                skipped_names.push_back(found->second.native_device_name);
                continue;
            }

            ub::JfcOptions jfc_options;
            const auto& device_caps = found->second.capabilities;
            if (rm_runtime) {
                // RM creates an RM receive-side JFR; it never falls
                // back to RC.
                jfc_options.trans_mode = ub::kUrmaTransportModeRm;
            }
            if (device_caps.max_jfc_depth != 0) {
                jfc_options.depth =
                    std::min(jfc_options.depth, device_caps.max_jfc_depth);
                jfc_options.receiver_depth = std::min(
                    jfc_options.receiver_depth, device_caps.max_jfc_depth);
            }
            if (device_caps.max_jfr_depth != 0) {
                jfc_options.receiver_depth = std::min(
                    jfc_options.receiver_depth, device_caps.max_jfr_depth);
            }
            auto context = std::make_shared<ub::UbContext>(
                static_cast<Topology::NicID>(id), found->second, adapter);
            status = context->initialize(params.jfc_per_context, jfc_options);
            if (!status.ok()) {
                if (context->state() == ub::UbContext::State::kDraining) {
                    // Initialization produced a native handle whose cleanup
                    // also failed. Adopt it into the transport graph and abort
                    // installation so uninstall can retry without a leak.
                    contexts.push_back(std::move(context));
                    return failInstall(status);
                }
                LOG(WARNING) << "Disable UB device " << nic->name << ": "
                             << status.ToString();
                continue;
            }
            preferred_names.push_back(found->second.native_device_name);
            context_by_topology_id.emplace(static_cast<int>(id), context);
            context_by_topology_name.emplace(nic->name, context);
            jfc_depth_by_device.emplace(static_cast<Topology::NicID>(id),
                                        jfc_options.depth);
            contexts.push_back(std::move(context));
        }

        {
            std::ostringstream selection_log;
            if (params.physical_only) {
                selection_log << "UB device selection: mode=physical-only";
                if (explicit_filter) selection_log << "+explicit-filter";
            } else if (explicit_filter) {
                selection_log << "UB device selection: mode=explicit-filter";
            } else if (prefer_bonding) {
                selection_log
                    << "UB device selection: mode=auto-prefer-bonding";
            } else {
                selection_log << "UB device selection: mode=all-devices";
            }
            if (!preferred_names.empty()) {
                selection_log << " preferred=[";
                for (size_t i = 0; i < preferred_names.size(); ++i) {
                    if (i != 0) selection_log << ", ";
                    selection_log << preferred_names[i];
                }
                selection_log << "]";
            }
            if (!skipped_names.empty()) {
                selection_log << " skipped=[";
                for (size_t i = 0; i < skipped_names.size(); ++i) {
                    if (i != 0) selection_log << ", ";
                    selection_log << skipped_names[i];
                }
                selection_log << "]";
            }
            LOG(INFO) << selection_log.str();
        }

        if (contexts.empty()) {
            // Name the devices that device selection excluded (e.g. bonding
            // devices under physical-only RM Legacy) so an exhausted-
            // selection failure is directly attributable instead of a bare
            // "no context" message.
            std::string reason = "No UB context initialized successfully";
            if (!skipped_names.empty()) {
                reason += "; skipped by device selection:";
                for (size_t i = 0; i < skipped_names.size(); ++i) {
                    reason += i == 0 ? " " : ", ";
                    reason += skipped_names[i];
                }
            }
            return failInstall(
                Status::DeviceNotFound(reason + LOC_MARK));
        }

        size_t safe_slice_size = params.slice_size;
        ub::JettyOptions jetty_options;
        if (rm_runtime) {
            jetty_options.trans_mode = ub::kUrmaTransportModeRm;
            jetty_options.bind_tp_type_mask =
                rm_capability_bind_tp_type_mask;
            jetty_options.priority = rm_capability_priority;
        }
        for (const auto& context : contexts) {
            const auto& device_caps = context->deviceInfo().capabilities;
            auto clamp_slice = [&safe_slice_size](uint64_t limit) {
                if (limit != 0) {
                    safe_slice_size = static_cast<size_t>(
                        std::min<uint64_t>(safe_slice_size, limit));
                }
            };
            clamp_slice(device_caps.max_read_size);
            clamp_slice(device_caps.max_write_size);
            if (device_caps.max_jetty_depth != 0) {
                jetty_options.depth =
                    std::min(jetty_options.depth, device_caps.max_jetty_depth);
            }
            if (device_caps.max_send_sge != 0) {
                jetty_options.max_sge = static_cast<uint8_t>(std::min<uint32_t>(
                    jetty_options.max_sge, device_caps.max_send_sge));
            }
            if (device_caps.max_remote_sge != 0) {
                jetty_options.max_sge = static_cast<uint8_t>(std::min<uint32_t>(
                    jetty_options.max_sge, device_caps.max_remote_sge));
            }
        }
        if (safe_slice_size == 0 || jetty_options.depth == 0 ||
            jetty_options.max_sge == 0) {
            return failInstall(Status::InvalidArgument(
                "UB device capabilities cannot support the configured data "
                "path" LOC_MARK));
        }
        if (safe_slice_size != params.slice_size) {
            LOG(WARNING) << "Clamp UB slice_size from " << params.slice_size
                         << " to device limit " << safe_slice_size;
            params.slice_size = safe_slice_size;
        }

        buffers = std::make_unique<ub::UbBufferManager>(adapter, contexts);
        endpoints = std::make_unique<ub::EndpointStore>(
            adapter, params.max_endpoints, params.jetty_per_endpoint,
            jetty_options);
        // Simple send-Jetty reuse (RM Legacy only): attach a per-context idle
        // pool so endpoint incarnations lease Jettys LIFO instead of always
        // creating and deleting them. RC installs never attach a pool, so the
        // legacy create/delete path is unchanged.
        if (runtime_mode == ub::UbRuntimeMode::kRmLegacy &&
            params.jetty_reuse.enable) {
            for (auto& context : contexts) {
                context->attachJettyPool(
                    std::make_shared<ub::UbSendJettyPool>(
                        adapter, context->handle(), context->jfcs(),
                        jetty_options));
            }
        }
        // rm_pool data path (C01/C05/C06): attach one SendJettyPool per
        // context plus the process-wide admission ledgers. Every gate below
        // fails install closed; nothing is attached partially. The pool is
        // the only producer of pooled members, and the workers' data path is
        // the only consumer: a context without an attached pool never enters
        // the pooled branch.
        if (runtime_mode == ub::UbRuntimeMode::kPooledRm) {
            native_budget = std::make_shared<ub::NativeBudget>();
            std::unordered_set<std::string> configured_devices;
            for (const auto& context : contexts) {
                const auto& device = context->deviceInfo();
                const std::string device_key = ub::makePhysicalDeviceIdentity(
                    device.native_device_path, device.eid);
                if (!configured_devices.insert(device_key).second) continue;
                const uint64_t verified = device.capabilities.max_jetty;
                uint64_t configured =
                    params.native_budget.max_jetty_per_device;
                uint64_t reserve = params.native_budget.replacement_reserve;
                if (configured == 0) {
                    // Derive the budget from the verified limit while keeping
                    // headroom for the replacement reserve; the budget
                    // ceiling (configured + reserve) must never exceed the
                    // verified device limit.
                    configured =
                        verified > reserve ? verified - reserve : verified;
                    if (verified <= reserve) reserve = 0;
                }
                if (configured == 0) {
                    return failInstall(Status::InvalidArgument(
                        "rm_pool requires a positive native Jetty budget for "
                        "device " +
                        device_key + LOC_MARK));
                }
                status =
                    native_budget->configure(device_key, verified, configured,
                                             reserve);
                if (!status.ok()) return failInstall(status);
            }
            task_permits = std::make_shared<ub::TaskPermitLedger>(
                params.send_pool.task_max_inflight_attempts);
            attempt_gate = std::make_shared<ub::AttemptGate>();
            for (auto& context : contexts) {
                const auto& device = context->deviceInfo();
                const uint32_t depth =
                    jfc_depth_by_device.at(context->topologyId());
                auto credits = std::make_shared<ub::JfcCreditLedger>();
                for (size_t jfc_index = 0; jfc_index < context->jfcs().size();
                     ++jfc_index) {
                    const uint64_t capacity =
                        depth > params.send_pool.jfc_credit_reserve
                            ? static_cast<uint64_t>(depth) -
                                  params.send_pool.jfc_credit_reserve
                            : 0;
                    if (capacity == 0) {
                        return failInstall(Status::InvalidArgument(
                            "rm_pool JFC credit capacity is exhausted on "
                            "device " +
                            device.native_device_name + LOC_MARK));
                    }
                    status = credits->install(jfc_index, capacity);
                    if (!status.ok()) return failInstall(status);
                }
                ub::SendJettyPoolOptions pool_options;
                pool_options.desired_serving =
                    params.send_pool.desired_per_context;
                pool_options.max_creating =
                    params.send_pool.max_creating_per_key;
                pool_options.background_refill =
                    params.send_pool.background_refill;
                pool_options.background_delete =
                    params.send_pool.background_delete;
                pool_options.refill_interval_ms =
                    params.send_pool.refill_interval_ms;
                pool_options.jetty_options = jetty_options;
                context->attachPooledSendPool(
                    std::make_shared<ub::SendJettyPool>(
                        adapter, context->handle(), context->jfcs(),
                        native_budget,
                        ub::makePhysicalDeviceIdentity(
                            device.native_device_path, device.eid),
                        credits, pool_options));
            }
        }
        ub::RailMonitorConfig rail_config;
        rail_config.cooldown_ns =
            static_cast<uint64_t>(params.endpoint_cooldown_ms) * 1'000'000ULL;
        rails = std::make_unique<ub::RailMonitor>(rail_config);
        auto saturatedProduct = [](uint64_t lhs, uint64_t rhs) {
            if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
                return std::numeric_limits<uint64_t>::max();
            }
            return lhs * rhs;
        };
        const uint64_t path_wrs =
            saturatedProduct(jetty_options.depth, params.jetty_per_endpoint);
        ub::QuotaLimits path_limits{
            saturatedProduct(path_wrs, params.slice_size), path_wrs};
        quota = std::make_unique<ub::QuotaManager>(path_limits, path_limits);
        for (const auto& context : contexts) {
            const auto depth = jfc_depth_by_device.at(context->topologyId());
            const uint64_t device_wrs =
                saturatedProduct(depth, context->jfcs().size());
            (void)quota->setDeviceLimits(
                context->topologyId(),
                ub::QuotaLimits{saturatedProduct(device_wrs, params.slice_size),
                                device_wrs});
        }

        status = publishLocalDevices();
        if (!status.ok()) return failInstall(status);

        metadata->setBootstrapUbCallback(
            [this](const UbBootstrapDesc& request, UbBootstrapDesc& response) {
                return onBootstrap(request, response);
            });
        callback_installed = true;

        workers = std::make_unique<ub::UbWorkers>(
            adapter, contexts, local_topology, &metadata->segmentManager(),
            buffers.get(), rails.get(), quota.get(), params,
            [this](const ub::EndpointResolveRequest& request,
                   std::shared_ptr<ub::UbEndpoint>& endpoint) {
                return resolveEndpoint(request, endpoint);
            },
            [this](const std::shared_ptr<ub::UbEndpoint>& endpoint) {
                if (endpoints) (void)endpoints->retire(endpoint);
            },
            [this](Topology::NicID local_topology_id) {
                if (!endpoints) {
                    return Status::InternalError(
                        "UB endpoint store is unavailable during device "
                        "failure cleanup" LOC_MARK);
                }
                return endpoints->retireLocalDevice(local_topology_id);
            },
            task_permits, attempt_gate);
        status = workers->start();
        if (!status.ok()) return failInstall(status);

        installed.store(true, std::memory_order_release);
        return Status::OK();
    }

    Status uninstall() {
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        return shutdownUnlocked();
    }

    Status failInstall(Status failure) {
        // Install now fails fast, so a failed rollback can leave native
        // resources owned by this object. Surface that instead of silently
        // discarding it, otherwise the leak is only visible as a later
        // install/uninstall failure.
        auto cleanup = shutdownUnlocked();
        if (!cleanup.ok()) {
            LOG(WARNING)
                << "UB transport rollback after a failed install could "
                   "not release every native resource: "
                << cleanup.ToString();
        }
        return failure;
    }

    Status shutdownUnlocked() {
        installed.store(false, std::memory_order_release);
        shutting_down.store(true, std::memory_order_release);

        // Callback replacement waits for a currently executing UB bootstrap
        // handler, fencing all control-plane access before resources retire.
        if (callback_installed && metadata) {
            metadata->setBootstrapUbCallback({});
            callback_installed = false;
        }
        if (workers) {
            auto status = workers->stop();
            if (!status.ok()) {
                // A failed native drain fence is not permission to destroy
                // memory registrations, JFCs, Contexts, or the adapter. Keep
                // the complete ownership graph alive so uninstall can be
                // retried after a late completion/provider recovery. The
                // pooled background executors stop anyway: no new Jetty may
                // be created while the transport is shutting down, and every
                // later retirement destroys synchronously.
                for (auto& context : contexts) {
                    if (context && context->pooledSendPool()) {
                        context->pooledSendPool()->beginStop();
                    }
                }
                return status;
            }
            workers.reset();
        }
        if (endpoints) {
            auto status = endpoints->clear();
            if (!status.ok()) return status;
            endpoints.reset();
        }
        if (buffers) {
            auto status = buffers->clear();
            if (!status.ok()) return status;
            buffers.reset();
        }
        for (auto it = contexts.rbegin(); it != contexts.rend(); ++it) {
            if (!*it) continue;
            auto status = (*it)->shutdown();
            if (!status.ok()) {
                // Keep the entire context vector and both lookup maps until
                // every JFC and Context has been released. Some contexts may
                // already be closed, but their idempotent shutdown keeps the
                // ownership graph intact while the failed handle is retried.
                return status;
            }
        }
        contexts.clear();
        context_by_topology_id.clear();
        context_by_topology_name.clear();
        // The workers stopped first (settling every pooled lease) and the
        // contexts just drained their pools; the admission components can be
        // released now.
        native_budget.reset();
        task_permits.reset();
        attempt_gate.reset();
        rails.reset();
        quota.reset();
        if (adapter_initialized && adapter) {
            auto status = adapter->shutdown();
            if (!status.ok()) {
                // A failed provider shutdown remains an initialized adapter
                // for lifecycle purposes. Do not permit reinstall or discard
                // it; a later uninstall retries the provider operation.
                return status;
            }
            adapter_initialized = false;
        }
        metadata.reset();
        local_topology.reset();
        conf.reset();
        local_segment_name.clear();
        auto_import_snapshot.clear();
        auto_import_snapshot_valid = false;
        shutting_down.store(false, std::memory_order_release);
        return Status::OK();
    }

    Status publishLocalDevices() {
        auto& manager = metadata->segmentManager();
        CHECK_STATUS(
            manager.updateLocal([this](SegmentDesc& segment) -> Status {
                if (segment.type != SegmentType::Memory) {
                    return Status::InvalidMetadataType(
                        "Local segment is not memory-backed" LOC_MARK);
                }
                auto& detail = std::get<MemorySegmentDesc>(segment.detail);
                detail.transport_attrs[TransportType::UB] = nlohmann::json{
                    {"schema_version", 1},
                    {"protocol", "urma"},
                    {"notifications", false}}.dump();
                std::unordered_set<std::string> existing;
                for (const auto& device : detail.devices) {
                    existing.insert(device.name);
                }
                for (const auto& context : contexts) {
                    if (!context || !context->active()) continue;
                    if (existing.insert(context->deviceInfo().topology_name)
                            .second) {
                        DeviceDesc device;
                        device.name = context->deviceInfo().topology_name;
                        device.lid = 0;
                        device.gid.clear();
                        device.transport_attrs[TransportType::UB] =
                            ub::encodeDeviceMetadata(context->topologyId(),
                                                     context->deviceInfo());
                        detail.devices.push_back(std::move(device));
                    }
                }
                return Status::OK();
            }));
        return manager.synchronizeLocal();
    }

    void recordReadyEndpoint(const std::shared_ptr<ub::UbEndpoint>& endpoint) {
        if (!endpoint || !endpoint->ready() || !rails) return;

        // RailMonitor keeps the per-rail generation watermark and ignores
        // generations it has already seen, so this is safe to call from every
        // resolve as well as from converging or retried rebuild paths. Keeping
        // the watermark there also means no peer-keyed state accumulates here
        // across peer restarts, and the posting hot path takes only
        // RailMonitor's lock instead of a second transport-wide one.
        const auto& key = endpoint->key();
        rails->recordEndpointRebuild(
            ub::UbPostPath{key.local_topology_id, key.remote_segment_id,
                           key.remote_topology_id, endpoint->generation()});
    }

    Status resolveEndpoint(const ub::EndpointResolveRequest& request,
                           std::shared_ptr<ub::UbEndpoint>& endpoint) {
        endpoint.reset();
        if (shutting_down.load(std::memory_order_acquire) || !endpoints ||
            !request.local_context || !request.remote_segment) {
            return Status::InvalidArgument(
                "UB transport is shutting down or route is invalid" LOC_MARK);
        }
        const auto* remote_nic =
            request.remote_segment->getMemory().topology.getNicEntry(
                request.remote_topology_id);
        if (!remote_nic || remote_nic->type != Topology::NIC_UB) {
            return Status::DeviceNotFound(
                "Remote segment does not advertise a UB topology "
                "device" LOC_MARK);
        }
        const std::string peer_path =
            MakeNicPath(request.remote_segment->name, remote_nic->name);
        ub::UbEndpointKey key{request.local_context->topologyId(),
                              request.remote_segment_id,
                              request.remote_topology_id, peer_path};
        CHECK_STATUS(
            endpoints->getOrCreate(key, request.local_context, endpoint));
        if (endpoint->ready()) {
            recordReadyEndpoint(endpoint);
            return Status::OK();
        }

        const std::string local_path =
            MakeNicPath(local_segment_name,
                        request.local_context->deviceInfo().topology_name);
        UbBootstrapDesc bootstrap;
        Status status;
        const bool rm_endpoint_runtime =
            runtime_mode == ub::UbRuntimeMode::kRmLegacy ||
            runtime_mode == ub::UbRuntimeMode::kPooledRm;
        if (rm_endpoint_runtime) {
            status = endpoint->makeBootstrapDescV2(
                local_segment_name, local_path, peer_path,
                currentSegmentGeneration(), node_incarnation, topology_epoch,
                bootstrap);
        } else {
            status = endpoint->makeBootstrapDesc(
                local_segment_name, local_path, peer_path,
                request.segment_generation, bootstrap);
        }
        if (!status.ok()) {
            (void)endpoints->retire(endpoint);
            endpoint.reset();
            return status;
        }
        UbBootstrapDesc response;
        status = ControlClient::bootstrapUb(
            request.remote_segment->rpc_server_addr, bootstrap, response);
        if (status.ok()) {
            if (rm_endpoint_runtime) {
                // Trusted anchor for the peer identity: the device metadata
                // the peer published into its registered segment descriptor
                // at install time (validated remote segment metadata), never
                // the peer's self-asserted bootstrap fields. Every field the
                // manual flow used to configure is validated here; any
                // mismatch or missing metadata fails closed before import.
                ub::UbExpectedPeerIdentity expected;
                status = ub::anchorPeerIdentityFromSegment(
                    *request.remote_segment, request.remote_topology_id,
                    expected);
                if (status.ok()) {
                    status = endpoint->bindRm(response, expected);
                }
            } else {
                status = endpoint->bind(response);
            }
        }
        if (!status.ok()) {
            (void)endpoints->retire(endpoint);
            endpoint.reset();
            return status;
        }
        recordReadyEndpoint(endpoint);
        return Status::OK();
    }

    int onBootstrap(const UbBootstrapDesc& request, UbBootstrapDesc& response) {
        if (shutting_down.load(std::memory_order_acquire) || !endpoints) {
            response.reply_msg = "UB transport is shutting down";
            return -1;
        }
        // The mode gate precedes endpoint eviction, Jetty creation and native
        // import/bind side effects. RC remains on v1; RM requires a strict v2
        // request with no reply_msg.
        const bool rm_runtime =
            runtime_mode == ub::UbRuntimeMode::kRmLegacy ||
            runtime_mode == ub::UbRuntimeMode::kPooledRm;
        auto gate_status = ub::gateUbBootstrapForMode(
            rm_runtime ? ub::UbBootstrapMode::RM : ub::UbBootstrapMode::RC,
            request);
        if (!gate_status.ok()) {
            response = request;
            response.reply_msg = gate_status.ToString();
            return -1;
        }
        const std::string local_name =
            getNicNameFromNicPath(request.peer_nic_path);
        auto context_it = context_by_topology_name.find(local_name);
        if (local_name.empty() ||
            context_it == context_by_topology_name.end()) {
            response.reply_msg = "UB bootstrap selected an unknown local NIC";
            return -1;
        }
        if (rm_runtime) {
            return onBootstrapRm(request, response, context_it->second);
        }
        if (request.local_device_id < 0 || request.local_eid.empty() ||
            request.jetty_ids.empty() || request.endpoint_generation == 0) {
            response.reply_msg = "UB bootstrap request is incomplete";
            return -1;
        }

        ub::UbEndpointKey key{context_it->second->topologyId(),
                              LOCAL_SEGMENT_ID, request.local_device_id,
                              request.local_nic_path};
        std::shared_ptr<ub::UbEndpoint> endpoint;
        auto status = endpoints->getOrCreate(key, context_it->second, endpoint);
        if (status.ok()) status = endpoint->bind(request);
        if (status.ok()) {
            status = endpoint->makeBootstrapDesc(
                local_segment_name, request.peer_nic_path,
                request.local_nic_path, currentSegmentGeneration(), response);
        }
        if (status.ok()) {
            recordReadyEndpoint(endpoint);
        }
        if (!status.ok()) {
            if (endpoint) (void)endpoints->retire(endpoint);
            response = UbBootstrapDesc{};
            response.reply_msg = status.ToString();
            return -1;
        }
        return 0;
    }

    int onBootstrapRm(const UbBootstrapDesc& request, UbBootstrapDesc& response,
                      const ub::UbContextPtr& context) {
        // Server-side consumer of the peer's v2 request descriptor. The
        // trusted identity anchor is the requester's registered segment
        // metadata (the device entry its publishLocalDevices published at
        // install time), never the self-asserted request fields; every
        // strict v2 validation and the RM target profile checks still run
        // in bindRm before any native import.
        SegmentDescRef requester_segment;
        Status anchor_status;
        if (request.segment_name == local_segment_name) {
            // Self/loopback bootstrap: the local snapshot is authoritative
            // and no registry round-trip is required.
            requester_segment = metadata->segmentManager().getLocal();
        } else {
            // fetching the requester's descriptor goes to the metadata
            // registry over RPC, this handler runs on an RPC thread where
            // nested synchronous RPC is forbidden. so hop to a worker
            // thread for the fetch and block this handler on the result.
            auto fetch = std::async(
                std::launch::async,
                [this, name = request.segment_name]() {
                    SegmentDescRef seg;
                    auto st = metadata->segmentManager().getRemote(seg, name);
                    return std::make_pair(st, std::move(seg));
                }
            );
            auto fetched = fetch.get();
            anchor_status = fetched.first;
            requester_segment = std::move(fetched.second);
        }
        if (anchor_status.ok() && !requester_segment) {
            anchor_status = Status::InvalidArgument(
                "UB RM bootstrap cannot resolve the requester segment '" +
                request.segment_name + "'" LOC_MARK);
        }
        ub::UbExpectedPeerIdentity expected;
        if (anchor_status.ok()) {
            anchor_status = ub::anchorPeerIdentityFromSegment(
                *requester_segment, request.local_device_id, expected);
        }
        if (!anchor_status.ok()) {
            response = UbBootstrapDesc{};
            response.protocol_version = 2;
            response.reply_msg =
                "UB RM bootstrap rejected: requester identity could not be "
                "anchored from registered segment metadata: " +
                anchor_status.ToString();
            return -1;
        }

        ub::UbEndpointKey key{context->topologyId(), LOCAL_SEGMENT_ID,
                              request.local_device_id, request.local_nic_path};
        std::shared_ptr<ub::UbEndpoint> endpoint;
        auto status = endpoints->getOrCreate(key, context, endpoint);
        if (status.ok()) status = endpoint->bindRm(request, expected);
        if (status.ok()) {
            status = endpoint->makeBootstrapDescV2(
                local_segment_name, request.peer_nic_path,
                request.local_nic_path, currentSegmentGeneration(),
                node_incarnation, topology_epoch, response);
        }
        if (status.ok()) {
            recordReadyEndpoint(endpoint);
        }
        if (!status.ok()) {
            if (endpoint) (void)endpoints->retire(endpoint);
            response = UbBootstrapDesc{};
            response.protocol_version = 2;
            response.reply_msg = status.ToString();
            return -1;
        }
        return 0;
    }

    uint64_t currentSegmentGeneration() const {
        // Buffer generations are carried in buffer metadata. The bootstrap
        // field is a peer restart/refresh hint and must still be nonzero even
        // before the first user buffer is registered.
        return segment_generation.load(std::memory_order_relaxed);
    }

    mutable std::mutex lifecycle_mutex;
    std::shared_ptr<ub::UrmaAdapter> adapter;
    bool adapter_initialized{false};
    // Discovery snapshot taken by the auto-import pre-pass; reused by
    // install so injected identities are verified against their source.
    std::vector<ub::DeviceInfo> auto_import_snapshot;
    bool auto_import_snapshot_valid{false};
    bool callback_installed{false};
    std::atomic<bool> installed{false};
    std::atomic<bool> shutting_down{false};
    std::atomic<uint64_t> segment_generation{generationSeed()};
    ub::UbParams params;
    // Resolved once during install and never switched at runtime. RM Legacy
    // carries the node incarnation and topology epoch published in bootstrap
    // v2 descriptors; both are transport-lifetime stable in the P1 baseline.
    ub::UbRuntimeMode runtime_mode{ub::UbRuntimeMode::kRcLegacy};
    const uint64_t node_incarnation{generationSeed()};
    const uint64_t topology_epoch{generationSeed()};
    std::string local_segment_name;
    std::shared_ptr<ControlService> metadata;
    std::shared_ptr<Topology> local_topology;
    std::shared_ptr<Config> conf;
    std::vector<ub::UbContextPtr> contexts;
    std::unordered_map<Topology::NicID, ub::UbContextPtr>
        context_by_topology_id;
    std::unordered_map<std::string, ub::UbContextPtr> context_by_topology_name;
    std::unique_ptr<ub::UbBufferManager> buffers;
    std::unique_ptr<ub::EndpointStore> endpoints;
    std::unique_ptr<ub::RailMonitor> rails;
    std::unique_ptr<ub::QuotaManager> quota;
    std::unique_ptr<ub::UbWorkers> workers;
    // rm_pool admission and completion-gate components, owned by the install
    // generation and shared with the workers. Reset during shutdown after the
    // workers stop and the contexts (pool owners) are released.
    std::shared_ptr<ub::NativeBudget> native_budget;
    std::shared_ptr<ub::TaskPermitLedger> task_permits;
    std::shared_ptr<ub::AttemptGate> attempt_gate;
};

UbTransport::UbTransport(std::shared_ptr<ub::UrmaAdapter> adapter)
    : impl_(std::make_unique<Impl>(std::move(adapter))) {}

UbTransport::~UbTransport() {
    auto status = uninstall();
    if (!status.ok()) {
        // There is no caller left to retry an explicit uninstall. Leaking the
        // still-live ownership graph is the only safe failure mode: destroying
        // joinable pollers or registered memory after a failed device fence
        // would terminate the process or permit DMA-after-free. The OS/provider
        // reclaims these resources at process exit.
        LOG(ERROR) << "Preserve undrained UB resources during destruction: "
                   << status.ToString();
        (void)impl_.release();
    }
}

Status UbTransport::install(std::string& local_segment_name,
                            std::shared_ptr<ControlService> metadata,
                            std::shared_ptr<Topology> local_topology,
                            std::shared_ptr<Config> conf) {
    auto status = impl_->install(local_segment_name, std::move(metadata),
                                 std::move(local_topology), std::move(conf));
    if (status.ok()) {
        caps = Capabilities{};
        caps.dram_to_dram = true;
    }
    return status;
}

Status UbTransport::uninstall() {
    auto status = impl_->uninstall();
    caps = Capabilities{};
    return status;
}

Status UbTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    batch = nullptr;
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->installed.load(std::memory_order_acquire)) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto* ub_batch = new (std::nothrow) UbSubBatch();
    if (!ub_batch) {
        return Status::InternalError(
            "Unable to allocate UB sub-batch" LOC_MARK);
    }
    ub_batch->max_size = max_size;
    ub_batch->task_list.reserve(max_size);
    batch = ub_batch;
    return Status::OK();
}

Status UbTransport::freeSubBatch(SubBatchRef& batch) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument("Invalid UB sub-batch" LOC_MARK);
    }
    ub_batch->task_list.clear();
    delete ub_batch;
    batch = nullptr;
    return Status::OK();
}

Status UbTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument("Invalid UB sub-batch" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->installed.load(std::memory_order_acquire) || !impl_->workers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    if (request_list.size() > ub_batch->max_size - ub_batch->task_list.size()) {
        return Status::TooManyRequests("Exceed UB batch capacity" LOC_MARK);
    }
    for (const auto& request : request_list) {
        const auto local_address = reinterpret_cast<uintptr_t>(request.source);
        if (!request.source || request.length == 0 ||
            request.length >
                std::numeric_limits<uint64_t>::max() - request.target_offset ||
            request.length >
                std::numeric_limits<uintptr_t>::max() - local_address) {
            return Status::InvalidArgument(
                "UB request range is empty or overflows" LOC_MARK);
        }
    }

    const auto notify_progress = ub_batch->notify_progress;
    const auto progress_batch_id = ub_batch->progress_batch_id;
    std::vector<ub::UbTask::Ptr> new_tasks;
    new_tasks.reserve(request_list.size());
    for (const auto& request : request_list) {
        const size_t slice_count =
            request.length / impl_->params.slice_size +
            (request.length % impl_->params.slice_size != 0 ? 1 : 0);
        if (slice_count > impl_->params.max_slices_per_task) {
            return Status::InvalidArgument(
                "UB request exceeds "
                "transports/ub/max_slices_per_task" LOC_MARK);
        }
        auto task = ub::UbTask::create(
            request,
            [notify_progress, progress_batch_id](const TransferStatus&) {
                if (notify_progress) notify_progress(progress_batch_id);
            });
        size_t offset = 0;
        while (offset < request.length) {
            const size_t length =
                std::min(impl_->params.slice_size, request.length - offset);
            ub::UbSliceSpec spec;
            spec.local_address = static_cast<char*>(request.source) + offset;
            spec.remote_address = request.target_offset + offset;
            spec.length = length;
            spec.request_offset = offset;
            spec.max_retries = impl_->params.max_retries;
            if (!task->addSlice(spec)) {
                return Status::InternalError(
                    "Unable to construct UB slice" LOC_MARK);
            }
            offset += length;
        }
        (void)task->seal();
        new_tasks.push_back(std::move(task));
    }

    for (auto& task : new_tasks) {
        ub_batch->task_list.push_back(task);
        auto status = impl_->workers->submit(task, ub_batch->device_mask);
        if (!status.ok()) {
            for (auto& queued : new_tasks) {
                if (queued) queued->requestCancellation();
            }
            return status;
        }
    }
    return Status::OK();
}

Status UbTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                      TransferStatus& status) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch || task_id < 0 ||
        static_cast<size_t>(task_id) >= ub_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid UB task ID" LOC_MARK);
    }
    status = ub_batch->task_list[task_id]->transferStatus();
    return Status::OK();
}

Status UbTransport::cancelTransferTask(SubBatchRef batch, int task_id) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch || task_id < 0 ||
        static_cast<size_t>(task_id) >= ub_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid UB task ID" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->workers) {
        return Status::InvalidArgument("UB workers are not running" LOC_MARK);
    }
    return impl_->workers->cancel(ub_batch->task_list[task_id]);
}

Status UbTransport::addMemoryBuffer(BufferDesc& desc,
                                    const MemoryOptions& options) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto status = impl_->buffers->addBuffer(desc, options);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

Status UbTransport::addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                    const MemoryOptions& options) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto status = impl_->buffers->addBuffers(desc_list, options);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

Status UbTransport::removeMemoryBuffer(BufferDesc& desc) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) return Status::OK();
    auto status = impl_->buffers->removeBuffer(desc);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

bool UbTransport::warmupMemory(void* addr, size_t length) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!addr || length == 0 || !impl_->adapter || impl_->contexts.empty()) {
        return false;
    }
    ub::LocalSegmentPtr segment;
    ub::SegmentOptions options;
    auto status = impl_->adapter->registerLocalSegment(
        impl_->contexts.front()->handle(), reinterpret_cast<uint64_t>(addr),
        length, options, segment);
    if (!status.ok()) return false;
    return impl_->adapter->unregisterLocalSegment(segment).ok();
}

double UbTransport::getEstimatedBandwidth() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->params.enable_bandwidth_estimation || !impl_->rails)
        return -1.0;
    return impl_->rails->aggregateBandwidth();
}

Status UbTransport::getNicLoadStats(std::vector<NicLoadStats>& stats) const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->quota || !impl_->rails || !impl_->local_topology) {
        return Status::OK();
    }

    std::unordered_map<Topology::NicID, double> bandwidth_by_device;
    for (const auto& rail : impl_->rails->allStats()) {
        if (rail.paused || rail.ewma_bandwidth_bytes_per_second < 0.0) {
            continue;
        }
        auto [it, inserted] = bandwidth_by_device.emplace(
            rail.key.local_topology_id, rail.ewma_bandwidth_bytes_per_second);
        if (!inserted) {
            it->second =
                std::max(it->second, rail.ewma_bandwidth_bytes_per_second);
        }
    }

    const auto device_stats = impl_->quota->allDeviceStats();
    stats.reserve(stats.size() + device_stats.size());
    for (const auto& device : device_stats) {
        std::string name =
            impl_->local_topology->getNicName(device.local_topology_id);
        if (name.empty()) name = std::to_string(device.local_topology_id);
        const auto bandwidth =
            bandwidth_by_device.find(device.local_topology_id);
        stats.push_back(NicLoadStats{
            std::move(name),
            device.usage.inflight_bytes,
            bandwidth == bandwidth_by_device.end() ? 0.0 : bandwidth->second,
        });
    }
    return Status::OK();
}

}  // namespace mooncake::tent
