// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_bootstrap.h"
#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/probe_peer_coordinator.h"
#include "tent/transport/ub/probe_peer_registry.h"
#include "tent/transport/ub/probe_peer_source.h"
#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/probe_poster_urma.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "tent/transport/ub/probe_topology.h"
#include "tent/transport/ub/probe_scheduler.h"
#include "tent/transport/ub/ub_transport.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <limits>
#include <condition_variable>
#include <map>
#include <memory>
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
#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/bootstrap.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/topology_attrs.h"

#include "jetty_pool.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/endpoint.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/identity_import.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/target_registry.h"
#include "tent/transport/ub/tombstone.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/task_trace.h"
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
    Impl(std::shared_ptr<ub::UrmaAdapter> injected,
         std::shared_ptr<ub::ProbeResourceProvider> injected_probe_provider,
         std::shared_ptr<ub::ProbePoster> injected_probe_poster,
         ub::ProbePeerDescriptorSource* injected_peer_source)
        : adapter(injected ? std::move(injected)
                           : ub::createDefaultUrmaAdapter()),
          probe_peer_source(injected_peer_source),
          probe_provider(std::move(injected_probe_provider)),
          probe_poster(std::move(injected_probe_poster)) {}

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

    Status createBusinessContext(
        Topology::NicID local_nic, const ub::DeviceInfo& device,
        std::unordered_map<Topology::NicID, uint32_t>& jfc_depth_by_device,
        ub::UbContextPtr& created) {
        ub::JfcOptions jfc_options;
        const bool rm_runtime = runtime_mode == ub::UbRuntimeMode::kRmLegacy ||
                                runtime_mode == ub::UbRuntimeMode::kPooledRm;
        if (params.runtime_profile == ub::UbRuntimeProfile::kRmPool) {
            jfc_options.transport_mode = ub::JettyTransportMode::kRm;
        }
        if (rm_runtime) jfc_options.trans_mode = ub::kUrmaTransportModeRm;
        const auto& device_caps = device.capabilities;
        if (device_caps.max_jfc_depth != 0) {
            jfc_options.depth =
                std::min(jfc_options.depth, device_caps.max_jfc_depth);
            jfc_options.receiver_depth =
                std::min(jfc_options.receiver_depth, device_caps.max_jfc_depth);
        }
        if (device_caps.max_jfr_depth != 0) {
            jfc_options.receiver_depth =
                std::min(jfc_options.receiver_depth, device_caps.max_jfr_depth);
        }
        auto context =
            std::make_shared<ub::UbContext>(local_nic, device, adapter);
        auto status = context->initialize(params.jfc_per_context, jfc_options);
        created = context;
        if (!status.ok()) return status;
        context_by_topology_id.emplace(local_nic, context);
        context_by_topology_name.emplace(device.topology_name, context);
        jfc_depth_by_device.emplace(local_nic, jfc_options.depth);
        contexts.push_back(std::move(context));
        return Status::OK();
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
        {
            uint64_t next_epoch = generationSeed();
            while (next_epoch == topology_epoch) {
                next_epoch = generationSeed();
            }
            topology_epoch = next_epoch;
        }

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
        // RM legacy is a first-class runtime profile: the merged baseline
        // carries bootstrap-v2 target import (bindRm/importRemoteTarget) and
        // the RM endpoint data path, so it no longer fails closed here. Its
        // provider/capability gates still run during install and reject an
        // inconsistent device before any native resource is created.
        ub::UbRuntimeMode selected_mode = ub::UbRuntimeMode::kRcLegacy;
        auto mode_status = ub::resolveUbRuntimeMode(parsed, selected_mode);
        if (!mode_status.ok()) return abort_after_auto_import(mode_status);
        LOG(INFO) << "UB runtime selection: mode="
                  << ub::ubRuntimeModeName(selected_mode) << " profile="
                  << ub::ubRuntimeProfileName(parsed.runtime_profile)
                  << " trans_mode="
                  << ub::ubTransportModeName(parsed.trans_mode)
                  << " jetty_pool=" << (parsed.jetty_pool.enable ? "on" : "off")
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

        // Publish the trusted kind, not the raw discovery kind: the peer's
        // ProbeBootstrapUb identity check requires physical UB NICs.
        std::vector<Topology::UbNicKindOverride> kind_overrides;
        kind_overrides.reserve(discovered.size());
        for (const auto& device : discovered) {
            kind_overrides.push_back(Topology::UbNicKindOverride{
                device.topology_name,
                std::string(ub::deviceKindName(device.kind)),
                std::string(ub::deviceKindSourceName(device.kind_source))});
        }
        status = local_topology->applyUbDeviceKinds(kind_overrides);
        if (!status.ok()) return failInstall(status);

        // Declared topology is resolved once, against the same discovery
        // snapshot that selects the business devices. A declared binding that
        // matches zero or multiple physical EIDs fails the install (design
        // §4.3 rule 9); it is never skipped or guessed.
        probe_topology = std::make_shared<ub::ProbeTopology>();
        status = ub::ProbeTopology::resolve(
            params.probe, discovered, *local_topology, *probe_topology,
            params.observability.topology_dump_limit);
        if (!status.ok()) return failInstall(status);

        const bool explicit_filter = !params.device_filter.empty();
        const auto preferred_devices = params.physical_only
                                           ? ub::physicalDevices(discovered)
                                           : ub::preferBondingDevicesIfPresent(
                                                 discovered, explicit_filter);
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
        // INV-1: in declared mode the bindings are the only business device
        // source. Restricting the RM-capability gate here also prevents an
        // undeclared device from blocking or joining the install.
        if (probe_topology->declared()) {
            preferred_topology_names.clear();
            for (const auto& binding : probe_topology->ports()) {
                preferred_topology_names.insert(binding.topology_name);
            }
        }

        // RM capability gate (legacy and pooled): every selectable device
        // must be a trusted PHYSICAL device whose provider capabilities pass
        // the LocalSendProfile derivation (RM transport mode, RM/CTP support,
        // priority bind mask). The gate runs before any native resource
        // (context, JFC, Jetty) is created and fails closed; there is never a
        // silent fallback to RC or bonding.
        const bool rm_runtime = runtime_mode == ub::UbRuntimeMode::kRmLegacy ||
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
                        device.native_device_name +
                        " is not physical" LOC_MARK));
                }
                ub::LocalSendProfile profile;
                const auto profile_status = ub::buildLocalSendProfile(
                    device, static_cast<uint8_t>(ub::JettyOptions{}.priority),
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

        if (probe_topology->declared()) {
            for (const auto& binding : probe_topology->ports()) {
                auto found = by_topology_name.find(binding.topology_name);
                if (found == by_topology_name.end() || !found->second.active) {
                    return failInstall(Status::DeviceNotFound(
                        "declared probe port '" + binding.id +
                        "' resolved device is not active" LOC_MARK));
                }
                ub::UbContextPtr context;
                status = createBusinessContext(binding.local_nic, found->second,
                                               jfc_depth_by_device, context);
                if (!status.ok()) {
                    if (context &&
                        context->state() == ub::UbContext::State::kDraining) {
                        contexts.push_back(std::move(context));
                    }
                    return failInstall(status);
                }
                preferred_names.push_back(found->second.native_device_name);
            }
        } else {
            for (size_t id = 0; id < local_topology->getNicCount(); ++id) {
                const auto* nic =
                    local_topology->getNicEntry(static_cast<int>(id));
                if (!nic || nic->type != Topology::NIC_UB) continue;
                auto found = by_topology_name.find(nic->name);
                if (found == by_topology_name.end() || !found->second.active) {
                    continue;
                }
                if (!filterAllows(params.device_filter, found->second))
                    continue;
                if (preferred_topology_names.find(
                        found->second.topology_name) ==
                    preferred_topology_names.end()) {
                    skipped_names.push_back(found->second.native_device_name);
                    continue;
                }
                ub::UbContextPtr context;
                status = createBusinessContext(static_cast<Topology::NicID>(id),
                                               found->second,
                                               jfc_depth_by_device, context);
                if (!status.ok()) {
                    if (context &&
                        context->state() == ub::UbContext::State::kDraining) {
                        contexts.push_back(std::move(context));
                        return failInstall(status);
                    }
                    LOG(WARNING) << "Disable UB device " << nic->name << ": "
                                 << status.ToString();
                    continue;
                }
                preferred_names.push_back(found->second.native_device_name);
            }
        }

        {
            std::ostringstream selection_log;
            if (probe_topology->declared()) {
                selection_log << "UB device selection: mode=declared-topology";
            } else if (params.physical_only) {
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
            // EID reachability scopes among the selected devices. A mixed list
            // is legal (a dual-plane deployment), but legacy selection pairs
            // only same-scope devices unless require_same_eid_scope=false.
            std::vector<std::string> scopes;
            for (const auto& context : contexts) {
                if (!context) continue;
                const auto scope =
                    ub::eidScopeForEid(context->deviceInfo().eid);
                if (!scope.trusted) continue;
                // The last four digits of the prefix are the reachability
                // group (df00 vs df04); the full prefix is repetitive.
                const std::string short_scope =
                    scope.key.size() >= 4
                        ? scope.key.substr(scope.key.size() - 4)
                        : scope.key;
                if (std::find(scopes.begin(), scopes.end(), short_scope) ==
                    scopes.end()) {
                    scopes.push_back(short_scope);
                }
            }
            if (!scopes.empty()) {
                selection_log << " scopes=[";
                for (size_t i = 0; i < scopes.size(); ++i) {
                    if (i != 0) selection_log << ", ";
                    selection_log << scopes[i];
                }
                selection_log << "]";
            }
            selection_log << " require_same_eid_scope="
                          << (params.require_same_eid_scope ? "true" : "false");
            LOG(INFO) << selection_log.str();
            if (scopes.size() > 1 && !params.require_same_eid_scope) {
                LOG(WARNING)
                    << "UB device selection: " << scopes.size()
                    << " EID scopes selected with "
                       "require_same_eid_scope=false; cross-scope pairs may "
                       "fail with LOCAL_ACCESS_ERROR";
            }
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
            return failInstall(Status::DeviceNotFound(reason + LOC_MARK));
        }

        size_t safe_slice_size = params.slice_size;
        ub::JettyOptions jetty_options;
        if (rm_runtime) {
            jetty_options.trans_mode = ub::kUrmaTransportModeRm;
            jetty_options.bind_tp_type_mask = rm_capability_bind_tp_type_mask;
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
            // Only the per-operation limits bound a slice: the data path
            // issues URMA_OPC_READ / URMA_OPC_WRITE, so max_message_size
            // (SEND/RECV) must not pin slice_size (yuanhao-1113 5dcde226).
            // The clamp itself stays: exceeding the device limit is not a
            // tuning choice, and yuanhao's [TEST]-only bypass never shipped.
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
        // rm_pool runs the RM transport (validated by the config gate). Both
        // the pooled members and this side's endpoint Jetties are created in RM
        // mode, and an RM Jetty is never bound: every WR carries its own
        // imported target (design §5.2 / M-4).
        if (parsed.runtime_profile == ub::UbRuntimeProfile::kRmPool) {
            jetty_options.transport_mode = ub::JettyTransportMode::kRm;
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
                context->attachJettyPool(std::make_shared<ub::UbSendJettyPool>(
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
            // M9: rm_pool runs the RM transport, so every selected device must
            // advertise it. Failing here is much cheaper than failing on the
            // first createJetty.
            for (const auto& context : contexts) {
                if (!context) continue;
                const uint16_t modes =
                    context->deviceInfo().capabilities.transport_modes;
                if (modes != 0 && (modes & ub::kUrmaTransportModeRm) == 0) {
                    return failInstall(Status::InvalidArgument(
                        "runtime_profile=rm_pool requires devices that support "
                        "the RM transport mode" LOC_MARK));
                }
            }
            native_budget = std::make_shared<ub::NativeBudget>();
            // One bounded tombstone table per install: it records unproven
            // destructions and is what authorizes a final budget release.
            tombstones =
                std::make_shared<ub::TombstoneTable>(params.tombstone_budget);
            if (params.jetty_pool.async_drain) {
                // Every drain step runs here, never on a JFC poller and never
                // on the error-return path. A refused thread degrades to the
                // synchronous fence instead of terminating the process.
                drain_executor = std::make_unique<ub::ThreadedControlExecutor>(
                    "ub-jetty-drain");
                if (!drain_executor->start()) {
                    LOG(WARNING)
                        << "UB cannot start the Jetty drain control executor; "
                           "falling back to the synchronous retirement fence";
                    drain_executor.reset();
                }
            }
            std::unordered_set<std::string> configured_devices;
            for (const auto& context : contexts) {
                const auto& device = context->deviceInfo();
                const std::string device_key = ub::physicalDeviceKey(device);
                if (!configured_devices.insert(device_key).second) continue;
                const uint64_t verified = device.capabilities.max_jetty;
                uint64_t configured = params.native_budget.max_jetty_per_device;
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
                status = native_budget->configure(device_key, verified,
                                                  configured, reserve);
                if (!status.ok()) return failInstall(status);
            }
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
                    params.jetty_pool.business_desired_serving;
                pool_options.max_creating =
                    params.send_pool.max_creating_per_key;
                pool_options.background_refill =
                    params.send_pool.background_refill;
                pool_options.background_delete =
                    params.send_pool.background_delete;
                pool_options.refill_interval_ms =
                    params.jetty_pool.refill_interval_ms;
                pool_options.retire_timeout_ms =
                    params.jetty_pool.retire_timeout_ms;
                pool_options.cooldown_ms = params.jetty_pool.cooldown_ms;
                pool_options.quarantine_budget = params.quarantine_budget;
                pool_options.tombstones = tombstones;
                pool_options.jetty_options = jetty_options;
                pool_options.verbose_events =
                    params.observability.verbose_events;
                pool_options.sample_every =
                    params.observability.leaseSampleEvery();
                // Asynchronous drain (design §4.4). The chain is only armed
                // when a control executor exists; otherwise the pool keeps the
                // synchronous fence and says so once at install.
                pool_options.async_drain =
                    params.jetty_pool.async_drain && drain_executor != nullptr;
                pool_options.drain_flush_batch =
                    params.jetty_pool.drain_flush_batch;
                pool_options.drain_quarantine_threshold =
                    params.jetty_pool.drain_quarantine_threshold;
                auto pool = std::make_shared<ub::SendJettyPool>(
                    adapter, context->handle(), context->jfcs(), native_budget,
                    ub::physicalDeviceKey(device), credits, pool_options);
                context->attachPooledSendPool(pool);
                if (pool_options.async_drain) {
                    // The pool owns the verdicts; the workers own the two
                    // settlement queries. Both are resolved lazily at call time
                    // so the controller can be built before the workers exist.
                    std::weak_ptr<ub::SendJettyPool> weak_pool = pool;
                    ub::DrainCallbacks callbacks;
                    callbacks.resolveJetty =
                        [weak_pool](
                            const ub::PoolLaneKey& lane) -> ub::JettyPtr {
                        auto pool = weak_pool.lock();
                        return pool ? pool->drainJettyFor(lane) : nullptr;
                    };
                    callbacks.markDeleteReady = [weak_pool](
                                                    const ub::PoolLaneKey& lane,
                                                    uint64_t error_epoch) {
                        auto pool = weak_pool.lock();
                        if (pool) {
                            (void)pool->markDrainDeleteReady(lane, error_epoch);
                        }
                    };
                    callbacks.quarantine = [weak_pool](
                                               const ub::PoolLaneKey& lane,
                                               const std::string& reason) {
                        auto pool = weak_pool.lock();
                        if (pool) (void)pool->quarantineDrain(lane, reason);
                    };
                    callbacks.settleDrainedWr =
                        [this](const ub::Completion& completion,
                               uint64_t jfc_index) {
                            return workers != nullptr &&
                                   workers->settleDrainedWr(completion,
                                                            jfc_index);
                        };
                    callbacks.responsibilityConverged =
                        [this](const ub::LaneRefKey& lane) {
                            // No workers means no record book: fail closed.
                            return workers != nullptr &&
                                   workers->pooledLaneConverged(lane);
                        };
                    ub::DrainOptions drain_options;
                    drain_options.retire_timeout_ms =
                        params.jetty_pool.retire_timeout_ms;
                    drain_options.flush_batch =
                        params.jetty_pool.drain_flush_batch;
                    drain_options.quarantine_threshold =
                        params.jetty_pool.drain_quarantine_threshold;
                    auto controller =
                        std::make_shared<ub::JettyDrainController>(
                            adapter, tombstones.get(), drain_executor.get(),
                            std::move(callbacks), drain_options);
                    context->attachDrainController(controller);
                    pool->attachDrainController(std::move(controller));
                }
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

        // Probe assembly (design §13.3). Probing is disabled by default; when
        // enabled, the pooling side must supply a complete graph and a poster,
        // otherwise installation fails fast instead of degrading silently.
        std::vector<ub::ProbeJfcSlot> probe_jfcs;
        if (params.probe.enabled) {
            if (!probe_provider || !probe_poster) {
                if (params.probe.provider !=
                    ub::ProbeProviderKind::kStandalone) {
                    return failInstall(Status::InvalidArgument(
                        "UB probe is enabled but no probe provider/poster is "
                        "installed; inject one or set "
                        "transports/ub/probe/provider=standalone" LOC_MARK));
                }
                // Production declared mode builds its own registry/source;
                // the injected source remains the test/legacy seam.
                if (probe_peer_source == nullptr &&
                    !probe_topology->declared()) {
                    return failInstall(Status::InvalidArgument(
                        "UB probe provider=standalone requires declared "
                        "adjacency or an injected peer descriptor "
                        "source" LOC_MARK));
                }
                auto standalone =
                    std::make_shared<ub::StandaloneProbeResourceProvider>(
                        adapter, params.probe);
                standalone->setNodeIdentity(local_segment_name,
                                            node_incarnation, topology_epoch);
                if (probe_topology->declared()) {
                    // Declared mode: the configured bindings decide which
                    // ports exist. A missing business context is an install
                    // error, never a silent skip (design §6.2).
                    std::vector<ub::ProbePortSpec> specs;
                    specs.reserve(probe_topology->ports().size());
                    for (const auto& binding : probe_topology->ports()) {
                        auto device =
                            by_topology_name.find(binding.topology_name);
                        if (device == by_topology_name.end()) {
                            return failInstall(Status::InvalidArgument(
                                "declared probe port '" + binding.id +
                                "' has no discovered device" LOC_MARK));
                        }
                        ub::ProbePortSpec spec;
                        spec.port_id = binding.id;
                        spec.local_nic = binding.local_nic;
                        spec.device = device->second;
                        spec.topology_name = binding.topology_name;
                        spec.native_device_name = binding.native_device_name;
                        spec.eid = binding.eid;
                        spec.generation = 1;
                        specs.push_back(std::move(spec));
                    }
                    auto bind = standalone->bindPorts(specs);
                    if (!bind.ok()) return failInstall(bind);
                    auto prepared = standalone->prepareLocal();
                    if (!prepared.ok()) return failInstall(prepared);
                } else {
                    auto discover = standalone->discover();
                    if (!discover.ok()) return failInstall(discover);
                }
                if (probe_peer_source == nullptr) {
                    std::map<std::string,
                             std::vector<ub::ProbeDeclaredPeerPort>>
                        declared;
                    for (const auto& [peer, decl] :
                         probe_topology->peerDecls()) {
                        auto& ports = declared[peer];
                        for (const auto& port : decl.ports) {
                            ports.push_back(
                                ub::ProbeDeclaredPeerPort{port.id, port.eid});
                        }
                    }
                    std::map<std::string, uint64_t> peer_ids;
                    for (const auto& [peer, decl] :
                         probe_topology->peerDecls()) {
                        (void)decl;
                        auto id = probe_topology->probePeerId(peer);
                        if (id.has_value()) peer_ids.emplace(peer, *id);
                    }
                    probe_registry = std::make_unique<ub::ProbePeerRegistry>(
                        declared, params.probe.handshake.withdraw_grace,
                        peer_ids);
                    probe_source =
                        std::make_unique<ub::BootstrapProbePeerSource>(
                            probe_topology.get(), probe_registry.get());
                    probe_topology->setPeerRegistry(probe_registry.get());
                }
                auto* directory_source =
                    probe_peer_source != nullptr
                        ? probe_peer_source
                        : static_cast<ub::ProbePeerDescriptorSource*>(
                              probe_source.get());
                probe_directory =
                    std::make_shared<ub::HandshakeProbePeerDirectory>(
                        adapter, standalone->contexts(), directory_source);
                standalone->setRemoteDirectory(probe_directory.get());
                probe_provider = standalone;
                probe_poster = std::make_shared<ub::UrmaProbePoster>(adapter);
            }
            ub::ProbeGraph graph;
            status = probe_provider->snapshot(graph);
            if (!status.ok()) return failInstall(status);
            probe_manager =
                std::make_unique<ub::ProbeLinkManager>(params.probe);
            probe_manager->setVerboseEvents(
                params.observability.verbose_events);
            probe_manager->setReportSink(probe_provider.get());
            status = probe_manager->initialize(graph);
            if (!status.ok()) return failInstall(status);
            probe_provider->setAvailabilitySink(
                probe_manager->availabilitySink());
            probe_provider->setCompletionSink(
                [manager = probe_manager.get()](
                    const ub::Completion& completion) -> Status {
                    if (manager == nullptr) {
                        return Status::InvalidArgument(
                            "probe manager is gone" LOC_MARK);
                    }
                    // Quiesce is per probe Jetty, so every completion here
                    // belongs to the probe namespace.
                    if (!ub::isProbeToken(completion.token)) {
                        return Status::InvalidArgument(
                            "non-probe completion on a probe lane" LOC_MARK);
                    }
                    if (!manager->onProbeCompletion(completion)) {
                        return Status::InternalError(
                            "probe completion was not accepted" LOC_MARK);
                    }
                    return Status::OK();
                });
            // Responder side of ProbeBootstrapUb. The callback is installed
            // only while the probe resources exist and is cleared before they
            // are released.
            metadata->setProbeBootstrapCallback(
                [this](const ub::ProbeBootstrapRequest& request,
                       ub::ProbeBootstrapResponse& response) {
                    return onProbeBootstrap(request, response);
                });
            probe_bootstrap_callback_installed = true;
            for (const auto& supply : graph.ports) {
                if (context_by_topology_id.find(supply.local_nic) ==
                    context_by_topology_id.end()) {
                    return failInstall(
                        Status::InvalidArgument("UB probe port has no matching "
                                                "local context" LOC_MARK));
                }
                // nullptr means shared mode: probe completions land on the
                // business JFC and are routed by token namespace.
                if (!supply.probe_jfc) continue;
                probe_jfcs.push_back(
                    ub::ProbeJfcSlot{supply.local_nic, supply.probe_jfc});
            }
            if (probe_registry && probe_source) {
                auto* standalone =
                    dynamic_cast<ub::StandaloneProbeResourceProvider*>(
                        probe_provider.get());
                if (standalone == nullptr) {
                    return failInstall(Status::InvalidArgument(
                        "declared probe topology requires the standalone "
                        "probe provider" LOC_MARK));
                }
                ub::ProbePeerCoordinator::Dependencies dependencies;
                dependencies.segment_manager = &metadata->segmentManager();
                dependencies.registry = probe_registry.get();
                dependencies.provider = standalone;
                dependencies.topology = probe_topology.get();
                dependencies.rpc = [](const std::string& server_addr,
                                      const ub::ProbeBootstrapRequest& request,
                                      ub::ProbeBootstrapResponse& response) {
                    return ControlClient::probeBootstrapUb(server_addr, request,
                                                           response);
                };
                probe_coordinator = std::make_unique<ub::ProbePeerCoordinator>(
                    params.probe, std::move(dependencies));
                // The provider bumps the publication epoch after a lane
                // rebuild. Without this callback the coordinator only notices
                // it on the next handshake refresh tick (30s by default), so a
                // recovered link can stay unused for that whole window. The
                // callback only wakes the coordinator; it never reads provider
                // state, so it cannot deadlock against the provider mutex.
                if (probe_provider) {
                    auto* standalone =
                        dynamic_cast<ub::StandaloneProbeResourceProvider*>(
                            probe_provider.get());
                    if (standalone != nullptr) {
                        standalone->setPublicationChangedCallback(
                            [coordinator = probe_coordinator.get()](
                                uint64_t publication_epoch) {
                                if (coordinator != nullptr) {
                                    coordinator->onLocalPublicationChanged(
                                        publication_epoch);
                                }
                            });
                    }
                }
                status = probe_coordinator->start();
                if (!status.ok()) return failInstall(status);
            }
        }

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
            probe_manager.get(), std::move(probe_jfcs), tombstones,
            probe_topology.get());
        if (probe_coordinator) {
            workers->setProbeRefreshHint([this](uint64_t peer_id) {
                if (probe_coordinator) {
                    probe_coordinator->noteBusinessPathError(peer_id);
                }
            });
        }
        status = workers->start();
        if (!status.ok()) return failInstall(status);

        // Pollers run before the scheduler so a probe completion can always be
        // consumed (design §13.3).
        if (probe_manager) {
            try {
                probe_scheduler = std::make_unique<ub::ProbeScheduler>(
                    *probe_manager, *probe_poster);
                probe_scheduler->start();
            } catch (const std::exception& error) {
                return failInstall(Status::InternalError(
                    std::string("Cannot start the UB probe scheduler: ") +
                    error.what() + LOC_MARK));
            }
        }

        // Opt-in task tracing (yuanhao-1113). Off unless
        // MC_TRANSFER_TRACE_MIN_BYTES is set: the reporter is the only thread
        // it starts and every data-path hook is a null check.
        if (ub::ubTaskTraceMinBytes() > 0) {
            trace_reporter =
                std::make_shared<ub::UbTaskTraceReporter>(local_topology);
            LOG(INFO) << "UB task tracing enabled for requests of at least "
                      << ub::ubTaskTraceMinBytes() << " bytes";
        }
        installed.store(true, std::memory_order_release);
        LOG(INFO) << "UB installed: probe_links="
                  << (probe_manager ? probe_manager->pathCount() : 0)
                  << " probe_generation="
                  << (probe_manager ? probe_manager->probeGeneration() : 0)
                  << " pool_generation="
                  << (contexts.empty() || !contexts.front()
                          ? 0
                          : contexts.front()->incarnation());
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

        // Wake every single-flight follower. Copy the flights under the map
        // lock and signal them after releasing it: the leader takes the flight
        // lock before the map lock, so the reverse order here would deadlock.
        std::vector<std::shared_ptr<ResolveFlight>> pending_flights;
        {
            std::lock_guard<std::mutex> lock(resolve_flights_mutex);
            pending_flights.reserve(resolve_flights.size());
            for (const auto& [key, flight] : resolve_flights) {
                (void)key;
                if (flight) pending_flights.push_back(flight);
            }
        }
        for (const auto& flight : pending_flights) {
            {
                std::lock_guard<std::mutex> lock(flight->mutex);
                if (flight->done) continue;
                flight->done = true;
                flight->status = Status::InvalidArgument(
                    "UB transport is shutting down" LOC_MARK);
                flight->endpoint.reset();
            }
            flight->cv.notify_all();
        }
        resolve_flights.clear();

        // Callback replacement waits for a currently executing UB bootstrap
        // handler, fencing all control-plane access before resources retire.
        if (callback_installed && metadata) {
            metadata->setBootstrapUbCallback({});
            callback_installed = false;
        }
        if (probe_bootstrap_callback_installed && metadata) {
            metadata->setProbeBootstrapCallback({});
            probe_bootstrap_callback_installed = false;
        }
        if (probe_provider) {
            auto* standalone =
                dynamic_cast<ub::StandaloneProbeResourceProvider*>(
                    probe_provider.get());
            if (standalone != nullptr) {
                standalone->setPublicationChangedCallback({});
            }
        }
        // Fence the control plane before any probe resource is released.
        if (probe_coordinator) probe_coordinator->stop();
        // Stop interface-7 rebuilds before draining workers: a retire during
        // teardown must stay a record, not create resources that are about to
        // be destroyed.
        if (probe_provider) {
            probe_provider->setAvailabilitySink(nullptr);
            probe_provider->setCompletionSink(nullptr);
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
            // drainIdle() already degraded every in-flight drain to the
            // synchronous fence, so no callback may still need the workers.
            // Stopping the executor here bounds the thread's lifetime to the
            // ownership graph that references it.
            if (drain_executor) {
                drain_executor->stop();
                drain_executor.reset();
            }
            workers.reset();
        }
        // Workers joined the scheduler through drainBeforePollStop(); destroy
        // the PLM only after the workers that reference it are gone.
        probe_scheduler.reset();
        probe_manager.reset();
        probe_coordinator.reset();
        probe_poster.reset();
        // Unimport remote probe areas while the probe Contexts are still
        // alive; the self-owned provider closes them in its shutdown.
        if (probe_directory) (void)probe_directory->shutdown();
        probe_provider.reset();
        probe_directory.reset();
        probe_source.reset();
        probe_registry.reset();
        probe_topology.reset();
        // W3 pooled teardown safety net (design §11.2). workers->stop() already
        // drained the pools, but an install that failed before the workers
        // existed can leave a pool attached to its context. beginStop() and
        // both drains are idempotent, so running them here covers both paths
        // and guarantees that no pooled Jetty outlives its budget ledger.
        for (const auto& context : contexts) {
            if (!context) continue;
            auto pool = context->pooledSendPool();
            if (!pool) continue;
            pool->beginStop();
            (void)pool->drainIdle();
            (void)pool->drainQuarantined();
        }
        // The trace reporter owns a formatting thread: stop it last, after the
        // workers that push completed tasks into it (yuanhao-1113).
        trace_reporter.reset();
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
        tombstones.reset();
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
        // Failed installs may never have created the workers; the drain
        // executor still must not outlive this ownership graph.
        if (drain_executor) {
            drain_executor->stop();
            drain_executor.reset();
        }
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
                // setupLocalSegment() copied topology_ before install ran, so
                // the published snapshot still carries the raw discovery kind
                // (unknown). Refresh it from the install-time topology that
                // applyUbDeviceKinds() has already patched.
                if (local_topology) detail.topology = *local_topology;
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
        // The single-flight key is exactly the EndpointStore key the leader
        // will use. Invalid routes bypass the flight and let the leader return
        // the precise error.
        if (shutting_down.load(std::memory_order_acquire) ||
            !request.local_context || !request.remote_segment) {
            return resolveEndpointLeader(request, endpoint);
        }
        const auto* remote_nic =
            request.remote_segment->getMemory().topology.getNicEntry(
                request.remote_topology_id);
        if (!remote_nic || remote_nic->type != Topology::NIC_UB) {
            return resolveEndpointLeader(request, endpoint);
        }
        const ub::UbEndpointKey key{
            request.local_context->topologyId(), request.remote_segment_id,
            request.remote_topology_id,
            MakeNicPath(request.remote_segment->name, remote_nic->name)};
        std::shared_ptr<ResolveFlight> flight;
        bool leader = false;
        {
            std::lock_guard<std::mutex> lock(resolve_flights_mutex);
            auto [it, inserted] = resolve_flights.try_emplace(key, nullptr);
            if (inserted || !it->second) {
                it->second = std::make_shared<ResolveFlight>();
                flight = it->second;
                leader = true;
            } else {
                flight = it->second;
            }
        }
        if (!leader) {
            std::unique_lock<std::mutex> lock(flight->mutex);
            constexpr auto kResolveFlightWait = std::chrono::milliseconds(5000);
            if (!flight->cv.wait_for(lock, kResolveFlightWait,
                                     [&] { return flight->done; })) {
                return Status::TooManyRequests(
                    "UB endpoint resolution is still in flight" LOC_MARK);
            }
            if (!flight->status.ok()) return flight->status;
            if (!flight->endpoint || !flight->endpoint->ready()) {
                return Status::TooManyRequests(
                    "UB endpoint resolution finished without a ready "
                    "endpoint" LOC_MARK);
            }
            endpoint = flight->endpoint;
            return Status::OK();
        }

        Status status = resolveEndpointLeader(request, endpoint);
        {
            std::lock_guard<std::mutex> lock(flight->mutex);
            flight->status = status;
            flight->endpoint = status.ok() ? endpoint : nullptr;
            flight->done = true;
        }
        flight->cv.notify_all();
        {
            std::lock_guard<std::mutex> lock(resolve_flights_mutex);
            auto it = resolve_flights.find(key);
            if (it != resolve_flights.end() && it->second == flight) {
                resolve_flights.erase(it);
            }
        }
        return status;
    }

    Status resolveEndpointLeader(const ub::EndpointResolveRequest& request,
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
        const uint64_t resolve_total =
            endpoint_resolve_total.fetch_add(1, std::memory_order_relaxed) + 1;
        auto log_resolve_summary = [&]() {
            const uint64_t bootstraps =
                endpoint_resolve_bootstrap.load(std::memory_order_relaxed);
            LOG(INFO)
                << "UB endpoint resolve summary: total=" << resolve_total
                << " cache_hit="
                << endpoint_resolve_cache_hit.load(std::memory_order_relaxed)
                << " bootstrap=" << bootstraps << " bootstrap_fail="
                << endpoint_resolve_bootstrap_fail.load(
                       std::memory_order_relaxed)
                << " rpc_us_avg="
                << (bootstraps == 0 ? 0
                                    : endpoint_resolve_rpc_us_sum.load(
                                          std::memory_order_relaxed) /
                                          bootstraps)
                << " rpc_us_max="
                << endpoint_resolve_rpc_us_max.load(std::memory_order_relaxed);
        };
        if (endpoint->ready()) {
            endpoint_resolve_cache_hit.fetch_add(1, std::memory_order_relaxed);
            if (resolve_total % 5000 == 0) log_resolve_summary();
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
        const auto rpc_begin = std::chrono::steady_clock::now();
        status = ControlClient::bootstrapUb(
            request.remote_segment->rpc_server_addr, bootstrap, response);
        const uint64_t rpc_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - rpc_begin)
                .count());
        endpoint_resolve_bootstrap.fetch_add(1, std::memory_order_relaxed);
        endpoint_resolve_rpc_us_sum.fetch_add(rpc_us,
                                              std::memory_order_relaxed);
        uint64_t prev_max =
            endpoint_resolve_rpc_us_max.load(std::memory_order_relaxed);
        while (rpc_us > prev_max &&
               !endpoint_resolve_rpc_us_max.compare_exchange_weak(
                   prev_max, rpc_us, std::memory_order_relaxed)) {
        }
        if (!status.ok()) {
            endpoint_resolve_bootstrap_fail.fetch_add(
                1, std::memory_order_relaxed);
        }
        LOG_EVERY_N(INFO, 50) << "UB endpoint resolve: local_nic="
                              << request.local_context->topologyId()
                              << " remote_nic=" << request.remote_topology_id
                              << " cache_hit=0 rpc_us=" << rpc_us
                              << " status=" << status.ToString();
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
            endpoint_bind_fail.fetch_add(1, std::memory_order_relaxed);
            LOG_EVERY_N(WARNING, 20)
                << "UB endpoint bind failed: local_nic="
                << request.local_context->topologyId()
                << " remote_nic=" << request.remote_topology_id << " state="
                << (endpoint ? static_cast<int>(endpoint->state()) : -1)
                << " status=" << status.ToString();
            (void)endpoints->retire(endpoint);
            endpoint.reset();
            return status;
        }
        recordReadyEndpoint(endpoint);
        return Status::OK();
    }

    int onProbeBootstrap(const ub::ProbeBootstrapRequest& request,
                         ub::ProbeBootstrapResponse& response) {
        response.responder_segment = local_segment_name;
        response.node_incarnation = node_incarnation;
        response.topology_epoch = topology_epoch;
        response.transport_mode = ub::kProbeBootstrapTransportMode;
        response.device_kind = ub::kProbeBootstrapDeviceKind;
        if (shutting_down.load(std::memory_order_acquire) || !probe_provider) {
            response.reply_msg = "probe not ready";
            return -1;
        }
        auto standalone =
            std::dynamic_pointer_cast<ub::StandaloneProbeResourceProvider>(
                probe_provider);
        if (!standalone) {
            response.reply_msg = "probe provider does not publish resources";
            return -1;
        }
        auto status = ub::validateProbeBootstrapRequest(request);
        if (!status.ok()) {
            response.reply_msg = status.ToString();
            return -1;
        }

        // The registered segment metadata is the only trusted anchor for the
        // requester's ports. Never accept a self-asserted EID (design §7.7).
        SegmentDescRef requester_segment;
        if (request.requester_segment == local_segment_name) {
            requester_segment = metadata->segmentManager().getLocal();
        } else {
            auto fetch = std::async(
                std::launch::async, [this, name = request.requester_segment]() {
                    SegmentDescRef segment;
                    auto fetch_status =
                        metadata->segmentManager().getRemote(segment, name);
                    return std::make_pair(fetch_status, std::move(segment));
                });
            auto fetched = fetch.get();
            if (!fetched.first.ok() || !fetched.second) {
                response.reply_msg = "requester segment is not registered";
                return -1;
            }
            requester_segment = std::move(fetched.second);
        }
        if (!requester_segment) {
            response.reply_msg = "requester segment is not registered";
            return -1;
        }

        auto lower = [](std::string value) {
            std::transform(value.begin(), value.end(), value.begin(),
                           [](unsigned char c) {
                               return static_cast<char>(std::tolower(c));
                           });
            return value;
        };
        const auto& topology = requester_segment->getMemory().topology;
        for (const auto& port : request.ports) {
            size_t matches = 0;
            bool physical = true;
            for (size_t id = 0; id < topology.getNicCount(); ++id) {
                const auto* nic =
                    topology.getNicEntry(static_cast<Topology::NicID>(id));
                if (!nic || nic->type != Topology::NIC_UB) continue;
                auto eid =
                    nic->device_attrs.find(std::string(ub::kTopologyEidAttr));
                if (eid == nic->device_attrs.end()) continue;
                if (nic->name != port.topology_name) continue;
                if (lower(eid->second) != lower(port.eid)) continue;
                auto kind = nic->device_attrs.find(
                    std::string(ub::kTopologyDeviceKindAttr));
                if (kind != nic->device_attrs.end() &&
                    kind->second !=
                        ub::deviceKindName(ub::DeviceKind::PHYSICAL)) {
                    physical = false;
                }
                ++matches;
            }
            if (matches != 1 || !physical) {
                probe_publication_identity_fail.fetch_add(
                    1, std::memory_order_relaxed);
                LOG_EVERY_N(WARNING, 100)
                    << "UB probe publication identity mismatch: port="
                    << port.port_id << " topology=" << port.topology_name
                    << " eid=" << port.eid << " matches=" << matches
                    << " physical=" << (physical ? 1 : 0);
                response.reply_msg =
                    "probe publication identity mismatch for port '" +
                    port.port_id + "'";
                return -1;
            }
        }

        // declared_peers is diagnostic only: a mismatch between both sides'
        // declarations is logged, never used to create resources.
        if (request.declared_peers.size() != params.probe.peers.size()) {
            LOG_EVERY_N(INFO, 100)
                << "UB probe handshake: requester " << request.requester_segment
                << " declares " << request.declared_peers.size()
                << " peer(s), local declares " << params.probe.peers.size();
        }

        // The request carries the requester's publication. Hand it to the
        // coordinator, which applies it on a control executor (never on this
        // RPC thread) and refreshes the graph. If this side did not declare
        // the requester, the coordinator drops it.
        if (probe_coordinator) {
            ub::ProbeBootstrapResponse requester_publication;
            requester_publication.responder_segment = request.requester_segment;
            requester_publication.node_incarnation =
                request.requester_node_incarnation;
            requester_publication.topology_epoch =
                request.requester_topology_epoch;
            requester_publication.publication_epoch =
                request.requester_publication_epoch;
            requester_publication.transport_mode =
                ub::kProbeBootstrapTransportMode;
            requester_publication.device_kind = ub::kProbeBootstrapDeviceKind;
            requester_publication.ports = request.ports;
            (void)probe_coordinator->ingestPeerPublication(
                request.requester_segment, requester_segment,
                std::move(requester_publication));
        }

        auto publication = standalone->publication();
        if (publication.ports.empty()) {
            response.reply_msg = "probe not ready";
            return -1;
        }
        response.publication_epoch = publication.publication_epoch;
        response.ports.reserve(publication.ports.size());
        for (const auto& port : publication.ports) {
            response.ports.push_back(ub::toProbeBootstrapPort(port));
        }
        return 0;
    }

    int onBootstrap(const UbBootstrapDesc& request, UbBootstrapDesc& response) {
        if (shutting_down.load(std::memory_order_acquire) || !endpoints) {
            response.reply_msg = "UB transport is shutting down";
            return -1;
        }
        // The mode gate precedes endpoint eviction, Jetty creation and native
        // import/bind side effects. RC remains on v1; RM requires a strict v2
        // request with no reply_msg.
        const bool rm_runtime = runtime_mode == ub::UbRuntimeMode::kRmLegacy ||
                                runtime_mode == ub::UbRuntimeMode::kPooledRm;
        auto gate_status = ub::gateUbBootstrapForMode(
            rm_runtime ? ub::UbBootstrapMode::RM : ub::UbBootstrapMode::RC,
            params.bootstrap_protocol, request);
        if (request.ignored_v2_payload) {
            // A v1 peer sent a v2 block. It was sanitized away during
            // deserialization and never reached an endpoint; count it so a
            // protocol mismatch is observable instead of silent.
            bootstrap_v2_ignored.fetch_add(1, std::memory_order_relaxed);
            LOG_EVERY_N(WARNING, 100)
                << "UB bootstrap v1 peer carried a v2 payload; the block was "
                   "ignored";
        }
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
            request.jetty_ids.empty() || request.endpoint_generation == 0 ||
            request.jetty_uasids.size() != request.jetty_ids.size()) {
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
                std::launch::async, [this, name = request.segment_name]() {
                    SegmentDescRef seg;
                    auto st = metadata->segmentManager().getRemote(seg, name);
                    return std::make_pair(st, std::move(seg));
                });
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
        if (!status.ok() && status.IsAlreadyExists()) {
            // The incumbent cannot be merged with this bootstrap (same
            // incarnation/different content, or a newer generation). Retire it
            // and retry once in the same RPC: the requester has already paid
            // the round trip, and leaving the stale incumbent in place makes
            // the requester loop forever. A stale request never reaches this
            // branch (kStale stays InvalidArgument), so a late request cannot
            // evict a newer incumbent.
            endpoint_bind_conflict.fetch_add(1, std::memory_order_relaxed);
            endpoint_bind_replace_retry.fetch_add(1, std::memory_order_relaxed);
            LOG_EVERY_N(WARNING, 100)
                << "UB RM bootstrap replacing incumbent: local_nic="
                << context->topologyId()
                << " remote_device=" << request.local_device_id
                << " reason=" << status.ToString();
            if (endpoint) (void)endpoints->retire(endpoint);
            endpoint.reset();
            status = endpoints->getOrCreate(key, context, endpoint);
            if (status.ok()) status = endpoint->bindRm(request, expected);
            if (status.ok()) {
                endpoint_bind_replace_ok.fetch_add(1,
                                                   std::memory_order_relaxed);
            }
        }
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
            endpoint_bind_fail.fetch_add(1, std::memory_order_relaxed);
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
    // topology_epoch identifies one transport install. It must change on
    // every install (including uninstall/reinstall of the same object), since
    // the provider publication epoch restarts at 1.
    uint64_t topology_epoch{generationSeed()};
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
    // Declared before `workers` so destruction runs workers -> scheduler ->
    // manager -> provider/poster (the workers hold a raw manager pointer).
    // probe_directory is declared before probe_provider so it is destroyed
    // after the provider that borrows it.
    // Asynchronous Jetty drain chain (design §4.2). Declared before the probe
    // block/workers/contexts so they are destroyed after both: destruction of a
    // controller never invokes a callback, and the pools (owned by the
    // contexts) are expected to be gone by then.
    std::unique_ptr<ub::ThreadedControlExecutor> drain_executor;
    ub::ProbePeerDescriptorSource* probe_peer_source{nullptr};
    std::unique_ptr<ub::ProbePeerRegistry> probe_registry;
    std::unique_ptr<ub::BootstrapProbePeerSource> probe_source;
    std::shared_ptr<ub::HandshakeProbePeerDirectory> probe_directory;
    std::unique_ptr<ub::ProbePeerCoordinator> probe_coordinator;
    std::shared_ptr<ub::ProbeResourceProvider> probe_provider;
    std::shared_ptr<ub::ProbePoster> probe_poster;
    // Declared adjacency policy for the business routing gate. Destroyed
    // after the workers that borrow it.
    std::shared_ptr<ub::ProbeTopology> probe_topology;
    std::unique_ptr<ub::ProbeLinkManager> probe_manager;
    std::unique_ptr<ub::ProbeScheduler> probe_scheduler;
    std::unique_ptr<ub::UbWorkers> workers;
    // rm_pool admission components, owned by the install generation and
    // shared with the workers. Reset during shutdown after the workers stop
    // and the contexts (pool owners) are released.
    std::shared_ptr<ub::NativeBudget> native_budget;
    // Bounded tombstones for unproven native destruction (design §7.2). One
    // table per install generation, shared by the pools and the workers.
    std::shared_ptr<ub::TombstoneTable> tombstones;
    // v1 peers that carried a v2 block (sanitized ignore, design §6.4).
    std::atomic<uint64_t> bootstrap_v2_ignored{0};
    std::atomic<uint64_t> probe_publication_identity_fail{0};
    std::atomic<uint64_t> endpoint_resolve_total{0};
    std::atomic<uint64_t> endpoint_resolve_cache_hit{0};
    std::atomic<uint64_t> endpoint_resolve_bootstrap{0};
    std::atomic<uint64_t> endpoint_resolve_bootstrap_fail{0};
    std::atomic<uint64_t> endpoint_resolve_rpc_us_sum{0};
    std::atomic<uint64_t> endpoint_resolve_rpc_us_max{0};
    std::atomic<uint64_t> endpoint_bind_fail{0};
    std::atomic<uint64_t> endpoint_bind_conflict{0};
    std::atomic<uint64_t> endpoint_bind_replace_retry{0};
    std::atomic<uint64_t> endpoint_bind_replace_ok{0};

    struct ResolveFlight {
        std::mutex mutex;
        std::condition_variable cv;
        bool done{false};
        Status status;
        std::shared_ptr<ub::UbEndpoint> endpoint;
    };
    std::mutex resolve_flights_mutex;
    std::unordered_map<ub::UbEndpointKey, std::shared_ptr<ResolveFlight>,
                       ub::UbEndpointKeyHash>
        resolve_flights;
    bool probe_bootstrap_callback_installed{false};
    // Opt-in per-task UB trace reporter (yuanhao-1113). Null unless
    // MC_TRANSFER_TRACE_MIN_BYTES is set at install time.
    std::shared_ptr<ub::UbTaskTraceReporter> trace_reporter;
};

UbTransport::UbTransport(
    std::shared_ptr<ub::UrmaAdapter> adapter,
    std::shared_ptr<ub::ProbeResourceProvider> probe_provider,
    std::shared_ptr<ub::ProbePoster> probe_poster,
    ub::ProbePeerDescriptorSource* probe_peer_source)
    : impl_(
          std::make_unique<Impl>(std::move(adapter), std::move(probe_provider),
                                 std::move(probe_poster), probe_peer_source)) {}

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
    ub_batch->trace_reporter = impl_->trace_reporter;
    batch = ub_batch;
    return Status::OK();
}

Status UbTransport::freeSubBatch(SubBatchRef& batch) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument("Invalid UB sub-batch" LOC_MARK);
    }
    // Uses the batch's own reporter reference: the engine calls this with its
    // batch lock held, so taking lifecycle_mutex here could deadlock against
    // the progress notification issued by submitTransferTasks().
    if (ub_batch->trace_reporter) {
        ub_batch->trace_reporter->submit(ub_batch->task_list,
                                         ub_batch->progress_batch_id);
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
    auto progress_armed = std::make_shared<std::atomic<bool>>(false);
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
        // W3 D-6: K lives on the task and only binds in pooled mode. A pooled
        // task is capped at task_max_inflight_attempts inflight attempts; the
        // legacy profiles keep the unlimited slice behavior of W2.
        const uint32_t task_limit =
            impl_->params.pooledSendPoolRequested()
                ? impl_->params.task_max_inflight_attempts
                : 0;
        auto task = ub::UbTask::create(
            request,
            [notify_progress, progress_batch_id,
             progress_armed](const TransferStatus&) {
                // Armed only once the task is queryable through the batch, so
                // a fast completion cannot notify about an unknown task
                // (yuanhao-1113).
                if (progress_armed->load(std::memory_order_acquire) &&
                    notify_progress) {
                    notify_progress(progress_batch_id);
                }
            },
            /*created_ns=*/0, task_limit);
        // Opt-in tracing (yuanhao-1113): only traced tasks pay the per-slice
        // checkpoint stores; only the reporter formats anything.
        if (impl_->trace_reporter &&
            request.length >= ub::ubTaskTraceMinBytes()) {
            task->enableTrace();
        }
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

    CHECK_STATUS(impl_->workers->submitBatch(new_tasks, ub_batch->device_mask));
    ub_batch->task_list.insert(ub_batch->task_list.end(), new_tasks.begin(),
                               new_tasks.end());
    progress_armed->store(true, std::memory_order_release);
    // A very small task can finish between worker publication and the batch
    // append above. Its callback intentionally stays disarmed until the task
    // is queryable; this notification closes that race.
    if (!new_tasks.empty() && notify_progress) {
        notify_progress(progress_batch_id);
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

size_t UbTransport::probeLinkCount() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    return impl_->probe_manager ? impl_->probe_manager->pathCount() : 0;
}

size_t UbTransport::probeCampaignCount() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    return impl_->probe_manager ? impl_->probe_manager->campaignCount() : 0;
}

bool UbTransport::probeStopped() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    return impl_->probe_manager != nullptr &&
           impl_->probe_manager->postingStopped();
}

uint64_t UbTransport::probeGeneration() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    return impl_->probe_manager ? impl_->probe_manager->probeGeneration() : 0;
}

ub::ProbeTopologyStats UbTransport::probeTopologyStats() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->probe_topology) return ub::ProbeTopologyStats{};
    return impl_->probe_topology->stats();
}

ub::ProbeCoordinatorStats UbTransport::probeCoordinatorStats() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->probe_coordinator) return ub::ProbeCoordinatorStats{};
    return impl_->probe_coordinator->stats();
}

uint64_t UbTransport::probePublicationIdentityFailures() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    return impl_->probe_publication_identity_fail.load(
        std::memory_order_relaxed);
}

ub::ProbeStats UbTransport::probeLinkStats() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->probe_manager) return ub::ProbeStats{};
    return impl_->probe_manager->stats();
}

uint64_t UbTransport::poolGeneration() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    uint64_t generation = 0;
    for (const auto& context : impl_->contexts) {
        if (context) generation = std::max(generation, context->incarnation());
    }
    return generation;
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
