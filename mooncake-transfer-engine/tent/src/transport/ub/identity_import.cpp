// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/identity_import.h"

#include <climits>
#include <string>

#include "tent/common/types.h"
#include "tent/common/utils/string_builder.h"

namespace mooncake::tent::ub {
namespace {

template <typename T>
Status requireField(const nlohmann::json& json, const std::string& key,
                    T& out) {
    auto iter = json.find(key);
    if (iter == json.end() || iter->is_null()) {
        return Status::InvalidArgument(
            "UB device metadata is missing required field '" + key +
            "'" LOC_MARK);
    }
    try {
        out = iter->get<T>();
    } catch (const std::exception&) {
        return Status::InvalidArgument(
            "UB device metadata field '" + key +
            "' has an invalid format" LOC_MARK);
    }
    return Status::OK();
}

Status decodeCapabilities(const nlohmann::json& json,
                          DeviceCapabilities& caps) {
    caps = DeviceCapabilities{};
    auto iter = json.find("capabilities");
    if (iter == json.end() || !iter->is_object()) {
        return Status::InvalidArgument(
            "UB device metadata is missing the capabilities object" LOC_MARK);
    }
    const auto& caps_json = *iter;
    CHECK_STATUS(requireField(caps_json, "transport_modes",
                              caps.transport_modes));
    // Remaining capability fields are informational for identity anchoring;
    // decode them when present and keep safe defaults otherwise.
    auto optional = [&caps_json](const char* key, auto& out) {
        auto field = caps_json.find(key);
        if (field != caps_json.end() && field->is_number()) {
            out = field->get<std::decay_t<decltype(out)>>();
        }
    };
    optional("max_jfc", caps.max_jfc);
    optional("max_jfc_depth", caps.max_jfc_depth);
    optional("max_jfr_depth", caps.max_jfr_depth);
    optional("max_jetty", caps.max_jetty);
    optional("max_jetty_depth", caps.max_jetty_depth);
    optional("max_send_sge", caps.max_send_sge);
    optional("max_remote_sge", caps.max_remote_sge);
    optional("max_message_size", caps.max_message_size);
    optional("max_read_size", caps.max_read_size);
    optional("max_write_size", caps.max_write_size);
    optional("feature_flags", caps.feature_flags);
    optional("rm_tp_cap", caps.rm_tp_cap);
    auto masks = caps_json.find("priority_tp_type_masks");
    if (masks != caps_json.end() && masks->is_array()) {
        for (size_t priority = 0;
             priority < caps.priority_tp_type_masks.size() &&
             priority < masks->size();
             ++priority) {
            if ((*masks)[priority].is_number_unsigned()) {
                caps.priority_tp_type_masks[priority] =
                    (*masks)[priority].get<uint32_t>();
            }
        }
    }
    return Status::OK();
}

}  // namespace

std::string encodeDeviceMetadata(Topology::NicID topology_id,
                                 const DeviceInfo& device) {
    const auto& caps = device.capabilities;
    return nlohmann::json{{"schema_version", 1},
                          {"topology_id", topology_id},
                          {"native_device_name", device.native_device_name},
                          {"native_device_path", device.native_device_path},
                          {"eid_index", device.eid_index},
                          {"eid", device.eid},
                          {"active", device.active},
                          {"device_kind", deviceKindName(device.kind)},
                          {"device_kind_source",
                           deviceKindSourceName(device.kind_source)},
                          {"capabilities",
                           {{"max_jfc", caps.max_jfc},
                            {"max_jfc_depth", caps.max_jfc_depth},
                            {"max_jfr_depth", caps.max_jfr_depth},
                            {"max_jetty", caps.max_jetty},
                            {"max_jetty_depth", caps.max_jetty_depth},
                            {"max_send_sge", caps.max_send_sge},
                            {"max_remote_sge", caps.max_remote_sge},
                            {"max_message_size", caps.max_message_size},
                            {"max_read_size", caps.max_read_size},
                            {"max_write_size", caps.max_write_size},
                            {"feature_flags", caps.feature_flags},
                            {"transport_modes", caps.transport_modes},
                            {"rm_tp_cap", caps.rm_tp_cap},
                            {"priority_tp_type_masks",
                             caps.priority_tp_type_masks}}}}
        .dump();
}

Status decodeUbDeviceMetadata(const std::string& attrs_json, DeviceInfo& out) {
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(attrs_json);
    } catch (const std::exception&) {
        return Status::InvalidArgument(
            "UB device metadata is not valid JSON" LOC_MARK);
    }
    if (!json.is_object()) {
        return Status::InvalidArgument(
            "UB device metadata must be a JSON object" LOC_MARK);
    }
    int schema_version = 0;
    CHECK_STATUS(requireField(json, "schema_version", schema_version));
    if (schema_version != 1) {
        return Status::NotImplemented(
            "UB device metadata schema version " +
            std::to_string(schema_version) + " is not supported" LOC_MARK);
    }

    DeviceInfo device{};
    // topology_id is part of the schema and must be present, but the decoded
    // identity is addressed through the segment topology, not this field.
    int topology_id = -1;
    CHECK_STATUS(requireField(json, "topology_id", topology_id));
    CHECK_STATUS(requireField(json, "native_device_name",
                              device.native_device_name));
    CHECK_STATUS(requireField(json, "native_device_path",
                              device.native_device_path));
    CHECK_STATUS(requireField(json, "eid", device.eid));
    CHECK_STATUS(requireField(json, "active", device.active));

    uint32_t eid_index = 0;
    CHECK_STATUS(requireField(json, "eid_index", eid_index));
    device.eid_index = eid_index;

    std::string device_kind;
    CHECK_STATUS(requireField(json, "device_kind", device_kind));
    if (!parseDeviceKind(device_kind, device.kind)) {
        return Status::InvalidArgument("UB device metadata device_kind '" +
                                       device_kind +
                                       "' is not a valid device kind" LOC_MARK);
    }
    std::string kind_source;
    CHECK_STATUS(requireField(json, "device_kind_source", kind_source));
    if (!parseDeviceKindSource(kind_source, device.kind_source)) {
        return Status::InvalidArgument(
            "UB device metadata device_kind_source '" + kind_source +
            "' is not a valid device kind source" LOC_MARK);
    }
    CHECK_STATUS(decodeCapabilities(json, device.capabilities));
    out = std::move(device);
    return Status::OK();
}

Status collectLocalDeviceIdentities(const std::vector<DeviceInfo>& discovered,
                                    std::vector<DeviceIdentity>& out) {
    out.clear();
    for (const auto& device : discovered) {
        // A device without a complete identity cannot be imported
        // automatically; skip it instead of publishing a partial record.
        if (device.native_device_name.empty() ||
            device.native_device_path.empty() || device.eid.empty()) {
            continue;
        }
        DeviceIdentity identity;
        identity.native_device_name = device.native_device_name;
        identity.native_device_path = device.native_device_path;
        identity.eid = device.eid;
        identity.eid_index = device.eid_index;
        identity.transport_modes = device.capabilities.transport_modes;
        // Parity with generate_rm_legacy_config.py: every provider-visible
        // native device is recorded as physical. The RM capability gate at
        // install time still verifies each selected device before any
        // native resource is created, and device_filter remains available
        // for explicit exclusions.
        identity.kind = DeviceKind::PHYSICAL;
        CHECK_STATUS(validateDeviceIdentityRecord(identity));
        out.push_back(std::move(identity));
    }
    if (out.empty()) {
        return Status::InvalidArgument(
            "UB auto-import found no complete device identity in the "
            "provider discovery snapshot; configure "
            "transports/ub/device_identities manually or repair the URMA "
            "device environment" LOC_MARK);
    }
    return Status::OK();
}

nlohmann::json deviceIdentitiesToJson(
    const std::vector<DeviceIdentity>& identities) {
    auto array = nlohmann::json::array();
    for (const auto& identity : identities) {
        array.push_back({{"native_device_name", identity.native_device_name},
                         {"native_device_path", identity.native_device_path},
                         {"eid", identity.eid},
                         {"eid_index", identity.eid_index},
                         {"transport_modes", identity.transport_modes},
                         {"kind", deviceKindName(identity.kind)}});
    }
    return array;
}

Status anchorPeerIdentityFromSegment(const SegmentDesc& segment,
                                     Topology::NicID remote_topology_id,
                                     UbExpectedPeerIdentity& out) {
    const auto& memory = segment.getMemory();
    const auto* nic = memory.topology.getNicEntry(remote_topology_id);
    if (!nic) {
        return Status::InvalidArgument(
            "Remote segment '" + segment.name +
            "' publishes no UB NIC for topology id " +
            std::to_string(static_cast<int>(remote_topology_id)) + LOC_MARK);
    }
    const DeviceDesc* device = nullptr;
    for (const auto& entry : memory.devices) {
        if (entry.name == nic->name) {
            device = &entry;
            break;
        }
    }
    if (!device) {
        return Status::InvalidArgument(
            "Remote segment '" + segment.name +
            "' publishes no device entry for NIC '" + nic->name +
            "'; the peer must install the UB transport and publish its "
            "device metadata before RM import" LOC_MARK);
    }
    auto attrs = device->transport_attrs.find(TransportType::UB);
    if (attrs == device->transport_attrs.end() || attrs->second.empty()) {
        return Status::InvalidArgument(
            "Remote segment '" + segment.name + "' device '" + nic->name +
            "' carries no UB transport metadata" LOC_MARK);
    }
    DeviceInfo meta{};
    CHECK_STATUS(decodeUbDeviceMetadata(attrs->second, meta));

    // Completeness and format validation: every field the manual flow used
    // to provide must be present and plausible before it may anchor an RM
    // import.
    if (meta.native_device_name.empty() || meta.native_device_path.empty() ||
        meta.eid.empty()) {
        return Status::InvalidArgument(
            "Remote segment '" + segment.name + "' device '" + nic->name +
            "' metadata has incomplete identity fields" LOC_MARK);
    }
    if (!meta.active) {
        return Status::InvalidArgument(
            "Remote segment '" + segment.name + "' device '" + nic->name +
            "' is marked inactive in its published metadata" LOC_MARK);
    }
    if (meta.kind != DeviceKind::PHYSICAL) {
        return Status::NotImplemented(
            "RM import requires a trusted PHYSICAL peer device; remote "
            "segment '" +
            segment.name + "' device '" + nic->name + "' is published as " +
            std::string(deviceKindName(meta.kind)) + LOC_MARK);
    }
    if (meta.kind_source == DeviceKindSource::NONE) {
        return Status::NotImplemented(
            "Remote segment '" + segment.name + "' device '" + nic->name +
            "' metadata carries no device kind classification" LOC_MARK);
    }
    if ((meta.capabilities.transport_modes & kUrmaTransportModeRm) == 0) {
        return Status::NotImplemented(
            "Remote segment '" + segment.name + "' device '" + nic->name +
            "' metadata does not advertise the RM transport mode" LOC_MARK);
    }
    if (meta.eid_index > static_cast<uint32_t>(INT_MAX)) {
        return Status::InvalidArgument(
            "Remote segment '" + segment.name + "' device '" + nic->name +
            "' metadata eid_index is out of range" LOC_MARK);
    }

    UbExpectedPeerIdentity expected;
    expected.segment_name = segment.name;
    expected.local_nic_path = MakeNicPath(segment.name, nic->name);
    expected.native_device_name = meta.native_device_name;
    expected.device_id = remote_topology_id;
    expected.eid_index = static_cast<int>(meta.eid_index);
    expected.eid = meta.eid;
    expected.transport_modes = meta.capabilities.transport_modes;
    expected.physical_device_identity =
        makePhysicalDeviceIdentity(meta.native_device_path, meta.eid);
    out = std::move(expected);
    return Status::OK();
}

}  // namespace mooncake::tent::ub
