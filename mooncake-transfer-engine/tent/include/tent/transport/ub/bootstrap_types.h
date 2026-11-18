// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_BOOTSTRAP_TYPES_H_
#define TENT_TRANSPORT_UB_BOOTSTRAP_TYPES_H_

#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent {

template <typename T>
T ubBootstrapJsonUnsigned(const nlohmann::json& j, const char* key) {
    const auto& value = j.at(key);
    uint64_t parsed = 0;
    if (value.is_number_unsigned()) {
        parsed = value.get<uint64_t>();
    } else if (value.is_number_integer()) {
        const int64_t signed_value = value.get<int64_t>();
        if (signed_value < 0) {
            throw std::invalid_argument(
                std::string(
                    "UB bootstrap numeric field must not be negative: ") +
                key);
        }
        parsed = static_cast<uint64_t>(signed_value);
    } else {
        throw std::invalid_argument(
            std::string("UB bootstrap numeric field must be an integer: ") +
            key);
    }
    if (parsed > std::numeric_limits<T>::max()) {
        throw std::invalid_argument(
            std::string("UB bootstrap numeric field is out of range: ") + key);
    }
    return static_cast<T>(parsed);
}

// RM bootstrap v2 uses structured targets. All fields in these nested
// structures are required on the wire; omitted fields never inherit v1 values.
// Numeric values mirror urma_transport_mode_t, urma_target_type_t and
// urma_tp_type_t. Local JFS priority/bind masks never belong to a remote target.
struct UbRemoteTargetProfile {
    uint16_t trans_mode = ub::kUrmaTransportModeRm;
    uint8_t target_type = ub::kUrmaTargetTypeJetty;
    uint8_t tp_type = ub::kUrmaTpTypeCtp;
};

inline void to_json(nlohmann::json& j, const UbRemoteTargetProfile& profile) {
    j = nlohmann::json{{"trans_mode", profile.trans_mode},
                       {"target_type", profile.target_type},
                       {"tp_type", profile.tp_type}};
}

inline void from_json(const nlohmann::json& j, UbRemoteTargetProfile& profile) {
    if (!j.is_object()) {
        throw std::invalid_argument("remote target profile must be an object");
    }
    // A strict allowlist prevents local/legacy fields and unknown extensions
    // from being silently reinterpreted as a native remote target profile.
    for (const auto& item : j.items()) {
        if (item.key() != "trans_mode" && item.key() != "target_type" &&
            item.key() != "tp_type") {
            throw std::invalid_argument(
                "field is not allowed in remote target profile: " + item.key());
        }
    }
    profile.trans_mode =
        ubBootstrapJsonUnsigned<uint16_t>(j, "trans_mode");
    profile.target_type =
        ubBootstrapJsonUnsigned<uint8_t>(j, "target_type");
    profile.tp_type = ubBootstrapJsonUnsigned<uint8_t>(j, "tp_type");
}

struct UbRmTargetDescriptor {
    std::string target_identity;
    std::string eid;
    uint32_t uasid = 0;
    uint32_t id = 0;
    uint64_t target_epoch = 0;
    UbRemoteTargetProfile profile;
};

inline void to_json(nlohmann::json& j, const UbRmTargetDescriptor& target) {
    j = nlohmann::json{{"target_identity", target.target_identity},
                       {"eid", target.eid},
                       {"uasid", target.uasid},
                       {"id", target.id},
                       {"target_epoch", target.target_epoch},
                       {"profile", target.profile}};
}

inline void from_json(const nlohmann::json& j, UbRmTargetDescriptor& target) {
    target.target_identity = j.at("target_identity").get<std::string>();
    target.eid = j.at("eid").get<std::string>();
    target.uasid = ubBootstrapJsonUnsigned<uint32_t>(j, "uasid");
    target.id = ubBootstrapJsonUnsigned<uint32_t>(j, "id");
    target.target_epoch = ubBootstrapJsonUnsigned<uint64_t>(j, "target_epoch");
    target.profile = j.at("profile").get<UbRemoteTargetProfile>();
}

struct UbRmBootstrapDesc {
    std::string transport_mode;
    std::string device_kind;
    std::string physical_device_identity;
    uint64_t node_incarnation = 0;
    uint64_t context_incarnation = 0;
    uint64_t topology_epoch = 0;
    uint16_t transport_modes = 0;
    std::vector<std::string> provider_capabilities;
    std::vector<UbRmTargetDescriptor> targets;
    std::map<std::string, std::vector<std::string>> probe_target_map;
};

inline void to_json(nlohmann::json& j, const UbRmBootstrapDesc& rm) {
    j = nlohmann::json{
        {"transport_mode", rm.transport_mode},
        {"device_kind", rm.device_kind},
        {"physical_device_identity", rm.physical_device_identity},
        {"node_incarnation", rm.node_incarnation},
        {"context_incarnation", rm.context_incarnation},
        {"topology_epoch", rm.topology_epoch},
        {"transport_modes", rm.transport_modes},
        {"provider_capabilities", rm.provider_capabilities},
        {"targets", rm.targets},
        {"probe_target_map", rm.probe_target_map}};
}

inline void from_json(const nlohmann::json& j, UbRmBootstrapDesc& rm) {
    rm.transport_mode = j.at("transport_mode").get<std::string>();
    rm.device_kind = j.at("device_kind").get<std::string>();
    rm.physical_device_identity =
        j.at("physical_device_identity").get<std::string>();
    rm.node_incarnation =
        ubBootstrapJsonUnsigned<uint64_t>(j, "node_incarnation");
    rm.context_incarnation =
        ubBootstrapJsonUnsigned<uint64_t>(j, "context_incarnation");
    rm.topology_epoch = ubBootstrapJsonUnsigned<uint64_t>(j, "topology_epoch");
    rm.transport_modes =
        ubBootstrapJsonUnsigned<uint16_t>(j, "transport_modes");
    rm.provider_capabilities =
        j.at("provider_capabilities").get<std::vector<std::string>>();
    rm.targets = j.at("targets").get<std::vector<UbRmTargetDescriptor>>();
    rm.probe_target_map =
        j.at("probe_target_map")
            .get<std::map<std::string, std::vector<std::string>>>();
}

// UB/URMA has Jetty, JFC and EID concepts that are not wire-compatible with
// RDMA QPs, CQs and GIDs. Keep the v1 fields unchanged for RC compatibility.
struct UbBootstrapDesc {
    uint32_t protocol_version = 1;
    std::string segment_name;
    std::string local_nic_path;
    std::string peer_nic_path;
    std::string local_device_name;
    int local_device_id = -1;
    int local_eid_index = -1;
    std::string local_eid;
    std::vector<uint32_t> jetty_ids;
    // UASID is part of urma_jetty_id_t on providers that use nonzero address
    // spaces. Kept parallel to jetty_ids for protocol-v1 compatibility.
    std::vector<uint32_t> jetty_uasids;
    uint64_t endpoint_generation = 0;
    uint64_t segment_generation = 0;
    std::vector<std::string> capabilities;
    std::string reply_msg;
    std::optional<UbRmBootstrapDesc> rm;
};

inline void to_json(nlohmann::json& j, const UbBootstrapDesc& desc) {
    if (desc.protocol_version == 1) {
        j = nlohmann::json{{"protocol_version", desc.protocol_version},
                           {"segment_name", desc.segment_name},
                           {"local_nic_path", desc.local_nic_path},
                           {"peer_nic_path", desc.peer_nic_path},
                           {"local_device_name", desc.local_device_name},
                           {"local_device_id", desc.local_device_id},
                           {"local_eid_index", desc.local_eid_index},
                           {"local_eid", desc.local_eid},
                           {"jetty_ids", desc.jetty_ids},
                           {"jetty_uasids", desc.jetty_uasids},
                           {"endpoint_generation", desc.endpoint_generation},
                           {"segment_generation", desc.segment_generation},
                           {"capabilities", desc.capabilities},
                           {"reply_msg", desc.reply_msg}};
        return;
    }
    if (desc.protocol_version != 2) {
        throw std::invalid_argument("invalid UB bootstrap v2 envelope");
    }
    if (!desc.rm.has_value()) {
        if (desc.reply_msg.empty()) {
            throw std::invalid_argument("invalid UB bootstrap v2 envelope");
        }
        j = nlohmann::json{{"protocol_version", desc.protocol_version},
                           {"reply_msg", desc.reply_msg}};
        return;
    }
    j = nlohmann::json{{"protocol_version", desc.protocol_version},
                       {"segment_name", desc.segment_name},
                       {"local_nic_path", desc.local_nic_path},
                       {"peer_nic_path", desc.peer_nic_path},
                       {"local_device_name", desc.local_device_name},
                       {"local_device_id", desc.local_device_id},
                       {"local_eid_index", desc.local_eid_index},
                       {"local_eid", desc.local_eid},
                       {"endpoint_generation", desc.endpoint_generation},
                       {"segment_generation", desc.segment_generation},
                       {"reply_msg", desc.reply_msg},
                       {"rm", *desc.rm}};
}

inline void from_json(const nlohmann::json& j, UbBootstrapDesc& desc) {
    desc = UbBootstrapDesc{};
    if (!j.contains("protocol_version")) {
        throw std::invalid_argument("missing UB bootstrap version");
    }
    desc.protocol_version =
        ubBootstrapJsonUnsigned<uint32_t>(j, "protocol_version");
    if (desc.protocol_version == 1) {
        desc.segment_name = j.value("segment_name", "");
        desc.local_nic_path = j.value("local_nic_path", "");
        desc.peer_nic_path = j.value("peer_nic_path", "");
        desc.local_device_name = j.value("local_device_name", "");
        desc.local_device_id = j.value("local_device_id", -1);
        desc.local_eid_index = j.value("local_eid_index", -1);
        desc.local_eid = j.value("local_eid", "");
        desc.jetty_ids = j.value("jetty_ids", std::vector<uint32_t>{});
        desc.jetty_uasids = j.value("jetty_uasids", std::vector<uint32_t>{});
        desc.endpoint_generation = j.value("endpoint_generation", uint64_t{0});
        desc.segment_generation = j.value("segment_generation", uint64_t{0});
        desc.capabilities = j.value("capabilities", std::vector<std::string>{});
        desc.reply_msg = j.value("reply_msg", "");
        return;
    }
    if (desc.protocol_version != 2) {
        throw std::invalid_argument("unsupported UB bootstrap version");
    }

    desc.reply_msg = j.value("reply_msg", "");
    if (!j.contains("rm")) {
        if (desc.reply_msg.empty()) {
            throw std::invalid_argument("missing UB bootstrap RM payload");
        }
        return;
    }
    desc.segment_name = j.at("segment_name").get<std::string>();
    desc.local_nic_path = j.at("local_nic_path").get<std::string>();
    desc.peer_nic_path = j.at("peer_nic_path").get<std::string>();
    desc.local_device_name = j.at("local_device_name").get<std::string>();
    desc.local_device_id = ubBootstrapJsonUnsigned<int>(j, "local_device_id");
    desc.local_eid_index = ubBootstrapJsonUnsigned<int>(j, "local_eid_index");
    desc.local_eid = j.at("local_eid").get<std::string>();
    desc.endpoint_generation =
        ubBootstrapJsonUnsigned<uint64_t>(j, "endpoint_generation");
    desc.segment_generation =
        ubBootstrapJsonUnsigned<uint64_t>(j, "segment_generation");
    desc.rm = j.at("rm").get<UbRmBootstrapDesc>();
}

}  // namespace mooncake::tent

#endif  // TENT_TRANSPORT_UB_BOOTSTRAP_TYPES_H_
