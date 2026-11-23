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
// urma_tp_type_t. Local JFS priority/bind masks never belong to a remote
// target.
struct UbRemoteTargetProfile {
    uint16_t trans_mode = ub::kUrmaTransportModeRm;
    uint8_t target_type = ub::kUrmaTargetTypeJetty;
    uint8_t tp_type = ub::kUrmaTpTypeCtp;

    bool operator==(const UbRemoteTargetProfile&) const = default;
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
    profile.trans_mode = ubBootstrapJsonUnsigned<uint16_t>(j, "trans_mode");
    profile.target_type = ubBootstrapJsonUnsigned<uint8_t>(j, "target_type");
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

// Strict allowlist check shared by the pooled v3 decoder: every key present
// on the wire must be part of the frozen schema for that object. Unknown or
// v1/v2-only fields are rejected instead of being silently reinterpreted.
inline void ubBootstrapRequireOnlyKeys(const nlohmann::json& j,
                                       std::initializer_list<const char*> allowed,
                                       const char* context) {
    if (!j.is_object()) {
        throw std::invalid_argument(std::string(context) +
                                    " must be an object");
    }
    for (const auto& item : j.items()) {
        bool known = false;
        for (const char* key : allowed) {
            known = known || item.key() == key;
        }
        if (!known) {
            throw std::invalid_argument(
                std::string("field is not allowed in ") + context + ": " +
                item.key());
        }
    }
}

// Pooled bootstrap v3 (guidance C03 / V5 sections 5.2 and 23.2). Unlike the
// legacy v2 endpoint-owned target list, v3 publishes a shared per-port target
// inventory: one LocalPortTarget per configured port, referenced by business
// endpoints and probe resources alike. All fields below are required on the
// wire; nothing inherits from v1/v2 and unknown fields are rejected.
struct UbPooledPortDesc {
    std::string port_id;
    // Context mapping: the incarnation of the local Context that owns this
    // port's native resources. Nonzero and unique per port.
    uint64_t context_id = 0;
    std::string eid;
    uint32_t eid_index = 0;
};

inline void to_json(nlohmann::json& j, const UbPooledPortDesc& port) {
    j = nlohmann::json{{"port_id", port.port_id},
                       {"context_id", port.context_id},
                       {"eid", port.eid},
                       {"eid_index", port.eid_index}};
}

inline void from_json(const nlohmann::json& j, UbPooledPortDesc& port) {
    ubBootstrapRequireOnlyKeys(
        j, {"port_id", "context_id", "eid", "eid_index"}, "pooled port");
    port.port_id = j.at("port_id").get<std::string>();
    port.context_id = ubBootstrapJsonUnsigned<uint64_t>(j, "context_id");
    port.eid = j.at("eid").get<std::string>();
    port.eid_index = ubBootstrapJsonUnsigned<uint32_t>(j, "eid_index");
}

// Versioned opaque target descriptor bytes (hex on the wire). The payload is
// only consumed after schema/ABI matching and provider verification; EIDs and
// access rights are never guessed from these bytes.
struct UbPooledTargetDescriptor {
    std::string port_id;
    std::string target_identity;
    std::string eid;
    uint32_t uasid = 0;
    uint32_t id = 0;
    uint64_t target_epoch = 0;
    UbRemoteTargetProfile profile;
    std::string descriptor;
};

inline void to_json(nlohmann::json& j,
                    const UbPooledTargetDescriptor& target) {
    j = nlohmann::json{{"port_id", target.port_id},
                       {"target_identity", target.target_identity},
                       {"eid", target.eid},
                       {"uasid", target.uasid},
                       {"id", target.id},
                       {"target_epoch", target.target_epoch},
                       {"profile", target.profile},
                       {"descriptor", target.descriptor}};
}

inline void from_json(const nlohmann::json& j,
                      UbPooledTargetDescriptor& target) {
    ubBootstrapRequireOnlyKeys(j,
                               {"port_id", "target_identity", "eid", "uasid",
                                "id", "target_epoch", "profile", "descriptor"},
                               "pooled target");
    target.port_id = j.at("port_id").get<std::string>();
    target.target_identity = j.at("target_identity").get<std::string>();
    target.eid = j.at("eid").get<std::string>();
    target.uasid = ubBootstrapJsonUnsigned<uint32_t>(j, "uasid");
    target.id = ubBootstrapJsonUnsigned<uint32_t>(j, "id");
    target.target_epoch = ubBootstrapJsonUnsigned<uint64_t>(j, "target_epoch");
    target.profile = j.at("profile").get<UbRemoteTargetProfile>();
    target.descriptor = j.at("descriptor").get<std::string>();
}

// Probe authorization region bound to (peer, local port). Only WRITE access
// to an isolated region is granted; business addresses are never exposed to
// probe traffic. Slots are indexed with the given stride inside
// [offset, offset + length).
struct UbPooledProbeRegionDesc {
    std::string peer;
    std::string local_port;
    std::string target_identity;
    uint64_t target_epoch = 0;
    uint64_t region_generation = 0;
    std::string access;
    uint64_t offset = 0;
    uint64_t length = 0;
    uint64_t slots = 0;
    uint64_t stride = 0;
};

inline void to_json(nlohmann::json& j,
                    const UbPooledProbeRegionDesc& region) {
    j = nlohmann::json{{"peer", region.peer},
                       {"local_port", region.local_port},
                       {"target_identity", region.target_identity},
                       {"target_epoch", region.target_epoch},
                       {"region_generation", region.region_generation},
                       {"access", region.access},
                       {"offset", region.offset},
                       {"length", region.length},
                       {"slots", region.slots},
                       {"stride", region.stride}};
}

inline void from_json(const nlohmann::json& j,
                      UbPooledProbeRegionDesc& region) {
    ubBootstrapRequireOnlyKeys(j,
                               {"peer", "local_port", "target_identity",
                                "target_epoch", "region_generation", "access",
                                "offset", "length", "slots", "stride"},
                               "pooled probe region");
    region.peer = j.at("peer").get<std::string>();
    region.local_port = j.at("local_port").get<std::string>();
    region.target_identity = j.at("target_identity").get<std::string>();
    region.target_epoch =
        ubBootstrapJsonUnsigned<uint64_t>(j, "target_epoch");
    region.region_generation =
        ubBootstrapJsonUnsigned<uint64_t>(j, "region_generation");
    region.access = j.at("access").get<std::string>();
    region.offset = ubBootstrapJsonUnsigned<uint64_t>(j, "offset");
    region.length = ubBootstrapJsonUnsigned<uint64_t>(j, "length");
    region.slots = ubBootstrapJsonUnsigned<uint64_t>(j, "slots");
    region.stride = ubBootstrapJsonUnsigned<uint64_t>(j, "stride");
}

// Explicit READ/WRITE support and the business MR range the publisher grants.
struct UbPooledAccessCapabilities {
    bool read = false;
    bool write = false;
    uint64_t mr_offset = 0;
    uint64_t mr_length = 0;
};

inline void to_json(nlohmann::json& j,
                    const UbPooledAccessCapabilities& access) {
    j = nlohmann::json{{"read", access.read},
                       {"write", access.write},
                       {"mr_offset", access.mr_offset},
                       {"mr_length", access.mr_length}};
}

inline void from_json(const nlohmann::json& j,
                      UbPooledAccessCapabilities& access) {
    ubBootstrapRequireOnlyKeys(j, {"read", "write", "mr_offset", "mr_length"},
                               "pooled access capabilities");
    if (!j.at("read").is_boolean() || !j.at("write").is_boolean()) {
        throw std::invalid_argument(
            "pooled access capabilities read/write must be booleans");
    }
    access.read = j.at("read").get<bool>();
    access.write = j.at("write").get<bool>();
    access.mr_offset = ubBootstrapJsonUnsigned<uint64_t>(j, "mr_offset");
    access.mr_length = ubBootstrapJsonUnsigned<uint64_t>(j, "mr_length");
}

struct UbPooledBootstrapDesc {
    uint32_t schema_version = 3;
    std::string transport_mode;
    std::string device_kind;
    std::string physical_device_identity;
    uint64_t node_incarnation = 0;
    uint64_t context_incarnation = 0;
    uint64_t topology_epoch = 0;
    uint16_t transport_modes = 0;
    std::vector<UbPooledPortDesc> ports;
    // Adjacency view of the publishing node: local published port to the
    // remote port ids it may reach. Values reference peer-published ports
    // and are validated against the trusted peer inventory by the consumer.
    std::map<std::string, std::vector<std::string>> adjacency;
    std::string inventory_checksum;
    std::vector<UbPooledTargetDescriptor> targets;
    std::map<std::string, std::vector<std::string>> probe_target_map;
    std::vector<UbPooledProbeRegionDesc> probe_regions;
    std::vector<std::string> provider_capabilities;
    std::string descriptor_abi;
    UbPooledAccessCapabilities access_capabilities;
};

inline void to_json(nlohmann::json& j, const UbPooledBootstrapDesc& pooled) {
    j = nlohmann::json{{"schema_version", pooled.schema_version},
                       {"transport_mode", pooled.transport_mode},
                       {"device_kind", pooled.device_kind},
                       {"physical_device_identity",
                        pooled.physical_device_identity},
                       {"node_incarnation", pooled.node_incarnation},
                       {"context_incarnation", pooled.context_incarnation},
                       {"topology_epoch", pooled.topology_epoch},
                       {"transport_modes", pooled.transport_modes},
                       {"ports", pooled.ports},
                       {"adjacency", pooled.adjacency},
                       {"inventory_checksum", pooled.inventory_checksum},
                       {"targets", pooled.targets},
                       {"probe_target_map", pooled.probe_target_map},
                       {"probe_regions", pooled.probe_regions},
                       {"provider_capabilities", pooled.provider_capabilities},
                       {"descriptor_abi", pooled.descriptor_abi},
                       {"access_capabilities", pooled.access_capabilities}};
}

inline void from_json(const nlohmann::json& j, UbPooledBootstrapDesc& pooled) {
    ubBootstrapRequireOnlyKeys(
        j,
        {"schema_version", "transport_mode", "device_kind",
         "physical_device_identity", "node_incarnation", "context_incarnation",
         "topology_epoch", "transport_modes", "ports", "adjacency",
         "inventory_checksum", "targets", "probe_target_map", "probe_regions",
         "provider_capabilities", "descriptor_abi", "access_capabilities"},
        "pooled bootstrap");
    pooled.schema_version =
        ubBootstrapJsonUnsigned<uint32_t>(j, "schema_version");
    pooled.transport_mode = j.at("transport_mode").get<std::string>();
    pooled.device_kind = j.at("device_kind").get<std::string>();
    pooled.physical_device_identity =
        j.at("physical_device_identity").get<std::string>();
    pooled.node_incarnation =
        ubBootstrapJsonUnsigned<uint64_t>(j, "node_incarnation");
    pooled.context_incarnation =
        ubBootstrapJsonUnsigned<uint64_t>(j, "context_incarnation");
    pooled.topology_epoch =
        ubBootstrapJsonUnsigned<uint64_t>(j, "topology_epoch");
    pooled.transport_modes =
        ubBootstrapJsonUnsigned<uint16_t>(j, "transport_modes");
    pooled.ports = j.at("ports").get<std::vector<UbPooledPortDesc>>();
    pooled.adjacency =
        j.at("adjacency")
            .get<std::map<std::string, std::vector<std::string>>>();
    pooled.inventory_checksum =
        j.at("inventory_checksum").get<std::string>();
    pooled.targets =
        j.at("targets").get<std::vector<UbPooledTargetDescriptor>>();
    pooled.probe_target_map =
        j.at("probe_target_map")
            .get<std::map<std::string, std::vector<std::string>>>();
    pooled.probe_regions =
        j.at("probe_regions")
            .get<std::vector<UbPooledProbeRegionDesc>>();
    pooled.provider_capabilities =
        j.at("provider_capabilities").get<std::vector<std::string>>();
    pooled.descriptor_abi = j.at("descriptor_abi").get<std::string>();
    pooled.access_capabilities =
        j.at("access_capabilities").get<UbPooledAccessCapabilities>();
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
    // A v1 envelope that also carried a v2 `rm` block. The block is never
    // parsed and never reaches the runtime (sanitized ignore), but the caller
    // counts it so a peer that claims v1 while sending v2 fields is visible.
    bool ignored_v2_payload = false;
    // Pooled v3 envelope fields. local_port_identity is the publisher's
    // configured port id (never an index); local_device_identity is the
    // canonical full device identity. Both stay empty for v1/v2, whose wire
    // formats are frozen.
    std::string local_port_identity;
    std::string local_device_identity;
    std::optional<UbPooledBootstrapDesc> pooled;
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
    if (desc.protocol_version == 2) {
        if (!desc.rm.has_value()) {
            if (desc.reply_msg.empty()) {
                throw std::invalid_argument(
                    "invalid UB bootstrap v2 envelope");
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
        return;
    }
    if (desc.protocol_version != 3) {
        throw std::invalid_argument("invalid UB bootstrap version");
    }
    if (!desc.pooled.has_value()) {
        if (desc.reply_msg.empty()) {
            throw std::invalid_argument("invalid UB bootstrap v3 envelope");
        }
        j = nlohmann::json{{"protocol_version", desc.protocol_version},
                           {"reply_msg", desc.reply_msg}};
        return;
    }
    j = nlohmann::json{{"protocol_version", desc.protocol_version},
                       {"segment_name", desc.segment_name},
                       {"local_nic_path", desc.local_nic_path},
                       {"peer_nic_path", desc.peer_nic_path},
                       {"local_port_identity", desc.local_port_identity},
                       {"local_device_identity", desc.local_device_identity},
                       {"local_eid", desc.local_eid},
                       {"local_eid_index", desc.local_eid_index},
                       {"endpoint_generation", desc.endpoint_generation},
                       {"segment_generation", desc.segment_generation},
                       {"reply_msg", desc.reply_msg},
                       {"pooled", *desc.pooled}};
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
        // A v1 peer may not carry v2 fields: ignore them for real (they never
        // reach `rm`) and record the mismatch for the caller to count.
        desc.ignored_v2_payload = j.contains("rm");
        return;
    }
    if (desc.protocol_version != 2) {
        if (desc.protocol_version != 3) {
            throw std::invalid_argument("unsupported UB bootstrap version");
        }
        // v3 envelope: strict allowlist. Nothing inherits from v1/v2, and
        // unknown or legacy-only fields are rejected up front.
        ubBootstrapRequireOnlyKeys(j,
                                   {"protocol_version", "segment_name",
                                    "local_nic_path", "peer_nic_path",
                                    "local_port_identity",
                                    "local_device_identity", "local_eid",
                                    "local_eid_index", "endpoint_generation",
                                    "segment_generation", "reply_msg",
                                    "pooled"},
                                   "UB bootstrap v3 envelope");
        desc.reply_msg = j.contains("reply_msg")
                             ? j.at("reply_msg").get<std::string>()
                             : std::string();
        if (!j.contains("pooled")) {
            if (desc.reply_msg.empty()) {
                throw std::invalid_argument(
                    "missing UB bootstrap pooled payload");
            }
            return;
        }
        desc.segment_name = j.at("segment_name").get<std::string>();
        desc.local_nic_path = j.at("local_nic_path").get<std::string>();
        desc.peer_nic_path = j.at("peer_nic_path").get<std::string>();
        desc.local_port_identity =
            j.at("local_port_identity").get<std::string>();
        desc.local_device_identity =
            j.at("local_device_identity").get<std::string>();
        desc.local_eid = j.at("local_eid").get<std::string>();
        desc.local_eid_index =
            ubBootstrapJsonUnsigned<int>(j, "local_eid_index");
        desc.endpoint_generation =
            ubBootstrapJsonUnsigned<uint64_t>(j, "endpoint_generation");
        desc.segment_generation =
            ubBootstrapJsonUnsigned<uint64_t>(j, "segment_generation");
        desc.pooled = j.at("pooled").get<UbPooledBootstrapDesc>();
        return;
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
