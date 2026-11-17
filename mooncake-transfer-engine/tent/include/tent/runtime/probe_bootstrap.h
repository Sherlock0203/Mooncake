// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Wire-neutral probe endpoint descriptors exchanged by the TENT control plane.
// This header intentionally has no URMA type dependency: the area descriptor is
// an opaque, versioned envelope and is validated before it reaches the adapter.

#ifndef TENT_RUNTIME_PROBE_BOOTSTRAP_H_
#define TENT_RUNTIME_PROBE_BOOTSTRAP_H_

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include "tent/common/status.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {

inline constexpr uint32_t kProbeBootstrapSchemaVersion = 1;
inline constexpr uint32_t kProbeAreaSchemaVersion = 1;
inline constexpr size_t kProbeMaxEndpoints = 256;
inline constexpr uint64_t kProbeMaxAreaLength = 64 * 1024;
inline constexpr size_t kProbeMaxAreaHexBytes = 64 * 1024;

struct ProbeEndpointKey {
    std::string node_id;
    std::string native_device_name;
    uint32_t eid_index{0};

    bool operator==(const ProbeEndpointKey& other) const {
        return node_id == other.node_id &&
               native_device_name == other.native_device_name &&
               eid_index == other.eid_index;
    }
    bool operator<(const ProbeEndpointKey& other) const {
        return std::tie(node_id, native_device_name, eid_index) <
               std::tie(other.node_id, other.native_device_name,
                        other.eid_index);
    }
};

struct ProbeRemoteJetty {
    std::string eid;
    uint32_t id{0};
    uint32_t uasid{0};
    uint32_t token{0};
};

// Mirrors SegmentDescriptor without depending on urma_adapter.h.
struct ProbeAreaDescriptor {
    uint32_t schema_version{0};
    uint32_t urma_api_version{0};
    uint32_t urma_abi_size{0};
    std::string hex;
};

struct ProbeEndpointDesc {
    ProbeEndpointKey key;
    std::string topology_name;
    std::string eid;
    int local_nic{-1};
    ProbeAreaDescriptor area;
    uint64_t area_length{0};
    uint32_t area_token{0};
    uint64_t area_epoch{0};
    ProbeRemoteJetty probe_jetty;
    uint64_t target_epoch{0};
    uint64_t endpoint_generation{0};
    bool node_gate_ok{true};
    bool port_gate_ok{true};
    bool profile_gate_ok{true};
};

struct ProbeBootstrapRequest {
    uint32_t schema_version{kProbeBootstrapSchemaVersion};
    std::string requester_node;
    uint64_t known_node_epoch{0};
};

struct ProbeBootstrapResponse {
    uint32_t schema_version{kProbeBootstrapSchemaVersion};
    std::string node_id;
    uint64_t node_epoch{0};
    std::string rpc_server_addr;
    uint64_t topology_epoch{0};
    bool unchanged{false};
    std::vector<ProbeEndpointDesc> endpoints;
    std::string reply_msg;
};

inline void to_json(nlohmann::json& j, const ProbeEndpointKey& key) {
    j = nlohmann::json{{"node_id", key.node_id},
                       {"native_device_name", key.native_device_name},
                       {"eid_index", key.eid_index}};
}

inline void from_json(const nlohmann::json& j, ProbeEndpointKey& key) {
    key.node_id = j.value("node_id", std::string());
    key.native_device_name = j.value("native_device_name", std::string());
    key.eid_index = j.value("eid_index", 0u);
}

inline void to_json(nlohmann::json& j, const ProbeRemoteJetty& jetty) {
    j = nlohmann::json{{"eid", jetty.eid},
                       {"id", jetty.id},
                       {"uasid", jetty.uasid},
                       {"token", jetty.token}};
}

inline void from_json(const nlohmann::json& j, ProbeRemoteJetty& jetty) {
    jetty.eid = j.value("eid", std::string());
    jetty.id = j.value("id", 0u);
    jetty.uasid = j.value("uasid", 0u);
    jetty.token = j.value("token", 0u);
}

inline void to_json(nlohmann::json& j, const ProbeAreaDescriptor& area) {
    j = nlohmann::json{{"schema_version", area.schema_version},
                       {"urma_api_version", area.urma_api_version},
                       {"urma_abi_size", area.urma_abi_size},
                       {"hex", area.hex}};
}

inline void from_json(const nlohmann::json& j, ProbeAreaDescriptor& area) {
    area.schema_version = j.value("schema_version", 0u);
    area.urma_api_version = j.value("urma_api_version", 0u);
    area.urma_abi_size = j.value("urma_abi_size", 0u);
    area.hex = j.value("hex", std::string());
}

inline void to_json(nlohmann::json& j, const ProbeEndpointDesc& endpoint) {
    j = nlohmann::json{{"key", endpoint.key},
                       {"topology_name", endpoint.topology_name},
                       {"eid", endpoint.eid},
                       {"local_nic", endpoint.local_nic},
                       {"area", endpoint.area},
                       {"area_length", endpoint.area_length},
                       {"area_token", endpoint.area_token},
                       {"area_epoch", endpoint.area_epoch},
                       {"probe_jetty", endpoint.probe_jetty},
                       {"target_epoch", endpoint.target_epoch},
                       {"endpoint_generation", endpoint.endpoint_generation},
                       {"node_gate_ok", endpoint.node_gate_ok},
                       {"port_gate_ok", endpoint.port_gate_ok},
                       {"profile_gate_ok", endpoint.profile_gate_ok}};
}

inline void from_json(const nlohmann::json& j, ProbeEndpointDesc& endpoint) {
    endpoint.key = j.value("key", ProbeEndpointKey{});
    endpoint.topology_name = j.value("topology_name", std::string());
    endpoint.eid = j.value("eid", std::string());
    endpoint.local_nic = j.value("local_nic", -1);
    endpoint.area = j.value("area", ProbeAreaDescriptor{});
    endpoint.area_length = j.value("area_length", 0ULL);
    endpoint.area_token = j.value("area_token", 0u);
    endpoint.area_epoch = j.value("area_epoch", 0ULL);
    endpoint.probe_jetty = j.value("probe_jetty", ProbeRemoteJetty{});
    endpoint.target_epoch = j.value("target_epoch", 0ULL);
    endpoint.endpoint_generation = j.value("endpoint_generation", 0ULL);
    endpoint.node_gate_ok = j.value("node_gate_ok", true);
    endpoint.port_gate_ok = j.value("port_gate_ok", true);
    endpoint.profile_gate_ok = j.value("profile_gate_ok", true);
}

inline void to_json(nlohmann::json& j, const ProbeBootstrapRequest& r) {
    j = nlohmann::json{{"schema_version", r.schema_version},
                       {"requester_node", r.requester_node},
                       {"known_node_epoch", r.known_node_epoch}};
}

inline void from_json(const nlohmann::json& j, ProbeBootstrapRequest& r) {
    r.schema_version = j.value("schema_version", 0u);
    r.requester_node = j.value("requester_node", std::string());
    r.known_node_epoch = j.value("known_node_epoch", 0ULL);
}

inline void to_json(nlohmann::json& j, const ProbeBootstrapResponse& r) {
    j = nlohmann::json{{"schema_version", r.schema_version},
                       {"node_id", r.node_id},
                       {"node_epoch", r.node_epoch},
                       {"rpc_server_addr", r.rpc_server_addr},
                       {"topology_epoch", r.topology_epoch},
                       {"unchanged", r.unchanged},
                       {"endpoints", r.endpoints},
                       {"reply_msg", r.reply_msg}};
}

inline void from_json(const nlohmann::json& j, ProbeBootstrapResponse& r) {
    r.schema_version = j.value("schema_version", 0u);
    r.node_id = j.value("node_id", std::string());
    r.node_epoch = j.value("node_epoch", 0ULL);
    r.rpc_server_addr = j.value("rpc_server_addr", std::string());
    r.topology_epoch = j.value("topology_epoch", 0ULL);
    r.unchanged = j.value("unchanged", false);
    r.endpoints = j.value("endpoints", std::vector<ProbeEndpointDesc>{});
    r.reply_msg = j.value("reply_msg", std::string());
}

// Validates a response before any descriptor is imported. Returns the first
// structural error; a caller must reject the whole response on failure.
inline Status ValidateProbeBootstrapResponse(
    const ProbeBootstrapResponse& response) {
    if (response.schema_version != kProbeBootstrapSchemaVersion) {
        return Status::InvalidArgument(
            "unsupported probe bootstrap schema" LOC_MARK);
    }
    if (!response.reply_msg.empty()) {
        return Status::RpcServiceError(response.reply_msg);
    }
    if (response.node_id.empty() || response.node_epoch == 0 ||
        response.topology_epoch == 0) {
        return Status::InvalidArgument(
            "probe bootstrap response has no node identity" LOC_MARK);
    }
    if (response.endpoints.size() > kProbeMaxEndpoints) {
        return Status::InvalidArgument(
            "probe bootstrap response has too many endpoints" LOC_MARK);
    }
    std::vector<ProbeEndpointKey> keys;
    keys.reserve(response.endpoints.size());
    for (const auto& endpoint : response.endpoints) {
        const auto& key = endpoint.key;
        if (key.node_id != response.node_id || key.native_device_name.empty()) {
            return Status::InvalidArgument(
                "probe endpoint has an invalid key" LOC_MARK);
        }
        if (endpoint.local_nic < 0 || endpoint.topology_name.empty() ||
            endpoint.eid.empty()) {
            return Status::InvalidArgument(
                "probe endpoint has no topology identity" LOC_MARK);
        }
        if (endpoint.area.schema_version != kProbeAreaSchemaVersion ||
            endpoint.area.hex.empty() ||
            endpoint.area.hex.size() > kProbeMaxAreaHexBytes) {
            return Status::InvalidArgument(
                "probe endpoint has an invalid area descriptor" LOC_MARK);
        }
        if (endpoint.area_length == 0 ||
            endpoint.area_length > kProbeMaxAreaLength) {
            return Status::InvalidArgument(
                "probe endpoint area_length is out of range" LOC_MARK);
        }
        if (endpoint.area_epoch == 0 || endpoint.target_epoch == 0 ||
            endpoint.probe_jetty.id == 0 || endpoint.probe_jetty.eid.empty() ||
            endpoint.probe_jetty.token == 0) {
            return Status::InvalidArgument(
                "probe endpoint has an incomplete identity" LOC_MARK);
        }
        for (const auto& seen : keys) {
            if (seen == key) {
                return Status::InvalidArgument(
                    "duplicate probe endpoint key" LOC_MARK);
            }
        }
        keys.push_back(key);
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_PROBE_BOOTSTRAP_H_
