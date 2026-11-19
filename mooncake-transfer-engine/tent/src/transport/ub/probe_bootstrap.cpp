// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_bootstrap.h"

#include <optional>
#include <unordered_set>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake::tent::ub {
namespace {

using nlohmann::json;

Status invalid(const std::string& reason) {
    return Status::InvalidArgument("invalid ProbeBootstrapUb: " + reason);
}

uint64_t readUnsigned(const json& value, const char* key, uint64_t fallback) {
    auto it = value.find(key);
    if (it == value.end() || it->is_null()) return fallback;
    if (!it->is_number_unsigned() && !it->is_number_integer()) {
        throw std::runtime_error(std::string(key) + " must be an integer");
    }
    return it->get<uint64_t>();
}

bool readBool(const json& value, const char* key, bool fallback) {
    auto it = value.find(key);
    if (it == value.end() || it->is_null()) return fallback;
    if (!it->is_boolean()) {
        throw std::runtime_error(std::string(key) + " must be a boolean");
    }
    return it->get<bool>();
}

std::string readString(const json& value, const char* key,
                       const std::string& fallback = {}) {
    auto it = value.find(key);
    if (it == value.end() || it->is_null()) return fallback;
    if (!it->is_string()) {
        throw std::runtime_error(std::string(key) + " must be a string");
    }
    return it->get<std::string>();
}

json encodeArea(const ProbeBootstrapPort& port) {
    return json{
        {"descriptor",
         json{{"schema_version", port.area_descriptor.schema_version},
              {"urma_api_version", port.area_descriptor.urma_api_version},
              {"urma_abi_size", port.area_descriptor.urma_abi_size},
              {"hex", port.area_descriptor.hex}}},
        {"length", port.area_length},
        {"epoch", port.area_epoch},
        {"token", port.area_token}};
}

void decodeArea(const json& area, ProbeBootstrapPort& port) {
    if (!area.is_object()) throw std::runtime_error("area must be an object");
    const auto descriptor = area.find("descriptor");
    if (descriptor == area.end() || !descriptor->is_object()) {
        throw std::runtime_error("area.descriptor must be an object");
    }
    port.area_descriptor.schema_version =
        static_cast<uint32_t>(readUnsigned(*descriptor, "schema_version", 0));
    port.area_descriptor.urma_api_version =
        static_cast<uint32_t>(readUnsigned(*descriptor, "urma_api_version", 0));
    port.area_descriptor.urma_abi_size =
        static_cast<uint32_t>(readUnsigned(*descriptor, "urma_abi_size", 0));
    port.area_descriptor.hex = readString(*descriptor, "hex");
    port.area_length = readUnsigned(area, "length", 0);
    port.area_epoch = readUnsigned(area, "epoch", 0);
    port.area_token = static_cast<uint32_t>(readUnsigned(area, "token", 0));
}

json encodePort(const ProbeBootstrapPort& port) {
    return json{{"port_id", port.port_id},
                {"topology_name", port.topology_name},
                {"eid", port.eid},
                {"target_epoch", port.target_epoch},
                {"jetty", json{{"eid", port.jetty.eid},
                               {"uasid", port.jetty.uasid},
                               {"id", port.jetty.id},
                               {"token", port.jetty.token}}},
                {"area", encodeArea(port)},
                {"gates", json{{"node", port.node_gate_ok},
                               {"port", port.port_gate_ok},
                               {"profile", port.profile_gate_ok}}}};
}

void decodePort(const json& value, ProbeBootstrapPort& port) {
    if (!value.is_object()) throw std::runtime_error("port must be an object");
    port.port_id = readString(value, "port_id");
    port.topology_name = readString(value, "topology_name");
    port.eid = readString(value, "eid");
    port.target_epoch = readUnsigned(value, "target_epoch", 0);
    const auto jetty = value.find("jetty");
    if (jetty == value.end() || !jetty->is_object()) {
        throw std::runtime_error("port.jetty must be an object");
    }
    port.jetty.eid = readString(*jetty, "eid");
    port.jetty.uasid = static_cast<uint32_t>(readUnsigned(*jetty, "uasid", 0));
    port.jetty.id = static_cast<uint32_t>(readUnsigned(*jetty, "id", 0));
    port.jetty.token = static_cast<uint32_t>(readUnsigned(*jetty, "token", 0));
    const auto area = value.find("area");
    if (area == value.end()) throw std::runtime_error("port.area is missing");
    decodeArea(*area, port);
    const auto gates = value.find("gates");
    if (gates != value.end() && gates->is_object()) {
        port.node_gate_ok = readBool(*gates, "node", false);
        port.port_gate_ok = readBool(*gates, "port", false);
        port.profile_gate_ok = readBool(*gates, "profile", false);
    }
}

json encodeRequest(const ProbeBootstrapRequest& request) {
    json ports = json::array();
    for (const auto& port : request.ports) ports.push_back(encodePort(port));
    json peers = json::object();
    for (const auto& [peer, peer_ports] : request.declared_peers) {
        json list = json::array();
        for (const auto& port : peer_ports) {
            list.push_back(json{{"id", port.id}, {"eid", port.eid}});
        }
        peers[peer] = std::move(list);
    }
    return json{
        {"schema_version", request.schema_version},
        {"requester_segment", request.requester_segment},
        {"requester_node_incarnation", request.requester_node_incarnation},
        {"requester_topology_epoch", request.requester_topology_epoch},
        {"requester_publication_epoch", request.requester_publication_epoch},
        {"declared_local_ports", request.declared_local_ports},
        {"ports", std::move(ports)},
        {"declared_peers", std::move(peers)}};
}

json encodeResponse(const ProbeBootstrapResponse& response) {
    json ports = json::array();
    for (const auto& port : response.ports) ports.push_back(encodePort(port));
    return json{{"schema_version", response.schema_version},
                {"reply_msg", response.reply_msg},
                {"responder_segment", response.responder_segment},
                {"node_incarnation", response.node_incarnation},
                {"topology_epoch", response.topology_epoch},
                {"publication_epoch", response.publication_epoch},
                {"transport_mode", response.transport_mode},
                {"device_kind", response.device_kind},
                {"ports", std::move(ports)}};
}

Status validatePort(const ProbeBootstrapPort& port, const char* side) {
    if (port.port_id.empty())
        return invalid(std::string(side) + " has an empty port_id");
    if (port.topology_name.empty()) {
        return invalid(std::string(side) + " has an empty topology_name");
    }
    if (!isCanonicalEid(port.eid)) {
        return invalid(std::string(side) + " has a malformed EID");
    }
    if (port.target_epoch == 0) {
        return invalid(std::string(side) + " has target_epoch 0");
    }
    if (port.jetty.id == 0 || !isCanonicalEid(port.jetty.eid)) {
        return invalid(std::string(side) + " has a malformed probe Jetty");
    }
    if (port.area_length == 0 || port.area_epoch == 0 ||
        port.area_descriptor.hex.empty() || port.area_token == 0) {
        return invalid(std::string(side) + " has a malformed probe area");
    }
    return Status::OK();
}

}  // namespace

ProbeBootstrapPort toProbeBootstrapPort(const ProbeLocalPortPublication& port) {
    ProbeBootstrapPort wire;
    wire.port_id = port.port_id;
    wire.topology_name = port.topology_name;
    wire.eid = port.eid;
    wire.target_epoch = port.target_epoch;
    wire.jetty = port.jetty;
    wire.area_descriptor = port.area_descriptor;
    wire.area_length = port.area_length;
    wire.area_epoch = port.area_epoch;
    wire.area_token = port.area_token;
    wire.node_gate_ok = port.node_gate_ok;
    wire.port_gate_ok = port.port_gate_ok;
    wire.profile_gate_ok = port.profile_gate_ok;
    return wire;
}

ProbeLocalPortPublication fromProbeBootstrapPort(
    const ProbeBootstrapPort& port) {
    ProbeLocalPortPublication local;
    local.port_id = port.port_id;
    local.topology_name = port.topology_name;
    local.eid = port.eid;
    local.target_epoch = port.target_epoch;
    local.jetty = port.jetty;
    local.area_descriptor = port.area_descriptor;
    local.area_length = port.area_length;
    local.area_epoch = port.area_epoch;
    local.area_token = port.area_token;
    local.node_gate_ok = port.node_gate_ok;
    local.port_gate_ok = port.port_gate_ok;
    local.profile_gate_ok = port.profile_gate_ok;
    return local;
}

Status validateProbeBootstrapRequest(const ProbeBootstrapRequest& request) {
    if (request.schema_version != kProbeBootstrapSchemaVersion) {
        return invalid("unsupported schema version");
    }
    if (request.requester_segment.empty()) {
        return invalid("requester_segment is empty");
    }
    if (request.requester_node_incarnation == 0 ||
        request.requester_topology_epoch == 0 ||
        request.requester_publication_epoch == 0) {
        return invalid("requester identity epochs must be non-zero");
    }
    if (request.declared_local_ports.empty()) {
        return invalid("declared_local_ports must not be empty");
    }
    if (request.ports.empty()) return invalid("ports must not be empty");
    std::unordered_set<std::string> ids;
    std::unordered_set<std::string> eids;
    for (const auto& port : request.ports) {
        auto status = validatePort(port, "requester port");
        if (!status.ok()) return status;
        if (!ids.insert(port.port_id).second) {
            return invalid("duplicate requester port_id");
        }
        if (!eids.insert(port.eid).second) {
            return invalid("duplicate requester EID");
        }
    }
    return Status::OK();
}

Status validateProbeBootstrapResponse(const ProbeBootstrapResponse& response) {
    if (response.schema_version != kProbeBootstrapSchemaVersion) {
        return invalid("unsupported schema version");
    }
    if (!response.reply_msg.empty()) {
        return Status::RpcServiceError(response.reply_msg);
    }
    if (response.responder_segment.empty()) {
        return invalid("responder_segment is empty");
    }
    if (response.node_incarnation == 0 || response.topology_epoch == 0 ||
        response.publication_epoch == 0) {
        return invalid("responder identity epochs must be non-zero");
    }
    if (response.transport_mode != kProbeBootstrapTransportMode) {
        return invalid("probe transport mode must be rm");
    }
    if (response.device_kind != kProbeBootstrapDeviceKind) {
        return invalid("probe device kind must be physical");
    }
    if (response.ports.empty()) return invalid("ports must not be empty");
    std::unordered_set<std::string> ids;
    std::unordered_set<std::string> eids;
    for (const auto& port : response.ports) {
        auto status = validatePort(port, "responder port");
        if (!status.ok()) return status;
        if (!ids.insert(port.port_id).second) {
            return invalid("duplicate responder port_id");
        }
        if (!eids.insert(port.eid).second) {
            return invalid("duplicate responder EID");
        }
    }
    return Status::OK();
}

Status encodeProbeBootstrapRequest(const ProbeBootstrapRequest& request,
                                   std::string& output) {
    try {
        output = encodeRequest(request).dump();
    } catch (const std::exception& error) {
        return Status::InvalidArgument(std::string("cannot encode probe "
                                                   "bootstrap request: ") +
                                       error.what());
    }
    return Status::OK();
}

Status decodeProbeBootstrapRequest(std::string_view input,
                                   ProbeBootstrapRequest& output) {
    try {
        const auto value = json::parse(input.begin(), input.end());
        if (!value.is_object()) return invalid("request must be an object");
        ProbeBootstrapRequest request;
        request.schema_version =
            static_cast<uint32_t>(readUnsigned(value, "schema_version", 0));
        request.requester_segment = readString(value, "requester_segment");
        request.requester_node_incarnation =
            readUnsigned(value, "requester_node_incarnation", 0);
        request.requester_topology_epoch =
            readUnsigned(value, "requester_topology_epoch", 0);
        request.requester_publication_epoch =
            readUnsigned(value, "requester_publication_epoch", 0);
        auto ports = value.find("ports");
        if (ports == value.end() || !ports->is_array()) {
            return invalid("ports must be an array");
        }
        for (const auto& entry : *ports) {
            ProbeBootstrapPort port;
            decodePort(entry, port);
            request.ports.push_back(std::move(port));
        }
        auto local_ports = value.find("declared_local_ports");
        if (local_ports != value.end() && local_ports->is_array()) {
            for (const auto& entry : *local_ports) {
                if (!entry.is_string()) {
                    return invalid("declared_local_ports must be strings");
                }
                request.declared_local_ports.push_back(
                    entry.get<std::string>());
            }
        }
        auto peers = value.find("declared_peers");
        if (peers != value.end() && peers->is_object()) {
            for (const auto& peer : peers->items()) {
                if (!peer.value().is_array()) {
                    return invalid("declared_peers entries must be arrays");
                }
                std::vector<ProbeBootstrapPeerPort> ports;
                for (const auto& entry : peer.value()) {
                    if (!entry.is_object()) {
                        return invalid("declared_peers ports must be objects");
                    }
                    ports.push_back(ProbeBootstrapPeerPort{
                        readString(entry, "id"), readString(entry, "eid")});
                }
                request.declared_peers.emplace(peer.key(), std::move(ports));
            }
        }
        output = std::move(request);
    } catch (const std::exception& error) {
        return Status::MalformedJson(
            std::string("Malformed ProbeBootstrapUb request: ") + error.what());
    }
    return Status::OK();
}

Status encodeProbeBootstrapResponse(const ProbeBootstrapResponse& response,
                                    std::string& output) {
    try {
        output = encodeResponse(response).dump();
    } catch (const std::exception& error) {
        return Status::InvalidArgument(std::string("cannot encode probe "
                                                   "bootstrap response: ") +
                                       error.what());
    }
    return Status::OK();
}

Status decodeProbeBootstrapResponse(std::string_view input,
                                    ProbeBootstrapResponse& output) {
    try {
        const auto value = json::parse(input.begin(), input.end());
        if (!value.is_object()) return invalid("response must be an object");
        ProbeBootstrapResponse response;
        response.schema_version =
            static_cast<uint32_t>(readUnsigned(value, "schema_version", 0));
        response.reply_msg = readString(value, "reply_msg");
        response.responder_segment = readString(value, "responder_segment");
        response.node_incarnation = readUnsigned(value, "node_incarnation", 0);
        response.topology_epoch = readUnsigned(value, "topology_epoch", 0);
        response.publication_epoch =
            readUnsigned(value, "publication_epoch", 0);
        response.transport_mode = readString(value, "transport_mode");
        response.device_kind = readString(value, "device_kind");
        auto ports = value.find("ports");
        if (ports != value.end() && ports->is_array()) {
            for (const auto& entry : *ports) {
                ProbeBootstrapPort port;
                decodePort(entry, port);
                response.ports.push_back(std::move(port));
            }
        }
        output = std::move(response);
    } catch (const std::exception& error) {
        return Status::MalformedJson(
            std::string("Malformed ProbeBootstrapUb response: ") +
            error.what());
    }
    return Status::OK();
}

}  // namespace mooncake::tent::ub
