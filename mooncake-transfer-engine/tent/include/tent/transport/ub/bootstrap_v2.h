// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3g-b: bootstrap v2 is a field addition, not a new protocol.
//
// The v1 envelope (`protocol_version == 1`) and every v1 field keep their exact
// meaning. A peer that advertises the `bootstrap_v2` capability may add the
// optional objects below; a peer that does not never receives them. Missing v2
// fields fall back to v1 behaviour, and a missing probe area means probing
// stays off for that peer - it is never guessed from nearby devices (design
// D-6/D-7).
//
// Design: docs/analyze/tent-ub-w3g-pool-claim-and-v2-design.md §5.2 / §7.

#ifndef TENT_TRANSPORT_UB_BOOTSTRAP_V2_H_
#define TENT_TRANSPORT_UB_BOOTSTRAP_V2_H_

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

inline constexpr uint32_t kUbBootstrapSchemaV1 = 1;
inline constexpr uint32_t kUbBootstrapSchemaV2 = 2;

// Capability strings. `bootstrap_v2` gates every v2 object; the two feature
// strings let a peer publish only what it understands.
inline constexpr const char* kUbCapabilityBootstrapV2 = "bootstrap_v2";
inline constexpr const char* kUbCapabilityProbeAreaV1 = "probe_area_v1";
inline constexpr const char* kUbCapabilityTargetProfileV1 = "target_profile_v1";

[[nodiscard]] inline bool hasUbCapability(
    const std::vector<std::string>& capabilities, const char* capability) {
    for (const auto& entry : capabilities) {
        if (entry == capability) return true;
    }
    return false;
}

// True when the peer's capability list permits the v2 field block.
[[nodiscard]] inline bool ubBootstrapV2Capable(
    const std::vector<std::string>& peer_capabilities) {
    return hasUbCapability(peer_capabilities, kUbCapabilityBootstrapV2);
}

// Strict unsigned reader for v2 fields: a missing key, a negative value, a
// floating point, a string or an out-of-range number all throw. Callers wrap
// parsing in the usual invalid_argument path, which is the fail-closed
// contract.
template <typename T>
T ubBootstrapJsonUnsigned(const nlohmann::json& j, const char* key) {
    if (!j.is_object() || !j.contains(key)) {
        throw std::invalid_argument(
            std::string("missing UB bootstrap field: ") + key);
    }
    const auto& value = j.at(key);
    uint64_t parsed = 0;
    if (value.is_number_unsigned()) {
        parsed = value.get<uint64_t>();
    } else if (value.is_number_integer()) {
        const int64_t signed_value = value.get<int64_t>();
        if (signed_value < 0) {
            throw std::invalid_argument(
                std::string("UB bootstrap field must not be negative: ") + key);
        }
        parsed = static_cast<uint64_t>(signed_value);
    } else {
        throw std::invalid_argument(
            std::string("UB bootstrap field must be an integer: ") + key);
    }
    if (parsed > static_cast<uint64_t>(std::numeric_limits<T>::max())) {
        throw std::invalid_argument(
            std::string("UB bootstrap field is out of range: ") + key);
    }
    return static_cast<T>(parsed);
}

// Mirror of urma target attributes. Zero means "provider default" and keeps the
// v1 behaviour; a nonzero profile is advisory until RM legacy imports targets.
struct UbRemoteTargetProfile {
    uint16_t trans_mode = 0;
    uint8_t target_type = 0;
    uint8_t tp_type = 0;

    bool operator==(const UbRemoteTargetProfile&) const = default;
};

inline void to_json(nlohmann::json& j, const UbRemoteTargetProfile& profile) {
    j = nlohmann::json{{"trans_mode", profile.trans_mode},
                       {"target_type", profile.target_type},
                       {"tp_type", profile.tp_type}};
}

inline void from_json(const nlohmann::json& j, UbRemoteTargetProfile& profile) {
    if (!j.is_object()) {
        throw std::invalid_argument(
            "UB remote target profile must be an object");
    }
    profile.trans_mode = ubBootstrapJsonUnsigned<uint16_t>(j, "trans_mode");
    profile.target_type = ubBootstrapJsonUnsigned<uint8_t>(j, "target_type");
    profile.tp_type = ubBootstrapJsonUnsigned<uint8_t>(j, "tp_type");
}

// Segment descriptor round-trip. Kept as the structured value rather than an
// opaque string so the receiving side validates schema/ABI before importing the
// peer page.
inline void to_json(nlohmann::json& j, const SegmentDescriptor& descriptor) {
    j = nlohmann::json{{"schema_version", descriptor.schema_version},
                       {"urma_api_version", descriptor.urma_api_version},
                       {"urma_abi_size", descriptor.urma_abi_size},
                       {"hex", descriptor.hex}};
}

inline void from_json(const nlohmann::json& j, SegmentDescriptor& descriptor) {
    if (!j.is_object()) {
        throw std::invalid_argument("UB segment descriptor must be an object");
    }
    descriptor.schema_version =
        ubBootstrapJsonUnsigned<uint32_t>(j, "schema_version");
    descriptor.urma_api_version =
        ubBootstrapJsonUnsigned<uint32_t>(j, "urma_api_version");
    descriptor.urma_abi_size =
        ubBootstrapJsonUnsigned<uint32_t>(j, "urma_abi_size");
    if (!j.contains("hex") || !j.at("hex").is_string()) {
        throw std::invalid_argument(
            "UB segment descriptor hex must be a string");
    }
    descriptor.hex = j.at("hex").get<std::string>();
}

// One peer probe area. `descriptor` is the peer's registered page; the local
// side imports it and never invents an address (design §7.2 / IN-4).
struct UbProbeAreaDescriptor {
    SegmentDescriptor descriptor;
    uint64_t address = 0;
    uint64_t length = 0;
    uint32_t token = 0;
    uint64_t area_epoch = 0;
    uint32_t uasid = 0;
    uint32_t jetty_id = 0;

    bool operator==(const UbProbeAreaDescriptor&) const = default;
};

inline void to_json(nlohmann::json& j, const UbProbeAreaDescriptor& area) {
    j = nlohmann::json{
        {"descriptor", area.descriptor}, {"address", area.address},
        {"length", area.length},         {"token", area.token},
        {"area_epoch", area.area_epoch}, {"uasid", area.uasid},
        {"jetty_id", area.jetty_id}};
}

inline void from_json(const nlohmann::json& j, UbProbeAreaDescriptor& area) {
    if (!j.is_object()) {
        throw std::invalid_argument("UB probe area must be an object");
    }
    area.descriptor = j.at("descriptor").get<SegmentDescriptor>();
    area.address = ubBootstrapJsonUnsigned<uint64_t>(j, "address");
    area.length = ubBootstrapJsonUnsigned<uint64_t>(j, "length");
    area.token = ubBootstrapJsonUnsigned<uint32_t>(j, "token");
    area.area_epoch = ubBootstrapJsonUnsigned<uint64_t>(j, "area_epoch");
    area.uasid = ubBootstrapJsonUnsigned<uint32_t>(j, "uasid");
    area.jetty_id = ubBootstrapJsonUnsigned<uint32_t>(j, "jetty_id");
    if (area.length == 0 || area.jetty_id == 0 || area.descriptor.hex.empty()) {
        throw std::invalid_argument(
            "UB probe area requires a nonzero length, Jetty and descriptor");
    }
}

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_BOOTSTRAP_V2_H_
