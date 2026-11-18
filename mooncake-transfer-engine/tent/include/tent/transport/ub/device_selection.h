// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_DEVICE_SELECTION_H_
#define TENT_TRANSPORT_UB_DEVICE_SELECTION_H_

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

// An explicit identity is an administrator-approved classification record, not
// a name-pattern inference. transport_modes must exactly match the live
// provider snapshot so a stale record cannot be attached to another device.
struct DeviceIdentity {
    std::string native_device_name;
    std::string native_device_path;
    uint32_t eid_index = 0;
    std::string eid;
    DeviceKind kind = DeviceKind::UNKNOWN;
    uint16_t transport_modes = 0;
};

inline bool parseDeviceKind(std::string_view value, DeviceKind& kind) {
    if (value == "unknown") {
        kind = DeviceKind::UNKNOWN;
    } else if (value == "physical") {
        kind = DeviceKind::PHYSICAL;
    } else if (value == "bonding") {
        kind = DeviceKind::BONDING;
    } else if (value == "virtual") {
        kind = DeviceKind::VIRTUAL;
    } else if (value == "aggregate") {
        kind = DeviceKind::AGGREGATE;
    } else {
        return false;
    }
    return true;
}

inline bool parseDeviceKindSource(std::string_view value,
                                  DeviceKindSource& source) {
    if (value == "none") {
        source = DeviceKindSource::NONE;
    } else if (value == "explicit_config") {
        source = DeviceKindSource::EXPLICIT_CONFIG;
    } else if (value == "validated_metadata") {
        source = DeviceKindSource::VALIDATED_METADATA;
    } else {
        return false;
    }
    return true;
}

inline Status validateDeviceIdentityRecord(const DeviceIdentity& identity) {
    if (identity.native_device_name.empty() ||
        identity.native_device_path.empty() || identity.eid.empty()) {
        return Status::InvalidArgument(
            "UB device identity requires native name, path and EID");
    }
    if (identity.kind == DeviceKind::UNKNOWN) {
        return Status::InvalidArgument(
            "UB device identity must have a trusted non-unknown kind");
    }
    if (identity.transport_modes == 0 ||
        (identity.transport_modes &
         static_cast<uint16_t>(~kUrmaTransportModeMask)) != 0) {
        return Status::InvalidArgument(
            "UB device identity transport_modes contains unsupported bits");
    }
    return Status::OK();
}

inline bool sameDeviceLocator(const DeviceIdentity& identity,
                              const DeviceInfo& device) {
    return identity.native_device_name == device.native_device_name &&
           identity.native_device_path == device.native_device_path &&
           identity.eid_index == device.eid_index && identity.eid == device.eid;
}

inline bool sameDeviceLocator(const DeviceIdentity& lhs,
                              const DeviceIdentity& rhs) {
    return lhs.native_device_name == rhs.native_device_name &&
           lhs.native_device_path == rhs.native_device_path &&
           lhs.eid_index == rhs.eid_index && lhs.eid == rhs.eid;
}

// Resolves classifications without changing discovery order. Every configured
// record and every matched native identity must be unique. Unmapped devices
// remain UNKNOWN and can never pass the physical-only filter.
inline Status resolveDeviceKindIdentities(
    const std::vector<DeviceIdentity>& identities,
    std::vector<DeviceInfo>& devices) {
    for (size_t i = 0; i < identities.size(); ++i) {
        CHECK_STATUS(validateDeviceIdentityRecord(identities[i]));
        for (size_t j = 0; j < i; ++j) {
            if (sameDeviceLocator(identities[i], identities[j])) {
                return Status::InvalidArgument(
                    "duplicate UB device identity configuration");
            }
        }
    }

    std::vector<size_t> match_counts(identities.size(), 0);
    for (auto& device : devices) {
        device.kind = DeviceKind::UNKNOWN;
        device.kind_source = DeviceKindSource::NONE;
        for (size_t i = 0; i < identities.size(); ++i) {
            if (!sameDeviceLocator(identities[i], device)) continue;
            if (++match_counts[i] > 1) {
                return Status::InvalidArgument(
                    "multiple discovered devices match one UB identity");
            }
            if (device.capabilities.transport_modes !=
                identities[i].transport_modes) {
                return Status::InvalidArgument(
                    "UB device transport_modes differ from trusted identity");
            }
            device.kind = identities[i].kind;
            device.kind_source = DeviceKindSource::EXPLICIT_CONFIG;
        }
    }
    for (size_t i = 0; i < match_counts.size(); ++i) {
        if (match_counts[i] == 0) {
            return Status::DeviceNotFound(
                "trusted UB device identity is absent or stale: " +
                identities[i].native_device_name);
        }
    }
    return Status::OK();
}

inline std::vector<DeviceInfo> physicalDevices(
    const std::vector<DeviceInfo>& devices) {
    std::vector<DeviceInfo> selected;
    for (const auto& device : devices) {
        if (device.kind == DeviceKind::PHYSICAL &&
            device.kind_source != DeviceKindSource::NONE && device.active) {
            selected.push_back(device);
        }
    }
    return selected;
}

// Heuristic used only by the legacy selection mode. A resolved kind always
// takes precedence, so physical-only never infers PHYSICAL from a device name.
inline bool isBondingDeviceName(std::string_view name) {
    std::string lower(name);
    std::transform(
        lower.begin(), lower.end(), lower.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (lower.rfind("bonding", 0) == 0) return true;
    if (lower.find(":bonding") != std::string::npos) return true;
    if (lower.find("_bond") != std::string::npos) return true;
    if (lower.find("-bond") != std::string::npos) return true;
    return false;
}

inline bool isBondingDevice(const DeviceInfo& device) {
    if (device.kind_source != DeviceKindSource::NONE) {
        return device.kind == DeviceKind::BONDING;
    }
    return isBondingDeviceName(device.native_device_name) ||
           isBondingDeviceName(device.topology_name);
}

// When explicit_filter is true, returns devices unchanged (caller already
// applied device_filter). When false and at least one bonding device is
// present, returns only bonding devices; otherwise returns all devices.
inline std::vector<DeviceInfo> preferBondingDevicesIfPresent(
    const std::vector<DeviceInfo>& devices, bool explicit_filter) {
    if (explicit_filter || devices.empty()) return devices;
    const bool has_bonding =
        std::any_of(devices.begin(), devices.end(),
                    [](const DeviceInfo& d) { return isBondingDevice(d); });
    if (!has_bonding) return devices;

    std::vector<DeviceInfo> selected;
    selected.reserve(devices.size());
    for (const auto& device : devices) {
        if (isBondingDevice(device)) selected.push_back(device);
    }
    return selected;
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_DEVICE_SELECTION_H_
