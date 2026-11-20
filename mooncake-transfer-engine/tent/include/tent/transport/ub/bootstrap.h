// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_BOOTSTRAP_H_
#define TENT_TRANSPORT_UB_BOOTSTRAP_H_

#include <string>
#include <string_view>

#include "tent/common/status.h"
#include "tent/transport/ub/bootstrap_types.h"

namespace mooncake::tent::ub {

enum class UbBootstrapMode : uint8_t {
    RC,
    RM,
};

// Explicit bootstrap protocol selector (design §4.2). `kAuto` derives the
// required version from the runtime mode, which is the historical behavior.
// `kV1`/`kV2` force one version and reject the other instead of silently
// switching: an RM/RC or RM/UM peer mismatch must be a visible error, never a
// quiet fallback to v1.
enum class UbBootstrapProtocol : uint8_t {
    kAuto = 0,
    kV2,
    kV1,
};

inline const char* ubBootstrapProtocolName(UbBootstrapProtocol protocol) {
    switch (protocol) {
        case UbBootstrapProtocol::kAuto:
            return "auto";
        case UbBootstrapProtocol::kV2:
            return "v2";
        case UbBootstrapProtocol::kV1:
            return "v1";
    }
    return "unknown";
}

enum class UbBootstrapGenerationComparison : uint8_t {
    kDuplicate,
    kStale,
    kNewer,
    kConflict,
};

// Expected values come from a trusted local allowlist or validated remote
// segment metadata, never from the peer's self-asserted bootstrap fields.
struct UbExpectedPeerIdentity {
    std::string segment_name;
    std::string local_nic_path;
    std::string physical_device_identity;
    std::string native_device_name;
    int device_id = -1;
    int eid_index = -1;
    std::string eid;
    uint16_t transport_modes = 0;
};

// Canonical physical device identity used by bootstrap v2: the native device
// path plus the first EID group (for example "/dev/udma0#0001"). Both sides
// derive it from trusted local discovery/segment metadata; it is never taken
// from a peer's self-asserted fields.
std::string makePhysicalDeviceIdentity(std::string_view native_device_path,
                                       std::string_view eid);

// Validates the strict v2 wire contract. V1 retains its legacy optional-field
// behavior and is validated by the existing RC endpoint path.
Status validateUbBootstrap(const UbBootstrapDesc& desc);

// Checks only the version/mode contract. A successful RM v2 gate does not
// claim that the current runtime or provider implements the RM data plane.
// `requested` is the explicit selector: kAuto keeps the historical mode-derived
// behavior, kV2 rejects a v1 peer and kV1 rejects a v2 peer. A v1 peer is only
// served by the RC legacy runtime; RM (legacy or pooled) requires v2.
Status gateUbBootstrapForMode(UbBootstrapMode expected,
                              UbBootstrapProtocol requested,
                              const UbBootstrapDesc& desc);

Status validateUbBootstrapV2PeerIdentity(
    const UbBootstrapDesc& desc, const UbExpectedPeerIdentity& expected);

// Compares candidates for one stable physical device identity. The caller must
// publish only kNewer snapshots, treat kDuplicate as idempotent, and leave the
// incumbent untouched for kStale/kConflict.
Status compareUbBootstrapV2(const UbBootstrapDesc& candidate,
                            const UbBootstrapDesc& current,
                            UbBootstrapGenerationComparison& comparison);

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_BOOTSTRAP_H_
