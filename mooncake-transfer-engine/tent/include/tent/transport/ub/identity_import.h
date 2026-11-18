// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_TRANSPORT_UB_IDENTITY_IMPORT_H_
#define TENT_TRANSPORT_UB_IDENTITY_IMPORT_H_

#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/topology.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/bootstrap.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

// Encodes one discovered UB device into the JSON string published in the
// local segment metadata (DeviceDesc::transport_attrs[TransportType::UB]).
// The schema is versioned by "schema_version".
std::string encodeDeviceMetadata(Topology::NicID topology_id,
                                 const DeviceInfo& device);

// Decodes a metadata string produced by encodeDeviceMetadata() back into a
// DeviceInfo. Fails closed on parse errors, a missing/unknown schema
// version, or malformed required fields.
Status decodeUbDeviceMetadata(const std::string& attrs_json, DeviceInfo& out);

// Automatically collects local device identities from a provider discovery
// snapshot, covering every field that manual tent-perf.json configuration
// used to provide under transports/ub/device_identities:
// native_device_name, native_device_path, eid, eid_index, transport_modes
// and kind. Devices with incomplete identity fields are skipped; the call
// fails closed when no complete identity exists. Collected identities are
// stamped PHYSICAL, matching the manual generator
// (generate_rm_legacy_config.py); the RM capability gate at install time
// still verifies every selected device before any native resource is
// created.
Status collectLocalDeviceIdentities(const std::vector<DeviceInfo>& discovered,
                                    std::vector<DeviceIdentity>& out);

// Serializes identities into the transports/ub/device_identities JSON array
// schema (the exact layout manual configuration and
// generate_rm_legacy_config.py produce), so injected records stay readable
// by the existing UbParams::FromConfig path without any code change there.
nlohmann::json deviceIdentitiesToJson(
    const std::vector<DeviceIdentity>& identities);

// Builds the trusted expected peer identity from registered remote segment
// metadata: the device entry published by the peer's publishLocalDevices()
// during install. This replaces peer self-asserted bootstrap fields as the
// anchor for RM import; every required field (native_device_name,
// native_device_path, eid, eid_index, transport_modes, kind, active) is
// validated and the call fails closed when the entry is missing or
// malformed. `remote_topology_id` indexes the peer's own topology (the
// peer's local NIC ID).
Status anchorPeerIdentityFromSegment(const SegmentDesc& segment,
                                     Topology::NicID remote_topology_id,
                                     UbExpectedPeerIdentity& out);

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_IDENTITY_IMPORT_H_
