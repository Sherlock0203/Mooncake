// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/bootstrap.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "tent/transport/ub/sha256.h"

namespace mooncake::tent::ub {
namespace {

Status invalidBootstrap(const std::string& reason) {
    return Status::InvalidArgument("invalid UB bootstrap v2: " + reason);
}

bool isCanonicalNonzeroEid(const std::string& eid) {
    // URMA EIDs are rendered as eight groups of four hexadecimal digits.
    if (eid.size() != 39) return false;
    bool nonzero = false;
    for (size_t i = 0; i < eid.size(); ++i) {
        if ((i + 1) % 5 == 0) {
            if (eid[i] != ':') return false;
            continue;
        }
        const unsigned char c = static_cast<unsigned char>(eid[i]);
        if (!std::isxdigit(c)) return false;
        nonzero = nonzero || eid[i] != '0';
    }
    return nonzero;
}

Status invalidPooled(const std::string& reason) {
    return Status::InvalidArgument("invalid UB bootstrap v3: " + reason);
}

Status validateCapabilities(const std::vector<std::string>& capabilities,
                            bool pooled_v3) {
    const auto fail = [&](const std::string& reason) {
        return pooled_v3 ? invalidPooled(reason) : invalidBootstrap(reason);
    };
    if (capabilities.empty()) {
        return fail("provider capabilities are empty");
    }
    const auto has = [&](const std::string& capability) {
        return std::find(capabilities.begin(), capabilities.end(),
                         capability) != capabilities.end();
    };
    if (!has("read") || !has("write") || !has("rm")) {
        return fail("provider capabilities must include read, write and rm");
    }
    return Status::OK();
}

Status validateV2(const UbBootstrapDesc& desc) {
    if (desc.protocol_version != 2) {
        return invalidBootstrap("protocol_version must be 2");
    }
    if (!desc.rm.has_value()) return invalidBootstrap("missing RM payload");
    const auto& rm = *desc.rm;
    if (rm.transport_mode != "RM") {
        return invalidBootstrap("transport_mode must be RM");
    }
    if (rm.device_kind != "PHYSICAL") {
        return invalidBootstrap("device_kind must be PHYSICAL");
    }
    if (desc.segment_name.empty() || desc.local_nic_path.empty() ||
        desc.peer_nic_path.empty() || desc.local_device_name.empty() ||
        rm.physical_device_identity.empty()) {
        return invalidBootstrap("device or segment identity is incomplete");
    }
    if (desc.local_device_id < 0 || desc.local_eid_index < 0) {
        return invalidBootstrap("device or EID index is negative");
    }
    if (!isCanonicalNonzeroEid(desc.local_eid)) {
        return invalidBootstrap("local EID is malformed or zero");
    }
    if (desc.endpoint_generation == 0 || desc.segment_generation == 0 ||
        rm.node_incarnation == 0 || rm.context_incarnation == 0 ||
        rm.topology_epoch == 0) {
        return invalidBootstrap("all generations must be nonzero");
    }
    if ((rm.transport_modes & static_cast<uint16_t>(~kUrmaTransportModeMask)) !=
        0) {
        return invalidBootstrap(
            "provider transport_modes has unsupported bits");
    }
    if ((rm.transport_modes & kUrmaTransportModeRm) == 0) {
        return invalidBootstrap("provider transport_modes does not include RM");
    }
    CHECK_STATUS(validateCapabilities(rm.provider_capabilities, false));
    if (rm.targets.empty()) return invalidBootstrap("target list is empty");

    std::unordered_set<std::string> target_identities;
    std::unordered_set<std::string> native_identities;
    for (const auto& target : rm.targets) {
        if (target.target_identity.empty() || target.id == 0 ||
            target.target_epoch == 0) {
            return invalidBootstrap("target identity, ID or epoch is invalid");
        }
        if (!isCanonicalNonzeroEid(target.eid)) {
            return invalidBootstrap("target EID is malformed or zero");
        }
        // Remote targets carry only native target properties. JFS priority is
        // local-only and is validated in [0, kUrmaMaxPriority] by
        // buildLocalSendProfile and the native Jetty creation path.
        if (target.profile.trans_mode != kUrmaTransportModeRm) {
            return invalidBootstrap("target trans_mode must be URMA_TM_RM");
        }
        if (target.profile.target_type != kUrmaTargetTypeJetty) {
            return invalidBootstrap("target type must be URMA_JETTY");
        }
        if (target.profile.tp_type != kUrmaTpTypeCtp) {
            return invalidBootstrap("target tp_type must be URMA_CTP");
        }
        if (!target_identities.insert(target.target_identity).second) {
            return invalidBootstrap("duplicate target identity");
        }
        const std::string native_identity = target.eid + "/" +
                                            std::to_string(target.uasid) + "/" +
                                            std::to_string(target.id);
        if (!native_identities.insert(native_identity).second) {
            return invalidBootstrap("duplicate native target identity");
        }
    }

    for (const auto& [local_port, targets] : rm.probe_target_map) {
        if (local_port.empty() || targets.empty()) {
            return invalidBootstrap("probe target map entry is incomplete");
        }
        std::unordered_set<std::string> probes;
        for (const auto& identity : targets) {
            if (target_identities.find(identity) == target_identities.end()) {
                return invalidBootstrap("probe references an unknown target");
            }
            if (!probes.insert(identity).second) {
                return invalidBootstrap("duplicate probe target");
            }
        }
    }
    return Status::OK();
}

int compareUint64(uint64_t lhs, uint64_t rhs) {
    if (lhs < rhs) return -1;
    return lhs > rhs ? 1 : 0;
}

bool isNonEmptyHexString(const std::string& text) {
    if (text.empty() || text.size() % 2 != 0) return false;
    for (const unsigned char c : text) {
        if (!std::isxdigit(c)) return false;
    }
    return true;
}

// Sum-free bounds helper: rejects a + b combinations that would overflow.
bool sumWouldOverflow(uint64_t lhs, uint64_t rhs) {
    return lhs > std::numeric_limits<uint64_t>::max() - rhs;
}

Status validateV3(const UbBootstrapDesc& desc) {
    if (desc.protocol_version != 3) {
        return invalidPooled("protocol_version must be 3");
    }
    if (!desc.pooled.has_value()) {
        return invalidPooled("missing pooled payload");
    }
    const auto& pooled = *desc.pooled;

    // Envelope identity: strict non-empty canonical fields; the port identity
    // is a configured id, never a numeric index in disguise.
    if (desc.segment_name.empty() || desc.local_nic_path.empty() ||
        desc.peer_nic_path.empty() || desc.local_port_identity.empty() ||
        desc.local_device_identity.empty()) {
        return invalidPooled("segment, port or device identity is incomplete");
    }
    if (!isCanonicalNonzeroEid(desc.local_eid)) {
        return invalidPooled("local EID is malformed or zero");
    }
    if (desc.local_eid_index < 0) {
        return invalidPooled("local EID index is negative");
    }
    if (desc.endpoint_generation == 0 || desc.segment_generation == 0 ||
        pooled.node_incarnation == 0 || pooled.context_incarnation == 0 ||
        pooled.topology_epoch == 0) {
        return invalidPooled("all generations must be nonzero");
    }

    // Pooled payload identity and capability groups.
    if (pooled.schema_version != 3) {
        return invalidPooled("schema_version must be 3");
    }
    if (pooled.transport_mode != "RM") {
        return invalidPooled("transport_mode must be RM");
    }
    if (pooled.device_kind != "PHYSICAL") {
        return invalidPooled("device_kind must be PHYSICAL");
    }
    if (pooled.physical_device_identity.empty()) {
        return invalidPooled("physical device identity is empty");
    }
    if ((pooled.transport_modes &
         static_cast<uint16_t>(~kUrmaTransportModeMask)) != 0) {
        return invalidPooled("provider transport_modes has unsupported bits");
    }
    if ((pooled.transport_modes & kUrmaTransportModeRm) == 0) {
        return invalidPooled("provider transport_modes does not include RM");
    }
    CHECK_STATUS(validateCapabilities(pooled.provider_capabilities, true));
    if (pooled.descriptor_abi.empty()) {
        return invalidPooled("descriptor ABI is empty");
    }
    if (pooled.access_capabilities.mr_length == 0) {
        return invalidPooled("access capabilities must declare an MR range");
    }
    if (sumWouldOverflow(pooled.access_capabilities.mr_offset,
                         pooled.access_capabilities.mr_length)) {
        return invalidPooled("access MR range overflows");
    }
    if (!pooled.access_capabilities.read && !pooled.access_capabilities.write) {
        return invalidPooled("access capabilities must allow read or write");
    }
    if (!isCanonicalSha256Hex(pooled.inventory_checksum)) {
        return invalidPooled("inventory checksum is not canonical SHA-256 hex");
    }

    // Ports: non-empty, unique configured ids with a nonzero Context
    // mapping. Each Context owns at most one port (native resources have a
    // unique owner); two ports never share one Context identity.
    if (pooled.ports.empty()) {
        return invalidPooled("port inventory is empty");
    }
    std::map<std::string, const UbPooledPortDesc*> ports;
    std::unordered_set<uint64_t> port_contexts;
    for (const auto& port : pooled.ports) {
        if (port.port_id.empty() || port.context_id == 0) {
            return invalidPooled("port identity or Context mapping is invalid");
        }
        if (!isCanonicalNonzeroEid(port.eid)) {
            return invalidPooled("port EID is malformed or zero");
        }
        if (!ports.emplace(port.port_id, &port).second) {
            return invalidPooled("duplicate port identity");
        }
        if (!port_contexts.insert(port.context_id).second) {
            return invalidPooled("two ports claim the same Context");
        }
    }

    // Targets: exactly one per configured port, each carrying the full native
    // identity, the RM/JETTY/CTP profile and a versioned opaque descriptor.
    std::map<std::string, const UbPooledTargetDescriptor*> targets_by_port;
    std::unordered_set<std::string> target_identities;
    std::unordered_set<std::string> native_identities;
    for (const auto& target : pooled.targets) {
        if (target.port_id.empty() ||
            ports.find(target.port_id) == ports.end()) {
            return invalidPooled("target references an unknown port");
        }
        if (!targets_by_port.emplace(target.port_id, &target).second) {
            return invalidPooled("port carries more than one target");
        }
        if (target.target_identity.empty() || target.id == 0 ||
            target.target_epoch == 0) {
            return invalidPooled("target identity, ID or epoch is invalid");
        }
        if (!isCanonicalNonzeroEid(target.eid)) {
            return invalidPooled("target EID is malformed or zero");
        }
        if (target.profile.trans_mode != kUrmaTransportModeRm ||
            target.profile.target_type != kUrmaTargetTypeJetty ||
            target.profile.tp_type != kUrmaTpTypeCtp) {
            return invalidPooled(
                "target profile must be URMA_TM_RM/URMA_JETTY/URMA_CTP");
        }
        if (!isNonEmptyHexString(target.descriptor)) {
            return invalidPooled(
                "target descriptor must be non-empty even-length hex");
        }
        if (!target_identities.insert(target.target_identity).second) {
            return invalidPooled("duplicate target identity");
        }
        const std::string native_identity = target.eid + "/" +
                                            std::to_string(target.uasid) + "/" +
                                            std::to_string(target.id);
        if (!native_identities.insert(native_identity).second) {
            return invalidPooled("duplicate native target identity");
        }
    }
    if (targets_by_port.size() != ports.size()) {
        return invalidPooled(
            "every configured port must publish exactly one target");
    }

    // Adjacency: local keys must be published ports; entries non-empty and
    // deduplicated. Remote port ids are validated against the trusted peer
    // inventory by the consumer, not reinterpreted here.
    for (const auto& [local_port, remote_ports] : pooled.adjacency) {
        if (ports.find(local_port) == ports.end()) {
            return invalidPooled("adjacency references an unknown local port");
        }
        std::unordered_set<std::string> seen;
        for (const auto& remote_port : remote_ports) {
            if (remote_port.empty() || !seen.insert(remote_port).second) {
                return invalidPooled(
                    "adjacency entries must be non-empty and unique");
            }
        }
        if (remote_ports.empty()) {
            return invalidPooled("adjacency entry lists no remote ports");
        }
    }

    // Probe target map: keys are published ports, values reference published
    // targets without duplicates.
    for (const auto& [local_port, probes] : pooled.probe_target_map) {
        if (ports.find(local_port) == ports.end()) {
            return invalidPooled(
                "probe target map references an unknown local port");
        }
        std::unordered_set<std::string> seen;
        for (const auto& identity : probes) {
            if (target_identities.find(identity) == target_identities.end()) {
                return invalidPooled("probe references an unknown target");
            }
            if (!seen.insert(identity).second) {
                return invalidPooled("duplicate probe target");
            }
        }
        if (probes.empty()) {
            return invalidPooled("probe target map entry is empty");
        }
    }

    // Probe regions: WRITE-only isolated regions bound to (peer, local port)
    // with an epoch-matching target and overflow-free slot arithmetic.
    std::unordered_set<std::string> region_keys;
    for (const auto& region : pooled.probe_regions) {
        if (region.peer.empty()) {
            return invalidPooled("probe region peer is empty");
        }
        if (ports.find(region.local_port) == ports.end()) {
            return invalidPooled(
                "probe region references an unknown local port");
        }
        const auto target = targets_by_port.find(region.local_port);
        if (target == targets_by_port.end() ||
            target->second->target_identity != region.target_identity) {
            return invalidPooled(
                "probe region references a target outside its port");
        }
        if (target->second->target_epoch != region.target_epoch) {
            return invalidPooled(
                "probe region epoch does not match the target epoch");
        }
        if (region.region_generation == 0) {
            return invalidPooled("probe region generation is zero");
        }
        if (region.access != "WRITE") {
            return invalidPooled("probe regions only authorize WRITE access");
        }
        if (region.length == 0 || region.slots == 0 || region.stride == 0) {
            return invalidPooled(
                "probe region length, slots and stride must be nonzero");
        }
        if (sumWouldOverflow(region.offset, region.length)) {
            return invalidPooled("probe region range overflows");
        }
        if (region.slots >
            std::numeric_limits<uint64_t>::max() / region.stride) {
            return invalidPooled("probe region slot arithmetic overflows");
        }
        if (!region_keys.emplace(region.peer + "/" + region.local_port)
                 .second) {
            return invalidPooled("duplicate probe region for peer and port");
        }
    }

    // Checksum self-consistency: the canonical encoding of this inventory
    // must reproduce the published checksum. A mismatch means the payload
    // was altered or corrupted after signing.
    if (computePooledInventoryChecksum(desc) != pooled.inventory_checksum) {
        return invalidPooled("inventory checksum does not match content");
    }
    return Status::OK();
}

}  // namespace

std::string makePhysicalDeviceIdentity(std::string_view native_device_path,
                                       std::string_view eid) {
    // The first EID group (four hex digits before the first ':') is enough to
    // disambiguate one physical device path; the full EID remains part of the
    // trusted identity fields compared separately.
    std::string_view group = eid;
    const auto separator = eid.find(':');
    if (separator != std::string_view::npos) group = eid.substr(0, separator);
    std::string identity;
    identity.reserve(native_device_path.size() + 1 + group.size());
    identity.append(native_device_path);
    identity.push_back('#');
    identity.append(group);
    return identity;
}

Status validateUbBootstrap(const UbBootstrapDesc& desc) {
    if (desc.protocol_version == 1) return Status::OK();
    if (desc.protocol_version == 2) return validateV2(desc);
    if (desc.protocol_version == 3) return validateV3(desc);
    return Status::InvalidArgument("unsupported UB bootstrap version");
}

Status gateUbBootstrapForMode(UbBootstrapMode expected,
                              UbBootstrapProtocol requested,
                              const UbBootstrapDesc& desc) {
    CHECK_STATUS(validateUbBootstrap(desc));
    switch (requested) {
        case UbBootstrapProtocol::kV1:
            if (desc.protocol_version != 1) {
                return Status::InvalidArgument(
                    "bootstrap/protocol=v1 rejects a non-v1 peer");
            }
            // v1 carries per-lane Jetty ids and no RM target; it can only be
            // served by the RC legacy runtime. RM (legacy or pooled) is
            // rejected explicitly instead of degrading to v1.
            if (expected == UbBootstrapMode::RM) {
                return Status::NotImplemented(
                    "bootstrap/protocol=v1 cannot serve an RM runtime");
            }
            return Status::OK();
        case UbBootstrapProtocol::kV2:
            if (desc.protocol_version != 2) {
                return Status::InvalidArgument(
                    "bootstrap/protocol=v2 rejects a non-v2 peer");
            }
            break;
        case UbBootstrapProtocol::kAuto:
            break;
    }
    if (expected == UbBootstrapMode::RC) {
        if (desc.protocol_version == 1) return Status::OK();
        return Status::NotImplemented(
            "RM bootstrap v2/v3 is not supported by the current RC runtime");
    }
    if (expected == UbBootstrapMode::kPooledRm) {
        // Pooled RM only speaks protocol v3 with a pooled payload. Mixed
        // versions are explicitly unsupported and never upgraded silently.
        if (desc.protocol_version != 3) {
            return Status::InvalidArgument(
                "pooled RM bootstrap requires protocol v3");
        }
        if (!desc.reply_msg.empty()) {
            return Status::InvalidArgument(
                "pooled RM bootstrap request must not carry reply_msg");
        }
        return Status::OK();
    }
    if (desc.protocol_version != 2) {
        return Status::InvalidArgument("RM bootstrap requires protocol v2");
    }
    if (!desc.reply_msg.empty()) {
        return Status::InvalidArgument(
            "RM bootstrap request must not carry reply_msg");
    }
    return Status::OK();
}

Status validateUbBootstrapV2PeerIdentity(
    const UbBootstrapDesc& desc, const UbExpectedPeerIdentity& expected) {
    CHECK_STATUS(validateV2(desc));
    if (!desc.reply_msg.empty()) {
        return Status::InvalidArgument(
            "RM bootstrap peer request must not carry reply_msg");
    }
    const auto& rm = *desc.rm;
    if (desc.segment_name != expected.segment_name ||
        desc.local_nic_path != expected.local_nic_path ||
        desc.local_device_name != expected.native_device_name ||
        desc.local_device_id != expected.device_id ||
        desc.local_eid_index != expected.eid_index ||
        desc.local_eid != expected.eid ||
        rm.physical_device_identity != expected.physical_device_identity ||
        rm.transport_modes != expected.transport_modes) {
        return Status::InvalidArgument(
            "UB bootstrap v2 peer identity does not match trusted metadata");
    }
    return Status::OK();
}

Status compareUbBootstrapV2(const UbBootstrapDesc& candidate,
                            const UbBootstrapDesc& current,
                            UbBootstrapGenerationComparison& comparison) {
    comparison = UbBootstrapGenerationComparison::kConflict;
    CHECK_STATUS(validateV2(candidate));
    CHECK_STATUS(validateV2(current));
    const auto& next = *candidate.rm;
    const auto& incumbent = *current.rm;
    if (next.physical_device_identity != incumbent.physical_device_identity) {
        // A different physical device is not a generation conflict: the
        // incumbent belongs to another device identity and must not be
        // compared field by field. Report it as a replacement.
        comparison = UbBootstrapGenerationComparison::kNewer;
        return Status::OK();
    }

    // 1) Parents: a parent change opens a new comparison scope.
    const uint64_t candidate_parents[] = {
        next.node_incarnation, next.context_incarnation, next.topology_epoch};
    const uint64_t current_parents[] = {incumbent.node_incarnation,
                                        incumbent.context_incarnation,
                                        incumbent.topology_epoch};
    for (size_t i = 0; i < std::size(candidate_parents); ++i) {
        const int order =
            compareUint64(candidate_parents[i], current_parents[i]);
        if (order < 0) {
            comparison = UbBootstrapGenerationComparison::kStale;
            return Status::OK();
        }
        if (order > 0) {
            comparison = UbBootstrapGenerationComparison::kNewer;
            return Status::OK();
        }
    }

    // 2) Leaf generations BEFORE the target set. A re-created endpoint
    //    necessarily has a new Jetty id range, so target_identity can never be
    //    matched against the incumbent. Comparing the target set first (the
    //    old order) returned kConflict before the newer endpoint_generation
    //    was ever looked at, which made every post-link-down re-bootstrap fail
    //    forever.
    bool has_newer_generation = false;
    bool has_older_generation = false;
    const uint64_t candidate_leaves[] = {candidate.segment_generation,
                                         candidate.endpoint_generation};
    const uint64_t current_leaves[] = {current.segment_generation,
                                       current.endpoint_generation};
    for (size_t i = 0; i < std::size(candidate_leaves); ++i) {
        const int order = compareUint64(candidate_leaves[i], current_leaves[i]);
        has_newer_generation = has_newer_generation || order > 0;
        has_older_generation = has_older_generation || order < 0;
    }
    if (has_newer_generation && has_older_generation) {
        comparison = UbBootstrapGenerationComparison::kConflict;
        return Status::OK();
    }
    if (has_older_generation) {
        comparison = UbBootstrapGenerationComparison::kStale;
        return Status::OK();
    }
    if (has_newer_generation) {
        comparison = UbBootstrapGenerationComparison::kNewer;
        return Status::OK();
    }

    // 3) All generations tie. Only now is the target set required to match;
    //    a differing set at the same generation is a genuine content conflict.
    std::map<std::string, const UbRmTargetDescriptor*> current_targets;
    for (const auto& target : incumbent.targets) {
        current_targets.emplace(target.target_identity, &target);
    }
    if (current_targets.size() != next.targets.size()) {
        comparison = UbBootstrapGenerationComparison::kConflict;
        return Status::OK();
    }
    for (const auto& target : next.targets) {
        const auto found = current_targets.find(target.target_identity);
        if (found == current_targets.end()) {
            comparison = UbBootstrapGenerationComparison::kConflict;
            return Status::OK();
        }
        const int order =
            compareUint64(target.target_epoch, found->second->target_epoch);
        has_newer_generation = has_newer_generation || order > 0;
        has_older_generation = has_older_generation || order < 0;
    }
    if (has_newer_generation && has_older_generation) {
        comparison = UbBootstrapGenerationComparison::kConflict;
        return Status::OK();
    }
    if (has_older_generation) {
        comparison = UbBootstrapGenerationComparison::kStale;
        return Status::OK();
    }
    if (has_newer_generation) {
        comparison = UbBootstrapGenerationComparison::kNewer;
        return Status::OK();
    }

    // 4) Fully identical generation tuple: duplicate or genuine conflict.
    UbBootstrapDesc candidate_identity = candidate;
    UbBootstrapDesc current_identity = current;
    candidate_identity.reply_msg.clear();
    current_identity.reply_msg.clear();
    comparison =
        nlohmann::json(candidate_identity) == nlohmann::json(current_identity)
            ? UbBootstrapGenerationComparison::kDuplicate
            : UbBootstrapGenerationComparison::kConflict;
    return Status::OK();
}

std::string computePooledInventoryChecksum(const UbBootstrapDesc& desc) {
    if (desc.protocol_version != 3 || !desc.pooled.has_value()) {
        throw std::invalid_argument(
            "inventory checksum requires a pooled v3 descriptor");
    }
    nlohmann::json canonical = desc;
    canonical.erase("reply_msg");
    canonical["pooled"].erase("inventory_checksum");
    return sha256Hex(canonical.dump());
}

Status validateUbBootstrapV3PeerIdentity(
    const UbBootstrapDesc& desc,
    const UbExpectedPooledPeerIdentity& expected) {
    CHECK_STATUS(validateV3(desc));
    if (!desc.reply_msg.empty()) {
        return Status::InvalidArgument(
            "pooled bootstrap peer request must not carry reply_msg");
    }
    const auto& pooled = *desc.pooled;
    if (desc.segment_name != expected.segment_name ||
        desc.local_nic_path != expected.local_nic_path ||
        desc.local_device_identity != expected.local_device_identity ||
        desc.local_eid_index != expected.eid_index ||
        desc.local_eid != expected.local_eid ||
        pooled.physical_device_identity != expected.physical_device_identity ||
        pooled.transport_modes != expected.transport_modes ||
        pooled.descriptor_abi != expected.descriptor_abi ||
        pooled.inventory_checksum != expected.inventory_checksum) {
        return Status::InvalidArgument(
            "UB bootstrap v3 peer identity does not match the trusted "
            "inventory anchor");
    }
    return Status::OK();
}

Status compareUbBootstrapV3(const UbBootstrapDesc& candidate,
                            const UbBootstrapDesc& current,
                            UbBootstrapGenerationComparison& comparison) {
    comparison = UbBootstrapGenerationComparison::kConflict;
    CHECK_STATUS(validateV3(candidate));
    CHECK_STATUS(validateV3(current));
    const auto& next = *candidate.pooled;
    const auto& incumbent = *current.pooled;
    if (next.physical_device_identity != incumbent.physical_device_identity) {
        return Status::OK();
    }

    // Parent incarnations create the comparison scope. Within one unchanged
    // scope, no newer leaf may hide an older per-port target or segment.
    const uint64_t candidate_parents[] = {
        next.node_incarnation, next.context_incarnation, next.topology_epoch};
    const uint64_t current_parents[] = {incumbent.node_incarnation,
                                        incumbent.context_incarnation,
                                        incumbent.topology_epoch};
    for (size_t i = 0; i < std::size(candidate_parents); ++i) {
        const int order =
            compareUint64(candidate_parents[i], current_parents[i]);
        if (order < 0) {
            comparison = UbBootstrapGenerationComparison::kStale;
            return Status::OK();
        }
        if (order > 0) {
            comparison = UbBootstrapGenerationComparison::kNewer;
            return Status::OK();
        }
    }

    // Shared targets are keyed by their configured port; the inventory is
    // port-stable across generations. A different port set under identical
    // parents is a conflict, never a silent replacement.
    std::map<std::string, const UbPooledTargetDescriptor*> current_targets;
    for (const auto& target : incumbent.targets) {
        current_targets.emplace(target.port_id, &target);
    }
    if (current_targets.size() != next.targets.size()) return Status::OK();
    bool has_newer_generation = false;
    bool has_older_generation = false;
    for (const auto& target : next.targets) {
        const auto found = current_targets.find(target.port_id);
        if (found == current_targets.end()) return Status::OK();
        const int order =
            compareUint64(target.target_epoch, found->second->target_epoch);
        has_newer_generation = has_newer_generation || order > 0;
        has_older_generation = has_older_generation || order < 0;
    }
    const uint64_t candidate_leaves[] = {candidate.segment_generation,
                                         candidate.endpoint_generation};
    const uint64_t current_leaves[] = {current.segment_generation,
                                       current.endpoint_generation};
    for (size_t i = 0; i < std::size(candidate_leaves); ++i) {
        const int order = compareUint64(candidate_leaves[i], current_leaves[i]);
        has_newer_generation = has_newer_generation || order > 0;
        has_older_generation = has_older_generation || order < 0;
    }
    if (has_newer_generation && has_older_generation) return Status::OK();
    if (has_older_generation) {
        comparison = UbBootstrapGenerationComparison::kStale;
        return Status::OK();
    }
    if (has_newer_generation) {
        comparison = UbBootstrapGenerationComparison::kNewer;
        return Status::OK();
    }

    // Identical generations: byte-equal canonical content (diagnostics
    // excluded) is a duplicate; anything else is a same-generation conflict
    // and the healthy incumbent must be preserved.
    UbBootstrapDesc candidate_identity = candidate;
    UbBootstrapDesc current_identity = current;
    candidate_identity.reply_msg.clear();
    current_identity.reply_msg.clear();
    comparison =
        nlohmann::json(candidate_identity) == nlohmann::json(current_identity)
            ? UbBootstrapGenerationComparison::kDuplicate
            : UbBootstrapGenerationComparison::kConflict;
    return Status::OK();
}

namespace {

// SAX visitor that rejects duplicate JSON object keys before a DOM exists
// (guidance 3.4 / V5 23.2). nlohmann's DOM parser silently keeps the last
// occurrence; pooled v3 must fail closed on such payloads instead.
class DuplicateKeyRejector {
  public:
    bool null() { return true; }
    bool boolean(bool) { return true; }
    bool number_integer(int64_t) { return true; }
    bool number_unsigned(uint64_t) { return true; }
    bool number_float(double, const std::string&) { return true; }
    bool string(std::string&) { return true; }
    bool binary(nlohmann::json::binary_t&) { return true; }

    bool start_object(std::size_t) {
        object_keys_.emplace_back();
        return true;
    }

    bool key(std::string& value) {
        if (object_keys_.empty()) return false;
        if (!object_keys_.back().insert(value).second) {
            duplicate_key_ = value;
            return false;
        }
        return true;
    }

    bool end_object() {
        if (object_keys_.empty()) return false;
        object_keys_.pop_back();
        return true;
    }

    bool start_array(std::size_t) { return true; }
    bool end_array() { return true; }

    bool parse_error(std::size_t, const std::string&,
                     const nlohmann::json::exception&) {
        parse_error_ = true;
        return false;
    }

    bool had_duplicate_key() const { return !duplicate_key_.empty(); }
    bool had_parse_error() const { return parse_error_; }
    const std::string& duplicate_key() const { return duplicate_key_; }

  private:
    std::vector<std::unordered_set<std::string>> object_keys_;
    std::string duplicate_key_;
    bool parse_error_ = false;
};

}  // namespace

Status decodeUbBootstrapV3Strict(const std::string& raw,
                                 UbBootstrapDesc& desc) {
    DuplicateKeyRejector duplicate_rejector;
    if (!nlohmann::json::sax_parse(
            raw, &duplicate_rejector,
            nlohmann::json::input_format_t::json,
            /*strict=*/true)) {
        if (duplicate_rejector.had_duplicate_key()) {
            return Status::InvalidArgument(
                "UB bootstrap v3 wire has a duplicate JSON key: " +
                duplicate_rejector.duplicate_key());
        }
        return Status::InvalidArgument(
            "UB bootstrap v3 wire is not valid JSON");
    }

    nlohmann::json parsed = nlohmann::json::parse(raw);
    desc = parsed.get<UbBootstrapDesc>();
    if (desc.protocol_version != 3) {
        return Status::InvalidArgument(
            "strict v3 decoding requires protocol_version 3");
    }
    return validateV3(desc);
}

}  // namespace mooncake::tent::ub
