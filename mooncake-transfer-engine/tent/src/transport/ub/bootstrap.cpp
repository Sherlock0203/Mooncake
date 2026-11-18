// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/bootstrap.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iterator>
#include <map>
#include <string>
#include <unordered_set>

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

Status validateCapabilities(const std::vector<std::string>& capabilities) {
    if (capabilities.empty()) {
        return invalidBootstrap("provider capabilities are empty");
    }
    const auto has = [&](const std::string& capability) {
        return std::find(capabilities.begin(), capabilities.end(),
                         capability) != capabilities.end();
    };
    if (!has("read") || !has("write") || !has("rm")) {
        return invalidBootstrap(
            "provider capabilities must include read, write and rm");
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
    CHECK_STATUS(validateCapabilities(rm.provider_capabilities));
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
    if (desc.protocol_version != 2) {
        return Status::InvalidArgument("unsupported UB bootstrap version");
    }
    return validateV2(desc);
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
            "RM bootstrap v2 is not supported by the current RC runtime");
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
        return Status::OK();
    }

    // A parent incarnation change creates a new comparison scope. Within one
    // unchanged parent scope, no newer leaf may hide an older target or
    // segment.
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

    std::map<std::string, const UbRmTargetDescriptor*> current_targets;
    for (const auto& target : incumbent.targets) {
        current_targets.emplace(target.target_identity, &target);
    }
    if (current_targets.size() != next.targets.size()) return Status::OK();
    bool has_newer_generation = false;
    bool has_older_generation = false;
    for (const auto& target : next.targets) {
        const auto found = current_targets.find(target.target_identity);
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

}  // namespace mooncake::tent::ub
