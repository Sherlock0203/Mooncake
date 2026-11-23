// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_topology.h"

#include <algorithm>
#include <cctype>
#include <unordered_set>

#include <glog/logging.h>

#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/probe_peer_registry.h"
#include "tent/transport/ub/topology_attrs.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

std::string lowerAscii(std::string_view value) {
    std::string result(value);
    std::transform(
        result.begin(), result.end(), result.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return result;
}

std::string shortEidName(const DeviceInfo& device) {
    return device.native_device_name + ":eid" +
           std::to_string(device.eid_index);
}

bool matchesBind(const DeviceInfo& device, const std::string& bind) {
    return device.topology_name == bind || device.native_device_name == bind ||
           shortEidName(device) == bind;
}

bool supportsRm(const DeviceInfo& device) {
    const uint16_t modes = device.capabilities.transport_modes;
    // Zero means the provider did not report capabilities; the install-time
    // RM gate is authoritative in that case, so do not reject here.
    return modes == 0 || (modes & kUrmaTransportModeRm) != 0;
}

// Resolves one configured peer EID against the peer segment metadata. The EID
// is published by the peer in the NIC's opaque device attributes; a missing or
// duplicate match is fail-closed.
std::optional<Topology::NicID> remoteNicForEid(const Topology& topology,
                                               const std::string& eid) {
    const std::string wanted = lowerAscii(eid);
    std::optional<Topology::NicID> resolved;
    for (size_t id = 0; id < topology.getNicCount(); ++id) {
        const auto* nic =
            topology.getNicEntry(static_cast<Topology::NicID>(id));
        if (nic == nullptr || nic->type != Topology::NIC_UB) continue;
        auto found = nic->device_attrs.find(std::string(kTopologyEidAttr));
        if (found == nic->device_attrs.end()) continue;
        if (lowerAscii(found->second) != wanted) continue;
        if (resolved.has_value()) {
            // Duplicate EIDs cannot be resolved deterministically.
            return std::nullopt;
        }
        resolved = static_cast<Topology::NicID>(id);
    }
    return resolved;
}

}  // namespace

EidScope eidScopeForEid(std::string_view eid) {
    EidScope scope;
    if (!isCanonicalEid(eid)) return scope;
    // Eight groups of four hex digits. The last group is the interface/port
    // identity; the prefix is the reachability scope (df00 vs df04).
    scope.key = lowerAscii(eid.substr(0, 34));
    scope.trusted = true;
    return scope;
}

bool sameEidScope(std::string_view local_eid, std::string_view remote_eid) {
    const EidScope local = eidScopeForEid(local_eid);
    const EidScope remote = eidScopeForEid(remote_eid);
    // Untrusted (non-canonical / absent) EIDs are not a reason to refuse a
    // pair: compatibility first, and the provider still owns the final word.
    if (!local.trusted || !remote.trusted) return true;
    return local.key == remote.key;
}

void ProbeTopology::recordAdjacency(AdjacencyDecision decision,
                                    bool unverified) const {
    size_t index = kAllowedIndex;
    switch (decision) {
        case AdjacencyDecision::kAllowed:
            index = kAllowedIndex;
            break;
        case AdjacencyDecision::kUndeclaredPeer:
            index = kUndeclaredPeerIndex;
            break;
        case AdjacencyDecision::kUndeclaredLocal:
            index = kUndeclaredLocalIndex;
            break;
        case AdjacencyDecision::kUndeclaredRemote:
            index = kUndeclaredRemoteIndex;
            break;
        case AdjacencyDecision::kPeerEidUnmatched:
            index = kPeerEidUnmatchedIndex;
            break;
        case AdjacencyDecision::kPortMissing:
            index = kPortMissingIndex;
            break;
        case AdjacencyDecision::kPeerGateBlocked:
            index = kPeerGateBlockedIndex;
            break;
    }
    counters_->decision[index].fetch_add(1, std::memory_order_relaxed);
    if (unverified) {
        counters_->unverified.fetch_add(1, std::memory_order_relaxed);
    }
}

ProbeTopologyStats ProbeTopology::stats() const {
    ProbeTopologyStats result;
    result.ports = ports_.size();
    for (const auto& [peer, edges] : adjacency_) {
        (void)peer;
        for (const auto& [local_id, remote_ids] : edges) {
            (void)local_id;
            result.edges += remote_ids.size();
        }
    }
    result.adjacency_allowed =
        counters_->decision[kAllowedIndex].load(std::memory_order_relaxed);
    result.adjacency_undeclared_peer =
        counters_->decision[kUndeclaredPeerIndex].load(
            std::memory_order_relaxed);
    result.adjacency_undeclared_local =
        counters_->decision[kUndeclaredLocalIndex].load(
            std::memory_order_relaxed);
    result.adjacency_undeclared_remote =
        counters_->decision[kUndeclaredRemoteIndex].load(
            std::memory_order_relaxed);
    result.adjacency_peer_eid_unmatched =
        counters_->decision[kPeerEidUnmatchedIndex].load(
            std::memory_order_relaxed);
    result.adjacency_port_missing =
        counters_->decision[kPortMissingIndex].load(std::memory_order_relaxed);
    result.adjacency_peer_gate_blocked =
        counters_->decision[kPeerGateBlockedIndex].load(
            std::memory_order_relaxed);
    result.adjacency_unverified_peer =
        counters_->unverified.load(std::memory_order_relaxed);
    return result;
}

AdjacencyDecision AdjacencyView::check(Topology::NicID local_nic,
                                       Topology::NicID remote_nic,
                                       bool* local_declared) const {
    if (local_declared != nullptr) *local_declared = false;
    if (!enforced) return AdjacencyDecision::kAllowed;
    if (!peer_declared) return AdjacencyDecision::kUndeclaredPeer;
    if (peer_eid_unmatched) return AdjacencyDecision::kPeerEidUnmatched;

    const auto local_it =
        std::find(local_nics.begin(), local_nics.end(), local_nic);
    if (local_it == local_nics.end()) {
        return AdjacencyDecision::kUndeclaredLocal;
    }
    if (local_declared != nullptr) *local_declared = true;
    const auto remote_it =
        std::find(remote_nics.begin(), remote_nics.end(), remote_nic);
    if (remote_it == remote_nics.end()) {
        return AdjacencyDecision::kUndeclaredRemote;
    }
    const size_t local_slot =
        static_cast<size_t>(std::distance(local_nics.begin(), local_it));
    const size_t remote_slot =
        static_cast<size_t>(std::distance(remote_nics.begin(), remote_it));
    const size_t index = local_slot * remote_nics.size() + remote_slot;
    if (index >= allowed.size() || allowed[index] == 0) {
        return AdjacencyDecision::kUndeclaredRemote;
    }
    if (publication_seen && remote_slot < remote_status.size()) {
        if (remote_status[remote_slot] == 1) {
            return AdjacencyDecision::kPortMissing;
        }
        if (remote_status[remote_slot] == 2) {
            return AdjacencyDecision::kPeerGateBlocked;
        }
    }
    return AdjacencyDecision::kAllowed;
}

Status ProbeTopology::resolve(const ProbeParams& params,
                              const std::vector<DeviceInfo>& devices,
                              const Topology& topology, ProbeTopology& output,
                              uint32_t topology_dump_limit) {
    ProbeTopology resolved;
    resolved.peers_ = params.peers;
    resolved.adjacency_ = params.adjacency;
    resolved.declared_ = params.hasDeclaredTopology();
    if (!resolved.declared_) {
        resolved.enforces_ = false;
        output = std::move(resolved);
        return Status::OK();
    }
    if (params.local_ports.empty() || params.peers.empty() ||
        params.adjacency.empty()) {
        return Status::InvalidArgument(
            "declared probe topology requires local_ports, peers and "
            "adjacency together");
    }
    resolved.enforces_ = params.enforcesAdjacency();

    std::unordered_set<std::string> port_ids;
    std::unordered_set<Topology::NicID> local_nics;
    for (const auto& declaration : params.local_ports) {
        if (!port_ids.insert(declaration.id).second) {
            return Status::InvalidArgument(
                "declared probe topology has a duplicate local port id '" +
                declaration.id + "'");
        }
        std::vector<const DeviceInfo*> matches;
        for (const auto& device : devices) {
            if (!device.active) continue;
            if (device.kind != DeviceKind::PHYSICAL) continue;
            if (isBondingDevice(device)) continue;
            if (!supportsRm(device)) continue;
            if (!matchesBind(device, declaration.bind)) continue;
            matches.push_back(&device);
        }
        if (matches.size() != 1) {
            return Status::InvalidArgument(
                "probe/local_ports '" + declaration.id + "' bind '" +
                declaration.bind + "' matched " +
                std::to_string(matches.size()) +
                " active physical RM EIDs; it must match exactly one");
        }
        const DeviceInfo& device = *matches.front();
        if (!declaration.eid.empty() &&
            lowerAscii(declaration.eid) != lowerAscii(device.eid)) {
            return Status::InvalidArgument(
                "probe/local_ports '" + declaration.id + "' expected EID '" +
                declaration.eid + "' but bind '" + declaration.bind +
                "' resolved to '" + device.eid + "'");
        }
        const Topology::NicID local_nic =
            topology.getNicId(device.topology_name);
        if (local_nic < 0) {
            return Status::InvalidArgument(
                "probe/local_ports '" + declaration.id + "' resolved device '" +
                device.topology_name +
                "' is not present in the business topology");
        }
        if (!local_nics.insert(local_nic).second) {
            return Status::InvalidArgument(
                "probe/local_ports binds two ids to the same business NIC " +
                std::to_string(local_nic));
        }
        ProbeLocalPortBinding binding;
        binding.id = declaration.id;
        binding.bind = declaration.bind;
        binding.eid = device.eid;
        binding.topology_name = device.topology_name;
        binding.native_device_name = device.native_device_name;
        binding.local_nic = local_nic;
        resolved.bindings_.emplace(binding.id, local_nic);
        resolved.local_by_nic_.emplace(local_nic, binding.id);
        resolved.ports_.push_back(std::move(binding));
    }

    // INV-4: declared edges must be reachable at install time. The default
    // EID-prefix scope rejects a cross-subnet edge (e.g. df00 <-> df04)
    // instead of letting it fail later as a business MEMORY_ERROR.
    bool scope_mismatch = false;
    std::string first_scope_error;
    for (const auto& [peer_name, edges] : resolved.adjacency_) {
        auto peer = resolved.peers_.find(peer_name);
        if (peer == resolved.peers_.end()) continue;
        for (const auto& [local_id, remote_ids] : edges) {
            const ProbeLocalPortBinding* binding = nullptr;
            for (const auto& candidate : resolved.ports_) {
                if (candidate.id == local_id) {
                    binding = &candidate;
                    break;
                }
            }
            if (binding == nullptr) continue;
            const EidScope local_scope = eidScopeForEid(binding->eid);
            for (const auto& remote_id : remote_ids) {
                const ProbePeerPortDecl* remote = nullptr;
                for (const auto& port : peer->second.ports) {
                    if (port.id == remote_id) {
                        remote = &port;
                        break;
                    }
                }
                if (remote == nullptr) continue;
                const EidScope remote_scope = eidScopeForEid(remote->eid);
                if (!local_scope.trusted || !remote_scope.trusted) continue;
                if (local_scope.key == remote_scope.key) continue;
                scope_mismatch = true;
                if (first_scope_error.empty()) {
                    first_scope_error =
                        "declared edge " + peer_name + "/" + local_id + "->" +
                        remote_id + " crosses EID scope '" + local_scope.key +
                        "' -> '" + remote_scope.key + "'";
                }
            }
        }
    }
    if (scope_mismatch) {
        if (params.adjacency_policy.require_same_eid_scope) {
            return Status::InvalidArgument(first_scope_error);
        }
        LOG(WARNING) << "UB probe: " << first_scope_error
                     << "; adjacency_policy.require_same_eid_scope=false, "
                        "allowing it";
    }

    // Re-validate every adjacency reference against the resolved bindings and
    // peer declarations. The parser already checked the shape; this is the
    // install-time fence that no runtime consumer can bypass.
    for (const auto& [peer_name, edges] : resolved.adjacency_) {
        auto peer = resolved.peers_.find(peer_name);
        if (peer == resolved.peers_.end()) {
            return Status::InvalidArgument(
                "probe/adjacency references undeclared peer '" + peer_name +
                "'");
        }
        for (const auto& [local_id, remote_ids] : edges) {
            if (resolved.bindings_.count(local_id) == 0) {
                return Status::InvalidArgument(
                    "probe/adjacency/" + peer_name +
                    " references undeclared local port '" + local_id + "'");
            }
            for (const auto& remote_id : remote_ids) {
                const bool declared = std::any_of(
                    peer->second.ports.begin(), peer->second.ports.end(),
                    [&](const ProbePeerPortDecl& port) {
                        return port.id == remote_id;
                    });
                if (!declared) {
                    return Status::InvalidArgument(
                        "probe/adjacency/" + peer_name + "/" + local_id +
                        " references undeclared remote port '" + remote_id +
                        "'");
                }
            }
        }
    }

    LOG(INFO) << "UB probe topology: " << resolved.ports_.size()
              << " declared local port(s), " << resolved.peers_.size()
              << " peer(s), " << resolved.adjacency_.size()
              << " adjacency group(s), enforce="
              << (resolved.enforces_ ? "true" : "false");
    {
        size_t dumped = 0;
        bool truncated = false;
        auto may_dump = [&]() {
            if (topology_dump_limit != 0 && dumped >= topology_dump_limit) {
                truncated = true;
                return false;
            }
            ++dumped;
            return true;
        };
        LOG(INFO) << "UB probe topology dump: local_ports="
                  << resolved.ports_.size()
                  << " peers=" << resolved.peers_.size()
                  << " adjacency_groups=" << resolved.adjacency_.size()
                  << " enforce=" << (resolved.enforces_ ? "true" : "false");
        for (const auto& binding : resolved.ports_) {
            if (!may_dump()) break;
            const auto scope = eidScopeForEid(binding.eid);
            LOG(INFO) << "UB probe topology port: id=" << binding.id
                      << " bind=" << binding.bind
                      << " device=" << binding.native_device_name
                      << " local_nic=" << binding.local_nic
                      << " eid=" << binding.eid << " scope=" << scope.key;
        }
        if (!truncated) {
            for (const auto& [peer_name, peer] : resolved.peers_) {
                for (const auto& port : peer.ports) {
                    if (!may_dump()) break;
                    const auto scope = eidScopeForEid(port.eid);
                    LOG(INFO) << "UB probe topology peer: peer=" << peer_name
                              << " port=" << port.id << " eid=" << port.eid
                              << " scope=" << scope.key;
                }
                if (truncated) break;
            }
        }
        if (!truncated) {
            for (const auto& [peer_name, edges] : resolved.adjacency_) {
                for (const auto& [local_id, remote_ids] : edges) {
                    for (const auto& remote_id : remote_ids) {
                        if (!may_dump()) break;
                        LOG(INFO)
                            << "UB probe topology edge: peer=" << peer_name
                            << " " << local_id << "->" << remote_id;
                    }
                    if (truncated) break;
                }
                if (truncated) break;
            }
        }
        if (truncated) {
            LOG(INFO) << "UB probe topology dump truncated at "
                      << topology_dump_limit << " line(s)";
        }
    }
    output = std::move(resolved);
    return Status::OK();
}

AdjacencyView ProbeTopology::viewFor(const std::string& peer,
                                     const SegmentDesc& remote) const {
    AdjacencyView view;
    if (!enforces_) return view;
    view.enforced = true;
    auto peer_edges = adjacency_.find(peer);
    if (peer_edges == adjacency_.end()) {
        view.peer_declared = false;
        return view;
    }
    view.peer_declared = true;
    auto peer_decl = peers_.find(peer);
    if (peer_decl == peers_.end()) {
        view.peer_eid_unmatched = true;
        return view;
    }

    if (remote.type != SegmentType::Memory) {
        view.peer_eid_unmatched = true;
        return view;
    }
    const auto& remote_topology = remote.getMemory().topology;
    std::unordered_map<std::string, Topology::NicID> remote_by_id;
    for (const auto& port : peer_decl->second.ports) {
        auto nic = remoteNicForEid(remote_topology, port.eid);
        if (!nic.has_value()) {
            view.peer_eid_unmatched = true;
            return view;
        }
        remote_by_id.emplace(port.id, *nic);
    }

    struct LocalSlot {
        Topology::NicID nic;
        std::vector<bool> remotes;
    };
    std::vector<LocalSlot> locals;
    view.remote_nics.clear();
    for (const auto& [local_id, remote_ids] : peer_edges->second) {
        auto binding = bindings_.find(local_id);
        if (binding == bindings_.end()) continue;
        auto local = std::find_if(
            locals.begin(), locals.end(),
            [&](const LocalSlot& slot) { return slot.nic == binding->second; });
        if (local == locals.end()) {
            locals.push_back(LocalSlot{
                binding->second,
                std::vector<bool>(peer_decl->second.ports.size(), false)});
            local = std::prev(locals.end());
        }
        for (const auto& remote_id : remote_ids) {
            auto remote = remote_by_id.find(remote_id);
            if (remote == remote_by_id.end()) {
                view.peer_eid_unmatched = true;
                return view;
            }
            if (std::find(view.remote_nics.begin(), view.remote_nics.end(),
                          remote->second) == view.remote_nics.end()) {
                view.remote_nics.push_back(remote->second);
            }
            size_t port_index = 0;
            for (size_t i = 0; i < peer_decl->second.ports.size(); ++i) {
                if (peer_decl->second.ports[i].id == remote_id) {
                    port_index = i;
                    break;
                }
            }
            local->remotes[port_index] = true;
        }
    }

    view.local_nics.reserve(locals.size());
    for (const auto& local : locals) view.local_nics.push_back(local.nic);
    view.allowed.assign(view.local_nics.size() * view.remote_nics.size(), 0);
    for (size_t local_slot = 0; local_slot < locals.size(); ++local_slot) {
        for (size_t port_index = 0; port_index < peer_decl->second.ports.size();
             ++port_index) {
            if (!locals[local_slot].remotes[port_index]) continue;
            const auto nic =
                remote_by_id.at(peer_decl->second.ports[port_index].id);
            auto remote_it = std::find(view.remote_nics.begin(),
                                       view.remote_nics.end(), nic);
            if (remote_it == view.remote_nics.end()) continue;
            const size_t remote_slot = static_cast<size_t>(
                std::distance(view.remote_nics.begin(), remote_it));
            view.allowed[local_slot * view.remote_nics.size() + remote_slot] =
                1;
        }
    }

    // Publication state (design §5.3). Before the first successful
    // publication every declared edge stays allowed (ELIGIBLE_UNVERIFIED);
    // afterwards a missing/withdrawn port or a closed peer gate rejects it.
    if (peer_registry_ == nullptr) return view;
    auto publication = peer_registry_->publicationView(peer);
    if (!publication || !publication->published) return view;
    view.publication_seen = true;
    view.remote_status.assign(view.remote_nics.size(), 0);
    for (size_t remote_slot = 0; remote_slot < view.remote_nics.size();
         ++remote_slot) {
        const auto remote_nic = view.remote_nics[remote_slot];
        const ProbePeerPortDecl* declared_port = nullptr;
        for (const auto& port : peer_decl->second.ports) {
            auto mapped = remote_by_id.find(port.id);
            if (mapped != remote_by_id.end() && mapped->second == remote_nic) {
                declared_port = &port;
                break;
            }
        }
        if (declared_port == nullptr) continue;
        auto published = publication->ports.find(declared_port->id);
        if (published == publication->ports.end() ||
            published->second.withdrawn) {
            view.remote_status[remote_slot] = 1;
            continue;
        }
        if (!published->second.gates_ok) {
            view.remote_status[remote_slot] = 2;
        }
    }
    return view;
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
