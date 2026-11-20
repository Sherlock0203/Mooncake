// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_peer_registry.h"

#include <algorithm>

#include <glog/logging.h>

namespace mooncake::tent::ub {

ProbePeerRegistry::ProbePeerRegistry(
    const std::map<std::string, std::vector<ProbeDeclaredPeerPort>>& declared,
    uint32_t withdraw_grace, const std::map<std::string, uint64_t>& peer_ids)
    : declared_(declared),
      withdraw_grace_(withdraw_grace == 0 ? 1 : withdraw_grace) {
    for (const auto& [peer, ports] : declared_) {
        ProbePeerBinding binding;
        binding.segment = peer;
        auto provided = peer_ids.find(peer);
        binding.peer_id =
            provided != peer_ids.end() ? provided->second : next_peer_id_++;
        binding.ports.resize(ports.size());
        for (size_t index = 0; index < ports.size(); ++index) {
            binding.ports[index].id = ports[index].id;
            binding.ports[index].eid = ports[index].eid;
        }
        bindings_.emplace(peer, std::move(binding));
    }
}

Status ProbePeerRegistry::apply(const ProbePeerBinding& publication,
                                uint64_t now_ns, bool* applied, bool* stale) {
    if (applied != nullptr) *applied = false;
    if (stale != nullptr) *stale = false;
    std::lock_guard<std::mutex> lock(mutex_);
    auto existing = bindings_.find(publication.segment);
    if (existing == bindings_.end()) {
        return Status::InvalidArgument(
            "probe publication for an undeclared peer '" + publication.segment +
            "'");
    }
    ProbePeerBinding& current = existing->second;
    if (publication.peer_id != 0 && current.peer_id != 0 &&
        publication.peer_id != current.peer_id) {
        return Status::InvalidArgument(
            "probe publication peer id does not match the declared peer");
    }
    // The incarnation and topology epochs come from a non-monotonic seed, so
    // numeric ordering cannot decide freshness. A different node incarnation
    // or topology epoch is a new generation and is always accepted; only
    // within one generation is publication_epoch ordered. The coordinator
    // serializes attempts per peer, so an older response cannot race a newer
    // one from the same peer.
    const bool same_generation =
        current.published &&
        publication.node_incarnation == current.node_incarnation &&
        publication.topology_epoch == current.topology_epoch;
    if (same_generation) {
        if (publication.publication_epoch < current.publication_epoch) {
            // A late response from the current generation: keep the incumbent.
            if (stale != nullptr) *stale = true;
            return Status::OK();
        }
        if (publication.publication_epoch == current.publication_epoch) {
            // Repeated handshake with the same publication: acknowledged
            // without rebuilding the edge set, but it proves the peer is
            // reachable again and clears a stale marker (design §9.3).
            if (current.stale) {
                current.stale = false;
                current.last_success_ns = now_ns;
            }
            return Status::OK();
        }
    }

    std::vector<ProbePeerPortBinding> next;
    next.reserve(current.ports.size());
    size_t matched_ports = 0;
    for (const auto& declared : declared_.at(publication.segment)) {
        auto published = std::find_if(
            publication.ports.begin(), publication.ports.end(),
            [&](const ProbePeerPortBinding& port) {
                return port.id == declared.id ||
                       (!declared.eid.empty() && port.eid == declared.eid);
            });
        ProbePeerPortBinding port;
        auto previous =
            std::find_if(current.ports.begin(), current.ports.end(),
                         [&](const ProbePeerPortBinding& candidate) {
                             return candidate.id == declared.id;
                         });
        if (previous != current.ports.end()) port = *previous;
        port.id = declared.id;
        port.eid = declared.eid.empty() ? port.eid : declared.eid;
        if (published != publication.ports.end()) {
            ++matched_ports;
            port.remote_nic = published->remote_nic;
            port.topology_name = published->topology_name;
            port.peer_nic_path = published->peer_nic_path;
            port.target_epoch = published->target_epoch;
            port.jetty = published->jetty;
            port.area_descriptor = published->area_descriptor;
            port.area_length = published->area_length;
            port.area_epoch = published->area_epoch;
            port.area_token = published->area_token;
            port.node_gate_ok = published->node_gate_ok;
            port.port_gate_ok = published->port_gate_ok;
            port.profile_gate_ok = published->profile_gate_ok;
            port.missing_publications = 0;
            port.withdrawn = false;
        } else {
            ++port.missing_publications;
            if (port.missing_publications >= withdraw_grace_) {
                if (!port.withdrawn) {
                    LOG(WARNING) << "UB probe port withdrawn: peer="
                                 << publication.segment << " port=" << port.id
                                 << " missing=" << port.missing_publications;
                }
                port.withdrawn = true;
            }
        }
        next.push_back(std::move(port));
    }

    // A publication that matches no declared port is invalid: keep the old
    // snapshot so a misconfigured peer cannot withdraw every edge at once
    // (design §7.6 rule 9).
    if (matched_ports == 0) {
        current.stale = true;
        return Status::InvalidArgument(
            "probe publication matched no declared port for peer '" +
            publication.segment + "'");
    }

    current.node_incarnation = publication.node_incarnation;
    current.topology_epoch = publication.topology_epoch;
    current.publication_epoch = publication.publication_epoch;
    current.published = true;
    current.stale = false;
    current.last_success_ns = now_ns;
    current.ports = std::move(next);
    if (applied != nullptr) *applied = true;
    return Status::OK();
}

void ProbePeerRegistry::markStale(const std::string& peer, uint64_t now_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = bindings_.find(peer);
    if (found == bindings_.end()) return;
    found->second.stale = true;
    found->second.last_success_ns = found->second.last_success_ns == 0
                                        ? now_ns
                                        : found->second.last_success_ns;
}

std::optional<ProbePeerPublicationView> ProbePeerRegistry::publicationView(
    const std::string& peer) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = bindings_.find(peer);
    if (found == bindings_.end()) return std::nullopt;
    ProbePeerPublicationView view;
    view.published = found->second.published;
    view.stale = found->second.stale;
    for (const auto& port : found->second.ports) {
        view.ports.emplace(
            port.id, ProbePeerPortView{port.withdrawn,
                                       port.node_gate_ok && port.port_gate_ok &&
                                           port.profile_gate_ok});
    }
    return view;
}

std::optional<ProbePeerBinding> ProbePeerRegistry::find(
    const std::string& peer) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = bindings_.find(peer);
    if (found == bindings_.end()) return std::nullopt;
    return found->second;
}

std::vector<ProbePeerBinding> ProbePeerRegistry::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ProbePeerBinding> result;
    result.reserve(bindings_.size());
    for (const auto& [peer, binding] : bindings_) {
        result.push_back(binding);
    }
    return result;
}

std::optional<uint64_t> ProbePeerRegistry::peerId(
    const std::string& peer) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = bindings_.find(peer);
    if (found == bindings_.end()) return std::nullopt;
    return found->second.peer_id;
}

std::vector<std::string> ProbePeerRegistry::peers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    result.reserve(bindings_.size());
    for (const auto& [peer, binding] : bindings_) result.push_back(peer);
    return result;
}

size_t ProbePeerRegistry::peerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return bindings_.size();
}

}  // namespace mooncake::tent::ub
