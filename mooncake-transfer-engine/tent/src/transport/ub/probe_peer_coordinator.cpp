// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_peer_coordinator.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <unordered_set>

#include <glog/logging.h>

#include "tent/common/utils/string_builder.h"
#include "tent/transport/ub/topology_attrs.h"

namespace mooncake::tent::ub {
namespace {

uint64_t steadyNow() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

std::string lowerAscii(std::string value) {
    std::transform(
        value.begin(), value.end(), value.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

bool sameEid(const std::string& lhs, const std::string& rhs) {
    return lowerAscii(lhs) == lowerAscii(rhs);
}

// Resolves one published port against the peer's registered segment metadata.
// A missing or duplicate (name, EID) pair is rejected: the responder cannot
// invent a port (design §7.3 rule 6).
std::optional<Topology::NicID> resolveRemoteNic(
    const SegmentDesc& segment, const std::string& topology_name,
    const std::string& eid) {
    const auto& topology = segment.getMemory().topology;
    std::optional<Topology::NicID> resolved;
    for (size_t id = 0; id < topology.getNicCount(); ++id) {
        const auto* nic =
            topology.getNicEntry(static_cast<Topology::NicID>(id));
        if (nic == nullptr || nic->type != Topology::NIC_UB) continue;
        if (nic->name != topology_name) continue;
        auto attr = nic->device_attrs.find(std::string(kTopologyEidAttr));
        if (attr == nic->device_attrs.end() || !sameEid(attr->second, eid)) {
            continue;
        }
        if (resolved.has_value()) return std::nullopt;
        resolved = static_cast<Topology::NicID>(id);
    }
    return resolved;
}

}  // namespace

ProbePeerCoordinator::ProbePeerCoordinator(ProbeParams params,
                                           Dependencies dependencies,
                                           Clock clock)
    : params_(std::move(params)),
      deps_(std::move(dependencies)),
      clock_(std::move(clock)) {
    uint64_t now = nowNs();
    if (deps_.provider != nullptr) {
        last_local_publication_epoch_ = deps_.provider->publicationEpoch();
    }
    for (const auto& [peer, decl] : deps_.topology->peerDecls()) {
        (void)decl;
        PeerState state;
        state.next_attempt_ns = now;
        peers_.emplace(peer, state);
    }
}

ProbePeerCoordinator::~ProbePeerCoordinator() { stop(); }

uint64_t ProbePeerCoordinator::nowNs(uint64_t explicit_ns) const {
    if (explicit_ns != 0) return explicit_ns;
    if (clock_) {
        const uint64_t value = clock_();
        if (value != 0) return value;
    }
    return steadyNow();
}

Status ProbePeerCoordinator::start() {
    if (running_.load(std::memory_order_acquire)) return Status::OK();
    if (deps_.segment_manager == nullptr || deps_.registry == nullptr ||
        deps_.provider == nullptr || deps_.topology == nullptr || !deps_.rpc) {
        return Status::InvalidArgument(
            "probe coordinator has incomplete dependencies");
    }
    stopping_.store(false, std::memory_order_release);
    const uint32_t threads =
        std::max<uint32_t>(1, std::min<uint32_t>(4, params_.handshake.threads));
    executors_.clear();
    for (uint32_t index = 0; index < threads; ++index) {
        auto executor = std::make_unique<ThreadedControlExecutor>(
            "ub-probe-handshake-" + std::to_string(index), 4096);
        if (!executor->start()) {
            LOG(WARNING) << "UB probe: cannot start handshake thread " << index;
            continue;
        }
        executors_.push_back(std::move(executor));
    }
    if (executors_.empty()) {
        return Status::InternalError("cannot start any probe handshake thread");
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [peer, state] : peers_) {
            state.next_attempt_ns = nowNs();
        }
    }
    running_.store(true, std::memory_order_release);
    scheduleTick(0);
    LOG(INFO) << "UB probe coordinator started: peers=" << peers_.size()
              << " threads=" << executors_.size();
    return Status::OK();
}

void ProbePeerCoordinator::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        for (auto& executor : executors_) executor->stop();
        executors_.clear();
        return;
    }
    stopping_.store(true, std::memory_order_release);
    for (auto& executor : executors_) executor->stop();
    executors_.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [peer, state] : peers_) {
        (void)peer;
        state.in_flight = false;
        state.refresh_queued = false;
    }
    LOG(INFO) << "UB probe coordinator stopped";
}

bool ProbePeerCoordinator::running() const {
    return running_.load(std::memory_order_acquire);
}

void ProbePeerCoordinator::scheduleTick(uint64_t delay_ns) {
    if (executors_.empty()) return;
    auto status =
        executors_.front()->scheduleAfter(delay_ns, [this] { tickChain(); });
    if (!status.ok() && !stopping_.load(std::memory_order_acquire)) {
        LOG_EVERY_N(WARNING, 100)
            << "UB probe: cannot schedule handshake tick: "
            << status.ToString();
    }
}

void ProbePeerCoordinator::tickChain() {
    if (stopping_.load(std::memory_order_acquire) ||
        !running_.load(std::memory_order_acquire)) {
        return;
    }
    const uint64_t now = nowNs();
    const uint64_t publication_epoch = deps_.provider->publicationEpoch();
    bool bumped = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (publication_epoch != last_local_publication_epoch_) {
            last_local_publication_epoch_ = publication_epoch;
            bumped = true;
            for (auto& [peer, state] : peers_) {
                (void)peer;
                state.next_attempt_ns = now;
            }
            ++stats_.publication_bump;
        }
    }
    if (bumped) {
        LOG(INFO) << "UB probe local publication changed to epoch "
                  << publication_epoch << "; refreshing every peer";
    }
    processDue();
    scheduleTick(params_.handshakeRefreshNs());
}

void ProbePeerCoordinator::processDue() {
    if (stopping_.load(std::memory_order_acquire)) return;
    const uint64_t now = nowNs();
    std::vector<std::string> due;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [peer, state] : peers_) {
            if (state.in_flight) {
                if (state.next_attempt_ns <= now) {
                    state.refresh_queued = true;
                    ++stats_.skipped_inflight;
                }
                continue;
            }
            if (state.next_attempt_ns > now) continue;
            state.in_flight = true;
            due.push_back(peer);
        }
    }
    for (const auto& peer : due) submitAttempt(peer, now);
}

void ProbePeerCoordinator::submitAttempt(const std::string& peer,
                                         uint64_t now_ns) {
    if (executors_.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = peers_.find(peer);
        if (found != peers_.end()) found->second.in_flight = false;
        return;
    }
    auto& executor = executors_[round_robin_++ % executors_.size()];
    auto status = executor->submit([this, peer, now_ns] {
        if (stopping_.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = peers_.find(peer);
            if (found != peers_.end()) found->second.in_flight = false;
            return;
        }
        auto result = runAttempt(peer, now_ns);
        const uint64_t finished = nowNs();
        std::lock_guard<std::mutex> lock(mutex_);
        noteAttemptFinishedLocked(peer, finished, result.ok());
        auto found = peers_.find(peer);
        if (found != peers_.end() && found->second.refresh_queued) {
            found->second.refresh_queued = false;
            found->second.next_attempt_ns = finished;
        }
    });
    if (!status.ok()) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = peers_.find(peer);
        if (found != peers_.end()) found->second.in_flight = false;
        LOG_EVERY_N(WARNING, 100) << "UB probe: cannot submit handshake for "
                                  << peer << ": " << status.ToString();
    }
}

void ProbePeerCoordinator::noteAttemptFinishedLocked(const std::string& peer,
                                                     uint64_t now_ns,
                                                     bool success) {
    auto found = peers_.find(peer);
    if (found == peers_.end()) return;
    PeerState& state = found->second;
    state.in_flight = false;
    if (success) {
        state.consecutive_failures = 0;
        state.backoff_ns = 0;
        state.next_attempt_ns = now_ns + params_.handshakeRefreshNs();
        return;
    }
    ++state.consecutive_failures;
    const uint64_t base = params_.handshake.retry_backoff_ms * 1'000'000ULL;
    const uint64_t cap =
        std::min<uint64_t>(base * 60, 60'000ULL * 1'000'000ULL);
    uint64_t delay = base;
    for (uint32_t shift = 1; shift < state.consecutive_failures && delay < cap;
         ++shift) {
        delay = std::min<uint64_t>(delay * 2, cap);
    }
    state.backoff_ns = delay;
    state.next_attempt_ns = now_ns + delay;
}

Status ProbePeerCoordinator::handshakeOnce(const std::string& peer,
                                           uint64_t now_ns) {
    const uint64_t now = nowNs(now_ns);
    auto result = runAttempt(peer, now);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        noteAttemptFinishedLocked(peer, nowNs(), result.ok());
    }
    return result;
}

Status ProbePeerCoordinator::buildRequest(
    ProbeBootstrapRequest& request) const {
    const auto publication = deps_.provider->publication();
    if (publication.ports.empty()) {
        return Status::InvalidArgument("probe not ready");
    }
    request = ProbeBootstrapRequest{};
    request.requester_segment = publication.segment;
    request.requester_node_incarnation = publication.node_incarnation;
    request.requester_topology_epoch = publication.topology_epoch;
    request.requester_publication_epoch = publication.publication_epoch;
    for (const auto& binding : deps_.topology->ports()) {
        request.declared_local_ports.push_back(binding.id);
    }
    request.ports.reserve(publication.ports.size());
    for (const auto& port : publication.ports) {
        request.ports.push_back(toProbeBootstrapPort(port));
    }
    for (const auto& [peer, decl] : deps_.topology->peerDecls()) {
        std::vector<ProbeBootstrapPeerPort> ports;
        ports.reserve(decl.ports.size());
        for (const auto& port : decl.ports) {
            ports.push_back(ProbeBootstrapPeerPort{port.id, port.eid});
        }
        request.declared_peers.emplace(peer, std::move(ports));
    }
    return Status::OK();
}

Status ProbePeerCoordinator::runAttempt(const std::string& peer,
                                        uint64_t now_ns) {
    if (stopping_.load(std::memory_order_acquire)) {
        return Status::InvalidArgument("probe coordinator is stopping");
    }
    auto publication = deps_.provider->publication();
    if (publication.ports.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.not_ready;
        return Status::InvalidArgument("probe not ready");
    }
    SegmentDescRef segment;
    auto resolve = deps_.segment_manager->getRemote(segment, peer);
    if (!resolve.ok() || !segment) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.peer_unresolved;
        deps_.registry->markStale(peer, now_ns);
        return Status::DeviceNotFound("probe handshake cannot resolve peer '" +
                                      peer + "': " + resolve.ToString());
    }
    ProbeBootstrapRequest request;
    auto status = buildRequest(request);
    if (!status.ok()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.not_ready;
        return status;
    }
    ProbeBootstrapResponse response;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.attempts;
    }
    status = deps_.rpc(segment->rpc_server_addr, request, response);
    if (!status.ok()) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.failure;
        }
        deps_.registry->markStale(peer, now_ns);
        LOG_EVERY_N(WARNING, 100) << "UB probe handshake failed: peer=" << peer
                                  << " status=" << status.ToString();
        return status;
    }
    status = applyResponse(peer, *segment, response, now_ns);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (status.ok()) {
            ++stats_.success;
        } else {
            ++stats_.failure;
        }
    }
    if (!status.ok()) {
        deps_.registry->markStale(peer, now_ns);
        LOG_EVERY_N(WARNING, 100)
            << "UB probe publication rejected: peer=" << peer
            << " status=" << status.ToString();
    }
    return status;
}

Status ProbePeerCoordinator::applyResponse(
    const std::string& peer, const SegmentDesc& segment,
    const ProbeBootstrapResponse& response, uint64_t now_ns) {
    auto validation = validateProbeBootstrapResponse(response);
    if (!validation.ok()) return validation;
    if (response.responder_segment != peer) {
        return Status::InvalidArgument(
            "probe responder segment does not match the configured peer");
    }
    const auto peer_id = deps_.topology->probePeerId(peer);
    if (!peer_id.has_value()) {
        return Status::InvalidArgument("probe peer is not declared");
    }
    const auto* declared_ports = deps_.topology->peerPorts(peer);
    if (declared_ports == nullptr) {
        return Status::InvalidArgument("probe peer has no declared ports");
    }
    ProbePeerBinding binding;
    binding.segment = peer;
    binding.peer_id = *peer_id;
    binding.node_incarnation = response.node_incarnation;
    binding.topology_epoch = response.topology_epoch;
    binding.publication_epoch = response.publication_epoch;
    size_t matched = 0;
    for (const auto& declared : *declared_ports) {
        std::vector<const ProbeBootstrapPort*> hits;
        for (const auto& port : response.ports) {
            if (sameEid(port.eid, declared.eid)) hits.push_back(&port);
        }
        if (hits.size() > 1) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.publication_identity_mismatch;
            return Status::InvalidArgument(
                "probe publication repeats a declared EID");
        }
        if (hits.empty()) continue;
        const auto remote_nic = resolveRemoteNic(
            segment, hits.front()->topology_name, hits.front()->eid);
        if (!remote_nic.has_value()) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.publication_identity_mismatch;
            return Status::InvalidArgument(
                "probe publication port is not anchored in the peer segment");
        }
        ProbePeerPortBinding port;
        port.id = declared.id;
        port.eid = declared.eid;
        port.remote_nic = *remote_nic;
        port.topology_name = hits.front()->topology_name;
        port.peer_nic_path = MakeNicPath(segment.nicPathServerName(),
                                         hits.front()->topology_name);
        port.target_epoch = hits.front()->target_epoch;
        port.jetty = hits.front()->jetty;
        port.area_descriptor = hits.front()->area_descriptor;
        port.area_length = hits.front()->area_length;
        port.area_epoch = hits.front()->area_epoch;
        port.area_token = hits.front()->area_token;
        port.node_gate_ok = hits.front()->node_gate_ok;
        port.port_gate_ok = hits.front()->port_gate_ok;
        port.profile_gate_ok = hits.front()->profile_gate_ok;
        binding.ports.push_back(std::move(port));
        ++matched;
    }
    // Ports the peer publishes but this side did not declare are ignored and
    // counted (design §5.3/§7.3 rule 5).
    if (response.ports.size() > matched) {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.publication_unknown_port += response.ports.size() - matched;
    }
    if (matched == 0) {
        return Status::InvalidArgument(
            "probe publication matched no declared peer port");
    }
    bool applied = false;
    auto status = deps_.registry->apply(binding, now_ns, &applied);
    if (!status.ok()) return status;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (applied) {
            ++stats_.publication_apply;
        } else {
            ++stats_.publication_unchanged;
        }
    }
    if (!applied) return Status::OK();
    status = deps_.provider->refreshPeerEdges();
    if (!status.ok()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.graph_event_failed;
        // The publication is valid; the provider keeps the old graph and the
        // next refresh retries the same diff.
        LOG_EVERY_N(WARNING, 100)
            << "UB probe graph update failed: peer=" << peer
            << " status=" << status.ToString();
    }
    return Status::OK();
}

void ProbePeerCoordinator::requestRefresh(const std::string& peer,
                                          const char* reason) {
    if (!running_.load(std::memory_order_acquire)) return;
    const uint64_t now = nowNs();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = peers_.find(peer);
        if (found == peers_.end()) return;
        found->second.next_attempt_ns = now;
        if (found->second.in_flight) found->second.refresh_queued = true;
        LOG_EVERY_N(INFO, 100) << "UB probe refresh requested: peer=" << peer
                               << " reason=" << (reason ? reason : "");
    }
    if (!executors_.empty()) {
        (void)executors_.front()->submit([this] { processDue(); });
    }
}

void ProbePeerCoordinator::onLocalPublicationChanged(
    uint64_t publication_epoch) {
    const uint64_t now = nowNs();
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (publication_epoch != last_local_publication_epoch_) {
            last_local_publication_epoch_ = publication_epoch;
            changed = true;
            ++stats_.publication_bump;
            for (auto& [peer, state] : peers_) {
                (void)peer;
                state.next_attempt_ns = now;
                if (state.in_flight) state.refresh_queued = true;
            }
        }
    }
    if (changed && !executors_.empty()) {
        (void)executors_.front()->submit([this] { processDue(); });
    }
}

void ProbePeerCoordinator::noteBusinessPathError(uint64_t peer_id) {
    if (!running_.load(std::memory_order_acquire)) return;
    const uint64_t now = nowNs();
    std::string target;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [peer, state] : peers_) {
            auto id = deps_.topology->probePeerId(peer);
            if (id.has_value() && *id == peer_id) {
                target = peer;
                break;
            }
        }
        if (target.empty()) return;
        auto found = peers_.find(target);
        if (found == peers_.end()) return;
        // Same-peer 1 s dedupe: a burst of business errors must not turn into
        // a handshake storm (design §7.4).
        const uint64_t dedupe_ns = 1'000'000'000ULL;
        if (now - found->second.last_hint_ns < dedupe_ns) return;
        found->second.last_hint_ns = now;
        found->second.next_attempt_ns = now;
        if (found->second.in_flight) found->second.refresh_queued = true;
        ++stats_.refresh_hint;
    }
    if (!executors_.empty()) {
        (void)executors_.front()->submit([this] { processDue(); });
    }
}

uint64_t ProbePeerCoordinator::localPublicationEpoch() const {
    return deps_.provider ? deps_.provider->publicationEpoch() : 0;
}

ProbeCoordinatorStats ProbePeerCoordinator::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

uint64_t ProbePeerCoordinator::nextAttemptNs(const std::string& peer) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = peers_.find(peer);
    return found == peers_.end() ? 0 : found->second.next_attempt_ns;
}

}  // namespace mooncake::tent::ub
