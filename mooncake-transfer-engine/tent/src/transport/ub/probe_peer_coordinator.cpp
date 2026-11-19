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
    stopping_.store(true, std::memory_order_release);
    running_.store(false, std::memory_order_release);
    std::vector<std::unique_ptr<ThreadedControlExecutor>> executors;
    {
        std::lock_guard<std::mutex> lock(executor_mutex_);
        executors.swap(executors_);
    }
    for (auto& executor : executors) executor->stop();
    lifetime_->stopping.store(true, std::memory_order_release);
    {
        // Callbacks that already passed the stopping check decrement inflight
        // before they finish; bound the wait so a wedged peer cannot hang
        // uninstall forever.
        std::unique_lock<std::mutex> wait(lifetime_->mutex);
        const auto budget = std::chrono::milliseconds(
            std::max<uint64_t>(params_.handshake.timeout_ms * 2, 1000));
        lifetime_->cv.wait_for(wait, budget, [this] {
            return lifetime_->inflight.load(std::memory_order_acquire) == 0;
        });
    }
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

Status ProbePeerCoordinator::submitControlTask(std::function<void()> task) {
    std::lock_guard<std::mutex> lock(executor_mutex_);
    if (executors_.empty()) {
        return Status::InvalidArgument("probe coordinator is stopped");
    }
    auto& executor = executors_[round_robin_++ % executors_.size()];
    return executor->submit(std::move(task));
}

void ProbePeerCoordinator::scheduleControlTask(uint64_t delay_ns,
                                               std::function<void()> task) {
    std::lock_guard<std::mutex> lock(executor_mutex_);
    if (executors_.empty()) return;
    auto status = executors_.front()->scheduleAfter(delay_ns, std::move(task));
    if (!status.ok() && !stopping_.load(std::memory_order_acquire)) {
        LOG_EVERY_N(WARNING, 100)
            << "UB probe: cannot schedule handshake task: "
            << status.ToString();
    }
}

void ProbePeerCoordinator::scheduleTick(uint64_t delay_ns) {
    scheduleControlTask(delay_ns, [this] { tickChain(); });
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
    if (now - last_summary_ns_ >= 30'000'000'000ULL) {
        last_summary_ns_ = now;
        const auto stats = this->stats();
        uint64_t max_age_ns = 0;
        uint64_t stale_peers = 0;
        for (const auto& binding : deps_.registry->snapshot()) {
            if (!binding.published) continue;
            if (binding.stale) ++stale_peers;
            if (binding.last_success_ns != 0 && now > binding.last_success_ns) {
                max_age_ns =
                    std::max(max_age_ns, now - binding.last_success_ns);
            }
        }
        LOG(INFO) << "UB probe handshake summary:"
                  << " attempt=" << stats.attempts
                  << " success=" << stats.success
                  << " failure=" << stats.failure
                  << " timeout=" << stats.timeout
                  << " skipped_inflight=" << stats.skipped_inflight
                  << " peer_unresolved=" << stats.peer_unresolved
                  << " unsupported=" << stats.unsupported
                  << " not_ready=" << stats.not_ready
                  << " publication_apply=" << stats.publication_apply
                  << " unchanged=" << stats.publication_unchanged
                  << " stale=" << stats.publication_stale
                  << " unknown_port=" << stats.publication_unknown_port
                  << " identity_mismatch="
                  << stats.publication_identity_mismatch
                  << " graph_event_failed=" << stats.graph_event_failed
                  << " refresh_hint=" << stats.refresh_hint
                  << " publication_bump=" << stats.publication_bump
                  << " peer_publication_stale=" << stale_peers
                  << " publication_age_ns_max=" << max_age_ns;
    }
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
    auto status = submitControlTask([this, peer, now_ns] {
        if (stopping_.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = peers_.find(peer);
            if (found != peers_.end()) found->second.in_flight = false;
            return;
        }
        if (deps_.async_rpc) {
            (void)startAsyncAttempt(peer, now_ns);
            return;
        }
        auto result = runAttempt(peer, now_ns);
        const uint64_t finished = nowNs();
        bool wake = false;
        uint64_t delay = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            noteAttemptFinishedLocked(peer, finished, result.ok());
            auto found = peers_.find(peer);
            if (found != peers_.end() && found->second.refresh_queued) {
                found->second.refresh_queued = false;
                found->second.next_attempt_ns = finished;
            }
            if (found != peers_.end() &&
                found->second.next_attempt_ns > finished) {
                wake = true;
                delay = found->second.next_attempt_ns - finished;
            }
        }
        if (wake) {
            // Failure backoff must wake on its own; the periodic tick alone
            // would stretch a 1 s retry to refresh_interval (P1-3).
            scheduleControlTask(delay, [this] { processDue(); });
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

Status ProbePeerCoordinator::startAsyncAttempt(const std::string& peer,
                                               uint64_t now_ns) {
    auto finish_without_attempt = [this, &peer, now_ns](bool success) {
        bool wake = false;
        uint64_t delay = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            noteAttemptFinishedLocked(peer, now_ns, success);
            auto found = peers_.find(peer);
            if (found != peers_.end() &&
                found->second.next_attempt_ns > now_ns) {
                wake = true;
                delay = found->second.next_attempt_ns - now_ns;
            }
        }
        if (wake) {
            scheduleControlTask(delay, [this] { processDue(); });
        }
    };
    auto publication = deps_.provider->publication();
    if (publication.ports.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.not_ready;
        finish_without_attempt(false);
        return Status::InvalidArgument("probe not ready");
    }
    SegmentDescRef segment;
    auto resolve = deps_.segment_manager->getRemote(segment, peer);
    if (!resolve.ok() || !segment) {
        deps_.registry->markStale(peer, now_ns);
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.peer_unresolved;
        finish_without_attempt(false);
        return Status::DeviceNotFound("probe peer is unresolved");
    }
    ProbeBootstrapRequest request;
    auto status = buildRequest(request);
    if (!status.ok()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.not_ready;
        finish_without_attempt(false);
        return status;
    }
    auto attempt = std::make_shared<AttemptState>(peer);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        async_attempts_[peer] = attempt;
        ++stats_.attempts;
    }
    auto lifetime = lifetime_;
    lifetime->inflight.fetch_add(1, std::memory_order_acq_rel);
    deps_.async_rpc(
        segment->rpc_server_addr, request,
        [this, attempt, lifetime, segment = std::move(segment)](
            Status rpc_status, ProbeBootstrapResponse response) mutable {
            bool expected = false;
            if (!attempt->finished.compare_exchange_strong(
                    expected, true, std::memory_order_acq_rel)) {
                // The timeout path owns the single inflight release.
                return;
            }
            if (lifetime->stopping.load(std::memory_order_acquire)) {
                if (lifetime->inflight.fetch_sub(
                        1, std::memory_order_acq_rel) == 1) {
                    lifetime->cv.notify_all();
                }
                return;
            }
            auto task = [this, attempt, lifetime, rpc_status,
                         response = std::move(response),
                         segment = std::move(segment)]() mutable {
                if (stopping_.load(std::memory_order_acquire)) {
                    if (lifetime->inflight.fetch_sub(
                            1, std::memory_order_acq_rel) == 1) {
                        lifetime->cv.notify_all();
                    }
                    return;
                }
                bool success = false;
                if (rpc_status.ok()) {
                    auto applied = applyResponse(attempt->peer, *segment,
                                                 response, nowNs());
                    success = applied.ok();
                    if (!success) {
                        deps_.registry->markStale(attempt->peer, nowNs());
                    }
                } else {
                    deps_.registry->markStale(attempt->peer, nowNs());
                    LOG_EVERY_N(WARNING, 100)
                        << "UB probe handshake failed: peer=" << attempt->peer
                        << " status=" << rpc_status.ToString();
                }
                finishAsyncAttempt(attempt, success, /*timeout=*/false);
                if (lifetime->inflight.fetch_sub(
                        1, std::memory_order_acq_rel) == 1) {
                    lifetime->cv.notify_all();
                }
            };
            if (!submitControlTask(std::move(task)).ok()) {
                deps_.registry->markStale(attempt->peer, nowNs());
                finishAsyncAttempt(attempt, false, /*timeout=*/false);
                if (lifetime->inflight.fetch_sub(
                        1, std::memory_order_acq_rel) == 1) {
                    lifetime->cv.notify_all();
                }
            }
        });
    scheduleControlTask(params_.handshakeTimeoutNs(), [this, attempt,
                                                       lifetime] {
        bool expected = false;
        if (!attempt->finished.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.timeout;
        }
        deps_.registry->markStale(attempt->peer, nowNs());
        LOG_EVERY_N(WARNING, 100)
            << "UB probe handshake timed out: peer=" << attempt->peer;
        finishAsyncAttempt(attempt, false, /*timeout=*/true);
        if (lifetime->inflight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            lifetime->cv.notify_all();
        }
    });
    return Status::OK();
}

void ProbePeerCoordinator::finishAsyncAttempt(
    const std::shared_ptr<AttemptState>& attempt, bool success, bool timeout) {
    (void)timeout;
    const uint64_t now = nowNs();
    bool wake = false;
    uint64_t wake_delay = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = async_attempts_.find(attempt->peer);
        if (found != async_attempts_.end() && found->second == attempt) {
            async_attempts_.erase(found);
        }
        if (success) {
            ++stats_.success;
        } else {
            ++stats_.failure;
        }
        noteAttemptFinishedLocked(attempt->peer, now, success);
        auto peer = peers_.find(attempt->peer);
        if (peer != peers_.end() && peer->second.next_attempt_ns > now) {
            wake = true;
            wake_delay = peer->second.next_attempt_ns - now;
        }
    }
    if (wake) {
        scheduleControlTask(wake_delay, [this] { processDue(); });
    }
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
    if (response.schema_version != kProbeBootstrapSchemaVersion) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.unsupported;
        return Status::InvalidArgument("unsupported probe schema version");
    }
    auto validation = validateProbeBootstrapResponse(response);
    if (!validation.ok()) return validation;
    if (response.responder_segment != peer) {
        return Status::InvalidArgument(
            "probe responder segment does not match the configured peer");
    }
    if (segment.type != SegmentType::Memory) {
        return Status::InvalidArgument(
            "probe peer segment is not memory-backed");
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
    if (matched < declared_ports->size()) {
        const size_t missing = declared_ports->size() - matched;
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.missing_remote_ports += missing;
        if (params_.handshake.require_peer_ports) {
            LOG(WARNING) << "UB probe publication is missing " << missing
                         << " declared peer port(s): peer=" << peer;
        }
    }
    if (matched == 0) {
        return Status::InvalidArgument(
            "probe publication matched no declared peer port");
    }
    bool applied = false;
    bool stale = false;
    auto status = deps_.registry->apply(binding, now_ns, &applied, &stale);
    if (!status.ok()) return status;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (applied) {
            ++stats_.publication_apply;
        } else if (stale) {
            ++stats_.publication_stale;
        } else {
            ++stats_.publication_unchanged;
        }
    }
    if (!applied) return Status::OK();
    LOG(INFO) << "UB probe publication applied: peer=" << peer
              << " node_incarnation=" << binding.node_incarnation
              << " topology_epoch=" << binding.topology_epoch
              << " publication_epoch=" << binding.publication_epoch
              << " ports=" << matched;
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
    (void)submitControlTask([this] { processDue(); });
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
    if (changed) {
        (void)submitControlTask([this] { processDue(); });
    }
}

bool ProbePeerCoordinator::acceptsPeer(const std::string& peer) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return peers_.count(peer) != 0;
}

Status ProbePeerCoordinator::ingestPeerPublication(
    const std::string& peer, SegmentDescRef segment,
    ProbeBootstrapResponse publication) {
    if (!acceptsPeer(peer)) return Status::OK();
    if (!running_.load(std::memory_order_acquire) || !segment) {
        // The peer retries on its next refresh; dropping is safe and never
        // blocks the RPC handler.
        return Status::OK();
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.publication_ingest;
    }
    submitControlTask([this, peer = std::string(peer), segment,
                       publication = std::move(publication)]() mutable {
        if (stopping_.load(std::memory_order_acquire)) return;
        (void)applyResponse(peer, *segment, publication, nowNs());
    });
    return Status::OK();
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
    (void)submitControlTask([this] { processDue(); });
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
