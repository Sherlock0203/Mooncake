// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/endpoint.h"
#include "tent/transport/ub/target_registry.h"

#include "tent/transport/ub/bootstrap.h"

#include "jetty_pool.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <exception>
#include <iterator>
#include <limits>
#include <utility>

namespace mooncake::tent::ub {
namespace {

uint64_t generationSeed() {
    const auto wall = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const auto steady = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    const uint64_t seed =
        (wall ^ (steady << 13) ^ (steady >> 7)) & 0x7fffffffffffffffULL;
    return seed == 0 ? 1 : seed;
}

std::atomic<uint64_t> g_next_endpoint_generation{generationSeed()};

bool samePeer(const UbBootstrapDesc& lhs, const UbBootstrapDesc& rhs) {
    return lhs.protocol_version == rhs.protocol_version &&
           lhs.local_eid == rhs.local_eid && lhs.jetty_ids == rhs.jetty_ids &&
           lhs.jetty_uasids == rhs.jetty_uasids &&
           lhs.endpoint_generation == rhs.endpoint_generation;
}

}  // namespace

size_t UbEndpointKeyHash::operator()(const UbEndpointKey& key) const noexcept {
    size_t seed = std::hash<int>{}(key.local_topology_id);
    auto combine = [&seed](size_t value) {
        seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    };
    combine(std::hash<SegmentID>{}(key.remote_segment_id));
    combine(std::hash<int>{}(key.remote_topology_id));
    combine(std::hash<std::string>{}(key.peer_nic_path));
    return seed;
}

uint64_t UbEndpoint::allocateGeneration() noexcept {
    const uint64_t generation =
        g_next_endpoint_generation.fetch_add(1, std::memory_order_relaxed);
    // Generation zero is reserved for "no endpoint". Exhausting the 64-bit
    // process-wide sequence is not recoverable without reusing generations.
    if (generation == 0 || generation == std::numeric_limits<uint64_t>::max()) {
        std::terminate();
    }
    return generation;
}

UbEndpoint::UbEndpoint(UbEndpointKey key, UbContextPtr context,
                       std::shared_ptr<UrmaAdapter> adapter,
                       uint32_t jetty_count, JettyOptions jetty_options)
    : key_(std::move(key)),
      context_(std::move(context)),
      adapter_(std::move(adapter)),
      jetty_count_(jetty_count),
      jetty_options_(jetty_options),
      generation_(allocateGeneration()) {}

UbEndpoint::~UbEndpoint() {
    auto status = retire();
    if (!status.ok() || outstanding_wrs_.load(std::memory_order_relaxed) != 0) {
        // A caller that drops the final endpoint reference after a failed
        // native fence must not let Jetty destructors force RESET/delete. A
        // process-lifetime quarantine is safer than DMA-after-free. Imported
        // RM targets are quarantined together with the Jettys: a WR that was
        // never proven complete may still reference them.
        static auto* leaked = new std::vector<JettyPtr>();
        static auto* leaked_targets = new std::vector<RemoteTargetPtr>();
        static auto* leaked_mutex = new std::mutex();
        std::scoped_lock lock(lifecycle_mutex_, *leaked_mutex);
        leaked->insert(leaked->end(), std::make_move_iterator(jetties_.begin()),
                       std::make_move_iterator(jetties_.end()));
        jetties_.clear();
        leaked_targets->insert(
            leaked_targets->end(),
            std::make_move_iterator(imported_targets_.begin()),
            std::make_move_iterator(imported_targets_.end()));
        imported_targets_.clear();
    }
}

void UbEndpoint::rememberFirstError(const Status& candidate, Status& first) {
    if (first.ok() && !candidate.ok()) first = candidate;
}

Status UbEndpoint::resetAndUnbindLocked() {
    Status first_error = Status::OK();
    if (!adapter_) return first_error;

    // Recomputed on every pass: jetty_reset_ok_ records which Jettys proved
    // drained (RESET succeeded), which is the gate for returning a Jetty to
    // the context send-Jetty pool instead of deleting it.
    jetty_reset_ok_.assign(jetties_.size(), false);
    for (size_t index = jetties_.size(); index > 0;) {
        --index;
        auto& jetty = jetties_[index];
        if (!jetty) continue;
        auto reset_status = adapter_->resetJetty(jetty);
        rememberFirstError(reset_status, first_error);
        // Unimporting a peer before RESET succeeds can sever resources still
        // referenced by hardware. Keep this Jetty intact for a retry.
        if (!reset_status.ok()) continue;
        jetty_reset_ok_[index] = true;
        // RM Legacy never calls bindJetty/unbindJetty: WRs reference explicit
        // imported targets instead of a bound remote Jetty. RESET remains part
        // of the accepted provider delete contract for this Jetty.
        if (!rmMode()) {
            rememberFirstError(adapter_->unbindJetty(jetty), first_error);
        }
    }
    return first_error;
}

Status UbEndpoint::unimportTargetsLocked(bool retry_retained) {
    Status first_error = Status::OK();
    auto registry = context_ ? context_->targetRegistry() : nullptr;
    if (!registry) {
        return Status::InternalError(
            "UB endpoint cannot unimport RM targets without a Context target "
            "registry" LOC_MARK);
    }
    if (imported_targets_.empty()) {
        // A previous retire already released this incarnation's references but
        // a provider unimport failed. The handle lives in the registry's
        // quarantine; a retire retry is the place that retries it.
        return retry_retained ? registry->drainRetained() : Status::OK();
    }
    // Release this incarnation's references. The registry unimports the native
    // target on the last release and retains a failed unimport as quarantine,
    // so this endpoint never drops a handle blindly.
    const Status status = registry->releaseAll(imported_targets_);
    rememberFirstError(status, first_error);
    imported_targets_.clear();
    return first_error;
}

Status UbEndpoint::deleteJettysLocked() {
    Status first_error = Status::OK();
    if (!adapter_ && !jetties_.empty()) {
        return Status::InternalError(
            "UB endpoint cannot delete Jettys without an adapter" LOC_MARK);
    }

    // A provider may reject deletion transiently (for example while a Jetty
    // is still busy). Remove only handles whose deletion succeeded. Failed
    // handles and their JFC association remain owned by this endpoint so the
    // next retire()/clear()/uninstall() attempt can retry the exact resource.
    const auto pool = context_ ? context_->jettyPool() : nullptr;
    std::vector<bool> deleted(jetties_.size(), false);
    std::vector<JettyPtr> pooled_jetties;
    std::vector<size_t> pooled_jfc_indices;
    for (size_t index = jetties_.size(); index > 0; --index) {
        auto& jetty = jetties_[index - 1];
        if (!jetty) {
            deleted[index - 1] = true;
            continue;
        }
        if (pool && index - 1 < jetty_reset_ok_.size() &&
            jetty_reset_ok_[index - 1]) {
            // Simple Jetty reuse (RM Legacy only): a Jetty whose RESET
            // succeeded is proven drained, so it returns to the context pool
            // instead of being deleted. A future incarnation re-leasing it
            // posts new WRs with its own imported targets, so the old
            // targets can still be unimported right after this loop.
            pooled_jetties.push_back(std::move(jetty));
            pooled_jfc_indices.push_back(index - 1 < jfc_indices_.size()
                                             ? jfc_indices_[index - 1]
                                             : size_t{0});
            deleted[index - 1] = true;
            continue;
        }
        auto status = adapter_->deleteJetty(jetty);
        rememberFirstError(status, first_error);
        deleted[index - 1] = status.ok();
    }
    if (pool && !pooled_jetties.empty()) {
        pool->release(std::move(pooled_jetties), std::move(pooled_jfc_indices));
    }

    std::vector<JettyPtr> retained_jetties;
    std::vector<size_t> retained_jfc_indices;
    retained_jetties.reserve(jetties_.size());
    retained_jfc_indices.reserve(jfc_indices_.size());
    for (size_t index = 0; index < jetties_.size(); ++index) {
        if (deleted[index]) continue;
        // Both arrays are appended together, so a missing index means the
        // bookkeeping is already corrupt. Fail loudly instead of silently
        // rebinding a retained Jetty to JFC 0; the guard keeps release builds
        // memory-safe.
        assert(index < jfc_indices_.size());
        retained_jetties.push_back(std::move(jetties_[index]));
        retained_jfc_indices.push_back(
            index < jfc_indices_.size() ? jfc_indices_[index] : size_t{0});
    }
    jetties_ = std::move(retained_jetties);
    jfc_indices_ = std::move(retained_jfc_indices);
    return first_error;
}

Status UbEndpoint::failLocked(Status status) {
    if (status.ok()) {
        status = Status::InternalError(
            "UB endpoint entered failed state without an error" LOC_MARK);
    }
    lifecycle_status_ = status;
    state_.store(State::kFailed, std::memory_order_release);

    // Roll back native dependencies in reverse creation order. Imported RM
    // targets are released only after every Jetty that could reference them
    // has been deleted.
    rememberFirstError(resetAndUnbindLocked(), retire_status_);
    rememberFirstError(deleteJettysLocked(), retire_status_);
    if (jetties_.empty()) {
        rememberFirstError(unimportTargetsLocked(/*retry_retained=*/false),
                           retire_status_);
    }
    return lifecycle_status_;
}

Status UbEndpoint::prepare() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kPrepared || current == State::kReady) {
        return Status::OK();
    }
    if (current == State::kFailed) return lifecycle_status_;
    if (current != State::kUninitialized) {
        return Status::InvalidArgument(
            "UB endpoint cannot be prepared in its current state" LOC_MARK);
    }

    state_.store(State::kHandshaking, std::memory_order_release);
    if (!key_.valid()) {
        return failLocked(
            Status::InvalidArgument("Invalid UB endpoint key" LOC_MARK));
    }
    if (!adapter_ || !context_ || !context_->active() || !context_->handle() ||
        !context_->handle()->valid()) {
        return failLocked(Status::InvalidArgument(
            "UB endpoint requires an active context and adapter" LOC_MARK));
    }
    if (context_->topologyId() != key_.local_topology_id) {
        return failLocked(Status::InvalidArgument(
            "UB endpoint key does not match its local context" LOC_MARK));
    }
    if (jetty_count_ == 0 || context_->jfcs().empty()) {
        return failLocked(Status::InvalidArgument(
            "UB endpoint requires at least one Jetty and JFC" LOC_MARK));
    }
    const uint32_t max_jetty = context_->deviceInfo().capabilities.max_jetty;
    if (max_jetty != 0 && jetty_count_ > max_jetty) {
        return failLocked(
            Status::InvalidArgument("Requested endpoint Jetty count exceeds "
                                    "device capability" LOC_MARK));
    }

    // Simple send-Jetty reuse (RM Legacy only): lease the whole set from the
    // context pool when attached. Acquisition is atomic — on failure this
    // incarnation holds nothing, so failLocked() has no Jetties to clean up.
    if (auto pool = context_->jettyPool()) {
        auto status = pool->acquire(jetty_count_, jetties_, jfc_indices_);
        if (!status.ok()) return failLocked(std::move(status));
        jetty_reset_ok_.assign(jetties_.size(), false);
        lifecycle_status_ = Status::OK();
        state_.store(State::kPrepared, std::memory_order_release);
        return Status::OK();
    }

    jetties_.reserve(jetty_count_);
    jfc_indices_.reserve(jetty_count_);
    for (uint32_t index = 0; index < jetty_count_; ++index) {
        const size_t jfc_index = index % context_->jfcs().size();
        auto jfc = context_->jfc(jfc_index);
        if (!jfc || !jfc->valid()) {
            return failLocked(Status::InvalidArgument(
                "UB endpoint selected an inactive JFC" LOC_MARK));
        }

        JettyPtr jetty;
        auto status = adapter_->createJetty(context_->handle(), jfc->handle(),
                                            jetty_options_, jetty);
        if (!status.ok()) {
            // A provider may fail after returning a partially allocated
            // Jetty. Adopt it before entering the retryable failure cleanup.
            if (jetty) {
                jetties_.push_back(std::move(jetty));
                jfc_indices_.push_back(jfc_index);
            }
            return failLocked(std::move(status));
        }
        if (!jetty || !jetty->valid() || jetty->id() == 0) {
            if (jetty) {
                // Adopt the invalid Jetty together with its JFC index: the
                // cleanup path walks jetties_ and jfc_indices_ in lockstep, so
                // appending to only one of them would misalign every later
                // entry.
                jetties_.push_back(std::move(jetty));
                jfc_indices_.push_back(jfc_index);
            }
            return failLocked(Status::InternalError(
                "URMA adapter returned an invalid Jetty" LOC_MARK));
        }
        jetties_.push_back(std::move(jetty));
        jfc_indices_.push_back(jfc_index);
    }
    jetty_reset_ok_.assign(jetties_.size(), false);

    lifecycle_status_ = Status::OK();
    state_.store(State::kPrepared, std::memory_order_release);
    return Status::OK();
}

Status UbEndpoint::bind(const UbBootstrapDesc& peer) {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (rmMode()) {
        return Status::InvalidArgument(
            "RM Legacy endpoints must use bindRm, not RC bind" LOC_MARK);
    }
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kReady) {
        if (samePeer(peer_, peer)) return Status::OK();
        return Status::InvalidArgument(
            "UB endpoint is already bound to another peer generation" LOC_MARK);
    }
    if (current == State::kFailed) return lifecycle_status_;
    if (current != State::kPrepared) {
        return Status::InvalidArgument(
            "UB endpoint must be prepared before bind" LOC_MARK);
    }

    state_.store(State::kBinding, std::memory_order_release);
    if (!peer.reply_msg.empty()) {
        return failLocked(Status::RpcServiceError(
            std::string("Peer rejected UB bootstrap: ") + peer.reply_msg));
    }
    if (peer.protocol_version != 1) {
        return failLocked(Status::InvalidArgument(
            "Unsupported UB bootstrap protocol version" LOC_MARK));
    }
    if (peer.endpoint_generation == 0 || peer.local_eid.empty()) {
        return failLocked(
            Status::InvalidArgument("Peer UB bootstrap is missing EID or "
                                    "endpoint generation" LOC_MARK));
    }
    if (peer.jetty_ids.size() != jetties_.size()) {
        return failLocked(
            Status::InvalidArgument("Peer UB bootstrap Jetty count does not "
                                    "match local endpoint" LOC_MARK));
    }
    if (!peer.jetty_uasids.empty() &&
        peer.jetty_uasids.size() != jetties_.size()) {
        return failLocked(
            Status::InvalidArgument("Peer UB bootstrap UASID count does not "
                                    "match local endpoint" LOC_MARK));
    }

    for (size_t index = 0; index < jetties_.size(); ++index) {
        if (peer.jetty_ids[index] == 0) {
            return failLocked(Status::InvalidArgument(
                "Peer UB bootstrap contains a zero Jetty ID" LOC_MARK));
        }
        if (rmMode()) {
            // RM Legacy: a local Jetty is never bound to a remote one; the WR
            // carries its target per post (see rmTarget()). The peer
            // descriptor is still validated above and kept as the target
            // identity, so the pooled path can post through a leased member.
            //
            // Review M9: an RM WR can only target a peer whose receive side is
            // RM as well. Peers advertise it as an extra capability string; a
            // peer that does not is rejected fail-closed instead of producing a
            // silently unusable target.
            const auto rm_capability =
                std::find(peer.capabilities.begin(), peer.capabilities.end(),
                          kUbCapabilityRmTransport);
            if (rm_capability == peer.capabilities.end()) {
                return failLocked(Status::InvalidArgument(
                    "Peer UB bootstrap does not advertise RM transport; an "
                    "RM endpoint cannot target it" LOC_MARK));
            }
            continue;
        }
        RemoteJettyInfo remote;
        remote.eid = peer.local_eid;
        remote.id = peer.jetty_ids[index];
        if (!peer.jetty_uasids.empty()) {
            remote.uasid = peer.jetty_uasids[index];
        }
        auto status = adapter_->bindJetty(jetties_[index], remote);
        if (!status.ok()) return failLocked(std::move(status));
    }

    peer_ = peer;
    peer_generation_.store(peer.endpoint_generation, std::memory_order_release);
    lifecycle_status_ = Status::OK();
    state_.store(State::kReady, std::memory_order_release);
    return Status::OK();
}

Status UbEndpoint::bindRm(const UbBootstrapDesc& peer,
                          const UbExpectedPeerIdentity& expected) {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (!rmMode()) {
        return Status::InvalidArgument(
            "RC legacy endpoints must use bind, not bindRm" LOC_MARK);
    }
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kReady) {
        // P1-02: classify a repeated RM bootstrap against the incumbent with
        // the full v2 content and hierarchical generation comparison instead
        // of the RM-blind samePeer(). Every non-duplicate outcome preserves
        // the incumbent: stale/conflict are rejected, and a newer peer
        // generation turns over through retire + a fresh incarnation rather
        // than an in-place rebind that could unimport targets still
        // referenced by in-flight WRs.
        UbBootstrapGenerationComparison comparison =
            UbBootstrapGenerationComparison::kConflict;
        const auto compare_status =
            compareUbBootstrapV2(peer, peer_, comparison);
        if (!compare_status.ok()) {
            return Status::InvalidArgument(
                "Repeated RM bootstrap descriptor is structurally invalid: " +
                compare_status.ToString() + LOC_MARK);
        }
        switch (comparison) {
            case UbBootstrapGenerationComparison::kDuplicate:
                return Status::OK();
            case UbBootstrapGenerationComparison::kStale:
                return Status::InvalidArgument(
                    "RM bootstrap generation is stale; the incumbent endpoint "
                    "is preserved" LOC_MARK);
            case UbBootstrapGenerationComparison::kConflict:
                return Status::InvalidArgument(
                    "RM bootstrap conflicts with the incumbent generation: "
                    "the same incarnation published different "
                    "content" LOC_MARK);
            case UbBootstrapGenerationComparison::kNewer:
            default:
                return Status::InvalidArgument(
                    "RM peer generation advanced; the endpoint must be "
                    "re-created through a new incarnation" LOC_MARK);
        }
    }
    if (current == State::kFailed) return lifecycle_status_;
    if (current != State::kPrepared) {
        return Status::InvalidArgument(
            "UB endpoint must be prepared before bindRm" LOC_MARK);
    }

    state_.store(State::kBinding, std::memory_order_release);
    if (!peer.reply_msg.empty()) {
        return failLocked(Status::RpcServiceError(
            std::string("Peer rejected UB bootstrap: ") + peer.reply_msg));
    }
    if (peer.protocol_version != 2 || !peer.rm.has_value()) {
        return failLocked(
            Status::InvalidArgument("RM bootstrap requires a v2 descriptor "
                                    "with an RM payload" LOC_MARK));
    }
    auto valid_status = validateUbBootstrap(peer);
    if (!valid_status.ok()) return failLocked(std::move(valid_status));
    const UbRmBootstrapDesc& rm = *peer.rm;
    if (rm.transport_mode != "RM" || rm.device_kind != "PHYSICAL") {
        return failLocked(Status::InvalidArgument(
            "RM bootstrap must declare transport_mode=RM and "
            "device_kind=PHYSICAL" LOC_MARK));
    }
    // Identity gate: every peer-asserted identity field must match the
    // trusted metadata the transport derived (registered segment name, the
    // locally constructed peer NIC path, and the control-plane-anchored
    // device fields). Mismatch fails closed; nothing is imported.
    auto identity_status = validateUbBootstrapV2PeerIdentity(peer, expected);
    if (!identity_status.ok()) {
        return failLocked(Status::InvalidArgument(
            "Peer RM bootstrap identity does not match trusted metadata: " +
            identity_status.ToString() + LOC_MARK));
    }
    if (rm.targets.size() != jetties_.size()) {
        return failLocked(
            Status::InvalidArgument("Peer RM target count does not match "
                                    "local endpoint" LOC_MARK));
    }
    for (const auto& target : rm.targets) {
        if (target.eid != peer.local_eid || target.id == 0 ||
            target.target_epoch != peer.endpoint_generation ||
            target.profile.trans_mode != kUrmaTransportModeRm ||
            target.profile.target_type != kUrmaTargetTypeJetty ||
            target.profile.tp_type != kUrmaTpTypeCtp) {
            return failLocked(Status::InvalidArgument(
                "Peer RM target descriptor violates the RM/CTP profile "
                "contract" LOC_MARK));
        }
    }

    // Import every peer target through the Context registry. A target is a
    // node-wide resource shared by every endpoint/probe on this Context, so the
    // registry deduplicates it and reference-counts this incarnation; the
    // native import is released only when the last reference goes away. A
    // failed import rolls the registry back to its previous state, and any
    // partially allocated native handle is adopted and quarantined there (same
    // provider contract as createJetty in prepare()).
    auto registry = context_ ? context_->targetRegistry() : nullptr;
    if (!registry) {
        return failLocked(Status::InternalError(
            "UB endpoint has no Context target registry" LOC_MARK));
    }
    const std::string peer_scope =
        peer.segment_name + "|" + rm.physical_device_identity;
    std::vector<RemoteTargetPtr> imported;
    const Status import_status =
        registry->importAll(peer_scope, rm.targets, imported);
    if (!import_status.ok()) {
        return failLocked(import_status);
    }

    // Compare-and-commit: the incumbent state is only replaced after every
    // validation and import succeeded.
    imported_targets_ = std::move(imported);
    peer_ = peer;
    peer_generation_.store(peer.endpoint_generation, std::memory_order_release);
    lifecycle_status_ = Status::OK();
    state_.store(State::kReady, std::memory_order_release);
    return Status::OK();
}

Status UbEndpoint::makeBootstrapDesc(const std::string& segment_name,
                                     const std::string& local_nic_path,
                                     const std::string& peer_nic_path,
                                     uint64_t segment_generation,
                                     UbBootstrapDesc& output) const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (rmMode()) {
        return Status::InvalidArgument(
            "RM Legacy endpoints must use makeBootstrapDescV2" LOC_MARK);
    }
    const State current = state_.load(std::memory_order_relaxed);
    if (current != State::kPrepared && current != State::kReady) {
        return Status::InvalidArgument(
            "UB endpoint must be prepared before bootstrap" LOC_MARK);
    }

    output = UbBootstrapDesc{};
    output.protocol_version = 1;
    output.segment_name = segment_name;
    output.local_nic_path = local_nic_path.empty()
                                ? context_->deviceInfo().native_device_path
                                : local_nic_path;
    output.peer_nic_path =
        peer_nic_path.empty() ? key_.peer_nic_path : peer_nic_path;
    output.local_device_name = context_->deviceInfo().native_device_name;
    output.local_device_id = context_->topologyId();
    output.local_eid_index = static_cast<int>(context_->deviceInfo().eid_index);
    output.local_eid = context_->deviceInfo().eid;
    output.jetty_ids.reserve(jetties_.size());
    output.jetty_uasids.reserve(jetties_.size());
    for (const auto& jetty : jetties_) {
        if (!jetty || !jetty->valid() || jetty->id() == 0) {
            return Status::InternalError(
                "UB endpoint contains an invalid Jetty" LOC_MARK);
        }
        output.jetty_ids.push_back(jetty->id());
        output.jetty_uasids.push_back(jetty->uasid());
    }
    output.endpoint_generation = generation_;
    output.segment_generation = segment_generation;
    // Review M9/B3: the capability list is assigned exactly once, and the
    // transport mode is part of it. A separate push_back() before this
    // assignment was overwritten, so peers never saw the mode and an RM
    // endpoint rejected every peer while the peer rejected it back.
    output.capabilities = {"read", "write", "endpoint_generation"};
    output.capabilities.push_back(rmMode() ? kUbCapabilityRmTransport
                                           : kUbCapabilityRcTransport);
    return Status::OK();
}

Status UbEndpoint::makeBootstrapDescV2(const std::string& segment_name,
                                       const std::string& local_nic_path,
                                       const std::string& peer_nic_path,
                                       uint64_t segment_generation,
                                       uint64_t node_incarnation,
                                       uint64_t topology_epoch,
                                       UbBootstrapDesc& output) const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (!rmMode()) {
        return Status::InvalidArgument(
            "RC legacy endpoints must use makeBootstrapDesc" LOC_MARK);
    }
    const State current = state_.load(std::memory_order_relaxed);
    if (current != State::kPrepared && current != State::kReady) {
        return Status::InvalidArgument(
            "UB endpoint must be prepared before bootstrap" LOC_MARK);
    }

    const auto& device = context_->deviceInfo();
    if (device.kind != DeviceKind::PHYSICAL) {
        return Status::InvalidArgument(
            "RM bootstrap may only be produced by a trusted physical "
            "device" LOC_MARK);
    }
    const std::string local_identity =
        makePhysicalDeviceIdentity(device.native_device_path, device.eid);

    output = UbBootstrapDesc{};
    output.protocol_version = 2;
    output.segment_name = segment_name;
    output.local_nic_path =
        local_nic_path.empty() ? device.native_device_path : local_nic_path;
    output.peer_nic_path =
        peer_nic_path.empty() ? key_.peer_nic_path : peer_nic_path;
    output.local_device_name = device.native_device_name;
    output.local_device_id = context_->topologyId();
    output.local_eid_index = static_cast<int>(device.eid_index);
    output.local_eid = device.eid;
    output.endpoint_generation = generation_;
    output.segment_generation = segment_generation;

    UbRmBootstrapDesc rm;
    rm.transport_mode = "RM";
    rm.device_kind = "PHYSICAL";
    rm.physical_device_identity = local_identity;
    rm.node_incarnation = node_incarnation;
    rm.context_incarnation = context_->incarnation();
    rm.topology_epoch = topology_epoch;
    rm.transport_modes = device.capabilities.transport_modes;
    rm.provider_capabilities = {"read", "write", "rm"};
    rm.targets.reserve(jetties_.size());
    for (const auto& jetty : jetties_) {
        if (!jetty || !jetty->valid() || jetty->id() == 0) {
            return Status::InternalError(
                "UB endpoint contains an invalid Jetty" LOC_MARK);
        }
        UbRmTargetDescriptor target;
        target.target_identity =
            local_identity + "#jetty#" + std::to_string(jetty->id());
        target.eid = device.eid;
        target.uasid = jetty->uasid();
        target.id = jetty->id();
        target.target_epoch = generation_;
        target.profile.trans_mode = jetty_options_.trans_mode;
        target.profile.target_type = kUrmaTargetTypeJetty;
        target.profile.tp_type = kUrmaTpTypeCtp;
        rm.targets.push_back(std::move(target));
    }
    output.rm = std::move(rm);
    return Status::OK();
}

bool UbEndpoint::tryAcquireOutstanding(uint64_t bytes) noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_relaxed) != State::kReady || !context_ ||
        !context_->active()) {
        return false;
    }

    outstanding_wrs_.fetch_add(1, std::memory_order_relaxed);
    outstanding_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    context_->addInflight(bytes);
    return true;
}

void UbEndpoint::releaseOutstanding(uint64_t bytes) noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const uint64_t current_wrs =
        outstanding_wrs_.load(std::memory_order_relaxed);
    if (current_wrs == 0) return;

    outstanding_wrs_.store(current_wrs - 1, std::memory_order_relaxed);
    const uint64_t current_bytes =
        outstanding_bytes_.load(std::memory_order_relaxed);
    outstanding_bytes_.store(current_bytes >= bytes ? current_bytes - bytes : 0,
                             std::memory_order_relaxed);
    context_->removeInflight(bytes);

    if (current_wrs == 1 &&
        state_.load(std::memory_order_relaxed) == State::kDestroying) {
        (void)finishRetireLocked();
    }
}

Status UbEndpoint::finishRetireLocked() {
    if (state_.load(std::memory_order_relaxed) == State::kDestroyed) {
        return retire_status_;
    }
    if (outstanding_wrs_.load(std::memory_order_relaxed) != 0) {
        return retire_status_;
    }

    // With no outstanding WR, RESET itself is a sufficient fence. When
    // quiesce() ran earlier this is an idempotent cleanup pass.
    auto reset_status = resetAndUnbindLocked();
    if (!reset_status.ok()) {
        retire_status_ = reset_status;
        return reset_status;
    }
    native_quiesced_ = true;
    auto delete_status = deleteJettysLocked();
    if (!delete_status.ok()) {
        retire_status_ = delete_status;
        return delete_status;
    }
    // Imported RM targets are unimported only after every Jetty that could
    // reference them has been deleted; a failed unimport keeps the handle
    // owned and leaves the incarnation short of kDestroyed.
    auto unimport_status = unimportTargetsLocked(/*retry_retained=*/true);
    if (!unimport_status.ok()) {
        retire_status_ = unimport_status;
        return unimport_status;
    }
    if (!imported_targets_.empty()) {
        retire_status_ = Status::InternalError(
            "UB endpoint retains RM targets whose unimport failed" LOC_MARK);
        return retire_status_;
    }
    retire_status_ = Status::OK();
    peer_ = UbBootstrapDesc{};
    peer_generation_.store(0, std::memory_order_release);
    state_.store(State::kDestroyed, std::memory_order_release);
    return retire_status_;
}

Status UbEndpoint::retire() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kDestroyed) return retire_status_;

    if (current != State::kDestroying) {
        state_.store(State::kDestroying, std::memory_order_release);
    }
    if (outstanding_wrs_.load(std::memory_order_relaxed) != 0) {
        return retire_status_;
    }
    return finishRetireLocked();
}

Status UbEndpoint::quiesce(uint32_t timeout_ms,
                           std::vector<Completion>& completions) {
    completions.clear();
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kDestroyed) return retire_status_;
    if (!adapter_ || timeout_ms == 0) {
        return Status::InvalidArgument(
            "UB endpoint quiesce requires an adapter and timeout" LOC_MARK);
    }

    state_.store(State::kDestroying, std::memory_order_release);
    Status fence_error = Status::OK();
    if (!native_quiesced_) {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        for (const auto& jetty : jetties_) {
            if (!jetty) continue;
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                if (fence_error.ok()) {
                    fence_error = Status::RdmaError(
                        "UB endpoint Jetty drain budget exhausted");
                }
                break;
            }
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                      now);
            const uint32_t remaining_ms =
                static_cast<uint32_t>(std::max<int64_t>(1, remaining.count()));
            std::vector<Completion> drained;
            auto status = adapter_->quiesceJetty(jetty, remaining_ms, drained);
            completions.insert(completions.end(),
                               std::make_move_iterator(drained.begin()),
                               std::make_move_iterator(drained.end()));
            if (!status.ok() && fence_error.ok()) fence_error = status;
        }
        if (!fence_error.ok()) {
            // Do not RESET, unbind, or delete anything without a fence for
            // every Jetty. A later shutdown attempt can safely retry.
            return fence_error;
        }
        native_quiesced_ = true;
    }

    auto reset_status = resetAndUnbindLocked();
    if (!reset_status.ok()) return reset_status;
    if (outstanding_wrs_.load(std::memory_order_relaxed) == 0) {
        return finishRetireLocked();
    }
    return Status::OK();
}

bool UbEndpoint::reusable() const noexcept {
    switch (state()) {
        case State::kUninitialized:
        case State::kHandshaking:
        case State::kPrepared:
        case State::kBinding:
        case State::kReady:
            return true;
        case State::kFailed:
        case State::kDestroying:
        case State::kDestroyed:
            return false;
    }
    return false;
}

size_t UbEndpoint::jettyCount() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return jetties_.size();
}

JettyPtr UbEndpoint::jetty(size_t index) const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (jetties_.empty()) return nullptr;
    return jetties_[index % jetties_.size()];
}

size_t UbEndpoint::jfcIndex(size_t jetty_index) const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (jfc_indices_.empty()) return 0;
    return jfc_indices_[jetty_index % jfc_indices_.size()];
}

std::vector<JettyPtr> UbEndpoint::jetties() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return jetties_;
}

RemoteTargetPtr UbEndpoint::rmTarget(size_t jetty_index) const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (imported_targets_.empty()) return nullptr;
    return imported_targets_[jetty_index % imported_targets_.size()];
}

size_t UbEndpoint::importedTargetCount() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return imported_targets_.size();
}

Status UbEndpoint::lifecycleStatus() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return lifecycle_status_;
}

}  // namespace mooncake::tent::ub
