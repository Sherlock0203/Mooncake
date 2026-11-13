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

#include "tent/transport/ub/probe_provider_standalone.h"

#include <cstdlib>
#include <cstring>
#include <set>
#include <utility>

#include <glog/logging.h>
#include <unistd.h>

namespace mooncake {
namespace tent {
namespace ub {
namespace {

size_t probePageSize() {
    const long page = ::sysconf(_SC_PAGESIZE);
    return page > 0 ? static_cast<size_t>(page) : 4096;
}

}  // namespace

StandaloneProbeResourceProvider::StandaloneProbeResourceProvider(
    std::shared_ptr<UrmaAdapter> adapter, ProbeParams params,
    std::vector<ProbeLocalPortConfig> ports, ProbePeerDirectory* directory)
    : adapter_(std::move(adapter)),
      params_(std::move(params)),
      directory_(directory) {
    ports_.reserve(ports.size());
    for (auto& config : ports) {
        PortState state;
        state.config = std::move(config);
        state.source_generation = state.config.generation;
        ports_.push_back(std::move(state));
    }
}

StandaloneProbeResourceProvider::~StandaloneProbeResourceProvider() {
    (void)shutdown();
}

Status StandaloneProbeResourceProvider::createPortLocked(PortState& port) {
    if (!params_.valid()) {
        return Status::InvalidArgument("invalid probe parameters");
    }
    if (!adapter_) {
        return Status::InvalidArgument("probe provider has no URMA adapter");
    }
    if (!port.config.context || !port.config.context->valid()) {
        return Status::InvalidArgument("probe port has no active context");
    }
    if (port.config.local_nic < 0) {
        return Status::InvalidArgument("probe port has an invalid local NIC");
    }
    if (port.config.generation == 0) {
        return Status::InvalidArgument(
            "probe port generation must be non-zero");
    }

    JfcPtr jfc;
    bool owns_jfc = false;
    if (params_.jfc_mode == ProbeJfcMode::kDedicated) {
        JfcOptions options;
        options.depth = params_.jfc_depth;
        options.enable_completion_events = false;
        auto status = adapter_->createJfc(port.config.context, options, jfc);
        if (!status.ok()) return status;
        if (!jfc) {
            return Status::InternalError("URMA adapter returned no probe JFC");
        }
        owns_jfc = true;
    } else {
        jfc = port.config.shared_jfc;
        if (!jfc || !jfc->valid()) {
            return Status::InvalidArgument(
                "shared probe mode requires an active business JFC");
        }
    }

    JettyOptions jetty_options;
    jetty_options.max_sge = 1;
    JettyPtr jetty;
    auto status =
        adapter_->createJetty(port.config.context, jfc, jetty_options, jetty);
    if (!status.ok() || !jetty) {
        if (owns_jfc) (void)adapter_->deleteJfc(jfc);
        return status.ok() ? Status::InternalError(
                                 "URMA adapter returned no probe Jetty")
                           : status;
    }

    const size_t page = probePageSize();
    void* memory = nullptr;
    if (::posix_memalign(&memory, page, page) != 0 || memory == nullptr) {
        (void)adapter_->deleteJetty(jetty);
        if (owns_jfc) (void)adapter_->deleteJfc(jfc);
        return Status::InternalError("Cannot allocate the probe source page");
    }
    // Content is irrelevant: the peer never reads it and the payload is a
    // single constant byte. It must still cover a whole page because that is
    // the registration granularity (payload=1 != registration length 1).
    std::memset(memory, 0x5A, page);

    SegmentOptions segment_options;
    segment_options.access = SEGMENT_ACCESS_LOCAL_ONLY;
    LocalSegmentPtr segment;
    status = adapter_->registerLocalSegment(port.config.context,
                                            reinterpret_cast<uint64_t>(memory),
                                            page, segment_options, segment);
    if (!status.ok() || !segment) {
        (void)adapter_->deleteJetty(jetty);
        if (owns_jfc) (void)adapter_->deleteJfc(jfc);
        std::free(memory);
        return status.ok()
                   ? Status::InternalError(
                         "URMA adapter returned no probe source segment")
                   : status;
    }

    port.probe_jfc = std::move(jfc);
    port.owns_jfc = owns_jfc;
    port.probe_jetty = std::move(jetty);
    port.source_segment = std::move(segment);
    port.source_memory = memory;
    port.source_size = page;
    port.source_generation = port.config.generation;
    return Status::OK();
}

Status StandaloneProbeResourceProvider::destroyPortLocked(PortState& port) {
    Status first_error = Status::OK();
    if (adapter_ && port.probe_jetty) {
        auto status = adapter_->deleteJetty(port.probe_jetty);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    if (adapter_ && port.owns_jfc && port.probe_jfc) {
        auto status = adapter_->deleteJfc(port.probe_jfc);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    if (adapter_ && port.source_segment) {
        auto status = adapter_->unregisterLocalSegment(port.source_segment);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    if (port.source_memory != nullptr) {
        std::free(port.source_memory);
        port.source_memory = nullptr;
    }
    port.probe_jfc.reset();
    port.owns_jfc = false;
    port.probe_jetty.reset();
    port.source_segment.reset();
    port.source_size = 0;
    port.source_generation = 0;
    return first_error;
}

void StandaloneProbeResourceProvider::rollbackPortsLocked() {
    for (auto& port : ports_) {
        (void)destroyPortLocked(port);
    }
}

void StandaloneProbeResourceProvider::releaseQuarantineLocked() {
    if (quarantine_.empty()) return;
    // P1 keeps the references alive until shutdown. P4 turns this into a real
    // quarantine with per-generation source-page reclamation.
    LOG(INFO) << "UB standalone probe provider: releasing "
              << quarantine_.size()
              << " quarantined probe handoff(s) at shutdown";
    quarantine_.clear();
}

Status StandaloneProbeResourceProvider::buildGraphLocked(ProbeGraph& graph) {
    std::vector<ProbeEdgeSupply> edges;
    if (directory_ == nullptr) {
        return Status::InvalidArgument("probe provider has no peer directory");
    }
    auto status = directory_->snapshotEdges(edges);
    if (!status.ok()) return status;

    std::set<PathVersion> versions;
    std::set<Topology::NicID> ports;
    for (const auto& port : ports_) ports.insert(port.config.local_nic);
    for (const auto& edge : edges) {
        if (!edge.version.valid()) {
            return Status::InvalidArgument(
                "probe directory returned an invalid path version");
        }
        if (!edge.peer.valid()) {
            return Status::InvalidArgument(
                "probe directory returned an invalid peer");
        }
        if (!edge.area_segment || edge.area_length == 0 ||
            edge.area_epoch == 0) {
            return Status::InvalidArgument(
                "probe directory returned an invalid probe area");
        }
        if (ports.find(edge.version.path.local_nic) == ports.end()) {
            return Status::InvalidArgument(
                "probe directory edge references an unknown local port");
        }
        if (!versions.insert(edge.version).second) {
            return Status::InvalidArgument(
                "probe directory returned a duplicate path version");
        }
    }

    ProbeGraph built;
    built.graph_epoch = next_graph_epoch_;
    built.ports.reserve(ports_.size());
    for (const auto& port : ports_) {
        ProbePortSupply supply;
        supply.local_nic = port.config.local_nic;
        supply.context = port.config.context;
        supply.probe_jetty = port.probe_jetty;
        supply.jetty_generation = port.config.generation;
        supply.probe_jfc = params_.jfc_mode == ProbeJfcMode::kDedicated
                               ? port.probe_jfc
                               : nullptr;
        supply.source_segment = port.source_segment;
        supply.source_address = reinterpret_cast<uint64_t>(port.source_memory);
        supply.source_length = 1;
        supply.source_generation = port.source_generation;
        supply.control_credit = 1;
        supply.peer_budget = params_.peer_budget;
        built.ports.push_back(std::move(supply));
    }
    built.edges = std::move(edges);
    graph = std::move(built);
    return Status::OK();
}

Status StandaloneProbeResourceProvider::snapshot(ProbeGraph& graph) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) {
        return Status::InvalidArgument(
            "standalone probe provider is shut down");
    }
    if (initialized_) {
        return Status::InvalidArgument(
            "standalone probe provider is already snapshotted");
    }
    if (!params_.valid()) {
        return Status::InvalidArgument("invalid probe parameters");
    }
    if (!params_.enabled) {
        return Status::InvalidArgument("probe is disabled by configuration");
    }
    if (directory_ == nullptr) {
        return Status::InvalidArgument("probe provider has no peer directory");
    }
    if (ports_.empty()) {
        return Status::InvalidArgument("probe provider has no local port");
    }

    std::set<Topology::NicID> local_nics;
    for (const auto& port : ports_) {
        if (port.config.local_nic < 0 ||
            !local_nics.insert(port.config.local_nic).second ||
            port.config.generation == 0 || !port.config.context ||
            !port.config.context->valid()) {
            return Status::InvalidArgument(
                "invalid or duplicate local probe port");
        }
    }

    for (auto& port : ports_) {
        auto status = createPortLocked(port);
        if (!status.ok()) {
            rollbackPortsLocked();
            return status;
        }
    }

    ProbeGraph built;
    auto status = buildGraphLocked(built);
    if (!status.ok()) {
        rollbackPortsLocked();
        return status;
    }
    ++next_graph_epoch_;
    initialized_ = true;
    graph = std::move(built);
    LOG(INFO) << "UB standalone probe provider created " << ports_.size()
              << " probe port(s)";
    return Status::OK();
}

void StandaloneProbeResourceProvider::retireProbeLane(const LaneKey& lane,
                                                      uint64_t jetty_generation,
                                                      uint64_t error_epoch,
                                                      const PathVersion& path,
                                                      const Status& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++retire_calls_;
    // P1 only records the retirement: the PLM already stopped posting on this
    // lane (handle_valid=false). Native modify/flush/delete, the replacement
    // generation and kProbeHandleReplaced land in P4.
    LOG_EVERY_N(WARNING, 100)
        << "UB standalone probe provider: lane retire recorded "
           "(native rebuild lands in P4): local_nic="
        << lane.local_nic << " native_jetty_id=" << lane.native_jetty_id
        << " jetty_generation=" << jetty_generation
        << " error_epoch=" << error_epoch << " path=" << path.path.local_nic
        << "/" << path.path.segment_id << "/" << path.path.remote_nic
        << " reason=" << reason.ToString();
}

void StandaloneProbeResourceProvider::handoffUnproven(
    const ProbeHandoff& handoff) {
    std::lock_guard<std::mutex> lock(mutex_);
    quarantine_.push_back(handoff);
    LOG_EVERY_N(INFO, 100)
        << "UB standalone probe provider: quarantined probe handoff token="
        << handoff.token
        << " reason=" << static_cast<int>(handoff.orphan_reason);
}

Status StandaloneProbeResourceProvider::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) return Status::OK();
    Status first_error = Status::OK();
    // Drop handoff references first: they may still point at older-generation
    // source pages that the current port state does not own.
    releaseQuarantineLocked();
    for (auto& port : ports_) {
        auto status = destroyPortLocked(port);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    initialized_ = false;
    shut_down_ = true;
    LOG(INFO) << "UB standalone probe provider shut down";
    return first_error;
}

std::vector<StandaloneProbeResourceProvider::PortView>
StandaloneProbeResourceProvider::portViews() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PortView> views;
    views.reserve(ports_.size());
    for (const auto& port : ports_) {
        if (!port.probe_jetty) continue;
        views.push_back(PortView{port.config.local_nic, port.config.generation,
                                 port.probe_jetty->id(),
                                 reinterpret_cast<uint64_t>(port.source_memory),
                                 1, port.source_generation, port.owns_jfc});
    }
    return views;
}

size_t StandaloneProbeResourceProvider::quarantineCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return quarantine_.size();
}

uint64_t StandaloneProbeResourceProvider::retireCalls() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return retire_calls_;
}

bool StandaloneProbeResourceProvider::initialized() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return initialized_;
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
