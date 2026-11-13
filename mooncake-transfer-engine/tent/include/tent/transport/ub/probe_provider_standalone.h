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
//
// Standalone probe resource provider (P1). It owns the per-port probe
// resources directly through the URMA adapter, so link probing does not depend
// on the business Jetty pool:
//
//   * one dedicated probe JFC per port (or a borrowed business JFC in shared
//     mode; the provider never deletes a borrowed JFC),
//   * one probe Jetty per port (no local RC bind),
//   * one page-aligned probe source page per port and generation.
//
// Remote probe areas and remote probe Jetty identities come through
// ProbePeerDirectory. P1 ships this seam with a test/stub implementation; the
// peer handshake (bootstrap extension) lands in P2. Lane retire/rebuild and
// quarantine transfer are recorded here and become native in P4.
//
// Design: docs/analyze/tent-ub-probe-standalone-supply.md.

#ifndef TENT_TRANSPORT_UB_PROBE_PROVIDER_STANDALONE_H_
#define TENT_TRANSPORT_UB_PROBE_PROVIDER_STANDALONE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/probe_resources.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

// One local probe port supplied by the transport. `generation` is the probe
// channel generation (non-zero); `shared_jfc` is required in shared mode and
// ignored in dedicated mode.
struct ProbeLocalPortConfig {
    Topology::NicID local_nic{-1};
    ContextPtr context;
    JfcPtr shared_jfc;
    uint64_t generation{1};
};

// Supplies the remote half of the probe graph: per-edge probe area descriptor
// and remote probe Jetty identity. P2 replaces the stub with the bootstrap
// handshake; the provider itself does not parse peer metadata. The directory
// must outlive the provider and must not call back into it.
class ProbePeerDirectory {
   public:
    virtual ~ProbePeerDirectory() = default;

    virtual Status snapshotEdges(std::vector<ProbeEdgeSupply>& edges) = 0;
};

// Creates and owns probe-native resources independently of the business Jetty
// pool. Not copyable; all entry points are safe to call from different
// threads.
class StandaloneProbeResourceProvider final : public ProbeResourceProvider {
   public:
    StandaloneProbeResourceProvider(std::shared_ptr<UrmaAdapter> adapter,
                                    ProbeParams params,
                                    std::vector<ProbeLocalPortConfig> ports,
                                    ProbePeerDirectory* directory);
    ~StandaloneProbeResourceProvider() override;

    StandaloneProbeResourceProvider(const StandaloneProbeResourceProvider&) =
        delete;
    StandaloneProbeResourceProvider& operator=(
        const StandaloneProbeResourceProvider&) = delete;

    // Interface 4: creates the local resources once and returns the full
    // graph. A second call after success is rejected; a failed call rolls back
    // every resource it created.
    Status snapshot(ProbeGraph& graph) override;

    // Interface 2 seam. P1 records the request; native modify/flush/delete and
    // the replacement generation land in P4.
    void retireProbeLane(const LaneKey& lane, uint64_t jetty_generation,
                         uint64_t error_epoch, const PathVersion& path,
                         const Status& reason) override;

    // Unresolved responsibility handoff. P1 retains the references in a
    // quarantine list until shutdown; P4 gives the quarantine its own lifetime
    // so source pages can be reclaimed only after every reference is gone.
    void handoffUnproven(const ProbeHandoff& handoff) override;

    // Releases every local probe resource. Idempotent; must run before the
    // owning contexts are shut down.
    Status shutdown();

    struct PortView {
        Topology::NicID local_nic{-1};
        uint64_t generation{0};
        uint32_t native_jetty_id{0};
        uint64_t source_address{0};
        uint64_t source_length{0};
        uint64_t source_generation{0};
        bool owns_jfc{false};
    };

    [[nodiscard]] std::vector<PortView> portViews() const;
    [[nodiscard]] size_t quarantineCount() const;
    [[nodiscard]] uint64_t retireCalls() const;
    [[nodiscard]] bool initialized() const;

   private:
    struct PortState {
        ProbeLocalPortConfig config;
        JfcPtr probe_jfc;
        bool owns_jfc{false};
        JettyPtr probe_jetty;
        LocalSegmentPtr source_segment;
        void* source_memory{nullptr};
        size_t source_size{0};
        uint64_t source_generation{1};
    };

    Status createPortLocked(PortState& port);
    Status destroyPortLocked(PortState& port);
    void rollbackPortsLocked();
    void releaseQuarantineLocked();
    Status buildGraphLocked(ProbeGraph& graph);

    mutable std::mutex mutex_;
    const std::shared_ptr<UrmaAdapter> adapter_;
    const ProbeParams params_;
    std::vector<PortState> ports_;
    ProbePeerDirectory* const directory_;
    std::vector<ProbeHandoff> quarantine_;
    uint64_t retire_calls_{0};
    uint64_t next_graph_epoch_{1};
    bool initialized_{false};
    bool shut_down_{false};
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_PROVIDER_STANDALONE_H_
