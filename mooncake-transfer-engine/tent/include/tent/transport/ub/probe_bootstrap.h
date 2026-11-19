// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// ProbeBootstrapUb wire (design §7). One node-level, bidirectional exchange:
// the request carries the requester's probe publication plus its declared
// topology (diagnostics only), the response carries the responder's current
// publication. The wire never carries a route that the configuration did not
// declare.

#ifndef TENT_TRANSPORT_UB_PROBE_BOOTSTRAP_H_
#define TENT_TRANSPORT_UB_PROBE_BOOTSTRAP_H_

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/probe_resources.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

inline constexpr uint32_t kProbeBootstrapSchemaVersion = 1;
// Probe transport is always RM, independent of the business runtime mode.
inline constexpr const char* kProbeBootstrapTransportMode = "rm";
inline constexpr const char* kProbeBootstrapDeviceKind = "physical";

// One port's runtime handles. `port_id` is the publisher's local alias and is
// diagnostic only; the cross-node identity is `eid`.
struct ProbeBootstrapPort {
    std::string port_id;
    std::string topology_name;
    std::string eid;
    uint64_t target_epoch{0};
    RemoteJettyRef jetty;
    SegmentDescriptor area_descriptor;
    uint64_t area_length{0};
    uint64_t area_epoch{0};
    uint32_t area_token{0};
    bool node_gate_ok{false};
    bool port_gate_ok{false};
    bool profile_gate_ok{false};
};

struct ProbeBootstrapPeerPort {
    std::string id;
    std::string eid;
};

struct ProbeBootstrapRequest {
    uint32_t schema_version{kProbeBootstrapSchemaVersion};
    std::string requester_segment;
    uint64_t requester_node_incarnation{0};
    uint64_t requester_topology_epoch{0};
    uint64_t requester_publication_epoch{0};
    std::vector<std::string> declared_local_ports;
    std::vector<ProbeBootstrapPort> ports;
    // Diagnostics/cross-check only: the responder must not create resources
    // from this declaration.
    std::map<std::string, std::vector<ProbeBootstrapPeerPort>> declared_peers;
};

struct ProbeBootstrapResponse {
    uint32_t schema_version{kProbeBootstrapSchemaVersion};
    std::string reply_msg;
    std::string responder_segment;
    uint64_t node_incarnation{0};
    uint64_t topology_epoch{0};
    uint64_t publication_epoch{0};
    std::string transport_mode{kProbeBootstrapTransportMode};
    std::string device_kind{kProbeBootstrapDeviceKind};
    std::vector<ProbeBootstrapPort> ports;
};

// Field-level validation shared by the RPC codec and both endpoints. It only
// checks the wire contract; identity anchoring against registered segment
// metadata is the caller's responsibility.
Status validateProbeBootstrapRequest(const ProbeBootstrapRequest& request);
Status validateProbeBootstrapResponse(const ProbeBootstrapResponse& response);

// Publication <-> wire conversion shared by the responder and the coordinator.
ProbeBootstrapPort toProbeBootstrapPort(const ProbeLocalPortPublication& port);
ProbeLocalPortPublication fromProbeBootstrapPort(
    const ProbeBootstrapPort& port);

// JSON codec. A malformed body returns a Status instead of throwing.
Status encodeProbeBootstrapRequest(const ProbeBootstrapRequest& request,
                                   std::string& output);
Status decodeProbeBootstrapRequest(std::string_view input,
                                   ProbeBootstrapRequest& output);
Status encodeProbeBootstrapResponse(const ProbeBootstrapResponse& response,
                                    std::string& output);
Status decodeProbeBootstrapResponse(std::string_view input,
                                    ProbeBootstrapResponse& output);

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_BOOTSTRAP_H_
