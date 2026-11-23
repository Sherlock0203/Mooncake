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

#ifndef TENT_TRANSPORT_UB_URMA_ADAPTER_H_
#define TENT_TRANSPORT_UB_URMA_ADAPTER_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "tent/common/status.h"
// W3 D-12: PostAcceptance is defined once, in the shared completion kernel, so
// the gate and the adapter agree on the enum identity (interface 5). This is a
// leaf header: completion_record.h includes no adapter header, so there is no
// include cycle.
#include "tent/transport/ub/completion_record.h"

namespace mooncake {
namespace tent {
namespace ub {

// URMA does not expose a provider-independent device-kind attribute. Discovery
// therefore starts at UNKNOWN; only validated configuration or metadata may
// attach a different kind through DeviceKindSource.
enum class DeviceKind : uint8_t {
    UNKNOWN = 0,
    PHYSICAL,
    BONDING,
    VIRTUAL,
    AGGREGATE,
};

enum class DeviceKindSource : uint8_t {
    NONE = 0,
    EXPLICIT_CONFIG,
    VALIDATED_METADATA,
};

inline std::string_view deviceKindName(DeviceKind kind) {
    switch (kind) {
        case DeviceKind::PHYSICAL:
            return "physical";
        case DeviceKind::BONDING:
            return "bonding";
        case DeviceKind::VIRTUAL:
            return "virtual";
        case DeviceKind::AGGREGATE:
            return "aggregate";
        case DeviceKind::UNKNOWN:
            return "unknown";
    }
    return "unknown";
}

inline std::string_view deviceKindSourceName(DeviceKindSource source) {
    switch (source) {
        case DeviceKindSource::EXPLICIT_CONFIG:
            return "explicit_config";
        case DeviceKindSource::VALIDATED_METADATA:
            return "validated_metadata";
        case DeviceKindSource::NONE:
            return "none";
    }
    return "none";
}

// Adapter-neutral constants mirroring the public UMDK ABI. Native builds
// static_assert these values against the UMDK headers.
inline constexpr uint16_t kUrmaTransportModeRm = 0x1;
inline constexpr uint16_t kUrmaTransportModeRc = 0x1 << 1;
inline constexpr uint16_t kUrmaTransportModeUm = 0x1 << 2;
inline constexpr uint16_t kUrmaTransportModeMask =
    kUrmaTransportModeRm | kUrmaTransportModeRc | kUrmaTransportModeUm;
inline constexpr uint8_t kUrmaMaxPriority = 15;
inline constexpr size_t kUrmaPriorityCount = 16;
inline constexpr uint8_t kUrmaTargetTypeJetty = 1;
inline constexpr uint8_t kUrmaTpTypeRtp = 0;
inline constexpr uint8_t kUrmaTpTypeCtp = 1;
inline constexpr uint32_t kUrmaTpTypeRtpMask = 1U << kUrmaTpTypeRtp;
inline constexpr uint32_t kUrmaTpTypeCtpMask = 1U << kUrmaTpTypeCtp;
// Default receive-side JFR token shared by the RC and RM import paths.
inline constexpr uint32_t kUrmaJfrDefaultToken = 0xACFE;

// Adapter-neutral subset of the capabilities needed by the TENT UB data
// plane. Values of zero mean that the provider did not report a limit.
struct DeviceCapabilities {
    uint32_t max_jfc = 0;
    uint32_t max_jfc_depth = 0;
    uint32_t max_jfr_depth = 0;
    uint32_t max_jetty = 0;
    uint32_t max_jetty_depth = 0;
    uint32_t max_send_sge = 0;
    uint32_t max_remote_sge = 0;
    uint64_t max_message_size = 0;
    uint32_t max_read_size = 0;
    uint32_t max_write_size = 0;
    uint32_t feature_flags = 0;
    uint16_t transport_modes = 0;
    uint32_t rm_tp_cap = 0;
    std::array<uint32_t, kUrmaPriorityCount> priority_tp_type_masks{};
};

// One DeviceInfo represents one native URMA device/EID pair. A native device
// with multiple EIDs therefore produces multiple entries. topology_name is a
// stable TENT-facing identity and native_device_name is passed only to URMA.
struct DeviceInfo {
    std::string topology_name;
    std::string native_device_name;
    std::string native_device_path;
    uint32_t eid_index = 0;
    std::string eid;
    bool active = false;
    DeviceKind kind = DeviceKind::UNKNOWN;
    DeviceKindSource kind_source = DeviceKindSource::NONE;
    DeviceCapabilities capabilities;
};

// LocalSendProfile is derived only from trusted local provider capabilities.
// bind_tp_type_mask is never accepted as a peer-supplied JFS configuration.
struct LocalSendProfile {
    uint16_t trans_mode = kUrmaTransportModeRm;
    uint8_t priority = kUrmaMaxPriority;
    uint32_t bind_tp_type_mask = 0;
    DeviceCapabilities local_capabilities;
};

inline Status buildLocalSendProfile(const DeviceInfo& device, int64_t priority,
                                    LocalSendProfile& output) {
    const auto& capabilities = device.capabilities;
    if (priority < 0 || priority > kUrmaMaxPriority) {
        return Status::InvalidArgument("URMA JFS priority must be in [0, 15]");
    }
    if ((capabilities.transport_modes &
         static_cast<uint16_t>(~kUrmaTransportModeMask)) != 0 ||
        (capabilities.transport_modes & kUrmaTransportModeRm) == 0) {
        return Status::InvalidArgument(
            "URMA device transport_modes must be valid and include RM");
    }
    // RM over CTR: rm_tp_cap is advisory here  because some providers leave it
    // zero while still exposing CPT in the per-priority table.
    // the per-priority bind mask is the authoritative local signal.
    uint8_t selected_priority = static_cast<uint8_t>(priority);
    uint32_t bind_tp_type_mask =
        capabilities
            .priority_tp_type_masks[static_cast<size_t>(selected_priority)];
    if ((bind_tp_type_mask & kUrmaTpTypeCtpMask) == 0) {
        bool ctp_priority_found = false;
        for (uint8_t candidate = 0; candidate < kUrmaPriorityCount;
             ++candidate) {
            const uint32_t candidate_mask =
                capabilities.priority_tp_type_masks[candidate];
            if ((candidate_mask & kUrmaTpTypeCtpMask) != 0) {
                selected_priority = candidate;
                bind_tp_type_mask = candidate_mask;
                ctp_priority_found = true;
                break;
            }
        }
        if (!ctp_priority_found) {
            return Status::InvalidArgument(
                "URMA JFS priority does not support the remote CTP target");
        }
    }

    LocalSendProfile profile;
    profile.priority = selected_priority;
    profile.bind_tp_type_mask = bind_tp_type_mask;
    profile.local_capabilities = capabilities;
    output = profile;
    return Status::OK();
}

// The segment descriptor is intentionally an explicit envelope. Raw
// urma_seg_t bytes are ABI-specific; import must reject a different API
// version, structure size, schema, or malformed hex instead of copying an
// arbitrary byte sequence into a native structure.
struct SegmentDescriptor {
    static constexpr uint32_t kSchemaVersion = 1;

    uint32_t schema_version = kSchemaVersion;
    uint32_t urma_api_version = 0;
    uint32_t urma_abi_size = 0;
    std::string hex;
};

// Native handles never escape through this interface. Concrete adapters own
// their raw handles and make explicit release operations retryable. All typed
// handles are reference counted so slices can retain segments until their
// completion has been consumed.
class OpaqueHandle {
   public:
    OpaqueHandle() = default;
    virtual ~OpaqueHandle() = default;

    OpaqueHandle(const OpaqueHandle&) = delete;
    OpaqueHandle& operator=(const OpaqueHandle&) = delete;

    [[nodiscard]] virtual bool valid() const noexcept = 0;
};

class Context : public OpaqueHandle {
   public:
    [[nodiscard]] virtual const DeviceInfo& deviceInfo() const noexcept = 0;
    [[nodiscard]] virtual int asyncFd() const noexcept = 0;
};

class Jfc : public OpaqueHandle {
   public:
    // Returns -1 when completion events were not requested.
    [[nodiscard]] virtual int eventFd() const noexcept = 0;
};

class LocalSegment : public OpaqueHandle {
   public:
    [[nodiscard]] virtual uint64_t address() const noexcept = 0;
    [[nodiscard]] virtual uint64_t length() const noexcept = 0;
    [[nodiscard]] virtual const SegmentDescriptor& descriptor()
        const noexcept = 0;
};

class RemoteSegment : public OpaqueHandle {
   public:
    [[nodiscard]] virtual uint64_t address() const noexcept = 0;
    [[nodiscard]] virtual uint64_t length() const noexcept = 0;
    [[nodiscard]] virtual const SegmentDescriptor& descriptor()
        const noexcept = 0;
};

class Jetty : public OpaqueHandle {
   public:
    [[nodiscard]] virtual uint32_t id() const noexcept = 0;
    [[nodiscard]] virtual uint32_t uasid() const noexcept = 0;
};

// RM Legacy imported remote target. One handle represents one peer Jetty
// imported into a local context; it is referenced explicitly by each RM WR
// and is never bound to a local Jetty. Ownership follows the endpoint
// incarnation: the owner may release the handle only after every WR that
// referenced it has completed or a provider fence proved it is unreferenced.
class RemoteTarget : public OpaqueHandle {
   public:
    [[nodiscard]] virtual uint32_t id() const noexcept = 0;
    [[nodiscard]] virtual uint32_t uasid() const noexcept = 0;
    [[nodiscard]] virtual const std::string& eid() const noexcept = 0;
};

using ContextPtr = std::shared_ptr<Context>;
using JfcPtr = std::shared_ptr<Jfc>;
using LocalSegmentPtr = std::shared_ptr<LocalSegment>;
using RemoteSegmentPtr = std::shared_ptr<RemoteSegment>;
using JettyPtr = std::shared_ptr<Jetty>;
using RemoteTargetPtr = std::shared_ptr<RemoteTarget>;

enum SegmentAccess : uint32_t {
    SEGMENT_ACCESS_READ = 1U << 0,
    SEGMENT_ACCESS_WRITE = 1U << 1,
    SEGMENT_ACCESS_ATOMIC = 1U << 2,
    // Local CPU/URMA access remains unrestricted, but the provider must reject
    // every access initiated by a remote endpoint.
    SEGMENT_ACCESS_LOCAL_ONLY = 1U << 3,
};

struct SegmentOptions {
    uint32_t access = SEGMENT_ACCESS_READ | SEGMENT_ACCESS_WRITE;
    uint32_t token = 0xACFE;
    bool cacheable = false;
};

// RC Jetties are bound once and post through their binding. RM Jetties stay
// unbound and carry a remote target on every WR (see the per-WR post overload
// below). Business traffic keeps the RC default; probe traffic uses RM.
enum class JettyTransportMode : uint8_t {
    kRc,
    kRm,
};

struct JfcOptions {
    uint32_t depth = 4096;
    uint32_t receiver_depth = 2048;
    uint32_t token = kUrmaJfrDefaultToken;
    bool enable_completion_events = false;
    // UMDK requires the JFS and its shared JFR to use the same transport mode.
    // Probe JFCs therefore create an RM JFR even though they only send.
    JettyTransportMode transport_mode{JettyTransportMode::kRc};
    // Experimental send-only mode; not used by the probe path because UMDK
    // currently validates JFS/JFR mode through the shared JFR.
    bool send_only = false;
    // RM Legacy: one of the two RM spellings normalized by
    // isReliableMessage(). Never read directly to build the JFR. Defaults to
    // RC so the RC legacy baseline is byte-for-byte unchanged.
    uint16_t trans_mode = kUrmaTransportModeRc;
};

struct JettyOptions {
    uint32_t depth = 256;
    uint32_t priority = kUrmaMaxPriority;
    uint8_t max_sge = 1;
    uint8_t rnr_retry = 7;
    uint8_t error_timeout = 17;
    JettyTransportMode transport_mode{JettyTransportMode::kRc};
    // RM Legacy: selects reliable-message semantics through
    // isReliableMessage() and drives the CTP bind mask. Both are derived from
    // the trusted LocalSendProfile at install time and are never taken from
    // peer-supplied bootstrap fields. Defaults keep the RC legacy creation path
    // unchanged.
    uint16_t trans_mode = kUrmaTransportModeRc;
    uint32_t bind_tp_type_mask = 0;
};

// The options above carry the same "is this RM?" concept in two spellings.
// transport_mode is the adapter-level spelling used by the probe path and by
// rm_pool; trans_mode is the native URMA value used by RM Legacy. A caller is
// allowed to set only one of them, so every native decision (JFR/JFS mode,
// min_rnr_timer, post gate) must be derived from this single predicate instead
// of reading either field directly.
[[nodiscard]] inline bool isReliableMessage(
    const JfcOptions& options) noexcept {
    return options.transport_mode == JettyTransportMode::kRm ||
           options.trans_mode == kUrmaTransportModeRm;
}

[[nodiscard]] inline bool isReliableMessage(
    const JettyOptions& options) noexcept {
    return options.transport_mode == JettyTransportMode::kRm ||
           options.trans_mode == kUrmaTransportModeRm;
}

// Native URMA transport mode selected by the normalized predicate. The
// adapter-neutral constants are pinned to the UMDK enum by static_asserts in
// urma_adapter.cpp.
[[nodiscard]] inline uint16_t nativeTransportMode(
    bool reliable_message) noexcept {
    return reliable_message ? kUrmaTransportModeRm : kUrmaTransportModeRc;
}

// A native RM Jetty carries an explicit imported target on every WR, so a
// freshly created Jetty is postable without a bind. A native RC Jetty must
// complete a bind first. A Jetty in ERROR/RESET is never postable. Kept here
// so the post gate and its unit tests share one definition.
[[nodiscard]] inline bool isJettyPostable(bool reliable_message, bool created,
                                          bool bound) noexcept {
    return reliable_message ? (created || bound) : bound;
}

struct RemoteJettyInfo {
    std::string eid;
    uint32_t id = 0;
    uint32_t uasid = 0;
    uint32_t token = 0xACFE;
};

enum class Operation : uint8_t {
    READ,
    WRITE,
};

// W3 tristate acceptance for one post attempt (D-12, upstream §6). The single
// normalization point: the posting side may roll back only on a *definite*
// zero acceptance, and must keep every owner alive otherwise.
//
//   posted_count == request_count  -> Accepted
//       The WR crossed the native boundary. A status that turns non-OK
//       afterwards does not undo the acceptance (§9.1), so it still waits for
//       a CQE or a fence.
//   !status.ok() && posted_count == 0 -> NotAccepted
//       Definite zero acceptance: the only case the post side may roll back
//       (and the only rollback that may hand the member straight back).
//   everything else -> Unknown
//       Contradictory provider result (success with a short count, partial
//       acceptance, more accepted WRs than submitted). Never replay, never
//       roll back: drain until a CQE or the timeout fence settles it.
//
// F5: what the provider actually proved about the post. `posted_count` stays
// the honest "WRs that crossed the native boundary" number (segment ownership
// depends on it), so the acceptance decision needs its own evidence channel:
// an error without a failing-WR position does NOT prove zero acceptance.
enum class PostEvidence : uint8_t {
    // The provider gave a definite answer: success, or a bad_wr position.
    kProviderReported = 0,
    // The call failed but the provider did not say where it stopped. The
    // accepted prefix is unknown, so the outcome must be fail-closed.
    kNoFailingWr = 1,
};

struct PostOutcome {
    Status status;
    // WRs that crossed the native boundary. Only for segment accounting.
    size_t posted_count = 0;
    PostEvidence evidence = PostEvidence::kProviderReported;
    size_t request_count = 0;
};

// INV-POST-1: kNotAccepted requires a provider-reported failure position with
// posted_count == 0. Missing evidence is never "definitely not accepted".
[[nodiscard]] inline PostAcceptance classifyPostAcceptance(
    const PostOutcome& outcome) noexcept {
    if (outcome.evidence == PostEvidence::kNoFailingWr) {
        return PostAcceptance::kUnknown;
    }
    if (outcome.posted_count == outcome.request_count) {
        // Includes the empty batch: nothing was requested and nothing was
        // posted, so there is nothing to wait for. (`!ok` with an empty batch
        // also lands here; the caller owns no WR either way.)
        return PostAcceptance::kAccepted;
    }
    if (!outcome.status.ok() && outcome.posted_count == 0) {
        return PostAcceptance::kNotAccepted;
    }
    return PostAcceptance::kUnknown;
}

// Compatibility overload for callers that only have the legacy triple: an
// explicit count is treated as provider-reported (the pre-F5 contract).
[[nodiscard]] inline PostAcceptance classifyPostAcceptance(
    const Status& post_status, size_t posted_count,
    size_t request_count) noexcept {
    PostOutcome outcome;
    outcome.status = post_status;
    outcome.posted_count = posted_count;
    outcome.request_count = request_count;
    outcome.evidence = PostEvidence::kProviderReported;
    return classifyPostAcceptance(outcome);
}

struct WorkRequest {
    Operation operation = Operation::READ;
    uint64_t local_address = 0;
    uint64_t remote_address = 0;
    size_t length = 0;

    // Zero is reserved for native completion records that do not correspond
    // to a posted work request (for example a synthetic flush-done record).
    uint64_t token = 0;

    LocalSegmentPtr local_segment;
    RemoteSegmentPtr remote_segment;

    // RM Legacy: the explicit imported target for this WR. RM posts must set
    // it; RC posts must leave it null. The adapter never substitutes a bound
    // remote Jetty for a missing RM target.
    RemoteTargetPtr target;
};

enum class CompletionCategory : uint8_t {
    SUCCESS,
    LOCAL_DEVICE_ERROR,
    REMOTE_PATH_ERROR,
    ENDPOINT_ERROR,
    MEMORY_ERROR,
    // A link-down can surface as a local access error. Kept separate from
    // MEMORY_ERROR so the business path treats it as retryable.
    LOCAL_ACCESS_ERROR,
    TIMEOUT,
    UNKNOWN_ERROR,
};

inline const char* completionCategoryName(CompletionCategory category) {
    switch (category) {
        case CompletionCategory::SUCCESS:
            return "SUCCESS";
        case CompletionCategory::LOCAL_DEVICE_ERROR:
            return "LOCAL_DEVICE_ERROR";
        case CompletionCategory::REMOTE_PATH_ERROR:
            return "REMOTE_PATH_ERROR";
        case CompletionCategory::ENDPOINT_ERROR:
            return "ENDPOINT_ERROR";
        case CompletionCategory::MEMORY_ERROR:
            return "MEMORY_ERROR";
        case CompletionCategory::LOCAL_ACCESS_ERROR:
            return "LOCAL_ACCESS_ERROR";
        case CompletionCategory::TIMEOUT:
            return "TIMEOUT";
        case CompletionCategory::UNKNOWN_ERROR:
            return "UNKNOWN_ERROR";
    }
    return "UNKNOWN_ERROR";
}

struct Completion {
    CompletionCategory category = CompletionCategory::UNKNOWN_ERROR;
    int native_status = 0;
    uint64_t token = 0;
    uint32_t completed_bytes = 0;
    // Native Jetty ID that produced the completion. This is also populated
    // for entity-level flush markers whose token is deliberately zero.
    uint32_t local_jetty_id = 0;
};

// Classification of a provider entity marker (design §4.1/§6.1). A marker is a
// completion record that carries no work-request identity (the adapter forces
// token 0); only these shapes may reach the asynchronous drain chain, and only
// a flush-done marker for the current ERROR epoch may advance a drain.
enum class EntityMarkerKind : uint8_t {
    kNone = 0,
    kFlushDone = 1,    // URMA_CR_WR_FLUSH_ERR_DONE
    kSuspendDone = 2,  // URMA_CR_WR_SUSPEND_DONE
};

inline const char* entityMarkerKindName(EntityMarkerKind kind) noexcept {
    switch (kind) {
        case EntityMarkerKind::kNone:
            return "none";
        case EntityMarkerKind::kFlushDone:
            return "flush_done";
        case EntityMarkerKind::kSuspendDone:
            return "suspend_done";
    }
    return "none";
}

// Three-way native delete result (design §8.1). Only kConsumed proves that the
// provider reclaimed the handle, so only kConsumed may release budget and erase
// the tombstone.
struct DeleteOutcome {
    enum class Kind : uint8_t {
        kConsumed = 0,  // handle reclaimed (or provably already gone)
        kRetained = 1,  // provider says the handle is still valid: retry
        kUnknown = 2,   // undecidable: keep every owner, never re-dereference
    };
    Kind kind = Kind::kUnknown;
    Status status;
};

inline const char* deleteOutcomeKindName(DeleteOutcome::Kind kind) noexcept {
    switch (kind) {
        case DeleteOutcome::Kind::kConsumed:
            return "consumed";
        case DeleteOutcome::Kind::kRetained:
            return "retained";
        case DeleteOutcome::Kind::kUnknown:
            return "unknown";
    }
    return "unknown";
}

// Pure, injectable URMA boundary. It deliberately does not know about TENT
// Request, SubBatch, UbTask, UbSlice, scheduling, retry, or rail health.
//
// Thread-safety contract (RC-07): implementations must make the object
// lifecycle operations safe for concurrent callers operating on DISTINCT
// handles. The pooled send path calls createJetty/resetJetty/deleteJetty
// from posting threads, completion/polling threads, the background
// refill/delete executors, and the shutdown thread without external
// serialization; per-handle internal locking (as RealUrmaAdapter provides)
// is sufficient, but a replacement adapter must not assume single-threaded
// use.
class UrmaAdapter {
   public:
    virtual ~UrmaAdapter() = default;

    [[nodiscard]] virtual bool available() const noexcept = 0;
    [[nodiscard]] virtual uint32_t nativeApiVersion() const noexcept = 0;
    [[nodiscard]] virtual size_t nativeSegmentDescriptorSize()
        const noexcept = 0;

    virtual Status initialize() = 0;
    virtual Status shutdown() = 0;

    virtual Status discoverDevices(std::vector<DeviceInfo>& devices) = 0;

    virtual Status openContext(const DeviceInfo& device,
                               ContextPtr& context) = 0;
    // close/delete/unregister/unimport reset the caller's shared_ptr only after
    // the provider has released the native resource. If another owner still
    // retains the handle, or if the provider returns busy/error, the operation
    // fails and leaves the shared_ptr intact for a later retry. Calling any of
    // these methods again with a null handle is successful.
    virtual Status closeContext(ContextPtr& context) = 0;

    virtual Status createJfc(const ContextPtr& context,
                             const JfcOptions& options, JfcPtr& jfc) = 0;
    virtual Status deleteJfc(JfcPtr& jfc) = 0;

    virtual Status registerLocalSegment(const ContextPtr& context,
                                        uint64_t address, size_t length,
                                        const SegmentOptions& options,
                                        LocalSegmentPtr& segment) = 0;
    virtual Status unregisterLocalSegment(LocalSegmentPtr& segment) = 0;

    virtual Status importRemoteSegment(const ContextPtr& context,
                                       const SegmentDescriptor& descriptor,
                                       const SegmentOptions& options,
                                       RemoteSegmentPtr& segment) = 0;
    virtual Status unimportRemoteSegment(RemoteSegmentPtr& segment) = 0;

    virtual Status createJetty(const ContextPtr& context, const JfcPtr& jfc,
                               const JettyOptions& options,
                               JettyPtr& jetty) = 0;
    virtual Status deleteJetty(JettyPtr& jetty) = 0;
    // RC-only connection operations. RM Legacy never calls bindJetty or
    // unbindJetty: every RM WR carries its own explicit imported target.
    virtual Status bindJetty(const JettyPtr& jetty,
                             const RemoteJettyInfo& remote) = 0;
    virtual Status unbindJetty(const JettyPtr& jetty) = 0;
    virtual Status resetJetty(const JettyPtr& jetty) = 0;

    // RM Legacy remote target import. The descriptor must be a validated
    // bootstrap v2 target (RM/JETTY/RTP profile); the adapter maps it onto
    // the native import call and never derives the target from a local bind.
    // unimportRemoteTarget follows the same retryable shared_ptr contract as
    // the other release operations and must only be called after every WR
    // referencing the target has completed or been fenced.
    virtual Status importRemoteTarget(const ContextPtr& context,
                                      const std::string& eid, uint32_t uasid,
                                      uint32_t id, uint16_t trans_mode,
                                      uint8_t target_type, uint8_t tp_type,
                                      RemoteTargetPtr& target) {
        (void)context;
        (void)eid;
        (void)uasid;
        (void)id;
        (void)trans_mode;
        (void)target_type;
        (void)tp_type;
        (void)target;
        return Status::NotImplemented(
            "RM remote target import is not supported by this adapter");
    }
    virtual Status unimportRemoteTarget(RemoteTargetPtr& target) {
        if (!target) return Status::OK();
        return Status::NotImplemented(
            "RM remote target unimport is not supported by this adapter");
    }

    // Synchronously fences one Jetty. A successful return guarantees that no
    // previously posted WR on this Jetty can perform any further DMA. The
    // implementation transitions the native Jetty to ERROR, consumes the
    // provider's flush-done marker, and returns both already-processed and
    // unhandled WR completions. Callers must dispatch every non-zero token
    // through their normal completion path before resetting or deleting the
    // Jetty. On failure no drain guarantee is made and native resources must
    // remain alive.
    //
    // This blocking form is the *legacy* fallback. The pooled error hot path
    // must not use it (source design §1.4): it holds the JFC poll lock across a
    // provider wait and freezes every other completion on the same JFC. It
    // stays for shutdown, Context-fatal recovery and probe-Jetty teardown,
    // which are the only branches the design permits.
    virtual Status quiesceJetty(const JettyPtr& jetty, uint32_t timeout_ms,
                                std::vector<Completion>& completions) = 0;

    // --- Asynchronous drain primitives (design §4.1) -----------------------
    // These are the fine-grained steps of quiesceJetty(), exposed so the pooled
    // retirement chain can drive them from its control executor instead of one
    // blocking call on the fault path. Every default implementation is
    // fail-closed: an adapter that does not override them makes the controller
    // quarantine the member rather than delete on unproven evidence.

    // Moves the Jetty into ERROR. Idempotent: an already-ERROR Jetty returns
    // OK. Entering a new epoch discards the stale flush-done record for this
    // native id. The implementation takes the Jetty's own mutex, which is also
    // held by post(), so a successful return proves that no post is in flight.
    virtual Status modifyJettyError(const JettyPtr& jetty,
                                    uint64_t error_epoch) {
        (void)jetty;
        (void)error_epoch;
        return Status::NotImplemented(
            "asynchronous Jetty drain is not supported by this adapter");
    }

    // Classifies a token==0 completion as a provider entity marker. kNone means
    // "not a marker" (an async event or an unknown record); the poller must
    // keep routing it through its legacy unknown-token path.
    virtual EntityMarkerKind classifyEntityMarker(
        const Completion& completion) const {
        (void)completion;
        return EntityMarkerKind::kNone;
    }

    // Runs one bounded round of urma_flush_jetty(): returns the WR completions
    // that were accepted by the provider but never completed. OK with an empty
    // `out` means the software SQ is empty (the only proof of that condition).
    // A token==0 entity marker in the returned batch is an internal error, the
    // same judgment quiesceJetty makes.
    virtual Status flushJettyOnce(const JettyPtr& jetty,
                                  std::vector<Completion>& out) {
        (void)jetty;
        out.clear();
        return Status::NotImplemented(
            "asynchronous Jetty drain is not supported by this adapter");
    }

    // Drain teardown: clears the flush-done record, releases every segment
    // retained for this Jetty and marks the Jetty flush-fenced. Called once,
    // after the software SQ is proven empty.
    virtual Status finishDrain(const JettyPtr& jetty) {
        (void)jetty;
        return Status::NotImplemented(
            "asynchronous Jetty drain is not supported by this adapter");
    }

    // Classified native delete (design §8.1). The default derives the
    // classification from the legacy delete contract: any error leaves the
    // owner in place and is reported as Unknown (fail-closed). Adapters that
    // can prove "handle already gone" or "provider says busy" override this.
    virtual DeleteOutcome deleteJettyWithOutcome(JettyPtr& jetty) {
        DeleteOutcome outcome;
        outcome.status = deleteJetty(jetty);
        outcome.kind = outcome.status.ok() ? DeleteOutcome::Kind::kConsumed
                                           : DeleteOutcome::Kind::kUnknown;
        return outcome;
    }

    // On a native post error, posted_count is the number of leading requests
    // accepted before the provider's bad WR. Requests counted as posted must
    // still be completed through poll().
    virtual Status post(const JettyPtr& jetty,
                        const std::vector<WorkRequest>& requests,
                        size_t& posted_count) = 0;

    // RM post: the Jetty must have been created with
    // JettyOptions::transport_mode == kRm; the target is imported and cached by
    // the adapter and installed as wr.tjetty for this WR only. The default
    // implementation rejects the call for adapters without RM support.
    virtual Status post(const JettyPtr& jetty, const RemoteJettyInfo& target,
                        const WorkRequest& request, size_t& posted_count) {
        (void)jetty;
        (void)target;
        (void)request;
        posted_count = 0;
        return Status::NotImplemented(
            "per-WR remote target is not supported by this adapter");
    }

    // F5: adapter boundary result including the evidence behind posted_count.
    // The default preserves the legacy contract for adapters (and fakes) that
    // only implement post(): their failure without a position is still
    // reported as provider-reported, exactly as before. Adapters with a native
    // bad_wr channel override this so a missing position becomes kNoFailingWr.
    virtual PostOutcome postDetailed(const JettyPtr& jetty,
                                     const std::vector<WorkRequest>& requests) {
        PostOutcome outcome;
        outcome.request_count = requests.size();
        outcome.status = post(jetty, requests, outcome.posted_count);
        return outcome;
    }

    virtual PostOutcome postDetailed(const JettyPtr& jetty,
                                     const RemoteJettyInfo& target,
                                     const WorkRequest& request) {
        PostOutcome outcome;
        outcome.request_count = 1;
        outcome.status = post(jetty, target, request, outcome.posted_count);
        return outcome;
    }

    // RM Legacy post with explicit acceptance classification. The default
    // derives it from postDetailed(), so every adapter that implements only
    // post() keeps its exact behavior.
    virtual Status post(const JettyPtr& jetty,
                        const std::vector<WorkRequest>& requests,
                        size_t& posted_count, PostAcceptance& acceptance) {
        const PostOutcome outcome = postDetailed(jetty, requests);
        posted_count = outcome.posted_count;
        acceptance = classifyPostAcceptance(outcome);
        return outcome.status;
    }

    // RM variant of the tristate post, used by the pooled data path when the
    // lease member is an RM Jetty: the WR must reference the endpoint's
    // imported target and the acceptance classification is identical.
    virtual Status post(const JettyPtr& jetty, const RemoteJettyInfo& target,
                        const WorkRequest& request, size_t& posted_count,
                        PostAcceptance& acceptance) {
        const PostOutcome outcome = postDetailed(jetty, target, request);
        posted_count = outcome.posted_count;
        const Status& status = outcome.status;
        if (status.IsNotImplemented()) {
            // The adapter has no RM path at all: nothing was posted, but this
            // is a hard configuration error, not a business rejection.
            // Classifying it as Unknown keeps the record off the rollback+retry
            // path and lets the fence settle it (rm_pool also rejects such a
            // device at install, review minor).
            posted_count = 0;
            acceptance = PostAcceptance::kUnknown;
            return status;
        }
        acceptance = classifyPostAcceptance(outcome);
        return status;
    }

    virtual Status poll(const JfcPtr& jfc, size_t max_completions,
                        std::vector<Completion>& completions) = 0;
};

// Returns the raw-liburma implementation when TENT_HAS_REAL_URMA is enabled;
// otherwise returns an injectable-compatible stub whose operational methods
// report Status::NotImplemented with an explicit unavailable message.
std::shared_ptr<UrmaAdapter> createDefaultUrmaAdapter();

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_URMA_ADAPTER_H_
