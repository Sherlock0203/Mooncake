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

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "tent/transport/ub/bootstrap.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/sha256.h"
#include "tent/transport/ub/slice.h"
#include "tent/transport/ub/urma_adapter.h"
#include "tent/transport/ub/workers.h"

namespace mooncake::tent::ub {
namespace {

TEST(UbParamsTest, ParsesStrictValuesAndAllowsRetryDisable) {
    Config config;
    config.set("transports/ub/max_retries", 0);
    config.set("transports/ub/worker_count", 2);
    config.set("transports/ub/device_filter",
               std::vector<std::string>{"ub:fake0:eid0"});
    UbParams params;
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_EQ(params.max_retries, 0U);
    EXPECT_EQ(params.worker_count, 2U);
    ASSERT_EQ(params.device_filter.size(), 1U);

    config.set("transports/ub/worker_count", -1);
    EXPECT_TRUE(UbParams::FromConfig(config, params).IsInvalidArgument());
}

// yuanhao-1113's idle-spin knobs, re-added after the conflict resolution kept
// tent_merge's params test file wholesale. Defaults must be on (performance
// baseline), and 0 must restore the pre-optimisation sleep/wait behaviour.
TEST(UbParamsTest, IdleSpinDefaultsOnAndAcceptsZeroToDisable) {
    Config config;
    UbParams params;
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_EQ(params.poll_idle_spin_us, 1000U);
    EXPECT_EQ(params.post_idle_spin_us, 1000U);

    config.set("transports/ub/poll_idle_spin_us", 0);
    config.set("transports/ub/post_idle_spin_us", 0);
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_EQ(params.poll_idle_spin_us, 0U);
    EXPECT_EQ(params.post_idle_spin_us, 0U);

    config.set("transports/ub/poll_idle_spin_us", 1000001);
    EXPECT_TRUE(UbParams::FromConfig(config, params).IsInvalidArgument());
    config.set("transports/ub/poll_idle_spin_us", -1);
    EXPECT_TRUE(UbParams::FromConfig(config, params).IsInvalidArgument());
}

TEST(UbParamsTest, RuntimeDefaultsSelectRcLegacyWithPoolAndProbeOff) {
    Config config;
    UbParams params;
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_EQ(params.runtime_profile, UbRuntimeProfile::kRcLegacy);
    EXPECT_EQ(params.trans_mode, UbTransportMode::kRc);
    EXPECT_FALSE(params.jetty_pool.enable);
    EXPECT_FALSE(params.jetty_pool.force_disable);
    EXPECT_FALSE(params.jetty_reuse.enable);
    EXPECT_FALSE(params.probe.enabled);
    EXPECT_TRUE(params.probe.stop_on_recovery);

    UbRuntimeMode mode = UbRuntimeMode::kPooledRm;
    ASSERT_TRUE(resolveUbRuntimeMode(params, mode).ok());
    EXPECT_EQ(mode, UbRuntimeMode::kRcLegacy);
}

TEST(UbParamsTest, ParsesDormantPoolAndProbeKeysWithoutStartingResources) {
    Config config;
    config.set("transports/ub/runtime_profile", "rm_pool");
    config.set("transports/ub/trans_mode", "rm");
    config.set("transports/ub/jetty_pool/enable", true);
    config.set("transports/ub/probe/enabled", true);
    config.set("transports/ub/probe/interval_ms", 300);
    config.set("transports/ub/probe/timeout_ms", 200);
    config.set("transports/ub/probe/backoff_max_ms", 400);
    config.set("transports/ub/probe/peer_budget", 2);
    config.set("transports/ub/probe/recover_success_threshold", 1);
    config.set("transports/ub/probe/failure_threshold", 4);
    config.set("transports/ub/probe/stop_on_recovery", true);

    UbParams params;
    const Status parsed = UbParams::FromConfig(config, params);
    ASSERT_TRUE(parsed.ok()) << parsed.ToString();
    EXPECT_EQ(params.runtime_profile, UbRuntimeProfile::kRmPool);
    EXPECT_EQ(params.trans_mode, UbTransportMode::kRm);
    EXPECT_TRUE(params.jetty_pool.enable);
    EXPECT_FALSE(params.jetty_pool.force_disable);
    EXPECT_TRUE(params.probe.enabled);
    EXPECT_EQ(params.probe.interval_ms, 300U);
    EXPECT_EQ(params.probe.timeout_ms, 200U);
    EXPECT_EQ(params.probe.backoff_max_ms, 400U);
    EXPECT_EQ(params.probe.peer_budget, 2U);
    EXPECT_EQ(params.probe.recover_success_threshold, 1U);
    EXPECT_EQ(params.probe.failure_threshold, 4U);

    // The schema is accepted; force_disable is the runtime brake and the
    // selection fails closed instead of overriding the operator.
    params.jetty_pool.force_disable = true;
    UbRuntimeMode mode = UbRuntimeMode::kRcLegacy;
    EXPECT_TRUE(resolveUbRuntimeMode(params, mode).IsInvalidArgument());
}

TEST(UbParamsTest, RuntimeMatrixRejectsInconsistentOrUnsupportedModes) {
    auto evaluate = [](UbRuntimeProfile profile, UbTransportMode mode,
                       bool pool, bool probe, bool physical_only) {
        UbParams params;
        params.runtime_profile = profile;
        params.trans_mode = mode;
        params.jetty_pool.enable = pool;
        params.probe.enabled = probe;
        params.physical_only = physical_only;
        UbRuntimeMode resolved = UbRuntimeMode::kRcLegacy;
        return resolveUbRuntimeMode(params, resolved);
    };

    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRcLegacy, UbTransportMode::kRm,
                         false, false, false)
                    .IsInvalidArgument());
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRcLegacy, UbTransportMode::kRc,
                         true, false, false)
                    .IsInvalidArgument());
    // Probing is an independent decision: it does not select the runtime
    // mode and is valid on the RC baseline (v1.7 standalone probing).
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRcLegacy, UbTransportMode::kRc,
                         false, true, false)
                    .ok());

    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmLegacy, UbTransportMode::kRc,
                         false, false, true)
                    .IsInvalidArgument());
    // A well-formed RM Legacy configuration passes the config gate; the
    // provider/capability gates run during install and fail closed there.
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmLegacy, UbTransportMode::kRm,
                         false, false, true)
                    .ok());
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmLegacy, UbTransportMode::kRm,
                         true, false, true)
                    .IsInvalidArgument());
    // Probing stays independent of the RM profile as well.
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmLegacy, UbTransportMode::kRm,
                         false, true, true)
                    .ok());
    // RM Legacy is physical-only: no bonding device may be selected.
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmLegacy, UbTransportMode::kRm,
                         false, false, false)
                    .IsInvalidArgument());

    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmPool, UbTransportMode::kRm, false,
                         true, true)
                    .IsInvalidArgument());
    // rm_pool does not require probing to be enabled (M-3): the pooled data
    // path only needs the pool switch and the RM transport.
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmPool, UbTransportMode::kRm, true,
                         false, true)
                    .ok());
    // A well-formed pooled configuration passes the config gate; install-time
    // capability/budget gates still fail closed before native resources.
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmPool, UbTransportMode::kRm, true,
                         true, true)
                    .ok());
    // rm_pool is physical-only: no bonding device may be selected.
    EXPECT_TRUE(evaluate(UbRuntimeProfile::kRmPool, UbTransportMode::kRm, true,
                         true, false)
                    .IsInvalidArgument());
}

TEST(UbParamsTest, RmLegacyResolvesToRmRuntimeMode) {
    UbParams params;
    params.runtime_profile = UbRuntimeProfile::kRmLegacy;
    params.trans_mode = UbTransportMode::kRm;
    params.physical_only = true;
    UbRuntimeMode resolved = UbRuntimeMode::kRcLegacy;
    ASSERT_TRUE(resolveUbRuntimeMode(params, resolved).ok());
    EXPECT_EQ(resolved, UbRuntimeMode::kRmLegacy);
}

TEST(UbParamsTest, RequireSameEidScopeDefaultsOnAndRoundTrips) {
    UbParams params;
    Config config;
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_TRUE(params.require_same_eid_scope)
        << "cross-scope pairing must be refused by default";

    config.set("transports/ub/require_same_eid_scope", false);
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_FALSE(params.require_same_eid_scope);

    config.set("transports/ub/require_same_eid_scope", true);
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_TRUE(params.require_same_eid_scope);
}

TEST(UbParamsTest, JettyReuseRequiresRmLegacyProfile) {
    auto evaluate = [](UbRuntimeProfile profile, UbRuntimeMode& resolved) {
        UbParams params;
        params.runtime_profile = profile;
        params.trans_mode = profile == UbRuntimeProfile::kRcLegacy
                                ? UbTransportMode::kRc
                                : UbTransportMode::kRm;
        params.jetty_reuse.enable = true;
        // rm_pool is otherwise well-formed (pool+probe on) so the rejection
        // can only come from the jetty_reuse gate itself.
        params.jetty_pool.enable = profile == UbRuntimeProfile::kRmPool;
        params.probe.enabled = profile == UbRuntimeProfile::kRmPool;
        params.physical_only = profile == UbRuntimeProfile::kRmLegacy;
        return resolveUbRuntimeMode(params, resolved);
    };

    UbRuntimeMode resolved = UbRuntimeMode::kRcLegacy;
    EXPECT_TRUE(
        evaluate(UbRuntimeProfile::kRcLegacy, resolved).IsInvalidArgument());
    EXPECT_TRUE(
        evaluate(UbRuntimeProfile::kRmPool, resolved).IsInvalidArgument());
    ASSERT_TRUE(evaluate(UbRuntimeProfile::kRmLegacy, resolved).ok());
    EXPECT_EQ(resolved, UbRuntimeMode::kRmLegacy);

    // The config key must round-trip through FromConfig.
    Config config;
    config.set("transports/ub/jetty_reuse/enable", true);
    UbParams params;
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_TRUE(params.jetty_reuse.enable);
}

TEST(UbAdapterContractTest, ClassifiesPostAcceptanceFromNativeResult) {
    // Full success: Accepted.
    EXPECT_EQ(classifyPostAcceptance(Status::OK(), 1, 1),
              PostAcceptance::kAccepted);
    // Deterministic rejection before any WR crossed: NotAccepted.
    EXPECT_EQ(classifyPostAcceptance(Status::InvalidArgument("rejected"), 0, 1),
              PostAcceptance::kNotAccepted);
    // Accepted-prefix error: the WR may have crossed, so Unknown.
    // A fully accepted count stays Accepted even when the call reports an
    // error afterwards: the WR already crossed, so it is not a rollback case.
    EXPECT_EQ(classifyPostAcceptance(Status::RdmaError("late"), 1, 1),
              PostAcceptance::kAccepted);
    // Success with a short count is contradictory: Unknown.
    EXPECT_EQ(classifyPostAcceptance(Status::OK(), 0, 1),
              PostAcceptance::kUnknown);
    // Empty post succeeds trivially: Accepted.
    EXPECT_EQ(classifyPostAcceptance(Status::OK(), 0, 0),
              PostAcceptance::kAccepted);
    EXPECT_STREQ(postAcceptanceName(PostAcceptance::kAccepted), "accepted");
    EXPECT_STREQ(postAcceptanceName(PostAcceptance::kNotAccepted),
                 "not_accepted");
    EXPECT_STREQ(postAcceptanceName(PostAcceptance::kUnknown), "unknown");
}

TEST(UbAdapterContractTest, MissingFailingWrIsUnknownNotNotAccepted) {
    // INV-POST-1: an error without a provider-reported position proves
    // nothing about acceptance, so it must never authorize a rollback.
    PostOutcome no_position;
    no_position.status = Status::RdmaError("post failed, no bad_wr");
    no_position.posted_count = 0;
    no_position.request_count = 1;
    no_position.evidence = PostEvidence::kNoFailingWr;
    EXPECT_EQ(classifyPostAcceptance(no_position), PostAcceptance::kUnknown);

    // The same triple with provider-reported evidence is a definite zero
    // acceptance and may roll back.
    PostOutcome reported = no_position;
    reported.evidence = PostEvidence::kProviderReported;
    EXPECT_EQ(classifyPostAcceptance(reported), PostAcceptance::kNotAccepted);

    // A partial prefix is always Unknown, regardless of evidence.
    PostOutcome partial;
    partial.status = Status::RdmaError("third WR rejected");
    partial.posted_count = 1;
    partial.request_count = 3;
    EXPECT_EQ(classifyPostAcceptance(partial), PostAcceptance::kUnknown);

    // Success with a short count stays Unknown (a provider contradiction).
    PostOutcome short_success;
    short_success.status = Status::OK();
    short_success.posted_count = 0;
    short_success.request_count = 1;
    EXPECT_EQ(classifyPostAcceptance(short_success), PostAcceptance::kUnknown);
}

TEST(UbBootstrapTest, BuildsCanonicalPhysicalDeviceIdentity) {
    EXPECT_EQ(
        makePhysicalDeviceIdentity("/dev/uburma/udma0", "0001:0002:0000:0000"),
        "/dev/uburma/udma0#0001");
    EXPECT_EQ(makePhysicalDeviceIdentity("/dev/uburma/udma1", "0002"),
              "/dev/uburma/udma1#0002");
    EXPECT_EQ(makePhysicalDeviceIdentity("", ""), "#");
}

TEST(UbParamsTest, RejectsUnknownRuntimeKeysAndNonFixedProbeRecovery) {
    Config config;
    UbParams params;

    config.set("transports/ub/runtime_profile", "rm");
    EXPECT_TRUE(UbParams::FromConfig(config, params).IsInvalidArgument());

    config.set("transports/ub/runtime_profile", "rm_pool");
    config.set("transports/ub/trans_mode", "uc");
    EXPECT_TRUE(UbParams::FromConfig(config, params).IsInvalidArgument());

    config.set("transports/ub/trans_mode", "rm");
    config.set("transports/ub/probe/stop_on_recovery", false);
    EXPECT_TRUE(UbParams::FromConfig(config, params).IsInvalidArgument());
}

UbBootstrapDesc makeValidRmBootstrap() {
    UbBootstrapDesc desc;
    desc.protocol_version = 2;
    desc.segment_name = "segment-a";
    desc.local_nic_path = "segment-a/ub:udma0:eid0";
    desc.peer_nic_path = "segment-b/ub:udma1:eid0";
    desc.local_device_name = "udma0";
    desc.local_device_id = 0;
    desc.local_eid_index = 0;
    desc.local_eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    desc.endpoint_generation = 6;
    desc.segment_generation = 5;

    UbRmTargetDescriptor target;
    target.target_identity = "P0";
    target.eid = desc.local_eid;
    target.uasid = 0;
    target.id = 10;
    target.target_epoch = 4;
    target.profile.trans_mode = kUrmaTransportModeRm;
    target.profile.target_type = kUrmaTargetTypeJetty;
    target.profile.tp_type = kUrmaTpTypeCtp;

    UbRmBootstrapDesc rm;
    rm.transport_mode = "RM";
    rm.device_kind = "PHYSICAL";
    rm.physical_device_identity = "/dev/uburma/udma0#0001";
    // 父代际变小 -> kStale
    rm.node_incarnation = 2;
    rm.context_incarnation = 2;
    rm.topology_epoch = 3;
    rm.transport_modes = 3;
    rm.provider_capabilities = {"read", "write", "rm"};
    rm.targets = {target};
    rm.probe_target_map = {{"local-P0", {"P0"}}};
    desc.rm = std::move(rm);
    return desc;
}

TEST(UbBootstrapTest, GatesVersionAndModeWithoutClaimingRmRuntimeSupport) {
    UbBootstrapDesc legacy;
    legacy.protocol_version = 1;
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RC,
                                       UbBootstrapProtocol::kAuto, legacy)
                    .ok());
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RM,
                                       UbBootstrapProtocol::kAuto, legacy)
                    .IsInvalidArgument());

    const auto rm = makeValidRmBootstrap();
    EXPECT_TRUE(validateUbBootstrap(rm).ok());
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RM,
                                       UbBootstrapProtocol::kAuto, rm)
                    .ok());
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RC,
                                       UbBootstrapProtocol::kAuto, rm)
                    .IsNotImplemented());
    auto response_envelope = rm;
    response_envelope.reply_msg = "response is not a request";
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RM,
                                       UbBootstrapProtocol::kAuto,
                                       response_envelope)
                    .IsInvalidArgument());
}

TEST(UbBootstrapTest, ExplicitProtocolSelectorForcesOneVersion) {
    UbBootstrapDesc legacy;
    legacy.protocol_version = 1;
    const auto rm = makeValidRmBootstrap();

    // Forced v1: a v1 peer is accepted by the RC runtime and rejected by RM,
    // while a v2 peer is rejected outright.
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RC,
                                       UbBootstrapProtocol::kV1, legacy)
                    .ok());
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RM,
                                       UbBootstrapProtocol::kV1, legacy)
                    .IsNotImplemented());
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RC,
                                       UbBootstrapProtocol::kV1, rm)
                    .IsInvalidArgument());

    // Forced v2: a v1 peer is rejected even on the RC runtime, and an RM peer
    // passes.
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RC,
                                       UbBootstrapProtocol::kV2, legacy)
                    .IsInvalidArgument());
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RM,
                                       UbBootstrapProtocol::kV2, rm)
                    .ok());
}

TEST(UbBootstrapTest, V1PeerIgnoresAndFlagsAV2Payload) {
    auto desc = makeValidRmBootstrap();
    nlohmann::json wire = desc;
    // Simulate a peer that claims v1 but still sends a v2 block.
    wire["protocol_version"] = 1;
    const auto decoded = wire.get<UbBootstrapDesc>();
    EXPECT_EQ(decoded.protocol_version, 1U);
    // The block is dropped, never interpreted.
    EXPECT_FALSE(decoded.rm.has_value());
    EXPECT_TRUE(decoded.ignored_v2_payload);

    // A plain v1 envelope is not flagged.
    nlohmann::json plain = {{"protocol_version", 1}};
    const auto clean = plain.get<UbBootstrapDesc>();
    EXPECT_FALSE(clean.ignored_v2_payload);
}

TEST(UbBootstrapTest, UnknownTopLevelKeysAreIgnoredButBadTypesFailClosed) {
    auto desc = makeValidRmBootstrap();
    nlohmann::json wire = desc;
    // An unknown top-level key is forward-compatible: it is ignored and the
    // descriptor still validates.
    wire["future_extension"] = {{"anything", 1}};
    const auto decoded = wire.get<UbBootstrapDesc>();
    EXPECT_TRUE(validateUbBootstrap(decoded).ok());

    // A known key with the wrong type is a wire error, never a default.
    nlohmann::json bad_generation = wire;
    bad_generation["endpoint_generation"] = "not-a-number";
    EXPECT_THROW(bad_generation.get<UbBootstrapDesc>(), std::invalid_argument);

    nlohmann::json bad_targets = wire;
    bad_targets["rm"]["targets"] = 7;
    EXPECT_THROW(bad_targets.get<UbBootstrapDesc>(),
                 nlohmann::json::type_error);
}

TEST(UbBootstrapTest, RmWireFormatParsesStrictlyAndRejectsUnknownFields) {
    const auto desc = makeValidRmBootstrap();
    ASSERT_TRUE(desc.rm.has_value());

    // Round-trip: a valid v2 request survives encode/decode with the RM
    // payload intact.
    nlohmann::json wire = desc;
    UbBootstrapDesc decoded = wire.get<UbBootstrapDesc>();
    EXPECT_EQ(decoded.protocol_version, 2U);
    ASSERT_TRUE(decoded.rm.has_value());
    EXPECT_EQ(decoded.rm->physical_device_identity,
              desc.rm->physical_device_identity);
    ASSERT_EQ(decoded.rm->targets.size(), 1U);
    EXPECT_EQ(decoded.rm->targets[0].id, desc.rm->targets[0].id);
    EXPECT_EQ(decoded.rm->targets[0].profile.trans_mode,
              desc.rm->targets[0].profile.trans_mode);

    // A remote target profile is a closed allowlist: unknown extensions and
    // local-only fields are never silently reinterpreted.
    {
        auto profile = wire["rm"]["targets"][0]["profile"];
        profile["bind_tp_type_mask"] = 4;
        EXPECT_THROW(profile.get<UbRemoteTargetProfile>(),
                     std::invalid_argument);
    }
    {
        auto profile = wire["rm"]["targets"][0]["profile"];
        profile["trans_mode"] = -1;
        EXPECT_THROW(profile.get<UbRemoteTargetProfile>(),
                     std::invalid_argument);
    }
    // Required RM fields may not be dropped from the wire.
    {
        auto rm = wire["rm"];
        rm.erase("targets");
        EXPECT_THROW(rm.get<UbRmBootstrapDesc>(), nlohmann::json::out_of_range);
    }
    // v2 wire bytes stay immutable: v1-only fields never leak into v2.
    EXPECT_EQ(wire.count("jetty_ids"), 0U);

    // A v2 rejection response carries only the version and the reply.
    UbBootstrapDesc rejection;
    rejection.protocol_version = 2;
    rejection.reply_msg = "peer rejected the request";
    nlohmann::json rejection_wire = rejection;
    EXPECT_EQ(rejection_wire.size(), 2U);
    EXPECT_TRUE(rejection_wire.contains("protocol_version"));
    EXPECT_TRUE(rejection_wire.contains("reply_msg"));
    const auto decoded_rejection = rejection_wire.get<UbBootstrapDesc>();
    EXPECT_EQ(decoded_rejection.protocol_version, 2U);
    EXPECT_FALSE(decoded_rejection.rm.has_value());
    EXPECT_EQ(decoded_rejection.reply_msg, "peer rejected the request");

    // An empty v2 envelope without a payload is not a valid rejection.
    UbBootstrapDesc empty;
    empty.protocol_version = 2;
    EXPECT_THROW(nlohmann::json{empty}, std::invalid_argument);
}

TEST(UbBootstrapTest, ValidatesRmIdentityProfilesAndProbeReferences) {
    auto desc = makeValidRmBootstrap();
    desc.rm->targets[0].uasid = 0;
    EXPECT_TRUE(validateUbBootstrap(desc).ok());
    desc.rm->targets[0].uasid = 17;
    EXPECT_TRUE(validateUbBootstrap(desc).ok());

    UbBootstrapDesc error_envelope;
    error_envelope.protocol_version = 2;
    error_envelope.reply_msg = "error-only envelope is not a valid request";
    EXPECT_TRUE(validateUbBootstrap(error_envelope).IsInvalidArgument());

    desc = makeValidRmBootstrap();
    desc.rm->transport_mode = "RC";
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->device_kind = "UNKNOWN";
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->transport_modes = 6;  // RC|UM, deliberately without RM.
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.local_eid = "not-an-eid";
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->node_incarnation = 0;
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->provider_capabilities = {"read"};
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->transport_modes = 0;
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->transport_modes = 8;
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->transport_modes = 0x8000;
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->targets[0].profile.trans_mode = kUrmaTransportModeRc;
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->targets[0].profile.trans_mode = 3;
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->targets[0].profile.target_type = 0;  // URMA_JFR is out of scope.
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->targets[0].profile.tp_type =
        kUrmaTpTypeRtp;  // RM binds CTP targets only.
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->targets[0].id = 0;
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->targets.push_back(desc.rm->targets[0]);
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
    desc = makeValidRmBootstrap();
    desc.rm->probe_target_map["local-P0"] = {"missing-target"};
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());
}

TEST(UbBootstrapTest, CrossChecksPeerIdentityAgainstTrustedMetadata) {
    const auto desc = makeValidRmBootstrap();
    UbExpectedPeerIdentity expected;
    expected.segment_name = desc.segment_name;
    expected.local_nic_path = desc.local_nic_path;
    expected.physical_device_identity = desc.rm->physical_device_identity;
    expected.native_device_name = desc.local_device_name;
    expected.device_id = desc.local_device_id;
    expected.eid_index = desc.local_eid_index;
    expected.eid = desc.local_eid;
    expected.transport_modes = desc.rm->transport_modes;
    EXPECT_TRUE(validateUbBootstrapV2PeerIdentity(desc, expected).ok());
    auto response_envelope = desc;
    response_envelope.reply_msg = "response is not a request";
    EXPECT_TRUE(validateUbBootstrapV2PeerIdentity(response_envelope, expected)
                    .IsInvalidArgument());
    auto disguised_v1 = desc;
    disguised_v1.protocol_version = 1;
    EXPECT_TRUE(validateUbBootstrapV2PeerIdentity(disguised_v1, expected)
                    .IsInvalidArgument());

    auto mismatched = expected;
    mismatched.transport_modes ^= 1;
    EXPECT_TRUE(validateUbBootstrapV2PeerIdentity(desc, mismatched)
                    .IsInvalidArgument());
    mismatched = expected;
    mismatched.eid = "0002:0000:0000:0000:0000:0000:0000:0000";
    EXPECT_TRUE(validateUbBootstrapV2PeerIdentity(desc, mismatched)
                    .IsInvalidArgument());
}

TEST(UbBootstrapTest, ComparesGenerationsWithoutOverwritingNewerSnapshots) {
    const auto current = makeValidRmBootstrap();
    auto candidate = current;
    UbBootstrapGenerationComparison comparison;
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kDuplicate);

    candidate.reply_msg = "duplicate responses may differ only in diagnostics";
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kDuplicate);

    candidate = current;
    candidate.rm->node_incarnation--;
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kStale);

    candidate = current;
    candidate.rm->context_incarnation++;
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kNewer);

    candidate = current;
    candidate.rm->targets[0].target_epoch--;
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kStale);

    candidate = current;
    candidate.rm->targets[0].target_epoch++;
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kNewer);

    candidate = current;
    candidate.rm->targets[0].target_epoch--;
    candidate.endpoint_generation++;
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    // Leaf generation is compared before the target set: a newer endpoint
    // generation wins even when one incumbent target appears older.
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kNewer);

    // D1 regression: a re-created endpoint has a new endpoint_generation and
    // a completely new Jetty id range. The old order looked up the new target
    // identities first and returned kConflict before reading the leaf.
    candidate = current;
    candidate.endpoint_generation++;
    candidate.rm->probe_target_map.clear();
    for (auto& target : candidate.rm->targets) {
        target.id += 100;
        target.target_identity += "-new";
        target.target_epoch = candidate.endpoint_generation;
    }
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kNewer);

    candidate = current;
    candidate.rm->physical_device_identity = "different-device";
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kNewer);

    candidate = current;
    candidate.rm->targets[0].eid = "0002:0000:0000:0000:0000:0000:0000:0000";
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kConflict);

    candidate = current;
    candidate.rm->targets[0].target_identity = "different-target";
    candidate.rm->probe_target_map["local-P0"] = {"different-target"};
    ASSERT_TRUE(compareUbBootstrapV2(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kConflict);
}

TEST(UbSha256Test, MatchesFips180v4GoldenVectors) {
    EXPECT_EQ(sha256Hex(""),
              "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    EXPECT_EQ(sha256Hex("abc"),
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(
        sha256Hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
    // One million 'a' bytes exercises multi-block hashing plus the
    // two-block padding edge (remainder >= 56).
    EXPECT_EQ(sha256Hex(std::string(1000000, 'a')),
              "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
    EXPECT_TRUE(isCanonicalSha256Hex(sha256Hex("x")));
    EXPECT_FALSE(isCanonicalSha256Hex("ABC"));
    EXPECT_FALSE(isCanonicalSha256Hex(std::string(63, '0')));
    EXPECT_FALSE(isCanonicalSha256Hex(std::string(65, '0')));
}

// Builds a structurally valid pooled v3 descriptor: one port, one shared
// target, one probe region and one adjacency entry per configured port, with
// a self-consistent inventory checksum.
UbBootstrapDesc makeValidPooledBootstrap(size_t port_count = 4) {
    UbBootstrapDesc desc;
    desc.protocol_version = 3;
    desc.segment_name = "segment-a";
    desc.local_nic_path = "segment-a/ub:udma0:eid0";
    desc.peer_nic_path = "segment-b/ub:udma1:eid0";
    desc.local_port_identity = "P0";
    desc.local_device_identity = "/dev/uburma/udma0#0001";
    desc.local_eid_index = 0;
    desc.local_eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    desc.endpoint_generation = 6;
    desc.segment_generation = 5;

    UbPooledBootstrapDesc pooled;
    pooled.schema_version = 3;
    pooled.transport_mode = "RM";
    pooled.device_kind = "PHYSICAL";
    pooled.physical_device_identity = "/dev/uburma/udma0#0001";
    pooled.node_incarnation = 2;
    pooled.context_incarnation = 2;
    pooled.topology_epoch = 3;
    pooled.transport_modes = 3;
    pooled.provider_capabilities = {"read", "write", "rm"};
    pooled.descriptor_abi = "urma-rjetty-v1";
    pooled.access_capabilities.read = true;
    pooled.access_capabilities.write = true;
    pooled.access_capabilities.mr_offset = 0;
    pooled.access_capabilities.mr_length = 1ULL << 30;

    for (size_t i = 0; i < port_count; ++i) {
        const std::string index = std::to_string(i);
        UbPooledPortDesc port;
        port.port_id = "P" + index;
        port.context_id = 100 + i;
        port.eid = desc.local_eid;
        port.eid_index = static_cast<uint32_t>(i);
        pooled.ports.push_back(port);

        UbPooledTargetDescriptor target;
        target.port_id = port.port_id;
        target.target_identity = "T" + index;
        target.eid = desc.local_eid;
        target.uasid = 0;
        target.id = 10 + i;
        target.target_epoch = 4;
        target.profile.trans_mode = kUrmaTransportModeRm;
        target.profile.target_type = kUrmaTargetTypeJetty;
        target.profile.tp_type = kUrmaTpTypeCtp;
        target.descriptor = "00aabbcc";
        pooled.targets.push_back(target);

        pooled.probe_target_map.emplace(
            port.port_id, std::vector<std::string>{"T" + index});

        UbPooledProbeRegionDesc region;
        region.peer = "peer-b";
        region.local_port = port.port_id;
        region.target_identity = target.target_identity;
        region.target_epoch = target.target_epoch;
        region.region_generation = 1;
        region.access = "WRITE";
        region.offset = 4096;
        region.length = 65536;
        region.slots = 4;
        region.stride = 64;
        pooled.probe_regions.push_back(region);

        pooled.adjacency.emplace(port.port_id,
                                 std::vector<std::string>{"R0", "R1"});
    }
    desc.pooled = std::move(pooled);
    desc.pooled->inventory_checksum = computePooledInventoryChecksum(desc);
    return desc;
}

TEST(UbBootstrapTest, PooledV3WireFormatRoundTripsStrictly) {
    const auto desc = makeValidPooledBootstrap();
    ASSERT_TRUE(desc.pooled.has_value());

    nlohmann::json wire = desc;
    UbBootstrapDesc decoded = wire.get<UbBootstrapDesc>();
    EXPECT_EQ(decoded.protocol_version, 3U);
    ASSERT_TRUE(decoded.pooled.has_value());
    EXPECT_EQ(decoded.pooled->schema_version, 3U);
    ASSERT_EQ(decoded.pooled->ports.size(), 4U);
    EXPECT_EQ(decoded.pooled->ports[0].port_id, "P0");
    ASSERT_EQ(decoded.pooled->targets.size(), 4U);
    EXPECT_EQ(decoded.pooled->targets[1].target_identity, "T1");
    EXPECT_EQ(decoded.pooled->targets[0].descriptor, "00aabbcc");
    EXPECT_TRUE(validateUbBootstrap(decoded).ok());

    // The v3 envelope is a closed allowlist: legacy v1/v2 fields never leak
    // into a pooled descriptor.
    EXPECT_EQ(wire.count("jetty_ids"), 0U);
    EXPECT_EQ(wire.count("local_device_name"), 0U);
    auto legacy_field = wire;
    legacy_field["jetty_ids"] = std::vector<uint32_t>{1};
    EXPECT_THROW(legacy_field.get<UbBootstrapDesc>(), std::invalid_argument);
    auto v2_payload = wire;
    v2_payload["rm"] = nlohmann::json(makeValidRmBootstrap().rm.value());
    EXPECT_THROW(v2_payload.get<UbBootstrapDesc>(), std::invalid_argument);

    // Unknown fields are rejected at every nesting level.
    auto unknown_nested = wire;
    unknown_nested["pooled"]["future_field"] = 1;
    EXPECT_THROW(unknown_nested.get<UbBootstrapDesc>(), std::invalid_argument);
    auto unknown_port = wire;
    unknown_port["pooled"]["ports"][0]["bind_tp_type_mask"] = 4;
    EXPECT_THROW(unknown_port.get<UbBootstrapDesc>(), std::invalid_argument);
    auto unknown_target = wire;
    unknown_target["pooled"]["targets"][0]["priority"] = 2;
    EXPECT_THROW(unknown_target.get<UbBootstrapDesc>(), std::invalid_argument);
    auto unknown_region = wire;
    unknown_region["pooled"]["probe_regions"][0]["read"] = true;
    EXPECT_THROW(unknown_region.get<UbBootstrapDesc>(),
                 std::invalid_argument);

    // Required nested fields may not be dropped.
    auto missing = wire;
    missing["pooled"].erase("inventory_checksum");
    EXPECT_THROW(missing.get<UbBootstrapDesc>(), nlohmann::json::out_of_range);
    auto missing_envelope = wire;
    missing_envelope.erase("local_port_identity");
    EXPECT_THROW(missing_envelope.get<UbBootstrapDesc>(),
                 nlohmann::json::out_of_range);

    // Booleans must be real booleans; numbers must be integers.
    auto bad_bool = wire;
    bad_bool["pooled"]["access_capabilities"]["read"] = 1;
    EXPECT_THROW(bad_bool.get<UbBootstrapDesc>(), std::invalid_argument);
    auto bad_number = wire;
    bad_number["pooled"]["ports"][0]["context_id"] = -1;
    EXPECT_THROW(bad_number.get<UbBootstrapDesc>(), std::invalid_argument);

    // A v3 rejection response carries only the version and the reply.
    UbBootstrapDesc rejection;
    rejection.protocol_version = 3;
    rejection.reply_msg = "peer rejected the pooled request";
    nlohmann::json rejection_wire = rejection;
    EXPECT_EQ(rejection_wire.size(), 2U);
    const auto decoded_rejection = rejection_wire.get<UbBootstrapDesc>();
    EXPECT_EQ(decoded_rejection.protocol_version, 3U);
    EXPECT_FALSE(decoded_rejection.pooled.has_value());
    EXPECT_EQ(decoded_rejection.reply_msg, "peer rejected the pooled request");

    // An empty v3 envelope without payload or reply is not valid.
    UbBootstrapDesc empty;
    empty.protocol_version = 3;
    EXPECT_THROW(nlohmann::json{empty}, std::invalid_argument);
}

TEST(UbBootstrapTest, PooledV3RejectsDuplicateJsonKeysBeforeDom) {
    const auto desc = makeValidPooledBootstrap();
    const std::string clean = nlohmann::json(desc).dump();
    UbBootstrapDesc decoded;
    ASSERT_TRUE(decodeUbBootstrapV3Strict(clean, decoded).ok());
    EXPECT_EQ(decoded.pooled->ports.size(), 4U);

    // A duplicated top-level key fails closed before the DOM parser could
    // silently keep the last occurrence.
    std::string duplicated = clean;
    duplicated.insert(1, "\"segment_name\":\"evil\",");
    EXPECT_TRUE(
        decodeUbBootstrapV3Strict(duplicated, decoded).IsInvalidArgument());

    // Duplicates inside nested objects (a pooled port) are rejected too.
    std::string nested = clean;
    const auto ports_pos = nested.find("\"ports\"");
    ASSERT_NE(ports_pos, std::string::npos);
    const auto port_brace = nested.find('{', nested.find('[', ports_pos));
    ASSERT_NE(port_brace, std::string::npos);
    nested.insert(port_brace + 1, "\"port_id\":\"evil\",");
    EXPECT_TRUE(
        decodeUbBootstrapV3Strict(nested, decoded).IsInvalidArgument());

    // Malformed JSON fails closed.
    EXPECT_TRUE(decodeUbBootstrapV3Strict("{", decoded).IsInvalidArgument());

    // The strict decoder only accepts v3; a valid v2 request is rejected
    // instead of being reinterpreted.
    const auto v2 = makeValidRmBootstrap();
    EXPECT_TRUE(decodeUbBootstrapV3Strict(nlohmann::json(v2).dump(), decoded)
                    .IsInvalidArgument());
}

TEST(UbBootstrapTest, ValidatesPooledInventoryAndTargetTopology) {
    auto desc = makeValidPooledBootstrap();
    EXPECT_TRUE(validateUbBootstrap(desc).ok());

    // Helper: mutate the descriptor, re-sign it, then validate, so each
    // failure below is attributed to the mutated rule and not to a stale
    // checksum.
    const auto revalidate = [](UbBootstrapDesc& mutated) {
        mutated.pooled->inventory_checksum =
            computePooledInventoryChecksum(mutated);
        return validateUbBootstrap(mutated);
    };

    // A checksum that does not cover the actual content is rejected.
    desc = makeValidPooledBootstrap();
    desc.pooled->inventory_checksum = std::string(64, '0');
    EXPECT_TRUE(validateUbBootstrap(desc).IsInvalidArgument());

    // Every configured port must publish exactly one target.
    desc = makeValidPooledBootstrap();
    desc.pooled->targets.pop_back();
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->targets[0].port_id = "P1";
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->targets[0].port_id = "unknown-port";
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());

    // Port inventory: duplicate ids, zero Context mapping, bad EIDs.
    desc = makeValidPooledBootstrap();
    desc.pooled->ports[1].port_id = "P0";
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->ports[1].context_id = 0;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->ports[1].eid = "not-an-eid";
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->ports[1].context_id = desc.pooled->ports[0].context_id;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());

    // Envelope identity: empty port id, missing device identity, bad EID.
    desc = makeValidPooledBootstrap();
    desc.local_port_identity.clear();
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.local_device_identity.clear();
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.local_eid = "not-an-eid";
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());

    // Descriptors must be non-empty even-length hex.
    desc = makeValidPooledBootstrap();
    desc.pooled->targets[0].descriptor = "0";
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->targets[0].descriptor = "zz";
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());

    // Adjacency must reference published local ports, deduplicated.
    desc = makeValidPooledBootstrap();
    desc.pooled->adjacency["unknown-port"] = {"R0"};
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->adjacency["P0"] = {"R0", "R0"};
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());

    // Probe target map keys must be published ports.
    desc = makeValidPooledBootstrap();
    desc.pooled->probe_target_map["unknown-port"] = {"T0"};
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());

    // Probe regions: epoch binding, WRITE-only, range/slot overflow and
    // (peer, port) uniqueness.
    desc = makeValidPooledBootstrap();
    desc.pooled->probe_regions[0].target_epoch++;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->probe_regions[0].access = "READ";
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->probe_regions[0].offset =
        std::numeric_limits<uint64_t>::max();
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->probe_regions[0].slots =
        std::numeric_limits<uint64_t>::max();
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->probe_regions[1].peer = desc.pooled->probe_regions[0].peer;
    desc.pooled->probe_regions[1].local_port =
        desc.pooled->probe_regions[0].local_port;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());

    // Access capabilities: MR range and overflow.
    desc = makeValidPooledBootstrap();
    desc.pooled->access_capabilities.mr_length = 0;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->access_capabilities.mr_offset =
        std::numeric_limits<uint64_t>::max();
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->access_capabilities.read = false;
    desc.pooled->access_capabilities.write = false;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());

    // Schema version, ABI and generations.
    desc = makeValidPooledBootstrap();
    desc.pooled->schema_version = 2;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->descriptor_abi.clear();
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.pooled->topology_epoch = 0;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.endpoint_generation = 0;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
    desc = makeValidPooledBootstrap();
    desc.local_eid_index = -1;
    EXPECT_TRUE(revalidate(desc).IsInvalidArgument());
}

TEST(UbBootstrapTest, PooledV3GateClassifiesVersionsAndModes) {
    const auto pooled = makeValidPooledBootstrap();
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::kPooledRm,
                                       UbBootstrapProtocol::kAuto, pooled)
                    .ok());
    // A successful gate does not claim the pooled data plane exists.
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RM,
                                       UbBootstrapProtocol::kAuto, pooled)
                    .IsInvalidArgument());
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RC,
                                       UbBootstrapProtocol::kAuto, pooled)
                    .IsNotImplemented());

    // Requests carrying a reply envelope are responses, not requests.
    auto response = pooled;
    response.reply_msg = "response is not a request";
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::kPooledRm,
                                       UbBootstrapProtocol::kAuto, response)
                    .IsInvalidArgument());

    // Mixed versions are explicitly unsupported: v1/v2 never satisfy the
    // pooled gate and are not silently upgraded.
    const auto v2 = makeValidRmBootstrap();
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::kPooledRm,
                                       UbBootstrapProtocol::kAuto, v2)
                    .IsInvalidArgument());
    UbBootstrapDesc v1;
    v1.protocol_version = 1;
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::kPooledRm,
                                       UbBootstrapProtocol::kAuto, v1)
                    .IsInvalidArgument());
}

TEST(UbBootstrapTest,
     PooledV3CrossChecksPeerIdentityAgainstTrustedInventory) {
    const auto desc = makeValidPooledBootstrap();
    UbExpectedPooledPeerIdentity expected;
    expected.segment_name = desc.segment_name;
    expected.local_nic_path = desc.local_nic_path;
    expected.local_device_identity = desc.local_device_identity;
    expected.local_eid = desc.local_eid;
    expected.eid_index = desc.local_eid_index;
    expected.physical_device_identity = desc.pooled->physical_device_identity;
    expected.transport_modes = desc.pooled->transport_modes;
    expected.descriptor_abi = desc.pooled->descriptor_abi;
    expected.inventory_checksum = desc.pooled->inventory_checksum;
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(desc, expected).ok());

    // Responses are not requests.
    auto response = desc;
    response.reply_msg = "response is not a request";
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(response, expected)
                    .IsInvalidArgument());

    // A v2 descriptor never satisfies the v3 trusted anchor.
    const auto v2 = makeValidRmBootstrap();
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(v2, expected)
                    .IsInvalidArgument());

    // Every trusted-anchor field must match; the checksum and the descriptor
    // ABI are part of the identity, not advisory metadata.
    auto mismatch = expected;
    mismatch.inventory_checksum = std::string(64, '1');
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(desc, mismatch)
                    .IsInvalidArgument());
    mismatch = expected;
    mismatch.descriptor_abi = "urma-rjetty-v2";
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(desc, mismatch)
                    .IsInvalidArgument());
    mismatch = expected;
    mismatch.local_eid = "0002:0000:0000:0000:0000:0000:0000:0000";
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(desc, mismatch)
                    .IsInvalidArgument());
    mismatch = expected;
    mismatch.physical_device_identity = "/dev/uburma/udma9#0009";
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(desc, mismatch)
                    .IsInvalidArgument());
    mismatch = expected;
    mismatch.transport_modes ^= 1;
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(desc, mismatch)
                    .IsInvalidArgument());
    mismatch = expected;
    mismatch.local_device_identity = "/dev/uburma/udma9#0009";
    EXPECT_TRUE(validateUbBootstrapV3PeerIdentity(desc, mismatch)
                    .IsInvalidArgument());
}

TEST(UbBootstrapTest, ComparesPooledV3GenerationsWithoutOverwritingIncumbent) {
    const auto current = makeValidPooledBootstrap();
    // Candidate mutations must stay self-consistent (re-signed checksum) so
    // the comparison exercises generation logic, not validation failures.
    const auto resign = [](UbBootstrapDesc& mutated) {
        mutated.pooled->inventory_checksum =
            computePooledInventoryChecksum(mutated);
    };

    auto candidate = current;
    UbBootstrapGenerationComparison comparison;
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kDuplicate);

    // Diagnostic reply differences never change the duplicate verdict.
    candidate.reply_msg = "duplicate responses may differ in diagnostics";
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kDuplicate);

    candidate = current;
    candidate.pooled->node_incarnation--;
    resign(candidate);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kStale);

    candidate = current;
    candidate.pooled->context_incarnation++;
    resign(candidate);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kNewer);

    candidate = current;
    candidate.pooled->topology_epoch++;
    resign(candidate);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kNewer);

    // Per-port target epochs are leaves within the parent scope. Probe
    // regions stay bound to their target's epoch when it moves.
    candidate = current;
    candidate.pooled->targets[0].target_epoch--;
    candidate.pooled->probe_regions[0].target_epoch =
        candidate.pooled->targets[0].target_epoch;
    resign(candidate);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kStale);

    candidate = current;
    candidate.pooled->targets[1].target_epoch++;
    candidate.pooled->probe_regions[1].target_epoch =
        candidate.pooled->targets[1].target_epoch;
    resign(candidate);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kNewer);

    // A newer endpoint generation cannot hide an older target epoch.
    candidate = current;
    candidate.pooled->targets[0].target_epoch--;
    candidate.pooled->probe_regions[0].target_epoch =
        candidate.pooled->targets[0].target_epoch;
    candidate.endpoint_generation++;
    resign(candidate);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kConflict);

    // A different physical device identity is a different comparison scope.
    candidate = current;
    candidate.pooled->physical_device_identity = "/dev/uburma/udma9#0009";
    resign(candidate);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kConflict);

    // A different published port set under identical parents is a conflict.
    candidate = makeValidPooledBootstrap(6);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kConflict);

    // Same generations with different content (probe region resized and
    // re-signed) is a conflict; the healthy incumbent is preserved.
    candidate = current;
    candidate.pooled->probe_regions[0].slots = 8;
    resign(candidate);
    ASSERT_TRUE(compareUbBootstrapV3(candidate, current, comparison).ok());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kConflict);

    // Structurally invalid candidates are rejected outright: the incumbent
    // is never destroyed by an unparseable or tampered payload.
    candidate = current;
    candidate.pooled->targets.pop_back();
    resign(candidate);
    EXPECT_TRUE(compareUbBootstrapV3(candidate, current, comparison)
                    .IsInvalidArgument());
    EXPECT_EQ(comparison, UbBootstrapGenerationComparison::kConflict);
}

TEST(UbBootstrapTest, PooledV3SupportsFourSixPortAndAsymmetricTopologies) {
    // The DTO layer carries no hard-coded port count: 4-port and 6-port
    // inventories (and anything in between or beyond) validate with exactly
    // one shared target per configured port.
    for (size_t port_count : {2U, 4U, 6U, 9U}) {
        const auto desc = makeValidPooledBootstrap(port_count);
        ASSERT_EQ(desc.pooled->ports.size(), port_count);
        ASSERT_EQ(desc.pooled->targets.size(), port_count);
        EXPECT_TRUE(validateUbBootstrap(desc).ok())
            << "port_count=" << port_count;
    }

    // Asymmetric adjacency is legal (V5 5.2 item 5): the local side
    // publishes its own port count and its own view of remote ports; the
    // remote inventory is not required to match sizes.
    auto desc = makeValidPooledBootstrap(4);
    desc.pooled->adjacency["P0"] = {"R0", "R1", "R2"};
    desc.pooled->adjacency["P3"] = {"R5"};
    desc.pooled->adjacency.erase("P1");
    desc.pooled->inventory_checksum = computePooledInventoryChecksum(desc);
    EXPECT_TRUE(validateUbBootstrap(desc).ok());
    const auto decoded = nlohmann::json(desc).get<UbBootstrapDesc>();
    EXPECT_TRUE(validateUbBootstrap(decoded).ok());
    ASSERT_EQ(decoded.pooled->adjacency.at("P0").size(), 3U);
    EXPECT_EQ(decoded.pooled->adjacency.count("P1"), 0U);
}

Request makeRequest(size_t length) {
    Request request{};
    request.opcode = Request::WRITE;
    request.length = length;
    return request;
}

UbPostPath makePath(Topology::NicID local_id = 0, int remote_id = 1,
                    uint64_t generation = 1) {
    return UbPostPath{local_id, 42, remote_id, generation};
}

TEST(UbSliceTest, RetryRejectsStaleCompletionAndNotifiesTerminalOnce) {
    int callback_count = 0;
    TransferStatus callback_status{INITIAL, 0};
    auto task = UbTask::create(
        makeRequest(128),
        [&](const TransferStatus& status) {
            ++callback_count;
            callback_status = status;
        },
        1);
    auto slice = task->addSlice(UbSliceSpec{nullptr, 0, 128, 0, 1}, 2);
    ASSERT_NE(slice, nullptr);
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(slice->markQueued(3));

    auto first_attempt = slice->beginAttempt(makePath(0, 1, 10), 4);
    ASSERT_TRUE(first_attempt.has_value());
    auto first_completion = slice->completionToken(*first_attempt);
    ASSERT_TRUE(first_completion.has_value());
    ASSERT_TRUE(first_completion->markPosted(5));
    EXPECT_EQ(first_completion->resolve(TIMEOUT, 0, true, 6),
              UbAttemptResolution::kRetryScheduled);
    EXPECT_EQ(task->transferStatus().s, PENDING);
    EXPECT_EQ(slice->snapshot().retry_count, 1U);

    ASSERT_TRUE(slice->markQueued(7));
    auto second_attempt = slice->beginAttempt(makePath(0, 2, 11), 8);
    ASSERT_TRUE(second_attempt.has_value());
    auto second_completion = slice->completionToken(*second_attempt);
    ASSERT_TRUE(second_completion.has_value());
    ASSERT_TRUE(second_completion->markPosted(9));

    // A completion from the retired endpoint generation cannot resolve the
    // replacement attempt.
    EXPECT_EQ(first_completion->resolve(COMPLETED, 128, false, 10),
              UbAttemptResolution::kIgnored);
    EXPECT_EQ(second_completion->resolve(COMPLETED, 128, false, 11),
              UbAttemptResolution::kTerminal);
    EXPECT_EQ(second_completion->resolve(COMPLETED, 128, false, 12),
              UbAttemptResolution::kIgnored);

    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(callback_status.s, COMPLETED);
    EXPECT_EQ(callback_status.transferred_bytes, 128U);
    const auto task_snapshot = task->snapshot();
    EXPECT_EQ(task_snapshot.remaining_slices, 0U);
    EXPECT_NE(task_snapshot.terminal_ns, 0U);
}

TEST(UbSliceTest, CancellationStopsQueuedWorkAndDrainsPostedWork) {
    int callback_count = 0;
    TransferStatus callback_status{INITIAL, 0};
    auto task =
        UbTask::create(makeRequest(128), [&](const TransferStatus& status) {
            ++callback_count;
            callback_status = status;
        });
    auto posted = task->addSlice(UbSliceSpec{nullptr, 0, 64, 0, 1});
    auto queued = task->addSlice(UbSliceSpec{nullptr, 64, 64, 64, 1});
    ASSERT_NE(posted, nullptr);
    ASSERT_NE(queued, nullptr);
    ASSERT_TRUE(task->seal());

    ASSERT_TRUE(posted->markQueued(1));
    auto attempt = posted->beginAttempt(makePath(), 2);
    ASSERT_TRUE(attempt.has_value());
    auto completion = posted->completionToken(*attempt);
    ASSERT_TRUE(completion.has_value());
    ASSERT_TRUE(completion->markPosted(3));
    ASSERT_TRUE(queued->markQueued(4));

    EXPECT_EQ(task->requestCancellation(5), 1U);
    EXPECT_EQ(task->transferStatus().s, PENDING);
    EXPECT_EQ(posted->snapshot().state, UbSliceState::kPosted);
    EXPECT_EQ(queued->snapshot().state, UbSliceState::kCanceled);

    // Posted work is not fabricated as immediately canceled. Its real
    // completion drains first, then the task reaches its single terminal
    // state (CANCELED because another slice never reached the device).
    EXPECT_EQ(completion->resolve(COMPLETED, 64, false, 6),
              UbAttemptResolution::kTerminal);
    EXPECT_EQ(completion->resolve(FAILED, 0, false, 7),
              UbAttemptResolution::kIgnored);
    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(callback_status.s, CANCELED);
    EXPECT_EQ(callback_status.transferred_bytes, 64U);
}

TEST(UbSliceTest, ConcurrentDuplicateCompletionsChooseOneTerminalWinner) {
    std::atomic<int> callback_count{0};
    auto task = UbTask::create(makeRequest(64), [&](const TransferStatus&) {
        callback_count.fetch_add(1, std::memory_order_relaxed);
    });
    auto slice = task->addSlice(UbSliceSpec{nullptr, 0, 64, 0, 0});
    ASSERT_NE(slice, nullptr);
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(slice->markQueued());
    auto attempt = slice->beginAttempt(makePath());
    ASSERT_TRUE(attempt.has_value());
    auto completion = slice->completionToken(*attempt);
    ASSERT_TRUE(completion.has_value());
    ASSERT_TRUE(completion->markPosted());

    std::atomic<int> terminal_winners{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            if (completion->resolve(COMPLETED, 64, false) ==
                UbAttemptResolution::kTerminal) {
                terminal_winners.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(terminal_winners.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(callback_count.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    EXPECT_EQ(task->transferStatus().transferred_bytes, 64U);
}

TEST(UbQuotaTest, EnforcesBothLevelsAndReleasesIdempotently) {
    QuotaManager quota(/*default_device_limits=*/{100, 2},
                       /*default_path_limits=*/{60, 1});
    const auto first_path = makePath(0, 1, 1);
    const auto second_path = makePath(0, 2, 1);

    auto reservation = quota.tryAcquire(first_path, 60);
    ASSERT_TRUE(reservation.has_value());
    EXPECT_FALSE(quota.tryAcquire(first_path, 1).has_value());
    // The second path has room, but the physical-device byte cap is shared.
    EXPECT_FALSE(quota.tryAcquire(second_path, 50).has_value());

    auto copied_token = *reservation;
    copied_token.path = second_path;
    copied_token.bytes = 1;
    copied_token.wrs = 99;
    EXPECT_TRUE(quota.release(copied_token));
    EXPECT_FALSE(quota.release(*reservation));

    const auto device = quota.deviceStats(0);
    const auto path = quota.pathStats(first_path);
    const auto aggregate = quota.aggregateStats();
    EXPECT_EQ(device.usage, QuotaUsage{});
    EXPECT_EQ(path.usage, QuotaUsage{});
    EXPECT_EQ(aggregate.usage, QuotaUsage{});
    EXPECT_EQ(aggregate.active_reservations, 0U);
    EXPECT_EQ(aggregate.duplicate_release_attempts, 1U);
    EXPECT_TRUE(quota.tryAcquire(first_path, 60).has_value());
}

TEST(UbQuotaTest, EndpointGenerationsShareOnePhysicalRailLimit) {
    QuotaManager quota(/*default_device_limits=*/{200, 4},
                       /*default_path_limits=*/{60, 1});
    const auto old_generation = makePath(0, 1, 10);
    const auto replacement = makePath(0, 1, 11);

    auto old_reservation = quota.tryAcquire(old_generation, 60);
    ASSERT_TRUE(old_reservation.has_value());
    EXPECT_FALSE(quota.tryAcquire(replacement, 1).has_value());
    EXPECT_EQ(quota.pathStats(replacement).usage.inflight_bytes, 60u);
    ASSERT_EQ(quota.allPathStats().size(), 1u);
    // Diagnostics follow the newest observed endpoint incarnation even when
    // its acquisition is rejected by pressure left by the old generation.
    EXPECT_EQ(quota.allPathStats().front().path.endpoint_generation, 11u);

    EXPECT_TRUE(quota.release(*old_reservation));
    auto replacement_reservation = quota.tryAcquire(replacement, 60);
    ASSERT_TRUE(replacement_reservation.has_value());
    ASSERT_EQ(quota.allPathStats().size(), 1u);
    EXPECT_EQ(quota.allPathStats().front().path.endpoint_generation, 11u);
    EXPECT_TRUE(quota.release(*replacement_reservation));
}

TEST(UbQuotaTest, ScoresProjectedPressureAndFallsBackAtomically) {
    QuotaManager quota(/*default_device_limits=*/{200, 4},
                       /*default_path_limits=*/{100, 2});
    const auto preferred = makePath(0, 1, 1);
    const auto alternate = makePath(0, 2, 1);

    auto first = quota.tryAcquire(preferred, 50);
    ASSERT_TRUE(first.has_value());
    const auto preferred_pressure = quota.availability(preferred, 50);
    const auto alternate_pressure = quota.availability(alternate, 50);
    ASSERT_TRUE(preferred_pressure.can_acquire);
    ASSERT_TRUE(alternate_pressure.can_acquire);
    EXPECT_DOUBLE_EQ(preferred_pressure.normalized_inflight, 1.0);
    EXPECT_DOUBLE_EQ(alternate_pressure.normalized_inflight, 0.5);

    auto fill_preferred = quota.tryAcquire(preferred, 50);
    ASSERT_TRUE(fill_preferred.has_value());
    EXPECT_FALSE(quota.availability(preferred, 1).can_acquire);

    // Preflight selection can race with another worker. The ordered commit
    // must skip the now-full first rail and reserve the alternate rail while
    // holding one quota lock.
    auto selected = quota.tryAcquireFirst({preferred, alternate}, 50);
    ASSERT_TRUE(selected.has_value());
    EXPECT_EQ(selected->path, alternate);
    EXPECT_EQ(quota.pathStats(preferred).rejected_acquisitions, 1U);
    EXPECT_EQ(quota.aggregateStats().rejected_acquisitions, 0U);

    EXPECT_TRUE(quota.release(*first));
    EXPECT_TRUE(quota.release(*fill_preferred));
    EXPECT_TRUE(quota.release(*selected));
}

TEST(UbPathSelectionScoreTest, AppliesCapacityLocalityPressureAndBandwidth) {
    UbPathSelectionScore base;
    base.quota_available = true;
    base.topology_rank = 0;
    base.normalized_inflight = 0.5;
    base.normalized_quota_wrs = 0.5;
    base.path_inflight_wrs = 2;
    base.path_inflight_bytes = 128;
    base.has_bandwidth_sample = true;
    base.ewma_bandwidth_bytes_per_second = 100.0;

    auto saturated = base;
    saturated.quota_available = false;
    saturated.topology_rank = 0;
    saturated.normalized_inflight = 0.0;
    EXPECT_TRUE(betterUbPathScore(base, saturated));

    auto farther = base;
    farther.topology_rank = 1;
    farther.normalized_inflight = 0.0;
    EXPECT_TRUE(betterUbPathScore(base, farther));

    auto busier = base;
    busier.normalized_inflight = 0.75;
    EXPECT_TRUE(betterUbPathScore(base, busier));

    // W3 D-11: the tie-break is the per-path in-flight ledger, not the
    // endpoint counter.
    auto path_busier = base;
    path_busier.path_inflight_wrs = 3;
    EXPECT_TRUE(betterUbPathScore(base, path_busier));

    auto slower = base;
    slower.ewma_bandwidth_bytes_per_second = 50.0;
    EXPECT_TRUE(betterUbPathScore(base, slower));
}

TEST(UbRailMonitorTest, PausesOnErrorWindowAndRecoversAfterCooldown) {
    RailMonitor monitor(RailMonitorConfig{/*error_threshold=*/2,
                                          /*error_window_ns=*/100,
                                          /*cooldown_ns=*/50,
                                          /*ewma_alpha=*/0.5});
    const auto path = makePath();

    EXPECT_DOUBLE_EQ(monitor.aggregateBandwidth(1), -1.0);
    monitor.recordSuccess(path, 100, 10, 50);
    EXPECT_DOUBLE_EQ(monitor.aggregateBandwidth(50), 10'000'000'000.0);

    monitor.recordError(path, 100);
    monitor.recordTimeout(path, 110);
    EXPECT_FALSE(monitor.available(path, 159));
    EXPECT_DOUBLE_EQ(monitor.aggregateBandwidth(159), 0.0);

    EXPECT_TRUE(monitor.available(path, 160));
    const auto stats = monitor.stats(path, 160);
    EXPECT_FALSE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 0U);
    EXPECT_EQ(stats.completion_errors, 2U);
    EXPECT_EQ(stats.timeouts, 1U);
    EXPECT_EQ(stats.pauses, 1U);
    EXPECT_EQ(stats.recoveries, 1U);
    EXPECT_DOUBLE_EQ(monitor.aggregateBandwidth(160), 10'000'000'000.0);
}

TEST(UbRailMonitorTest, ErrorsOutsideWindowDoNotPauseRail) {
    RailMonitor monitor(RailMonitorConfig{/*error_threshold=*/2,
                                          /*error_window_ns=*/10,
                                          /*cooldown_ns=*/50,
                                          /*ewma_alpha=*/0.5});
    const auto path = makePath();
    monitor.recordError(path, 1);
    monitor.recordError(path, 11);
    EXPECT_TRUE(monitor.available(path, 11));
    EXPECT_EQ(monitor.stats(path, 11).errors_in_window, 1U);
}

TEST(UbRailMonitorTest, OutOfOrderErrorsUseLatestEventForCooldown) {
    RailMonitor monitor(RailMonitorConfig{/*error_threshold=*/2,
                                          /*error_window_ns=*/100,
                                          /*cooldown_ns=*/50,
                                          /*ewma_alpha=*/0.5});
    const auto path = makePath();

    monitor.recordError(path, 120);
    monitor.recordError(path, 100);

    auto stats = monitor.stats(path, 120);
    EXPECT_TRUE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 2U);
    EXPECT_EQ(stats.pause_started_ns, 120U);
    EXPECT_EQ(stats.cooldown_until_ns, 170U);
    EXPECT_FALSE(monitor.available(path, 169));
    EXPECT_TRUE(monitor.available(path, 170));

    stats = monitor.stats(path, 170);
    EXPECT_FALSE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 0U);
    EXPECT_EQ(stats.pauses, 1U);
    EXPECT_EQ(stats.recoveries, 1U);

    // Neither an older query nor an error from the recovered epoch may
    // rewind the rail or resurrect its completed pause.
    EXPECT_TRUE(monitor.available(path, 130));
    monitor.recordError(path, 110);
    stats = monitor.stats(path, 130);
    EXPECT_FALSE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 0U);
    EXPECT_EQ(stats.completion_errors, 3U);
    EXPECT_EQ(stats.pauses, 1U);
    EXPECT_EQ(stats.recoveries, 1U);
}

TEST(UbRailMonitorTest, OutOfOrderExpiredErrorDoesNotTriggerPause) {
    RailMonitor monitor(RailMonitorConfig{/*error_threshold=*/2,
                                          /*error_window_ns=*/50,
                                          /*cooldown_ns=*/25,
                                          /*ewma_alpha=*/0.5});
    const auto path = makePath();

    monitor.recordError(path, 200);
    monitor.recordError(path, 100);

    const auto stats = monitor.stats(path, 200);
    EXPECT_FALSE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 1U);
    EXPECT_EQ(stats.completion_errors, 2U);
    EXPECT_EQ(stats.last_error_ns, 200U);
    EXPECT_EQ(stats.pauses, 0U);
    EXPECT_EQ(stats.recoveries, 0U);
}

TEST(UbLocalSendProfileTest, DerivesRmRtpProfileFromTrustedCapabilities) {
    DeviceInfo device;
    device.capabilities.transport_modes =
        kUrmaTransportModeRm | kUrmaTransportModeRc;
    device.capabilities.rm_tp_cap = kUrmaTpTypeRtpMask;
    device.capabilities.priority_tp_type_masks[3] =
        kUrmaTpTypeRtpMask | (1U << 1);

    LocalSendProfile profile;
    ASSERT_TRUE(buildLocalSendProfile(device, 3, profile).ok());
    EXPECT_EQ(profile.trans_mode, kUrmaTransportModeRm);
    EXPECT_EQ(profile.priority, 3);
    EXPECT_EQ(profile.bind_tp_type_mask,
              device.capabilities.priority_tp_type_masks[3]);
    EXPECT_EQ(profile.local_capabilities.rm_tp_cap,
              device.capabilities.rm_tp_cap);
    device.capabilities.priority_tp_type_masks[0] = kUrmaTpTypeRtpMask;
    device.capabilities.priority_tp_type_masks[15] = kUrmaTpTypeRtpMask;
    EXPECT_TRUE(buildLocalSendProfile(device, 0, profile).ok());
    EXPECT_TRUE(buildLocalSendProfile(device, 15, profile).ok());
}

TEST(UbLocalSendProfileTest, RejectsInvalidPriorityAndRmRtpCapabilities) {
    DeviceInfo device;
    device.capabilities.transport_modes = kUrmaTransportModeRm;
    device.capabilities.rm_tp_cap = kUrmaTpTypeRtpMask;
    device.capabilities.priority_tp_type_masks[3] = kUrmaTpTypeRtpMask;

    LocalSendProfile profile;
    EXPECT_TRUE(buildLocalSendProfile(device, -1, profile).IsInvalidArgument());
    EXPECT_TRUE(buildLocalSendProfile(device, 16, profile).IsInvalidArgument());
    EXPECT_TRUE(
        buildLocalSendProfile(device, 256, profile).IsInvalidArgument());

    auto invalid = device;
    invalid.capabilities.transport_modes = 8;
    EXPECT_TRUE(buildLocalSendProfile(invalid, 3, profile).IsInvalidArgument());
    invalid = device;
    invalid.capabilities.transport_modes = kUrmaTransportModeRc;
    EXPECT_TRUE(buildLocalSendProfile(invalid, 3, profile).IsInvalidArgument());
    invalid = device;
    invalid.capabilities.rm_tp_cap = 0;
    EXPECT_TRUE(buildLocalSendProfile(invalid, 3, profile).IsInvalidArgument());
    invalid = device;
    // invalid.capabilities.priority_tp_type_masks[3] = 1U << 1;
    invalid.capabilities.priority_tp_type_masks.fill(0);
    EXPECT_TRUE(buildLocalSendProfile(invalid, 3, profile).IsInvalidArgument());
}

DeviceInfo makeDevice(std::string native_name, std::string topology_name = "") {
    DeviceInfo info;
    info.native_device_name = std::move(native_name);
    info.topology_name = topology_name.empty()
                             ? ("ub:" + info.native_device_name + ":eid0")
                             : std::move(topology_name);
    info.native_device_path = "/fake/" + info.native_device_name;
    info.eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    info.active = true;
    info.capabilities.transport_modes = 2;
    return info;
}

DeviceIdentity makeIdentity(const DeviceInfo& device, DeviceKind kind) {
    DeviceIdentity identity;
    identity.native_device_name = device.native_device_name;
    identity.native_device_path = device.native_device_path;
    identity.eid_index = device.eid_index;
    identity.eid = device.eid;
    identity.kind = kind;
    identity.transport_modes = device.capabilities.transport_modes;
    return identity;
}

TEST(UbDeviceSelectionTest, DetectsBondingNamesCaseInsensitively) {
    EXPECT_TRUE(isBondingDeviceName("bonding_dev_0"));
    EXPECT_TRUE(isBondingDeviceName("Bonding_Dev_0"));
    EXPECT_TRUE(isBondingDeviceName("ub:bonding_dev_0:eid0"));
    EXPECT_TRUE(isBondingDeviceName("foo_bond_bar"));
    EXPECT_FALSE(isBondingDeviceName("udmac1d1e2"));
    EXPECT_FALSE(isBondingDeviceName("ub:udmac1d1e2:eid0"));
}

TEST(UbDeviceSelectionTest, PrefersBondingWhenPresentAndFilterEmpty) {
    const std::vector<DeviceInfo> devices = {
        makeDevice("udmac1d1e2"),
        makeDevice("bonding_dev_0"),
        makeDevice("udmac0d0e1"),
    };
    const auto selected =
        preferBondingDevicesIfPresent(devices, /*explicit_filter=*/false);
    ASSERT_EQ(selected.size(), 1U);
    EXPECT_EQ(selected[0].native_device_name, "bonding_dev_0");
}

TEST(UbDeviceSelectionTest, KeepsAllDevicesWhenNoBondingPresent) {
    const std::vector<DeviceInfo> devices = {
        makeDevice("udmac1d1e2"),
        makeDevice("udmac0d0e1"),
    };
    const auto selected =
        preferBondingDevicesIfPresent(devices, /*explicit_filter=*/false);
    ASSERT_EQ(selected.size(), 2U);
    EXPECT_EQ(selected[0].native_device_name, "udmac1d1e2");
    EXPECT_EQ(selected[1].native_device_name, "udmac0d0e1");
}

TEST(UbDeviceSelectionTest, ExplicitFilterSkipsAutoPrefer) {
    const std::vector<DeviceInfo> devices = {
        makeDevice("udmac1d1e2"),
        makeDevice("bonding_dev_0"),
    };
    const auto selected =
        preferBondingDevicesIfPresent(devices, /*explicit_filter=*/true);
    ASSERT_EQ(selected.size(), 2U);
}

TEST(UbDeviceSelectionTest, EmptyInputStaysEmpty) {
    const auto selected =
        preferBondingDevicesIfPresent({}, /*explicit_filter=*/false);
    EXPECT_TRUE(selected.empty());
}

TEST(UbDeviceSelectionTest, PhysicalOnlyRejectsUnmappedNames) {
    std::vector<DeviceInfo> devices = {makeDevice("bonding_dev_0"),
                                       makeDevice("udmac0d0e1")};
    ASSERT_TRUE(resolveDeviceKindIdentities({}, devices).ok());
    for (const auto& device : devices) {
        EXPECT_EQ(device.kind, DeviceKind::UNKNOWN);
        EXPECT_EQ(device.kind_source, DeviceKindSource::NONE);
    }
    EXPECT_TRUE(physicalDevices(devices).empty());
}

TEST(UbDeviceSelectionTest, TrustedKindOverridesLegacyNameHeuristic) {
    std::vector<DeviceInfo> devices = {makeDevice("bonding_dev_0"),
                                       makeDevice("udmac0d0e1")};
    const std::vector<DeviceIdentity> identities = {
        makeIdentity(devices[0], DeviceKind::PHYSICAL),
        makeIdentity(devices[1], DeviceKind::BONDING)};
    ASSERT_TRUE(resolveDeviceKindIdentities(identities, devices).ok());

    const auto selected = physicalDevices(devices);
    ASSERT_EQ(selected.size(), 1U);
    EXPECT_EQ(selected[0].native_device_name, "bonding_dev_0");
    EXPECT_FALSE(isBondingDevice(selected[0]));
    EXPECT_TRUE(isBondingDevice(devices[1]));
    EXPECT_EQ(devices[0].kind_source, DeviceKindSource::EXPLICIT_CONFIG);
}

TEST(UbDeviceSelectionTest, TrustedIdentityRejectsStalePathAndCapabilities) {
    auto device = makeDevice("udmac0d0e1");
    auto identity = makeIdentity(device, DeviceKind::PHYSICAL);
    identity.native_device_path = "/stale/path";
    std::vector<DeviceInfo> devices = {device};
    EXPECT_TRUE(
        resolveDeviceKindIdentities({identity}, devices).IsDeviceNotFound());
    EXPECT_EQ(devices[0].kind, DeviceKind::UNKNOWN);

    identity = makeIdentity(device, DeviceKind::PHYSICAL);
    identity.transport_modes ^= 1;
    EXPECT_TRUE(
        resolveDeviceKindIdentities({identity}, devices).IsInvalidArgument());

    identity = makeIdentity(device, DeviceKind::PHYSICAL);
    identity.transport_modes = 8;
    EXPECT_TRUE(
        resolveDeviceKindIdentities({identity}, devices).IsInvalidArgument());

    identity = makeIdentity(device, DeviceKind::PHYSICAL);
    auto duplicate = identity;
    duplicate.kind = DeviceKind::BONDING;
    EXPECT_TRUE(resolveDeviceKindIdentities({identity, duplicate}, devices)
                    .IsInvalidArgument());
}

TEST(UbParamsTest,
     ParsesTrustedDeviceIdentitiesAndRequiresThemForPhysicalOnly) {
    const auto device = makeDevice("udmac0d0e1");
    const auto identity = makeIdentity(device, DeviceKind::PHYSICAL);
    Config config;
    config.set("transports/ub/physical_only", true);
    config.set("transports/ub/device_identities",
               nlohmann::json::array(
                   {{{"native_device_name", identity.native_device_name},
                     {"native_device_path", identity.native_device_path},
                     {"eid_index", identity.eid_index},
                     {"eid", identity.eid},
                     {"kind", "physical"},
                     {"transport_modes", identity.transport_modes}}}));

    UbParams params;
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_TRUE(params.physical_only);
    ASSERT_EQ(params.device_identities.size(), 1U);
    EXPECT_EQ(params.device_identities[0].kind, DeviceKind::PHYSICAL);

    config.set("transports/ub/device_identities", nlohmann::json::array());
    EXPECT_TRUE(UbParams::FromConfig(config, params).IsInvalidArgument());
}

TEST(UbRailMonitorTest, EndpointRebuildTelemetryDeduplicatesGeneration) {
    RailMonitor monitor;
    const auto first = makePath(0, 1, 10);
    const auto replacement = makePath(0, 1, 11);

    // The first ready generation is the rail's initial build, not a rebuild,
    // so it seeds the watermark without advancing telemetry.
    EXPECT_FALSE(monitor.recordEndpointRebuild(first, 100));
    EXPECT_FALSE(monitor.recordEndpointRebuild(first, 101));
    EXPECT_TRUE(monitor.recordEndpointRebuild(replacement, 102));
    // A late callback for an older incarnation cannot double count it.
    EXPECT_FALSE(monitor.recordEndpointRebuild(first, 103));

    const auto stats = monitor.stats(replacement, 103);
    EXPECT_EQ(stats.endpoint_rebuilds, 1U);
    EXPECT_EQ(stats.latest_endpoint_generation, 11U);
}

TEST(UbRailMonitorTest, RebuildBaselineIsPerRail) {
    RailMonitor monitor;
    const auto rail_a = makePath(0, 1, 5);
    const auto rail_b = makePath(0, 2, 9);

    EXPECT_FALSE(monitor.recordEndpointRebuild(rail_a, 100));
    EXPECT_FALSE(monitor.recordEndpointRebuild(rail_b, 101));
    EXPECT_TRUE(monitor.recordEndpointRebuild(makePath(0, 1, 6), 102));

    EXPECT_EQ(monitor.stats(rail_a, 102).endpoint_rebuilds, 1U);
    EXPECT_EQ(monitor.stats(rail_b, 102).endpoint_rebuilds, 0U);
}

TEST(UbRailMonitorTest, StatsIfPresentDoesNotCreateOrSeedRails) {
    RailMonitor monitor;
    const auto unknown = makePath(0, 3, 1);

    const auto before = monitor.statsIfPresent(unknown, 100);
    EXPECT_FALSE(before.paused);
    EXPECT_LT(before.ewma_bandwidth_bytes_per_second, 0.0);
    EXPECT_EQ(monitor.pathCount(), 0U);

    // A real record still creates the rail and a non-creating lookup then
    // returns that state.
    monitor.recordError(makePath(0, 1, 7), 101);
    EXPECT_EQ(monitor.pathCount(), 1U);
    EXPECT_EQ(monitor.statsIfPresent(makePath(0, 1, 7), 101).completion_errors,
              1U);
    EXPECT_EQ(monitor.statsIfPresent(unknown, 101).completion_errors, 0U);
    EXPECT_EQ(monitor.pathCount(), 1U);
}

TEST(UbRailSelectionTest, AttemptOrderTakesOneRailPerLocalDevice) {
    // Pre-score order puts both rails of device 0 above device 1's, which is
    // what a cold peer with no quota pressure looks like.
    const std::vector<Topology::NicID> ids{0, 0, 1, 1};
    const auto order = ubResolutionAttemptOrder(ids);
    ASSERT_EQ(order.size(), 4U);
    // Device 0's best rail first, then device 1's, before device 0's second
    // rail: the first two attempts must not share a device.
    EXPECT_EQ(order[0], 0U);
    EXPECT_EQ(order[1], 2U);
    EXPECT_EQ(order[2], 1U);
    EXPECT_EQ(order[3], 3U);
}

TEST(UbRailSelectionTest, AttemptOrderCoversEveryLocalDevice) {
    const std::vector<Topology::NicID> ids{0, 0, 1, 1, 2, 2, 3, 3};
    const auto order = ubResolutionAttemptOrder(ids);
    ASSERT_EQ(order.size(), 8U);
    EXPECT_EQ(order[0], 0U);
    EXPECT_EQ(order[1], 2U);
    EXPECT_EQ(order[2], 4U);
    EXPECT_EQ(order[3], 6U);
    // The resolve limit for four local devices is four, so every device is
    // represented before any device gets a second candidate.
    for (size_t index = 0; index < 4; ++index) {
        EXPECT_EQ(ids[order[index]], static_cast<Topology::NicID>(index));
    }
}

TEST(UbRailSelectionTest, SingleDeviceKeepsPreScoreOrder) {
    const std::vector<Topology::NicID> ids{7, 7, 7};
    // With one device there is nothing to spread across, so the attempt order
    // is the pre-score order and the caller's floor of two candidates takes two
    // rails on that device.
    EXPECT_EQ(ubResolutionAttemptOrder(ids), (std::vector<size_t>{0, 1, 2}));
}

TEST(UbRailSelectionTest, AttemptOrderVisitsEveryRailExactlyOnce) {
    const std::vector<Topology::NicID> ids{3, 1, 3, 2, 1};
    const auto order = ubResolutionAttemptOrder(ids);
    ASSERT_EQ(order.size(), ids.size());
    for (size_t index = 0; index < ids.size(); ++index) {
        EXPECT_EQ(std::count(order.begin(), order.end(), index), 1);
    }
}

// ---------------------------------------------------------------------------
// Transport mode normalization.
//
// The adapter options carry the same "is this RM?" concept twice:
// transport_mode (probe path, rm_pool) and trans_mode (RM Legacy). A caller
// may set only one of them, so JFC creation, JFS creation and the post gate
// must all derive the native mode from isReliableMessage(). These tests pin
// the four production profiles and the native invariants they depend on.
// ---------------------------------------------------------------------------

TEST(UbTransportModeNormalizationTest, AllProfilesResolveConsistently) {
    struct Case {
        const char* name;
        JettyTransportMode transport_mode;
        uint16_t trans_mode;
        bool reliable_message;
    };
    const Case cases[] = {
        // Probe: only the adapter-level spelling is set. Before the fix this
        // combination created an RC JFR and an RC JFS.
        {"probe", JettyTransportMode::kRm, kUrmaTransportModeRc, true},
        // RM Legacy: only the native spelling is set.
        {"rm_legacy", JettyTransportMode::kRc, kUrmaTransportModeRm, true},
        // rm_pool: both spellings are set.
        {"rm_pool", JettyTransportMode::kRm, kUrmaTransportModeRm, true},
        // RC Legacy: neither spelling selects RM.
        {"rc_legacy", JettyTransportMode::kRc, kUrmaTransportModeRc, false},
    };
    for (const Case& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        JfcOptions jfc_options;
        jfc_options.transport_mode = test_case.transport_mode;
        jfc_options.trans_mode = test_case.trans_mode;
        JettyOptions jetty_options;
        jetty_options.transport_mode = test_case.transport_mode;
        jetty_options.trans_mode = test_case.trans_mode;

        EXPECT_EQ(isReliableMessage(jfc_options), test_case.reliable_message);
        EXPECT_EQ(isReliableMessage(jetty_options), test_case.reliable_message);

        const uint16_t expected_native = test_case.reliable_message
                                             ? kUrmaTransportModeRm
                                             : kUrmaTransportModeRc;
        // INV-1: the JFR mode comes from the same predicate as the shared-JFC
        // decision (shared_jfc_ is true exactly when the predicate is true).
        EXPECT_EQ(nativeTransportMode(isReliableMessage(jfc_options)),
                  expected_native);
        // INV-2: the JFS mode and reliableMessage() agree.
        EXPECT_EQ(nativeTransportMode(isReliableMessage(jetty_options)),
                  expected_native);
        // INV-3: JFS.trans_mode == shared JFR.trans_mode.
        EXPECT_EQ(nativeTransportMode(isReliableMessage(jfc_options)),
                  nativeTransportMode(isReliableMessage(jetty_options)));
    }
}

TEST(UbTransportModeNormalizationTest, ProbeRmIsPostableBeforeBind) {
    JettyOptions options;
    options.transport_mode = JettyTransportMode::kRm;
    // trans_mode keeps its RC default: this is the single-flag probe caller.
    ASSERT_EQ(options.trans_mode, kUrmaTransportModeRc);
    ASSERT_TRUE(isReliableMessage(options));

    // INV-4: a native RM Jetty posts per-WR targets and needs no bind.
    EXPECT_TRUE(isJettyPostable(true, /*created=*/true, /*bound=*/false));
    EXPECT_TRUE(isJettyPostable(true, /*created=*/false, /*bound=*/true));
    // An ERROR or RESET RM Jetty is still not postable.
    EXPECT_FALSE(isJettyPostable(true, /*created=*/false, /*bound=*/false));
}

TEST(UbTransportModeNormalizationTest, RcLegacyStillRequiresBind) {
    JettyOptions options;
    ASSERT_FALSE(isReliableMessage(options));

    EXPECT_FALSE(isJettyPostable(false, /*created=*/true, /*bound=*/false));
    EXPECT_TRUE(isJettyPostable(false, /*created=*/false, /*bound=*/true));
    EXPECT_FALSE(isJettyPostable(false, /*created=*/false, /*bound=*/false));
}

TEST(UbTransportModeNormalizationTest, NativeValuesMatchUrmaConstants) {
    // The adapter-neutral constants are static_asserted against UMDK in
    // urma_adapter.cpp; this pins the normalized result the probe log reports.
    EXPECT_EQ(kUrmaTransportModeRm, 0x1);
    EXPECT_EQ(kUrmaTransportModeRc, 0x2);
    EXPECT_EQ(nativeTransportMode(true), kUrmaTransportModeRm);
    EXPECT_EQ(nativeTransportMode(false), kUrmaTransportModeRc);
}

}  // namespace
}  // namespace mooncake::tent::ub
