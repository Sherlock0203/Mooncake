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
// ProbeLinkManager unit tests for the W1a exit matrix (design §14.2):
//   #1  state-machine transitions and their side effects,
//   #9  token namespace,
//   #11 parameter-class business errors never isolate a path,
//   #13 business-error idempotency while isolated,
// plus the recovery gate and the generation-scoped port slot.

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "tent/transport/ub/probe_link_manager.h"
#include "ub_probe_test_support.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace probe_test;

constexpr uint64_t kIntervalNs = 1000ULL * 1000 * 1000;

TEST(UbProbeLinkManagerTest, InitializesEligibleAndBusinessSuccessPromotes) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);

    auto graph = makeGraph({makePortSupply(0, 1), makePortSupply(1, 1)},
                           {makeEdgeSupply(0, 10, 0), makeEdgeSupply(0, 10, 1),
                            makeEdgeSupply(1, 20, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    EXPECT_TRUE(plm.initialized());
    EXPECT_EQ(plm.graphEpoch(), 1U);

    const auto view = plm.healthView();
    ASSERT_EQ(view.size(), 3U);
    for (const auto& entry : view) {
        EXPECT_EQ(entry.health, LinkHealth::kEligibleUnverified);
        EXPECT_EQ(entry.task, TaskState::kIdle);
        EXPECT_FALSE(entry.pending);
    }
    EXPECT_EQ(plm.stats().path_state_eligible, 3U);

    const PathVersion v00{ProbePathKey{0, 10, 0}, 1, 1};
    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    ASSERT_TRUE(hasHealth(plm, v00));
    EXPECT_EQ(healthOf(plm, v00).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().path_state_healthy, 1U);

    // A repeated success on a healthy path is not a second publication.
    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    EXPECT_EQ(plm.stats().path_state_healthy, 1U);

    // The probe namespace is claimed first: a business token is not a probe,
    // and an unknown probe token is accounted instead of crashing.
    EXPECT_FALSE(plm.onProbeCompletion(makeCompletion(42), clock.now()));
    EXPECT_EQ(plm.stats().probe_unknown_token, 0U);
    EXPECT_TRUE(plm.onProbeCompletion(makeCompletion(makeProbeToken(999)),
                                      clock.now()));
    EXPECT_EQ(plm.stats().probe_unknown_token, 1U);
}

TEST(UbProbeLinkManagerTest,
     ProbePathKeyBusinessEntriesMatchTheSyntheticGraph) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto edge = makeEdgeSupply(0, 10, 0);
    edge.version.path.segment_id = static_cast<SegmentID>(kProbePeerIdBase);
    auto graph = makeGraph({makePortSupply(0, 1)}, {edge});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const ProbePathKey key{0, static_cast<SegmentID>(kProbePeerIdBase), 0};

    EXPECT_TRUE(plm.businessPathUsable(key));
    EXPECT_EQ(plm.businessFaultEpoch(key), 0U);
    plm.onBusinessPathError(key, BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(plm.businessFaultEpoch(key), 1U);
    EXPECT_FALSE(plm.businessPathUsable(key));

    // An invalid key (no synthetic peer) is a no-op for every entry point.
    const ProbePathKey invalid{0, LOCAL_SEGMENT_ID, 0};
    EXPECT_TRUE(plm.businessPathUsable(invalid));
    EXPECT_EQ(plm.businessFaultEpoch(invalid), 0U);
    plm.onBusinessPathSuccess(invalid, clock.now());
    plm.onBusinessPathError(invalid, BusinessErrorKind::kPathFault,
                            clock.now());
}

TEST(UbProbeLinkManagerTest, TargetEpochChangeReplacesAcrossTopologyEpochs) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion old_version{ProbePathKey{0, 10, 0}, 1, 1};
    ASSERT_TRUE(hasHealth(plm, old_version));

    // Node-level epoch change: topology_epoch and target_epoch both move.
    ProbeEvent event;
    event.kind = ProbeEventKind::kTargetEpochChanged;
    event.graph_epoch = 1;
    event.event_seq = 1;
    event.edge = makeEdgeSupply(0, 10, 0, /*topology_epoch=*/2,
                                /*target_epoch=*/2);
    ASSERT_TRUE(plm.onPoolAvailabilityChange(event).ok());
    EXPECT_FALSE(hasHealth(plm, old_version));
    const PathVersion new_version{ProbePathKey{0, 10, 0}, 2, 2};
    EXPECT_TRUE(hasHealth(plm, new_version));
    EXPECT_EQ(plm.pathCount(), 1U);
    EXPECT_EQ(healthOf(plm, new_version).health,
              LinkHealth::kEligibleUnverified);

    // The replacement is atomic: no "multiple versions for one path" error.
    ProbeEvent same_path_added;
    same_path_added.kind = ProbeEventKind::kEdgeAdded;
    same_path_added.graph_epoch = 1;
    same_path_added.event_seq = 2;
    same_path_added.edge = makeEdgeSupply(0, 10, 0, 3, 3);
    EXPECT_TRUE(
        plm.onPoolAvailabilityChange(same_path_added).IsInvalidArgument());
}

TEST(UbProbeLinkManagerTest, DynamicEdgesAllowAnEmptyStartupGraph) {
    TestClock clock;
    auto params = defaultParams();
    params.dynamic_edges = true;
    ProbeLinkManager plm(params, clock.clock());

    // Declared topology: ports exist, the peer handshake has not completed.
    auto graph = makeGraph({makePortSupply(0, 1)}, {});
    ASSERT_TRUE(plm.initialize(graph).ok());
    EXPECT_EQ(plm.pathCount(), 0U);
    EXPECT_EQ(plm.graphEpoch(), 1U);

    ProbeEvent added;
    added.kind = ProbeEventKind::kEdgeAdded;
    added.graph_epoch = 1;
    added.event_seq = 1;
    added.edge = makeEdgeSupply(0, 10, 0);
    ASSERT_TRUE(plm.onPoolAvailabilityChange(added).ok());
    EXPECT_EQ(plm.pathCount(), 1U);
    EXPECT_EQ(plm.stats().path_state_eligible, 1U);
}

TEST(UbProbeLinkManagerTest, EmptyGraphStillFailsClosedWithoutDynamicEdges) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {});
    EXPECT_TRUE(plm.initialize(graph).IsInvalidArgument());
}

TEST(UbProbeLinkManagerTest, BusinessGateRequiresExternalPeerGates) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph =
        makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0, 1, 1, 1,
                                                          /*gates_ok=*/false)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    // The path is in the graph and healthy-unverified, but the peer's
    // published node/port/profile gate is closed: business must not use it.
    EXPECT_FALSE(plm.businessPathUsable(makeEvidence(0, 10, 0)));
    EXPECT_EQ(plm.stats().business_path_gate_blocked, 1U);

    ProbeEvent event;
    event.kind = ProbeEventKind::kGateChanged;
    event.local_nic = 0;
    event.graph_epoch = 1;
    event.event_seq = 1;
    event.gate = GateBits{true, true, true};
    ASSERT_TRUE(plm.onPoolAvailabilityChange(event).ok());
    EXPECT_TRUE(plm.businessPathUsable(makeEvidence(0, 10, 0)));
}

TEST(UbProbeLinkManagerTest, BusinessSuccessNeverClearsIsolation) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);

    // An old success arriving after a newer fault must not undo the fault.
    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);
}

TEST(UbProbeLinkManagerTest, HealthyPathFaultIsolatesWithNewEpoch) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    const auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kIsolated);
    EXPECT_EQ(health.fault_epoch, 1U);
    EXPECT_EQ(health.next_probe_ns, clock.now());
    EXPECT_EQ(plm.stats().path_state_isolated, 1U);
    EXPECT_EQ(plm.campaignView().size(), 1U);
}

TEST(UbProbeLinkManagerTest, CompletionBeforePostReturnIsSettledOnce) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());

    // F11: the CQE may legally beat the post call's return (invariant 4), but
    // the record must NOT settle before the post returns: the port slot and
    // peer budget encode "only one in-flight probe on this port", and the
    // provider call is still inside that window.
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    EXPECT_EQ(plm.inflightCount(), 1U);
    EXPECT_NE(healthOf(plm, version).health, LinkHealth::kHealthy);

    // The post result lands: now (and only now) the record settles once.
    plm.onPostResult(probe->record, ProbePostOutcome::kAccepted, clock.now());
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().probe_posted, 1U);
    EXPECT_EQ(plm.stats().probe_post_conflict, 0U);
}

TEST(UbProbeLinkManagerTest, ParameterAndLaneBusinessErrorsNeverIsolate) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};
    for (const auto kind :
         {BusinessErrorKind::kParamError, BusinessErrorKind::kLaneError,
          BusinessErrorKind::kUnknown}) {
        plm.onBusinessPathError(makeEvidence(0, 10, 0), kind, clock.now());
        EXPECT_EQ(healthOf(plm, version).health,
                  LinkHealth::kEligibleUnverified);
    }
    EXPECT_TRUE(plm.campaignView().empty());
    const auto stats = plm.stats();
    EXPECT_EQ(stats.business_param_error, 1U);
    EXPECT_EQ(stats.business_lane_error, 1U);
    EXPECT_EQ(stats.business_unknown_error, 1U);
    EXPECT_EQ(stats.campaign_created, 0U);
    EXPECT_EQ(stats.business_error_while_isolated, 0U);
}

TEST(UbProbeLinkManagerTest, BusinessFaultIsIdempotentWhileIsolated) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    auto isolated = healthOf(plm, first);
    EXPECT_EQ(isolated.health, LinkHealth::kIsolated);
    EXPECT_EQ(isolated.fault_epoch, 1U);
    EXPECT_EQ(isolated.task, TaskState::kQueued);
    EXPECT_EQ(isolated.next_probe_ns, clock.now());

    auto campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 1U);
    ASSERT_EQ(campaigns[0].entries.size(), 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->state, EntryState::kQueued);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->state,
              EntryState::kQueued);

    // Without business filtering the same isolated path keeps failing. That
    // must not raise its fault epoch or merge the campaign (design §6.3-3),
    // otherwise every in-flight probe would go stale forever.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, first).fault_epoch, 1U);
    campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 1U);
    EXPECT_EQ(plm.stats().business_error_while_isolated, 1U);

    // A fault on another path of the same peer merges the campaign, and the
    // queued entry is re-armed under the new epoch.
    plm.onBusinessPathError(makeEvidence(0, 10, 1),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kIsolated);
    campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->entry_epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->entry_epoch, 2U);
    EXPECT_EQ(plm.stats().campaign_merged, 1U);
}

TEST(UbProbeLinkManagerTest, ProbeSuccessRecoversOnlyWhenGatesPass) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph =
        makeGraph({makePortSupply(0, 1)},
                  {makeEdgeSupply(0, 10, 0, 1, 1, 1, /*gates_ok=*/false)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);

    uint64_t token_seq = 0;
    auto posted = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(posted.has_value());
    plm.onPostResult(posted->record, ProbePostOutcome::kAccepted, clock.now());
    EXPECT_TRUE(
        plm.onProbeCompletion(makeCompletion(posted->token), clock.now()));

    auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kRecovering);
    EXPECT_EQ(health.next_probe_ns, clock.now() + kIntervalNs);
    EXPECT_EQ(health.task, TaskState::kIdle);
    EXPECT_EQ(plm.stats().probe_gate_blocked_by_external, 1U);
    // Campaign exit is driven by entry terminal states, not by RECOVERING.
    EXPECT_TRUE(plm.campaignView().empty());
    EXPECT_EQ(plm.stats().campaign_exited, 1U);

    ProbeEvent gate_event;
    gate_event.kind = ProbeEventKind::kGateChanged;
    gate_event.local_nic = 0;
    gate_event.graph_epoch = 1;
    gate_event.event_seq = 1;
    gate_event.gate = GateBits{true, true, true};
    ASSERT_TRUE(plm.onPoolAvailabilityChange(gate_event).ok());

    health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kHealthy);
    EXPECT_EQ(health.next_probe_ns, 0U);
    EXPECT_EQ(plm.stats().path_state_healthy, 1U);
}

TEST(UbProbeLinkManagerTest, LaneErrorRetiresLaneOnceAndMarksPortPending) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto posted = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(posted.has_value());
    ASSERT_EQ(posted->record->key.version, first);

    plm.onPostResult(posted->record, ProbePostOutcome::kAccepted, clock.now());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(posted->token, CompletionCategory::ENDPOINT_ERROR),
        clock.now()));

    // A lane/configuration error must not mark the path bad (L-3), but the
    // whole port becomes Pending and the lane is reported exactly once.
    for (const auto& version : {first, second}) {
        const auto health = healthOf(plm, version);
        EXPECT_EQ(health.health, version == first
                                     ? LinkHealth::kIsolated
                                     : LinkHealth::kEligibleUnverified);
        EXPECT_TRUE(health.pending);
        EXPECT_EQ(health.pending_reason, PendingReason::kLaneRetired);
    }
    ASSERT_EQ(reports.retire_calls.size(), 1U);
    EXPECT_EQ(reports.retire_calls[0].jetty_generation, 1U);
    EXPECT_EQ(reports.retire_calls[0].lane.local_nic, 0);
    EXPECT_EQ(plm.stats().probe_failure_lane, 1U);
    EXPECT_EQ(plm.stats().probe_lane_retire, 1U);
    EXPECT_EQ(plm.inflightCount(), 0U);

    // Duplicate CQE: claimed already, so it only shows up as an unknown token;
    // it must not report the lane twice.
    EXPECT_TRUE(plm.onProbeCompletion(
        makeCompletion(posted->token, CompletionCategory::ENDPOINT_ERROR),
        clock.now()));
    EXPECT_EQ(reports.retire_calls.size(), 1U);
    EXPECT_EQ(plm.stats().probe_unknown_token, 1U);
    // A broken lane posts nothing until interface 7 replaces the handle.
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());
}

TEST(UbProbeLinkManagerTest, ContextUnavailableDisablesPortAndCancelsCampaign) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1), makePortSupply(1, 1)},
                           {makeEdgeSupply(0, 10, 0), makeEdgeSupply(0, 10, 1),
                            makeEdgeSupply(1, 20, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_EQ(plm.campaignView().size(), 1U);

    plm.onContextUnavailable(0, clock.now());
    for (const auto& entry : plm.healthView()) {
        if (entry.version.path.local_nic != 0) continue;
        EXPECT_EQ(entry.health, LinkHealth::kDisabled);
        EXPECT_TRUE(entry.pending);
        EXPECT_EQ(entry.pending_reason, PendingReason::kContextFailure);
        EXPECT_EQ(entry.task, TaskState::kIdle);
    }
    EXPECT_TRUE(plm.campaignView().empty());
    EXPECT_EQ(plm.stats().path_state_disabled, 2U);
    EXPECT_EQ(plm.stats().campaign_entry_canceled, 2U);
    // A context failure is reported through interface 9, not interface 2.
    EXPECT_TRUE(reports.retire_calls.empty());

    // A DISABLED path is not recovered by probing: further business errors are
    // counted, not turned into isolation or a new campaign.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(plm.stats().business_error_while_disabled, 1U);
    EXPECT_TRUE(plm.campaignView().empty());
}

// F7: the availability edge is the dual of onContextUnavailable(). It must
// revive exactly the paths the context failure disabled, and only into
// RECOVERING: HEALTHY is still earned by a real probe.
TEST(UbProbeLinkManagerTest, ContextAvailableRevivesContextDisabledPaths) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    plm.onContextUnavailable(0, clock.now());
    ASSERT_EQ(plm.stats().path_state_disabled, 2U);
    ASSERT_TRUE(plm.pendingActions(clock.now()).empty());

    ASSERT_TRUE(plm.onContextAvailable(0, clock.now()).ok());
    EXPECT_EQ(plm.stats().path_revived_on_context_available, 2U);
    for (const auto& entry : plm.healthView()) {
        EXPECT_EQ(entry.health, LinkHealth::kRecovering);
        EXPECT_FALSE(entry.pending);
        EXPECT_EQ(entry.pending_reason, PendingReason::kNone);
        EXPECT_EQ(entry.task, TaskState::kIdle);
    }
    // INV-REC-2: never HEALTHY without evidence.
    EXPECT_NE(healthOf(plm, PathVersion{ProbePathKey{0, 10, 0}, 1, 1}).health,
              LinkHealth::kHealthy);
    // The revived paths are immediately schedulable for verification.
    EXPECT_FALSE(plm.pendingActions(clock.now()).empty());
}

TEST(UbProbeLinkManagerTest, ContextAvailableRecoveryNeedsAProbeToBeHealthy) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onContextUnavailable(0, clock.now());
    ASSERT_TRUE(plm.onContextAvailable(0, clock.now()).ok());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kRecovering);

    uint64_t token_seq = 0;
    auto posted = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(posted.has_value());
    plm.onPostResult(posted->record, ProbePostOutcome::kAccepted, clock.now());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(posted->token), clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
}

TEST(UbProbeLinkManagerTest, ContextAvailableNeverResurrectsARemovedPath) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion kept{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion removed{ProbePathKey{0, 10, 1}, 1, 1};

    plm.onContextUnavailable(0, clock.now());
    ProbeEvent removal;
    removal.kind = ProbeEventKind::kEdgeRemoved;
    removal.graph_epoch = 1;
    removal.event_seq = 1;
    removal.version = removed;
    ASSERT_TRUE(plm.onPoolAvailabilityChange(removal).ok());
    ASSERT_FALSE(hasHealth(plm, removed));

    ASSERT_TRUE(plm.onContextAvailable(0, clock.now()).ok());
    EXPECT_EQ(plm.pathCount(), 1U);
    EXPECT_EQ(plm.stats().path_revived_on_context_available, 1U);
    EXPECT_EQ(healthOf(plm, kept).health, LinkHealth::kRecovering);
    EXPECT_FALSE(hasHealth(plm, removed));
}

TEST(UbProbeLinkManagerTest, ContextAvailableOnAnUnknownPortIsRejected) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    EXPECT_TRUE(plm.onContextAvailable(7, clock.now()).IsInvalidArgument());
    EXPECT_EQ(plm.stats().path_revived_on_context_available, 0U);
}

TEST(UbProbeLinkManagerTest, StaleCompletionReleasesSlotWithoutChangingHealth) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};

    // Fault on the second path starts a campaign that verifies both.
    plm.onBusinessPathError(makeEvidence(0, 10, 1),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto in_flight = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(in_flight.has_value());
    ASSERT_EQ(in_flight->record->key.version, first);
    ASSERT_EQ(plm.campaignView()[0].epoch, 1U);

    // A newer fault on the in-flight path bumps its fault epoch, so the
    // outstanding success can no longer publish health.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
    EXPECT_EQ(healthOf(plm, first).fault_epoch, 1U);
    auto campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->state,
              EntryState::kInflight);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->entry_epoch, 1U);

    plm.onPostResult(in_flight->record, ProbePostOutcome::kAccepted,
                     clock.now());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(in_flight->token), clock.now()));
    EXPECT_EQ(plm.stats().probe_stale, 1U);
    // A stale success is not current evidence, so it is not counted as one.
    EXPECT_EQ(plm.stats().probe_success, 0U);
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
    // The task slot is released by probe_seq even though the result was stale,
    // and the matching campaign entry converges to a terminal state.
    EXPECT_EQ(healthOf(plm, first).inflight_seq, 0U);
    EXPECT_EQ(healthOf(plm, first).task, TaskState::kIdle);
    campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->state,
              EntryState::kCanceled);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->state,
              EntryState::kQueued);

    // The next campaign attempt is the other path; it recovers normally.
    auto next = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(next->record->key.version, second);
    plm.onPostResult(next->record, ProbePostOutcome::kAccepted, clock.now());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(next->token), clock.now()));
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kHealthy);
    EXPECT_TRUE(plm.campaignView().empty());
}

TEST(UbProbeLinkManagerTest,
     GenerationReplacementOrphansInflightAndResetsSlot) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto old_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(old_probe.has_value());
    EXPECT_EQ(plm.inflightCount(), 1U);

    ProbeEvent replaced;
    replaced.kind = ProbeEventKind::kProbeHandleReplaced;
    replaced.local_nic = 0;
    replaced.graph_epoch = 1;
    replaced.event_seq = 1;
    replaced.port = makePortSupply(0, 2);
    ASSERT_TRUE(plm.onPoolAvailabilityChange(replaced).ok());

    // Old generation becomes an orphan (references retained), the slot is
    // reset for the new generation, and the isolated path probes immediately.
    EXPECT_EQ(plm.orphanCount(), 1U);
    EXPECT_EQ(plm.inflightCount(), 0U);
    auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kIsolated);
    EXPECT_FALSE(health.pending);
    EXPECT_EQ(health.next_probe_ns, clock.now());
    // Orphan transfer does not report interface 2.
    EXPECT_TRUE(reports.retire_calls.empty());

    auto new_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(new_probe.has_value());
    EXPECT_NE(new_probe->token, old_probe->token);
    EXPECT_EQ(plm.inflightCount(), 1U);

    // A late completion of the old generation only settles the orphan; it must
    // not disturb the new generation's slot or health.
    plm.onPostResult(old_probe->record, ProbePostOutcome::kAccepted,
                     clock.now());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(old_probe->token), clock.now()));
    EXPECT_EQ(plm.orphanCount(), 0U);
    EXPECT_EQ(plm.stats().probe_orphan_settled, 1U);
    EXPECT_EQ(plm.inflightCount(), 1U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);

    plm.onPostResult(new_probe->record, ProbePostOutcome::kAccepted,
                     clock.now());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(new_probe->token), clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().probe_slot_anomaly, 0U);
}

TEST(UbProbeLinkManagerTest, PeerBudgetGatesCrossPortScheduling) {
    TestClock clock;
    auto params = defaultParams();
    params.peer_budget = 1;
    ProbeLinkManager plm(params, clock.clock());
    auto graph =
        makeGraph({makePortSupply(0, 1), makePortSupply(1, 1)},
                  {makeEdgeSupply(0, 10, 0), makeEdgeSupply(1, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{1, 10, 1}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    auto actions = plm.pendingActions(clock.now());
    ASSERT_EQ(actions.size(), 2U);

    uint64_t token_seq = 0;
    auto first_probe =
        plm.beginPost(actions[0], makeProbeToken(++token_seq), clock.now());
    ASSERT_NE(first_probe, nullptr);
    // The peer budget is shared across ports, so the second port cannot claim
    // another in-flight probe to the same peer.
    auto blocked =
        plm.beginPost(actions[1], makeProbeToken(++token_seq), clock.now());
    EXPECT_EQ(blocked, nullptr);
    EXPECT_EQ(plm.stats().probe_skipped_peer_budget, 0U);

    // A lane error cancels the first entry and frees the budget; the second
    // path is then schedulable and can recover.
    plm.onPostResult(first_probe, ProbePostOutcome::kAccepted, clock.now());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(first_probe->core.token,
                       CompletionCategory::ENDPOINT_ERROR),
        clock.now()));
    auto retry = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(retry.has_value());
    EXPECT_EQ(retry->record->key.version, second);
    plm.onPostResult(retry->record, ProbePostOutcome::kAccepted, clock.now());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(retry->token), clock.now()));
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kHealthy);
    // The lane error did not mark the first path bad; it stays isolated from
    // its original business fault.
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
}

TEST(UbProbeLinkManagerTest, LocalAccessErrorIsolatesAndRetiresLaneOnce) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    plm.onPostResult(probe->record, ProbePostOutcome::kAccepted, clock.now());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::LOCAL_ACCESS_ERROR),
        clock.now()));

    const auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kIsolated);
    EXPECT_TRUE(health.pending);
    EXPECT_EQ(health.pending_reason, PendingReason::kLaneRetired);
    EXPECT_EQ(plm.stats().probe_failure_path, 1U);
    EXPECT_EQ(plm.stats().probe_lane_retire, 1U);
    ASSERT_EQ(reports.retire_calls.size(), 1U);
}

TEST(UbProbeLinkManagerTest, HandleReplacedPreservesBackoff) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const uint64_t now = clock.now();

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, now);
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, now);
    ASSERT_TRUE(probe.has_value());
    plm.onPostResult(probe->record, ProbePostOutcome::kAccepted, clock.now());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::REMOTE_PATH_ERROR),
        now));
    ASSERT_EQ(healthOf(plm, probe->record->key.version).health,
              LinkHealth::kIsolated);

    // The lane was retired; a replacement event must not erase the backoff.
    ProbeEvent event;
    event.kind = ProbeEventKind::kProbeHandleReplaced;
    event.local_nic = 0;
    event.graph_epoch = plm.graphEpoch();
    event.event_seq = 1;
    event.port = makePortSupply(0, 2);
    ASSERT_TRUE(plm.onPoolAvailabilityChange(event).ok());
    EXPECT_EQ(plm.stats().probe_lane_rebuild, 1U);

    EXPECT_TRUE(plm.pendingActions(now).empty());
    clock.advance(1000ULL * 1000 * 1000);
    EXPECT_FALSE(plm.pendingActions(clock.now()).empty());
}

// F8: a retryable (capacity/not-ready) post rejection must keep the campaign's
// verification obligation for that path. Cancelling the entry would leave a
// HEALTHY sibling path with no reason to ever be verified again
// (timeEligibleLocked never probes HEALTHY paths).
TEST(UbProbeLinkManagerTest, RetryablePostRejectionKeepsTheVerificationTodo) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion healthy{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion faulty{ProbePathKey{0, 10, 1}, 1, 1};

    // The faulty sibling starts a campaign that verifies both paths: the
    // healthy one is only probed because of this obligation.
    plm.onBusinessPathError(makeEvidence(0, 10, 1),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_FALSE(plm.campaignView().empty());

    uint64_t token_seq = 0;
    auto posted = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(posted.has_value());
    ASSERT_EQ(posted->record->key.version, healthy);
    // The provider rejected for a retryable reason.
    plm.onPostResult(posted->record, ProbePostOutcome::kRejectedRetryable,
                     clock.now());

    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(plm.stats().probe_post_rejected_retryable, 1U);
    EXPECT_EQ(plm.stats().probe_post_rejected, 0U);
    EXPECT_EQ(plm.stats().probe_verification_abandoned, 0U);
    // The entry is still queued: the obligation was not dropped.
    EXPECT_EQ(healthOf(plm, healthy).task, TaskState::kQueued);

    // Once the backoff expires the same path is scheduled again.
    const uint64_t retry_ns = plm.params().intervalNs() << 1;
    clock.advance(retry_ns + 1);
    // pendingActions() returns at most one probe per port, so drain the
    // currently eligible sibling before checking that the retained path comes
    // back. Every rejection is retryable, so this also proves the obligation
    // survives more than one rejection.
    bool scheduled = false;
    for (int round = 0; round < 3 && !scheduled; ++round) {
        auto next = postNext(plm, token_seq, clock.now());
        if (!next.has_value()) break;
        if (next->record->key.version == healthy) {
            scheduled = true;
            break;
        }
        plm.onPostResult(next->record, ProbePostOutcome::kRejectedRetryable,
                         clock.now());
        clock.advance(retry_ns + 1);
    }
    EXPECT_TRUE(scheduled);
}

// F8: a terminal rejection (revoked/superseded/invalid request) still cancels
// the obligation, and that give-up is not counted as an abandoned retry.
TEST(UbProbeLinkManagerTest, TerminalPostRejectionCancelsTheVerificationTodo) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion healthy{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 1),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto posted = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(posted.has_value());
    ASSERT_EQ(posted->record->key.version, healthy);
    plm.onPostResult(posted->record, ProbePostOutcome::kRejected, clock.now());

    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(plm.stats().probe_post_rejected, 1U);
    EXPECT_EQ(plm.stats().probe_post_rejected_retryable, 0U);
    EXPECT_EQ(plm.stats().probe_verification_abandoned, 0U);
    // This path's entry is closed rather than retried. The campaign may still
    // be alive for the other (faulty) sibling, which is the point: only the
    // terminal rejection's own obligation is dropped.
    EXPECT_EQ(healthOf(plm, healthy).task, TaskState::kIdle);
}

// F8: persistent retryable rejections are bounded. After the cap the retry
// obligation is abandoned explicitly and counted, so it no longer looks like a
// silent drop.
TEST(UbProbeLinkManagerTest, RetryableRejectionCapAbandonsAndCounts) {
    TestClock clock;
    auto params = defaultParams();
    params.post_rejection_cap = 3;
    ProbeLinkManager plm(params, clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    const uint32_t attempts = params.post_rejection_cap + 1;
    for (uint32_t i = 0; i < attempts; ++i) {
        clock.advance((plm.params().intervalNs() << 4) + 1);
        auto posted = postNext(plm, token_seq, clock.now());
        ASSERT_TRUE(posted.has_value()) << "attempt " << i;
        plm.onPostResult(posted->record, ProbePostOutcome::kRejectedRetryable,
                         clock.now());
        EXPECT_EQ(plm.inflightCount(), 0U);
    }
    EXPECT_EQ(plm.stats().probe_post_rejected_retryable, attempts);
    EXPECT_EQ(plm.stats().probe_verification_abandoned, 1U);
    EXPECT_TRUE(plm.campaignView().empty());
}

// F11: the completion may arrive before the post returns, but the record must
// not settle until both are known. The slot encodes "one in-flight probe per
// port", so releasing it early would start a second probe while the first is
// still inside the provider call.
TEST(UbProbeLinkManagerTest, ProbeGateHoldsTheSlotUntilPostReturns) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());

    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    EXPECT_EQ(plm.inflightCount(), 1U);
    EXPECT_NE(healthOf(plm, version).health, LinkHealth::kHealthy);

    // No second probe may be claimed on this port while the post is in flight:
    // the runtime stays kInflight, so the port offers no action at all.
    for (const auto& action : plm.pendingActions(clock.now())) {
        EXPECT_EQ(
            plm.beginPost(action, makeProbeToken(++token_seq), clock.now()),
            nullptr);
    }
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());

    // The post result is the other half of the evidence: settle exactly once.
    plm.onPostResult(probe->record, ProbePostOutcome::kAccepted, clock.now());
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().probe_posted, 1U);
    EXPECT_EQ(plm.stats().probe_post_conflict, 0U);
}

TEST(UbProbeLinkManagerTest, ProbeGateCompletionOnlyNeverSettles) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    // No post result ever arrives: only the timeout/shutdown path may take
    // this record over; the CQE alone is not permission to settle.
    EXPECT_EQ(plm.inflightCount(), 1U);
    // D2: the scanner must not fence a record whose post is still inside the
    // provider, so even past its deadline it stays inflight (and pinned).
    clock.advance(plm.params().timeoutNs() + 1);
    plm.scanTimeouts(clock.now());
    EXPECT_EQ(plm.inflightCount(), 1U);
    EXPECT_EQ(plm.stats().probe_timeout_unknown, 0U);

    // Shutdown is the owner of last resort: it hands the unproven record off
    // without ever fencing the in-flight post.
    plm.stopPosting();
    const auto drained = plm.drainBeforePollStop(clock.now());
    EXPECT_EQ(drained.unproven, 1U);
    EXPECT_EQ(plm.inflightCount(), 0U);
}

// F11: a CQE that arrived first and a later terminal reject are contradictory.
// The conflict is counted once and the resources are released once.
TEST(UbProbeLinkManagerTest, ProbeGateRejectedAfterCompletionSettlesOnce) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    ASSERT_EQ(plm.inflightCount(), 1U);

    plm.onPostResult(probe->record, ProbePostOutcome::kRejected, clock.now());
    EXPECT_EQ(plm.stats().probe_post_conflict, 1U);
    EXPECT_EQ(plm.stats().probe_post_rejected, 0U);
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(plm.stats().probe_slot_anomaly, 0U);
    // The completion was real evidence; health follows it, not the reject.
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
}

// F12: a replacement event without a drain receipt for the previous generation
// must fail closed: the port stays unusable instead of publishing a new handle
// whose predecessor may still have WRs in flight.
TEST(UbProbeLinkManagerTest, ReplacementWithoutDrainReceiptKeepsPortBroken) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    ProbeEvent event;
    event.kind = ProbeEventKind::kProbeHandleReplaced;
    event.graph_epoch = 1;
    event.event_seq = 1;
    ProbePortSupply replacement = makePortSupply(0, 2);
    replacement.previous_generation = 1;
    replacement.previous_drained = false;
    event.port = replacement;
    EXPECT_TRUE(plm.onPoolAvailabilityChange(event).IsInternalError());
    EXPECT_EQ(plm.stats().probe_replacement_unproven, 1U);
    for (const auto& entry : plm.healthView()) {
        EXPECT_TRUE(entry.pending);
        EXPECT_EQ(entry.pending_reason, PendingReason::kReplacementUnproven);
    }
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());
}

TEST(UbProbeLinkManagerTest, ReplacementWithDrainReceiptAdvances) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    ProbeEvent event;
    event.kind = ProbeEventKind::kProbeHandleReplaced;
    event.graph_epoch = 1;
    event.event_seq = 1;
    ProbePortSupply replacement = makePortSupply(0, 2);
    replacement.previous_generation = 1;
    replacement.previous_drained = true;
    event.port = replacement;
    ASSERT_TRUE(plm.onPoolAvailabilityChange(event).ok());
    EXPECT_EQ(plm.stats().probe_replacement_unproven, 0U);
    // The new generation is usable; the path is not left pending.
    EXPECT_FALSE(healthOf(plm, version).pending);
}

TEST(UbProbeLinkManagerTest, FirstPublicationNeedsNoDrainReceipt) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    ProbeEvent event;
    event.kind = ProbeEventKind::kProbeHandleReplaced;
    event.graph_epoch = 1;
    event.event_seq = 1;
    ProbePortSupply replacement = makePortSupply(0, 2);
    replacement.previous_generation = 0;  // first publication
    replacement.previous_drained = false;
    event.port = replacement;
    ASSERT_TRUE(plm.onPoolAvailabilityChange(event).ok());
    EXPECT_EQ(plm.stats().probe_replacement_unproven, 0U);
}

// D2/F11: the probe deadline is armed in beginPost(), i.e. before the provider
// call. If the native post overruns probe/timeout_ms, the scan must NOT retire
// the lane and hand the Jetty still inside adapter post() to quiesce/delete.
TEST(UbProbeLinkManagerTest, TimeoutScanWaitsForThePostToReturn) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());

    // The post is still inside the provider; the deadline has passed.
    clock.advance(plm.params().timeoutNs() + 1);
    plm.scanTimeouts(clock.now());
    EXPECT_EQ(plm.inflightCount(), 1U);
    EXPECT_EQ(plm.stats().probe_timeout_unknown, 0U);
    // The port is silently pinned by the stuck post; make that observable.
    EXPECT_EQ(plm.stats().probe_timeout_deferred_by_post_return, 1U);
    EXPECT_TRUE(reports.retire_calls.empty());

    // The post returns, its deadline is re-armed, and only then may the scan
    // take the record over.
    plm.onPostResult(probe->record, ProbePostOutcome::kAccepted, clock.now());
    clock.advance(plm.params().timeoutNs() + 1);
    plm.scanTimeouts(clock.now());
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(plm.stats().probe_timeout_unknown, 1U);
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
