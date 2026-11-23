// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Asynchronous pooled-Jetty drain chain tests
// (design docs/analyze/tent-ub-async-drain-design.md §11.1).
//
// Everything here is deterministic: the fake adapter answers every drain
// primitive from injected state, the control executor runs tasks only when the
// test says so, and the clock is a counter.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/control_executor.h"
#include "tent/transport/ub/jetty_drain_controller.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/tombstone.h"
#include "ub_fake_urma.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

// The control executor never runs a task behind the test's back: immediate
// tasks are drained by pump(), the re-armed tick is fired explicitly.
class ManualControlExecutor final : public ControlExecutor {
   public:
    Status submit(Task task) override {
        if (reject_submits_) {
            return Status::TooManyRequests("injected control queue full");
        }
        immediate_.push_back(std::move(task));
        return Status::OK();
    }
    Status scheduleAfter(uint64_t, Task task) override {
        delayed_.push_back(std::move(task));
        return Status::OK();
    }
    bool running() const override { return true; }
    void setRejectSubmits(bool reject) { reject_submits_ = reject; }

    size_t pumpImmediate() {
        size_t ran = 0;
        while (!immediate_.empty()) {
            auto task = std::move(immediate_.front());
            immediate_.pop_front();
            if (task) task();
            ++ran;
        }
        return ran;
    }
    size_t fireTimers() {
        std::deque<Task> timers = std::move(delayed_);
        delayed_.clear();
        size_t ran = 0;
        for (auto& task : timers) {
            if (task) task();
            ++ran;
        }
        return ran;
    }
    size_t pendingImmediate() const { return immediate_.size(); }
    size_t pendingTimers() const { return delayed_.size(); }

   private:
    std::deque<Task> immediate_;
    std::deque<Task> delayed_;
    bool reject_submits_{false};
};

// One pooled member with its drain chain attached to a manual executor.
class DrainHarness {
   public:
    explicit DrainHarness(bool async_drain = true,
                          bool with_controller = true) {
        adapter_ = std::make_shared<fake::FakeUrmaAdapter>(fake::fakeDevice());
        (void)adapter_->initialize();
        adapter_->setAutoBindCreatedJetties(true);
        (void)adapter_->openContext(fake::fakeDevice(), context_);
        JfcPtr handle;
        (void)adapter_->createJfc(context_, JfcOptions{}, handle);
        jfc_handle_ = handle;
        jfcs_.push_back(std::make_shared<UbJfc>(0, adapter_, handle));
        credits_ = std::make_shared<JfcCreditLedger>();
        (void)credits_->install(0, 16);
        budget_ = std::make_shared<NativeBudget>();
        (void)budget_->configure(kDeviceKey, /*verified=*/64,
                                 /*configured=*/8,
                                 /*replacement_reserve=*/2);
        tombstones_ = std::make_shared<TombstoneTable>(32);

        SendJettyPoolOptions pool_options;
        pool_options.desired_serving = 1;
        pool_options.max_creating = 1;
        pool_options.retire_timeout_ms = 1000;
        pool_options.cooldown_ms = 1;
        pool_options.refill_interval_ms = 1;
        pool_options.quarantine_budget = 64;
        pool_options.tombstones = tombstones_;
        pool_options.async_drain = async_drain;
        pool_options.drain_flush_batch = 8;
        pool_options.drain_quarantine_threshold = 3;
        pool_ = std::make_shared<SendJettyPool>(adapter_, context_, jfcs_,
                                                    budget_, kDeviceKey,
                                                    credits_, pool_options);
        if (with_controller) attachController();
    }

    ~DrainHarness() {
        pool_->beginStop();
        (void)pool_->drainIdle();
        (void)pool_->drainQuarantined();
    }

    [[nodiscard]] SendJettyPool& pool() { return *pool_; }
    [[nodiscard]] fake::FakeUrmaAdapter& adapter() { return *adapter_; }
    [[nodiscard]] std::shared_ptr<fake::FakeUrmaAdapter> adapterPtr() {
        return adapter_;
    }
    [[nodiscard]] NativeBudget& budget() { return *budget_; }
    [[nodiscard]] TombstoneTable& tombstones() { return *tombstones_; }
    [[nodiscard]] JettyDrainController& controller() { return *controller_; }
    [[nodiscard]] const JfcPtr& jfcHandle() const { return jfc_handle_; }
    [[nodiscard]] DrainStats drainStats() const { return controller_->stats(); }
    [[nodiscard]] const std::vector<uint64_t>& settledTokens() const {
        return settled_tokens_;
    }
    [[nodiscard]] size_t quarantineCallbacks() const {
        return quarantine_callbacks_;
    }

    void setConverged(bool converged) { converged_ = converged; }
    void setSettleReturns(bool returns) { settle_returns_ = returns; }
    void advance(uint64_t delta_ns) { now_ns_ += delta_ns; }
    [[nodiscard]] uint64_t now() const { return now_ns_; }

    // Runs every queued control task. A full chain resolves in one pump when
    // the provider answers synchronously (fake adapter).
    void pump() { (void)executor_.pumpImmediate(); }
    // Manual tick at the current fake time, then any task it queues.
    void tick() {
        if (controller_ != nullptr) controller_->tick(now_ns_);
        pump();
    }
    // Fires the re-armed drain timers (the timeout/retry path).
    void fireTimers() {
        (void)executor_.fireTimers();
        pump();
    }
    [[nodiscard]] size_t pendingTimers() const {
        return executor_.pendingTimers();
    }

    static PoolKey key() {
        PoolKey pool_key;
        pool_key.context_incarnation = 1;
        pool_key.trans_mode = kUrmaTransportModeRc;
        pool_key.priority = 15;
        return pool_key;
    }

    // Takes a lease, records the native id, then retires the member through the
    // same call the workers make on a real error completion.
    Status retireFaulted(bool fence_hardware = true) {
        LeaseWithCredit grant;
        const Status leased =
            pool_->tryLease(key(), AttemptKey{1, next_slice_++, 1}, grant);
        if (!leased.ok()) return leased;
        if (grant.lease.valid()) {
            last_jetty_id_ = grant.lease.lane().jetty_id;
            last_lane_ = grant.lease.lane();
        }
        const Status retired = pool_->retireFaulted(
            std::move(grant.lease), "cqe error", fence_hardware);
        credits_->release(grant.jfc_index, grant.jfc_credit_token);
        return retired;
    }

    [[nodiscard]] uint32_t lastJettyId() const { return last_jetty_id_; }
    void setRejectDrainScheduling(bool reject) {
        executor_.setRejectSubmits(reject);
    }
    [[nodiscard]] const PoolLaneKey& lastLane() const { return last_lane_; }

    static Completion marker(uint32_t jetty_id, bool flush_done = true) {
        return fake::FakeUrmaAdapter::makeEntityMarker(jetty_id, flush_done);
    }

    static constexpr const char* kDeviceKey = "fake0:/fake/fake0:eid0";

   private:
    void attachController() {
        DrainOptions options;
        options.retire_timeout_ms = 1000;
        options.flush_batch = 8;
        options.tick_interval_ms = 1;
        options.quarantine_threshold = 3;
        options.now_fn = [this] { return now_ns_; };
        std::weak_ptr<SendJettyPool> weak_pool = pool_;
        DrainCallbacks callbacks;
        callbacks.resolveJetty = [weak_pool](const PoolLaneKey& lane) {
            auto pool = weak_pool.lock();
            return pool ? pool->drainJettyFor(lane) : JettyPtr{};
        };
        callbacks.markDeleteReady = [weak_pool](const PoolLaneKey& lane,
                                                uint64_t error_epoch) {
            auto pool = weak_pool.lock();
            if (pool) {
                (void)pool->markDrainDeleteReady(lane, error_epoch);
            }
        };
        callbacks.quarantine = [this, weak_pool](const PoolLaneKey& lane,
                                                 const std::string& reason) {
            ++quarantine_callbacks_;
            auto pool = weak_pool.lock();
            if (pool) (void)pool->quarantineDrain(lane, reason);
        };
        callbacks.settleDrainedWr = [this](const Completion& completion,
                                           uint64_t) {
            settled_tokens_.push_back(completion.token);
            return settle_returns_;
        };
        callbacks.responsibilityConverged = [this](const LaneRefKey&) {
            return converged_;
        };
        controller_ = std::make_shared<JettyDrainController>(
            adapter_, tombstones_.get(), &executor_, std::move(callbacks),
            options);
        pool_->attachDrainController(controller_);
    }

    std::shared_ptr<fake::FakeUrmaAdapter> adapter_;
    ContextPtr context_;
    JfcPtr jfc_handle_;
    std::vector<std::shared_ptr<UbJfc>> jfcs_;
    std::shared_ptr<JfcCreditLedger> credits_;
    std::shared_ptr<NativeBudget> budget_;
    std::shared_ptr<TombstoneTable> tombstones_;
    std::shared_ptr<SendJettyPool> pool_;
    std::shared_ptr<JettyDrainController> controller_;
    ManualControlExecutor executor_;
    uint64_t now_ns_{1'000'000'000ULL};
    uint64_t next_slice_{1};
    uint32_t last_jetty_id_{0};
    PoolLaneKey last_lane_{};
    bool converged_{true};
    bool settle_returns_{true};
    std::vector<uint64_t> settled_tokens_;
    size_t quarantine_callbacks_{0};
};

// ---------------------------------------------------------------------------
// Normal chain
// ---------------------------------------------------------------------------

TEST(UbJettyDrainControllerTest, NormalChainReachesDeleteReady) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    ASSERT_NE(jetty_id, 0U);

    // Registration is non-blocking: no provider call happened yet and the
    // member is visible as draining, never as serving.
    EXPECT_EQ(harness.adapter().modifyErrorCalls(), 0U);
    EXPECT_EQ(harness.pool().stats().draining_members, 1U);
    EXPECT_EQ(harness.pool().stats().idle_members, 0U);
    EXPECT_EQ(harness.pool().stats().in_use_members, 0U);
    EXPECT_EQ(harness.drainStats().drain_begin, 1U);
    EXPECT_EQ(harness.drainStats().drain_pending, 1U);

    // Control step 1: modify ERROR. A new epoch is recorded and no marker is
    // accepted before it returns.
    harness.pump();
    EXPECT_EQ(harness.adapter().modifyErrorCalls(), 1U);
    EXPECT_EQ(harness.adapter().lastModifyEpoch(), 1U);
    EXPECT_EQ(harness.drainStats().modify_error_ok, 1U);
    EXPECT_EQ(harness.adapter().flushJettyCalls(), 0U);

    // Poller step: the flush-done marker for this (jfc, native id).
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    EXPECT_EQ(harness.drainStats().marker_flush_done, 1U);

    // Control steps 4-5: software flush, convergence, finish, DELETE_READY.
    harness.pump();
    EXPECT_GE(harness.adapter().flushJettyCalls(), 1U);
    EXPECT_EQ(harness.adapter().finishDrainCalls(), 1U);
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
    EXPECT_EQ(harness.drainStats().drain_pending, 0U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 0U);
    EXPECT_EQ(harness.pool().stats().delete_consumed, 1U);
    EXPECT_EQ(harness.pool().stats().draining_members, 0U);

    // Budget is released only after the provider proved destruction, and the
    // tombstone is erased with it.
    const auto snapshot = harness.budget().snapshot(DrainHarness::kDeviceKey);
    EXPECT_EQ(snapshot.total_jetties, 0U);
    EXPECT_EQ(harness.tombstones().size(), 0U);
}

TEST(UbJettyDrainControllerTest, MarkerDuringModifyIsAccepted) {
    DrainHarness harness;
    // The provider enters ERROR (and produces the marker) before
    // modifyJettyError() returns. Delivering the marker from inside the call
    // reproduces the exact window: modify_started is set, modify_done is not.
    harness.adapter().setModifyGate([&harness](uint32_t jetty_id, uint64_t) {
        EXPECT_TRUE(harness.controller().onEntityEvent(
            DrainHarness::marker(jetty_id), 0));
    });
    ASSERT_TRUE(harness.retireFaulted().ok());
    harness.pump();

    EXPECT_EQ(harness.drainStats().marker_before_error, 0U);
    EXPECT_EQ(harness.drainStats().marker_flush_done, 1U);
    EXPECT_EQ(harness.drainStats().drain_quarantined, 0U);
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 0U);
    harness.adapter().clearModifyGate();
}

TEST(UbJettyDrainControllerTest, MarkerBeforeModifyQuarantines) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();

    // The marker arrives before the control executor ran modify ERROR: the
    // provider (or a previous epoch) produced it early, so it can never be
    // accepted as proof for this epoch.
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    EXPECT_EQ(harness.drainStats().marker_before_error, 1U);

    harness.pump();
    EXPECT_EQ(harness.drainStats().drain_quarantined, 1U);
    EXPECT_EQ(harness.adapter().modifyErrorCalls(), 0U);
    EXPECT_EQ(harness.adapter().flushJettyCalls(), 0U);
    EXPECT_EQ(harness.pool().stats().quarantined_members, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
    const auto snapshot = harness.budget().snapshot(DrainHarness::kDeviceKey);
    EXPECT_EQ(snapshot.total_jetties, 1U);
}

TEST(UbJettyDrainControllerTest, DuplicateMarkerOnlyCounts) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();

    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    EXPECT_EQ(harness.drainStats().marker_flush_done, 2U);
    EXPECT_EQ(harness.drainStats().marker_dup, 1U);
    harness.pump();
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
}

TEST(UbJettyDrainControllerTest, SuspendDoneIsRecordedOnly) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();

    EXPECT_TRUE(harness.controller().onEntityEvent(
        DrainHarness::marker(jetty_id, /*flush_done=*/false), 0));
    EXPECT_EQ(harness.drainStats().marker_suspend_done, 1U);
    EXPECT_EQ(harness.drainStats().marker_flush_done, 0U);
    harness.pump();
    EXPECT_EQ(harness.adapter().flushJettyCalls(), 0U);

    // The real marker still advances the chain afterwards.
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
}

TEST(UbJettyDrainControllerTest, EntityEventOnDrainingJettyQuarantines) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    // A non-marker entity-level record for the draining Jetty: the legacy path
    // still sees it (false), but responsibility is unproven, so the drain must
    // not continue to DELETE_READY.
    Completion event;
    event.token = 0;
    event.local_jetty_id = jetty_id;
    event.category = CompletionCategory::UNKNOWN_ERROR;
    event.native_status = 0x55;
    EXPECT_FALSE(harness.controller().onEntityEvent(event, 0));
    EXPECT_EQ(harness.drainStats().entity_event_on_drain, 1U);
    harness.pump();
    EXPECT_EQ(harness.drainStats().drain_quarantined, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
    // An entity event is the most typical local anomaly (AE storm). It must
    // never trip the §9 provider-capability brake.
    EXPECT_TRUE(harness.drainStats().async_drain_supported);
}

TEST(UbJettyDrainControllerTest, MarkerOnAnotherJfcIsNotAttributed) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    // The same native id on a different JFC is another polling slot: the marker
    // is attributed to the tombstoned entity (stale) and must never advance
    // this drain.
    EXPECT_TRUE(harness.controller().onEntityEvent(
        DrainHarness::marker(jetty_id), /*jfc_index=*/1));
    EXPECT_EQ(harness.drainStats().marker_stale, 1U);
    EXPECT_EQ(harness.drainStats().marker_flush_done, 0U);
    EXPECT_EQ(harness.adapter().flushJettyCalls(), 0U);
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
}

TEST(UbJettyDrainControllerTest, MarkerForUnownedJettyIsNotConsumed) {
    DrainHarness harness;
    // No drain was registered and no tombstone exists: the record belongs to
    // the legacy unknown-token path, so the controller must not claim it.
    EXPECT_FALSE(harness.controller().onEntityEvent(
        DrainHarness::marker(/*jetty_id=*/77), 0));
}

// ---------------------------------------------------------------------------
// Software flush and settlement
// ---------------------------------------------------------------------------

TEST(UbJettyDrainControllerTest, DrainedWrSettlesExactlyOnce) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();

    constexpr uint64_t kToken = 0x777;
    harness.adapter().pushFlushCompletions(
        harness.pool().drainJettyFor(harness.lastLane()),
        {Completion{CompletionCategory::ENDPOINT_ERROR, 0, kToken, 0,
                    jetty_id}});
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();

    ASSERT_EQ(harness.settledTokens().size(), 1U);
    EXPECT_EQ(harness.settledTokens()[0], kToken);
    EXPECT_EQ(harness.drainStats().software_flush_wrs, 1U);
    EXPECT_EQ(harness.drainStats().drained_after_settle, 0U);
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
}

TEST(UbJettyDrainControllerTest, AlreadySettledDrainedWrIsCountedNotReleased) {
    DrainHarness harness;
    harness.setSettleReturns(false);
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    harness.adapter().pushFlushCompletions(
        harness.pool().drainJettyFor(harness.lastLane()),
        {Completion{CompletionCategory::ENDPOINT_ERROR, 0, 0x888, 0,
                    jetty_id}});
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();
    EXPECT_EQ(harness.drainStats().drained_after_settle, 1U);
    // A drained WR never changes the delete verdict by itself.
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
}

TEST(UbJettyDrainControllerTest, FlushReturningEntityMarkerQuarantines) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    harness.adapter().pushFlushCompletions(
        harness.pool().drainJettyFor(harness.lastLane()),
        {Completion{CompletionCategory::ENDPOINT_ERROR, 0, 0, 0, jetty_id}});
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();
    EXPECT_EQ(harness.adapter().flushMarkerErrors(), 1U);
    EXPECT_EQ(harness.drainStats().software_flush_unhandled, 1U);
    EXPECT_EQ(harness.drainStats().drain_quarantined, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
}

TEST(UbJettyDrainControllerTest, FlushFailureQuarantines) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    harness.adapter().failNextFlushJetty();
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();
    EXPECT_EQ(harness.adapter().flushJettyFailures(), 1U);
    EXPECT_EQ(harness.drainStats().drain_quarantined, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
}

// ---------------------------------------------------------------------------
// modify / timeout
// ---------------------------------------------------------------------------

TEST(UbJettyDrainControllerTest, ModifyFailureQuarantines) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    harness.adapter().failNextModifyError();
    harness.pump();
    EXPECT_EQ(harness.adapter().modifyErrorFailures(), 1U);
    EXPECT_EQ(harness.drainStats().modify_error_fail, 1U);
    EXPECT_EQ(harness.drainStats().drain_quarantined, 1U);
    EXPECT_EQ(harness.adapter().modifyErrorCalls(), 0U);
    EXPECT_EQ(harness.pool().stats().quarantined_members, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
    // A single modify failure is not the provider capability verdict.
    EXPECT_TRUE(harness.drainStats().async_drain_supported);
}

TEST(UbJettyDrainControllerTest, MissingMarkerTimesOutAndLateMarkerIsStale) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    EXPECT_EQ(harness.drainStats().modify_error_ok, 1U);

    // The timeout is responsibility-unknown, never a delete permission.
    harness.advance(1'000'000'001ULL);
    harness.tick();
    EXPECT_EQ(harness.drainStats().drain_timeout, 1U);
    EXPECT_EQ(harness.drainStats().drain_quarantined, 1U);
    EXPECT_EQ(harness.adapter().finishDrainCalls(), 0U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);

    // The late marker now belongs to a tombstoned entity: counted as stale and
    // consumed so it never reaches the unknown-token path.
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    EXPECT_EQ(harness.drainStats().marker_stale, 1U);
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 0U);
}

TEST(UbJettyDrainControllerTest, ResponsibilityNotConvergedBlocksDelete) {
    DrainHarness harness;
    harness.setConverged(false);
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();
    // Convergence gates the finish; without it the drain keeps its owner.
    EXPECT_EQ(harness.adapter().finishDrainCalls(), 0U);
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 0U);
    EXPECT_EQ(harness.drainStats().drain_pending, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);

    harness.setConverged(true);
    harness.tick();
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 0U);
}

TEST(UbJettyDrainControllerTest, RepeatedDrainRequestIsIdempotent) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    DrainRequest request;
    request.lane = harness.lastLane();
    request.error_epoch = 1;
    request.jfc_index = 0;
    request.post_gate_closed = true;
    // Same member generation: the second registration is a no-op instead of a
    // second drain over one native entity.
    EXPECT_TRUE(harness.controller().beginDrain(request).ok());
    EXPECT_EQ(harness.drainStats().drain_begin, 1U);
}

TEST(UbJettyDrainControllerTest, DrainRequestRequiresClosedPostGate) {
    DrainHarness harness;
    DrainRequest request;
    request.lane = PoolLaneKey{42, 1};
    request.error_epoch = 1;
    request.jfc_index = 0;
    request.post_gate_closed = false;
    EXPECT_TRUE(harness.controller().beginDrain(request).IsInvalidArgument());
}

// ---------------------------------------------------------------------------
// Shutdown / fail-closed
// ---------------------------------------------------------------------------

TEST(UbJettyDrainControllerTest, BeginDrainAfterStopQuarantines) {
    DrainHarness harness;
    // Shutdown typed the controller into its synchronous branch with no entry.
    harness.controller().drainSynchronously();
    ASSERT_TRUE(harness.retireFaulted().ok());
    EXPECT_EQ(harness.adapter().modifyErrorCalls(), 0U);
    EXPECT_EQ(harness.pool().stats().quarantined_members, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
}

TEST(UbJettyDrainControllerTest, SynchronousFallbackDrainsOnShutdown) {
    DrainHarness harness;
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    EXPECT_EQ(harness.drainStats().modify_error_ok, 1U);

    // Shutdown degrades the in-flight entry to the blocking quiesce fence; the
    // fake answers like a real provider (no WRs left behind).
    harness.controller().drainSynchronously();
    EXPECT_GE(harness.adapter().quiesceCalls(), 1U);
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
    EXPECT_EQ(harness.drainStats().drain_pending, 0U);
    (void)jetty_id;
    EXPECT_EQ(harness.adapter().liveJettyCount(), 0U);
}

TEST(UbJettyDrainControllerTest,
     SyncFallbackQuarantinesUnconvergedResponsibility) {
    DrainHarness harness;
    harness.setConverged(false);
    ASSERT_TRUE(harness.retireFaulted().ok());
    harness.pump();
    harness.controller().drainSynchronously();
    EXPECT_EQ(harness.drainStats().drain_quarantined, 1U);
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 0U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
}

TEST(UbJettyDrainControllerTest, AsyncDisabledUsesSynchronousFence) {
    DrainHarness harness(/*async_drain=*/false, /*with_controller=*/false);
    ASSERT_TRUE(harness.retireFaulted().ok());
    // The legacy fence is the same code path that shipped before the chain:
    // one blocking quiesce and no drain primitive.
    EXPECT_GE(harness.adapter().quiesceCalls(), 1U);
    EXPECT_EQ(harness.adapter().modifyErrorCalls(), 0U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 0U);
    EXPECT_EQ(harness.pool().stats().delete_consumed, 1U);
}

// Design §12 step 1: the asynchronous chain performs the same steps as the
// synchronous quiesce, one primitive per stage instead of one call.
TEST(UbJettyDrainControllerTest, AsyncChainStepsMatchSynchronousFence) {
    DrainHarness sync_harness(/*async_drain=*/false, /*with_controller=*/false);
    ASSERT_TRUE(sync_harness.retireFaulted().ok());
    const uint64_t sync_quiesce_calls = sync_harness.adapter().quiesceCalls();
    ASSERT_GE(sync_quiesce_calls, 1U);

    DrainHarness async_harness;
    ASSERT_TRUE(async_harness.retireFaulted().ok());
    const uint32_t jetty_id = async_harness.lastJettyId();
    async_harness.pump();
    EXPECT_TRUE(async_harness.controller().onEntityEvent(
        DrainHarness::marker(jetty_id), 0));
    async_harness.pump();

    EXPECT_EQ(async_harness.adapter().modifyErrorCalls(), 1U);
    EXPECT_EQ(async_harness.adapter().finishDrainCalls(), 1U);
    EXPECT_GE(async_harness.adapter().flushJettyCalls(), 1U);
    // The hot path no longer pays for a blocking quiesce.
    EXPECT_EQ(async_harness.adapter().quiesceCalls(), 0U);
}

TEST(UbJettyDrainControllerTest,
     EntityEventQuarantineDoesNotDisableAsyncDrain) {
    DrainHarness harness;
    // Three entity-event quarantines are local anomalies, not proof that the
    // provider cannot drain: they must not trip the §9 capability brake.
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(harness.retireFaulted().ok());
        const uint32_t jetty_id = harness.lastJettyId();
        harness.pump();
        Completion event;
        event.token = 0;
        event.local_jetty_id = jetty_id;
        event.category = CompletionCategory::UNKNOWN_ERROR;
        event.native_status = 0x55;
        EXPECT_FALSE(harness.controller().onEntityEvent(event, 0));
        harness.pump();
        EXPECT_EQ(harness.drainStats().drain_quarantined,
                  static_cast<uint64_t>(i + 1));
    }
    EXPECT_EQ(harness.drainStats().entity_event_on_drain, 3U);
    EXPECT_TRUE(harness.drainStats().async_drain_supported);
    EXPECT_TRUE(harness.controller().asyncDrainSupported());
    EXPECT_EQ(harness.drainStats().async_drain_disabled, 0U);

    // The chain still works for the next genuine fault.
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();
    EXPECT_EQ(harness.drainStats().drain_delete_ready, 1U);
}

// ---------------------------------------------------------------------------
// Delete outcomes (§8)
// ---------------------------------------------------------------------------

TEST(UbJettyDrainControllerTest, RetainedDeleteKeepsOwnerAndRetries) {
    DrainHarness harness;
    harness.adapter().queueDeleteOutcome(DeleteOutcome::Kind::kRetained);
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();

    EXPECT_EQ(harness.pool().stats().delete_retained, 1U);
    EXPECT_EQ(harness.pool().stats().delete_consumed, 0U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
    // Responsibility is not released: the tombstone and the budget stay.
    EXPECT_EQ(harness.tombstones().size(), 1U);
    auto snapshot = harness.budget().snapshot(DrainHarness::kDeviceKey);
    EXPECT_EQ(snapshot.total_jetties, 1U);

    // After the (real-clock) backoff the retry consumes the handle. The pool
    // schedules delete retries on the steady clock, so wait it out with a
    // bounded poll instead of pretending the fake clock drives it.
    size_t retried = 0;
    for (int attempt = 0; attempt < 200 && retried == 0; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        retried = harness.pool().retryRetainedDeletes();
    }
    EXPECT_EQ(retried, 1U);
    EXPECT_EQ(harness.pool().stats().delete_consumed, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 0U);
    snapshot = harness.budget().snapshot(DrainHarness::kDeviceKey);
    EXPECT_EQ(snapshot.total_jetties, 0U);
    EXPECT_EQ(harness.tombstones().size(), 0U);
}

TEST(UbJettyDrainControllerTest, UnknownDeleteQuarantinesWithBudget) {
    DrainHarness harness;
    harness.adapter().queueDeleteOutcome(DeleteOutcome::Kind::kUnknown);
    ASSERT_TRUE(harness.retireFaulted().ok());
    const uint32_t jetty_id = harness.lastJettyId();
    harness.pump();
    EXPECT_TRUE(
        harness.controller().onEntityEvent(DrainHarness::marker(jetty_id), 0));
    harness.pump();

    EXPECT_EQ(harness.pool().stats().delete_unknown, 1U);
    EXPECT_EQ(harness.pool().stats().quarantined_members, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
    EXPECT_EQ(harness.tombstones().size(), 1U);
    const auto snapshot = harness.budget().snapshot(DrainHarness::kDeviceKey);
    EXPECT_EQ(snapshot.total_jetties, 1U);
}

// ---------------------------------------------------------------------------
// Provider capability fail-closed (§9)
// ---------------------------------------------------------------------------

TEST(UbJettyDrainControllerTest, RepeatedDrainTimeoutsDisableAsyncDrain) {
    DrainHarness harness;
    // Three consecutive *provider* drain failures (a marker that never arrives)
    // are the documented threshold; the controller then refuses new drains
    // instead of retrying forever.
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(harness.retireFaulted().ok());
        harness.pump();
        harness.advance(1'000'000'001ULL);
        harness.tick();
        EXPECT_EQ(harness.drainStats().drain_timeout,
                  static_cast<uint64_t>(i + 1));
        EXPECT_EQ(harness.drainStats().drain_quarantined,
                  static_cast<uint64_t>(i + 1));
    }
    EXPECT_FALSE(harness.drainStats().async_drain_supported);

    // A further drain registration is refused with NotImplemented and
    // counted, so the caller quarantines instead of scheduling into a dead
    // chain.
    DrainRequest rejected;
    rejected.lane = harness.lastLane();
    rejected.error_epoch = 1;
    rejected.jfc_index = 0;
    rejected.post_gate_closed = true;
    EXPECT_TRUE(harness.controller().beginDrain(rejected).IsNotImplemented());
    EXPECT_EQ(harness.drainStats().async_drain_disabled, 1U);

    // Design §9.2: the runtime brake stops admitting new slices instead of
    // relying on the quarantine budget to notice.
    LeaseWithCredit blocked;
    EXPECT_TRUE(
        harness.pool()
            .tryLease(DrainHarness::key(), AttemptKey{1, 99, 1}, blocked)
            .IsTooManyRequests());
    EXPECT_TRUE(harness.pool().stats().drain_admission_stopped);
}

TEST(UbJettyDrainControllerTest, DrainSchedulingFailureIsCounted) {
    DrainHarness harness;
    // The control executor refuses the task: beginDrain() fails, the member is
    // quarantined, and the local backpressure is counted separately from a
    // provider failure.
    harness.setRejectDrainScheduling(true);
    ASSERT_TRUE(harness.retireFaulted().ok());
    EXPECT_EQ(harness.drainStats().drain_begin_failed, 1U);
    EXPECT_EQ(harness.drainStats().drain_begin, 1U);
    EXPECT_EQ(harness.pool().stats().quarantined_members, 1U);
    EXPECT_EQ(harness.adapter().liveJettyCount(), 1U);
    EXPECT_TRUE(harness.drainStats().async_drain_supported);
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
