// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3e integration: the real SendJettyPool on the real UbWorkers posting and
// polling paths, with the fake adapter standing in for URMA.
//
// This is the only test that proves the pooled ownership chain end to end:
// admission takes a lease, the tristate post rides the leased Jetty, the
// completion gate decides the exit, and onOwners() hands the member back
// exactly once while releasing the credit and the task K slot. The three risky
// interleavings of design §15.2 (early CQE, first error, timeout fence under a
// stopping pool) each have their own case.

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <thread>
#include <vector>

#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_registry.h"
#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/topology_attrs.h"
#include "tent/transport/ub/workers.h"
#include "ub_fake_topology.h"
#include "ub_fake_urma.h"

namespace mooncake::tent::ub {

// Declared as a friend by UbWorkers so the test can arm the thread-local
// enqueue-failure hook without a production setter.
class UbWorkersTestPeer {
   public:
    static void failSecondEnqueue() {
        UbWorkers::fail_second_enqueue_for_test_ = true;
    }
    // Forces the degraded fence path: no control executor, so scanTimeouts
    // executes the fence inline while it holds endpoint_drain_mutex_.
    static void dropControlExecutor(UbWorkers& workers) {
        workers.stopControlExecutor();
    }
    // The claim verdict is private, so exercise it through the friend rather
    // than widening the production API just for a test.
    static bool claimedWithEpochChange(bool path_epoch_changed) {
        UbWorkers::ClaimDecision decision;
        decision.closed = ClaimClosed::kNone;
        decision.path_epoch_changed = path_epoch_changed;
        return decision.claimed();
    }
};

namespace {

using namespace fake;

uint64_t steadyNow() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

// Deterministic control executor: the test decides when a queued fence or
// device-retirement job runs, so "the poller did not block" and "a refused job
// keeps its responsibility" are observable without racing a real thread.
class ManualControlExecutor final : public ControlExecutor {
   public:
    explicit ManualControlExecutor(size_t capacity = 64)
        : capacity_(capacity) {}

    Status submit(Task task) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (tasks_.size() >= capacity_) {
                return Status::TooManyRequests("manual control queue is full");
            }
            tasks_.push_back(std::move(task));
        }
        cv_.notify_all();
        return Status::OK();
    }

    Status scheduleAfter(uint64_t, Task task) override {
        return submit(std::move(task));
    }

    [[nodiscard]] bool running() const override { return true; }

    [[nodiscard]] size_t pending() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tasks_.size();
    }

    bool waitPending(size_t at_least, uint32_t timeout_ms = 3000) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                            [&] { return tasks_.size() >= at_least; });
    }

    size_t runAll() {
        size_t ran = 0;
        for (;;) {
            Task task;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (tasks_.empty()) break;
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            if (task) task();
            ++ran;
        }
        return ran;
    }

    void setCapacity(size_t capacity) {
        std::lock_guard<std::mutex> lock(mutex_);
        capacity_ = capacity;
    }

   private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<Task> tasks_;
    size_t capacity_;
};

// The fake device's budget ledger key (see installPooledSendPools()).
constexpr const char* kDeviceKey = "fake0:/fake/fake0:eid0";

// One pooled transport slice: context, buffers, segment manager, endpoint
// store, workers - everything a real install would build, minus the transport
// facade. The pool is attached directly, which is exactly what
// UbTransport::install does for runtime_profile=rm_pool.
class PooledDataPath {
   public:
    explicit PooledDataPath(
        uint32_t slice_timeout_ms = 5000, size_t desired_serving = 2,
        uint64_t credit_capacity = 8, bool rm_mode = false, uint32_t task_k = 2,
        uint64_t queue_budget = 65536, size_t jfc_count = 1,
        size_t jetty_per_endpoint = 1,
        std::shared_ptr<ControlExecutor> control_executor = nullptr,
        LocalDeviceFailureHandler device_failure_handler = {})
        : adapter_(std::make_shared<FakeUrmaAdapter>(fakeDevice())),
          context_(std::make_shared<UbContext>(0, fakeDevice(), adapter_)),
          slice_timeout_ms_(slice_timeout_ms),
          desired_serving_(desired_serving),
          credit_capacity_(credit_capacity),
          rm_mode_(rm_mode),
          task_k_(task_k),
          queue_budget_(queue_budget),
          jfc_count_(jfc_count),
          jetty_per_endpoint_(jetty_per_endpoint),
          control_executor_(std::move(control_executor)),
          device_failure_handler_(std::move(device_failure_handler)) {
        (void)adapter_->initialize();
        adapter_->setAutoBindCreatedJetties(true);

        for (size_t i = 0; i < source_.size(); ++i) {
            source_[i] = static_cast<char>(i + 1);
        }
        source_desc_.addr = reinterpret_cast<uint64_t>(source_.data());
        source_desc_.length = source_.size();
        source_desc_.location = kWildcardLocation;
        target_desc_.addr = reinterpret_cast<uint64_t>(target_.data());
        target_desc_.length = target_.size();
        target_desc_.location = kWildcardLocation;

        topology_ = fakeTopology();
    }

    bool start() {
        // B2 mirror: an RM JFS needs an RM JFR, so the context's JFC must be
        // created in the same transport mode as the pooled members.
        JfcOptions jfc_options;
        if (rm_mode_) jfc_options.transport_mode = JettyTransportMode::kRm;
        if (!context_
                 ->initialize(static_cast<uint32_t>(jfc_count_), jfc_options)
                 .ok()) {
            return false;
        }
        adapter_->setRmPostDeliversCompletion(rm_mode_);
        contexts_ = {context_};
        buffers_ = std::make_unique<UbBufferManager>(adapter_, contexts_);
        MemoryOptions options;
        options.perm = kGlobalReadWrite;
        if (!buffers_->addBuffer(source_desc_, options).ok()) return false;
        if (!buffers_->addBuffer(target_desc_, options).ok()) return false;

        manager_ =
            std::make_unique<SegmentManager>(std::make_unique<NullRegistry>());
        if (!manager_
                 ->updateLocal([&](SegmentDesc& segment) {
                     segment.name = "local";
                     segment.type = SegmentType::Memory;
                     segment.detail = MemorySegmentDesc{};
                     auto& memory = std::get<MemorySegmentDesc>(segment.detail);
                     memory.topology = *topology_;
                     memory.buffers = {source_desc_, target_desc_};
                     return Status::OK();
                 })
                 .ok()) {
            return false;
        }

        // ---- the pooled send resources -----------------------------------
        credits_ = std::make_shared<JfcCreditLedger>();
        for (const auto& jfc : context_->jfcs()) {
            if (!jfc) continue;
            if (!credits_->install(jfc->index(), credit_capacity_).ok()) {
                return false;
            }
        }
        budget_ = std::make_shared<NativeBudget>();
        if (!budget_
                 ->configure(kDeviceKey, /*verified=*/64,
                             /*configured=*/8, /*replacement_reserve=*/2)
                 .ok()) {
            return false;
        }
        SendJettyPoolOptions pool_options;
        pool_options.desired_serving = desired_serving_;
        pool_options.max_creating = 2;
        if (rm_mode_) {
            pool_options.jetty_options.transport_mode = JettyTransportMode::kRm;
        }
        pool_ = std::make_shared<SendJettyPool>(
            adapter_, context_->handle(), context_->jfcs(), budget_, kDeviceKey,
            credits_, pool_options);
        context_->attachPooledSendPool(pool_);

        JettyOptions endpoint_options;
        if (rm_mode_) endpoint_options.transport_mode = JettyTransportMode::kRm;
        endpoints_ = std::make_unique<EndpointStore>(
            adapter_, 16, static_cast<uint32_t>(jetty_per_endpoint_),
            endpoint_options);
        rails_ = std::make_unique<RailMonitor>();
        quota_ = std::make_unique<QuotaManager>();

        UbParams params;
        params.worker_count = 1;
        params.poller_count = 1;
        params.slice_size = 16;
        // Deterministic first-error cases: a retryable failure would retire the
        // first member and then succeed on a second attempt, which is correct
        // behavior but not what these interleaving tests pin.
        params.max_retries = 0;
        params.slice_timeout_ms = slice_timeout_ms_;
        params.runtime_profile =
            rm_mode_ ? UbRuntimeProfile::kRmPool : UbRuntimeProfile::kRcLegacy;
        params.task_max_inflight_attempts = task_k_;
        params.queue_budget = queue_budget_;
        params.jetty_pool.business_desired_serving =
            static_cast<uint32_t>(desired_serving_);

        EndpointResolver resolver = [this](
                                        const EndpointResolveRequest& request,
                                        std::shared_ptr<UbEndpoint>& out) {
            UbEndpointKey key{
                request.local_context->topologyId(), request.remote_segment_id,
                request.remote_topology_id, "local@ub:fake0:eid0"};
            auto status =
                endpoints_->getOrCreate(key, request.local_context, out);
            if (!status.ok() || out->ready()) return status;
            UbBootstrapDesc peer;
            peer.local_eid = fakeDevice().eid;
            peer.endpoint_generation = 100;
            peer.jetty_ids.clear();
            for (size_t i = 0; i < jetty_per_endpoint_; ++i) {
                // Stable per-slot peer Jetty ids: 777, 778, ...
                peer.jetty_ids.push_back(static_cast<uint32_t>(777 + i));
            }
            if (rm_mode_) {
                peer.capabilities = {kUbCapabilityRmTransport};
            }
            return out->bind(peer);
        };
        workers_ = std::make_unique<UbWorkers>(
            adapter_, contexts_, topology_, manager_.get(), buffers_.get(),
            rails_.get(), quota_.get(), params, std::move(resolver),
            [this](const std::shared_ptr<UbEndpoint>& endpoint) {
                (void)endpoints_->retire(endpoint);
            },
            device_failure_handler_, /*probe_manager=*/nullptr,
            std::vector<ProbeJfcSlot>{}, /*tombstones=*/nullptr,
            /*probe_topology=*/nullptr, control_executor_);
        return workers_->start().ok();
    }

    bool stop() {
        bool ok = true;
        if (workers_) ok = workers_->stop().ok() && ok;
        workers_.reset();
        if (endpoints_) ok = endpoints_->clear().ok() && ok;
        endpoints_.reset();
        if (buffers_) ok = buffers_->clear().ok() && ok;
        buffers_.reset();
        manager_.reset();
        if (context_) ok = context_->shutdown().ok() && ok;
        pool_.reset();
        ok = adapter_->shutdown().ok() && ok;
        return ok;
    }

    UbTask::Ptr makeTask(size_t slice_count = 1, uint32_t max_retries = 0,
                         uint64_t deadline_ns = 0) {
        Request request{};
        request.opcode = Request::WRITE;
        request.source = source_.data();
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = reinterpret_cast<uint64_t>(target_.data());
        request.length = 16 * slice_count;
        request.deadline_ns = deadline_ns;
        auto task = UbTask::create(request, {}, 0, task_k_);
        for (size_t i = 0; i < slice_count; ++i) {
            // max_retries = 0: the slice budget is per slice, so the transport
            // level setting above is not enough to make a first error terminal.
            (void)task->addSlice(UbSliceSpec{
                source_.data() + i * 16,
                reinterpret_cast<uint64_t>(target_.data() + i * 16), 16, i * 16,
                /*max_retries=*/0});
        }
        (void)task->seal();
        return task;
    }

    static bool waitFor(
        const std::function<bool()>& predicate,
        std::chrono::milliseconds timeout = std::chrono::seconds(3)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!predicate() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return predicate();
    }

    [[nodiscard]] UbWorkers& workers() { return *workers_; }
    [[nodiscard]] SendJettyPool& pool() { return *pool_; }
    [[nodiscard]] JfcCreditLedger& credits() { return *credits_; }
    [[nodiscard]] const std::shared_ptr<FakeUrmaAdapter>& adapter() const {
        return adapter_;
    }
    [[nodiscard]] std::array<char, 64>& source() { return source_; }
    [[nodiscard]] std::array<char, 64>& target() { return target_; }
    [[nodiscard]] NativeBudget& budget() { return *budget_; }
    [[nodiscard]] JfcPtr businessJfc() const {
        return context_->jfcs().empty() ? nullptr
                                        : context_->jfcs().front()->handle();
    }
    [[nodiscard]] bool rmMode() const { return rm_mode_; }
    [[nodiscard]] uint64_t contextIncarnation() const {
        return context_->incarnation();
    }

   private:
    std::shared_ptr<FakeUrmaAdapter> adapter_;
    std::shared_ptr<UbContext> context_;
    std::vector<UbContextPtr> contexts_;
    std::shared_ptr<Topology> topology_;
    std::unique_ptr<UbBufferManager> buffers_;
    std::unique_ptr<SegmentManager> manager_;
    std::unique_ptr<EndpointStore> endpoints_;
    std::unique_ptr<RailMonitor> rails_;
    std::unique_ptr<QuotaManager> quota_;
    std::unique_ptr<UbWorkers> workers_;
    std::shared_ptr<JfcCreditLedger> credits_;
    std::shared_ptr<NativeBudget> budget_;
    std::shared_ptr<SendJettyPool> pool_;

    std::shared_ptr<ControlExecutor> control_executor_;
    LocalDeviceFailureHandler device_failure_handler_;

    uint32_t slice_timeout_ms_{5000};
    size_t desired_serving_{2};
    uint64_t credit_capacity_{8};
    bool rm_mode_{false};
    uint32_t task_k_{2};
    uint64_t queue_budget_{65536};
    size_t jfc_count_{1};
    size_t jetty_per_endpoint_{1};
    std::array<char, 64> source_{};
    std::array<char, 64> target_{};
    BufferDesc source_desc_{};
    BufferDesc target_desc_{};
};

// Happy path: one attempt, one lease, one post, one return. Every pooled debit
// must be back to zero afterwards: the member is idle, the credit is released,
// the task K slot is free and the per-path ledger is balanced.
TEST(UbPoolIntegrationTest, SuccessfulPostReturnsTheMemberAndTheCredit) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    auto task = path.makeTask(/*slice_count=*/2);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s == COMPLETED; }));
    EXPECT_EQ(task->transferStatus().transferred_bytes, 32U);
    // Only the two submitted slices (32 bytes) were written; the tail of the
    // target buffer is intentionally untouched.
    EXPECT_TRUE(std::equal(path.source().begin(), path.source().begin() + 32,
                           path.target().begin()));

    // Return, not retire: the members stay in circulation.
    EXPECT_TRUE(PooledDataPath::waitFor([&] {
        const auto stats = path.pool().stats();
        return stats.total_returns == 2 && stats.in_use_members == 0 &&
               stats.total_leases == 2;
    }));
    const auto pool_stats = path.pool().stats();
    EXPECT_EQ(pool_stats.double_returns, 0U);
    EXPECT_EQ(pool_stats.leaked_leases, 0U);
    EXPECT_EQ(pool_stats.stop_rejections, 0U);
    EXPECT_GE(pool_stats.idle_members, 1U);
    EXPECT_EQ(pool_stats.pool_misses, 0U);

    // Credit: reserved by the lease, released by the settlement path.
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    // K: released with the owner record, so the task's fan-out is back to zero.
    EXPECT_EQ(task->inflightAttempts(), 0U);

    const auto business = path.workers().businessStats();
    EXPECT_EQ(business.pooled_return, 2U);
    EXPECT_EQ(business.pooled_retire, 0U);
    EXPECT_EQ(business.pool_lease_attempts, 2U);
    EXPECT_EQ(business.pool_lease_misses, 0U);
    EXPECT_EQ(business.task_K_wait, 0U);
    EXPECT_EQ(business.claim_after_isolation, 0U);
    EXPECT_EQ(business.path_load_imbalance, 0U);
    EXPECT_GT(business.path_inflight_peak, 0U);
    EXPECT_EQ(business.pool_return_double, 0U);
    EXPECT_EQ(path.workers().completionStats().one_shot_violations, 0U);

    EXPECT_TRUE(path.stop());
}

// First real error: the member is retired (INV07), never reused, and the
// endpoint is not blamed for a pooled failure.
TEST(UbPoolIntegrationTest, FirstErrorRetiresTheMember) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    path.adapter()->failNextCompletion(CompletionCategory::REMOTE_PATH_ERROR);
    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));

    EXPECT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().total_returns == 1; }));
    const auto pool_stats = path.pool().stats();
    EXPECT_EQ(pool_stats.in_use_members, 0U);
    // A retired member leaves the serving set entirely.
    EXPECT_EQ(pool_stats.idle_members, 0U);
    EXPECT_EQ(pool_stats.leaked_leases, 0U);
    EXPECT_EQ(pool_stats.double_returns, 0U);
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_EQ(task->inflightAttempts(), 0U);

    const auto business = path.workers().businessStats();
    EXPECT_EQ(business.pooled_retire, 1U);
    EXPECT_EQ(business.pooled_return, 0U);
    EXPECT_EQ(business.path_load_imbalance, 0U);

    EXPECT_TRUE(path.stop());
}

// Early CQE: the completion lands while the post call stack still holds a
// handler reference. The gate must not release the owners - and therefore must
// not return the member or the credit - until the post returns (design §15.2).
TEST(UbPoolIntegrationTest, EarlyCompletionDoesNotReleaseTheLeaseInsidePost) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());

    std::atomic<int> in_use_during_post{-1};
    std::atomic<uint64_t> credit_during_post{999};
    path.adapter()->setPostGate([&](uint64_t) {
        // Runs inside post(), after the completion became visible and before
        // post() returned.
        in_use_during_post.store(
            static_cast<int>(path.pool().stats().in_use_members),
            std::memory_order_release);
        credit_during_post.store(path.credits().outstanding(0),
                                 std::memory_order_release);
    });

    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s == COMPLETED; }));
    path.adapter()->clearPostGate();

    EXPECT_EQ(in_use_during_post.load(), 1);
    EXPECT_EQ(credit_during_post.load(), 1U);
    // After the post stack unwound, the settlement path returned both.
    EXPECT_TRUE(PooledDataPath::waitFor([&] {
        return path.pool().stats().in_use_members == 0 &&
               path.credits().outstanding(0) == 0;
    }));
    EXPECT_EQ(path.pool().stats().total_returns, 1U);
    EXPECT_EQ(task->inflightAttempts(), 0U);
    EXPECT_TRUE(path.stop());
}

// Timeout fence while the lease is in flight, with the pool already stopping.
// The fence must target the leased member (D-14), the exit must be kFinalize,
// and the return must turn into a retirement - leaving nothing behind for
// drainIdle() to guess about.
TEST(UbPoolIntegrationTest, TimeoutFenceRetiresTheLeasedMemberUnderStop) {
    // 300ms instead of a few ms: beginStop() must happen while the lease is
    // still in flight, and a very short timeout could fire first under load or
    // TSan (review note).
    PooledDataPath path(/*slice_timeout_ms=*/300);
    ASSERT_TRUE(path.start());
    // The completion never arrives on its own: the record has to be fenced.
    path.adapter()->holdNextCompletion();

    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().in_use_members == 1; }));

    // Stop the pool while the record still owns the lease: every later return
    // is retired by the pool itself.
    path.pool().beginStop();

    ASSERT_TRUE(PooledDataPath::waitFor([&] {
        return path.pool().stats().total_returns == 1 &&
               path.pool().stats().in_use_members == 0;
    }));
    const auto pool_stats = path.pool().stats();
    EXPECT_EQ(pool_stats.idle_members, 0U);
    EXPECT_EQ(pool_stats.leaked_leases, 0U);
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_EQ(task->inflightAttempts(), 0U);
    // W3-IN-10: the fence must target the leased member's Jetty - the same
    // Jetty the WR was posted through - not an endpoint Jetty.
    const auto post_ids = path.adapter()->postJettyIds();
    ASSERT_EQ(post_ids.size(), 1U);
    EXPECT_EQ(path.adapter()->lastQuiescedJettyId(), post_ids.front());
    EXPECT_TRUE(path.workers().stop().ok());
    EXPECT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));
    EXPECT_EQ(path.pool().stats().double_returns, 0U);
    (void)path.adapter()->shutdown();
}

// Shutdown with a lease still in flight: the bottom-out must finalize the
// record (kFinalize), retire the member and release the credit and the K slot
// exactly once - no lease may survive the drain (design §14.2 last-row matrix).
TEST(UbPoolIntegrationTest, ShutdownSettlesAnInflightLeaseWithoutLeaking) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().in_use_members == 1; }));

    // No fence can help here: the provider never delivers the completion, so
    // the shutdown bottom-out owns the settlement.
    ASSERT_TRUE(path.workers().stop().ok());
    EXPECT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));

    const auto pool_stats = path.pool().stats();
    EXPECT_EQ(pool_stats.in_use_members, 0U);
    EXPECT_EQ(pool_stats.leaked_leases, 0U);
    EXPECT_EQ(pool_stats.double_returns, 0U);
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_EQ(task->inflightAttempts(), 0U);

    const auto business = path.workers().businessStats();
    EXPECT_EQ(business.pooled_retire + business.pooled_return, 1U);
    // The member is gone after the drain: nothing is left to reuse.
    ASSERT_TRUE(path.workers().stop().ok());
    EXPECT_EQ(path.pool().stats().idle_members, 0U);
    (void)path.adapter()->shutdown();
}

// W3 review B1 regression: a slice deferred BEFORE beginAttempt() is still
// kQueued. The reflow used markQueued(), which refuses kQueued, so the slice
// was canceled instead of retried. Here the defer comes from the pool
// pre-screen (serving target already leased), i.e. the most common
// capacity-pressure exit.
TEST(UbPoolIntegrationTest, DeferredSliceBeforeAttemptIsReflowedNotCanceled) {
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*rm_mode=*/false, /*task_k=*/2);
    ASSERT_TRUE(path.start());
    // Keep the single pool member busy so the second Task hits the pre-screen.
    path.adapter()->holdNextCompletion();

    auto first = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(first).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().in_use_members == 1; }));

    auto second = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(second).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.workers().parkedCount() == 1; }));
    // Deferred, not failed: no retry was consumed and the Task is still
    // pending.
    EXPECT_EQ(second->transferStatus().s, PENDING);
    EXPECT_EQ(second->slices().front()->snapshot().state,
              UbSliceState::kQueued);

    // Release the held completion: the member returns, the kPoolIdle bucket is
    // bumped, and the parked slice must be re-queued and succeed.
    ASSERT_EQ(path.adapter()->completePending(), 1U);
    ASSERT_TRUE(PooledDataPath::waitFor([&] {
        return first->transferStatus().s == COMPLETED &&
               second->transferStatus().s == COMPLETED;
    }));
    EXPECT_EQ(second->transferStatus().s, COMPLETED);
    const auto business = path.workers().businessStats();
    EXPECT_GE(business.reflow_requeued, 1U);
    EXPECT_EQ(business.reflow_cancelled, 0U);
    EXPECT_EQ(business.task_K_wait, 0U);
    // defer_reflow_repark is deliberately not asserted: the pool pre-screen
    // defer carries a timed deadline, so the exact number of re-evaluations
    // depends on how long the member stays leased. The invariant that matters
    // is reflow_cancelled == 0 above - the B1 bug canceled the slice instead of
    // retrying it.
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_TRUE(path.stop());
}

// W3c end-to-end: K=1 with two slices. The second attempt finds K exhausted,
// must defer (not fail, not consume retry) and must resume once the first
// attempt's record dies and releases the slot.
TEST(UbPoolIntegrationTest, TaskKFullDefersWithoutChargingRetry) {
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/2,
                        /*credit_capacity=*/8, /*rm_mode=*/false, /*task_k=*/1);
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    auto task = path.makeTask(/*slice_count=*/2, /*max_retries=*/0);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.workers().businessStats().task_K_wait >= 1; }));
    EXPECT_EQ(task->transferStatus().s, PENDING);
    // K pressure is not a failure: the deferred slice kept its retry budget.
    for (const auto& slice : task->slices()) {
        EXPECT_EQ(slice->snapshot().retry_count, 0U);
    }
    EXPECT_EQ(task->inflightAttempts(), 1U);

    ASSERT_EQ(path.adapter()->completePending(), 1U);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s == COMPLETED; }));
    EXPECT_EQ(task->inflightAttempts(), 0U);
    EXPECT_EQ(task->slotUnderflows(), 0U);
    const auto business = path.workers().businessStats();
    EXPECT_GE(business.reflow_requeued, 1U);
    EXPECT_EQ(business.reflow_cancelled, 0U);
    EXPECT_TRUE(path.stop());
}

// §14.2 "NotAccepted" row: a definite zero acceptance rolls back in reverse
// order, hands the clean member back (never retires it) and does not charge the
// pool's double-return guard.
TEST(UbPoolIntegrationTest, NotAcceptedRollsBackAndReturnsTheMember) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    // !ok with zero posted WRs is the only case the post side may roll back.
    path.adapter()->setNextPostOutcome(
        Status::InternalError("injected definite zero acceptance"), 0);
    auto task = path.makeTask(/*slice_count=*/1, /*max_retries=*/0);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));
    EXPECT_EQ(task->transferStatus().s, FAILED);

    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().total_returns == 1; }));
    const auto pool_stats = path.pool().stats();
    EXPECT_EQ(pool_stats.in_use_members, 0U);
    EXPECT_EQ(pool_stats.leaked_leases, 0U);
    EXPECT_EQ(pool_stats.double_returns, 0U);
    EXPECT_EQ(pool_stats.idle_members, 1U);
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_EQ(path.credits().rejectedReleases(0), 0U);
    EXPECT_EQ(task->inflightAttempts(), 0U);
    EXPECT_EQ(task->slotUnderflows(), 0U);
    const auto business = path.workers().businessStats();
    EXPECT_EQ(business.pooled_return, 1U);
    EXPECT_EQ(business.pooled_retire, 0U);
    // The gate classified it as a reject, not as a normal return.
    EXPECT_EQ(path.workers().completionStats().record_rejected, 1U);
    EXPECT_TRUE(path.stop());
}

// F5/INV-POST-1: a provider error that does not say where it stopped is not a
// definite zero acceptance. The record must stay owned and settle through the
// fence instead of rolling back and handing the member straight back.
TEST(UbPoolIntegrationTest, ErrorWithoutFailingWrIsUnknownNotRolledBack) {
    PooledDataPath path(/*slice_timeout_ms=*/20);
    ASSERT_TRUE(path.start());
    path.adapter()->setNextPostOutcome(
        Status::RdmaError("injected post failure without a bad_wr"), 0);
    path.adapter()->setNextPostEvidence(PostEvidence::kNoFailingWr);

    auto task = path.makeTask(/*slice_count=*/1, /*max_retries=*/0);
    ASSERT_TRUE(path.workers().submit(task).ok());
    // The WR may still be in flight: make the record's post return first, then
    // deliver a completion so the acceptance block is observable.
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.workers().inflightCount() == 1; }));
    path.adapter()->injectCompletion(path.businessJfc(), /*token=*/1,
                                     CompletionCategory::SUCCESS,
                                     /*completed_bytes=*/16);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));

    const auto completion = path.workers().completionStats();
    EXPECT_EQ(completion.record_rejected, 0U);
    EXPECT_GE(completion.gate_blocked_by_acceptance, 1U);
    EXPECT_EQ(completion.not_accepted_with_completion, 0U);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().total_returns == 1; }));
    // The member was retired by the fence, never handed back as "clean".
    EXPECT_EQ(path.workers().businessStats().pooled_return, 0U);
    EXPECT_EQ(path.workers().businessStats().pooled_retire, 1U);
    EXPECT_EQ(path.pool().stats().in_use_members, 0U);
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_TRUE(path.stop());
}

// §9.2 "Unknown" row: a contradictory post result must neither be replayed nor
// rolled back. The record drains until the timeout fence, then finalizes and
// retires the member.
TEST(UbPoolIntegrationTest, UnknownAcceptanceDrainsUntilTheFence) {
    PooledDataPath path(/*slice_timeout_ms=*/20);
    ASSERT_TRUE(path.start());
    // Success status with nothing posted is self-contradictory -> kUnknown.
    path.adapter()->setNextPostOutcome(Status::OK(), 0);
    auto task = path.makeTask(/*slice_count=*/1, /*max_retries=*/0);
    ASSERT_TRUE(path.workers().submit(task).ok());
    // Let the record be published, then deliver a real completion for it: an
    // Unknown acceptance is only observable at the gate when a CQE arrives
    // (design §9.2 case 1). Tokens start at 1 for a fresh workers instance.
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.workers().inflightCount() == 1; }));
    path.adapter()->injectCompletion(path.businessJfc(), /*token=*/1,
                                     CompletionCategory::SUCCESS,
                                     /*completed_bytes=*/16);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));

    const auto completion = path.workers().completionStats();
    // The WR ended but the post was not provably accepted: no exit may release
    // the owners, so the gate blocks until the fence.
    EXPECT_GE(completion.gate_blocked_by_acceptance, 1U);
    EXPECT_EQ(completion.not_accepted_with_completion, 0U);
    EXPECT_EQ(completion.record_rejected, 0U);
    EXPECT_EQ(completion.record_finalized, 1U);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().total_returns == 1; }));
    const auto pool_stats = path.pool().stats();
    EXPECT_EQ(pool_stats.in_use_members, 0U);
    EXPECT_EQ(pool_stats.idle_members, 0U);
    EXPECT_EQ(pool_stats.leaked_leases, 0U);
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_EQ(path.workers().businessStats().pooled_retire, 1U);
    EXPECT_TRUE(path.stop());
}

// Production shape: an RM pooled member posts through the RM overload with the
// endpoint's peer target, and the completion settles normally.
TEST(UbPoolIntegrationTest, RmModePostsThroughThePooledMember) {
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/2,
                        /*credit_capacity=*/8, /*rm_mode=*/true);
    ASSERT_TRUE(path.start());
    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s == COMPLETED; }));

    EXPECT_TRUE(path.adapter()->lastPostHadTarget());
    // The peer target comes from the endpoint's bootstrap, not from a binding.
    EXPECT_EQ(path.adapter()->lastPostTarget().id, 777U);
    EXPECT_EQ(path.adapter()->lastPostTarget().eid, fakeDevice().eid);
    // Exactly one post, and it rode the leased member (the pool creates its
    // members after the endpoint set, so the ids differ).
    const auto post_ids = path.adapter()->postJettyIds();
    ASSERT_EQ(post_ids.size(), 1U);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().total_returns == 1; }));
    EXPECT_EQ(path.pool().stats().idle_members, 1U);
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_EQ(task->inflightAttempts(), 0U);
    EXPECT_TRUE(path.stop());
}

// F2/INV-ID-3: the cross-Context lane ledger must not alias pool-local lane
// generations. Every pool starts its generation sequence at 1, so a bare
// generation makes another Context's in-flight record look like this lane's
// convergence (and lets the drain delete a Jetty whose WR is still live).
TEST(UbPoolIntegrationTest, LaneConvergenceIsScopedByContextIncarnation) {
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/1);
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().in_use_members == 1; }));

    const uint64_t context = path.contextIncarnation();
    // The first lane of a fresh pool has generation 1.
    EXPECT_FALSE(path.workers().pooledLaneConverged(LaneRefKey{context, 1}));
    // The same numeric generation under another incarnation owns no record:
    // it converges immediately instead of borrowing this Context's count.
    EXPECT_TRUE(path.workers().pooledLaneConverged(
        LaneRefKey{context + (1ULL << 40), 1}));
    EXPECT_TRUE(
        path.workers().pooledLaneConverged(LaneRefKey{context, 987654}));

    ASSERT_TRUE(path.stop());
    EXPECT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));
}

// D1 regression: when no control executor exists, scanTimeouts holds
// endpoint_drain_mutex_ and the fence must reuse that lock, not take it again.
// A non-recursive std::mutex re-entry would deadlock the poller on the first
// deadline timeout (the pre-F6a code had one lock for both steps).
TEST(UbPoolIntegrationTest, DegradedFencePathDoesNotSelfDeadlock) {
    PooledDataPath path(/*slice_timeout_ms=*/20, /*desired_serving=*/1);
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    UbWorkersTestPeer::dropControlExecutor(path.workers());

    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; },
        std::chrono::seconds(5)));
    EXPECT_EQ(task->transferStatus().s, TIMEOUT);
    // No job was ever submitted: the fence ran inline.
    EXPECT_EQ(path.workers().businessStats().timeout_fence_submitted, 0U);
    EXPECT_EQ(path.workers().businessStats().timeout_fence_rejected, 0U);
    EXPECT_TRUE(path.stop());
}

// ---- F6a/F6a2/F6c: timeout fences, device retirement, request deadlines ----

// F6c stage 1: a request whose deadline already passed must be terminated
// before it consumes any post. It is a timeout, not backpressure.
TEST(UbPoolIntegrationTest, DeadlineExpiredInQueueIsNotPosted) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    auto task = path.makeTask(/*slice_count=*/1, /*max_retries=*/0,
                              /*deadline_ns=*/steadyNow() - 1'000'000ULL);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));

    EXPECT_EQ(task->transferStatus().s, TIMEOUT);
    EXPECT_TRUE(path.adapter()->postJettyIds().empty());
    const auto business = path.workers().businessStats();
    EXPECT_EQ(business.expired_in_queue, 1U);
    EXPECT_EQ(business.expired_pre_post, 0U);
    EXPECT_EQ(business.submit_backpressure, 0U);
    EXPECT_TRUE(path.stop());
}

// F6c stage 2: a parked slice that expires must be cancelled by the reflow, not
// parked again forever.
TEST(UbPoolIntegrationTest, DeadlineExpiredWhileParkedIsCancelled) {
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*rm_mode=*/false,
                        /*task_k=*/1);
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    auto first = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(first).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().in_use_members == 1; }));

    // The K slot is taken: this slice parks, and its deadline is 30ms away.
    auto parked = path.makeTask(/*slice_count=*/1, /*max_retries=*/0,
                                /*deadline_ns=*/steadyNow() + 30'000'000ULL);
    ASSERT_TRUE(path.workers().submit(parked).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.workers().parkedCount() >= 1; }));

    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return parked->transferStatus().s != PENDING; }));
    EXPECT_EQ(parked->transferStatus().s, TIMEOUT);
    EXPECT_GE(path.workers().businessStats().expired_parked, 1U);
    // Only the first task ever reached the provider.
    EXPECT_EQ(path.adapter()->postJettyIds().size(), 1U);
    EXPECT_TRUE(path.stop());
}

// F6c stage 4: in flight, the earlier of (posted + slice_timeout, request
// deadline) wins, so the F6a fence enforces the request deadline.
TEST(UbPoolIntegrationTest, InflightDeadlineUsesTheEarlierOfTwo) {
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/1);
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    auto task = path.makeTask(/*slice_count=*/1, /*max_retries=*/0,
                              /*deadline_ns=*/steadyNow() + 30'000'000ULL);
    const uint64_t started = steadyNow();
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));
    const uint64_t elapsed = steadyNow() - started;

    EXPECT_EQ(task->transferStatus().s, TIMEOUT);
    EXPECT_EQ(path.workers().businessStats().expired_inflight, 1U);
    // The 5s WR budget must not be what fired.
    EXPECT_LT(elapsed, 2'000'000'000ULL);
    EXPECT_TRUE(path.stop());
}

// F6c: deadline_ns == 0 keeps the legacy behavior bit for bit.
TEST(UbPoolIntegrationTest, ZeroDeadlineKeepsLegacyBehaviour) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    auto task = path.makeTask(/*slice_count=*/1, /*max_retries=*/0);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s == COMPLETED; }));
    const auto business = path.workers().businessStats();
    EXPECT_EQ(business.expired_in_queue, 0U);
    EXPECT_EQ(business.expired_parked, 0U);
    EXPECT_EQ(business.expired_pre_post, 0U);
    EXPECT_EQ(business.expired_inflight, 0U);
    EXPECT_TRUE(path.stop());
}

// F6a: the poller only discovers and enqueues. While the fence sits in the
// control queue, completions keep being polled and settled.
TEST(UbPoolIntegrationTest, TimeoutFenceRunsOffThePoller) {
    auto executor = std::make_shared<ManualControlExecutor>();
    PooledDataPath path(/*slice_timeout_ms=*/20, /*desired_serving=*/2,
                        /*credit_capacity=*/8, /*rm_mode=*/false,
                        /*task_k=*/2, /*queue_budget=*/65536,
                        /*jfc_count=*/1, /*jetty_per_endpoint=*/1, executor);
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    auto fenced = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(fenced).ok());
    // The scan claimed the timeout and queued the native fence, without
    // running it yet.
    ASSERT_TRUE(executor->waitPending(1));
    EXPECT_EQ(path.workers().businessStats().timeout_fence_submitted, 1U);
    EXPECT_EQ(fenced->transferStatus().s, PENDING);

    // The poller is still serving: a second WR completes while the fence waits.
    auto healthy = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(healthy).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return healthy->transferStatus().s == COMPLETED; }));
    EXPECT_EQ(fenced->transferStatus().s, PENDING);

    EXPECT_GT(executor->runAll(), 0U);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return fenced->transferStatus().s != PENDING; }));
    EXPECT_EQ(fenced->transferStatus().s, TIMEOUT);
    EXPECT_TRUE(path.stop());
}

// F6a failure policy: a refused fence submit must keep the record registered
// and let the next scan retry instead of dropping the obligation.
TEST(UbPoolIntegrationTest, RejectedTimeoutFenceKeepsTheRecord) {
    auto executor = std::make_shared<ManualControlExecutor>(/*capacity=*/0);
    PooledDataPath path(/*slice_timeout_ms=*/20, /*desired_serving=*/1,
                        /*credit_capacity=*/8, /*rm_mode=*/false,
                        /*task_k=*/1, /*queue_budget=*/65536,
                        /*jfc_count=*/1, /*jetty_per_endpoint=*/1, executor);
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor([&] {
        return path.workers().businessStats().timeout_fence_rejected >= 1;
    }));
    EXPECT_EQ(path.workers().inflightCount(), 1U);
    EXPECT_EQ(task->transferStatus().s, PENDING);

    // Capacity returns: the next scan retries and the queued fence settles it.
    executor->setCapacity(64);
    ASSERT_TRUE(executor->waitPending(1));
    EXPECT_GT(executor->runAll(), 0U);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));
    EXPECT_EQ(task->transferStatus().s, TIMEOUT);
    EXPECT_TRUE(path.stop());
}

// F6a2: the native endpoint barrier runs on the control executor; the poller
// keeps polling and the context state transition already happened.
TEST(UbPoolIntegrationTest, DeviceRetireRunsOffThePoller) {
    std::atomic<uint64_t> handler_calls{0};
    LocalDeviceFailureHandler handler = [&handler_calls](Topology::NicID) {
        handler_calls.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    };
    auto executor = std::make_shared<ManualControlExecutor>();
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/2,
                        /*credit_capacity=*/8, /*rm_mode=*/false,
                        /*task_k=*/2, /*queue_budget=*/65536,
                        /*jfc_count=*/1, /*jetty_per_endpoint=*/1, executor,
                        handler);
    ASSERT_TRUE(path.start());

    path.adapter()->failNextPoll();
    ASSERT_TRUE(executor->waitPending(1));
    // The native barrier has not run on the poller.
    EXPECT_EQ(handler_calls.load(), 0U);
    const uint64_t polls_before = path.adapter()->pollCalls();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_GT(path.adapter()->pollCalls(), polls_before);

    EXPECT_GT(executor->runAll(), 0U);
    EXPECT_GE(handler_calls.load(), 1U);
    EXPECT_EQ(path.workers().businessStats().device_retire_rejected, 0U);
    EXPECT_TRUE(path.stop());
}

// Design reverted-item lock-in (D4): the PLM fault epoch is part of the claim
// decision, not an observation. ClaimAnchorMovesOnlyOnIsolation() proves the
// epoch moves; this pins the other half - a moved epoch must reject the claim.
// Deleting `!path_epoch_changed` from claimed() makes this test fail.
TEST(UbPoolIntegrationTest, PathEpochChangeRejectsTheClaim) {
    EXPECT_TRUE(UbWorkersTestPeer::claimedWithEpochChange(false));
    EXPECT_FALSE(UbWorkersTestPeer::claimedWithEpochChange(true));
}

// §6.1 exception path: an enqueue failure after part of the batch was marked
// must roll the queues back and cancel every slice of the batch, so nothing is
// left kQueued behind the failure.
TEST(UbPoolIntegrationTest, BatchEnqueueExceptionRollsBackAndCancels) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    auto task = path.makeTask(/*slice_count=*/2);
    UbWorkersTestPeer::failSecondEnqueue();
    const Status status = path.workers().submit(task);
    EXPECT_TRUE(status.IsInternalError());
    EXPECT_EQ(path.workers().queuedCount(), 0U);
    EXPECT_EQ(path.workers().businessStats().queue_depth, 0U);
    for (const auto& slice : task->slices()) {
        EXPECT_EQ(slice->snapshot().state, UbSliceState::kCanceled);
    }
    // The failure did not poison the workers: a later submission still works.
    auto retry = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(retry).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return retry->transferStatus().s == COMPLETED; }));
    EXPECT_TRUE(path.stop());
}

// §6.1 at the workers level: a Task that does not fit the budget is rejected as
// a whole and NO slice is touched (its state must still be the pre-submit one).
TEST(UbPoolIntegrationTest, QueueBudgetRejectsWholeTaskWithoutTouchingSlices) {
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/2,
                        /*credit_capacity=*/8, /*rm_mode=*/false,
                        /*task_k=*/2, /*queue_budget=*/1);
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();

    // Fill the single slot with a one-slice Task.
    auto filler = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(filler).ok());

    auto oversized = path.makeTask(/*slice_count=*/2);
    const Status status = path.workers().submit(oversized);
    EXPECT_TRUE(status.IsTooManyRequests());
    for (const auto& slice : oversized->slices()) {
        // The capacity verdict came before any markQueued(): every slice is
        // still in its pre-submit state, so a later retry can succeed.
        EXPECT_EQ(slice->snapshot().state, UbSliceState::kInitial);
        EXPECT_TRUE(slice->queuedEligible());
    }
    EXPECT_GE(path.workers().businessStats().submit_backpressure, 1U);

    // A Task that fits is still accepted.
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().in_use_members == 1; }));
    ASSERT_EQ(path.adapter()->completePending(), 1U);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return filler->transferStatus().s == COMPLETED; }));
    auto small = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.workers().submit(small).ok(); }));
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return small->transferStatus().s == COMPLETED; }));
    EXPECT_TRUE(path.stop());
}

// Shutdown bottom-out: the fence proves nothing (the provider drops the
// completion), so onUnposted must own the settlement - retire the member, free
// the credit and the K slot, and leave no lease behind.
TEST(UbPoolIntegrationTest, ShutdownBottomOutSettlesAnUnprovenLease) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    path.adapter()->holdNextCompletion();
    path.adapter()->dropNextQuiescedCompletion();

    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().in_use_members == 1; }));

    ASSERT_TRUE(path.workers().stop().ok());
    EXPECT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s != PENDING; }));
    const auto pool_stats = path.pool().stats();
    EXPECT_EQ(pool_stats.in_use_members, 0U);
    EXPECT_EQ(pool_stats.leaked_leases, 0U);
    EXPECT_EQ(pool_stats.double_returns, 0U);
    EXPECT_EQ(path.credits().outstanding(0), 0U);
    EXPECT_EQ(task->inflightAttempts(), 0U);
    const auto business = path.workers().businessStats();
    EXPECT_EQ(business.pooled_retire + business.pooled_return, 1U);
    EXPECT_EQ(business.path_load_imbalance, 0U);
    (void)path.adapter()->shutdown();
}

// §13/§15.2 budget and quarantine: a failed native delete keeps the member (and
// its budget) and the next drain retries it. The budget must reach zero only
// after the deletion is proven.
TEST(UbPoolIntegrationTest, DrainQuarantinesFailedDeletesAndReleasesBudget) {
    PooledDataPath path;
    ASSERT_TRUE(path.start());
    auto task = path.makeTask(/*slice_count=*/1);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().total_returns == 1; }));
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return path.budget().snapshot(kDeviceKey).total_jetties == 1; }));

    // Two consecutive failures: drainIdle() and the retry right after it.
    path.adapter()->failJettyDeletes(2);
    ASSERT_TRUE(path.workers().stop().ok());
    const auto quarantined = path.budget().snapshot(kDeviceKey);
    EXPECT_EQ(quarantined.total_jetties, 1U);
    EXPECT_EQ(quarantined.quarantined_jetties, 1U);
    EXPECT_GT(path.pool().stats().quarantined_members, 0U);

    // The third attempt is not injected: the member is deleted and the budget
    // is released only now.
    ASSERT_TRUE(path.pool().drainQuarantined().ok());
    EXPECT_EQ(path.budget().snapshot(kDeviceKey).total_jetties, 0U);
    EXPECT_EQ(path.pool().stats().quarantined_members, 0U);
    (void)path.adapter()->shutdown();
}

// W3 review B3: makeBootstrapDesc must advertise the transport mode, otherwise
// an RM peer rejects an RM endpoint (and vice versa) and rm_pool never becomes
// ready. This pins the exact round-trip: our own descriptor must be bindable.
TEST(UbPoolIntegrationTest, BootstrapAdvertisesTransportModeAndBindRoundTrips) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    JfcOptions jfc_options;
    jfc_options.transport_mode = JettyTransportMode::kRm;
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, jfc_options).ok());

    JettyOptions rm_options;
    rm_options.transport_mode = JettyTransportMode::kRm;
    auto make_endpoint = [&](const std::string& peer_path) {
        UbEndpointKey key{0, /*remote_segment_id=*/11, 0, peer_path};
        return std::make_shared<UbEndpoint>(key, context, adapter,
                                            /*jetty_count=*/2, rm_options);
    };

    auto endpoint = make_endpoint("peer@ub:fake0:eid0");
    ASSERT_TRUE(endpoint->prepare().ok());
    UbBootstrapDesc desc;
    ASSERT_TRUE(
        endpoint->makeBootstrapDesc("local", "", "peer@ub:fake0:eid0", 1, desc)
            .ok());
    EXPECT_NE(std::find(desc.capabilities.begin(), desc.capabilities.end(),
                        kUbCapabilityRmTransport),
              desc.capabilities.end());
    // The baseline capabilities must still be there: the mode is appended, not
    // overwriting the list.
    EXPECT_NE(
        std::find(desc.capabilities.begin(), desc.capabilities.end(), "read"),
        desc.capabilities.end());

    // Our own descriptor is a valid peer descriptor for an RM endpoint: this is
    // the round-trip that was broken (mutual rejection).
    ASSERT_TRUE(endpoint->bind(desc).ok());
    EXPECT_TRUE(endpoint->ready());
    EXPECT_TRUE(endpoint->rmMode());
    EXPECT_EQ(endpoint->rmTarget(0)->id(), desc.jetty_ids[0]);

    // An RC peer must still be rejected by an RM endpoint (fail-closed).
    auto other = make_endpoint("peer2@ub:fake0:eid0");
    ASSERT_TRUE(other->prepare().ok());
    UbBootstrapDesc rc_peer = desc;
    rc_peer.capabilities = {"read", "write", "endpoint_generation",
                            kUbCapabilityRcTransport};
    EXPECT_FALSE(other->bind(rc_peer).ok());

    (void)context->shutdown();
    (void)adapter->shutdown();
}

// W3 review M6: the RM target slot follows the posting lane. With two peer
// Jetties and two members (whose JFC rotation indices differ) the old
// `jfc_index % jettyCount` produced 778 for the second WR; the lane-based slot
// produces 777 for both.
TEST(UbPoolIntegrationTest, RmTargetSlotFollowsThePostingLane) {
    PooledDataPath path(/*slice_timeout_ms=*/5000, /*desired_serving=*/2,
                        /*credit_capacity=*/8, /*rm_mode=*/true, /*task_k=*/2,
                        /*queue_budget=*/65536, /*jfc_count=*/2,
                        /*jetty_per_endpoint=*/2);
    ASSERT_TRUE(path.start());
    auto task = path.makeTask(/*slice_count=*/2);
    ASSERT_TRUE(path.workers().submit(task).ok());
    ASSERT_TRUE(PooledDataPath::waitFor(
        [&] { return task->transferStatus().s == COMPLETED; }));

    const auto targets = path.adapter()->postTargetIds();
    ASSERT_EQ(targets.size(), 2U);
    // Both WRs were posted by lane 0 (worker_count == 1), so both target the
    // peer's slot 0 - regardless of the member's JFC rotation index.
    for (uint32_t target : targets) EXPECT_EQ(target, 777U);
    EXPECT_TRUE(PooledDataPath::waitFor(
        [&] { return path.pool().stats().total_returns == 2; }));
    EXPECT_TRUE(path.stop());
}

// Regression for the rm_pool install failure observed on a real device:
// max_jetty_per_device=0 derived the budget as the device's own limit, and
// NativeBudget::configure() then rejected (limit + reserve) > limit, so the UB
// transport was skipped entirely ("UB native budget exceeds the verified
// device Jetty limit").
TEST(NativeBudgetDerivationTest, DerivedBudgetLeavesRoomForTheReserve) {
    constexpr uint64_t kVerified = 65532;
    constexpr uint64_t kReserve = 2;
    const uint64_t derived = deriveNativeBudgetJetties(
        kVerified, /*configured=*/0, kReserve, /*minimum=*/6);
    EXPECT_EQ(derived, kVerified - kReserve);

    NativeBudget budget;
    // This is the install-time call; it must succeed for a real device that
    // reports its limit and the default reserve.
    EXPECT_TRUE(budget.configure("dev", kVerified, derived, kReserve).ok());
}

TEST(NativeBudgetDerivationTest, ExplicitBudgetIsAuthoritative) {
    EXPECT_EQ(deriveNativeBudgetJetties(/*verified=*/64, /*configured=*/8,
                                        /*reserve=*/2, /*minimum=*/6),
              8U);
}

TEST(NativeBudgetDerivationTest, NoReportedLimitFallsBackToTheServingTarget) {
    EXPECT_EQ(deriveNativeBudgetJetties(/*verified=*/0, /*configured=*/0,
                                        /*reserve=*/2, /*minimum=*/6),
              6U);
    // A zero minimum still has to yield a positive budget.
    EXPECT_EQ(deriveNativeBudgetJetties(0, 0, 2, /*minimum=*/0), 1U);
}

TEST(NativeBudgetDerivationTest, ZeroReserveStillFitsTheDeviceLimit) {
    constexpr uint64_t kVerified = 64;
    const uint64_t derived =
        deriveNativeBudgetJetties(kVerified, 0, /*reserve=*/0, 6);
    EXPECT_EQ(derived, kVerified);
    NativeBudget budget;
    EXPECT_TRUE(budget.configure("dev", kVerified, derived, 0).ok());
}

}  // namespace
}  // namespace mooncake::tent::ub
