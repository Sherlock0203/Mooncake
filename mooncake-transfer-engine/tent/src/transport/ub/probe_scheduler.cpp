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

#include "tent/transport/ub/probe_scheduler.h"

#include <chrono>
#include <utility>

#include <glog/logging.h>

#include "tent/transport/ub/slice.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

// Safety net for a deadline that is already in the past (for example when
// beginPost() lost a race and the action is still due). Keeps the thread from
// spinning without delaying a healthy schedule.
constexpr auto kPastDeadlineBackoff = std::chrono::milliseconds(1);

}  // namespace

ProbeScheduler::ProbeScheduler(ProbeLinkManager& plm, ProbePoster& poster,
                               Clock clock)
    : plm_(plm), poster_(poster), clock_(std::move(clock)) {}

ProbeScheduler::~ProbeScheduler() {
    // Requests stop and joins. Clearing the hooks after the join keeps the PLM
    // from calling into a destroyed scheduler.
    stop();
    plm_.setWakeCallback({});
    plm_.setSchedulerStopHook({});
}

uint64_t ProbeScheduler::normalizedNow() const {
    if (clock_) {
        const uint64_t now = clock_();
        if (now != 0) return now;
    }
    return steadyNowNs();
}

void ProbeScheduler::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (thread_.joinable()) return;
    stopping_.store(false);
    // The wake callback takes the scheduler mutex and bumps a generation so a
    // state change that races the deadline computation cannot be lost between
    // "compute" and "wait".
    plm_.setWakeCallback([this] {
        std::lock_guard<std::mutex> guard(mutex_);
        ++wake_seq_;
        wake_.notify_all();
    });
    plm_.setSchedulerStopHook([this] { stop(); });
    thread_ = std::thread([this] { run(); });
}

void ProbeScheduler::notePostingStopped() { plm_.stopPosting(); }

void ProbeScheduler::stop() {
    std::thread to_join;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_.store(true);
        if (thread_.joinable()) {
            if (thread_.get_id() == std::this_thread::get_id()) {
                // Cannot join ourselves; the owner must not do this. Returning
                // lets the thread exit at its next loop check, and a later
                // stop() from another thread joins it.
                LOG(WARNING)
                    << "UB probe: scheduler stop requested from its own thread";
                return;
            }
            // Move the handle out under the mutex so concurrent stop() calls
            // cannot both try to join it.
            to_join = std::move(thread_);
        }
    }
    wake_.notify_all();
    if (to_join.joinable()) to_join.join();
}

bool ProbeScheduler::running() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return thread_.joinable() && !stopping_.load();
}

void ProbeScheduler::run() {
    while (!stopping_.load()) {
        const uint64_t now = normalizedNow();
        for (const auto& action : plm_.pendingActions(now)) {
            if (stopping_.load()) return;
            postOne(action, now);
        }
        const uint64_t scanned_at = normalizedNow();
        plm_.scanTimeouts(scanned_at);
        if (last_link_summary_ns_ == 0 ||
            scanned_at - last_link_summary_ns_ >= 30'000'000'000ULL) {
            last_link_summary_ns_ = scanned_at;
            const auto stats = plm_.stats();
            LOG(INFO) << "UB probe link summary:"
                      << " lane_retire=" << stats.probe_lane_retire
                      << " lane_rebuild=" << stats.probe_lane_rebuild
                      << " failure_path=" << stats.probe_failure_path
                      << " failure_lane=" << stats.probe_failure_lane
                      << " post_rejected=" << stats.probe_post_rejected
                      << " post_unknown=" << stats.probe_post_unknown
                      << " area_config_error=" << stats.probe_area_config_error
                      << " success=" << stats.probe_success;
        }

        uint64_t deadline = 0;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (stopping_.load()) return;
            deadline = plm_.nextDeadline(normalizedNow());
            const uint64_t seen = wake_seq_;
            auto changed = [this, seen] {
                return stopping_.load() || wake_seq_ != seen;
            };
            if (deadline == 0) {
                wake_.wait(lock, changed);
                continue;
            }
            const auto target = std::chrono::steady_clock::time_point(
                std::chrono::nanoseconds(deadline));
            if (target <= std::chrono::steady_clock::now()) {
                wake_.wait_for(lock, kPastDeadlineBackoff, changed);
            } else {
                wake_.wait_until(lock, target, changed);
            }
        }
    }
}

void ProbeScheduler::postOne(const ProbeAction& action, uint64_t now_ns) {
    const uint64_t token = makeProbeToken(++token_seq_);
    auto record = plm_.beginPost(action, token, now_ns);
    if (record == nullptr) return;

    // ProbePostTarget only carries handles/ownership; ProbeWorkRequest only
    // carries WR payload (design §8.4/§12.2).
    ProbePostTarget target;
    target.probe_jetty = action.probe_jetty;
    target.jetty_generation = action.jetty_generation;
    target.remote_jetty = action.remote_jetty;
    target.source_segment = action.source_segment;
    target.area_segment = action.area_segment;

    ProbeWorkRequest request;
    request.opcode = ProbeOpcode::kWrite;
    request.token = token;
    request.source_address = action.source_address;
    request.source_length = 1;
    request.area_address = action.area_address;
    request.area_length = 1;

    size_t posted = 0;
    ProbePostOutcome outcome = ProbePostOutcome::kUnknown;
    const auto started = std::chrono::steady_clock::now();
    // The poster owns the acceptance decision (F5): a provider error without a
    // failing-WR position is Unknown, not Rejected.
    const Status status = poster_.post(target, request, posted, outcome);
    const auto blocked_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started)
            .count();
    plm_.notePostBlockUs(static_cast<uint64_t>(blocked_us));

    if (plm_.verboseEvents() || VLOG_IS_ON(1)) {
        const char* outcome_name =
            outcome == ProbePostOutcome::kAccepted
                ? "accepted"
                : (outcome == ProbePostOutcome::kRejected ? "rejected"
                                                          : "unknown");
        if (plm_.verboseEvents()) {
            LOG(INFO) << "UB probe post: local_nic=" << action.local_nic
                      << " peer=" << action.peer.segment_id
                      << " remote_nic=" << action.version.path.remote_nic
                      << " topo_epoch=" << action.version.topology_epoch
                      << " target_epoch=" << action.version.target_epoch
                      << " fault_epoch=" << record->key.fault_epoch
                      << " probe_seq=" << record->key.probe_seq
                      << " jetty_gen=" << action.jetty_generation
                      << " area_epoch=" << action.area_epoch
                      << " campaign=" << record->key.campaign_id << "/"
                      << record->key.campaign_epoch << " token=" << token
                      << " outcome=" << outcome_name << " posted=" << posted
                      << " blocked_us=" << blocked_us
                      << " test=" << (action.test ? 1 : 0);
        } else {
            VLOG(1) << "UB probe post: local_nic=" << action.local_nic
                    << " peer=" << action.peer.segment_id
                    << " remote_nic=" << action.version.path.remote_nic
                    << " topo_epoch=" << action.version.topology_epoch
                    << " target_epoch=" << action.version.target_epoch
                    << " fault_epoch=" << record->key.fault_epoch
                    << " probe_seq=" << record->key.probe_seq
                    << " jetty_gen=" << action.jetty_generation
                    << " area_epoch=" << action.area_epoch
                    << " campaign=" << record->key.campaign_id << "/"
                    << record->key.campaign_epoch << " token=" << token
                    << " outcome=" << outcome_name << " posted=" << posted
                    << " blocked_us=" << blocked_us
                    << " test=" << (action.test ? 1 : 0);
        }
    }
    plm_.onPostResult(record, outcome, normalizedNow());
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
