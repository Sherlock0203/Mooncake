// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_JETTY_POOL_H_
#define TENT_TRANSPORT_UB_JETTY_POOL_H_

#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

// Legacy send-Jetty lifecycle pool, one per UbContext (RM Legacy only, gated
// by transports.ub.jetty_reuse.enable). Endpoints lease their whole Jetty set
// exclusively in prepare().
//
// Fail-closed reuse (C02 / G-01): a Jetty that has posted WRs and then been
// RESET is NOT re-leased. The current ProviderContract carries no proven
// RESET-rearm guarantee, and RealJetty::postable() only accepts CREATED/BOUND,
// so re-posting a RESET member would be rejected by the real wrapper. Without
// a rearm proof the pool therefore retires every drained member (deleteJetty)
// and acquire() always creates a fresh replacement. A failed delete keeps the
// member pending so drain() can retry it; a pending member is never leased.
//
// Safe shutdown (C12 / G-02): the pool holds strong references to the Context
// handle and its JFCs. beginStop() freezes new leases, drain() deletes pending
// members, and detach() drops those strong references so closeContext()/JFC
// close can succeed (releaseTypedHandle requires use_count()==1) even while an
// external owner still holds the pool shared_ptr.
class UbSendJettyPool {
   public:
    UbSendJettyPool(std::shared_ptr<UrmaAdapter> adapter,
                    const ContextPtr& context_handle,
                    std::vector<std::shared_ptr<UbJfc>> jfcs,
                    JettyOptions options);

    UbSendJettyPool(const UbSendJettyPool&) = delete;
    UbSendJettyPool& operator=(const UbSendJettyPool&) = delete;

    // Acquires count fresh Jetties exclusively on the context's JFCs
    // (round-robin). Fail-closed: drained RESET members are never re-leased,
    // so this always creates. On a creation failure the call is atomic: every
    // clean never-posted Jetty taken in this call is deleted (best-effort) and
    // the caller adopts nothing; a failed delete stays pending for drain().
    // Rejects once beginStop() has been called, and re-checks stopping after
    // creating so a stop interleaved with creation never publishes a member.
    Status acquire(size_t count, std::vector<JettyPtr>& jetties,
                   std::vector<size_t>& jfc_indices);

    // Retires drained (reset-succeeded) Jetties. Without a rearm proof every
    // returned member is deleted rather than idled for reuse. The caller
    // guarantees every Jetty was reset after its last WR completed. A failed
    // delete keeps the member pending for the next drain() retry.
    void release(std::vector<JettyPtr> jetties,
                 std::vector<size_t> jfc_indices);

    // Freezes new leases. After beginStop(), acquire() rejects and release()
    // still retires returned members without publishing. Idempotent.
    void beginStop();

    // Deletes every pending member. Called from UbContext::shutdown before the
    // JFCs are closed so no Jetty outlives its parent JFC. Handles whose delete
    // fails are kept pending for the next drain retry.
    Status drain();

    // Drops the pool's strong references to the Context handle and JFCs. Call
    // after drain() and before closeContext()/JFC close so an externally held
    // pool shared_ptr no longer pins the native Context (C12 / G-02).
    void detach();

    // Number of members the pool still owns pending deletion (never leased).
    [[nodiscard]] size_t idleCount() const;
    [[nodiscard]] size_t createdCount() const;

   private:
    const std::shared_ptr<UrmaAdapter> adapter_;
    std::shared_ptr<Context> context_handle_;  // detachable (G-02)
    std::vector<std::shared_ptr<UbJfc>> jfcs_;  // detachable (G-02)
    const JettyOptions options_;

    mutable std::mutex mutex_;  // protects retired_/stopping_; no native calls
    // Used RESET members pending deletion. Never re-leased (no rearm proof).
    std::vector<std::pair<JettyPtr, size_t>> retired_;
    bool stopping_{false};
    size_t created_count_{0};
    size_t next_jfc_rotation_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_JETTY_POOL_H_