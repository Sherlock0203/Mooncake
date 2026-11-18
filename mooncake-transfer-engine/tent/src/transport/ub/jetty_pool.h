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

// Simple send-Jetty reuse pool, one per UbContext (RM Legacy only, gated by
// transports.ub.jetty_reuse.enable). Endpoints lease their whole Jetty set
// exclusively in prepare(); a retired incarnation returns every Jetty whose
// reset succeeded (a RESET after the WRs drained is a sufficient fence), and
// a fresh incarnation re-leases them LIFO before any new createJetty. This
// avoids createJetty/deleteJetty churn without touching the data plane: a
// leased Jetty is still owned by exactly one endpoint incarnation at a time
// and the imported-target positional pairing is unchanged.
class UbSendJettyPool {
   public:
    UbSendJettyPool(std::shared_ptr<UrmaAdapter> adapter,
                    const ContextPtr& context_handle,
                    std::vector<std::shared_ptr<UbJfc>> jfcs,
                    JettyOptions options);

    UbSendJettyPool(const UbSendJettyPool&) = delete;
    UbSendJettyPool& operator=(const UbSendJettyPool&) = delete;

    // Acquires count Jetties exclusively: idle Jetties are handed out LIFO
    // first and the remainder is created on the context's JFCs
    // (round-robin). On a creation failure the call is atomic: every Jetty
    // taken in this call is returned to the idle set (all clean, never
    // posted since their last reset) so the caller adopts nothing.
    Status acquire(size_t count, std::vector<JettyPtr>& jetties,
                   std::vector<size_t>& jfc_indices);

    // Returns drained (reset-succeeded) Jetties to the idle set. The caller
    // guarantees every Jetty was reset after its last WR completed.
    void release(std::vector<JettyPtr> jetties,
                 std::vector<size_t> jfc_indices);

    // Deletes every idle Jetty. Called from UbContext::shutdown before the
    // JFCs are closed so no idle Jetty outlives its parent JFC. Handles whose
    // delete fails are kept in the idle set for the next drain retry.
    Status drain();

    [[nodiscard]] size_t idleCount() const;
    [[nodiscard]] size_t createdCount() const;

   private:
    const std::shared_ptr<UrmaAdapter> adapter_;
    const ContextPtr context_handle_;
    const std::vector<std::shared_ptr<UbJfc>> jfcs_;
    const JettyOptions options_;

    mutable std::mutex mutex_;  // protects idle_; no native calls held
    std::vector<std::pair<JettyPtr, size_t>> idle_;  // LIFO: back() out first
    size_t created_count_{0};
    size_t next_jfc_rotation_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_JETTY_POOL_H_
