// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Tombstone table for native entities whose destruction is not proven
// (design §7.2). A tombstone keeps the identity of a retired business member
// (Context/device incarnation, entity type, native id, generation, retirement
// epoch and the budget it still holds) so that:
//   1. a completion that no live record owns can be attributed to a retired
//      entity instead of being treated as a topology-wide fault, and
//   2. native budget is only released when the tombstone is erased, i.e. when
//      the provider proved the entity was destroyed.
//
// The table is bounded by `transports/ub/tombstone_budget`. When it is full,
// insert() fails and the caller must backpressure (pause refill / stop
// admission). Entries are never TTL-evicted: an unproven owner is not made
// "safe" by the passage of time.

#ifndef TENT_TRANSPORT_UB_TOMBSTONE_H_
#define TENT_TRANSPORT_UB_TOMBSTONE_H_

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "tent/common/status.h"
#include "tent/transport/ub/pool_key.h"

namespace mooncake::tent::ub {

// Whether the tombstone's owner is still responsible for the entity.
enum class Responsibility : uint8_t {
    kUnknown = 0,   // destruction and event ownership are both unproven
    kRetiring = 1,  // a delete is in flight; budget stays charged
    kProven = 2,    // provider proved destruction; the entry may be erased
};

inline const char* responsibilityName(Responsibility value) noexcept {
    switch (value) {
        case Responsibility::kUnknown:
            return "unknown";
        case Responsibility::kRetiring:
            return "retiring";
        case Responsibility::kProven:
            return "proven";
    }
    return "unknown";
}

struct JettyTombstone {
    uint64_t context_incarnation = 0;
    uint64_t device_incarnation = 0;
    EntityKind kind = EntityKind::kJetty;
    uint32_t native_id = 0;
    uint64_t generation = 0;
    uint64_t error_epoch = 0;
    Responsibility responsibility = Responsibility::kUnknown;
    // Native budget this entry still holds, in Jetty slots. Released only when
    // the entry is erased after proven destruction.
    uint64_t budget_hold = 0;
};

class TombstoneTable {
   public:
    explicit TombstoneTable(size_t capacity);

    TombstoneTable(const TombstoneTable&) = delete;
    TombstoneTable& operator=(const TombstoneTable&) = delete;

    // Inserts one tombstone keyed by its full ResourceIdentity. Idempotent for
    // the same identity; two different identities with the same numeric
    // generation (different Context or different native object) are stored
    // independently instead of silently colliding. A full table returns
    // TooManyRequests and does not shed, evict or trim anything; the caller
    // must backpressure.
    //
    // Fail-closed: a stone without a context incarnation has no global
    // identity, so it is rejected with InvalidArgument and counted.
    Status insert(const JettyTombstone& stone);

    // Returns the newest tombstone for one global entity, if any. This is the
    // event-attribution path: a completion without a live record can still be
    // attributed to a retired entity.
    [[nodiscard]] std::optional<JettyTombstone> lookup(
        const ResourceEntity& entity) const;

    // Attribution-only compatibility overload: newest entry for
    // (kind, native_id) across every Context. Used where the caller only has a
    // native id (drain markers) and only needs "a retired entity exists".
    [[nodiscard]] std::optional<JettyTombstone> lookup(
        EntityKind kind, uint32_t native_id) const;

    // Erases exactly the entry identified by `identity` after the provider
    // proved destruction. Returns true when that entry was removed (and its
    // budget may be released). Never touches another Context's entry.
    bool erase(const ResourceIdentity& identity);

    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t capacity() const;
    [[nodiscard]] bool full() const;
    [[nodiscard]] uint64_t rejectedInserts() const;
    // Inserts rejected because they carried no global identity (context
    // incarnation == 0). Exposed separately so the fail-closed path is
    // observable instead of looking like capacity backpressure.
    [[nodiscard]] uint64_t invalidIdentityInserts() const;
    [[nodiscard]] uint64_t erased() const;

   private:
    mutable std::mutex mutex_;
    const size_t capacity_;
    std::unordered_map<ResourceIdentity, JettyTombstone, ResourceIdentityHash>
        by_identity_;
    // (context_incarnation, kind, native_id) -> generation of the newest entry.
    std::unordered_map<ResourceEntity, uint64_t, ResourceEntityHash>
        newest_by_entity_;
    uint64_t rejected_ = 0;
    uint64_t invalid_identity_ = 0;
    uint64_t erased_ = 0;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_TOMBSTONE_H_
