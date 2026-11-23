// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/tombstone.h"

#include <utility>

namespace mooncake::tent::ub {

TombstoneTable::TombstoneTable(size_t capacity)
    : capacity_(capacity == 0 ? 1 : capacity) {}

Status TombstoneTable::insert(const JettyTombstone& stone) {
    if (stone.context_incarnation == 0) {
        // INV-ID-1: an entry without a global identity cannot be told apart
        // from another Context's entry, so it must never enter the table.
        // Fail closed and count it; the caller keeps its responsibility.
        std::lock_guard<std::mutex> lock(mutex_);
        ++invalid_identity_;
        return Status::InvalidArgument(
            "UB tombstone has no context incarnation; refusing to record an "
            "entry that cannot be distinguished across Contexts" LOC_MARK);
    }
    const ResourceIdentity identity{stone.context_incarnation, stone.kind,
                                    stone.native_id, stone.generation};
    std::lock_guard<std::mutex> lock(mutex_);
    if (by_identity_.count(identity) != 0) {
        // Idempotent re-insert (a retried retirement of the same generation).
        return Status::OK();
    }
    if (by_identity_.size() >= capacity_) {
        ++rejected_;
        return Status::TooManyRequests(
            "UB tombstone table is full; backpressure the caller instead of "
            "shedding a native owner" LOC_MARK);
    }
    const ResourceEntity entity = ResourceEntity::of(identity);
    by_identity_.emplace(identity, stone);
    auto newest = newest_by_entity_.find(entity);
    if (newest == newest_by_entity_.end()) {
        newest_by_entity_.emplace(entity, stone.generation);
    } else if (newest->second < stone.generation) {
        // Same native object, newer generation: the newer stone supersedes it
        // for attribution, while the older entry stays until it is erased.
        newest->second = stone.generation;
    }
    return Status::OK();
}

std::optional<JettyTombstone> TombstoneTable::lookup(
    const ResourceEntity& entity) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto newest = newest_by_entity_.find(entity);
    if (newest == newest_by_entity_.end()) return std::nullopt;
    auto stone = by_identity_.find(
        ResourceIdentity{entity.context_incarnation, entity.kind,
                         entity.native_id, newest->second});
    if (stone == by_identity_.end()) return std::nullopt;
    return stone->second;
}

std::optional<JettyTombstone> TombstoneTable::lookup(EntityKind kind,
                                                     uint32_t native_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    bool found = false;
    uint64_t newest_generation = 0;
    JettyTombstone newest_stone;
    for (const auto& [identity, stone] : by_identity_) {
        if (identity.kind != kind || identity.native_id != native_id) continue;
        if (found && identity.generation <= newest_generation) continue;
        found = true;
        newest_generation = identity.generation;
        newest_stone = stone;
    }
    if (!found) return std::nullopt;
    return newest_stone;
}

bool TombstoneTable::erase(const ResourceIdentity& identity) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto stone = by_identity_.find(identity);
    if (stone == by_identity_.end()) return false;
    by_identity_.erase(stone);
    ++erased_;
    const ResourceEntity entity = ResourceEntity::of(identity);
    auto newest = newest_by_entity_.find(entity);
    if (newest == newest_by_entity_.end()) return true;
    if (newest->second != identity.generation) return true;
    // The erased entry owned the "newest" pointer; recompute it from the
    // remaining generations of the same entity. The table is capacity-bounded,
    // so this linear scan is only paid once per proven destruction.
    bool remaining = false;
    uint64_t best = 0;
    for (const auto& [candidate, ignored] : by_identity_) {
        (void)ignored;
        if (candidate.context_incarnation != identity.context_incarnation ||
            candidate.kind != identity.kind ||
            candidate.native_id != identity.native_id) {
            continue;
        }
        if (!remaining || candidate.generation > best) {
            best = candidate.generation;
            remaining = true;
        }
    }
    if (remaining) {
        newest->second = best;
    } else {
        newest_by_entity_.erase(newest);
    }
    return true;
}

size_t TombstoneTable::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return by_identity_.size();
}

size_t TombstoneTable::capacity() const { return capacity_; }

bool TombstoneTable::full() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return by_identity_.size() >= capacity_;
}

uint64_t TombstoneTable::rejectedInserts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return rejected_;
}

uint64_t TombstoneTable::invalidIdentityInserts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return invalid_identity_;
}

uint64_t TombstoneTable::erased() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return erased_;
}

}  // namespace mooncake::tent::ub
