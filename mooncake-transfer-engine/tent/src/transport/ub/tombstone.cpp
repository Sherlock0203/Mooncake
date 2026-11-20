// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/tombstone.h"

#include <utility>

namespace mooncake::tent::ub {

TombstoneTable::TombstoneTable(size_t capacity)
    : capacity_(capacity == 0 ? 1 : capacity) {}

uint64_t TombstoneTable::entityKey(EntityKind kind,
                                   uint32_t native_id) noexcept {
    return (static_cast<uint64_t>(kind) << 32) | native_id;
}

Status TombstoneTable::insert(const JettyTombstone& stone) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (by_generation_.count(stone.generation) != 0) {
        // Idempotent re-insert (a retried retirement of the same generation).
        return Status::OK();
    }
    if (by_generation_.size() >= capacity_) {
        ++rejected_;
        return Status::TooManyRequests(
            "UB tombstone table is full; backpressure the caller instead of "
            "shedding a native owner" LOC_MARK);
    }
    const uint64_t key = entityKey(stone.kind, stone.native_id);
    by_generation_.emplace(stone.generation, stone);
    newest_by_entity_[key] = stone.generation;
    return Status::OK();
}

std::optional<JettyTombstone> TombstoneTable::lookup(EntityKind kind,
                                                     uint32_t native_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entity = newest_by_entity_.find(entityKey(kind, native_id));
    if (entity == newest_by_entity_.end()) return std::nullopt;
    auto stone = by_generation_.find(entity->second);
    if (stone == by_generation_.end()) return std::nullopt;
    return stone->second;
}

bool TombstoneTable::erase(uint64_t generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto stone = by_generation_.find(generation);
    if (stone == by_generation_.end()) return false;
    const uint64_t key = entityKey(stone->second.kind, stone->second.native_id);
    by_generation_.erase(stone);
    auto entity = newest_by_entity_.find(key);
    if (entity != newest_by_entity_.end() && entity->second == generation) {
        newest_by_entity_.erase(entity);
    }
    ++erased_;
    return true;
}

size_t TombstoneTable::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return by_generation_.size();
}

size_t TombstoneTable::capacity() const { return capacity_; }

bool TombstoneTable::full() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return by_generation_.size() >= capacity_;
}

uint64_t TombstoneTable::rejectedInserts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return rejected_;
}

uint64_t TombstoneTable::erased() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return erased_;
}

}  // namespace mooncake::tent::ub
