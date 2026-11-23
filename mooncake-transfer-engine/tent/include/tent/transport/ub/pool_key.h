// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Pool identity types shared by the send-Jetty pool and the asynchronous
// drain controller (design tent-ub-async-drain-design.md §4.2).
//
// These used to live in send_jetty_pool.h. The drain controller needs the
// exact same identities but cannot include the pool header: the pool includes
// the controller (it drives beginDrain()), so a dedicated leaf header breaks
// the cycle without changing a single name or hash.

#ifndef TENT_TRANSPORT_UB_POOL_KEY_H_
#define TENT_TRANSPORT_UB_POOL_KEY_H_

#include <cstddef>
#include <cstdint>
#include <functional>

namespace mooncake::tent::ub {

// PoolKey partitions send Jetties by Context and LocalSendProfile (D01).
// Two attempts with different profiles never share a member.
struct PoolKey {
    uint64_t context_incarnation = 0;
    uint16_t trans_mode = 0;
    uint8_t priority = 0;
    uint32_t bind_tp_type_mask = 0;

    bool operator==(const PoolKey&) const = default;

    // A zero context incarnation is the unassigned sentinel (bootstrap v2
    // rejects it); a pool key without one cannot partition members safely.
    [[nodiscard]] bool valid() const noexcept {
        return context_incarnation != 0;
    }
};

struct PoolKeyHash {
    size_t operator()(const PoolKey& key) const noexcept {
        size_t seed = std::hash<uint64_t>{}(key.context_incarnation);
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<uint16_t>{}(key.trans_mode));
        combine(std::hash<uint8_t>{}(key.priority));
        combine(std::hash<uint32_t>{}(key.bind_tp_type_mask));
        return seed;
    }
};

// AttemptKey identifies one business attempt of one slice (D01). Retries
// must use a new attempt_generation; a lease is bound to exactly one
// attempt and one native post of exactly one WR (INV03).
struct AttemptKey {
    uint64_t task_id = 0;
    uint64_t slice_id = 0;
    uint64_t attempt_generation = 0;

    bool operator==(const AttemptKey&) const = default;

    [[nodiscard]] bool valid() const noexcept {
        return task_id != 0 && slice_id != 0 && attempt_generation != 0;
    }
};

// PoolLaneKey identifies one pooled member generation. A retired member keeps
// its lane so late completions can be attributed; the generation is never
// reused by a fresh native Jetty (D01/INV15).
//
// Merge note (M-1): the pooling side spells this type `LaneKey`, but this
// baseline's W1 probe contract already froze `LaneKey` as the probe channel
// identity (probe_resources.h) in the same namespace. Only the name differs;
// the shape and the "generation is never reused" rule are unchanged.
struct PoolLaneKey {
    uint32_t jetty_id = 0;
    uint64_t lane_generation = 0;

    bool operator==(const PoolLaneKey&) const = default;
};

struct PoolLaneKeyHash {
    size_t operator()(const PoolLaneKey& key) const noexcept {
        size_t seed = std::hash<uint32_t>{}(key.jetty_id);
        seed ^= std::hash<uint64_t>{}(key.lane_generation) +
                0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        return seed;
    }
};

// Entity kinds covered by the global tombstone table. The business path only
// creates kJetty entries today; JFC/JFR entries exist so an event can be
// attributed once those entities join the retirement chain.
enum class EntityKind : uint8_t {
    kJetty = 0,
    kJfc = 1,
    kJfr = 2,
};

inline const char* entityKindName(EntityKind kind) noexcept {
    switch (kind) {
        case EntityKind::kJetty:
            return "jetty";
        case EntityKind::kJfc:
            return "jfc";
        case EntityKind::kJfr:
            return "jfr";
    }
    return "unknown";
}

// INV-ID-1: the globally unique identity of one native resource generation.
// `context_incarnation` comes from UbContext::incarnation() (process-wide
// unique and never reused), so two Contexts can never alias each other even
// when their per-pool lane generations both start at 1. Every table shared
// across Contexts must key on this, never on the pool-local generation alone.
struct ResourceIdentity {
    uint64_t context_incarnation = 0;
    EntityKind kind = EntityKind::kJetty;
    uint32_t native_id = 0;
    // Pool-local lane generation; unique within one Context. Not part of the
    // cross-context identity of the *object*, only of the *generation*.
    uint64_t generation = 0;

    bool operator==(const ResourceIdentity&) const = default;
};

struct ResourceIdentityHash {
    size_t operator()(const ResourceIdentity& identity) const noexcept {
        size_t seed = std::hash<uint64_t>{}(identity.context_incarnation);
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<uint8_t>{}(static_cast<uint8_t>(identity.kind)));
        combine(std::hash<uint32_t>{}(identity.native_id));
        combine(std::hash<uint64_t>{}(identity.generation));
        return seed;
    }
};

// One native object independent of its generation: the "newest entry for this
// object" index key. Context-scoped so two Contexts never share a row.
struct ResourceEntity {
    uint64_t context_incarnation = 0;
    EntityKind kind = EntityKind::kJetty;
    uint32_t native_id = 0;

    bool operator==(const ResourceEntity&) const = default;

    [[nodiscard]] static ResourceEntity of(const ResourceIdentity& identity) {
        return ResourceEntity{identity.context_incarnation, identity.kind,
                              identity.native_id};
    }
};

struct ResourceEntityHash {
    size_t operator()(const ResourceEntity& entity) const noexcept {
        size_t seed = std::hash<uint64_t>{}(entity.context_incarnation);
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<uint8_t>{}(static_cast<uint8_t>(entity.kind)));
        combine(std::hash<uint32_t>{}(entity.native_id));
        return seed;
    }
};

// One pooled lane (one Jetty generation of one Context) for the cross-Context
// reference ledger the drain controller queries. Pool-local generation alone
// is not enough: every pool starts at 1, so two Contexts would share a count
// and convergence would be reported too early.
struct LaneRefKey {
    uint64_t context_incarnation = 0;
    uint64_t lane_generation = 0;

    bool operator==(const LaneRefKey&) const = default;
};

struct LaneRefKeyHash {
    size_t operator()(const LaneRefKey& key) const noexcept {
        size_t seed = std::hash<uint64_t>{}(key.context_incarnation);
        seed ^= std::hash<uint64_t>{}(key.lane_generation) +
                0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        return seed;
    }
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_POOL_KEY_H_
