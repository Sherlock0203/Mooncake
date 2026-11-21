// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/sha256.h"

#include <cstring>

namespace mooncake::tent::ub {
namespace {

constexpr uint32_t kRoundConstants[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

inline uint32_t rotateRight(uint32_t value, unsigned bits) {
    return (value >> bits) | (value << (32 - bits));
}

void compressChunk(uint32_t state[8], const uint8_t chunk[64]) {
    uint32_t w[64];
    for (size_t i = 0; i < 16; ++i) {
        w[i] = (static_cast<uint32_t>(chunk[i * 4]) << 24) |
               (static_cast<uint32_t>(chunk[i * 4 + 1]) << 16) |
               (static_cast<uint32_t>(chunk[i * 4 + 2]) << 8) |
               static_cast<uint32_t>(chunk[i * 4 + 3]);
    }
    for (size_t i = 16; i < 64; ++i) {
        const uint32_t s0 = rotateRight(w[i - 15], 7) ^
                            rotateRight(w[i - 15], 18) ^ (w[i - 15] >> 3);
        const uint32_t s1 = rotateRight(w[i - 2], 17) ^
                            rotateRight(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }

    uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
    uint32_t e = state[4], f = state[5], g = state[6], h = state[7];
    for (size_t i = 0; i < 64; ++i) {
        const uint32_t sum1 =
            rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
        const uint32_t choice = (e & f) ^ (~e & g);
        const uint32_t temp1 = h + sum1 + choice + kRoundConstants[i] + w[i];
        const uint32_t sum0 =
            rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
        const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
        const uint32_t temp2 = sum0 + majority;
        h = g;
        g = f;
        f = e;
        e = d + temp1;
        d = c;
        c = b;
        b = a;
        a = temp1 + temp2;
    }
    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
    state[5] += f;
    state[6] += g;
    state[7] += h;
}

}  // namespace

void sha256Digest(std::string_view input,
                  std::array<uint8_t, kSha256DigestBytes>& digest) {
    uint32_t state[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                         0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};

    const size_t total = input.size();
    size_t offset = 0;
    uint8_t chunk[64];
    while (total - offset >= 64) {
        std::memcpy(chunk, input.data() + offset, 64);
        compressChunk(state, chunk);
        offset += 64;
    }

    // Final block: remaining bytes, one 0x80 pad byte, zeros and the 64-bit
    // big-endian bit length. The message plus 0x80 always fits in two blocks.
    const size_t remainder = total - offset;
    std::memset(chunk, 0, sizeof(chunk));
    std::memcpy(chunk, input.data() + offset, remainder);
    chunk[remainder] = 0x80;
    const bool needs_extra_block = remainder >= 56;
    const uint64_t bit_length = static_cast<uint64_t>(total) * 8;
    if (!needs_extra_block) {
        for (size_t i = 0; i < 8; ++i) {
            chunk[63 - i] = static_cast<uint8_t>(bit_length >> (8 * i));
        }
        compressChunk(state, chunk);
    } else {
        compressChunk(state, chunk);
        std::memset(chunk, 0, sizeof(chunk));
        for (size_t i = 0; i < 8; ++i) {
            chunk[63 - i] = static_cast<uint8_t>(bit_length >> (8 * i));
        }
        compressChunk(state, chunk);
    }

    for (size_t i = 0; i < 8; ++i) {
        digest[i * 4] = static_cast<uint8_t>(state[i] >> 24);
        digest[i * 4 + 1] = static_cast<uint8_t>(state[i] >> 16);
        digest[i * 4 + 2] = static_cast<uint8_t>(state[i] >> 8);
        digest[i * 4 + 3] = static_cast<uint8_t>(state[i]);
    }
}

std::string sha256Hex(std::string_view input) {
    std::array<uint8_t, kSha256DigestBytes> digest{};
    sha256Digest(input, digest);
    static const char* kHexDigits = "0123456789abcdef";
    std::string hex;
    hex.reserve(kSha256HexLength);
    for (const uint8_t byte : digest) {
        hex.push_back(kHexDigits[byte >> 4]);
        hex.push_back(kHexDigits[byte & 0x0f]);
    }
    return hex;
}

bool isCanonicalSha256Hex(std::string_view text) {
    if (text.size() != kSha256HexLength) return false;
    for (const char c : text) {
        const bool is_lower_hex =
            (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        if (!is_lower_hex) return false;
    }
    return true;
}

}  // namespace mooncake::tent::ub
