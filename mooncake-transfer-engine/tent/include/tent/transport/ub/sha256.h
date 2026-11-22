// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_SHA256_H_
#define TENT_TRANSPORT_UB_SHA256_H_

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace mooncake::tent::ub {

// Self-contained SHA-256 (FIPS 180-4). The bootstrap v3 inventory checksum
// needs a stable digest without introducing an OpenSSL dependency into tent;
// correctness is pinned by golden-vector unit tests ("abc" and the empty
// input from the FIPS 180-4 examples).
constexpr size_t kSha256DigestBytes = 32;
constexpr size_t kSha256HexLength = 2 * kSha256DigestBytes;

void sha256Digest(std::string_view input,
                  std::array<uint8_t, kSha256DigestBytes>& digest);

// Lowercase hexadecimal rendering of the SHA-256 digest (always 64 chars).
std::string sha256Hex(std::string_view input);

// True when the string is exactly 64 lowercase hexadecimal characters, the
// canonical wire form of an inventory checksum.
bool isCanonicalSha256Hex(std::string_view text);

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_SHA256_H_
