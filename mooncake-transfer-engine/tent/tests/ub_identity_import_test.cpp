// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3g-d: physical port identity normalization (design §5.3 / §8 / §11).
//
// The budget ledger keys native reservations by this identity. These tests pin
// the two failure modes that would silently double a device budget: two
// spellings of one port producing two keys, and an empty component being
// "normalized" into a guessed key instead of failing closed.

#include <gtest/gtest.h>

#include <string>

#include "tent/transport/ub/identity_import.h"

namespace mooncake::tent::ub {
namespace {

TEST(UbIdentityImportTest, NormalizesPathAndEidIntoAStableKey) {
    const auto identity = makePhysicalDeviceIdentity(
        "/dev/uburma/0/", " 0001:AB00:0000:0000:0000:0000:0000:0001 ");
    ASSERT_TRUE(identity.valid());
    EXPECT_EQ(identity.native_device_path, "/dev/uburma/0");
    EXPECT_EQ(identity.eid, "0001:ab00:0000:0000:0000:0000:0000:0001");
    EXPECT_EQ(identity.device_key,
              "/dev/uburma/0:0001:ab00:0000:0000:0000:0000:0000:0001");

    // Every alias of the same port collapses to the identical key.
    const auto aliased = makePhysicalDeviceIdentity(
        "//dev//uburma//0", "0001:AB00:0000:0000:0000:0000:0000:0001");
    EXPECT_EQ(aliased.device_key, identity.device_key);
}

TEST(UbIdentityImportTest, PreservesTheRootPath) {
    const auto identity = makePhysicalDeviceIdentity(
        "/", "0001:0000:0000:0000:0000:0000:0000:0001");
    ASSERT_TRUE(identity.valid());
    EXPECT_EQ(identity.native_device_path, "/");
    EXPECT_EQ(identity.device_key, "/:0001:0000:0000:0000:0000:0000:0000:0001");
}

TEST(UbIdentityImportTest, RejectsEmptyComponentsInsteadOfGuessing) {
    for (const auto& [path, eid] :
         {std::pair<std::string, std::string>{"", "eid0"},
          std::pair<std::string, std::string>{"   ", "eid0"},
          std::pair<std::string, std::string>{"/dev/x", ""},
          std::pair<std::string, std::string>{"/dev/x", "   "}}) {
        SCOPED_TRACE(path + "|" + eid);
        const auto identity = makePhysicalDeviceIdentity(path, eid);
        EXPECT_FALSE(identity.valid());
        EXPECT_TRUE(identity.device_key.empty());
    }
}

TEST(UbIdentityImportTest, CacheIsIdempotentAndCountsBothOutcomes) {
    PhysicalDeviceIdentityCache cache;
    PhysicalDeviceIdentity first;
    ASSERT_TRUE(cache
                    .import("/dev/uburma/0/",
                            "0001:AB00:0000:0000:0000:0000:0000:0001", first)
                    .ok());
    ASSERT_TRUE(first.valid());
    EXPECT_EQ(cache.size(), 1U);

    // A different spelling of the same port is the same entry.
    PhysicalDeviceIdentity second;
    ASSERT_TRUE(cache
                    .import("/dev/uburma/0",
                            "0001:ab00:0000:0000:0000:0000:0000:0001", second)
                    .ok());
    EXPECT_EQ(second.device_key, first.device_key);
    EXPECT_EQ(cache.size(), 1U);
    EXPECT_EQ(cache.importOk(), 2U);
    EXPECT_EQ(cache.importFailed(), 0U);

    PhysicalDeviceIdentity rejected;
    const Status status = cache.import("/dev/uburma/1", "", rejected);
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_FALSE(rejected.valid());
    EXPECT_EQ(cache.importFailed(), 1U);
    EXPECT_EQ(cache.size(), 1U);
}

TEST(UbIdentityImportTest, DeviceInfoOverloadUsesPathAndEid) {
    DeviceInfo device;
    device.topology_name = "ub:fake0:eid0";
    device.native_device_name = "fake0";
    device.native_device_path = "/dev/uburma/fake0/";
    device.eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    PhysicalDeviceIdentityCache cache;
    PhysicalDeviceIdentity identity;
    ASSERT_TRUE(cache.import(device, identity).ok());
    EXPECT_EQ(identity.native_device_path, "/dev/uburma/fake0");
    // The legacy key included the native device name; the normalized identity
    // deliberately does not, so an alias rename cannot fork the budget ledger.
    EXPECT_EQ(identity.device_key,
              "/dev/uburma/fake0:0001:0000:0000:0000:0000:0000:0000:0000");
}

}  // namespace
}  // namespace mooncake::tent::ub
