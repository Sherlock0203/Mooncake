// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_registry.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/identity_import.h"
#include "tent/transport/ub/topology_attrs.h"
#include "tent/transport/ub/ub_transport.h"
#include "tent/transport/ub/workers.h"

namespace mooncake::tent::ub {
namespace {

class FakeContext final : public Context {
   public:
    explicit FakeContext(DeviceInfo info) : info_(std::move(info)) {}
    bool valid() const noexcept override { return valid_; }
    const DeviceInfo& deviceInfo() const noexcept override { return info_; }
    int asyncFd() const noexcept override { return -1; }
    void close() { valid_ = false; }

   private:
    DeviceInfo info_;
    bool valid_{true};
};

class FakeJfc final : public Jfc {
   public:
    bool valid() const noexcept override { return valid_; }
    int eventFd() const noexcept override { return -1; }

    void push(Completion completion) {
        std::lock_guard<std::mutex> lock(mutex_);
        completions_.push_back(completion);
    }
    void poll(size_t maximum, std::vector<Completion>& output) {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!completions_.empty() && output.size() < maximum) {
            output.push_back(completions_.front());
            completions_.pop_front();
        }
    }
    void close() { valid_ = false; }

   private:
    std::mutex mutex_;
    std::deque<Completion> completions_;
    bool valid_{true};
};

SegmentDescriptor makeDescriptor(uint64_t address, uint64_t length) {
    return SegmentDescriptor{
        SegmentDescriptor::kSchemaVersion, 1, 16,
        std::to_string(address) + ":" + std::to_string(length)};
}

bool parseDescriptor(const SegmentDescriptor& descriptor, uint64_t& address,
                     uint64_t& length) {
    const auto colon = descriptor.hex.find(':');
    if (descriptor.schema_version != SegmentDescriptor::kSchemaVersion ||
        descriptor.urma_api_version != 1 || descriptor.urma_abi_size != 16 ||
        colon == std::string::npos) {
        return false;
    }
    try {
        address = std::stoull(descriptor.hex.substr(0, colon));
        length = std::stoull(descriptor.hex.substr(colon + 1));
        return length != 0;
    } catch (...) {
        return false;
    }
}

class FakeLocalSegment final : public LocalSegment {
   public:
    FakeLocalSegment(uint64_t address, uint64_t length)
        : address_(address),
          length_(length),
          descriptor_(makeDescriptor(address, length)) {}
    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeRemoteSegment final : public RemoteSegment {
   public:
    FakeRemoteSegment(uint64_t address, uint64_t length,
                      SegmentDescriptor descriptor)
        : address_(address),
          length_(length),
          descriptor_(std::move(descriptor)) {}
    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeJetty final : public Jetty {
   public:
    FakeJetty(uint32_t id, std::shared_ptr<FakeJfc> jfc, bool rm_mode = false)
        : id_(id), jfc_(std::move(jfc)), rm_mode_(rm_mode) {}
    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return 0; }
    std::shared_ptr<FakeJfc> jfc() const { return jfc_; }
    bool bound() const { return bound_; }
    bool rmMode() const { return rm_mode_; }
    void bind() { bound_ = true; }
    void unbind() { bound_ = false; }
    void close() {
        valid_ = false;
        bound_ = false;
    }

   private:
    uint32_t id_;
    std::shared_ptr<FakeJfc> jfc_;
    bool rm_mode_{false};
    bool valid_{true};
    bool bound_{false};
};

class FakeRemoteTarget final : public RemoteTarget {
   public:
    FakeRemoteTarget(std::string eid, uint32_t uasid, uint32_t id)
        : eid_(std::move(eid)), uasid_(uasid), id_(id) {}
    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return uasid_; }
    const std::string& eid() const noexcept override { return eid_; }
    void close() { valid_ = false; }

   private:
    std::string eid_;
    uint32_t uasid_{0};
    uint32_t id_{0};
    bool valid_{true};
};

// Minimal URMA adapter for auto-import tests: full operational contract, no
// fault injection.
class AutoImportAdapter final : public UrmaAdapter {
   public:
    explicit AutoImportAdapter(std::vector<DeviceInfo> devices)
        : devices_(std::move(devices)) {}

    bool available() const noexcept override { return true; }
    uint32_t nativeApiVersion() const noexcept override { return 1; }
    size_t nativeSegmentDescriptorSize() const noexcept override { return 16; }
    Status initialize() override {
        initialized_ = true;
        return Status::OK();
    }
    Status shutdown() override {
        initialized_ = false;
        return Status::OK();
    }
    bool initialized() const { return initialized_; }
    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        if (!initialized_) return Status::InvalidArgument("not initialized");
        devices = devices_;
        return Status::OK();
    }
    Status openContext(const DeviceInfo& device, ContextPtr& context) override {
        if (!initialized_) return Status::InvalidArgument("not initialized");
        for (const auto& candidate : devices_) {
            if (candidate.topology_name == device.topology_name) {
                context = std::make_shared<FakeContext>(device);
                return Status::OK();
            }
        }
        return Status::DeviceNotFound("fake device missing");
    }
    Status closeContext(ContextPtr& context) override {
        if (auto fake = std::dynamic_pointer_cast<FakeContext>(context)) {
            fake->close();
        }
        context.reset();
        return Status::OK();
    }
    Status createJfc(const ContextPtr&, const JfcOptions&,
                     JfcPtr& jfc) override {
        jfc = std::make_shared<FakeJfc>();
        return Status::OK();
    }
    Status deleteJfc(JfcPtr& jfc) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc)) fake->close();
        jfc.reset();
        return Status::OK();
    }
    Status registerLocalSegment(const ContextPtr&, uint64_t address,
                                size_t length, const SegmentOptions&,
                                LocalSegmentPtr& segment) override {
        if (length == 0) return Status::InvalidArgument("empty segment");
        segment = std::make_shared<FakeLocalSegment>(address, length);
        return Status::OK();
    }
    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        if (auto fake = std::dynamic_pointer_cast<FakeLocalSegment>(segment)) {
            fake->close();
        }
        segment.reset();
        return Status::OK();
    }
    Status importRemoteSegment(const ContextPtr&,
                               const SegmentDescriptor& descriptor,
                               const SegmentOptions&,
                               RemoteSegmentPtr& segment) override {
        uint64_t address = 0;
        uint64_t length = 0;
        if (!parseDescriptor(descriptor, address, length)) {
            return Status::InvalidArgument("bad descriptor");
        }
        segment =
            std::make_shared<FakeRemoteSegment>(address, length, descriptor);
        return Status::OK();
    }
    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        if (auto fake =
                std::dynamic_pointer_cast<FakeRemoteSegment>(segment)) {
            fake->close();
        }
        segment.reset();
        return Status::OK();
    }
    Status createJetty(const ContextPtr&, const JfcPtr& jfc,
                       const JettyOptions& options,
                       JettyPtr& jetty) override {
        auto fake_jfc = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake_jfc) return Status::InvalidArgument("bad JFC");
        const bool rm_mode = options.trans_mode == kUrmaTransportModeRm;
        if (rm_mode && (options.bind_tp_type_mask & kUrmaTpTypeCtpMask) == 0) {
            return Status::InvalidArgument(
                "RM JFS bind mask does not cover URMA_CTP");
        }
        jetty =
            std::make_shared<FakeJetty>(next_jetty_id_++, fake_jfc, rm_mode);
        return Status::OK();
    }
    Status deleteJetty(JettyPtr& jetty) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->close();
        }
        jetty.reset();
        return Status::OK();
    }
    Status bindJetty(const JettyPtr& jetty, const RemoteJettyInfo&) override {
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid())
            return Status::InvalidArgument("bad Jetty");
        if (fake->rmMode()) {
            return Status::InvalidArgument(
                "RM Legacy Jettys never bind a remote Jetty");
        }
        bind_jetty_calls_.fetch_add(1, std::memory_order_relaxed);
        fake->bind();
        return Status::OK();
    }
    Status unbindJetty(const JettyPtr& jetty) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->unbind();
        }
        return Status::OK();
    }
    Status resetJetty(const JettyPtr&) override { return Status::OK(); }
    Status importRemoteTarget(const ContextPtr&, const std::string& eid,
                              uint32_t uasid, uint32_t id, uint16_t trans_mode,
                              uint8_t target_type, uint8_t tp_type,
                              RemoteTargetPtr& target) override {
        target.reset();
        import_calls_.fetch_add(1, std::memory_order_relaxed);
        if (trans_mode != kUrmaTransportModeRm ||
            target_type != kUrmaTargetTypeJetty ||
            tp_type != kUrmaTpTypeCtp) {
            return Status::InvalidArgument(
                "RM target profile must be RM/JETTY/CTP");
        }
        if (id == 0) {
            return Status::InvalidArgument("RM target Jetty ID is zero");
        }
        imported_ids_.push_back(id);
        target = std::make_shared<FakeRemoteTarget>(eid, uasid, id);
        return Status::OK();
    }
    Status unimportRemoteTarget(RemoteTargetPtr& target) override {
        unimport_calls_.fetch_add(1, std::memory_order_relaxed);
        if (auto fake = std::dynamic_pointer_cast<FakeRemoteTarget>(target)) {
            fake->close();
        }
        target.reset();
        return Status::OK();
    }
    Status quiesceJetty(const JettyPtr& jetty, uint32_t timeout_ms,
                        std::vector<Completion>& completions) override {
        completions.clear();
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid() || timeout_ms == 0) {
            return Status::InvalidArgument("bad Jetty quiesce");
        }
        return Status::OK();
    }
    Status post(const JettyPtr& jetty, const std::vector<WorkRequest>& requests,
                size_t& posted_count) override {
        posted_count = 0;
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid() ||
            (!fake->bound() && !fake->rmMode())) {
            return Status::InvalidArgument("unbound Jetty");
        }
        const bool rm_mode = fake->rmMode();
        for (const auto& request : requests) {
            if (rm_mode != (request.target != nullptr)) {
                return Status::InvalidArgument(
                    rm_mode ? "RM work requests must set an explicit target"
                            : "RC work requests must not set a target");
            }
            if (!request.local_segment || !request.remote_segment ||
                request.token == 0 || request.length == 0) {
                return Status::InvalidArgument("bad WR");
            }
        }
        for (const auto& request : requests) {
            if (rm_mode) {
                rm_post_calls_.fetch_add(1, std::memory_order_relaxed);
            }
            if (request.operation == Operation::WRITE) {
                std::memcpy(reinterpret_cast<void*>(request.remote_address),
                            reinterpret_cast<const void*>(request.local_address),
                            request.length);
            } else {
                std::memcpy(reinterpret_cast<void*>(request.local_address),
                            reinterpret_cast<const void*>(request.remote_address),
                            request.length);
            }
            fake->jfc()->push(
                Completion{CompletionCategory::SUCCESS, 0, request.token,
                           static_cast<uint32_t>(request.length), fake->id()});
            ++posted_count;
        }
        return Status::OK();
    }
    Status poll(const JfcPtr& jfc, size_t maximum,
                std::vector<Completion>& completions) override {
        completions.clear();
        auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake || !fake->valid()) return Status::InvalidArgument("bad JFC");
        fake->poll(maximum, completions);
        return Status::OK();
    }

    uint64_t importCalls() const {
        return import_calls_.load(std::memory_order_relaxed);
    }
    uint64_t unimportCalls() const {
        return unimport_calls_.load(std::memory_order_relaxed);
    }
    uint64_t rmPostCalls() const {
        return rm_post_calls_.load(std::memory_order_relaxed);
    }
    uint64_t bindJettyCalls() const {
        return bind_jetty_calls_.load(std::memory_order_relaxed);
    }
    const std::vector<uint32_t>& importedIds() const { return imported_ids_; }

   private:
    std::vector<DeviceInfo> devices_;
    bool initialized_{false};
    uint32_t next_jetty_id_{1};
    std::vector<uint32_t> imported_ids_;
    std::atomic<uint64_t> import_calls_{0};
    std::atomic<uint64_t> unimport_calls_{0};
    std::atomic<uint64_t> rm_post_calls_{0};
    std::atomic<uint64_t> bind_jetty_calls_{0};
};

DeviceInfo fakeDevice() {
    DeviceInfo info;
    info.topology_name = "ub:fake0:eid0";
    info.native_device_name = "fake0";
    info.native_device_path = "/fake/fake0";
    info.eid_index = 0;
    info.eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    info.active = true;
    // Large enough for the default jfc_per_context (6 contexts x 2 JFCs each)
    // so install-level tests exercise the default sizing path.
    info.capabilities.max_jfc = 256;
    info.capabilities.max_jetty = 64;
    return info;
}

DeviceInfo rmCapableDevice() {
    auto device = fakeDevice();
    device.capabilities.transport_modes = 3;  // RC|RM
    device.kind = DeviceKind::PHYSICAL;
    // Mirrors what publishLocalDevices() publishes in production: identity
    // resolution (device_selection.h) stamps EXPLICIT_CONFIG on every device
    // selected through a device_identities record, manual or auto-imported.
    device.kind_source = DeviceKindSource::EXPLICIT_CONFIG;
    device.capabilities.rm_tp_cap = kUrmaTpTypeCtpMask;
    device.capabilities.priority_tp_type_masks[kUrmaMaxPriority] =
        kUrmaTpTypeCtpMask;
    return device;
}

std::shared_ptr<Topology> fakeTopology() {
    auto topology = std::make_shared<Topology>();
    Topology::NicEntry nic{.name = "ub:fake0:eid0",
                           .pci_bus_id = "0000:00:00.0",
                           .type = Topology::NIC_UB,
                           .numa_node = 0};
    encodeTopologyDeviceAttributes(fakeDevice(), 0, nic.device_attrs);
    topology->nic_list_.push_back(std::move(nic));
    Topology::MemEntry memory;
    memory.name = kWildcardLocation;
    memory.type = Topology::MEM_HOST;
    memory.numa_node = -1;
    memory.device_list[0].push_back(0);
    topology->mem_list_.push_back(std::move(memory));
    return topology;
}

void applyRmLegacyDefaults(Config& config) {
    config.set("transports/ub/enable", true);
    config.set("transports/ub/runtime_profile", "rm_legacy");
    config.set("transports/ub/trans_mode", "rm");
    config.set("transports/ub/physical_only", true);
}

// Builds a registered segment descriptor whose device entry carries UB
// transport metadata exactly the way publishLocalDevices() publishes it.
SegmentDesc makeSegmentWithUbDevice(const std::string& name,
                                    const DeviceInfo& device) {
    SegmentDesc segment;
    segment.name = name;
    segment.type = SegmentType::Memory;
    MemorySegmentDesc memory;
    memory.topology = *fakeTopology();
    DeviceDesc device_desc;
    device_desc.name = device.topology_name;
    device_desc.transport_attrs[TransportType::UB] =
        encodeDeviceMetadata(0, device);
    memory.devices.push_back(std::move(device_desc));
    segment.detail = std::move(memory);
    return segment;
}

// ---------------------------------------------------------------------------
// Collection: coverage of every manual field, skip/fail-closed semantics
// ---------------------------------------------------------------------------

TEST(UbAutoImportTest, CollectLocalDeviceIdentitiesCoversManualFields) {
    const auto device = rmCapableDevice();
    std::vector<DeviceIdentity> identities;
    ASSERT_TRUE(collectLocalDeviceIdentities({device}, identities).ok());
    ASSERT_EQ(identities.size(), 1U);
    const auto& identity = identities.front();
    EXPECT_EQ(identity.native_device_name, device.native_device_name);
    EXPECT_EQ(identity.native_device_path, device.native_device_path);
    EXPECT_EQ(identity.eid, device.eid);
    EXPECT_EQ(identity.eid_index, device.eid_index);
    EXPECT_EQ(identity.transport_modes, device.capabilities.transport_modes);
    EXPECT_EQ(identity.kind, DeviceKind::PHYSICAL);
}

TEST(UbAutoImportTest, CollectSkipsIncompleteAndFailsClosedWhenNone) {
    auto incomplete = rmCapableDevice();
    incomplete.eid.clear();
    std::vector<DeviceIdentity> identities;
    // Only an incomplete device: fail closed.
    auto status = collectLocalDeviceIdentities({incomplete}, identities);
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_TRUE(identities.empty());

    // Mixed snapshot: the complete device is still collected.
    const auto complete = rmCapableDevice();
    ASSERT_TRUE(
        collectLocalDeviceIdentities({incomplete, complete}, identities).ok());
    ASSERT_EQ(identities.size(), 1U);
    EXPECT_EQ(identities.front().eid, complete.eid);
}

TEST(UbAutoImportTest, DeviceIdentitiesToJsonMatchesManualSchema) {
    const auto device = rmCapableDevice();
    std::vector<DeviceIdentity> identities;
    ASSERT_TRUE(collectLocalDeviceIdentities({device}, identities).ok());
    const auto json = deviceIdentitiesToJson(identities);
    ASSERT_TRUE(json.is_array());
    ASSERT_EQ(json.size(), 1U);
    const auto& record = json.front();
    // Exactly the keys generate_rm_legacy_config.py and manual tent-perf.json
    // configuration use; UbParams::FromConfig reads the same schema.
    EXPECT_EQ(record.at("native_device_name"), device.native_device_name);
    EXPECT_EQ(record.at("native_device_path"), device.native_device_path);
    EXPECT_EQ(record.at("eid"), device.eid);
    EXPECT_EQ(record.at("eid_index"), device.eid_index);
    EXPECT_EQ(record.at("transport_modes"),
              device.capabilities.transport_modes);
    EXPECT_EQ(record.at("kind"), "physical");
}

// ---------------------------------------------------------------------------
// Metadata encode/decode round trip
// ---------------------------------------------------------------------------

TEST(UbAutoImportTest, EncodeDecodeDeviceMetadataRoundTrip) {
    const auto device = rmCapableDevice();
    const auto encoded = encodeDeviceMetadata(7, device);
    DeviceInfo decoded{};
    ASSERT_TRUE(decodeUbDeviceMetadata(encoded, decoded).ok());
    EXPECT_EQ(decoded.native_device_name, device.native_device_name);
    EXPECT_EQ(decoded.native_device_path, device.native_device_path);
    EXPECT_EQ(decoded.eid, device.eid);
    EXPECT_EQ(decoded.eid_index, device.eid_index);
    EXPECT_EQ(decoded.active, device.active);
    EXPECT_EQ(decoded.kind, device.kind);
    EXPECT_EQ(decoded.kind_source, device.kind_source);
    EXPECT_EQ(decoded.capabilities.transport_modes,
              device.capabilities.transport_modes);
    EXPECT_EQ(decoded.capabilities.max_jetty, device.capabilities.max_jetty);

    EXPECT_TRUE(decodeUbDeviceMetadata("not json", decoded).IsInvalidArgument());
    EXPECT_TRUE(decodeUbDeviceMetadata("{}", decoded).IsInvalidArgument());
}

// ---------------------------------------------------------------------------
// Peer identity anchoring from registered segment metadata
// ---------------------------------------------------------------------------

TEST(UbAutoImportTest, AnchorPeerIdentityFromPublishedMetadata) {
    const auto device = rmCapableDevice();
    const auto segment = makeSegmentWithUbDevice("peer-node", device);
    UbExpectedPeerIdentity expected;
    ASSERT_TRUE(anchorPeerIdentityFromSegment(segment, 0, expected).ok());
    EXPECT_EQ(expected.segment_name, "peer-node");
    EXPECT_EQ(expected.local_nic_path, "peer-node@ub:fake0:eid0");
    EXPECT_EQ(expected.native_device_name, device.native_device_name);
    EXPECT_EQ(expected.device_id, 0);
    EXPECT_EQ(expected.eid_index, static_cast<int>(device.eid_index));
    EXPECT_EQ(expected.eid, device.eid);
    EXPECT_EQ(expected.transport_modes, device.capabilities.transport_modes);
    EXPECT_EQ(expected.physical_device_identity,
              makePhysicalDeviceIdentity(device.native_device_path,
                                         device.eid));
}

TEST(UbAutoImportTest, AnchorRejectsMissingOrMalformedMetadata) {
    const auto device = rmCapableDevice();
    UbExpectedPeerIdentity expected;

    // No device entry at all.
    SegmentDesc empty_segment;
    empty_segment.name = "peer-node";
    empty_segment.type = SegmentType::Memory;
    MemorySegmentDesc empty_memory;
    empty_memory.topology = *fakeTopology();
    empty_segment.detail = empty_memory;
    EXPECT_TRUE(
        anchorPeerIdentityFromSegment(empty_segment, 0, expected)
            .IsInvalidArgument());

    // Unknown topology id.
    const auto segment = makeSegmentWithUbDevice("peer-node", device);
    EXPECT_TRUE(
        anchorPeerIdentityFromSegment(segment, 9, expected).IsInvalidArgument());

    // Device entry without UB transport attributes.
    SegmentDesc no_attrs = segment;
    std::get<MemorySegmentDesc>(no_attrs.detail).devices.front()
        .transport_attrs.clear();
    EXPECT_TRUE(
        anchorPeerIdentityFromSegment(no_attrs, 0, expected).IsInvalidArgument());

    // Empty eid in the published metadata.
    auto no_eid_device = device;
    no_eid_device.eid.clear();
    EXPECT_TRUE(anchorPeerIdentityFromSegment(
                    makeSegmentWithUbDevice("peer-node", no_eid_device), 0,
                    expected)
                    .IsInvalidArgument());

    // Non-physical peer device.
    auto bonding_device = device;
    bonding_device.kind = DeviceKind::BONDING;
    bonding_device.kind_source = DeviceKindSource::EXPLICIT_CONFIG;
    EXPECT_TRUE(anchorPeerIdentityFromSegment(
                    makeSegmentWithUbDevice("peer-node", bonding_device), 0,
                    expected)
                    .IsNotImplemented());

    // Peer metadata without the RM transport mode.
    auto rc_only_device = device;
    rc_only_device.capabilities.transport_modes = kUrmaTransportModeRc;
    EXPECT_TRUE(anchorPeerIdentityFromSegment(
                    makeSegmentWithUbDevice("peer-node", rc_only_device), 0,
                    expected)
                    .IsNotImplemented());

    // Inactive peer device.
    auto inactive_device = device;
    inactive_device.active = false;
    EXPECT_TRUE(anchorPeerIdentityFromSegment(
                    makeSegmentWithUbDevice("peer-node", inactive_device), 0,
                    expected)
                    .IsInvalidArgument());
}

// ---------------------------------------------------------------------------
// Install-level auto-import: injection, precedence, fail-closed, persistence
// ---------------------------------------------------------------------------

TEST(UbAutoImportTest, InstallAutoImportsIdentitiesWithoutManualConfig) {
    auto adapter = std::make_shared<AutoImportAdapter>(
        std::vector<DeviceInfo>{rmCapableDevice()});
    auto topology = fakeTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));

    // RM legacy config without any device_identities: the manual flow used to
    // fail the FromConfig gate here; auto-import must fill the gap.
    auto config = std::make_shared<Config>();
    applyRmLegacyDefaults(*config);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());

    // The injected records use the exact manual schema and stay readable by
    // the existing FromConfig path.
    const auto injected =
        config->get<nlohmann::json>("transports/ub/device_identities",
                                    nlohmann::json());
    ASSERT_TRUE(injected.is_array());
    ASSERT_EQ(injected.size(), 1U);
    const auto device = rmCapableDevice();
    EXPECT_EQ(injected.front().at("native_device_name"),
              device.native_device_name);
    EXPECT_EQ(injected.front().at("native_device_path"),
              device.native_device_path);
    EXPECT_EQ(injected.front().at("eid"), device.eid);
    EXPECT_EQ(injected.front().at("eid_index"), device.eid_index);
    EXPECT_EQ(injected.front().at("transport_modes"),
              device.capabilities.transport_modes);
    EXPECT_EQ(injected.front().at("kind"), "physical");

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(UbAutoImportTest, ExplicitIdentitiesTakePrecedenceOverAutoImport) {
    auto adapter = std::make_shared<AutoImportAdapter>(
        std::vector<DeviceInfo>{rmCapableDevice()});
    auto topology = fakeTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));

    auto config = std::make_shared<Config>();
    applyRmLegacyDefaults(*config);
    const auto device = rmCapableDevice();
    const nlohmann::json explicit_identities = nlohmann::json::array(
        {{{"native_device_name", device.native_device_name},
          {"native_device_path", device.native_device_path},
          {"eid_index", device.eid_index},
          {"eid", device.eid},
          {"kind", "physical"},
          {"transport_modes", device.capabilities.transport_modes}}});
    config->set("transports/ub/device_identities", explicit_identities);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    // Explicit configuration must be left byte-for-byte untouched.
    EXPECT_EQ(config->get<nlohmann::json>("transports/ub/device_identities",
                                          nlohmann::json()),
              explicit_identities);
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(UbAutoImportTest, AutoImportFailsClosedWhenNoCompleteDevice) {
    auto incomplete = rmCapableDevice();
    incomplete.eid.clear();
    auto adapter = std::make_shared<AutoImportAdapter>(
        std::vector<DeviceInfo>{incomplete});
    auto topology = fakeTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));

    auto config = std::make_shared<Config>();
    applyRmLegacyDefaults(*config);
    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto status =
        transport.install(mutable_segment_name, control, topology, config);
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_NE(status.ToString().find("auto-import"), std::string::npos);
    // The pre-pass initialized the adapter; the failed install must roll it
    // back so a retry starts clean.
    EXPECT_FALSE(adapter->initialized());
}

TEST(UbAutoImportTest, AutoImportPersistsToConfigSourceFile) {
    const auto path = std::filesystem::path(testing::TempDir()) /
                      "ub_auto_import_config.json";
    std::filesystem::remove(path);

    auto adapter = std::make_shared<AutoImportAdapter>(
        std::vector<DeviceInfo>{rmCapableDevice()});
    auto topology = fakeTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));

    // Simulate the MC_TENT_CONF file flow: load a config without identities
    // from disk and remember the source file, exactly like
    // ConfigHelper::loadFromEnv does.
    {
        Config seed;
        applyRmLegacyDefaults(seed);
        ASSERT_TRUE(seed.saveFile(path.string()).ok());
    }
    auto config = std::make_shared<Config>();
    ASSERT_TRUE(config->loadFile(path.string()).ok());
    config->set("config_source_file", path.string());

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    EXPECT_TRUE(transport.uninstall().ok());

    // The persisted file must now carry the auto-imported identities in the
    // manual schema, so the next start loads them without any hand editing.
    Config reloaded;
    ASSERT_TRUE(reloaded.loadFile(path.string()).ok());
    const auto persisted =
        reloaded.get<nlohmann::json>("transports/ub/device_identities",
                                     nlohmann::json());
    ASSERT_TRUE(persisted.is_array());
    ASSERT_EQ(persisted.size(), 1U);
    const auto device = rmCapableDevice();
    EXPECT_EQ(persisted.front().at("eid"), device.eid);
    EXPECT_EQ(persisted.front().at("kind"), "physical");
    std::filesystem::remove(path);
}

// ---------------------------------------------------------------------------
// End-to-end: RM self read/write with zero manual identity configuration
// ---------------------------------------------------------------------------

TEST(UbAutoImportTest, RmSelfReadWriteWithoutManualIdentities) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<AutoImportAdapter>(
        std::vector<DeviceInfo>{device});
    auto topology = fakeTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    applyRmLegacyDefaults(*config);
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    // Note: no device_identities anywhere. Install must auto-import them and
    // the RM bootstrap must anchor the peer identity from the published
    // segment metadata.

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GE(adapter->rmPostCalls(), 1U)
        << "the RM data plane must post with an explicit imported target";
    EXPECT_EQ(adapter->bindJettyCalls(), 0U)
        << "RM Legacy posts must not rely on bound remote Jettys";
    ASSERT_FALSE(adapter->importedIds().empty())
        << "the peer's Jetty target must be imported automatically";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_GE(adapter->unimportCalls(), adapter->importCalls())
        << "every imported target must be unimported before uninstall ends";
}

}  // namespace
}  // namespace mooncake::tent::ub
