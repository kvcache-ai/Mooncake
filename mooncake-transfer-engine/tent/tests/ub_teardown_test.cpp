// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/topology_attrs.h"

namespace mooncake::tent::ub {
namespace {

SegmentDescriptor makeDescriptor(uint64_t address, uint64_t length) {
    return SegmentDescriptor{
        SegmentDescriptor::kSchemaVersion, 1, 16,
        std::to_string(address) + ":" + std::to_string(length)};
}

DeviceInfo makeDevice(int index) {
    DeviceInfo device;
    device.topology_name = "ub:test:" + std::to_string(index);
    device.native_device_name = "test" + std::to_string(index);
    device.eid_index = static_cast<uint32_t>(index);
    device.eid = "0001:0002:0003:0004:0005:0006:0007:0008";
    device.active = true;
    device.capabilities.max_jfc = 16;
    return device;
}

TEST(UbTopologyAttributes, EncoderPreservesHardwareNeutralMap) {
    auto device = makeDevice(2);
    device.active = false;
    std::unordered_map<std::string, std::string> attributes{
        {"vendor.future_attribute", "preserved"}};
    encodeTopologyDeviceAttributes(device, 7, attributes);

    EXPECT_EQ(attributes.at(std::string(kTopologyNativeNameAttr)),
              device.native_device_name);
    EXPECT_EQ(attributes.at(std::string(kTopologyDeviceIndexAttr)), "7");
    EXPECT_EQ(attributes.at(std::string(kTopologyEidIndexAttr)), "2");
    EXPECT_EQ(attributes.at(std::string(kTopologyEidAttr)), device.eid);
    EXPECT_EQ(attributes.at(std::string(kTopologyDiscoveryActiveAttr)),
              "false");
    EXPECT_EQ(attributes.at("vendor.future_attribute"), "preserved");
}

class FakeContext final : public Context {
   public:
    explicit FakeContext(DeviceInfo device) : device_(std::move(device)) {}

    bool valid() const noexcept override { return valid_; }
    const DeviceInfo& deviceInfo() const noexcept override { return device_; }
    int asyncFd() const noexcept override { return -1; }
    void close() noexcept { valid_ = false; }

   private:
    DeviceInfo device_;
    bool valid_{true};
};

class FakeJfc final : public Jfc {
   public:
    bool valid() const noexcept override { return valid_; }
    int eventFd() const noexcept override { return -1; }
    void close() noexcept { valid_ = false; }

   private:
    bool valid_{true};
};

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
    void close() noexcept { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeRemoteSegment final : public RemoteSegment {
   public:
    explicit FakeRemoteSegment(SegmentDescriptor descriptor)
        : descriptor_(std::move(descriptor)) {}

    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return 0x1000; }
    uint64_t length() const noexcept override { return 0x1000; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() noexcept { valid_ = false; }

   private:
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeJetty final : public Jetty {
   public:
    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return 1; }
    uint32_t uasid() const noexcept override { return 0; }
    void close() noexcept { valid_ = false; }

   private:
    bool valid_{true};
};

class FailOnceAdapter final : public UrmaAdapter {
   public:
    bool available() const noexcept override { return true; }
    uint32_t nativeApiVersion() const noexcept override { return 1; }
    size_t nativeSegmentDescriptorSize() const noexcept override { return 16; }

    Status initialize() override { return Status::OK(); }
    Status shutdown() override { return Status::OK(); }
    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        devices = {makeDevice(0), makeDevice(1)};
        return Status::OK();
    }

    Status openContext(const DeviceInfo& device, ContextPtr& context) override {
        context = std::make_shared<FakeContext>(device);
        ++live_contexts;
        return Status::OK();
    }
    Status closeContext(ContextPtr& context) override {
        ++close_context_calls;
        if (close_context_failures > 0) {
            --close_context_failures;
            return Status::InternalError("injected close Context failure");
        }
        if (auto fake = std::dynamic_pointer_cast<FakeContext>(context)) {
            fake->close();
        }
        context.reset();
        --live_contexts;
        return Status::OK();
    }

    Status createJfc(const ContextPtr&, const JfcOptions&,
                     JfcPtr& jfc) override {
        jfc = std::make_shared<FakeJfc>();
        ++live_jfcs;
        return Status::OK();
    }
    Status deleteJfc(JfcPtr& jfc) override {
        ++delete_jfc_calls;
        if (delete_jfc_failures > 0) {
            --delete_jfc_failures;
            return Status::InternalError("injected delete JFC failure");
        }
        if (auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc)) fake->close();
        jfc.reset();
        --live_jfcs;
        return Status::OK();
    }

    Status registerLocalSegment(const ContextPtr&, uint64_t address,
                                size_t length, const SegmentOptions&,
                                LocalSegmentPtr& segment) override {
        ++register_calls;
        if (fail_register_call != 0 && register_calls == fail_register_call) {
            if (partial_register_on_failure) {
                segment = std::make_shared<FakeLocalSegment>(address, length);
                ++live_local_segments;
            }
            return Status::InternalError("injected register failure");
        }
        segment = std::make_shared<FakeLocalSegment>(address, length);
        ++live_local_segments;
        return Status::OK();
    }
    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        ++unregister_calls;
        if (unregister_failures > 0) {
            --unregister_failures;
            return Status::InternalError("injected unregister failure");
        }
        if (auto fake = std::dynamic_pointer_cast<FakeLocalSegment>(segment)) {
            fake->close();
        }
        segment.reset();
        --live_local_segments;
        return Status::OK();
    }

    Status importRemoteSegment(const ContextPtr&,
                               const SegmentDescriptor& descriptor,
                               const SegmentOptions&,
                               RemoteSegmentPtr& segment) override {
        if (import_failures > 0) {
            --import_failures;
            if (partial_import_on_failure) {
                segment = std::make_shared<FakeRemoteSegment>(descriptor);
                ++live_remote_segments;
            }
            return Status::InternalError("injected import failure");
        }
        segment = std::make_shared<FakeRemoteSegment>(descriptor);
        ++live_remote_segments;
        return Status::OK();
    }
    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        ++unimport_calls;
        if (unimport_failures > 0) {
            --unimport_failures;
            return Status::InternalError("injected unimport failure");
        }
        if (auto fake = std::dynamic_pointer_cast<FakeRemoteSegment>(segment)) {
            fake->close();
        }
        segment.reset();
        --live_remote_segments;
        return Status::OK();
    }

    Status createJetty(const ContextPtr&, const JfcPtr&, const JettyOptions&,
                       JettyPtr& jetty) override {
        jetty = std::make_shared<FakeJetty>();
        return Status::OK();
    }
    Status deleteJetty(JettyPtr& jetty) override {
        ++delete_jetty_calls;
        if (delete_jetty_failures > 0) {
            --delete_jetty_failures;
            return Status::InternalError("injected delete Jetty failure");
        }
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->close();
        }
        jetty.reset();
        return Status::OK();
    }
    Status bindJetty(const JettyPtr&, const RemoteJettyInfo&) override {
        return Status::OK();
    }
    Status unbindJetty(const JettyPtr&) override { return Status::OK(); }
    Status resetJetty(const JettyPtr&) override { return Status::OK(); }
    Status quiesceJetty(const JettyPtr&, uint32_t,
                        std::vector<Completion>& completions) override {
        completions.clear();
        return Status::OK();
    }
    Status post(const JettyPtr&, const std::vector<WorkRequest>& requests,
                size_t& posted_count) override {
        posted_count = requests.size();
        return Status::OK();
    }
    Status poll(const JfcPtr&, size_t,
                std::vector<Completion>& completions) override {
        completions.clear();
        return Status::OK();
    }

    int close_context_failures{0};
    int delete_jfc_failures{0};
    int unregister_failures{0};
    int unimport_failures{0};
    int import_failures{0};
    int delete_jetty_failures{0};
    int fail_register_call{0};
    bool partial_register_on_failure{false};
    bool partial_import_on_failure{false};
    int close_context_calls{0};
    int delete_jfc_calls{0};
    int register_calls{0};
    int unregister_calls{0};
    int unimport_calls{0};
    int delete_jetty_calls{0};
    int live_contexts{0};
    int live_jfcs{0};
    int live_local_segments{0};
    int live_remote_segments{0};
};

UbContextPtr makeActiveContext(const std::shared_ptr<FailOnceAdapter>& adapter,
                               int topology_id, uint32_t jfc_count = 1) {
    auto context = std::make_shared<UbContext>(
        topology_id, makeDevice(topology_id), adapter);
    EXPECT_TRUE(context->initialize(jfc_count, JfcOptions{}).ok());
    return context;
}

BufferDesc makeLocalBuffer(uint64_t address) {
    BufferDesc desc;
    desc.addr = address;
    desc.length = 4096;
    desc.location = "cpu:0";
    return desc;
}

TEST(UbTeardown, RemoveBufferRetainsOwnershipAndMetadataForRetry) {
    auto adapter = std::make_shared<FailOnceAdapter>();
    auto context = makeActiveContext(adapter, 0);
    UbBufferManager manager(adapter, {context});
    auto desc = makeLocalBuffer(0x10000);
    ASSERT_TRUE(manager.addBuffer(desc, MemoryOptions{}).ok());
    ASSERT_EQ(manager.localBufferCount(), 1u);

    adapter->unregister_failures = 1;
    EXPECT_FALSE(manager.removeBuffer(desc).ok());
    EXPECT_EQ(manager.localBufferCount(), 1u);
    EXPECT_EQ(adapter->live_local_segments, 1);
    EXPECT_TRUE(desc.transport_attrs.contains(TransportType::UB));
    EXPECT_NE(std::find(desc.transports.begin(), desc.transports.end(),
                        TransportType::UB),
              desc.transports.end());

    EXPECT_TRUE(manager.removeBuffer(desc).ok());
    EXPECT_EQ(manager.localBufferCount(), 0u);
    EXPECT_EQ(adapter->live_local_segments, 0);
    EXPECT_FALSE(desc.transport_attrs.contains(TransportType::UB));
    EXPECT_TRUE(context->shutdown().ok());
}

TEST(UbTeardown, ClearRetainsLocalAndImportedSegmentsForRetry) {
    auto adapter = std::make_shared<FailOnceAdapter>();
    auto context = makeActiveContext(adapter, 0);
    UbBufferManager manager(adapter, {context});
    auto local = makeLocalBuffer(0x20000);
    ASSERT_TRUE(manager.addBuffer(local, MemoryOptions{}).ok());

    UbBufferMetadata metadata;
    metadata.generation = 7;
    metadata.base = 0x1000;
    metadata.length = 0x1000;
    metadata.permission = kGlobalReadWrite;
    metadata.segments.push_back(UbBufferSegmentMetadata{
        9, "remote", makeDevice(9).eid, 9,
        makeDescriptor(metadata.base, metadata.length)});
    BufferDesc remote;
    remote.addr = metadata.base;
    remote.length = metadata.length;
    ASSERT_TRUE(encodeBufferMetadata(metadata,
                                     remote.transport_attrs[TransportType::UB])
                    .ok());
    ImportedSegmentRef imported;
    ASSERT_TRUE(manager
                    .importRemote(123, 0, 9, remote, Request::READ,
                                  metadata.base, 64, imported)
                    .ok());
    imported.segment.reset();
    ASSERT_EQ(manager.importedSegmentCount(), 1u);

    adapter->unregister_failures = 1;
    adapter->unimport_failures = 1;
    EXPECT_FALSE(manager.clear().ok());
    EXPECT_EQ(manager.localBufferCount(), 1u);
    EXPECT_EQ(manager.importedSegmentCount(), 1u);
    EXPECT_EQ(adapter->live_local_segments, 1);
    EXPECT_EQ(adapter->live_remote_segments, 1);

    EXPECT_TRUE(manager.clear().ok());
    EXPECT_EQ(manager.localBufferCount(), 0u);
    EXPECT_EQ(manager.importedSegmentCount(), 0u);
    EXPECT_EQ(adapter->live_local_segments, 0);
    EXPECT_EQ(adapter->live_remote_segments, 0);
    EXPECT_TRUE(context->shutdown().ok());
}

TEST(UbTeardown, NewGenerationProceedsWhileStaleImportCleanupRetries) {
    auto adapter = std::make_shared<FailOnceAdapter>();
    auto context = makeActiveContext(adapter, 0);
    UbBufferManager manager(adapter, {context});

    UbBufferMetadata metadata;
    metadata.generation = 7;
    metadata.base = 0x1000;
    metadata.length = 0x1000;
    metadata.permission = kGlobalReadWrite;
    metadata.segments.push_back(UbBufferSegmentMetadata{
        9, "remote", makeDevice(9).eid, 9,
        makeDescriptor(metadata.base, metadata.length)});
    BufferDesc remote;
    remote.addr = metadata.base;
    remote.length = metadata.length;
    ASSERT_TRUE(encodeBufferMetadata(metadata,
                                     remote.transport_attrs[TransportType::UB])
                    .ok());

    ImportedSegmentRef old_generation;
    ASSERT_TRUE(manager
                    .importRemote(123, 0, 9, remote, Request::READ,
                                  metadata.base, 64, old_generation)
                    .ok());
    metadata.generation = 8;
    ASSERT_TRUE(encodeBufferMetadata(metadata,
                                     remote.transport_attrs[TransportType::UB])
                    .ok());
    adapter->unimport_failures = 1;
    ImportedSegmentRef new_generation;
    EXPECT_TRUE(manager
                    .importRemote(123, 0, 9, remote, Request::READ,
                                  metadata.base, 64, new_generation)
                    .ok());
    EXPECT_EQ(new_generation.generation, 8u);
    EXPECT_EQ(manager.importedSegmentCount(), 2u);
    EXPECT_EQ(adapter->live_remote_segments, 2);

    old_generation.segment.reset();
    ImportedSegmentRef reused;
    EXPECT_TRUE(manager
                    .importRemote(123, 0, 9, remote, Request::READ,
                                  metadata.base, 64, reused)
                    .ok());
    EXPECT_EQ(reused.segment, new_generation.segment);
    EXPECT_EQ(manager.importedSegmentCount(), 1u);
    EXPECT_EQ(adapter->live_remote_segments, 1);

    reused.segment.reset();
    new_generation.segment.reset();
    EXPECT_TRUE(manager.clear().ok());
    EXPECT_EQ(adapter->live_remote_segments, 0);
    EXPECT_TRUE(context->shutdown().ok());
}

TEST(UbTeardown, FailedRegistrationRollbackIsRetainedUntilClear) {
    auto adapter = std::make_shared<FailOnceAdapter>();
    auto first = makeActiveContext(adapter, 0);
    auto second = makeActiveContext(adapter, 1);
    UbBufferManager manager(adapter, {first, second});
    auto desc = makeLocalBuffer(0x30000);
    adapter->fail_register_call = 2;
    adapter->partial_register_on_failure = true;
    adapter->unregister_failures = 1;

    EXPECT_FALSE(manager.addBuffer(desc, MemoryOptions{}).ok());
    EXPECT_EQ(manager.localBufferCount(), 0u);
    EXPECT_EQ(adapter->live_local_segments, 1);
    EXPECT_TRUE(manager.clear().ok());
    EXPECT_EQ(adapter->live_local_segments, 0);
    EXPECT_TRUE(first->shutdown().ok());
    EXPECT_TRUE(second->shutdown().ok());
}

TEST(UbTeardown, FailedPartialImportIsRetainedUntilClear) {
    auto adapter = std::make_shared<FailOnceAdapter>();
    auto context = makeActiveContext(adapter, 0);
    UbBufferManager manager(adapter, {context});

    UbBufferMetadata metadata;
    metadata.generation = 11;
    metadata.base = 0x1000;
    metadata.length = 0x1000;
    metadata.permission = kGlobalReadWrite;
    metadata.segments.push_back(UbBufferSegmentMetadata{
        9, "remote", makeDevice(9).eid, 9,
        makeDescriptor(metadata.base, metadata.length)});
    BufferDesc remote;
    remote.addr = metadata.base;
    remote.length = metadata.length;
    ASSERT_TRUE(encodeBufferMetadata(metadata,
                                     remote.transport_attrs[TransportType::UB])
                    .ok());

    adapter->import_failures = 1;
    adapter->partial_import_on_failure = true;
    adapter->unimport_failures = 1;
    ImportedSegmentRef imported;
    EXPECT_FALSE(manager
                     .importRemote(456, 0, 9, remote, Request::READ,
                                   metadata.base, 64, imported)
                     .ok());
    EXPECT_EQ(manager.importedSegmentCount(), 0u);
    EXPECT_EQ(adapter->live_remote_segments, 1);

    EXPECT_TRUE(manager.clear().ok());
    EXPECT_EQ(adapter->live_remote_segments, 0);
    EXPECT_TRUE(context->shutdown().ok());
}

TEST(UbTeardown, ContextShutdownRetainsFailedJfcBeforeClosingContext) {
    auto adapter = std::make_shared<FailOnceAdapter>();
    auto context = makeActiveContext(adapter, 0);
    adapter->delete_jfc_failures = 1;

    EXPECT_FALSE(context->shutdown().ok());
    EXPECT_EQ(context->state(), UbContext::State::kDraining);
    EXPECT_EQ(context->jfcs().size(), 1u);
    EXPECT_TRUE(context->handle());
    EXPECT_EQ(adapter->close_context_calls, 0);
    EXPECT_EQ(adapter->live_jfcs, 1);

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_EQ(context->state(), UbContext::State::kClosed);
    EXPECT_TRUE(context->jfcs().empty());
    EXPECT_FALSE(context->handle());
    EXPECT_EQ(adapter->close_context_calls, 1);
    EXPECT_EQ(adapter->live_jfcs, 0);
    EXPECT_EQ(adapter->live_contexts, 0);
}

TEST(UbTeardown, ContextShutdownRetainsContextAfterCloseFailure) {
    auto adapter = std::make_shared<FailOnceAdapter>();
    auto context = makeActiveContext(adapter, 0);
    adapter->close_context_failures = 1;

    EXPECT_FALSE(context->shutdown().ok());
    EXPECT_EQ(context->state(), UbContext::State::kDraining);
    EXPECT_TRUE(context->jfcs().empty());
    EXPECT_TRUE(context->handle());
    EXPECT_EQ(adapter->live_contexts, 1);

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_EQ(context->state(), UbContext::State::kClosed);
    EXPECT_FALSE(context->handle());
    EXPECT_EQ(adapter->live_contexts, 0);
}

}  // namespace
}  // namespace mooncake::tent::ub
