// Copyright 2026 Mooncake Authors

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "common.h"
#include "transfer_metadata.h"
#include "transport/shm_transport/shm_transport.h"
#include "transport/transport.h"
#ifdef USE_CXL
#include "transport/cxl_transport/cxl_transport.h"
#endif
#ifdef USE_HIP
#include "transport/hip_transport/hip_transport.h"
#endif

namespace mooncake {

// Exercise the actual transport implementations through their virtual methods.
// The peer only exposes the lifecycle operations normally called by the engine.
class TransportMetadataRemovalTestPeer {
   public:
    static int install(Transport &transport, std::string &name,
                       std::shared_ptr<TransferMetadata> metadata) {
        return transport.install(name, std::move(metadata), nullptr);
    }
    static int registerMemory(Transport &transport, void *address) {
        return transport.registerLocalMemory(address, 4096, "cpu:0", true,
                                             false);
    }
    static int unregisterMemory(Transport &transport, void *address) {
        return transport.unregisterLocalMemory(address, false);
    }
    static int unregisterBatch(Transport &transport,
                               const std::vector<void *> &addresses) {
        return transport.unregisterLocalMemoryBatch(addresses);
    }
};

namespace {
using Peer = TransportMetadataRemovalTestPeer;

class MetadataRemovalTest : public ::testing::Test {
   protected:
    void SetUp() override {
        metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
        segment->name = name;
        segment->protocol = "tcp";
        ASSERT_EQ(metadata->addLocalSegment(LOCAL_SEGMENT_ID, name,
                                            std::move(segment)),
                  0);
    }

    void addPeer(void *address, const std::string &protocol = "tcp") {
        // The peer is synthetic; CXL/SHM/HIP register and unregister are real.
        TransferMetadata::BufferDesc buffer{};
        buffer.addr = reinterpret_cast<uint64_t>(address);
        buffer.length = 4096;
        buffer.name = "cpu:0";
        buffer.protocol = protocol;
        ASSERT_EQ(metadata->addLocalMemoryBuffer(buffer, false), 0);
    }

    void expectPeer(void *address, const std::string &protocol = "tcp") {
        auto segment = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        ASSERT_NE(segment, nullptr);
        ASSERT_EQ(segment->buffers.size(), 1U);
        EXPECT_EQ(segment->buffers[0].addr,
                  reinterpret_cast<uint64_t>(address));
        EXPECT_EQ(segment->buffers[0].protocol, protocol);
    }

    std::string name = "127.0.0.1:0";
    std::shared_ptr<TransferMetadata> metadata;
};

TEST_F(MetadataRemovalTest, ShmSkippedRegistrationPreservesRdma) {
    ShmTransport transport;
    ASSERT_EQ(Peer::install(transport, name, metadata), 0);
    std::vector<char> host_buffer(4096);
    void *address = host_buffer.data();
    addPeer(address, "rdma");
    auto original = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(Peer::registerMemory(transport, address), 0);
    EXPECT_EQ(Peer::unregisterMemory(transport, address), 0);
    EXPECT_EQ(metadata->getSegmentDescByID(LOCAL_SEGMENT_ID), original);
    expectPeer(address, "rdma");
}

TEST_F(MetadataRemovalTest, ShmSharedAddressRoundTrip) {
    ShmTransport transport;
    ASSERT_EQ(Peer::install(transport, name, metadata), 0);
    void *address = transport.allocateSharedMemory(4096);
    ASSERT_NE(address, nullptr);
    for (bool peer_first : {false, true}) {
        if (peer_first) addPeer(address);
        ASSERT_EQ(Peer::registerMemory(transport, address), 0);
        if (!peer_first) addPeer(address);
        ASSERT_EQ(Peer::unregisterMemory(transport, address), 0);
        expectPeer(address);
        ASSERT_EQ(metadata->removeLocalMemoryBuffer(address, false, "tcp"), 0);
    }
    EXPECT_EQ(transport.freeSharedMemory(address), 0);
}

#ifdef USE_HIP
TEST_F(MetadataRemovalTest, HipSkippedRegistrationPreservesRdma) {
    int devices = 0;
    if (hipGetDeviceCount(&devices) != hipSuccess || devices == 0)
        GTEST_SKIP() << "HIP runtime device required for native HIP validation";
    HipTransport transport;
    ASSERT_EQ(Peer::install(transport, name, metadata), 0);
    std::vector<char> host_buffer(4096);
    void *address = host_buffer.data();
    addPeer(address, "rdma");
    auto original = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(Peer::registerMemory(transport, address), 0);
    EXPECT_EQ(Peer::unregisterMemory(transport, address), 0);
    EXPECT_EQ(Peer::unregisterBatch(transport, {address}), 0);
    EXPECT_EQ(metadata->getSegmentDescByID(LOCAL_SEGMENT_ID), original);
    expectPeer(address, "rdma");
}
#endif

#ifdef USE_CXL
class ScopedEnvironment {
   public:
    explicit ScopedEnvironment(const char *name) : name_(name) {
        const char *value = std::getenv(name);
        present_ = value != nullptr;
        if (value) value_ = value;
    }
    ~ScopedEnvironment() {
        if (present_)
            setenv(name_.c_str(), value_.c_str(), 1);
        else
            unsetenv(name_.c_str());
    }

   private:
    std::string name_;
    std::string value_;
    bool present_;
};

class CxlMetadataRemovalTest : public MetadataRemovalTest {
   protected:
    void SetUp() override {
        MetadataRemovalTest::SetUp();
        // Real mmap and CxlTransport methods; this does not require CXL
        // hardware.
        fd_ = mkstemp(path_);
        ASSERT_GE(fd_, 0);
        ASSERT_EQ(ftruncate(fd_, 65536), 0);
        ASSERT_EQ(setenv("MC_CXL_DEV_PATH", path_, 1), 0);
        ASSERT_EQ(setenv("MC_CXL_DEV_SIZE", "65536", 1), 0);
        void *existing_address = reinterpret_cast<void *>(0x1000);
        addPeer(existing_address);
        transport = std::make_unique<CxlTransport>();
        ASSERT_EQ(Peer::install(*transport, name, metadata), 0);
        expectPeer(existing_address);
        ASSERT_EQ(
            metadata->removeLocalMemoryBuffer(existing_address, false, "tcp"),
            0);
        address = static_cast<char *>(transport->getCxlBaseAddr()) + 4096;
    }

    void TearDown() override {
        transport.reset();
        if (fd_ >= 0) {
            close(fd_);
            unlink(path_);
        }
    }

    ScopedEnvironment path_env_{"MC_CXL_DEV_PATH"};
    ScopedEnvironment size_env_{"MC_CXL_DEV_SIZE"};
    char path_[64] = "/tmp/mooncake-cxl-removal-XXXXXX";
    int fd_ = -1;
    std::unique_ptr<CxlTransport> transport;
    void *address = nullptr;
};

TEST_F(CxlMetadataRemovalTest, UniqueRoundTrip) {
    ASSERT_EQ(Peer::registerMemory(*transport, address), 0);
    auto segment = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(segment->buffers.size(), 1U);
    EXPECT_EQ(segment->buffers[0].protocol, "cxl");
    EXPECT_EQ(segment->buffers[0].offset, 4096U);
    ASSERT_EQ(Peer::unregisterMemory(*transport, address), 0);
    EXPECT_TRUE(
        metadata->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
}

TEST_F(CxlMetadataRemovalTest, SharedAddressRoundTrip) {
    for (bool peer_first : {false, true}) {
        for (int repeat = 0; repeat < 3; ++repeat) {
            if (peer_first) addPeer(address);
            ASSERT_EQ(Peer::registerMemory(*transport, address), 0);
            if (!peer_first) addPeer(address);
            ASSERT_EQ(Peer::unregisterMemory(*transport, address), 0);
            expectPeer(address);
            EXPECT_EQ(Peer::unregisterMemory(*transport, address),
                      ERR_ADDRESS_NOT_REGISTERED);
            expectPeer(address);
            ASSERT_EQ(metadata->removeLocalMemoryBuffer(address, false, "tcp"),
                      0);
        }
    }
}

TEST_F(CxlMetadataRemovalTest, BatchPreservesPeerDescriptors) {
    void *second = static_cast<char *>(address) + 8192;
    for (void *buffer : {address, second}) {
        addPeer(buffer);
        ASSERT_EQ(Peer::registerMemory(*transport, buffer), 0);
    }
    ASSERT_EQ(Peer::unregisterBatch(*transport, {address, second}), 0);
    auto segment = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(segment->buffers.size(), 2U);
    EXPECT_EQ(segment->buffers[0].addr, reinterpret_cast<uint64_t>(address));
    EXPECT_EQ(segment->buffers[1].addr, reinterpret_cast<uint64_t>(second));
    for (const auto &buffer : segment->buffers)
        EXPECT_EQ(buffer.protocol, "tcp");
}
#endif
}  // namespace
}  // namespace mooncake
