#include "real_client.h"

#include <chrono>
#include <future>

#include <gtest/gtest.h>

namespace mooncake {

class DirectGpuReadTest : public ::testing::Test {
   protected:
    void SetProtocol(const std::string &protocol) {
        client_.protocol = protocol;
    }

    void RegisterRange(void *buffer, size_t size) {
        client_.registered_buffer_sizes_[buffer] = {size};
    }

    static Replica::Descriptor MemoryReplica(const std::string &protocol) {
        AllocatedBuffer::Descriptor buffer_descriptor{
            .size_ = 16,
            .buffer_address_ = 0x1000,
            .protocol_ = protocol,
            .transport_endpoint_ = "remote:12345",
        };
        return Replica::Descriptor{
            .id = 1,
            .descriptor_variant =
                MemoryDescriptor{std::move(buffer_descriptor)},
            .status = ReplicaStatus::COMPLETE,
        };
    }

    bool CanUseDirectRead(const std::string &replica_protocol, void *buffer,
                          size_t size) const {
        return client_.can_use_direct_memory_read(
            MemoryReplica(replica_protocol), buffer, size);
    }

    void AddDummyContext(const UUID &client_id) {
        std::unique_lock lock(client_.dummy_client_mutex_);
        client_.shm_contexts_.try_emplace(client_id);
    }

    std::shared_lock<std::shared_mutex> PinDummyMappings() {
        return std::shared_lock(client_.dummy_client_mutex_);
    }

    tl::expected<void, ErrorCode> UnmapDummyContext(const UUID &client_id) {
        return client_.unmap_shm_internal(client_id);
    }

    bool HasDummyContext(const UUID &client_id) {
        std::shared_lock lock(client_.dummy_client_mutex_);
        return client_.shm_contexts_.contains(client_id);
    }

    RealClient client_;
};

TEST_F(DirectGpuReadTest, NvlinkReplicaAcceptsUnregisteredLocalEndpoint) {
    SetProtocol("rdma");
    char buffer[16];

    EXPECT_TRUE(CanUseDirectRead("nvlink", buffer, sizeof(buffer)));
}

TEST_F(DirectGpuReadTest,
       RdmaReplicaRequiresRegisteredDestinationRegardlessOfRequestedProtocol) {
    SetProtocol("nvlink");
    char buffer[16];

    EXPECT_FALSE(CanUseDirectRead("rdma", buffer, sizeof(buffer)));
    RegisterRange(buffer, sizeof(buffer));
    EXPECT_TRUE(CanUseDirectRead("rdma", buffer + 4, 12));
    EXPECT_FALSE(CanUseDirectRead("rdma", buffer + 4, 13));
}

TEST_F(DirectGpuReadTest, OtherReplicaProtocolsKeepStagingFallback) {
    SetProtocol("nvlink");
    char buffer[16];
    RegisterRange(buffer, sizeof(buffer));

    EXPECT_FALSE(CanUseDirectRead("tcp", buffer, sizeof(buffer)));
}

TEST_F(DirectGpuReadTest, DummyBufferAcquiresAdmitConcurrentReaders) {
    const UUID client_id{1, 2};
    AddDummyContext(client_id);
    for (const bool batched : {false, true}) {
        // An existing read pins the mappings. Another acquire must enter
        // without waiting for that reader to finish. No backend is set up,
        // so the actual acquire methods then report a normal missing value.
        auto pin = PinDummyMappings();
        auto read = std::async(std::launch::async, [&] {
            if (batched) {
                auto results =
                    client_.batch_acquire_buffer_dummy({"missing"}, client_id);
                EXPECT_EQ(results.size(), 1u);
                if (results.empty()) {
                    return tl::expected<std::tuple<uint64_t, size_t>,
                                        ErrorCode>(
                        tl::unexpected(ErrorCode::INTERNAL_ERROR));
                }
                return results.front();
            }
            return client_.acquire_buffer_dummy("missing", client_id);
        });
        const auto status = read.wait_for(std::chrono::seconds(2));
        pin.unlock();  // Also let the baseline's exclusive acquire finish.
        const auto result = read.get();
        EXPECT_EQ(status, std::future_status::ready) << "batched=" << batched;
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
    }
}

TEST_F(DirectGpuReadTest, DummyMappingPinDelaysUnmapUntilReadersFinish) {
    const UUID client_id{3, 4};
    AddDummyContext(client_id);
    auto pin = PinDummyMappings();
    std::promise<void> started;
    auto unmap = std::async(std::launch::async, [&] {
        started.set_value();
        return UnmapDummyContext(client_id);
    });
    started.get_future().wait();
    const auto status = unmap.wait_for(std::chrono::milliseconds(50));
    pin.unlock();
    EXPECT_EQ(status, std::future_status::timeout);
    EXPECT_TRUE(unmap.get());
    EXPECT_FALSE(HasDummyContext(client_id));
}

}  // namespace mooncake
