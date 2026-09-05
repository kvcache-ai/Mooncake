#include "real_client.h"

#include <gtest/gtest.h>

namespace mooncake {

class DirectGpuReadTest : public ::testing::Test {
   protected:
    void SetProtocol(const std::string &protocol) {
        client_.protocol = protocol;
    }

    void RegisterRange(void *buffer, size_t size) {
        client_.registered_buffer_sizes_[buffer] = size;
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

}  // namespace mooncake
