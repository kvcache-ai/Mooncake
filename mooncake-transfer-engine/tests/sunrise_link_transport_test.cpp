#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "gpu_vendor/sunrise.h"
#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

DEFINE_string(metadata_server, "127.0.0.1:2379", "etcd server host address");
DEFINE_string(local_server_name, "sunrise_server:12345", "Local server name");
DEFINE_string(segment_id, "sunrise_server:12345", "Segment ID to access data");
DEFINE_int32(gpu_id, 0, "Sunrise device ID to use");

static void checkSunriseError(tangError_t result, const char* message) {
    if (result != tangSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << tangGetErrorString(result) << ")";
        std::exit(EXIT_FAILURE);
    }
}

static void* allocateSunriseBuffer(size_t size, int gpu_id) {
    checkSunriseError(tangSetDevice(gpu_id), "Failed to set device");
    void* buffer = nullptr;
    checkSunriseError(tangMalloc(&buffer, size),
                      "Failed to allocate Sunrise device memory");
    return buffer;
}

static void freeSunriseBuffer(void* addr) {
    checkSunriseError(tangFree(addr), "Failed to free Sunrise device memory");
}

TEST(SunriseLinkTransportTest, WriteAndRead) {
    const size_t kDataLength = 4096000;
    int gpu_id = FLAGS_gpu_id;

    auto server_engine = std::make_unique<TransferEngine>(false);
    server_engine->init(FLAGS_metadata_server, FLAGS_local_server_name);
    Transport* server_transport =
        server_engine->installTransport("sunrise_link", nullptr);
    ASSERT_NE(server_transport, nullptr);

    void* server_buffer = allocateSunriseBuffer(kDataLength * 2, gpu_id);
    int rc = server_engine->registerLocalMemory(
        server_buffer, kDataLength * 2, "cuda:" + std::to_string(gpu_id));
    ASSERT_EQ(rc, 0);

    auto segment_id = server_engine->openSegment(FLAGS_segment_id);

    auto client_engine = std::make_unique<TransferEngine>(false);
    client_engine->init(FLAGS_metadata_server, "sunrise_client:12346");
    Transport* client_transport =
        client_engine->installTransport("sunrise_link", nullptr);
    ASSERT_NE(client_transport, nullptr);

    void* client_buffer = allocateSunriseBuffer(kDataLength * 2, gpu_id);
    rc = client_engine->registerLocalMemory(client_buffer, kDataLength * 2,
                                            "cuda:" + std::to_string(gpu_id));
    ASSERT_EQ(rc, 0);

    {
        std::vector<char> host_data(kDataLength, 'A');
        checkSunriseError(tangMemcpy(client_buffer, host_data.data(),
                                     kDataLength, tangMemcpyHostToDevice),
                          "Memcpy to client_buffer");

        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = client_buffer;
        entry.target_id = segment_id;
        entry.target_offset = reinterpret_cast<uint64_t>(server_buffer);
        Status s = client_engine->submitTransfer(batch_id, {entry});
        ASSERT_TRUE(s.ok());

        TransferStatus status;
        do {
            s = client_engine->getTransferStatus(batch_id, 0, status);
            ASSERT_TRUE(s.ok());
        } while (status.s == TransferStatusEnum::WAITING);

        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
        s = client_engine->freeBatchID(batch_id);
        ASSERT_TRUE(s.ok());
    }

    {
        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = static_cast<char*>(client_buffer) + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = reinterpret_cast<uint64_t>(server_buffer);
        Status s = client_engine->submitTransfer(batch_id, {entry});
        ASSERT_TRUE(s.ok());

        TransferStatus status;
        do {
            s = client_engine->getTransferStatus(batch_id, 0, status);
            ASSERT_TRUE(s.ok());
        } while (status.s == TransferStatusEnum::WAITING);

        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
        s = client_engine->freeBatchID(batch_id);
        ASSERT_TRUE(s.ok());
    }

    std::vector<char> host_check(kDataLength);
    checkSunriseError(
        tangMemcpy(host_check.data(),
                   static_cast<char*>(client_buffer) + kDataLength, kDataLength,
                   tangMemcpyDeviceToHost),
        "Memcpy from client_buffer");
    for (size_t i = 0; i < kDataLength; ++i) {
        ASSERT_EQ(host_check[i], 'A');
    }

    client_engine->unregisterLocalMemory(client_buffer);
    freeSunriseBuffer(client_buffer);
    server_engine->unregisterLocalMemory(server_buffer);
    freeSunriseBuffer(server_buffer);
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
