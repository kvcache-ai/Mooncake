#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread>
#include <memory>
#include <cstring>

#include "cuda_alike.h"
#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

// Select protocol based on build configuration
#ifdef USE_HIP
#define MNNVL_PROTOCOL "hip"
#else
#define MNNVL_PROTOCOL "nvlink"
#endif

DEFINE_string(metadata_server, "127.0.0.1:2379", "etcd server host address");
DEFINE_string(local_server_name, "cuda_server:12345", "Local server name");
DEFINE_string(segment_id, "cuda_server:12345", "Segment ID to access data");
DEFINE_int32(gpu_id, 0, "GPU ID to use");

static void checkCudaError(cudaError_t result, const char* message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")";
        exit(EXIT_FAILURE);
    }
}

static void* allocateCudaBuffer(size_t size, int gpu_id) {
    checkCudaError(cudaSetDevice(gpu_id), "Failed to set device");
    void* d_buf = nullptr;
    checkCudaError(cudaMalloc(&d_buf, size),
                   "Failed to allocate device memory");
    return d_buf;
}

static void freeCudaBuffer(void* addr) {
    checkCudaError(cudaFree(addr), "Failed to free device memory");
}

TEST(NvlinkTransportTest, WriteAndRead) {
    const size_t kDataLength = 4096000;
    int gpu_id = FLAGS_gpu_id;

    // Server (target) setup
    auto server_engine = std::make_unique<TransferEngine>(false);
    server_engine->init(FLAGS_metadata_server, FLAGS_local_server_name);

    // Install MNNVL transport (nvlink or hip) on server
    Transport* server_transport =
        server_engine->installTransport(MNNVL_PROTOCOL, nullptr);
    ASSERT_NE(server_transport, nullptr);

    void* server_buffer = allocateCudaBuffer(kDataLength * 2, gpu_id);
    int rc = server_engine->registerLocalMemory(server_buffer, kDataLength * 2,
                                                "cuda:0");
    ASSERT_EQ(rc, 0);

    auto segment_id = server_engine->openSegment(FLAGS_segment_id);

    // Client (initiator) setup
    auto client_engine = std::make_unique<TransferEngine>(false);
    client_engine->init(FLAGS_metadata_server, "cuda_client:12346");

    // Install MNNVL transport (nvlink or hip) on client
    Transport* client_transport =
        client_engine->installTransport(MNNVL_PROTOCOL, nullptr);
    ASSERT_NE(client_transport, nullptr);

    void* client_buffer = allocateCudaBuffer(kDataLength * 2, gpu_id);
    rc = client_engine->registerLocalMemory(client_buffer, kDataLength * 2,
                                            "cuda:" + std::to_string(gpu_id));
    ASSERT_EQ(rc, 0);

    // Write: client -> server
    {
        // Fill client buffer with data
        std::vector<char> host_data(kDataLength, 'A');
        checkCudaError(cudaMemcpy(client_buffer, host_data.data(), kDataLength,
                                  cudaMemcpyHostToDevice),
                       "Memcpy to client_buffer");

        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = client_buffer;
        entry.target_id = segment_id;
        entry.target_offset = (uint64_t)server_buffer;
        Status s = client_engine->submitTransfer(batch_id, {entry});
        ASSERT_TRUE(s.ok());

        // Wait for completion
        TransferStatus status;
        do {
            s = client_engine->getTransferStatus(batch_id, 0, status);
            ASSERT_TRUE(s.ok());
        } while (status.s == TransferStatusEnum::WAITING);

        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
        s = client_engine->freeBatchID(batch_id);
        ASSERT_TRUE(s.ok());
    }

    // Read: server -> client
    {
        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (char*)client_buffer + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = (uint64_t)server_buffer;
        Status s = client_engine->submitTransfer(batch_id, {entry});
        ASSERT_TRUE(s.ok());

        // Wait for completion
        TransferStatus status;
        do {
            s = client_engine->getTransferStatus(batch_id, 0, status);
            ASSERT_TRUE(s.ok());
        } while (status.s == TransferStatusEnum::WAITING);

        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
        s = client_engine->freeBatchID(batch_id);
        ASSERT_TRUE(s.ok());
    }

    // Check data
    std::vector<char> host_check(kDataLength);
    checkCudaError(
        cudaMemcpy(host_check.data(), (char*)client_buffer + kDataLength,
                   kDataLength, cudaMemcpyDeviceToHost),
        "Memcpy from client_buffer");
    for (size_t i = 0; i < kDataLength; ++i) {
        ASSERT_EQ(host_check[i], 'A');
    }

    // Cleanup
    client_engine->unregisterLocalMemory(client_buffer);
    freeCudaBuffer(client_buffer);
    server_engine->unregisterLocalMemory(server_buffer);
    freeCudaBuffer(server_buffer);
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}