#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread>
#include <memory>
#include <cstring>
#include <future>

#include "cuda_alike.h"
#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

#define PROTOCOL "ubshmem"

DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server address");
DEFINE_string(local_server_name, "127.0.0.1:12345", "Local server name");
DEFINE_string(local_client_name, "127.0.0.1:12346", "Local client name");
DEFINE_int32(server_npu_id, 0, "Server NPU ID to use");
DEFINE_int32(client_npu_id, 2, "Client NPU ID to use");

static bool checkAcl(aclError result, const char* message) {
    if (result != ACL_ERROR_NONE) {
        const char* errMsg = aclGetRecentErrMsg();
        LOG(ERROR) << message << " (Error code: " << result << " - " << errMsg
                   << ")";
        return false;
    }
    return true;
}

static void checkAclError(aclError result, const char* message) {
    if (!checkAcl(result, message)) {
        exit(EXIT_FAILURE);
    }
}

static void* allocateAclBuffer(size_t size, int npu_id) {
    aclrtPhysicalMemProp prop = {};
    aclrtDrvMemHandle handle;
    void* ptr = nullptr;

    if (!checkAcl(aclrtGetDevice(&npu_id),
                  "UBShmemTransport: aclrtGetDevice failed")) {
        return nullptr;
    }

    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_HBM_MEM_HUGE;
    prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = npu_id;
    prop.reserve = 0;

    auto result = aclrtMallocPhysical(&handle, size, &prop, 0);
    if (!checkAcl(
            result,
            "UBShmemTransport: Failed to allocate specific device memory")) {
        return nullptr;
    }

    uint64_t page_type = 1;
    result = aclrtReserveMemAddress(&ptr, size, 0, nullptr, page_type);
    if (!checkAcl(result, "UBShmemTransport: aclrtReserveMemAddress failed")) {
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    result = aclrtMapMem(ptr, size, 0, handle, 0);
    if (!checkAcl(result, "UBShmemTransport: aclrtMapMem failed")) {
        (void)aclrtReleaseMemAddress(ptr);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    return ptr;
}

static void* allocateAclBufferHost(size_t size, int npu_id) {
    aclrtPhysicalMemProp prop = {};
    aclrtDrvMemHandle handle;
    void* ptr = nullptr;

    if (!checkAcl(aclrtGetDevice(&npu_id),
                  "UBShmemTransport: aclrtGetDevice failed")) {
        return nullptr;
    }

    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_DDR_MEM_P2P_HUGE;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST_NUMA;
    prop.location.id = int(npu_id / 2);
    prop.reserve = 0;

    auto result = aclrtMallocPhysical(&handle, size, &prop, 0);
    if (!checkAcl(
            result,
            "UBShmemTransport: Failed to allocate specific host memory")) {
        return nullptr;
    }

    uint64_t page_type = 1;
    result = aclrtReserveMemAddress(&ptr, size, 0, nullptr, page_type);
    if (!checkAcl(result, "UBShmemTransport: aclrtReserveMemAddress failed")) {
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    result = aclrtMapMem(ptr, size, 0, handle, 0);
    if (!checkAcl(result, "UBShmemTransport: aclrtMapMem failed")) {
        (void)aclrtReleaseMemAddress(ptr);
        (void)aclrtFreePhysical(handle);
        return nullptr;
    }

    return ptr;
}

static void freeAclBuffer(void* addr) {
    aclrtDrvMemHandle handle;

    if (!checkAcl(aclrtMemRetainAllocationHandle(addr, &handle),
                  "UBShmemTransport: aclrtMemRetainAllocationHandle failed")) {
        return;
    }

    (void)aclrtUnmapMem(addr);
    (void)aclrtReleaseMemAddress(addr);
    (void)aclrtFreePhysical(handle);
}

// Server thread function
static void serverThread(int npu_id, const std::string& metadataServer,
                         const std::string& localServerName,
                         std::promise<void>& serverReady,
                         std::future<void>& testComplete) {
    LOG(INFO) << "Server thread starting on NPU " << npu_id;
    checkAclError(aclrtSetDevice(npu_id), "Failed to set device");
    // Server (target) setup
    auto server_engine = std::make_unique<TransferEngine>(false);
    server_engine->init(metadataServer, localServerName);

    // Install ubshmem transport on server
    Transport* server_transport =
        server_engine->installTransport(PROTOCOL, nullptr);
    ASSERT_NE(server_transport, nullptr);

    const size_t kDataLength = 4194304;
    void* server_buffer = allocateAclBufferHost(kDataLength * 2, npu_id);
    int rc = server_engine->registerLocalMemory(server_buffer, kDataLength * 2,
                                                "cpu: 0");
    ASSERT_EQ(rc, 0);

    // Notify main thread that server is ready
    serverReady.set_value();

    // Wait for test to complete
    testComplete.wait();

    // Cleanup
    server_engine->unregisterLocalMemory(server_buffer);
    freeAclBuffer(server_buffer);

    LOG(INFO) << "Server thread completed";
}

// Client thread function
static void clientThread(int npu_id, const std::string& metadataServer,
                         const std::string& segmentId,
                         std::future<void>& serverReady,
                         std::promise<void>& testComplete) {
    LOG(INFO) << "Client thread starting on NPU " << npu_id;
    checkAclError(aclrtSetDevice(npu_id), "Failed to set device");
    // Wait for server to be ready
    serverReady.wait();

    const size_t kDataLength = 4194304;

    // Client (initiator) setup
    auto client_engine = std::make_unique<TransferEngine>(false);
    client_engine->init(metadataServer, "127.0.0.1:12346");

    // Install ubshmem transport on client
    Transport* client_transport =
        client_engine->installTransport(PROTOCOL, nullptr);
    ASSERT_NE(client_transport, nullptr);

    void* client_buffer = allocateAclBuffer(kDataLength * 2, npu_id);
    int rc = client_engine->registerLocalMemory(
        client_buffer, kDataLength * 2, "npu:" + std::to_string(npu_id));
    ASSERT_EQ(rc, 0);

    auto server_segment_id = client_engine->openSegment(segmentId);
    auto segment_desc =
        client_engine->getMetadata()->getSegmentDescByID(server_segment_id);
    // Write: client -> server
    {
        LOG(INFO) << "Client sending data";

        // Fill client buffer with data
        std::vector<char> host_data(kDataLength, 'A');
        checkAclError(aclrtMemcpy(client_buffer, kDataLength, host_data.data(),
                                  kDataLength, ACL_MEMCPY_HOST_TO_DEVICE),
                      "Failed to Memcpy to client_buffer");

        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = client_buffer;
        entry.target_id = server_segment_id;
        entry.target_offset = (uint64_t)segment_desc->buffers[0].addr;
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

        LOG(INFO) << "Client write completed";
    }

    // Read: server -> client
    {
        LOG(INFO) << "Client receiving data";

        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (char*)client_buffer + kDataLength;
        entry.target_id = server_segment_id;
        entry.target_offset = (uint64_t)segment_desc->buffers[0].addr;
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

        LOG(INFO) << "Client read completed";
    }

    // Check data
    std::vector<char> host_check(kDataLength);
    checkAclError(aclrtMemcpy(host_check.data(), kDataLength,
                              (char*)client_buffer + kDataLength, kDataLength,
                              ACL_MEMCPY_DEVICE_TO_HOST),
                  "Failed to Memcpy from client_buffer");
    for (size_t i = 0; i < kDataLength; ++i) {
        ASSERT_EQ(host_check[i], 'A');
    }

    // Cleanup
    client_engine->unregisterLocalMemory(client_buffer);
    freeAclBuffer(client_buffer);

    // Notify server to cleanup
    testComplete.set_value();

    LOG(INFO) << "Client thread completed";
}

TEST(UBShmemTransportTest, WriteAndReadCrossNPU) {
    LOG(INFO) << "Test started: Server on NPU " << FLAGS_server_npu_id
              << ", Client on NPU " << FLAGS_client_npu_id;

    // Create promises and futures for synchronization
    std::promise<void> serverReadyPromise;
    std::future<void> serverReadyFuture = serverReadyPromise.get_future();

    std::promise<void> testCompletePromise;
    std::future<void> testCompleteFuture = testCompletePromise.get_future();

    // Start server thread
    std::thread serverThreadObj(serverThread, FLAGS_server_npu_id,
                                FLAGS_metadata_server, FLAGS_local_server_name,
                                std::ref(serverReadyPromise),
                                std::ref(testCompleteFuture));

    // Start client thread
    std::thread clientThreadObj(clientThread, FLAGS_client_npu_id,
                                FLAGS_metadata_server, FLAGS_local_server_name,
                                std::ref(serverReadyFuture),
                                std::ref(testCompletePromise));

    // Wait for both threads to complete
    if (serverThreadObj.joinable()) {
        serverThreadObj.join();
    }
    if (clientThreadObj.joinable()) {
        clientThreadObj.join();
    }

    LOG(INFO) << "Test completed successfully";
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
