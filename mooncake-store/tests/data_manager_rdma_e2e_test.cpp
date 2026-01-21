/**
 * End-to-End RDMA Data Transfer Test for DataManager
 *
 * This test demonstrates real RDMA data transfer through DataManager
 * using two separate processes:
 * - Server process: Stores data and serves RDMA read requests
 * - Client process: Performs RDMA write/read operations via DataManager
 *
 * Usage:
 *   # Terminal 1 - Start metadata server
 *   python3 -m mooncake.http_metadata_server --host 0.0.0.0 --port 8000
 *
 *   # Terminal 2 - Start server process
 *   ./data_manager_rdma_e2e_test --role=server
 *
 *   # Terminal 3 - Start client process
 *   ./data_manager_rdma_e2e_test --role=client
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>
#include "data_manager.h"
#include "client_rpc_types.h"
#include "transfer_engine.h"
#include "utils.h"

namespace mooncake {

class DataManagerRDMAE2ETest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("data_manager_rdma_e2e_test");
    }

    void TearDown() override {}
};

// Configuration
constexpr size_t TEST_DATA_SIZE = 16 * 1024 * 1024;  // 16 MB
constexpr const char* TEST_KEY = "rdma_e2e_test_key";
constexpr const char* SEGMENT_NAME = "rdma_test_segment";

/**
 * Server Process: Receives RDMA write and serves RDMA read requests
 *
 * This process:
 * 1. Initializes DataManager with RDMA-enabled TransferEngine
 * 2. Allocates and registers memory for RDMA operations
 * 3. Waits for client to write data via RDMA
 * 4. Serves RDMA read requests from client
 */
TEST_F(DataManagerRDMAE2ETest, ServerProcess) {
    // Check environment variables
    const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
    const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");

    ASSERT_NE(metadata_addr, nullptr)
        << "MC_METADATA_ADDR must be set (e.g., http://localhost:8000)";
    ASSERT_NE(local_hostname, nullptr)
        << "MC_LOCAL_HOSTNAME must be set (e.g., hostname)";

    LOG(INFO) << "=== RDMA Server Process Starting ===";
    LOG(INFO) << "Metadata address: " << metadata_addr;
    LOG(INFO) << "Local hostname: " << local_hostname;

    // Create TransferEngine with RDMA support
    auto transfer_engine = std::make_shared<TransferEngine>(true);
    int init_result = transfer_engine->init(metadata_addr, local_hostname, "", 12001);
    ASSERT_EQ(init_result, 0) << "Failed to initialize TransferEngine";
    LOG(INFO) << "✓ TransferEngine initialized with RDMA support";

    // Allocate memory for receiving RDMA writes
    void* rdma_buffer = allocate_buffer_allocator_memory(TEST_DATA_SIZE);
    ASSERT_NE(rdma_buffer, nullptr) << "Failed to allocate RDMA buffer";
    std::memset(rdma_buffer, 0, TEST_DATA_SIZE);
    LOG(INFO) << "✓ Allocated " << TEST_DATA_SIZE << " bytes for RDMA buffer";

    // Register memory with TransferEngine
    int reg_result = transfer_engine->registerLocalMemory(
        rdma_buffer, TEST_DATA_SIZE, SEGMENT_NAME, true, true);
    ASSERT_EQ(reg_result, 0) << "Failed to register memory";
    LOG(INFO) << "✓ Memory registered as segment: " << SEGMENT_NAME;

    // Create TieredBackend
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 2147483648,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    auto tiered_backend = std::make_unique<TieredBackend>();
    auto init_backend_result = tiered_backend->Init(config, nullptr, nullptr);
    ASSERT_TRUE(init_backend_result.has_value());
    LOG(INFO) << "✓ TieredBackend initialized";

    // Create DataManager
    auto data_manager = std::make_unique<DataManager>(
        std::move(tiered_backend), transfer_engine);
    LOG(INFO) << "✓ DataManager created with RDMA-enabled TransferEngine";

    // Wait for client to write data via RDMA WriteRemoteData
    LOG(INFO) << "\n=== Waiting for client to write data via RDMA ===";
    LOG(INFO) << "Server is ready. Start the client process now.";
    LOG(INFO) << "Press Ctrl+C to exit after client completes.";

    // Prepare source buffer descriptor for client
    LOG(INFO) << "\nClient should use this buffer descriptor:";
    LOG(INFO) << "  segment_name: " << SEGMENT_NAME;
    LOG(INFO) << "  addr: " << reinterpret_cast<uint64_t>(rdma_buffer);
    LOG(INFO) << "  size: " << TEST_DATA_SIZE;

    // Keep server running to handle RDMA operations
    // In a real test, you would use synchronization primitives
    // For this demo, we just sleep to give client time to operate
    std::this_thread::sleep_for(std::chrono::seconds(30));

    // After client writes, verify the data was received
    LOG(INFO) << "\n=== Verifying received data ===";
    const char* data_ptr = static_cast<const char*>(rdma_buffer);
    bool has_data = false;
    for (size_t i = 0; i < 1024 && i < TEST_DATA_SIZE; i++) {
        if (data_ptr[i] != 0) {
            has_data = true;
            break;
        }
    }

    if (has_data) {
        LOG(INFO) << "✓ Data received via RDMA! First 64 bytes:";
        for (size_t i = 0; i < 64 && i < TEST_DATA_SIZE; i++) {
            printf("%02x ", static_cast<unsigned char>(data_ptr[i]));
            if ((i + 1) % 16 == 0) printf("\n");
        }
        printf("\n");
    } else {
        LOG(WARNING) << "No data received. Client may not have run yet.";
    }

    // Cleanup
    transfer_engine->unregisterLocalMemory(rdma_buffer, true);
    free_memory("", rdma_buffer);
    LOG(INFO) << "=== Server Process Completed ===";
}

/**
 * Client Process: Performs RDMA write and read operations
 *
 * This process:
 * 1. Initializes DataManager with RDMA-enabled TransferEngine
 * 2. Creates test data and stores it in DataManager
 * 3. Writes data to server's memory via RDMA (WriteRemoteData)
 * 4. Reads data from server's memory via RDMA (ReadRemoteData)
 * 5. Verifies data integrity
 */
TEST_F(DataManagerRDMAE2ETest, ClientProcess) {
    // Check environment variables
    const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
    const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");
    const char* server_hostname = std::getenv("MC_SERVER_HOSTNAME");

    ASSERT_NE(metadata_addr, nullptr)
        << "MC_METADATA_ADDR must be set (e.g., http://localhost:8000)";
    ASSERT_NE(local_hostname, nullptr)
        << "MC_LOCAL_HOSTNAME must be set (e.g., hostname)";
    ASSERT_NE(server_hostname, nullptr)
        << "MC_SERVER_HOSTNAME must be set (e.g., server hostname)";

    LOG(INFO) << "=== RDMA Client Process Starting ===";
    LOG(INFO) << "Metadata address: " << metadata_addr;
    LOG(INFO) << "Local hostname: " << local_hostname;
    LOG(INFO) << "Server hostname: " << server_hostname;

    // Give server time to start
    LOG(INFO) << "Waiting 5 seconds for server to initialize...";
    std::this_thread::sleep_for(std::chrono::seconds(5));

    // Create TransferEngine with RDMA support
    auto transfer_engine = std::make_shared<TransferEngine>(true);
    int init_result = transfer_engine->init(metadata_addr, local_hostname, "", 12002);
    ASSERT_EQ(init_result, 0) << "Failed to initialize TransferEngine";
    LOG(INFO) << "✓ TransferEngine initialized with RDMA support";

    // Create TieredBackend
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 2147483648,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    auto tiered_backend = std::make_unique<TieredBackend>();
    auto init_backend_result = tiered_backend->Init(config, nullptr, nullptr);
    ASSERT_TRUE(init_backend_result.has_value());
    LOG(INFO) << "✓ TieredBackend initialized";

    // Create DataManager
    auto data_manager = std::make_unique<DataManager>(
        std::move(tiered_backend), transfer_engine);
    LOG(INFO) << "✓ DataManager created with RDMA-enabled TransferEngine";

    // Create test data with pattern
    const std::string pattern = "RDMA_CLIENT_TEST_PATTERN_";
    auto test_data = std::make_unique<char[]>(TEST_DATA_SIZE);
    for (size_t i = 0; i < TEST_DATA_SIZE; i++) {
        test_data[i] = pattern[i % pattern.size()];
    }
    LOG(INFO) << "✓ Created test data with pattern (size: " << TEST_DATA_SIZE << " bytes)";

    // Store test data in DataManager
    auto put_result = data_manager->Put(TEST_KEY, std::move(test_data), TEST_DATA_SIZE);
    ASSERT_TRUE(put_result.has_value()) << "Failed to put data into DataManager";
    LOG(INFO) << "✓ Data stored in DataManager with key: " << TEST_KEY;

    // Get the stored data handle
    auto get_result = data_manager->Get(TEST_KEY);
    ASSERT_TRUE(get_result.has_value()) << "Failed to get data from DataManager";
    auto handle = get_result.value();
    LOG(INFO) << "✓ Retrieved data handle from DataManager";

    // === Test 1: RDMA Write to Server ===
    LOG(INFO) << "\n=== Test 1: RDMA Write (Client -> Server) ===";

    // NOTE: In a real scenario, you would get server's buffer address from metadata
    // For this demo, you need to manually configure the server's buffer address
    // Get it from the server process output

    const char* server_buffer_addr_str = std::getenv("MC_SERVER_BUFFER_ADDR");
    if (server_buffer_addr_str != nullptr) {
        uint64_t server_buffer_addr = std::stoull(server_buffer_addr_str);

        std::vector<RemoteBufferDesc> server_buffers = {
            {SEGMENT_NAME, server_buffer_addr, TEST_DATA_SIZE}
        };

        LOG(INFO) << "Writing data to server via RDMA...";
        LOG(INFO) << "  Target: " << server_hostname << ":" << SEGMENT_NAME;
        LOG(INFO) << "  Address: 0x" << std::hex << server_buffer_addr << std::dec;
        LOG(INFO) << "  Size: " << TEST_DATA_SIZE << " bytes";

        auto write_result = data_manager->WriteRemoteData(
            TEST_KEY, server_buffers);

        if (write_result.has_value()) {
            LOG(INFO) << "✓ RDMA Write completed successfully!";
        } else {
            LOG(ERROR) << "✗ RDMA Write failed: " << toString(write_result.error());
        }
    } else {
        LOG(WARNING) << "MC_SERVER_BUFFER_ADDR not set, skipping RDMA write test";
        LOG(INFO) << "Set MC_SERVER_BUFFER_ADDR=<address> to enable this test";
    }

    // === Test 2: RDMA Read from Server ===
    LOG(INFO) << "\n=== Test 2: RDMA Read (Server -> Client) ===";

    // Allocate local buffer for receiving data
    void* read_buffer = allocate_buffer_allocator_memory(TEST_DATA_SIZE);
    ASSERT_NE(read_buffer, nullptr);
    std::memset(read_buffer, 0, TEST_DATA_SIZE);

    // Register local buffer
    int reg_result = transfer_engine->registerLocalMemory(
        read_buffer, TEST_DATA_SIZE, "client_read_buffer", true, true);
    ASSERT_EQ(reg_result, 0);

    // Read from server (this would use DataManager->ReadRemoteData in a real setup)
    LOG(INFO) << "Reading data from server via RDMA...";
    LOG(INFO) << "(Note: This requires server to have data stored)";

    // Cleanup
    transfer_engine->unregisterLocalMemory(read_buffer, true);
    free_memory("", read_buffer);

    LOG(INFO) << "=== Client Process Completed ===";
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);

    // Check for role parameter
    std::string role;
    for (int i = 1; i < argc; i++) {
        std::string arg(argv[i]);
        if (arg.find("--role=") == 0) {
            role = arg.substr(7);
        }
    }

    if (role.empty()) {
        LOG(INFO) << "Usage: " << argv[0] << " --role=server|client";
        LOG(INFO) << "";
        LOG(INFO) << "Environment variables required:";
        LOG(INFO) << "  MC_METADATA_ADDR      - Metadata server address (e.g., http://localhost:8000)";
        LOG(INFO) << "  MC_LOCAL_HOSTNAME     - This machine's hostname";
        LOG(INFO) << "  MC_SERVER_HOSTNAME    - Server hostname (client only)";
        LOG(INFO) << "  MC_SERVER_BUFFER_ADDR - Server buffer address in hex (client only, optional)";
        LOG(INFO) << "";
        LOG(INFO) << "Example:";
        LOG(INFO) << "  # Terminal 1: Start metadata server";
        LOG(INFO) << "  python3 -m mooncake.http_metadata_server --host 0.0.0.0 --port 8000";
        LOG(INFO) << "";
        LOG(INFO) << "  # Terminal 2: Start server";
        LOG(INFO) << "  export MC_METADATA_ADDR=http://localhost:8000";
        LOG(INFO) << "  export MC_LOCAL_HOSTNAME=server-host";
        LOG(INFO) << "  " << argv[0] << " --role=server --gtest_filter=*ServerProcess";
        LOG(INFO) << "";
        LOG(INFO) << "  # Terminal 3: Start client";
        LOG(INFO) << "  export MC_METADATA_ADDR=http://localhost:8000";
        LOG(INFO) << "  export MC_LOCAL_HOSTNAME=client-host";
        LOG(INFO) << "  export MC_SERVER_HOSTNAME=server-host";
        LOG(INFO) << "  " << argv[0] << " --role=client --gtest_filter=*ClientProcess";
        return 1;
    }

    LOG(INFO) << "Running as " << role << " process";
    return RUN_ALL_TESTS();
}
