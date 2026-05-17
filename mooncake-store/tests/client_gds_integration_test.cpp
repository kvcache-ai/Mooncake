#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client.h"
#include "types.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(transfer_engine_metadata_url, "http://localhost:8080/metadata",
              "Metadata connection string for transfer engine");

namespace mooncake {
namespace testing {

class ClientGdsIntegrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog
        google::InitGoogleLogging("ClientGdsIntegrationTest");
        FLAGS_logtostderr = 1;

        // Create client
        auto client_opt = Client::Create(
            "localhost:17813",                           // Local hostname
            FLAGS_transfer_engine_metadata_url,  // Metadata connection string
            FLAGS_protocol,
            "localhost:50051"  // Master server address
        );

        ASSERT_TRUE(client_opt.has_value())
            << "Failed to create client";
        client_ = client_opt.value();
    }

    void TearDown() override {
        // Clean up
        client_.reset();
        google::ShutdownGoogleLogging();
    }

    std::shared_ptr<Client> client_;
};

// Test basic Put/Get operations with GDS KV
TEST_F(ClientGdsIntegrationTest, BasicPutGetOperations) {
    const std::string test_data = "Hello, GDS KV!";
    const std::string key = "test_key_gds";
    
    // Allocate buffer
    void* buffer = malloc(test_data.size());
    ASSERT_NE(buffer, nullptr);
    
    // Write test data to buffer
    memcpy(buffer, test_data.data(), test_data.size());
    
    // Create slices
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buffer, test_data.size()});
    
    // Test Put operation
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_result = client_->Put(key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put operation failed: " << toString(put_result.error());
    
    // Read back the data
    void* read_buffer = malloc(test_data.size());
    ASSERT_NE(read_buffer, nullptr);
    
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{read_buffer, test_data.size()});
    
    auto get_result = client_->Get(key, read_slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get operation failed: " << toString(get_result.error());
    
    // Verify data
    ASSERT_EQ(memcmp(read_buffer, test_data.data(), test_data.size()), 0);
    
    // Clean up
    free(buffer);
    free(read_buffer);
}

// Test batch Put/Get operations with GDS KV
TEST_F(ClientGdsIntegrationTest, BatchPutGetOperations) {
    int batch_size = 5;
    std::vector<std::string> keys;
    std::vector<std::string> test_data_list;
    std::vector<std::vector<Slice>> batched_slices;
    std::unordered_map<std::string, std::vector<Slice>> read_slices_map;
    
    // Prepare test data
    for (int i = 0; i < batch_size; i++) {
        std::string key = "test_key_gds_batch_" + std::to_string(i);
        std::string test_data = "test_data_" + std::to_string(i);
        
        keys.push_back(key);
        test_data_list.push_back(test_data);
        
        // Create slices for put
        void* buffer = malloc(test_data.size());
        ASSERT_NE(buffer, nullptr);
        memcpy(buffer, test_data.data(), test_data.size());
        
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buffer, test_data.size()});
        batched_slices.push_back(std::move(slices));
        
        // Create slices for get
        void* read_buffer = malloc(test_data.size());
        ASSERT_NE(read_buffer, nullptr);
        
        std::vector<Slice> read_slices;
        read_slices.emplace_back(Slice{read_buffer, test_data.size()});
        read_slices_map[key] = std::move(read_slices);
    }
    
    // Test BatchPut operation
    ReplicateConfig config;
    config.replica_num = 1;
    auto batch_put_results = client_->BatchPut(keys, batched_slices, config);
    
    // Check that all put operations succeeded
    for (const auto& result : batch_put_results) {
        ASSERT_TRUE(result.has_value())
            << "BatchPut operation failed: " << toString(result.error());
    }
    
    // Test BatchGet operation
    auto batch_get_results = client_->BatchGet(keys, read_slices_map);
    
    // Check that all get operations succeeded
    for (const auto& result : batch_get_results) {
        ASSERT_TRUE(result.has_value())
            << "BatchGet operation failed: " << toString(result.error());
    }
    
    // Verify data
    for (int i = 0; i < batch_size; i++) {
        const std::string& key = keys[i];
        const std::string& expected_data = test_data_list[i];
        const auto& slices = read_slices_map[key];
        
        ASSERT_EQ(slices.size(), 1);
        ASSERT_EQ(slices[0].size, expected_data.size());
        ASSERT_EQ(memcmp(slices[0].ptr, expected_data.data(), expected_data.size()), 0);
        
        // Clean up
        free(batched_slices[i][0].ptr);
        free(slices[0].ptr);
    }
}

// Test error handling - reading non-existent key
TEST_F(ClientGdsIntegrationTest, ReadNonExistentKey) {
    const std::string non_existent_key = "non_existent_key_gds";
    
    // Allocate buffer
    void* buffer = malloc(100);
    ASSERT_NE(buffer, nullptr);
    
    // Create slices
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buffer, 100});
    
    // Try to get non-existent key
    auto get_result = client_->Get(non_existent_key, slices);
    ASSERT_FALSE(get_result.has_value())
        << "Get should have failed for non-existent key";
    ASSERT_EQ(get_result.error(), ErrorCode::OBJECT_NOT_FOUND)
        << "Expected OBJECT_NOT_FOUND error, got: " << toString(get_result.error());
    
    // Clean up
    free(buffer);
}

// Test edge case - empty data
TEST_F(ClientGdsIntegrationTest, EmptyData) {
    const std::string empty_data = "";
    const std::string key = "test_key_gds_empty";
    
    // Allocate buffer (small buffer for empty data)
    void* buffer = malloc(1);
    ASSERT_NE(buffer, nullptr);
    
    // Create slices
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buffer, empty_data.size()});
    
    // Test Put operation
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_result = client_->Put(key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put operation failed: " << toString(put_result.error());
    
    // Read back the data
    void* read_buffer = malloc(1);
    ASSERT_NE(read_buffer, nullptr);
    
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{read_buffer, empty_data.size()});
    
    auto get_result = client_->Get(key, read_slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get operation failed: " << toString(get_result.error());
    
    // Clean up
    free(buffer);
    free(read_buffer);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    // Run all tests
    return RUN_ALL_TESTS();
}
