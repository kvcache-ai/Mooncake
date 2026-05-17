#include <gtest/gtest.h>
#include <glog/logging.h>

#include <string>
#include <vector>

#include "client.h"
#include "nds/nds_interface.h"
#include "types.h"

using namespace mooncake;

class GdsIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize GDS
        int32_t result = NDS::init(nullptr, 0);
        EXPECT_EQ(result, 0) << "Failed to initialize NDS";
        
        // Create client
        std::string local_hostname = "127.0.0.1:8080";
        std::string metadata_connstring = "127.0.0.1:8081";
        std::string protocol = "tcp";
        
        client_ = Client::Create(local_hostname, metadata_connstring, protocol);
        ASSERT_TRUE(client_) << "Failed to create client";
    }
    
    void TearDown() override {
        // Clean up if needed
    }
    
    std::shared_ptr<Client> client_;
};

// Test normal put-get operation
TEST_F(GdsIntegrationTest, NormalPutGet) {
    std::string key = "test_key_123";
    std::string value = "Hello, NDS!";
    
    // Create slice with data
    std::vector<Slice> slices;
    slices.push_back({const_cast<char*>(value.data()), value.size()});
    
    // Create replicate config
    ReplicateConfig config;
    config.memory_replica = 0;
    config.disk_replica = 1;
    
    // Put data
    auto put_result = client_->Put(key, slices, config);
    EXPECT_TRUE(put_result.has_value()) << "Put operation failed: " << toString(put_result.error());
    
    // Get data back
    std::vector<Slice> read_slices;
    std::vector<char> read_buffer(value.size());
    read_slices.push_back({read_buffer.data(), read_buffer.size()});
    
    auto get_result = client_->Get(key, read_slices);
    EXPECT_TRUE(get_result.has_value()) << "Get operation failed: " << toString(get_result.error());
    
    // Verify data
    std::string read_value(read_buffer.data(), read_buffer.size());
    EXPECT_EQ(read_value, value) << "Read data doesn't match written data";
}

// Test exception case: non-existent blockId
TEST_F(GdsIntegrationTest, NonExistentBlockId) {
    // This test needs to be modified based on actual GDS behavior
    // For now, we'll test the error handling in our code
    
    std::string key = "non_existent_key";
    std::vector<Slice> slices;
    
    // Create a buffer to read into
    std::vector<char> read_buffer(100);
    slices.push_back({read_buffer.data(), read_buffer.size()});
    
    // Try to read non-existent key
    auto get_result = client_->Get(key, slices);
    
    // Depending on the implementation, this might return OBJECT_NOT_FOUND
    // or another error code. Adjust the expectation based on actual behavior.
    EXPECT_FALSE(get_result.has_value()) << "Expected Get operation to fail for non-existent key";
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}