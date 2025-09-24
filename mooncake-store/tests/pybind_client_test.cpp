#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <random>
#include <barrier>

#include "pybind_client.h"
#include "test_server_helpers.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

class PyClientTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("PyClientTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        // Override flags from environment variables if present
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Metadata: P2PHANDSHAKE";

        py_client_ = PyClient::create();
    }

    void TearDown() override {
        if (py_client_) {
            py_client_->tearDownAll();
        }

        master_.Stop();
    }

    std::shared_ptr<PyClient> py_client_;

    // In-proc master for tests
    mooncake::testing::InProcMaster master_;
    std::string master_address_;
};

// Test basic Put and Get operations
TEST_F(PyClientTest, BasicPutGetOperations) {
    // Start in-proc master
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
        << "Failed to start in-proc master";
    master_address_ = master_.master_address();
    LOG(INFO) << "Started in-proc master at " << master_address_;

    // Setup the client
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");
    ASSERT_EQ(py_client_->setup("localhost:17813", "P2PHANDSHAKE",
                                16 * 1024 * 1024, 16 * 1024 * 1024,
                                FLAGS_protocol, rdma_devices, master_address_),
              0);

    const std::string test_data = "Hello, PyClient!";
    const std::string key = "test_key_pyclient";

    // Test Put operation using span
    std::span<const char> data_span(test_data.data(), test_data.size());
    ReplicateConfig config;
    config.replica_num = 1;

    int put_result = py_client_->put(key, data_span, config);
    EXPECT_EQ(put_result, 0) << "Put operation should succeed";

    // Test Get operation using buffer handle
    auto buffer_handle = py_client_->get_buffer(key);
    ASSERT_TRUE(buffer_handle != nullptr) << "Get buffer should succeed";
    EXPECT_EQ(buffer_handle->size(), test_data.size())
        << "Buffer size should match";

    // Verify the data
    std::string retrieved_data(static_cast<const char*>(buffer_handle->ptr()),
                               buffer_handle->size());
    EXPECT_EQ(retrieved_data, test_data)
        << "Retrieved data should match original";

    // Test isExist
    int exist_result = py_client_->isExist(key);
    EXPECT_EQ(exist_result, 1) << "Key should exist";
}

// Test Get Operation will fail if the lease has expired.
// Set the lease time to 1ms and use large data size to ensure the lease will
// expire.
TEST_F(PyClientTest, GetWithLeaseTimeOut) {
    // Start in-proc master
    const uint64_t kv_lease_ttl_ = 1;
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl_)
                                  .build()))
        << "Failed to start in-proc master";
    master_address_ = master_.master_address();

    // Setup the client
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");
    ASSERT_EQ(py_client_->setup("localhost:17813", "P2PHANDSHAKE",
                                512 * 1024 * 1024, 512 * 1024 * 1024,
                                FLAGS_protocol, rdma_devices, master_address_),
              0);

    const size_t data_size = 256 * 1024 * 1024;  // 256MB
    std::string test_data(data_size, 'A');       // Fill with 'A' characters
    // Register buffers for zero-copy operations
    int reg_result =
        py_client_->register_buffer(test_data.data(), test_data.size());
    EXPECT_EQ(reg_result, 0) << "Buffer registration should succeed";

    // Test Single Get Operation
    {
        const std::string key = "test_key_pyclient";

        // Put the data
        std::span<const char> data_span(test_data.data(), test_data.size());
        ReplicateConfig config;
        config.replica_num = 1;

        int put_result = py_client_->put(key, data_span, config);
        EXPECT_EQ(put_result, 0) << "Put operation should succeed";

        // Test Get operation using buffer handle
        auto buffer_handle = py_client_->get_buffer(key);
        ASSERT_TRUE(buffer_handle == nullptr) << "Get buffer should fail";

        // Test Get operation using buffer handle
        auto bytes_read =
            py_client_->get_into(key, test_data.data(), test_data.size());
        ASSERT_TRUE(bytes_read < 0) << "Get into should fail";

        // Clear the data for the next test
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl_));
        ASSERT_EQ(py_client_->remove(key), 0)
            << "Remove operation should succeed";
    }

    // Test Batch Get Operation
    {
        const size_t num_slices = 128;
        const size_t slice_size = data_size / num_slices;
        const std::string key_prefix = "batch_test_key_";

        // Prepare batch data
        std::vector<std::string> keys;
        std::vector<std::span<const char>> data_spans;
        std::vector<void*> buffers;
        std::vector<size_t> sizes;

        for (size_t i = 0; i < num_slices; ++i) {
            const std::string key = key_prefix + std::to_string(i);
            keys.push_back(key);

            const char* slice_data = test_data.data() + (i * slice_size);
            data_spans.emplace_back(slice_data, slice_size);
            buffers.push_back(const_cast<char*>(slice_data));
            sizes.push_back(slice_size);
        }

        // Batch Put operation
        ReplicateConfig config;
        config.replica_num = 1;
        int batch_put_result = py_client_->put_batch(keys, data_spans, config);
        EXPECT_EQ(batch_put_result, 0) << "Batch put operation should succeed";

        // Test Batch Get operation using batch_get_buffer
        auto buffer_handles = py_client_->batch_get_buffer(keys);
        ASSERT_EQ(buffer_handles.size(), num_slices)
            << "Should return handles for all keys";
        int fail_count = 0;
        for (size_t i = 0; i < buffer_handles.size(); ++i) {
            if (buffer_handles[i] == nullptr) {
                fail_count++;
            }
        }
        LOG(INFO) << "Batch get buffer " << fail_count << " out of "
                  << num_slices << " keys failed";
        ASSERT_NE(fail_count, 0) << "Should fail for some keys";

        // Test Batch Get operation using batch_get_into
        auto bytes_read_results =
            py_client_->batch_get_into(keys, buffers, sizes);
        ASSERT_EQ(bytes_read_results.size(), num_slices)
            << "Should return results for all keys";
        fail_count = 0;
        for (size_t i = 0; i < bytes_read_results.size(); ++i) {
            if (bytes_read_results[i] < 0) {
                fail_count++;
            }
        }
        LOG(INFO) << "Batch get into " << fail_count << " out of " << num_slices
                  << " keys failed";
        ASSERT_NE(fail_count, 0) << "Should fail for some keys";

        // Clear the data for the next test
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl_));
        ASSERT_EQ(py_client_->removeAll(), num_slices)
            << "Remove operation should succeed";
    }

    // Unregister buffers
    int unreg_result = py_client_->unregister_buffer(test_data.data());
    EXPECT_EQ(unreg_result, 0) << "Buffer unregistration should succeed";
}

// Concurrent Put and Get and check if will get wrong data.
TEST_F(PyClientTest, ConcurrentPutGetWithLeaseTimeOut) {
    // Start in-proc master
    const uint64_t kv_lease_ttl_ = 1;
    const size_t segment_size = 16 * 1024 * 1024;
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl_)
                                  .build()))
        << "Failed to start in-proc master";
    master_address_ = master_.master_address();

    // Setup the client
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");
    ASSERT_EQ(py_client_->setup("localhost:17813", "P2PHANDSHAKE", segment_size,
                                segment_size, FLAGS_protocol, rdma_devices,
                                master_address_),
              0);

    // Test Single Get Operation with Concurrent Put
    {
        const int num_threads = 4;
        std::vector<std::thread> threads;
        std::barrier sync_barrier(num_threads);

        // Start num_threads threads, each repeatedly putting their slice
        for (int thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
            threads.emplace_back([this, thread_idx, kv_lease_ttl_,
                                  &sync_barrier]() {
                const int num_iterations = 100;
                const size_t slice_size =
                    segment_size / num_threads +
                    1024;  // Ensure total size larger than segment size
                const std::string key =
                    "concurrent_test_key_" + std::to_string(thread_idx);

                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis_2(0, 1);
                std::uniform_int_distribution<> dis_26(0, 26);

                std::vector<char> put_data(slice_size);
                for (size_t i = 0; i < slice_size; ++i) {
                    put_data[i] = 'a' + dis_26(gen);
                }
                std::span<const char> data_span(put_data.data(),
                                                put_data.size());
                ReplicateConfig config;
                config.replica_num = 1;

                std::vector<char> get_data(slice_size);
                auto reg_result = py_client_->register_buffer(get_data.data(),
                                                              get_data.size());
                ASSERT_EQ(reg_result, 0)
                    << "Buffer registration should succeed";

                // Wait for all threads to be ready before starting iterations
                sync_barrier.arrive_and_wait();

                for (int iter = 0; iter < num_iterations; ++iter) {
                    py_client_->put(key, data_span, config);

                    // Randomly choose between get_buffer and get_into
                    if (dis_2(gen) == 0) {
                        auto buffer_handle = py_client_->get_buffer(key);
                        if (buffer_handle != nullptr) {
                            // Verify the retrieved data matches the put data
                            ASSERT_EQ(buffer_handle->size(), slice_size)
                                << "Buffer size should match for thread "
                                << thread_idx;
                            for (size_t i = 0; i < slice_size; ++i) {
                                ASSERT_EQ(static_cast<const char*>(
                                              buffer_handle->ptr())[i],
                                          put_data[i])
                                    << "Retrieved data should match put data "
                                       "for thread "
                                    << thread_idx;
                            }
                        }
                    } else {
                        auto bytes_read = py_client_->get_into(
                            key, get_data.data(), slice_size);
                        if (bytes_read > 0) {
                            // Verify the retrieved data matches the put data
                            ASSERT_EQ(bytes_read, static_cast<int>(slice_size))
                                << "Bytes read should match slice size for "
                                   "thread "
                                << thread_idx;
                            for (size_t i = 0; i < slice_size; ++i) {
                                ASSERT_EQ(static_cast<const char*>(
                                              get_data.data())[i],
                                          put_data[i])
                                    << "Retrieved data should match put data "
                                       "for thread "
                                    << thread_idx;
                            }
                        }
                    }

                    // Small delay to allow lease timeout to occur
                    std::this_thread::sleep_for(
                        std::chrono::microseconds(kv_lease_ttl_));
                }

                auto unreg_result =
                    py_client_->unregister_buffer(get_data.data());
                ASSERT_EQ(unreg_result, 0)
                    << "Buffer unregistration should succeed";
            });
        }

        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }

        // Clear the data for the next test
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl_));
        py_client_->removeAll();
    }

    // Test Batch Get Operation with Concurrent Put
    {
        const int num_threads = 4;
        std::vector<std::thread> threads;
        std::barrier sync_barrier(num_threads);

        // Start num_threads threads, each putting multiple slices
        for (int thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
            threads.emplace_back([this, thread_idx, kv_lease_ttl_,
                                  &sync_barrier]() {
                const int num_slices = 32;
                const size_t slice_size =
                    segment_size / (num_threads * num_slices) + 1024;
                const size_t data_size = num_slices * slice_size;
                const int num_iterations = 100;

                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis_2(0, 1);
                std::uniform_int_distribution<> dis_26(0, 26);

                // Prepare put data for this thread
                ReplicateConfig config;
                config.replica_num = 1;
                std::vector<char> put_data(data_size);
                for (size_t i = 0; i < data_size; ++i) {
                    put_data[i] = 'a' + dis_26(gen);
                }
                std::span<const char> data_span(put_data.data(), data_size);
                std::vector<std::string> keys;
                std::vector<std::span<const char>> data_spans;
                std::vector<size_t> sizes;

                for (int slice_idx = 0; slice_idx < num_slices; ++slice_idx) {
                    const std::string key = "batch_concurrent_key_" +
                                            std::to_string(thread_idx) + "_" +
                                            std::to_string(slice_idx);
                    keys.push_back(key);

                    data_spans.emplace_back(
                        put_data.data() + slice_idx * slice_size, slice_size);
                    sizes.push_back(slice_size);
                }

                // Prepare get data for this thread
                std::vector<char> get_data(data_size);
                auto reg_result = py_client_->register_buffer(get_data.data(),
                                                              get_data.size());
                ASSERT_EQ(reg_result, 0)
                    << "Buffer registration should succeed";
                std::vector<void*> get_buffers(num_slices);
                for (size_t i = 0; i < num_slices; ++i) {
                    get_buffers[i] = get_data.data() + i * slice_size;
                }

                // Wait for all threads to be ready
                sync_barrier.arrive_and_wait();

                for (int iter = 0; iter < num_iterations; ++iter) {
                    py_client_->put_batch(keys, data_spans, config);

                    if (dis_2(gen) == 0) {
                        // Test Batch Get operation using batch_get_buffer
                        auto buffer_handles =
                            py_client_->batch_get_buffer(keys);
                        for (size_t i = 0; i < buffer_handles.size(); ++i) {
                            if (buffer_handles[i] != nullptr) {
                                ASSERT_EQ(buffer_handles[i]->size(), slice_size)
                                    << "Buffer size should match put data size";
                                for (size_t j = 0; j < slice_size; ++j) {
                                    ASSERT_EQ(static_cast<const char*>(
                                                  buffer_handles[i]->ptr())[j],
                                              put_data[i * slice_size + j])
                                        << "Retrieved data should match put "
                                           "data";
                                }
                            }
                        }
                    } else {
                        // Test Batch Get operation using batch_get_into
                        auto get_results = py_client_->batch_get_into(
                            keys, get_buffers, sizes);
                        for (size_t i = 0; i < get_results.size(); ++i) {
                            if (get_results[i] > 0) {
                                ASSERT_EQ(get_results[i], slice_size)
                                    << "Buffer size should match put data size";
                                for (size_t j = 0; j < slice_size; ++j) {
                                    ASSERT_EQ(static_cast<const char*>(
                                                  get_buffers[i])[j],
                                              put_data[i * slice_size + j])
                                        << "Retrieved data should match put "
                                           "data";
                                }
                            }
                        }
                    }

                    std::this_thread::sleep_for(
                        std::chrono::microseconds(kv_lease_ttl_));
                }

                // Unregister buffers
                auto unreg_result =
                    py_client_->unregister_buffer(get_data.data());
                ASSERT_EQ(unreg_result, 0)
                    << "Buffer unregistration should succeed";
            });
        }

        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }

        // Clear the data for the next test
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl_));
        py_client_->removeAll();
    }
}

}  // namespace testing

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
