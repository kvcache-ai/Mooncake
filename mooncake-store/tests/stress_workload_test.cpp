#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <numa.h>

#include <memory>
#include <random>
#include <string>
#include <vector>

#include "allocator.h"
#include "client.h"
#include "types.h"
#include "utils.h"

DEFINE_string(protocol, "rdma", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "ibp6s0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(master_address, "localhost:50051", "Address of master server");

namespace mooncake {
namespace testing {

class RandomGen {
   public:
    std::random_device req_rd;
    std::vector<std::string> random_key;
    std::vector<std::string> random_value;
    int max_len;
    int max_key_size;
    int max_value_size;
    bool use_skew;
    long long ran_counter;
    const std::string characters =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789";
    RandomGen(int max_len_, int max_key_size_, int max_value_size_)
        : max_len(max_len_),
          max_key_size(max_key_size_),
          max_value_size(max_value_size_),
          ran_counter(0) {
        uniform();
    }
    ~RandomGen() {}

    void skew() {
        // not implement
    }

    std::string generateRandomString(size_t length) {
        std::string randomString;
        size_t charactersCount = characters.size();

        for (size_t i = 0; i < length; ++i) {
            // Generate a random index
            size_t randomIndex = req_rd() % charactersCount;
            // Append the character at the random index to the random string
            randomString += characters[randomIndex];
        }

        return randomString;
    }

    void uniform() {
        for (int i = 0; i < max_len; i++) {
            random_key.push_back(generateRandomString(max_key_size));
            std::string value;
            value.resize(max_value_size,
                         characters[req_rd() % characters.size()]);
            random_value.push_back(value);
        }
    }

    std::pair<std::string, std::string> get_pair() {
        ran_counter++;
        ran_counter %= max_len;
        return {random_key[ran_counter], random_value[ran_counter]};
    }

    std::string get_key() {
        ran_counter++;
        ran_counter %= max_len;
        return random_key[ran_counter];
    }
    std::string get_value() {
        ran_counter++;
        ran_counter %= max_len;
        return random_value[ran_counter];
    }
};

class ClientIntegrationTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTest");
        FLAGS_logtostderr = 1;

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name;
        InitializeClient();
        InitializeSegment();
    }

    static void TearDownTestSuite() {
        // Cleanup client, server, and master components
        CleanupSegment();
        CleanupClient();

        google::ShutdownGoogleLogging();
    }

    static void InitializeSegment() {
        ram_buffer_size_ = 3200ull * 1024 * 1024;
        segment_ptr_ = allocate_buffer_allocator_memory(ram_buffer_size_);
        ASSERT_TRUE(segment_ptr_);
        ErrorCode rc = client_->MountSegment(segment_ptr_,
                                             ram_buffer_size_);
        if (rc != ErrorCode::OK) {
            LOG(ERROR) << "Failed to mount segment: " << toString(rc);
        }
    }

    static void CleanupSegment() {
        if (client_->UnmountSegment(segment_ptr_, ram_buffer_size_) !=
            ErrorCode::OK) {
            LOG(ERROR) << "Failed to unmount segment";
        }
    }

    static void InitializeClient() {
        void** args =
            (FLAGS_protocol == "rdma") ? rdma_args(FLAGS_device_name) : nullptr;

        auto client_opt =
            Client::Create("localhost:12345",  // Local hostname
                           "127.0.0.1:2379",   // Metadata connection string
                           FLAGS_protocol, args, FLAGS_master_address);

        ASSERT_TRUE(client_opt.has_value()) << "Failed to create client";
        client_ = *client_opt;

        auto client_buffer_allocator_size = 128 * 1024 * 1024;
        client_buffer_allocator_ =
            std::make_unique<SimpleAllocator>(client_buffer_allocator_size);
        ErrorCode error_code = client_->RegisterLocalMemory(
            client_buffer_allocator_->getBase(), client_buffer_allocator_size,
            "cpu:0", false, false);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR) << "Failed to allocate transfer buffer: "
                       << toString(error_code);
        }
    }

    static void CleanupClient() {
        if (client_) {
            client_.reset();
        }
    }

    static std::shared_ptr<Client> client_;
    static std::unique_ptr<SimpleAllocator> client_buffer_allocator_;
    static void* segment_ptr_;
    static size_t ram_buffer_size_;
};

// Static members initialization
std::shared_ptr<Client> ClientIntegrationTest::client_ = nullptr;
void* ClientIntegrationTest::segment_ptr_ = nullptr;
std::unique_ptr<SimpleAllocator>
    ClientIntegrationTest::client_buffer_allocator_ = nullptr;
size_t ClientIntegrationTest::ram_buffer_size_ = 0;

// Test basic Put/Get operations through the client
TEST_F(ClientIntegrationTest, StressPutOperations) {
    const static int kThreads = 8;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, kThreads + 1);
    std::vector<std::thread> runner_list;
    const int rand_len = 100;
    const int value_length = kMaxSliceSize;

    for (int i = 0; i < kThreads; ++i) {
        runner_list.push_back(std::thread([&]() {
            RandomGen rand_gen(rand_len, 128, value_length);

            // Test Put operation
            ReplicateConfig config;
            config.replica_num = 1;

            void* write_buffer =
                client_buffer_allocator_->allocate(value_length);
            std::vector<Slice> slices;
            slices.emplace_back(Slice{write_buffer, value_length});
            pthread_barrier_wait(&barrier);
            for (int i = 0; i < rand_len; i++) {
                auto entry = rand_gen.get_pair();
                memcpy(write_buffer, entry.second.data(), entry.second.size());
                slices[0].size = entry.second.size();
                ASSERT_EQ(client_->Put(entry.first.data(), slices, config),
                          ErrorCode::OK);
                ASSERT_EQ(client_->Get(entry.first.data(), slices),
                          ErrorCode::OK);
                ASSERT_EQ(client_->Remove(entry.first.data()), ErrorCode::OK);
            }
            pthread_barrier_wait(&barrier);
            client_buffer_allocator_->deallocate(write_buffer, value_length);
        }));
    }

    LOG(INFO) << "Begin testing";
    pthread_barrier_wait(&barrier);
    auto start_ts = getCurrentTimeInNano();
    pthread_barrier_wait(&barrier);
    auto end_ts = getCurrentTimeInNano();
    auto duration_ms = (end_ts - start_ts) / 1000000.0;
    LOG(INFO) << "duration/ms: " << duration_ms << " throughput/kB/s: "
              << 2 * value_length * 1.00 * rand_len * kThreads / duration_ms;
    for (auto& runner : runner_list) runner.join();
    runner_list.clear();
}

}  // namespace testing
}  // namespace mooncake
