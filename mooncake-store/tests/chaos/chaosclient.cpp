#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "chaos.h"
#include "client.h"
#include "types.h"
#include "utils.h"

// Command line flags
DEFINE_string(metadata_connstring, "http://127.0.0.1:8080/metadata",
              "Metadata connection string for transfer engine");
DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "ibp6s0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(master_server_entry, "etcd://0.0.0.0:2379",
              "Master server entry");
DEFINE_int32(port, 9001, "Port to use for the client");

// Segment memory size: maximum 1280MB per client
const int kMaxSegmentNum = 10;
const int kSegmentSize = 1024 * 1024 * 128;
// Probability of each operation
const int kPutProb = 1000;
const int kGetProb = 1000;
const int kMountProb = 2;
const int kUnmountProb = 1;

namespace mooncake {
namespace testing {

struct SegmentInfo {
    void* base;
    size_t size;
};

class ClientRunner {
   private:
    std::shared_ptr<Client> client_;
    std::vector<SegmentInfo> segments_;

   public:
    ~ClientRunner() {
        for (auto& segment : segments_) {
            free(segment.base);
        }
    }

    void Run() {
        bool create_success = false;
        while (!create_success) {
            create_success = CreateClient();
            if (!create_success) {
                sleep(1);
            }
        }
        Mount();

        const int kOpProbTotal =
            kPutProb + kGetProb + kMountProb + kUnmountProb;
        while (true) {
            int operation = rand() % kOpProbTotal;
            if (operation < kPutProb) {
                Put();
            } else if (operation < kPutProb + kGetProb) {
                Get();
            } else if (operation < kPutProb + kGetProb + kMountProb) {
                Mount();
            } else {
                Unmount();
            }
            // 10 operations per second
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

   private:
    struct SliceGuard {
        std::vector<Slice> slices;
        SliceGuard(std::vector<AllocatedBuffer::Descriptor>& descriptors) {
            slices.resize(descriptors.size());
            for (size_t i = 0; i < descriptors.size(); i++) {
                void* buffer = malloc(descriptors[i].size_);
                slices[i] = Slice{buffer, descriptors[i].size_};
            }
        }
        ~SliceGuard() {
            for (auto& slice : slices) {
                free(slice.ptr);
            }
        }
    };

    bool CreateClient() {
        void** args =
            (FLAGS_protocol == "rdma") ? rdma_args(FLAGS_device_name) : nullptr;

        std::string hostname = "localhost:" + std::to_string(FLAGS_port);

        auto client_opt =
            Client::Create(hostname,  // Local hostname
                           FLAGS_metadata_connstring, FLAGS_protocol, args,
                           FLAGS_master_server_entry);

        if (!client_opt.has_value()) {
            return false;
        }

        client_ = client_opt.value();
        LOG(INFO) << "Successfully created client";
        return true;
    }

    void gen_kv(std::string& key, std::string& value) {
        int key_seed = rand() % 100;
        key = "key_" + std::to_string(key_seed);
        value = "value_" + std::to_string(key_seed * 997);
    }

    void gen_key(std::string& key) {
        int key_seed = rand() % 100;
        key = "key_" + std::to_string(key_seed);
    }

    bool verify_kv(const std::string& key, const std::string& value) {
        // Check if key starts with "key_"
        if (key.substr(0, 4) != "key_") {
            return false;
        }

        // Extract the numeric part from key
        std::string key_num_str = key.substr(4);

        // Check if value starts with "value_"
        if (value.substr(0, 6) != "value_") {
            return false;
        }

        // Extract the numeric part from value
        std::string value_num_str = value.substr(6);

        // Convert to integers and validate
        try {
            int key_num = std::stoi(key_num_str);
            int value_num = std::stoi(value_num_str);

            // Check if key is non-negative and no larger than 1000
            if (key_num < 0 || key_num > 100) {
                return false;
            }

            // Check if value equals key * 997
            return value_num == (key_num * 997);
        } catch (const std::exception&) {
            // If conversion fails, return false
            return false;
        }
    }

    void Put() {
        std::string key, value;
        gen_kv(key, value);

        // Allocate buffer for the value
        void* buffer = malloc(value.size());
        if (!buffer) {
            LOG(ERROR) << CHAOS_ERROR_STR
                       << " Failed to allocate memory for put value";
            exit(1);
        }

        // Copy value to buffer
        memcpy(buffer, value.data(), value.size());

        // Create slices
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buffer, value.size()});

        // Configure replication
        ReplicateConfig config;
        config.replica_num = 1;

        // Perform put operation
        ErrorCode error_code = client_->Put(key, slices, config);

        // Free the buffer
        free(buffer);

        if (error_code != ErrorCode::OK) {
            LOG(INFO) << CHAOS_PUT_FAILURE_STR << " key=" << key
                      << " value=" << value
                      << " error=" << toString(error_code);
        } else {
            LOG(INFO) << CHAOS_PUT_SUCCESS_STR << " key=" << key
                      << " value=" << value;
        }
    }

    void Get() {
        std::string key;
        gen_key(key);

        Client::ObjectInfo object_info;
        ErrorCode error_code = client_->Query(key, object_info);
        if (error_code != ErrorCode::OK) {
            LOG(INFO) << CHAOS_GET_FAILURE_STR << " key=" << key
                      << " error=" << toString(error_code);
            return;
        }

        // Create slices
        std::vector<AllocatedBuffer::Descriptor>& descriptors =
            object_info.replica_list[0].buffer_descriptors;
        SliceGuard slice_guard(descriptors);

        // Perform get operation
        error_code = client_->Get(key, object_info, slice_guard.slices);

        if (error_code != ErrorCode::OK) {
            LOG(INFO) << CHAOS_GET_FAILURE_STR << " key=" << key
                      << " error=" << toString(error_code);
            return;
        }

        // Print the value
        std::string value;
        for (const auto& slice : slice_guard.slices) {
            value.append(static_cast<const char*>(slice.ptr), slice.size);
        }
        if (!verify_kv(key, value)) {
            LOG(ERROR) << CHAOS_ERROR_STR << " key=" << key
                       << " value=" << value;
            return;
        } else {
            LOG(INFO) << CHAOS_GET_SUCCESS_STR << " key=" << key
                      << " value=" << value;
        }
    }

    void Mount() {
        if (segments_.size() >= kMaxSegmentNum) {
            return;
        }
        void* buffer = allocate_buffer_allocator_memory(kSegmentSize);
        if (!buffer) {
            LOG(ERROR) << CHAOS_ERROR_STR
                       << " Failed to allocate memory for segment";
            return;
        }

        ErrorCode error_code = client_->MountSegment(buffer, kSegmentSize);
        if (error_code != ErrorCode::OK) {
            LOG(INFO) << CHAOS_MOUNT_FAILURE_STR
                      << " error=" << toString(error_code);
            free(buffer);
            return;
        } else {
            segments_.emplace_back(SegmentInfo{buffer, kSegmentSize});
            LOG(INFO) << CHAOS_MOUNT_SUCCESS_STR << " buffer=" << buffer
                      << " size=" << kSegmentSize;
        }
    }

    void Unmount() {
        if (segments_.empty()) {
            return;
        }
        int index = rand() % segments_.size();
        SegmentInfo& segment = segments_[index];
        segments_.erase(segments_.begin() + index);
        ErrorCode error_code =
            client_->UnmountSegment(segment.base, segment.size);
        if (error_code != ErrorCode::OK) {
            LOG(INFO) << CHAOS_UNMOUNT_FAILURE_STR
                      << " error=" << toString(error_code);
        } else {
            LOG(INFO) << CHAOS_UNMOUNT_SUCCESS_STR << " buffer=" << segment.base
                      << " size=" << segment.size;
        }
        // Free the buffer no matter unmount succeeds or not
        free(segment.base);
    }
};

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize Google logging
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    mooncake::testing::ClientRunner client_runner;
    client_runner.Run();

    return 0;
}
