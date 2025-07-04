#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>
#include <vector>

#include "client_wrapper.h"
#include "e2e_utils.h"
#include "process_handler.h"
#include "types.h"
#include "utils.h"

// Command line flags
USE_engine_flags;
DEFINE_string(master_server_entry, "etcd://0.0.0.0:2379",
              "Master server entry");
DEFINE_int32(port, 9001, "Port to use for the client");
// Relative probability for each operation.
DEFINE_int32(put_prob, 1000, "Probability weight for PUT operations");
DEFINE_int32(get_prob, 1000, "Probability weight for GET operations");
DEFINE_int32(mount_prob, 2, "Probability weight for MOUNT operations");
DEFINE_int32(unmount_prob, 1, "Probability weight for UNMOUNT operations");

// Segment memory size: maximum 1280MB per client
const int kMaxSegmentNum = 10;
const int kSegmentSize = 1024 * 1024 * 128;

namespace mooncake {
namespace testing {

class ClientRunner {
   public:
    void Run() {
        // Try until the client is created successfully.
        bool create_success = false;
        while (!create_success) {
            create_success = CreateClient();
            if (!create_success) {
                sleep(1);
            }
        }
        // At lease mount one segment.
        Mount();

        const int kOpProbTotal = FLAGS_put_prob + FLAGS_get_prob +
                                 FLAGS_mount_prob + FLAGS_unmount_prob;
        // Randomly generate operations and execute them.
        while (true) {
            int operation = rand() % kOpProbTotal;
            if (operation < FLAGS_put_prob) {
                Put();
            } else if (operation < FLAGS_put_prob + FLAGS_get_prob) {
                Get();
            } else if (operation <
                       FLAGS_put_prob + FLAGS_get_prob + FLAGS_mount_prob) {
                Mount();
            } else {
                Unmount();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

   private:
    bool CreateClient() {
        std::string hostname = "localhost:" + std::to_string(FLAGS_port);

        auto client_opt = ClientTestWrapper::CreateClientWrapper(
            hostname, FLAGS_engine_meta_url, FLAGS_protocol, FLAGS_device_name,
            FLAGS_master_server_entry);

        if (!client_opt.has_value()) {
            return false;
        }

        client_ = client_opt.value();
        LOG(INFO) << "Successfully created client";
        return true;
    }

    // Randomly generate a key and value. The value can be computed from the
    // key. This feature is used by verify_kv to verify the correctness of
    // the value.
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

        ErrorCode error_code = client_->Put(key, value);

        if (error_code != ErrorCode::OK) {
            LOG(INFO) << TEST_PUT_FAILURE_STR << " key=" << key
                      << " value=" << value
                      << " error=" << toString(error_code);
        } else {
            LOG(INFO) << TEST_PUT_SUCCESS_STR << " key=" << key
                      << " value=" << value;
        }
    }

    void Get() {
        std::string key;
        gen_key(key);

        std::string value;
        ErrorCode error_code = client_->Get(key, value);

        if (error_code != ErrorCode::OK) {
            LOG(INFO) << TEST_GET_FAILURE_STR << " key=" << key
                      << " error=" << toString(error_code);
            return;
        }

        if (!verify_kv(key, value)) {
            LOG(ERROR) << TEST_ERROR_STR << " key=" << key
                       << " value=" << value;
            return;
        } else {
            LOG(INFO) << TEST_GET_SUCCESS_STR << " key=" << key
                      << " value=" << value;
        }
    }

    void Mount() {
        if (segments_.size() >= kMaxSegmentNum) {
            return;
        }

        void* buffer;
        ErrorCode error_code = client_->Mount(kSegmentSize, buffer);
        if (error_code != ErrorCode::OK) {
            LOG(INFO) << TEST_MOUNT_FAILURE_STR
                      << " error=" << toString(error_code);
            return;
        } else {
            segments_.emplace_back(buffer);
            LOG(INFO) << TEST_MOUNT_SUCCESS_STR << " buffer=" << buffer
                      << " size=" << kSegmentSize;
        }
    }

    void Unmount() {
        if (segments_.empty()) {
            return;
        }
        int index = rand() % segments_.size();
        void* base = segments_[index];
        ErrorCode error_code = client_->Unmount(base);
        if (error_code != ErrorCode::OK) {
            LOG(INFO) << TEST_UNMOUNT_FAILURE_STR
                      << " error=" << toString(error_code);
        } else {
            segments_.erase(segments_.begin() + index);
            LOG(INFO) << TEST_UNMOUNT_SUCCESS_STR << " buffer=" << base;
        }
    }

   private:
    std::shared_ptr<ClientTestWrapper> client_;
    std::vector<void*> segments_;
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