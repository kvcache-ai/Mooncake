#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "client_wrapper.h"
#include "e2e_utils.h"
#include "types.h"
#include "utils.h"

USE_engine_flags;
DEFINE_string(master_server_entry, "etcd://0.0.0.0:2379",
              "Master server entry");
DEFINE_string(mode, "", "provider, seed, verify, or pressure");
DEFINE_int32(port, 19001, "Client transfer-engine port");
DEFINE_string(key_prefix, "ha-e2e", "Key prefix");
DEFINE_uint64(start_index, 0, "First object index");
DEFINE_uint64(count, 1000, "Number of objects for seed");
DEFINE_uint64(payload_size, 4096, "Payload bytes per object");
DEFINE_string(manifest, "", "Acknowledged-write manifest path");
DEFINE_uint64(duration_sec, 45, "Pressure duration");
DEFINE_uint64(sleep_ms, 25, "Delay between pressure operations");
DEFINE_uint64(connect_timeout_sec, 30, "Client creation timeout");
DEFINE_uint64(segment_size, 134217728, "Provider memory-segment bytes");

namespace mooncake::testing {
namespace {

std::string Key(uint64_t index) {
    return FLAGS_key_prefix + "-" + std::to_string(index);
}

std::string Payload(uint64_t index) {
    std::string value(FLAGS_payload_size, '\0');
    for (uint64_t offset = 0; offset < FLAGS_payload_size; ++offset) {
        value[offset] = static_cast<char>(
            (index * 1315423911ULL + offset * 2654435761ULL) & 0xff);
    }
    return value;
}

std::shared_ptr<ClientTestWrapper> CreateClient() {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_connect_timeout_sec);
    do {
        auto client = ClientTestWrapper::CreateClientWrapper(
            "localhost:" + std::to_string(FLAGS_port), FLAGS_engine_meta_url,
            FLAGS_protocol, FLAGS_device_name, FLAGS_master_server_entry);
        if (client) return *client;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    } while (std::chrono::steady_clock::now() < deadline);
    return nullptr;
}

class AckManifest {
   public:
    explicit AckManifest(const std::string& path)
        : fd_(open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644)) {}

    ~AckManifest() {
        if (fd_ >= 0) close(fd_);
    }

    bool valid() const { return fd_ >= 0; }

    bool Append(uint64_t index) {
        const std::string line = std::to_string(index) + "\n";
        size_t written = 0;
        while (written < line.size()) {
            const ssize_t n =
                write(fd_, line.data() + written, line.size() - written);
            if (n <= 0) return false;
            written += static_cast<size_t>(n);
        }
        return fsync(fd_) == 0;
    }

   private:
    int fd_;
};

bool ReadManifest(std::vector<uint64_t>& indexes) {
    std::ifstream input(FLAGS_manifest);
    if (!input) return false;
    std::set<uint64_t> seen;
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty() ||
            line.find_first_not_of("0123456789") != std::string::npos) {
            return false;
        }
        try {
            size_t parsed = 0;
            uint64_t index = std::stoull(line, &parsed);
            if (parsed != line.size() || !seen.insert(index).second) {
                return false;
            }
            indexes.push_back(index);
        } catch (const std::exception&) {
            return false;
        }
    }
    return !indexes.empty();
}

bool VerifyOne(ClientTestWrapper& client, uint64_t index) {
    std::string actual;
    const ErrorCode error = client.Get(Key(index), actual);
    if (error != ErrorCode::OK) {
        std::cerr << "get_failed index=" << index
                  << " error=" << toString(error) << '\n';
        return false;
    }
    if (actual != Payload(index)) {
        std::cerr << "value_mismatch index=" << index
                  << " actual_size=" << actual.size() << '\n';
        return false;
    }
    return true;
}

int Seed(ClientTestWrapper& client) {
    AckManifest manifest(FLAGS_manifest);
    if (!manifest.valid()) return 2;
    uint64_t put_ok = 0;
    for (uint64_t offset = 0; offset < FLAGS_count; ++offset) {
        const uint64_t index = FLAGS_start_index + offset;
        const ErrorCode error = client.Put(Key(index), Payload(index));
        if (error != ErrorCode::OK || !manifest.Append(index)) {
            std::cerr << "seed_failed index=" << index
                      << " error=" << toString(error) << '\n';
            return 20;
        }
        ++put_ok;
    }
    uint64_t get_ok = 0;
    for (uint64_t offset = 0; offset < FLAGS_count; ++offset) {
        if (!VerifyOne(client, FLAGS_start_index + offset)) return 21;
        ++get_ok;
    }
    std::cout << "summary mode=seed put_ok=" << put_ok
              << " put_fail=0 get_ok=" << get_ok << " get_fail=0 mismatch=0\n";
    return 0;
}

int Verify(ClientTestWrapper& client) {
    std::vector<uint64_t> indexes;
    if (!ReadManifest(indexes)) return 2;
    uint64_t get_ok = 0;
    for (uint64_t index : indexes) {
        if (!VerifyOne(client, index)) return 21;
        ++get_ok;
    }
    std::cout << "summary mode=verify put_ok=0 put_fail=0 get_ok=" << get_ok
              << " get_fail=0 mismatch=0\n";
    return 0;
}

int Pressure(ClientTestWrapper& client) {
    AckManifest manifest(FLAGS_manifest);
    if (!manifest.valid()) return 2;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_duration_sec);
    uint64_t index = FLAGS_start_index;
    uint64_t put_ok = 0;
    uint64_t get_ok = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        const ErrorCode error = client.Put(Key(index), Payload(index));
        if (error != ErrorCode::OK || !manifest.Append(index)) {
            std::cerr << "pressure_put_failed index=" << index
                      << " error=" << toString(error) << '\n';
            return 20;
        }
        ++put_ok;
        if (!VerifyOne(client, index)) return 21;
        ++get_ok;
        ++index;
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_sleep_ms));
    }
    if (put_ok == 0 || get_ok == 0) return 22;
    std::cout << "summary mode=pressure put_ok=" << put_ok
              << " put_fail=0 get_ok=" << get_ok << " get_fail=0 mismatch=0\n";
    return 0;
}

int Provider(ClientTestWrapper& client) {
    void* buffer = nullptr;
    const ErrorCode error = client.Mount(FLAGS_segment_size, buffer);
    if (error != ErrorCode::OK) {
        std::cerr << "provider_mount_failed error=" << toString(error) << '\n';
        return 20;
    }
    std::cout << "provider_ready size=" << FLAGS_segment_size << std::endl;
    while (true) pause();
}

}  // namespace
}  // namespace mooncake::testing

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    if ((FLAGS_mode != "provider" && FLAGS_mode != "seed" &&
         FLAGS_mode != "verify" && FLAGS_mode != "pressure") ||
        (FLAGS_mode != "provider" && FLAGS_manifest.empty()) ||
        FLAGS_key_prefix.empty() || FLAGS_payload_size == 0 ||
        FLAGS_connect_timeout_sec == 0 ||
        (FLAGS_mode == "provider" && FLAGS_segment_size == 0) ||
        (FLAGS_mode == "seed" && FLAGS_count == 0) ||
        (FLAGS_mode == "pressure" && FLAGS_duration_sec == 0)) {
        std::cerr << "invalid arguments\n";
        return 2;
    }
    auto client = mooncake::testing::CreateClient();
    if (!client) return 10;
    if (FLAGS_mode == "provider") return mooncake::testing::Provider(*client);
    if (FLAGS_mode == "seed") return mooncake::testing::Seed(*client);
    if (FLAGS_mode == "verify") return mooncake::testing::Verify(*client);
    return mooncake::testing::Pressure(*client);
}
