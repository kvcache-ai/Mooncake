// Snapshot uploader daemon - long-running process with persistent etcd
// connection Communicates with master via Unix domain socket for better
// performance
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <limits>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <glog/logging.h>
#include <signal.h>

#ifdef STORE_USE_ETCD
#include "libetcd_wrapper.h"
#endif

constexpr size_t kEtcdMaxValueSize = 2000UL * 1000UL * 1000UL;
constexpr size_t kMaxInlineMessageSize =
    1000UL * 1000UL * 1000UL;  // 1GB per inline message
constexpr size_t kMaxPathSize = 4096;

volatile sig_atomic_t should_exit = 0;

void signal_handler(int sig) { should_exit = 1; }

// Protocol: Request format
// [num_files:4][key_len:4][key:N][payload_type:1][payload_len:4][payload:M]...
// payload_type: 0 = inline data, 1 = local file path
// Protocol: Response format
// [status:4][error_len:4][error_msg:N]

bool ReadExact(int fd, void* buf, size_t len) {
    size_t total = 0;
    char* ptr = static_cast<char*>(buf);
    while (total < len) {
        ssize_t n = read(fd, ptr + total, len - total);
        if (n <= 0) {
            if (n == 0) {
                // Connection closed - could be graceful shutdown or test
                // connection
                VLOG(1) << "[SnapshotDaemon] Connection closed by peer";
            } else {
                LOG(ERROR) << "[SnapshotDaemon] Read error: "
                           << strerror(errno);
            }
            return false;
        }
        total += n;
    }
    return true;
}

bool WriteExact(int fd, const void* buf, size_t len) {
    size_t total = 0;
    const char* ptr = static_cast<const char*>(buf);
    while (total < len) {
        ssize_t n = write(fd, ptr + total, len - total);
        if (n <= 0) {
            LOG(ERROR) << "[SnapshotDaemon] Write error: " << strerror(errno);
            return false;
        }
        total += n;
    }
    return true;
}

bool ProcessRequest(int client_fd) {
    auto request_start = std::chrono::steady_clock::now();

    // Read number of files
    uint32_t num_files;
    if (!ReadExact(client_fd, &num_files, sizeof(num_files))) {
        return false;
    }

    LOG(INFO) << "[SnapshotDaemon] Processing batch upload request, num_files="
              << num_files;

    if (num_files == 0 || num_files > 100) {
        LOG(ERROR) << "[SnapshotDaemon] Invalid num_files: " << num_files;
        uint32_t status = 1;
        std::string error = "Invalid number of files";
        uint32_t error_len = error.size();
        WriteExact(client_fd, &status, sizeof(status));
        WriteExact(client_fd, &error_len, sizeof(error_len));
        WriteExact(client_fd, error.data(), error_len);
        return false;
    }

#ifdef STORE_USE_ETCD
    // Upload all files
    auto upload_start = std::chrono::steady_clock::now();
    std::string error_msg;
    bool all_success = true;

    for (uint32_t i = 0; i < num_files; i++) {
        // Read key length and key
        uint32_t key_len = 0;
        if (!ReadExact(client_fd, &key_len, sizeof(key_len))) {
            return false;
        }

        if (key_len == 0 || key_len > 1024) {
            LOG(ERROR) << "[SnapshotDaemon] Invalid key_len: " << key_len;
            error_msg = "Invalid key length";
            all_success = false;
            break;
        }

        std::string key(key_len, '\0');
        if (!ReadExact(client_fd, key.data(), key_len)) {
            return false;
        }

        uint8_t payload_type = 0;
        if (!ReadExact(client_fd, &payload_type, sizeof(payload_type))) {
            return false;
        }

        uint32_t payload_len = 0;
        if (!ReadExact(client_fd, &payload_len, sizeof(payload_len))) {
            return false;
        }

        std::vector<uint8_t> data;
        if (payload_type == 0) {
            if (payload_len > kMaxInlineMessageSize) {
                LOG(ERROR) << "[SnapshotDaemon] Inline data too large: "
                           << payload_len;
                error_msg = "Inline data too large";
                all_success = false;
                break;
            }
            data.resize(payload_len);
            if (payload_len > 0 &&
                !ReadExact(client_fd, data.data(), payload_len)) {
                return false;
            }
        } else if (payload_type == 1) {
            if (payload_len == 0 || payload_len > kMaxPathSize) {
                LOG(ERROR) << "[SnapshotDaemon] Invalid path length: "
                           << payload_len;
                error_msg = "Invalid file path length";
                all_success = false;
                break;
            }
            std::string file_path(payload_len, '\0');
            if (!ReadExact(client_fd, file_path.data(), payload_len)) {
                return false;
            }

            std::error_code ec;
            auto file_size = std::filesystem::file_size(file_path, ec);
            if (ec) {
                LOG(ERROR) << "[SnapshotDaemon] Failed to stat file: "
                           << file_path << ", error=" << ec.message();
                error_msg = "Failed to stat file: " + file_path;
                all_success = false;
                break;
            }
            if (file_size == 0 ||
                file_size > kEtcdMaxValueSize ||
                file_size > std::numeric_limits<size_t>::max() ||
                file_size > static_cast<uintmax_t>(std::numeric_limits<int>::max())) {
                LOG(ERROR) << "[SnapshotDaemon] File size invalid: "
                           << file_path << ", size=" << file_size;
                error_msg = "File size invalid: " + file_path;
                all_success = false;
                break;
            }

            std::ifstream file(file_path, std::ios::binary);
            if (!file.is_open()) {
                LOG(ERROR) << "[SnapshotDaemon] Failed to open file: "
                           << file_path;
                error_msg = "Failed to open file: " + file_path;
                all_success = false;
                break;
            }

            data.resize(static_cast<size_t>(file_size));
            file.read(reinterpret_cast<char*>(data.data()),
                      static_cast<std::streamsize>(data.size()));
            if (!file) {
                LOG(ERROR) << "[SnapshotDaemon] Failed to read file: "
                           << file_path;
                error_msg = "Failed to read file: " + file_path;
                all_success = false;
                break;
            }
        } else {
            LOG(ERROR) << "[SnapshotDaemon] Unknown payload type: "
                       << static_cast<int>(payload_type);
            error_msg = "Unknown payload type";
            all_success = false;
            break;
        }

        LOG(INFO) << "[SnapshotDaemon] Uploading " << i << "/" << num_files
                  << ": key=" << key << ", size=" << data.size();

        char* err = nullptr;
        int ret = SnapshotStorePutWrapper(
            const_cast<char*>(key.c_str()), static_cast<int>(key.size()),
            const_cast<char*>(reinterpret_cast<const char*>(data.data())),
            static_cast<int>(data.size()), &err);

        if (ret != 0) {
            std::string error = err ? err : "unknown error";
            if (err) free(err);
            error_msg += "Failed to upload " + key + ": " + error + "; ";
            all_success = false;
            LOG(ERROR) << "[SnapshotDaemon] Upload failed: " << key
                       << ", error: " << error;
            break;  // Stop on first error
        }

        LOG(INFO) << "[SnapshotDaemon] Upload successful: " << key;
    }

    auto upload_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - upload_start)
                           .count();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - request_start)
                          .count();

    LOG(INFO) << "[SnapshotDaemon] Batch upload completed, success="
              << all_success << ", upload_time=" << upload_time
              << "ms, total_time=" << total_time << "ms";

    // Send response
    uint32_t status = all_success ? 0 : 1;
    uint32_t error_len = error_msg.size();

    if (!WriteExact(client_fd, &status, sizeof(status))) {
        return false;
    }
    if (!WriteExact(client_fd, &error_len, sizeof(error_len))) {
        return false;
    }
    if (error_len > 0 && !WriteExact(client_fd, error_msg.data(), error_len)) {
        return false;
    }

    return all_success;
#else
    LOG(ERROR) << "[SnapshotDaemon] ETCD support not enabled";
    uint32_t status = 1;
    std::string error = "ETCD not enabled";
    uint32_t error_len = error.size();
    WriteExact(client_fd, &status, sizeof(status));
    WriteExact(client_fd, &error_len, sizeof(error_len));
    WriteExact(client_fd, error.data(), error_len);
    return false;
#endif
}

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    auto start_time = std::chrono::steady_clock::now();

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <etcd_endpoints> <socket_path>"
                  << std::endl;
        return 1;
    }

    std::string etcd_endpoints = argv[1];
    std::string socket_path = argv[2];

    LOG(INFO) << "[SnapshotDaemon] ===== STARTING DAEMON =====";
    LOG(INFO) << "[SnapshotDaemon] etcd_endpoints=" << etcd_endpoints
              << ", socket_path=" << socket_path;

    // Setup signal handlers
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);

#ifdef STORE_USE_ETCD
    // Initialize etcd client ONCE
    auto init_start = std::chrono::steady_clock::now();
    LOG(INFO) << "[SnapshotDaemon] Initializing etcd client (ONE TIME)";

    char* err_msg = nullptr;
    int ret = NewSnapshotEtcdClient(const_cast<char*>(etcd_endpoints.c_str()),
                                    &err_msg);
    if (ret != 0) {
        std::string error = err_msg ? err_msg : "unknown error";
        if (err_msg) free(err_msg);
        LOG(ERROR) << "[SnapshotDaemon] Failed to initialize etcd client: "
                   << error;
        return 2;
    }

    auto init_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - init_start)
                         .count();
    LOG(INFO)
        << "[SnapshotDaemon] Etcd client initialized successfully, init_time="
        << init_time << "ms";
#else
    LOG(ERROR) << "[SnapshotDaemon] ETCD support not enabled";
    return 7;
#endif

    // Create Unix domain socket
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd == -1) {
        LOG(ERROR) << "[SnapshotDaemon] Failed to create socket: "
                   << strerror(errno);
        return 3;
    }

    // Remove existing socket file
    unlink(socket_path.c_str());

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        LOG(ERROR) << "[SnapshotDaemon] Failed to bind socket: "
                   << strerror(errno);
        close(server_fd);
        return 4;
    }

    if (listen(server_fd, 5) == -1) {
        LOG(ERROR) << "[SnapshotDaemon] Failed to listen: " << strerror(errno);
        close(server_fd);
        unlink(socket_path.c_str());
        return 5;
    }

    auto ready_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start_time)
                          .count();
    LOG(INFO) << "[SnapshotDaemon] ===== READY TO ACCEPT REQUESTS =====";
    LOG(INFO) << "[SnapshotDaemon] Socket listening on " << socket_path
              << ", startup_time=" << ready_time << "ms";

    // Main loop
    int request_count = 0;
    while (!should_exit) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(server_fd, &readfds);

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int ready = select(server_fd + 1, &readfds, nullptr, nullptr, &timeout);
        if (ready == -1) {
            if (errno == EINTR) {
                continue;  // Signal interrupted, check should_exit
            }
            LOG(ERROR) << "[SnapshotDaemon] Select error: " << strerror(errno);
            break;
        }

        if (ready == 0) {
            // Timeout, check should_exit
            continue;
        }

        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd == -1) {
            LOG(ERROR) << "[SnapshotDaemon] Accept error: " << strerror(errno);
            continue;
        }

        request_count++;
        LOG(INFO) << "[SnapshotDaemon] Accepted connection, request_count="
                  << request_count;

        bool success = ProcessRequest(client_fd);
        close(client_fd);

        LOG(INFO) << "[SnapshotDaemon] Request completed, success=" << success;
    }

    // Cleanup
    close(server_fd);
    unlink(socket_path.c_str());

    LOG(INFO) << "[SnapshotDaemon] ===== SHUTDOWN COMPLETE =====";
    LOG(INFO) << "[SnapshotDaemon] Total requests processed: " << request_count;

    return 0;
}
