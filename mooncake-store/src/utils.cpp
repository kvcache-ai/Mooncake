#include "utils.h"

#include <Slab.h>
#include <glog/logging.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <random>

namespace mooncake {

bool isPortAvailable(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    bool available = (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    close(sock);
    return available;
}

// AutoPortBinder implementation
AutoPortBinder::AutoPortBinder(int min_port, int max_port)
    : socket_fd_(-1), port_(-1) {
    static std::random_device rand_gen;
    std::mt19937 gen(rand_gen());
    std::uniform_int_distribution<> rand_dist(min_port, max_port);

    for (int attempt = 0; attempt < 20; ++attempt) {
        int port = rand_dist(gen);

        socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd_ < 0) continue;

        int opt = 1;
        setsockopt(socket_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (bind(socket_fd_, (sockaddr *)&addr, sizeof(addr)) == 0) {
            port_ = port;
            break;
        } else {
            close(socket_fd_);
            socket_fd_ = -1;
        }
    }
}

AutoPortBinder::~AutoPortBinder() {
    if (socket_fd_ >= 0) {
        close(socket_fd_);
    }
}

void *allocate_buffer_allocator_memory(size_t total_size) {
    const size_t alignment = facebook::cachelib::Slab::kSize;
    // Ensure total_size is a multiple of alignment
    if (total_size < alignment) {
        LOG(ERROR) << "Total size must be at least " << alignment;
        return nullptr;
    }
    // Allocate aligned memory
    return aligned_alloc(alignment, total_size);
}

std::string formatDeviceNames(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

static std::string loadNicPriorityMatrix(const std::string &device_name) {
    auto device_names = formatDeviceNames(device_name);
    return "{\"cpu:0\": [[" + device_names + "], []]}";
}

void **rdma_args(const std::string &device_name) {
    static auto nic_priority_matrix = loadNicPriorityMatrix(device_name);
    void **args = (void **)malloc(2 * sizeof(void *));
    args[0] = (void *)nic_priority_matrix.c_str();
    args[1] = nullptr;
    return args;
}

}  // namespace mooncake