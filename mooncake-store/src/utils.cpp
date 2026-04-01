#include "utils.h"

#include <Slab.h>
#include <glog/logging.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <memory>
#include <random>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <sys/mman.h>
#include <numa.h>
#include <numaif.h>
#include "config.h"
#include "common.h"
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include "ascend_allocator.h"
#endif

#include <ylt/coro_http/coro_http_client.hpp>

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

void *allocate_buffer_allocator_memory(size_t total_size,
                                       const std::string &protocol,
                                       size_t alignment) {
    const size_t default_alignment = facebook::cachelib::Slab::kSize;
    // Ensure total_size is a multiple of alignment
    if (alignment == default_alignment && total_size < alignment) {
        LOG(ERROR) << "Total size must be at least " << alignment;
        return nullptr;
    }
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    if (protocol == "ascend" || protocol == "ubshmem") {
        return ascend_allocate_memory(total_size, protocol);
    }
#endif

    // Allocate aligned memory
    return aligned_alloc(alignment, total_size);
}

void *allocate_buffer_mmap_memory(size_t total_size, size_t alignment) {
    if (total_size == 0) {
        LOG(ERROR) << "Total size must be greater than 0 for hugepage mmap";
        return nullptr;
    }

    unsigned int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
    const size_t effective_alignment =
        std::max(alignment, get_hugepage_size_from_env(&flags));
    const size_t map_size = align_up(total_size, effective_alignment);

    void *ptr = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "Hugepage mmap failed, size=" << map_size
                   << ", errno=" << errno << " (" << strerror(errno) << ")";
        return nullptr;
    }

    return ptr;
}

void free_buffer_mmap_memory(void *ptr, size_t total_size) {
    if (!ptr || total_size == 0) {
        return;
    }

    const size_t map_size = align_up(total_size, get_hugepage_size_from_env());
    if (munmap(ptr, map_size) != 0) {
        LOG(ERROR) << "munmap hugepage failed, size=" << map_size
                   << ", errno=" << errno << " (" << strerror(errno) << ")";
    }
}

// NUMA-segmented buffer allocation
void *allocate_buffer_numa_segments(size_t total_size,
                                    const std::vector<int> &numa_nodes,
                                    size_t page_size) {
    if (total_size == 0 || numa_nodes.empty()) {
        LOG(ERROR) << "Invalid params: total_size=" << total_size
                   << " numa_nodes.size=" << numa_nodes.size();
        return nullptr;
    }

    if (page_size == 0) page_size = getpagesize();
    size_t n = numa_nodes.size();
    size_t region_size = align_up(total_size / n, page_size);
    size_t map_size = region_size * n;

    // reserve contiguous VMA, no physical pages yet
    void *ptr = mmap(nullptr, map_size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "mmap failed, size=" << map_size << ", errno=" << errno
                   << " (" << strerror(errno) << ")";
        return nullptr;
    }

    // bind each region to its NUMA node
    int max_node = numa_num_possible_nodes();
    for (size_t i = 0; i < n; ++i) {
        struct bitmask *mask = numa_bitmask_alloc(max_node);
        numa_bitmask_setbit(mask, numa_nodes[i]);
        char *region = static_cast<char *>(ptr) + i * region_size;
        long rc =
            mbind(region, region_size, MPOL_BIND, mask->maskp, mask->size, 0);
        numa_bitmask_free(mask);
        if (rc != 0) {
            LOG(ERROR) << "mbind failed for NUMA " << numa_nodes[i]
                       << ", errno=" << errno << " (" << strerror(errno) << ")";
            munmap(ptr, map_size);
            return nullptr;
        }
    }

    // No explicit prefault needed — ibv_reg_mr() will call get_user_pages()
    // which triggers page faults that respect the mbind NUMA policy.
    // Pages are allocated directly on the target NUMA during MR registration,
    // avoiding a redundant full-buffer traversal.

    LOG(INFO) << "Allocated NUMA-segmented buffer: " << map_size << " bytes, "
              << n << " regions, page_size=" << page_size << ", nodes=[" <<
        [&]() {
            std::string s;
            for (size_t i = 0; i < n; ++i) {
                if (i) s += ",";
                s += std::to_string(numa_nodes[i]);
            }
            return s;
        }() << "]";
    return ptr;
}

void free_memory(const std::string &protocol, void *ptr) {
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    if (protocol == "ascend" || protocol == "ubshmem") {
        return ascend_free_memory(protocol, ptr);
    }
#endif

    free(ptr);
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

std::vector<std::string> splitString(const std::string &str, char delimiter,
                                     bool trim_spaces, bool keep_empty) {
    std::vector<std::string> result;

    boost::split(
        result, str, boost::is_any_of(std::string(1, delimiter)),
        keep_empty ? boost::token_compress_off : boost::token_compress_on);

    if (trim_spaces) {
        for (auto &token : result) {
            boost::trim(token);
        }
    }

    return result;
}

tl::expected<std::string, int> httpGet(const std::string &url) {
    coro_http::coro_http_client client;
    auto res = client.get(url);
    if (res.status == 200) {
        return std::string(res.resp_body);
    }
    return tl::unexpected(res.status);
}

tl::expected<std::string, std::string> GetInterfaceIPv4Address(
    const std::string &interface_name) {
    if (interface_name.empty()) {
        return tl::unexpected(std::string("network interface name is empty"));
    }

    struct ifaddrs *interfaces = nullptr;
    if (getifaddrs(&interfaces) != 0) {
        return tl::unexpected("getifaddrs failed: " +
                              std::string(strerror(errno)));
    }
    std::unique_ptr<struct ifaddrs, decltype(&freeifaddrs)> interface_guard(
        interfaces, freeifaddrs);

    bool found_interface = false;
    bool interface_is_up = false;
    for (auto *current = interfaces; current != nullptr;
         current = current->ifa_next) {
        if (current->ifa_name == nullptr ||
            interface_name != current->ifa_name) {
            continue;
        }

        found_interface = true;
        if ((current->ifa_flags & IFF_UP) == 0) {
            continue;
        }
        interface_is_up = true;

        if (current->ifa_addr == nullptr ||
            current->ifa_addr->sa_family != AF_INET) {
            continue;
        }

        char host[NI_MAXHOST] = {};
        auto *ipv4_addr = reinterpret_cast<sockaddr_in *>(current->ifa_addr);
        if (getnameinfo(reinterpret_cast<sockaddr *>(ipv4_addr),
                        sizeof(*ipv4_addr), host, sizeof(host), nullptr, 0,
                        NI_NUMERICHOST) == 0) {
            return std::string(host);
        }
    }

    if (!found_interface) {
        return tl::unexpected("network interface '" + interface_name +
                              "' was not found");
    }
    if (!interface_is_up) {
        return tl::unexpected("network interface '" + interface_name +
                              "' is down");
    }
    return tl::unexpected("network interface '" + interface_name +
                          "' has no IPv4 address");
}

int getFreeTcpPort() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);
    if (::bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
        ::close(sock);
        return -1;
    }
    int port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
}

int64_t time_gen() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string GetEnvStringOr(const char *name, const std::string &default_value) {
    const char *env_val = std::getenv(name);
    return env_val ? std::string(env_val) : default_value;
}

static std::string SanitizeKey(const std::string &key) {
    // Set of invalid filesystem characters to be replaced
    constexpr std::string_view kInvalidChars = "/\\:*?\"<>|";
    std::string sanitized_key;
    sanitized_key.reserve(key.size());

    for (char c : key) {
        // Replace invalid characters with underscore
        sanitized_key.push_back(
            kInvalidChars.find(c) != std::string_view::npos ? '_' : c);
    }
    return sanitized_key;
}

std::string ResolvePathFromKey(const std::string &key,
                               const std::string &root_dir,
                               const std::string &fsdir) {
    // Compute hash of the key
    size_t hash = std::hash<std::string>{}(key);

    // Use low 8 bits to create 2-level directory structure (e.g. "a1/b2")
    char dir1 =
        static_cast<char>('a' + (hash & 0x0F));  // Lower 4 bits -> 16 dirs
    char dir2 = static_cast<char>(
        'a' + ((hash >> 4) & 0x0F));  // Next 4 bits -> 16 subdirs

    // Safely construct path using std::filesystem
    namespace fs = std::filesystem;
    fs::path dir_path = fs::path(std::string(1, dir1)) / std::string(1, dir2);

    // Combine directory path with sanitized filename
    fs::path full_path =
        fs::path(root_dir) / fsdir / dir_path / SanitizeKey(key);

    return full_path.lexically_normal().string();
}
}  // namespace mooncake
