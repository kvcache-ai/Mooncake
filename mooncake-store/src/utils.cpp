#include "utils.h"
#include "mmap_arena.h"
#include "config.h"
#include "common.h"

#include <Slab.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <atomic>
#include <memory>
#include <random>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <sys/mman.h>
#include <numa.h>
#include <numaif.h>
#include <mutex>

// Feature flag to enable/disable arena allocator. Disabled by default so the
// library does not pre-map a large pool unless the operator opts in via gflag
// or an explicit MC_MMAP_ARENA_POOL_SIZE environment override.
DEFINE_bool(use_mmap_arena_allocator, false,
            "Enable the lock-free mmap arena allocator for mmap buffers");

// Arena pool size (default 8GiB when explicitly enabled without an env
// override).
DEFINE_uint64(mmap_arena_pool_size, 8ULL * 1024 * 1024 * 1024,
              "Arena allocator pool size in bytes");
#ifdef USE_ASCEND_DIRECT
#include "acl/acl.h"
#endif
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

// Global arena instance (lazy initialization)
static std::unique_ptr<MmapArena> g_mmap_arena;
static std::once_flag g_arena_init_flag;
static std::atomic<uint64_t> g_arena_oom_fallback_count{0};
static std::atomic<uint64_t> g_arena_noop_free_count{0};

static void initializeGlobalArena() {
    const std::string env_pool_size =
        GetEnvStringOr("MC_MMAP_ARENA_POOL_SIZE", "");
    // Allow env var to override the gflag (useful when loaded as .so from
    // Python). An explicit pool-size env var is also treated as an opt-in,
    // because pybind11 users cannot easily pass gflags.
    const std::string env_disable = GetEnvStringOr("MC_DISABLE_MMAP_ARENA", "");
    const std::optional<bool> disable_override = string_to_bool(env_disable);
    if (!env_disable.empty() && !disable_override.has_value()) {
        LOG(WARNING) << "Ignoring invalid MC_DISABLE_MMAP_ARENA='"
                     << env_disable
                     << "'; accepted values: 1/0, true/false, yes/no, on/off";
    }
    const bool arena_requested =
        FLAGS_use_mmap_arena_allocator || !env_pool_size.empty();
    const bool arena_disabled = disable_override.value_or(false);
    if (!arena_requested || arena_disabled) {
        LOG(INFO) << "=== ARENA ALLOCATOR DISABLED ===";
        if (arena_disabled) {
            LOG(INFO) << "MC_DISABLE_MMAP_ARENA=" << env_disable
                      << " forces direct mmap()";
        } else {
            LOG(INFO) << "Arena is opt-in; set --use_mmap_arena_allocator or "
                         "MC_MMAP_ARENA_POOL_SIZE to enable it";
        }
        return;
    }

    g_mmap_arena = std::make_unique<MmapArena>();
    // Keep arena init consistent with the direct-mmap path:
    //   MC_STORE_USE_HUGEPAGE=1  -> strict: hard fail if hugepage mmap fails
    //   unset                    -> permissive: try hugepages, retry on
    //                               regular pages if HugeTLB is unavailable
    // This preserves both pre-existing contracts and avoids surprising
    // operators with a silent hugepage downgrade.
    const bool hugepages_explicitly_requested =
        get_hugepage_size_from_env() > 0;

    // Allow env var override since gflags cannot be set from Python (pybind11).
    // Supports human-readable sizes via string_to_byte_size(): "20gb", "16GB",
    // etc.
    uint64_t arena_pool_size = FLAGS_mmap_arena_pool_size;
    if (!env_pool_size.empty()) {
        const uint64_t parsed = string_to_byte_size(env_pool_size);
        if (parsed > 0) {
            arena_pool_size = parsed;
            LOG(INFO) << "MC_MMAP_ARENA_POOL_SIZE override: " << env_pool_size
                      << " (" << byte_size_to_string(arena_pool_size) << ")";
        } else {
            LOG(WARNING) << "Invalid MC_MMAP_ARENA_POOL_SIZE='" << env_pool_size
                         << "', using default "
                         << byte_size_to_string(FLAGS_mmap_arena_pool_size);
        }
    }

    bool success =
        g_mmap_arena->initialize(arena_pool_size, MmapArena::kMinAlignment,
                                 !hugepages_explicitly_requested);

    if (success) {
        auto stats = g_mmap_arena->getStats();
        LOG(INFO) << "=== ARENA ALLOCATOR ENABLED ===";
        LOG(INFO) << "Arena pool size: " << (stats.pool_size / BYTES_PER_GIB)
                  << " GiB";
        LOG(INFO) << "Using lock-free atomic bump allocation";
    } else {
        LOG(ERROR) << "=== ARENA INITIALIZATION FAILED ===";
        LOG(ERROR) << "Falling back to traditional mmap()";
        if (hugepages_explicitly_requested) {
            LOG(ERROR) << "MC_STORE_USE_HUGEPAGE is set, so the fallback path "
                          "will also require hugepages";
        }
        LOG(ERROR) << "Arena initialization is only attempted once per "
                      "process; restart after fixing the environment if you "
                      "want to retry arena bring-up";
        g_mmap_arena.reset();
    }
}

// Compute the mmap/munmap size for the fallback (non-arena) path.
// Used by both allocate_buffer_mmap_memory and free_buffer_mmap_memory
// so they agree on the mapping size.
static inline size_t mmap_map_size(size_t total_size, size_t hugepage_size) {
    const size_t page_size =
        hugepage_size > 0 ? hugepage_size : static_cast<size_t>(getpagesize());
    return align_up(total_size, page_size);
}

void *allocate_buffer_mmap_memory(size_t total_size, size_t alignment) {
    if (total_size == 0) {
        LOG(ERROR) << "Total size must be greater than 0 for mmap";
        return nullptr;
    }

    // Initialize arena on first call
    std::call_once(g_arena_init_flag, initializeGlobalArena);

    // Try arena allocation first (if enabled).
    // Forward caller's alignment so the arena honors the contract.
    if (g_mmap_arena && g_mmap_arena->isInitialized()) {
        void *ptr = g_mmap_arena->allocate(total_size, alignment);
        if (ptr != nullptr) {
            VLOG(1) << "Allocated " << total_size << " bytes from arena at "
                    << ptr;
            return ptr;
        }
        // Arena OOM, fall through to traditional mmap
        const uint64_t fallback_count =
            g_arena_oom_fallback_count.fetch_add(1, std::memory_order_relaxed) +
            1;
        LOG_FIRST_N(WARNING, 3)
            << "Arena OOM, falling back to mmap() for size=" << total_size
            << " (count=" << fallback_count << ")"
            << " (further warnings suppressed)";
    }

    // Traditional mmap allocation (fallback or arena disabled).
    unsigned int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
    const size_t hugepage_size = get_hugepage_size_from_env(&flags);
    const size_t map_size = mmap_map_size(total_size, hugepage_size);
    const size_t guaranteed_alignment =
        hugepage_size > 0 ? hugepage_size : static_cast<size_t>(getpagesize());
    if (alignment > guaranteed_alignment) {
        LOG_FIRST_N(WARNING, 3)
            << "Fallback mmap cannot honor alignment=" << alignment
            << " (guaranteed=" << guaranteed_alignment
            << "); pointer may be under-aligned"
            << " (further warnings suppressed)";
    }

    void *ptr = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "mmap failed, size=" << map_size << ", errno=" << errno
                   << " (" << strerror(errno) << ")";
        return nullptr;
    }

    VLOG(1) << "Allocated " << total_size << " bytes via mmap() at " << ptr;
    return ptr;
}

void free_buffer_mmap_memory(void *ptr, size_t total_size) {
    if (!ptr || total_size == 0) {
        return;
    }

    // Check if pointer belongs to global arena
    std::call_once(g_arena_init_flag, initializeGlobalArena);

    if (g_mmap_arena && g_mmap_arena->owns(ptr)) {
        const uint64_t noop_free_count =
            g_arena_noop_free_count.fetch_add(1, std::memory_order_relaxed) + 1;
        LOG_FIRST_N(WARNING, 3)
            << "free_buffer_mmap_memory() does not individually release "
               "arena-owned pointer "
            << ptr << "; the global arena releases its pool at process shutdown"
            << " (count=" << noop_free_count << ")"
            << " (further warnings suppressed)";
        return;
    }

    // Direct mmap allocation - safe to unmap
    const size_t map_size =
        mmap_map_size(total_size, get_hugepage_size_from_env());
    if (munmap(ptr, map_size) != 0) {
        LOG(ERROR) << "munmap hugepage failed, size=" << map_size
                   << ", errno=" << errno << " (" << strerror(errno) << ")";
    } else {
        VLOG(1) << "Freed direct mmap allocation at " << ptr
                << ", size=" << map_size;
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

std::vector<int> getFreeTcpPorts(int count) {
    std::vector<int> ports;
    std::vector<int> sockets;
    ports.reserve(count);
    sockets.reserve(count);

    for (int i = 0; i < count; ++i) {
        int sock = ::socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) break;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(0);
        if (::bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) !=
            0) {
            ::close(sock);
            break;
        }
        socklen_t len = sizeof(addr);
        if (::getsockname(sock, reinterpret_cast<sockaddr *>(&addr), &len) !=
            0) {
            ::close(sock);
            break;
        }
        ports.push_back(ntohs(addr.sin_port));
        sockets.push_back(sock);
    }

    for (int sock : sockets) {
        ::close(sock);
    }
    return ports;
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
