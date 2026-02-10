#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <numa.h>
#include <pthread.h>
#include <signal.h>
#include <thread>
#include <stop_token>

#include <cstdlib>  // for atexit
#include <algorithm>
#include <cctype>
#include <optional>
#include <vector>

#include "real_client.h"
#include "client_buffer.hpp"
#include "config.h"
#include "mutex.h"
#include "types.h"
#include "utils.h"
#include "rpc_types.h"
#include "file_storage.h"
#include "default_config.h"

namespace mooncake {

PyClient::~PyClient() {}

// ResourceTracker implementation using singleton pattern
// Use a deliberately leaked heap object to avoid static destruction
// order issues with atexit/signal handlers during process teardown.
ResourceTracker &ResourceTracker::getInstance() {
    static ResourceTracker *instance = new ResourceTracker();
    return *instance;
}

ResourceTracker::ResourceTracker() {
    // Start dedicated signal handling thread (best-effort)
    startSignalThread();
    // Register exit handler as a backstop
    std::atexit(exitHandler);
}

ResourceTracker::~ResourceTracker() {
    // Cleanup is handled by exitHandler
}

void ResourceTracker::registerInstance(
    const std::shared_ptr<PyClient> &instance) {
    MutexLocker locker(&mutex_);
    instances_.push_back(instance);
}

void ResourceTracker::cleanupAllResources() {
    // Ensure this runs only once
    bool expected = false;
    if (!cleaned_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
        return;
    }

    MutexLocker locker(&mutex_);

    for (auto &wp : instances_) {
        if (auto sp = wp.lock()) {
            LOG(INFO) << "Cleaning up DistributedObjectStore instance";
            sp->tearDownAll();
        }
    }
}

void ResourceTracker::signalHandler(int signal) {
    // Legacy path (kept for compatibility if handlers are ever installed).
    // Prefer dedicated signal thread; avoid doing heavy work here.
    getInstance().cleanupAllResources();
    struct sigaction sa;
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(signal, &sa, nullptr);
    raise(signal);
}

void ResourceTracker::exitHandler() { getInstance().cleanupAllResources(); }

void ResourceTracker::startSignalThread() {
    std::call_once(signal_once_, [this]() {
        // Wait signal thread start
        std::promise<void> ready;
        auto ready_future = ready.get_future();

        // Block signals in this thread; new threads inherit this mask
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGINT);
        sigaddset(&set, SIGTERM);
        sigaddset(&set, SIGHUP);
        sigaddset(&set, SIGUSR1);  // used to interrupt sigwait on stop
        pthread_sigmask(SIG_BLOCK, &set, nullptr);

        signal_thread_ = std::jthread(
            [set, ready = std::move(ready)](std::stop_token st) mutable {
                // Register a stop callback to interrupt sigwait via SIGUSR1
                pthread_t self = pthread_self();
                std::stop_callback cb(
                    st, [self]() { pthread_kill(self, SIGUSR1); });
                ready.set_value();
                for (;;) {
                    int sig = 0;
                    int rc = sigwait(&set, &sig);
                    if (rc != 0) {
                        LOG(ERROR) << "sigwait failed: " << strerror(rc);
                        continue;
                    }

                    if (sig == SIGUSR1) {
                        if (st.stop_requested()) {
                            break;  // graceful stop
                        }
                        continue;  // spurious
                    }

                    // Perform cleanup in normal thread context
                    LOG(INFO) << "Received signal " << sig
                              << ", cleaning up resources";
                    ResourceTracker::getInstance().cleanupAllResources();

                    // Restore default action and re-raise to terminate normally
                    struct sigaction sa;
                    sa.sa_handler = SIG_DFL;
                    sigemptyset(&sa.sa_mask);
                    sa.sa_flags = 0;
                    sigaction(sig, &sa, nullptr);
                    raise(sig);

                    break;  // Should not reach due to process termination
                }
            });
        ready_future.wait();  // Ensure thread is ready
    });
}

RealClient::RealClient() {
    // Initialize logging severity (leave as before)
    mooncake::init_ylt_log_level();
    const char *hp = std::getenv("MC_STORE_USE_HUGEPAGE");
    use_hugepage_ = (hp != nullptr);
}

RealClient::~RealClient() {
    // Ensure resources are cleaned even if not explicitly closed
    tearDownAll_internal();
}

std::shared_ptr<RealClient> RealClient::create() {
    auto sp = std::shared_ptr<RealClient>(new RealClient());
    ResourceTracker::getInstance().registerInstance(sp);
    return sp;
}

tl::expected<void, ErrorCode> RealClient::setup_internal(
    const std::string &local_hostname, const std::string &metadata_server,
    size_t global_segment_size, size_t local_buffer_size,
    const std::string &protocol, const std::string &rdma_devices,
    const std::string &master_server_addr,
    const std::shared_ptr<TransferEngine> &transfer_engine,
    const std::string &ipc_socket_path, int local_rpc_port,
    bool enable_offload) {
    this->protocol = protocol;
    this->ipc_socket_path_ = ipc_socket_path;
    const bool should_use_hugepage =
        use_hugepage_ && this->protocol != "ascend";

    std::optional<std::string> device_name =
        (rdma_devices.empty() ? std::nullopt
                              : std::make_optional(rdma_devices));

    // Check if hostname already contains a port
    std::string hostname = local_hostname;
    size_t colon_pos = hostname.find(":");
    bool user_specified_port = (colon_pos != std::string::npos);

    if (user_specified_port) {
        // User specified port, no retry needed
        this->local_hostname = local_hostname;
        this->local_rpc_addr =
            hostname.substr(0, colon_pos + 1) + std::to_string(local_rpc_port);
        auto client_opt = mooncake::Client::Create(
            this->local_hostname, metadata_server, protocol, device_name,
            master_server_addr, transfer_engine);
        if (!client_opt) {
            LOG(ERROR) << "Failed to create client";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        client_ = *client_opt;
    } else {
        // Auto port binding with retry on metadata registration failure
        const int kMaxRetries =
            GetEnvOr<int>("MC_STORE_CLIENT_SETUP_RETRIES", 20);
        bool success = false;

        for (int retry = 0; retry < kMaxRetries; ++retry) {
            // Create port binder to hold a port
            port_binder_ = std::make_unique<AutoPortBinder>();
            int port = port_binder_->getPort();
            if (port < 0) {
                LOG(WARNING) << "Failed to bind available port, retry "
                             << (retry + 1) << "/" << kMaxRetries;
                port_binder_.reset();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            this->local_hostname = hostname + ":" + std::to_string(port);
            this->local_rpc_addr =
                hostname + ":" + std::to_string(local_rpc_port);
            auto client_opt = mooncake::Client::Create(
                this->local_hostname, metadata_server, protocol, device_name,
                master_server_addr, transfer_engine);
            if (client_opt) {
                client_ = *client_opt;
                success = true;
                LOG(INFO) << "Successfully created client on port " << port
                          << " after " << (retry + 1) << " attempt(s)";
                break;
            }

            // Failed to create client (possibly due to metadata registration
            // conflict), release port and retry with a different port
            LOG(WARNING) << "Failed to create client on port " << port
                         << ", retry " << (retry + 1) << "/" << kMaxRetries;
            port_binder_.reset();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!success) {
            LOG(ERROR) << "Failed to create client after " << kMaxRetries
                       << " retries";
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    // Local_buffer_size is allowed to be 0, but we only register memory when
    // local_buffer_size > 0. Invoke ibv_reg_mr() with size=0 is UB, and may
    // fail in some rdma implementations.
    // Dummy Client can create shm and share it with Real Client, so Real Client
    // can create client buffer allocator on the shared memory later.
    client_buffer_allocator_ = ClientBufferAllocator::create(
        local_buffer_size, this->protocol, should_use_hugepage);
    if (local_buffer_size > 0) {
        LOG(INFO) << "Registering local memory: " << local_buffer_size
                  << " bytes";
        auto result = client_->RegisterLocalMemory(
            client_buffer_allocator_->getBase(), local_buffer_size,
            kWildcardLocation, false, true);
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to register local memory: "
                       << toString(result.error());
            return tl::unexpected(result.error());
        }
    } else {
        LOG(INFO) << "Local buffer size is 0, skip registering local memory";
    }

    // If global_segment_size is 0, skip mount segment;
    // If global_segment_size is larger than max_mr_size, split to multiple
    // mapped_shms.
    if (protocol == "cxl") {
        size_t cxl_dev_size = 0;
        const char *env = std::getenv("MC_CXL_DEV_SIZE");
        if (env) {
            char *end = nullptr;
            unsigned long long val = strtoull(env, &end, 10);
            if (end != env && *end == '\0')
                cxl_dev_size = static_cast<size_t>(val);
        } else {
            LOG(FATAL) << "MC_CXL_DEV_SIZE not set";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        void *ptr = client_->GetBaseAddr();
        LOG(INFO) << "Mounting CXL segment: " << cxl_dev_size << " bytes, "
                  << ptr;
        auto mount_result = client_->MountSegment(ptr, cxl_dev_size, protocol);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount segment: "
                       << toString(mount_result.error());
            return tl::unexpected(mount_result.error());
        }

    } else {
        auto max_mr_size = globalConfig().max_mr_size;     // Max segment size
        uint64_t total_glbseg_size = global_segment_size;  // For logging
        uint64_t current_glbseg_size = 0;                  // For logging
        while (global_segment_size > 0) {
            size_t segment_size = std::min(global_segment_size, max_mr_size);
            global_segment_size -= segment_size;
            current_glbseg_size += segment_size;
            LOG(INFO) << "Mounting segment: " << segment_size << " bytes, "
                      << current_glbseg_size << " of " << total_glbseg_size;

            size_t mapped_size = segment_size;
            void *ptr = nullptr;
            if (should_use_hugepage) {
                mapped_size =
                    align_up(segment_size, get_hugepage_size_from_env());
                ptr = allocate_buffer_mmap_memory(mapped_size,
                                                  get_hugepage_size_from_env());
            } else {
                ptr = allocate_buffer_allocator_memory(segment_size,
                                                       this->protocol);
            }

            if (!ptr) {
                LOG(ERROR) << "Failed to allocate segment memory";
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (this->protocol == "ascend") {
                ascend_segment_ptrs_.emplace_back(ptr);
            } else if (should_use_hugepage) {
                hugepage_segment_ptrs_.emplace_back(
                    ptr, HugepageSegmentDeleter{mapped_size});
            } else {
                segment_ptrs_.emplace_back(ptr);
            }
            auto mount_result =
                client_->MountSegment(ptr, mapped_size, protocol);
            if (!mount_result.has_value()) {
                LOG(ERROR) << "Failed to mount segment: "
                           << toString(mount_result.error());
                return tl::unexpected(mount_result.error());
            }
        }
        if (total_glbseg_size == 0) {
            LOG(INFO) << "Global segment size is 0, skip mounting segment";
        }
    }

    // Start IPC server to accept FD from dummy clients
    if (!ipc_socket_path_.empty()) {
        if (start_ipc_server() != 0) {
            LOG(ERROR) << "Failed to start IPC server at " << ipc_socket_path_;
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        LOG(INFO) << "Starting IPC server at " << ipc_socket_path_;
    }
    if (enable_offload) {
        auto file_storage_config = FileStorageConfig::FromEnvironment();
        file_storage_ = std::make_shared<FileStorage>(
            file_storage_config, client_, this->local_rpc_addr);
        auto init_result = file_storage_->Init();
        if (!init_result) {
            LOG(ERROR) << "file storage init failed with error: "
                       << init_result.error();
            return init_result;
        }
    }
    client_requester_ = std::make_shared<ClientRequester>();
    return {};
}

int RealClient::setup_real(
    const std::string &local_hostname, const std::string &metadata_server,
    size_t global_segment_size, size_t local_buffer_size,
    const std::string &protocol, const std::string &rdma_devices,
    const std::string &master_server_addr,
    const std::shared_ptr<TransferEngine> &transfer_engine,
    const std::string &ipc_socket_path) {
    return to_py_ret(setup_internal(local_hostname, metadata_server,
                                    global_segment_size, local_buffer_size,
                                    protocol, rdma_devices, master_server_addr,
                                    transfer_engine, ipc_socket_path));
}

namespace {
// Helper to get value from config dict with default
inline std::string get_config(const ConfigDict &config, const std::string &key,
                              const std::string &default_value = "") {
    auto it = config.find(key);
    return (it != config.end()) ? it->second : default_value;
}

inline size_t get_config_size(const ConfigDict &config, const std::string &key,
                              size_t default_value) {
    auto it = config.find(key);
    if (it == config.end()) {
        return default_value;
    }
    const std::string &value = it->second;
    // Check for negative numbers (stoull incorrectly parses "-1" as large val)
    if (!value.empty() && value[0] == '-') {
        LOG(WARNING) << "Invalid negative value for config key '" << key
                     << "': " << value << ", using default: " << default_value;
        return default_value;
    }
    try {
        return std::stoull(value);
    } catch (const std::invalid_argument &e) {
        LOG(WARNING) << "Invalid non-numeric value for config key '" << key
                     << "': " << value << ", using default: " << default_value;
        return default_value;
    } catch (const std::out_of_range &e) {
        LOG(WARNING) << "Value out of range for config key '" << key
                     << "': " << value << ", using default: " << default_value;
        return default_value;
    }
}
}  // namespace

tl::expected<void, ErrorCode> RealClient::setup_internal(
    const ConfigDict &config) {
    // Extract required parameters (no defaults)
    std::string local_hostname = get_config(config, CONFIG_KEY_LOCAL_HOSTNAME);
    std::string metadata_server =
        get_config(config, CONFIG_KEY_METADATA_SERVER);

    // Validate required parameters
    if (local_hostname.empty()) {
        LOG(ERROR) << "Missing required config: " << CONFIG_KEY_LOCAL_HOSTNAME;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (metadata_server.empty()) {
        LOG(ERROR) << "Missing required config: " << CONFIG_KEY_METADATA_SERVER;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Extract optional parameters with defaults
    size_t global_segment_size = get_config_size(
        config, CONFIG_KEY_GLOBAL_SEGMENT_SIZE, DEFAULT_GLOBAL_SEGMENT_SIZE);
    size_t local_buffer_size = get_config_size(
        config, CONFIG_KEY_LOCAL_BUFFER_SIZE, DEFAULT_LOCAL_BUFFER_SIZE);
    std::string protocol =
        get_config(config, CONFIG_KEY_PROTOCOL, DEFAULT_PROTOCOL);
    std::string rdma_devices = get_config(config, CONFIG_KEY_RDMA_DEVICES);
    std::string master_server_addr = get_config(
        config, CONFIG_KEY_MASTER_SERVER_ADDR, DEFAULT_MASTER_SERVER_ADDR);
    std::string ipc_socket_path =
        get_config(config, CONFIG_KEY_IPC_SOCKET_PATH);

    // Validate size parameters are within acceptable ranges
    if (global_segment_size < MIN_SEGMENT_SIZE ||
        global_segment_size > MAX_SEGMENT_SIZE) {
        LOG(ERROR) << "Invalid " << CONFIG_KEY_GLOBAL_SEGMENT_SIZE << ": "
                   << global_segment_size << ", must be between "
                   << MIN_SEGMENT_SIZE << " and " << MAX_SEGMENT_SIZE;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (local_buffer_size < MIN_SEGMENT_SIZE ||
        local_buffer_size > MAX_SEGMENT_SIZE) {
        LOG(ERROR) << "Invalid " << CONFIG_KEY_LOCAL_BUFFER_SIZE << ": "
                   << local_buffer_size << ", must be between "
                   << MIN_SEGMENT_SIZE << " and " << MAX_SEGMENT_SIZE;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate protocol is supported
    if (protocol != "tcp" && protocol != "rdma") {
        LOG(ERROR) << "Invalid " << CONFIG_KEY_PROTOCOL << ": " << protocol
                   << ", must be 'tcp' or 'rdma'";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    return setup_internal(local_hostname, metadata_server, global_segment_size,
                          local_buffer_size, protocol, rdma_devices,
                          master_server_addr, nullptr, ipc_socket_path);
}

tl::expected<void, ErrorCode> RealClient::initAll_internal(
    const std::string &protocol_, const std::string &device_name,
    size_t mount_segment_size) {
    if (client_) {
        LOG(ERROR) << "Client is already initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t buffer_allocator_size = 1024 * 1024 * 1024;
    return setup_internal("localhost:12345", "127.0.0.1:2379",
                          mount_segment_size, buffer_allocator_size, protocol_,
                          device_name);
}

int RealClient::initAll(const std::string &protocol_,
                        const std::string &device_name,
                        size_t mount_segment_size) {
    return to_py_ret(
        initAll_internal(protocol_, device_name, mount_segment_size));
}

tl::expected<void, ErrorCode> RealClient::tearDownAll_internal() {
    // Ensure cleanup executes once across destructor/close/signal paths
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
        return {};
    }

    stop_ipc_server();

    if (!client_) {
        // Not initialized or already cleaned; treat as success for idempotence
        return {};
    }
    if (client_buffer_allocator_ && client_buffer_allocator_->size() > 0) {
        auto unregister_result = client_->unregisterLocalMemory(
            client_buffer_allocator_->getBase(), true);
        if (!unregister_result) {
            LOG(WARNING)
                << "Failed to unregister client local buffer on tear down: "
                << toString(unregister_result.error());
        }
    }
    // Reset all resources
    client_.reset();
    client_buffer_allocator_.reset();
    port_binder_.reset();
    hugepage_segment_ptrs_.clear();
    segment_ptrs_.clear();
    local_hostname = "";
    device_name = "";
    protocol = "";
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto shm_it = shm_contexts_.begin();
    while (shm_it != shm_contexts_.end()) {
        auto &context = shm_it->second;
        context.client_buffer_allocator.reset();

        // Iterate over all mapped_shms to unmap them
        for (auto &seg : context.mapped_shms) {
            if (seg.shm_buffer) {
                // Memory mapped from memfd needs munmap
                if (munmap(seg.shm_buffer, seg.shm_size) != 0) {
                    LOG(ERROR) << "Failed to unmap shm: " << seg.shm_name
                               << ", error: " << strerror(errno);
                }
                seg.shm_buffer = nullptr;
            }
        }
        context.mapped_shms.clear();

        shm_it = shm_contexts_.erase(shm_it);
    }
    return {};
}

int RealClient::tearDownAll() { return to_py_ret(tearDownAll_internal()); }

tl::expected<void, ErrorCode> RealClient::put_internal(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto alloc_result = client_buffer_allocator->allocate(value.size_bytes());
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for put operation, key: "
                   << key << ", value size: " << value.size();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &buffer_handle = *alloc_result;
    memcpy(buffer_handle.ptr(), value.data(), value.size_bytes());

    std::vector<Slice> slices = split_into_slices(buffer_handle);

    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        return tl::unexpected(put_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> RealClient::put_dummy_helper(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    return put_internal(key, value, config, context.client_buffer_allocator);
}

int RealClient::put(const std::string &key, std::span<const char> value,
                    const ReplicateConfig &config) {
    return to_py_ret(
        put_internal(key, value, config, client_buffer_allocator_));
}

tl::expected<void, ErrorCode> RealClient::put_batch_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (keys.size() != values.size()) {
        LOG(ERROR) << "Key and value size mismatch";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<BufferHandle> buffer_handles;
    std::unordered_map<std::string, std::vector<Slice>> batched_slices;
    batched_slices.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        auto &key = keys[i];
        auto &value = values[i];
        auto alloc_result =
            client_buffer_allocator->allocate(value.size_bytes());
        if (!alloc_result) {
            LOG(ERROR)
                << "Failed to allocate buffer for put_batch operation, key: "
                << key << ", value size: " << value.size();
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto &buffer_handle = *alloc_result;
        memcpy(buffer_handle.ptr(), value.data(), value.size_bytes());
        auto slices = split_into_slices(buffer_handle);
        buffer_handles.emplace_back(std::move(*alloc_result));
        batched_slices.emplace(key, std::move(slices));
    }

    // Convert unordered_map to vector format expected by BatchPut
    std::vector<std::vector<mooncake::Slice>> ordered_batched_slices;
    ordered_batched_slices.reserve(keys.size());
    for (const auto &key : keys) {
        auto it = batched_slices.find(key);
        if (it != batched_slices.end()) {
            ordered_batched_slices.emplace_back(it->second);
        } else {
            LOG(ERROR) << "Missing slices for key: " << key;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    auto results = client_->BatchPut(keys, ordered_batched_slices, config);

    // Check if any operations failed
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            return tl::unexpected(results[i].error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::put_batch_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    return put_batch_internal(keys, values, config,
                              context.client_buffer_allocator);
}

int RealClient::put_batch(const std::vector<std::string> &keys,
                          const std::vector<std::span<const char>> &values,
                          const ReplicateConfig &config) {
    return to_py_ret(
        put_batch_internal(keys, values, config, client_buffer_allocator_));
}

tl::expected<void, ErrorCode> RealClient::put_parts_internal(
    const std::string &key, std::vector<std::span<const char>> values,
    const ReplicateConfig &config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Calculate total size needed
    size_t total_size = 0;
    for (const auto &value : values) {
        total_size += value.size_bytes();
    }

    if (total_size == 0) {
        LOG(WARNING) << "Attempting to put empty data for key: " << key;
        return {};
    }

    // Allocate buffer using the new allocator
    auto alloc_result = client_buffer_allocator->allocate(total_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for put_parts operation, key: "
                   << key << ", total size: " << total_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto &buffer_handle = *alloc_result;

    // Copy all parts into the contiguous buffer
    size_t offset = 0;
    for (const auto &value : values) {
        memcpy(static_cast<char *>(buffer_handle.ptr()) + offset, value.data(),
               value.size_bytes());
        offset += value.size_bytes();
    }

    // Split into slices
    std::vector<Slice> slices = split_into_slices(buffer_handle);

    // Perform the put operation - buffer_handle will be automatically released
    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return tl::unexpected(put_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> RealClient::put_parts_dummy_helper(
    const std::string &key, std::vector<std::span<const char>> values,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    return put_parts_internal(key, values, config,
                              context.client_buffer_allocator);
}

int RealClient::put_parts(const std::string &key,
                          std::vector<std::span<const char>> values,
                          const ReplicateConfig &config) {
    return to_py_ret(
        put_parts_internal(key, values, config, client_buffer_allocator_));
}

tl::expected<void, ErrorCode> RealClient::remove_internal(
    const std::string &key, bool force) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Wait for any inflight cache-on-get for this key to complete,
    // otherwise the PROCESSING cache replica blocks Remove.
    wait_cache_inflight(key);
    auto remove_result = client_->Remove(key, force);
    if (!remove_result) {
        return tl::unexpected(remove_result.error());
    }
    return {};
}

int RealClient::remove(const std::string &key, bool force) {
    return to_py_ret(remove_internal(key, force));
}

tl::expected<long, ErrorCode> RealClient::removeByRegex_internal(
    const std::string &str, bool force) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Wait for all inflight cache-on-get operations to complete.
    wait_all_cache_inflight();
    return client_->RemoveByRegex(str, force);
}

long RealClient::removeByRegex(const std::string &str, bool force) {
    return to_py_ret(removeByRegex_internal(str, force));
}

tl::expected<int64_t, ErrorCode> RealClient::removeAll_internal(bool force) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Wait for all inflight cache-on-get operations to complete.
    wait_all_cache_inflight();
    return client_->RemoveAll(force);
}

long RealClient::removeAll(bool force) {
    return to_py_ret(removeAll_internal(force));
}

tl::expected<bool, ErrorCode> RealClient::isExist_internal(
    const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_->IsExist(key);
}

int RealClient::isExist(const std::string &key) {
    auto result = isExist_internal(key);

    if (result.has_value()) {
        return *result ? 1 : 0;  // 1 if exists, 0 if not
    } else {
        return toInt(result.error());
    }
}

std::vector<int> RealClient::batchIsExist(
    const std::vector<std::string> &keys) {
    auto internal_results = batchIsExist_internal(keys);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        if (result.has_value()) {
            results.push_back(result.value() ? 1 : 0);  // 1 if exists, 0 if not
        } else {
            results.push_back(toInt(result.error()));
        }
    }

    return results;
}

tl::expected<int64_t, ErrorCode> RealClient::getSize_internal(
    const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto query_result = client_->Query(key);

    if (!query_result) {
        return tl::unexpected(query_result.error());
    }

    const std::vector<Replica::Descriptor> &replica_list =
        query_result.value().replicas;

    // Calculate total size from all replicas' handles
    int64_t total_size = 0;
    if (!replica_list.empty()) {
        auto &replica = replica_list[0];
        total_size = calculate_total_size(replica);
    } else {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);  // Internal error
    }

    return total_size;
}

int64_t RealClient::getSize(const std::string &key) {
    return to_py_ret(getSize_internal(key));
}

tl::expected<void, ErrorCode> RealClient::map_shm_internal(
    int fd, uint64_t dummy_base_addr, size_t shm_size, bool is_local_buffer,
    const UUID &client_id) {
    std::stringstream addr_stream;
    addr_stream << "0x" << std::hex << dummy_base_addr;

    std::string shm_name =
        std::string(MOONCAKE_SHM_NAME) + "_" + std::to_string(client_id.first) +
        "_" + std::to_string(client_id.second) + "_" + addr_stream.str();
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);

    // Check if client context exists, create if not
    auto &context = shm_contexts_[client_id];

    // Check if this shm is already mapped
    for (const auto &shm : context.mapped_shms) {
        if (shm.dummy_base_addr == static_cast<uintptr_t>(dummy_base_addr)) {
            LOG(INFO) << "Segment already mapped: " << shm_name;
            if (fd >= 0) close(fd);
            return {};
        }
    }

    if (fd < 0) {
        LOG(ERROR) << "Invalid file descriptor: " << fd;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Map shared memory from FD
    void *shm_buffer =
        mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (shm_buffer == MAP_FAILED) {
        LOG(ERROR) << "Failed to map shared memory from fd: " << fd
                   << ", name: " << shm_name << ", error: " << strerror(errno);
        close(fd);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    close(fd);  // Close FD after mapping

    MappedShm shm;
    shm.shm_name = shm_name;
    shm.shm_buffer = shm_buffer;
    shm.shm_size = shm_size;
    shm.dummy_base_addr = static_cast<uintptr_t>(dummy_base_addr);
    shm.shm_addr_offset = reinterpret_cast<uintptr_t>(shm_buffer) -
                          reinterpret_cast<uintptr_t>(dummy_base_addr);

    if (shm_size > 0) {
        auto result = client_->RegisterLocalMemory(
            shm.shm_buffer, shm_size, kWildcardLocation, false, true);
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to register memory";
            munmap(shm_buffer, shm_size);
            return tl::unexpected(result.error());
        }
    }

    if (is_local_buffer) {
        if (context.client_buffer_allocator) {
            LOG(ERROR) << "A local buffer is already mapped for this "
                          "client shared memory.";
            munmap(shm_buffer, shm_size);
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        context.client_buffer_allocator =
            ClientBufferAllocator::create(shm_buffer, shm_size, this->protocol);
    }

    context.mapped_shms.push_back(std::move(shm));

    LOG(INFO) << "Mapped new shared memory: " << shm_name
              << ", size: " << shm_size;
    return {};
}

tl::expected<void, ErrorCode> RealClient::unmap_shm_internal(
    const UUID &client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto &context = it->second;
    context.client_buffer_allocator.reset();

    for (auto &shm : context.mapped_shms) {
        if (shm.shm_buffer) {
            auto rc =
                client_->unregisterLocalMemory(shm.shm_buffer, shm.shm_size);
            if (!rc) {
                LOG(ERROR) << "Failed to unregister memory";
                munmap(shm.shm_buffer, shm.shm_size);
                context.mapped_shms.clear();
                shm_contexts_.erase(it);
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            munmap(shm.shm_buffer, shm.shm_size);
        }
    }
    context.mapped_shms.clear();
    shm_contexts_.erase(it);
    return {};
}

tl::expected<void, ErrorCode> RealClient::unregister_shm_buffer_internal(
    uint64_t dummy_base_addr, const UUID &client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    // Find the shm corresponding to this dummy address
    auto shm_it = context.mapped_shms.end();
    for (auto sit = context.mapped_shms.begin();
         sit != context.mapped_shms.end(); ++sit) {
        if (dummy_base_addr == sit->dummy_base_addr) {
            shm_it = sit;
            break;
        }
    }

    if (shm_it == context.mapped_shms.end()) {
        std::stringstream addr_stream;
        addr_stream << "0x" << std::hex << dummy_base_addr;
        LOG(ERROR) << "Share memory not found for dummy address: "
                   << addr_stream.str() << " (client_id: " << client_id << ")";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Unmap and clean up the shm
    if (shm_it->shm_buffer) {
        // Unregister from transfer engine if it was registered
        if (shm_it->shm_size > 0) {
            // Matching the usage in unmap_shm_internal
            auto res = client_->unregisterLocalMemory(shm_it->shm_buffer,
                                                      shm_it->shm_size);
            if (!res) {
                LOG(WARNING)
                    << "Failed to unregister local memory for shared memory: "
                    << shm_it->shm_name << ", error: " << toString(res.error());
            }
        }

        if (munmap(shm_it->shm_buffer, shm_it->shm_size) != 0) {
            LOG(ERROR) << "Failed to munmap shared memory: " << shm_it->shm_name
                       << ", error: " << strerror(errno);
        } else {
            LOG(INFO) << "Unmapped and cleaned up shared memory: "
                      << shm_it->shm_name << ", size: " << shm_it->shm_size;
        }
    }

    // Remove shm from list
    context.mapped_shms.erase(shm_it);

    return {};
}

// Implementation of get_buffer_internal method
std::shared_ptr<BufferHandle> RealClient::get_buffer_internal(
    const std::string &key,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator,
    bool cache) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return nullptr;
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return nullptr;
    }

    // Query the object info
    auto query_result = client_->Query(key);
    if (!query_result) {
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
            query_result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            return nullptr;
        }
        LOG(ERROR) << "Query failed for key: " << key
                   << " with error: " << toString(query_result.error());
        return nullptr;
    }

    const std::vector<Replica::Descriptor> &replica_list =
        query_result.value().replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return nullptr;
    }

    auto res = client_->GetPreferredReplica(replica_list);
    if (!res) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return nullptr;
    }

    // Track whether we need to trigger async caching after the Get
    bool should_cache =
        cache && !client_->IsReplicaOnLocalMemory(res.value());

    const auto &replica = res.value();
    uint64_t total_length = calculate_total_size(replica);

    if (total_length == 0) {
        return nullptr;
    }

    // Allocate buffer using the new allocator
    auto alloc_result = client_buffer_allocator->allocate(total_length);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get_buffer, key: " << key;
        return nullptr;
    }

    auto &buffer_handle = *alloc_result;

    // Create slices for the allocated buffer
    std::vector<Slice> slices;
    allocateSlices(slices, replica, buffer_handle.ptr());

    // Get the object data
    auto get_result = client_->Get(key, query_result.value(), slices);
    if (!get_result) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(get_result.error());
        return nullptr;
    }

    // Trigger async caching AFTER the Get completes, so the background
    // caching transfer doesn't compete for bandwidth with the actual read.
    if (should_cache) {
        try_cache_on_get(key);
    }

    // Create BufferHandle with the allocated memory
    // The buffer will be managed by the BufferHandle's shared_ptr
    return std::make_shared<BufferHandle>(std::move(buffer_handle));
}

// Implementation of get_buffer method
std::shared_ptr<BufferHandle> RealClient::get_buffer(const std::string &key,
                                                     bool cache) {
    return get_buffer_internal(key, client_buffer_allocator_, cache);
}

std::tuple<uint64_t, size_t> RealClient::get_buffer_info(
    const std::string &key) {
    auto buffer_handle = get_buffer_internal(key, client_buffer_allocator_);
    if (!buffer_handle) {
        LOG(ERROR) << "Failed to get buffer for key: " << key;
        return std::make_tuple(0, 0);
    }
    uint64_t buffer_base = reinterpret_cast<uint64_t>(buffer_handle->ptr());
    size_t buffer_size = buffer_handle->size();
    return std::make_tuple(buffer_base, buffer_size);
}

tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>
RealClient::get_buffer_info_dummy_helper(const std::string &key,
                                         const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    auto buffer_handle =
        get_buffer_internal(key, context.client_buffer_allocator);
    if (!buffer_handle) {
        LOG(ERROR) << "Failed to get buffer for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t buffer_base = reinterpret_cast<uint64_t>(buffer_handle->ptr());
    size_t buffer_size = buffer_handle->size();

    for (const auto &shm : context.mapped_shms) {
        uint64_t shm_start = reinterpret_cast<uint64_t>(shm.shm_buffer);
        uint64_t shm_end = shm_start + shm.shm_size;

        if (buffer_base >= shm_start && buffer_base < shm_end) {
            // Convert real address to dummy address.
            return std::make_tuple(buffer_base - shm.shm_addr_offset,
                                   buffer_size);
        }
    }

    LOG(ERROR) << "Buffer allocated at " << buffer_base
               << " not found in any shared memory for client " << client_id;
    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
}

// Implementation of batch_get_buffer_internal method
std::vector<std::shared_ptr<BufferHandle>>
RealClient::batch_get_buffer_internal(const std::vector<std::string> &keys,
                                      bool cache) {
    std::vector<std::shared_ptr<BufferHandle>> final_results(keys.size(),
                                                             nullptr);

    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return final_results;
    }

    if (keys.empty()) {
        return final_results;
    }

    // 1. Query metadata for all keys
    auto query_results = client_->BatchQuery(keys);

    // Collect non-local keys that need async caching (triggered after Get)
    std::vector<std::string> keys_to_cache;
    if (cache) {
        for (size_t i = 0; i < keys.size(); ++i) {
            if (!query_results[i] ||
                query_results[i].value().replicas.empty()) {
                continue;
            }
            auto pref = client_->GetPreferredReplica(
                query_results[i].value().replicas);
            if (pref && !client_->IsReplicaOnLocalMemory(pref.value())) {
                keys_to_cache.push_back(keys[i]);
            }
        }
    }

    // 2. Prepare for batch get: filter valid keys and prepare buffers
    struct KeyOp {
        size_t original_index;
        std::string key;
        QueryResult query_result;
        std::unique_ptr<BufferHandle> buffer_handle;
        std::vector<Slice> slices;
    };
    std::vector<KeyOp> valid_ops;
    valid_ops.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto &key = keys[i];

        if (!query_results[i]) {
            if (query_results[i].error() != ErrorCode::OBJECT_NOT_FOUND &&
                query_results[i].error() != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(query_results[i].error());
            }
            continue;
        }

        auto query_result_values = query_results[i].value();
        if (query_result_values.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            continue;
        }

        const auto &replica = query_result_values.replicas[0];
        uint64_t total_size = calculate_total_size(replica);
        if (total_size == 0) {
            continue;
        }

        auto alloc_result = client_buffer_allocator_->allocate(total_size);
        if (!alloc_result) {
            LOG(ERROR) << "Failed to allocate buffer for key: " << key;
            continue;
        }

        auto buffer_handle =
            std::make_unique<BufferHandle>(std::move(*alloc_result));
        std::vector<Slice> slices;
        allocateSlices(slices, replica, buffer_handle->ptr());

        valid_ops.emplace_back(
            KeyOp{.original_index = i,
                  .key = key,
                  .query_result = std::move(query_result_values),
                  .buffer_handle = std::move(buffer_handle),
                  .slices = std::move(slices)});
    }

    if (valid_ops.empty()) {
        return final_results;
    }

    // 3. Execute batch get
    std::vector<std::string> batch_keys;
    std::vector<QueryResult> batch_query_results;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;
    batch_keys.reserve(valid_ops.size());
    batch_query_results.reserve(valid_ops.size());

    for (auto &op : valid_ops) {
        batch_keys.push_back(op.key);
        batch_query_results.push_back(op.query_result);
        batch_slices[op.key] = op.slices;
    }

    auto batch_get_results =
        client_->BatchGet(batch_keys, batch_query_results, batch_slices);

    // 4. Process results and create BufferHandles
    for (size_t i = 0; i < valid_ops.size(); ++i) {
        if (batch_get_results[i]) {
            auto &op = valid_ops[i];
            final_results[op.original_index] =
                std::make_shared<BufferHandle>(std::move(*op.buffer_handle));
        } else {
            LOG(ERROR) << "BatchGet failed for key '" << valid_ops[i].key
                       << "': " << toString(batch_get_results[i].error());
        }
    }

    // Trigger async caching AFTER the batch Get completes
    for (const auto &k : keys_to_cache) {
        try_cache_on_get(k);
    }

    return final_results;
}

// Implementation of batch_get_buffer method
std::vector<std::shared_ptr<BufferHandle>> RealClient::batch_get_buffer(
    const std::vector<std::string> &keys, bool cache) {
    return batch_get_buffer_internal(keys, cache);
}

tl::expected<void, ErrorCode> RealClient::register_buffer_internal(
    void *buffer, size_t size) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_->RegisterLocalMemory(buffer, size, kWildcardLocation, false,
                                        true);
}

int RealClient::register_buffer(void *buffer, size_t size) {
    return to_py_ret(register_buffer_internal(buffer, size));
}

tl::expected<void, ErrorCode> RealClient::unregister_buffer_internal(
    void *buffer) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto unregister_result = client_->unregisterLocalMemory(buffer, true);
    if (!unregister_result) {
        LOG(ERROR) << "Unregister buffer failed with error: "
                   << toString(unregister_result.error());
        return tl::unexpected(unregister_result.error());
    }
    return {};
}

int RealClient::unregister_buffer(void *buffer) {
    return to_py_ret(unregister_buffer_internal(buffer));
}

tl::expected<int64_t, ErrorCode> RealClient::get_into_internal(
    const std::string &key, void *buffer, size_t size, bool cache) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Step 1: Get object info
    auto query_result = client_->Query(key);
    if (!query_result) {
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
            query_result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            VLOG(1) << "Object not found for key: " << key;
            return tl::unexpected(query_result.error());
        }
        LOG(ERROR) << "Query failed for key: " << key
                   << " with error: " << toString(query_result.error());
        return tl::unexpected(query_result.error());
    }

    const std::vector<Replica::Descriptor> &replica_list =
        query_result.value().replicas;

    // Calculate total size from replica list
    if (replica_list.empty()) {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto res = client_->GetPreferredReplica(replica_list);
    if (!res) {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Track whether we need to trigger async caching after the Get
    bool should_cache =
        cache && !client_->IsReplicaOnLocalMemory(res.value());

    const auto &replica = res.value();
    uint64_t total_size = calculate_total_size(replica);

    // Check if user buffer is large enough
    if (size < total_size) {
        LOG(ERROR) << "User buffer too small. Required: " << total_size
                   << ", provided: " << size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Step 2: Split user buffer according to object info and create
    // slices
    std::vector<mooncake::Slice> slices;
    allocateSlices(slices, replica, buffer);

    // Step 3: Read data directly into user buffer
    auto get_result = client_->Get(key, query_result.value(), slices);
    if (!get_result) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(get_result.error());
        return tl::unexpected(get_result.error());
    }

    // Trigger async caching AFTER the Get completes
    if (should_cache) {
        try_cache_on_get(key);
    }

    return static_cast<int64_t>(total_size);
}

int64_t RealClient::get_into(const std::string &key, void *buffer,
                             size_t size, bool cache) {
    return to_py_ret(get_into_internal(key, buffer, size, cache));
}

std::string RealClient::get_hostname() const { return local_hostname; }

std::vector<int> RealClient::batch_put_from(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config) {
    auto internal_results =
        batch_put_from_internal(keys, buffers, sizes, config);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_put_from_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<uint64_t> &dummy_buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config,
    const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto &context = it->second;

    std::vector<void *> buffers;
    buffers.reserve(dummy_buffers.size());
    const MappedShm *last_hit_shm = nullptr;

    for (size_t i = 0; i < dummy_buffers.size(); ++i) {
        uint64_t dummy_addr = dummy_buffers[i];
        size_t size = sizes[i];
        bool found = false;

        if (last_hit_shm && dummy_addr >= last_hit_shm->dummy_base_addr &&
            dummy_addr + size <=
                last_hit_shm->dummy_base_addr + last_hit_shm->shm_size) {
            buffers.push_back(reinterpret_cast<void *>(
                dummy_addr + last_hit_shm->shm_addr_offset));
            found = true;
        } else {
            for (const auto &shm : context.mapped_shms) {
                if (dummy_addr >= shm.dummy_base_addr &&
                    dummy_addr + size <= shm.dummy_base_addr + shm.shm_size) {
                    buffers.push_back(reinterpret_cast<void *>(
                        dummy_addr + shm.shm_addr_offset));
                    found = true;
                    last_hit_shm = &shm;
                    break;
                }
            }
        }

        if (!found) {
            LOG(ERROR) << "Dummy buffer at " << dummy_addr
                       << " not found in any mapped shared memory";
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }
    return batch_put_from_internal(keys, buffers, sizes, config);
}

std::vector<tl::expected<void, ErrorCode>> RealClient::batch_put_from_internal(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Mismatched sizes for keys, buffers, and sizes";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::unordered_map<std::string, std::vector<mooncake::Slice>> all_slices;

    // Create slices from user buffers
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::string &key = keys[i];
        void *buffer = buffers[i];
        size_t size = sizes[i];

        std::vector<mooncake::Slice> slices;
        uint64_t offset = 0;

        while (offset < size) {
            auto chunk_size = std::min(size - offset, kMaxSliceSize);
            void *chunk_ptr = static_cast<char *>(buffer) + offset;
            slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }

        all_slices[key] = std::move(slices);
    }

    std::vector<std::vector<mooncake::Slice>> ordered_batched_slices;
    ordered_batched_slices.reserve(keys.size());
    for (const auto &key : keys) {
        auto it = all_slices.find(key);
        if (it != all_slices.end()) {
            ordered_batched_slices.emplace_back(it->second);
        } else {
            LOG(ERROR) << "Missing slices for key: " << key;
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }

    // Call client BatchPut and return the vector<expected> directly
    return client_->BatchPut(keys, ordered_batched_slices, config);
}

tl::expected<void, ErrorCode> RealClient::put_from_internal(
    const std::string &key, void *buffer, size_t size,
    const ReplicateConfig &config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (size == 0) {
        LOG(WARNING) << "Attempting to put empty data for key: " << key;
        return {};
    }

    // Create slices directly from the user buffer
    std::vector<mooncake::Slice> slices;
    uint64_t offset = 0;

    while (offset < size) {
        auto chunk_size = std::min(size - offset, kMaxSliceSize);
        void *chunk_ptr = static_cast<char *>(buffer) + offset;
        slices.emplace_back(Slice{chunk_ptr, chunk_size});
        offset += chunk_size;
    }

    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        return tl::unexpected(put_result.error());
    }

    return {};
}

int RealClient::put_from(const std::string &key, void *buffer, size_t size,
                         const ReplicateConfig &config) {
    return to_py_ret(put_from_internal(key, buffer, size, config));
}

std::vector<int64_t> RealClient::batch_get_into(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes, bool cache) {
    auto internal_results =
        batch_get_into_internal(keys, buffers, sizes, cache);
    std::vector<int64_t> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<uint64_t> &dummy_buffers,
    const std::vector<size_t> &sizes, const UUID &client_id, bool cache) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto &context = it->second;

    std::vector<void *> buffers;
    buffers.reserve(dummy_buffers.size());
    const MappedShm *last_hit_shm = nullptr;

    for (size_t i = 0; i < dummy_buffers.size(); ++i) {
        uint64_t dummy_addr = dummy_buffers[i];
        size_t size = sizes[i];
        bool found = false;

        if (last_hit_shm && dummy_addr >= last_hit_shm->dummy_base_addr &&
            dummy_addr + size <=
                last_hit_shm->dummy_base_addr + last_hit_shm->shm_size) {
            buffers.push_back(reinterpret_cast<void *>(
                dummy_addr + last_hit_shm->shm_addr_offset));
            found = true;
        } else {
            for (const auto &shm : context.mapped_shms) {
                if (dummy_addr >= shm.dummy_base_addr &&
                    dummy_addr + size <= shm.dummy_base_addr + shm.shm_size) {
                    buffers.push_back(reinterpret_cast<void *>(
                        dummy_addr + shm.shm_addr_offset));
                    found = true;
                    last_hit_shm = &shm;
                    break;
                }
            }
        }

        if (!found) {
            LOG(ERROR) << "Dummy buffer at " << dummy_addr << " (size " << size
                       << ") "
                       << "not found in any mapped shared memory for client "
                       << client_id;
            return std::vector<tl::expected<int64_t, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }
    return batch_get_into_internal(keys, buffers, sizes, cache);
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_internal(const std::vector<std::string> &keys,
                                    const std::vector<void *> &buffers,
                                    const std::vector<size_t> &sizes,
                                    bool cache) {
    auto start_time = std::chrono::steady_clock::now();
    // Validate preconditions
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << buffers.size()
                   << ", sizes=" << sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    const size_t num_keys = keys.size();
    std::vector<tl::expected<int64_t, ErrorCode>> results(num_keys);

    if (num_keys == 0) {
        return results;
    }

    // Query metadata for all keys
    auto query_results = client_->BatchQuery(keys);

    // Collect non-local keys that need async caching (triggered after Get)
    std::vector<std::string> keys_to_cache;
    if (cache) {
        for (size_t i = 0; i < num_keys; ++i) {
            if (!query_results[i] ||
                query_results[i].value().replicas.empty()) {
                continue;
            }
            auto pref = client_->GetPreferredReplica(
                query_results[i].value().replicas);
            if (pref && !client_->IsReplicaOnLocalMemory(pref.value())) {
                keys_to_cache.push_back(keys[i]);
            }
        }
    }

    // Process each key individually and prepare for batch transfer
    struct ValidKeyInfo {
        std::string key;
        size_t original_index;
        QueryResult query_result;
        std::vector<Slice> slices;
        uint64_t total_size;
    };

    std::vector<ValidKeyInfo> valid_operations;
    std::unordered_map<std::string, ValidKeyInfo> valid_local_disk_operations;
    valid_operations.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i) {
        const auto &key = keys[i];

        // Handle query failures
        if (!query_results[i]) {
            const auto error = query_results[i].error();
            results[i] = tl::unexpected(error);
            if (error != ErrorCode::OBJECT_NOT_FOUND &&
                error != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(error);
            }
            continue;
        }

        // Validate replica list
        auto query_result_values = query_results[i].value();
        if (query_result_values.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        // Calculate required buffer size
        const auto &replica = query_result_values.replicas[0];
        uint64_t total_size = calculate_total_size(replica);

        // Validate buffer capacity
        if (sizes[i] < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << key
                       << "': required=" << total_size
                       << ", available=" << sizes[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        // Create slices for this key's buffer
        std::vector<Slice> key_slices;
        allocateSlices(key_slices, replica, buffers[i]);

        if (query_result_values.replicas.size() == 1 &&
            query_result_values.replicas.at(0).is_local_disk_replica()) {
            valid_local_disk_operations.emplace(
                key,
                ValidKeyInfo{.key = key,
                             .original_index = i,
                             .query_result = std::move(query_result_values),
                             .slices = std::move(key_slices),
                             .total_size = total_size});
            results[i] = static_cast<int64_t>(total_size);
            continue;
        }
        // Store operation info for batch processing
        valid_operations.push_back(
            {.key = key,
             .original_index = i,
             .query_result = std::move(query_result_values),
             .slices = std::move(key_slices),
             .total_size = total_size});

        // Set success result (actual bytes transferred)
        results[i] = static_cast<int64_t>(total_size);
    }

    // Early return if no valid operations
    if (valid_operations.empty() && valid_local_disk_operations.empty()) {
        return results;
    }

    // Prepare batch transfer data structures
    std::vector<std::string> batch_keys;
    std::vector<QueryResult> batch_query_results;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;

    batch_keys.reserve(valid_operations.size());
    batch_query_results.reserve(valid_operations.size());

    for (const auto &op : valid_operations) {
        batch_keys.push_back(op.key);
        batch_query_results.push_back(op.query_result);
        batch_slices[op.key] = op.slices;
    }
    if (!valid_operations.empty()) {
        // Execute batch transfer
        const auto batch_get_results =
            client_->BatchGet(batch_keys, batch_query_results, batch_slices);

        // Process transfer results
        for (size_t j = 0; j < batch_get_results.size(); ++j) {
            const auto &op = valid_operations[j];

            if (!batch_get_results[j]) {
                const auto error = batch_get_results[j].error();
                LOG(ERROR) << "BatchGet failed for key '" << op.key
                           << "': " << toString(error);
                results[op.original_index] = tl::unexpected(error);
            }
        }
    }

    // Prepare batch transfer data structures
    std::unordered_map<std::string, std::unordered_map<std::string, Slice>>
        offload_objects;

    for (const auto &op_it : valid_local_disk_operations) {
        const auto &replica = op_it.second.query_result.replicas.at(0);
        auto [store_segment_it, _] = offload_objects.try_emplace(
            replica.get_local_disk_descriptor().transport_endpoint);
        store_segment_it->second.emplace(op_it.first,
                                         op_it.second.slices.at(0));
    }

    size_t offload_object_count = 0;
    auto start_read_store_time = std::chrono::steady_clock::now();
    for (auto &offload_objects_it : offload_objects) {
        offload_object_count += offload_objects_it.second.size();
        auto batch_get_offload_result = batch_get_into_offload_object_internal(
            offload_objects_it.first, offload_objects_it.second);
        if (!batch_get_offload_result) {
            LOG(ERROR) << "Batch get store object failed with error: "
                       << batch_get_offload_result.error();
            for (const auto &offload_object_it : offload_objects_it.second) {
                results[valid_local_disk_operations.at(offload_object_it.first)
                            .original_index] =
                    tl::make_unexpected(batch_get_offload_result.error());
            }
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time)
                            .count();
    auto read_store_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            end_time - start_read_store_time)
            .count();
    LOG(INFO) << "Time taken for batch_get_into: " << elapsed_time
              << "us, read store: " << read_store_time
              << "us, with memory key count: " << valid_operations.size()
              << ", offload key count: " << offload_object_count;

    // Trigger async caching AFTER the batch Get completes
    for (const auto &k : keys_to_cache) {
        try_cache_on_get(k);
    }

    return results;
}

std::vector<tl::expected<bool, ErrorCode>> RealClient::batchIsExist_internal(
    const std::vector<std::string> &keys) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.empty()) {
        LOG(WARNING) << "Empty keys vector provided to batchIsExist_internal";
        return std::vector<tl::expected<bool, ErrorCode>>();
    }

    // Call client BatchIsExist and return the vector<expected> directly
    return client_->BatchIsExist(keys);
}

int RealClient::put_from_with_metadata(const std::string &key, void *buffer,
                                       void *metadata_buffer, size_t size,
                                       size_t metadata_size,
                                       const ReplicateConfig &config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return -1;
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }

    if (size == 0) {
        LOG(WARNING) << "Attempting to put empty data for key: " << key;
        return 0;
    }

    // Create slices directly from the user buffer
    std::vector<mooncake::Slice> slices;
    // Add metadata slice
    uint64_t metadata_offset = 0;
    while (metadata_offset < metadata_size) {
        auto metadata_chunk_size =
            std::min(metadata_size - metadata_offset, kMaxSliceSize);
        void *metadata_chunk_ptr =
            static_cast<char *>(metadata_buffer) + metadata_offset;
        slices.emplace_back(Slice{metadata_chunk_ptr, metadata_chunk_size});
        metadata_offset += metadata_chunk_size;
    }

    uint64_t offset = 0;
    while (offset < size) {
        auto chunk_size = std::min(size - offset, kMaxSliceSize);
        void *chunk_ptr = static_cast<char *>(buffer) + offset;
        slices.emplace_back(Slice{chunk_ptr, chunk_size});
        offset += chunk_size;
    }
    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return -toInt(put_result.error());
    }
    return 0;
}

std::vector<int> RealClient::batch_put_from_multi_buffers(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &sizes,
    const ReplicateConfig &config) {
    auto start = std::chrono::steady_clock::now();

    auto internal_results =
        batch_put_from_multi_buffers_internal(keys, all_buffers, sizes, config);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    auto duration_call = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);
    VLOG(1) << "batch_put_from_multi_buffers: " << duration_call.count()
            << " us";
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_put_from_multi_buffers_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const ReplicateConfig &config) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if ((keys.size() != all_buffers.size()) ||
        (all_buffers.size() != all_sizes.size())) {
        LOG(ERROR) << "Mismatched sizes for keys, buffers, and sizes";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<std::vector<mooncake::Slice>> batched_slices(keys.size());
    for (size_t i = 0; i < all_buffers.size(); ++i) {
        const auto &buffers = all_buffers[i];
        const auto &sizes = all_sizes[i];
        if (buffers.size() != sizes.size()) {
            LOG(ERROR) << "Mismatched buffers and sizes of key:" << keys[i];
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        batched_slices[i].reserve(buffers.size());
        for (size_t j = 0; j < buffers.size(); ++j) {
            batched_slices[i].emplace_back(Slice{buffers[j], sizes[j]});
        }
    }
    // Call client BatchPut and return the vector<expected> directly
    return client_->BatchPut(keys, batched_slices, config);
}

std::vector<int> RealClient::batch_get_into_multi_buffers(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    bool prefer_alloc_in_same_node) {
    auto start = std::chrono::steady_clock::now();
    auto internal_results = batch_get_into_multi_buffers_internal(
        keys, all_buffers, all_sizes, prefer_alloc_in_same_node);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }
    auto duration_call = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);
    VLOG(1) << "batch_get_into_multi_buffers: " << duration_call.count()
            << " us";
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_multi_buffers_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    bool prefer_alloc_in_same_node) {
    // Validate preconditions
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << all_buffers.size()
                   << ", sizes=" << all_sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    const size_t num_keys = keys.size();
    std::vector<tl::expected<int64_t, ErrorCode>> results;
    results.reserve(num_keys);
    if (num_keys == 0) {
        return results;
    }
    // Query metadata for all keys
    const auto query_results = client_->BatchQuery(keys);
    // Process each key individually and prepare for batch transfer
    struct ValidKeyInfo {
        std::string key;
        size_t original_index;
        QueryResult query_result;
        std::vector<Slice> slices;
        uint64_t total_size;
    };

    std::vector<ValidKeyInfo> valid_operations;
    valid_operations.reserve(num_keys);
    for (size_t i = 0; i < num_keys; ++i) {
        const auto &key = keys[i];
        // Handle query failures
        if (!query_results[i]) {
            const auto error = query_results[i].error();
            results.emplace_back(tl::unexpected(error));
            if (error != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(error);
            }
            continue;
        }
        // Validate replica list
        auto query_result_values = query_results[i].value();
        if (query_result_values.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_REPLICA));
            continue;
        }
        // Calculate required buffer size
        const auto &replica = query_result_values.replicas[0];
        uint64_t total_size = calculate_total_size(replica);
        const auto &sizes = all_sizes[i];
        uint64_t dst_total_size = 0;
        for (auto &size : sizes) {
            dst_total_size += size;
        }
        if (dst_total_size < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << key
                       << "': required=" << total_size
                       << ", available=" << dst_total_size;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        // Create slices for this key's buffer
        const auto &buffers = all_buffers[i];
        std::vector<Slice> key_slices;
        key_slices.reserve(buffers.size());
        if (replica.is_memory_replica()) {
            for (size_t j = 0; j < buffers.size(); ++j) {
                key_slices.emplace_back(Slice{buffers[j], sizes[j]});
            }
        } else {
            LOG(ERROR) << "Invalid replica type for key: " << key;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        valid_operations.push_back(
            {.key = key,
             .original_index = i,
             .query_result = std::move(query_result_values),
             .slices = std::move(key_slices),
             .total_size = total_size});
        // Set success result (actual bytes transferred)
        results.emplace_back(static_cast<int64_t>(total_size));
    }
    // Early return if no valid operations
    if (valid_operations.empty()) {
        return results;
    }

    // Prepare batch transfer data structures
    std::vector<std::string> batch_keys;
    std::vector<QueryResult> batch_query_results;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;
    batch_keys.reserve(valid_operations.size());
    batch_query_results.reserve(valid_operations.size());
    for (auto &op : valid_operations) {
        batch_keys.push_back(op.key);
        batch_query_results.push_back(op.query_result);
        batch_slices[op.key] = op.slices;
    }

    auto batch_get_results =
        client_->BatchGet(batch_keys, batch_query_results, batch_slices,
                          prefer_alloc_in_same_node);

    // Process transfer results
    for (size_t j = 0; j < batch_get_results.size(); ++j) {
        const auto &op = valid_operations[j];

        if (!batch_get_results[j]) {
            const auto error = batch_get_results[j].error();
            LOG(ERROR) << "BatchGet failed for key '" << op.key
                       << "': " << toString(error);
            results[op.original_index] = tl::unexpected(error);
        }
    }
    return results;
}

tl::expected<PingResponse, ErrorCode> RealClient::ping(const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    ClientStatus client_status = ClientStatus::OK;

    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!dummy_client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=dummy_client_ping_queue_";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return PingResponse(view_version_, client_status);
}

void RealClient::dummy_client_monitor_func() {
    std::unordered_map<UUID, std::chrono::steady_clock::time_point,
                       boost::hash<UUID>>
        client_ttl;
    while (dummy_client_monitor_running_) {
        auto now = std::chrono::steady_clock::now();

        // Update the client ttl
        PodUUID pod_client_id;
        while (dummy_client_ping_queue_.pop(pod_client_id)) {
            UUID client_id = {pod_client_id.first, pod_client_id.second};
            client_ttl[client_id] =
                now + std::chrono::seconds(dummy_client_live_ttl_sec_);
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            if (it->second < now) {
                LOG(INFO) << "client_id=" << it->first
                          << ", action=client_expired";
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                ++it;
            }
        }

        // Update the client status to NEED_REMOUNT
        if (!expired_clients.empty()) {
            for (auto &client_id : expired_clients) {
                // Unmap mapped_shms associated with this client
                unmap_shm_internal(client_id);
            }
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kDummyClientMonitorSleepMs));
    }
}

int RealClient::start_dummy_client_monitor() {
    // Start client monitor thread in all modes so TTL/heartbeat works
    dummy_client_monitor_running_ = true;
    dummy_client_monitor_thread_ =
        std::thread(&RealClient::dummy_client_monitor_func, this);
    if (!dummy_client_monitor_thread_.joinable()) {
        LOG(ERROR) << "Failed to start dummy_client_monitor_thread";
        return -1;
    }
    LOG(INFO) << "start dummy_client_monitor_thread";
    return 0;
}

int RealClient::start_ipc_server() {
    ipc_running_ = true;
    ipc_thread_ = std::jthread(&RealClient::ipc_server_func, this);
    return 0;
}

int RealClient::stop_ipc_server() {
    ipc_running_ = false;
    // Connect to self to unblock accept if blocked, or unlink
    if (!ipc_socket_path_.empty()) {
        // Create a dummy socket and connect
        int sock = socket(AF_UNIX, SOCK_STREAM, 0);
        if (sock >= 0) {
            struct sockaddr_un addr;
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strncpy(&addr.sun_path[1], ipc_socket_path_.c_str(),
                    sizeof(addr.sun_path) - 2);
            connect(sock, (struct sockaddr *)&addr,
                    sizeof(sa_family_t) + strlen(&addr.sun_path[1]) + 1);
            close(sock);
        }
    }
    // jthread will join on destruction or we can explicitly join if needed
    // But recvmsg/accept might block. The logic above attempts to unblock.
    return 0;
}

static int recv_fd(int socket, void *data, size_t data_len) {
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    struct iovec iov;
    char buf[CMSG_SPACE(sizeof(int))];
    memset(buf, 0, sizeof(buf));

    iov.iov_base = data;
    iov.iov_len = data_len;

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = buf;
    msg.msg_controllen = sizeof(buf);

    if (recvmsg(socket, &msg, 0) < 0) return -1;

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_level == SOL_SOCKET &&
        cmsg->cmsg_type == SCM_RIGHTS) {
        int fd;
        memcpy(&fd, CMSG_DATA(cmsg), sizeof(int));
        return fd;
    }
    return -1;
}

void RealClient::ipc_server_func() {
    int server_sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_sock < 0) {
        LOG(ERROR) << "Failed to create IPC socket";
        return;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    // Using abstract namespace, so we don't need to unlink
    strncpy(&addr.sun_path[1], ipc_socket_path_.c_str(),
            sizeof(addr.sun_path) - 2);

    if (bind(server_sock, (struct sockaddr *)&addr,
             sizeof(sa_family_t) + strlen(&addr.sun_path[1]) + 1) < 0) {
        LOG(ERROR) << "Failed to bind IPC socket: " << strerror(errno);
        close(server_sock);
        return;
    }

    if (listen(server_sock, 5) < 0) {
        LOG(ERROR) << "Failed to listen on IPC socket: " << strerror(errno);
        close(server_sock);
        return;
    }

    LOG(INFO) << "IPC server is listening";

    while (ipc_running_) {
        int client_sock = accept(server_sock, nullptr, nullptr);
        if (client_sock < 0) {
            if (ipc_running_) {
                LOG(ERROR) << "Accept failed: " << strerror(errno);
            }
            continue;
        }

        if (!ipc_running_) {
            close(client_sock);
            break;
        }

        ShmRegisterRequest req;
        int fd = recv_fd(client_sock, &req, sizeof(req));

        int status = 0;
        if (fd < 0) {
            LOG(ERROR) << "Failed to receive FD from client";
            status = -1;
        } else {
            UUID client_id = {req.client_id_first, req.client_id_second};
            auto ret = map_shm_internal(fd, req.dummy_base_addr, req.shm_size,
                                        req.is_local_buffer, client_id);
            if (!ret) {
                status = toInt(ret.error());
                // FD is closed inside map_shm_internal
            }
        }

        // Send response
        if (send(client_sock, &status, sizeof(status), 0) < 0) {
            LOG(ERROR) << "Failed to send response to client";
        }
        close(client_sock);
    }

    close(server_sock);
    LOG(INFO) << "IPC server stopped";
}

std::vector<Replica::Descriptor> RealClient::get_replica_desc(
    const std::string &key) {
    auto query_result = client_->Query(key);
    if (!query_result) {
        std::vector<Replica::Descriptor> replica_list = {};
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
            query_result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            LOG(ERROR) << "Object not found for key: " << key;
        } else {
            LOG(ERROR) << "Query failed for key: " << key
                       << " with error: " << toString(query_result.error());
        }
        return replica_list;
    }
    const std::vector<Replica::Descriptor> &replica_list =
        query_result.value().replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
    }
    return replica_list;
}

std::map<std::string, std::vector<Replica::Descriptor>>
RealClient::batch_get_replica_desc(const std::vector<std::string> &keys) {
    auto query_results = client_->BatchQuery(keys);
    std::map<std::string, std::vector<Replica::Descriptor>> replica_map;
    if (query_results.size() != keys.size()) {
        LOG(ERROR) << "Batch query response size mismatch in "
                      "batch_get_allocated_buffer_desc: expected "
                   << keys.size() << ", got " << query_results.size() << ".";
        return replica_map;
    }

    for (size_t i = 0; i < query_results.size(); ++i) {
        if (query_results[i]) {
            replica_map[keys[i]] = query_results[i].value().replicas;
        } else {
            LOG(ERROR) << "batch_get_replica failed for key: " << keys[i]
                       << " with error: " << toString(query_results[i].error());
        }
    }
    return replica_map;
}

tl::expected<UUID, ErrorCode> RealClient::create_copy_task(
    const std::string &key, const std::vector<std::string> &targets) {
    return client_->CreateCopyTask(key, targets);
}

tl::expected<UUID, ErrorCode> RealClient::create_move_task(
    const std::string &key, const std::string &source,
    const std::string &target) {
    return client_->CreateMoveTask(key, source, target);
}

tl::expected<QueryTaskResponse, ErrorCode> RealClient::query_task(
    const UUID &task_id) {
    return client_->QueryTask(task_id);
}
tl::expected<BatchGetOffloadObjectResponse, ErrorCode>
RealClient::batch_get_offload_object(const std::vector<std::string> &keys,
                                     const std::vector<int64_t> &sizes) {
    auto result = file_storage_->BatchGet(keys, sizes);
    if (!result) {
        LOG(ERROR) << "Batch get offload object failed,err_code = "
                   << result.error();
        return tl::make_unexpected(result.error());
    }
    return BatchGetOffloadObjectResponse(
        std::move(result.value()), client_->GetTransportEndpoint(),
        file_storage_->config_.client_buffer_gc_ttl_ms);
}

tl::expected<void, ErrorCode>
RealClient::batch_get_into_offload_object_internal(
    const std::string &target_rpc_service_addr,
    std::unordered_map<std::string, Slice> &objects) {
    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    for (const auto &object_it : objects) {
        keys.emplace_back(object_it.first);
        sizes.emplace_back(object_it.second.size);
    }
    auto batchGetResp = client_requester_->batch_get_offload_object(
        target_rpc_service_addr, keys, sizes);
    if (!batchGetResp) {
        LOG(ERROR) << "Batch get offload object failed with error: "
                   << batchGetResp.error();
        return tl::make_unexpected(batchGetResp.error());
    }
    auto result =
        client_->BatchGetOffloadObject(batchGetResp->transfer_engine_addr, keys,
                                       batchGetResp->pointers, objects);
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count());
    LOG(INFO) << "Time taken for batch_get_into_offload_object_internal: "
              << elapsed_time
              << "ms, with target_rpc_service_addr: " << target_rpc_service_addr
              << ", key size: " << objects.size()
              << "gc ttl: " << batchGetResp->gc_ttl_ms << "ms.";
    if (!result) {
        LOG(ERROR) << "Batch get into offload object failed with error: "
                   << result.error();
        return result;
    }
    if (elapsed_time >= batchGetResp->gc_ttl_ms) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }
    return {};
}

ClientRequester::ClientRequester() {
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
    const char *value = std::getenv("MC_RPC_PROTOCOL");
    if (value && std::string_view(value) == "rdma") {
        pool_conf.client_config.socket_config =
            coro_io::ib_socket_t::config_t{};
    }
    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf);
}

tl::expected<BatchGetOffloadObjectResponse, ErrorCode>
ClientRequester::batch_get_offload_object(const std::string &client_addr,
                                          const std::vector<std::string> &keys,
                                          const std::vector<int64_t> sizes) {
    auto result =
        invoke_rpc<&RealClient::batch_get_offload_object,
                   BatchGetOffloadObjectResponse>(client_addr, keys, sizes);
    if (!result) {
        LOG(ERROR)
            << "Failed to invoke batch_get_offload_object, client_addr = "
            << client_addr << ", error is: " << result.error();
    }
    return result;
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> ClientRequester::invoke_rpc(
    const std::string &client_addr, Args &&...args) {
    auto client_pool = client_pools_->at(client_addr);
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await client_pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client &client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Dummy Client not available";
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "RPC call failed: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
}
void RealClient::try_cache_on_get(const std::string &key) {
    // Check if another thread is already caching this key (read lock)
    {
        std::shared_lock<std::shared_mutex> rlock(cache_inflight_mutex_);
        if (cache_inflight_.count(key)) {
            return;  // Already being cached by another thread
        }
    }

    // Try to claim this key for caching (write lock)
    {
        std::unique_lock<std::shared_mutex> wlock(cache_inflight_mutex_);
        if (!cache_inflight_.insert(key).second) {
            return;  // Another thread claimed it between read and write lock
        }
    }

    // Fire-and-forget: launch caching in background thread.
    // Caller proceeds with remote transfer at normal speed.
    // Capture shared_ptr to prevent use-after-free if RealClient is destroyed
    // while the background thread is still running.
    auto self = std::static_pointer_cast<RealClient>(shared_from_this());
    std::thread([self, key]() {
        auto result =
            self->client_->TryCacheOnGet(key, self->client_->GetLocalHostname());
        if (!result.has_value()) {
            LOG(WARNING) << "try_cache_on_get failed for key=" << key
                         << ", error=" << toString(result.error());
        }

        // Remove from inflight set
        std::unique_lock<std::shared_mutex> wlock(self->cache_inflight_mutex_);
        self->cache_inflight_.erase(key);
        wlock.unlock();
        self->cache_inflight_cv_.notify_all();
    }).detach();
}

void RealClient::wait_cache_inflight(const std::string &key) {
    std::shared_lock<std::shared_mutex> rlock(cache_inflight_mutex_);
    cache_inflight_cv_.wait(rlock,
        [&]() { return cache_inflight_.count(key) == 0; });
}

void RealClient::wait_all_cache_inflight() {
    std::shared_lock<std::shared_mutex> rlock(cache_inflight_mutex_);
    cache_inflight_cv_.wait(rlock,
        [&]() { return cache_inflight_.empty(); });
}

}  // namespace mooncake
