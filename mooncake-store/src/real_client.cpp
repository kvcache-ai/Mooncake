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
#include <optional>
#include <vector>

#include "real_client.h"
#include "client_buffer.hpp"
#include "mutex.h"
#include "types.h"
#include "utils.h"
#include "rpc_types.h"
#include "default_config.h"

namespace mooncake {

PyClient::~PyClient() {}

// ResourceTracker implementation using singleton pattern
// Use a deliberately leaked heap object to avoid static destruction
// order issues with atexit/signal handlers during process teardown.
ResourceTracker& ResourceTracker::getInstance() {
    static ResourceTracker* instance = new ResourceTracker();
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
    const std::shared_ptr<PyClient>& instance) {
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

    for (auto& wp : instances_) {
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

                    // Unblock the signal before raising it so it can be
                    // delivered immediately
                    sigset_t unblock_set;
                    sigemptyset(&unblock_set);
                    sigaddset(&unblock_set, sig);
                    pthread_sigmask(SIG_UNBLOCK, &unblock_set, nullptr);

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

template <typename ConfigT>
int RealClient::setup(ConfigT& config) {
    return to_py_ret(setup_internal(config));
}

// Explicit template instantiations
template tl::expected<void, ErrorCode> RealClient::setup_internal(
    CentralizedClientConfig&);
template tl::expected<void, ErrorCode> RealClient::setup_internal(
    P2PClientConfig&);
template int RealClient::setup(CentralizedClientConfig&);
template int RealClient::setup(P2PClientConfig&);

template <typename ConfigT>
tl::expected<void, ErrorCode> RealClient::setup_internal(ConfigT& config) {
    this->protocol = config.protocol;
    this->ipc_socket_path_ = config.ipc_socket_path;

    if (config.te_port == 0) {
        // Create port binder to hold a port
        port_binder_ = std::make_unique<AutoPortBinder>();
        int port = port_binder_->getPort();
        if (port < 0) {
            LOG(ERROR) << "Failed to bind available port";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.te_port = static_cast<uint16_t>(port);
    }
    this->local_ip = config.local_ip;
    this->te_port = config.te_port;

    auto client_opt = mooncake::ClientService::Create(config);
    if (!client_opt) {
        LOG(ERROR) << "Failed to create client";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    client_service_ = *client_opt;

    // Local_buffer_size is allowed to be 0 when we use separately deployment.
    // If it is 0, skip registering local memory in real client.
    // Dummy Client can create shm and share it with Real Client.
    // Moreover, invoke ibv_reg_mr() with size=0 is UB, and may
    // fail in some rdma implementations.
    client_buffer_allocator_ =
        ClientBufferAllocator::create(config.local_buffer_size, this->protocol);
    if (config.local_buffer_size > 0) {
        LOG(INFO) << "Registering local memory: " << config.local_buffer_size
                  << " bytes";
        auto result = client_service_->RegisterLocalMemory(
            client_buffer_allocator_->getBase(), config.local_buffer_size,
            kWildcardLocation, false, true);
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to register local memory: "
                       << toString(result.error());
            return tl::unexpected(result.error());
        }
    } else {
        LOG(INFO) << "Local buffer size is 0, skip registering local memory";
    }

    // Start IPC server to accept FD from dummy clients
    if (!ipc_socket_path_.empty()) {
        if (start_ipc_server() != 0) {
            LOG(ERROR) << "Failed to start IPC server at " << ipc_socket_path_;
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        LOG(INFO) << "Starting IPC server at " << ipc_socket_path_;
    }
    auto res = start_dummy_client_monitor();
    if (res != 0) {
        LOG(ERROR) << "Failed to start dummy client monitor";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::initAll_internal(
    const std::string& protocol, const std::string& device_name,
    size_t mount_segment_size) {
    if (client_service_) {
        LOG(ERROR) << "Client is already initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t buffer_allocator_size = 1024 * 1024 * 1024;
    auto config = ClientConfigBuilder::build_centralized_real_client(
        "localhost:12345", "127.0.0.1:2379", protocol,
        device_name.empty() ? std::nullopt
                            : std::optional<std::string>(device_name),
        "127.0.0.1:50051", mount_segment_size, buffer_allocator_size);
    return setup_internal(config);
}

int RealClient::initAll(const std::string& protocol_,
                        const std::string& device_name,
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
    stop_dummy_client_monitor();

    if (!client_service_) {
        // Not initialized or already cleaned; treat as success for idempotence
        return {};
    }
    // Gracefully stop accepting new requests and drain in-flight operations
    client_service_->Stop();
    client_service_->Destroy();

    // Reset all resources
    client_service_.reset();
    client_buffer_allocator_.reset();
    port_binder_.reset();
    local_ip = "";
    te_port = 0;
    device_name = "";
    protocol = "";
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto shm_it = shm_contexts_.begin();
    while (shm_it != shm_contexts_.end()) {
        auto& context = shm_it->second;
        context.client_buffer_allocator.reset();

        // Iterate over all mapped_shms to unmap them
        for (auto& seg : context.mapped_shms) {
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
    const std::string& key, std::span<const char> value,
    const WriteConfig& config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (std::holds_alternative<ReplicateConfig>(config)) {
        if (std::get<ReplicateConfig>(config).prefer_alloc_in_same_node) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    if (!client_service_) {
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
    auto& buffer_handle = *alloc_result;
    memcpy(buffer_handle.ptr(), value.data(), value.size_bytes());

    std::vector<Slice> slices = split_into_slices(buffer_handle);

    auto put_result = client_service_->Put(key, slices, config);
    if (!put_result) {
        return tl::unexpected(put_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> RealClient::put_dummy_helper(
    const std::string& key, std::span<const char> value,
    const WriteConfig& config, const UUID& client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& context = it->second;

    return put_internal(key, value, config, context.client_buffer_allocator);
}

int RealClient::put(const std::string& key, std::span<const char> value,
                    const WriteConfig& config) {
    return to_py_ret(
        put_internal(key, value, config, client_buffer_allocator_));
}

tl::expected<void, ErrorCode> RealClient::put_batch_internal(
    const std::vector<std::string>& keys,
    const std::vector<std::span<const char>>& values, const WriteConfig& config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (std::holds_alternative<ReplicateConfig>(config)) {
        if (std::get<ReplicateConfig>(config).prefer_alloc_in_same_node) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    if (!client_service_) {
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
        auto& key = keys[i];
        auto& value = values[i];
        auto alloc_result =
            client_buffer_allocator->allocate(value.size_bytes());
        if (!alloc_result) {
            LOG(ERROR)
                << "Failed to allocate buffer for put_batch operation, key: "
                << key << ", value size: " << value.size();
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto& buffer_handle = *alloc_result;
        memcpy(buffer_handle.ptr(), value.data(), value.size_bytes());
        auto slices = split_into_slices(buffer_handle);
        buffer_handles.emplace_back(std::move(*alloc_result));
        batched_slices.emplace(key, std::move(slices));
    }

    // Convert unordered_map to vector format expected by BatchPut
    std::vector<std::vector<mooncake::Slice>> ordered_batched_slices;
    ordered_batched_slices.reserve(keys.size());
    for (const auto& key : keys) {
        auto it = batched_slices.find(key);
        if (it != batched_slices.end()) {
            ordered_batched_slices.emplace_back(it->second);
        } else {
            LOG(ERROR) << "Missing slices for key: " << key;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    auto results =
        client_service_->BatchPut(keys, ordered_batched_slices, config);

    // Check if any operations failed
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            return tl::unexpected(results[i].error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::put_batch_dummy_helper(
    const std::vector<std::string>& keys,
    const std::vector<std::span<const char>>& values, const WriteConfig& config,
    const UUID& client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& context = it->second;

    return put_batch_internal(keys, values, config,
                              context.client_buffer_allocator);
}

int RealClient::put_batch(const std::vector<std::string>& keys,
                          const std::vector<std::span<const char>>& values,
                          const WriteConfig& config) {
    return to_py_ret(
        put_batch_internal(keys, values, config, client_buffer_allocator_));
}

tl::expected<void, ErrorCode> RealClient::put_parts_internal(
    const std::string& key, std::vector<std::span<const char>> values,
    const WriteConfig& config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (std::holds_alternative<ReplicateConfig>(config)) {
        if (std::get<ReplicateConfig>(config).prefer_alloc_in_same_node) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Calculate total size needed
    size_t total_size = 0;
    for (const auto& value : values) {
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

    auto& buffer_handle = *alloc_result;

    // Copy all parts into the contiguous buffer
    size_t offset = 0;
    for (const auto& value : values) {
        memcpy(static_cast<char*>(buffer_handle.ptr()) + offset, value.data(),
               value.size_bytes());
        offset += value.size_bytes();
    }

    // Split into slices
    std::vector<Slice> slices = split_into_slices(buffer_handle);

    // Perform the put operation - buffer_handle will be automatically released
    auto put_result = client_service_->Put(key, slices, config);
    if (!put_result) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return tl::unexpected(put_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> RealClient::put_parts_dummy_helper(
    const std::string& key, std::vector<std::span<const char>> values,
    const WriteConfig& config, const UUID& client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& context = it->second;

    return put_parts_internal(key, values, config,
                              context.client_buffer_allocator);
}

int RealClient::put_parts(const std::string& key,
                          std::vector<std::span<const char>> values,
                          const WriteConfig& config) {
    return to_py_ret(
        put_parts_internal(key, values, config, client_buffer_allocator_));
}

tl::expected<void, ErrorCode> RealClient::remove_internal(
    const std::string& key) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto remove_result = client_service_->Remove(key);
    if (!remove_result) {
        return tl::unexpected(remove_result.error());
    }
    return {};
}

int RealClient::remove(const std::string& key) {
    return to_py_ret(remove_internal(key));
}

tl::expected<long, ErrorCode> RealClient::removeByRegex_internal(
    const std::string& str) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_service_->RemoveByRegex(str);
}

long RealClient::removeByRegex(const std::string& str) {
    return to_py_ret(removeByRegex_internal(str));
}

tl::expected<int64_t, ErrorCode> RealClient::removeAll_internal() {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_service_->RemoveAll();
}

long RealClient::removeAll() { return to_py_ret(removeAll_internal()); }

tl::expected<bool, ErrorCode> RealClient::isExist_internal(
    const std::string& key) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_service_->IsExist(key);
}

int RealClient::isExist(const std::string& key) {
    auto result = isExist_internal(key);

    if (result.has_value()) {
        return *result ? 1 : 0;  // 1 if exists, 0 if not
    } else {
        return toInt(result.error());
    }
}

std::vector<int> RealClient::batchIsExist(
    const std::vector<std::string>& keys) {
    auto internal_results = batchIsExist_internal(keys);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
        if (result.has_value()) {
            results.push_back(result.value() ? 1 : 0);  // 1 if exists, 0 if not
        } else {
            results.push_back(toInt(result.error()));
        }
    }

    return results;
}

tl::expected<int64_t, ErrorCode> RealClient::getSize_internal(
    const std::string& key) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto query_result = client_service_->Query(key);

    if (!query_result) {
        return tl::unexpected(query_result.error());
    }

    const std::vector<Replica::Descriptor>& replica_list =
        query_result.value()->replicas;

    // Calculate total size from all replicas' handles
    int64_t total_size = 0;
    if (!replica_list.empty()) {
        auto& replica = replica_list[0];
        total_size = calculate_total_size(replica);
    } else {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);  // Internal error
    }

    return total_size;
}

int64_t RealClient::getSize(const std::string& key) {
    return to_py_ret(getSize_internal(key));
}

tl::expected<void, ErrorCode> RealClient::map_shm_internal(
    int fd, uint64_t dummy_base_addr, size_t shm_size, bool is_local_buffer,
    const UUID& client_id) {
    std::stringstream addr_stream;
    addr_stream << "0x" << std::hex << dummy_base_addr;

    std::string shm_name =
        std::string(MOONCAKE_SHM_NAME) + "_" + std::to_string(client_id.first) +
        "_" + std::to_string(client_id.second) + "_" + addr_stream.str();
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);

    // Check if client context exists, create if not
    auto& context = shm_contexts_[client_id];

    // Check if this shm is already mapped
    for (const auto& shm : context.mapped_shms) {
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
    void* shm_buffer =
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
        auto result = client_service_->RegisterLocalMemory(
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
    const UUID& client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& context = it->second;
    context.client_buffer_allocator.reset();

    for (auto& shm : context.mapped_shms) {
        if (shm.shm_buffer) {
            auto rc = client_service_->unregisterLocalMemory(shm.shm_buffer,
                                                             shm.shm_size);
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
    uint64_t dummy_base_addr, const UUID& client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& context = it->second;

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
            auto res = client_service_->unregisterLocalMemory(
                shm_it->shm_buffer, shm_it->shm_size);
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
    const std::string& key,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator,
    const ReadRouteConfig& config) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return nullptr;
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return nullptr;
    }

    auto result = client_service_->Get(key, client_buffer_allocator, config);
    if (!result) {
        if (result.error() != ErrorCode::OBJECT_NOT_FOUND &&
            result.error() != ErrorCode::REPLICA_IS_NOT_READY) {
            LOG(ERROR) << "Get failed for key: " << key
                       << " with error: " << toString(result.error());
        }
        return nullptr;
    }
    return result.value();
}

// Implementation of get_buffer method
std::shared_ptr<BufferHandle> RealClient::get_buffer(
    const std::string& key, const ReadRouteConfig& config) {
    return get_buffer_internal(key, client_buffer_allocator_, config);
}

std::tuple<uint64_t, size_t> RealClient::get_buffer_info(
    const std::string& key, const ReadRouteConfig& config) {
    auto buffer_handle =
        get_buffer_internal(key, client_buffer_allocator_, config);
    if (!buffer_handle) {
        LOG(ERROR) << "Failed to get buffer for key: " << key;
        return std::make_tuple(0, 0);
    }
    uint64_t buffer_base = reinterpret_cast<uint64_t>(buffer_handle->ptr());
    size_t buffer_size = buffer_handle->size();
    return std::make_tuple(buffer_base, buffer_size);
}

tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>
RealClient::get_buffer_info_dummy_helper(const std::string& key,
                                         const ReadRouteConfig& config,
                                         const UUID& client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& context = it->second;

    auto buffer_handle =
        get_buffer_internal(key, context.client_buffer_allocator, config);
    if (!buffer_handle) {
        LOG(ERROR) << "Failed to get buffer for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t buffer_base = reinterpret_cast<uint64_t>(buffer_handle->ptr());
    size_t buffer_size = buffer_handle->size();

    for (const auto& shm : context.mapped_shms) {
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
RealClient::batch_get_buffer_internal(const std::vector<std::string>& keys,
                                      const ReadRouteConfig& config) {
    std::vector<std::shared_ptr<BufferHandle>> final_results(keys.size(),
                                                             nullptr);

    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return final_results;
    }

    if (keys.empty()) {
        return final_results;
    }

    auto results =
        client_service_->BatchGet(keys, client_buffer_allocator_, config);

    for (size_t i = 0; i < keys.size(); ++i) {
        if (results[i]) {
            final_results[i] = results[i].value();
        } else {
            if (results[i].error() != ErrorCode::OBJECT_NOT_FOUND &&
                results[i].error() != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "BatchGet failed for key '" << keys[i]
                           << "': " << toString(results[i].error());
            }
        }
    }

    return final_results;
}

// Implementation of batch_get_buffer method
std::vector<std::shared_ptr<BufferHandle>> RealClient::batch_get_buffer(
    const std::vector<std::string>& keys, const ReadRouteConfig& config) {
    return batch_get_buffer_internal(keys, config);
}

tl::expected<void, ErrorCode> RealClient::register_buffer_internal(
    void* buffer, size_t size) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_service_->RegisterLocalMemory(buffer, size, kWildcardLocation,
                                                false, true);
}

int RealClient::register_buffer(void* buffer, size_t size) {
    return to_py_ret(register_buffer_internal(buffer, size));
}

tl::expected<void, ErrorCode> RealClient::unregister_buffer_internal(
    void* buffer) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto unregister_result =
        client_service_->unregisterLocalMemory(buffer, true);
    if (!unregister_result) {
        LOG(ERROR) << "Unregister buffer failed with error: "
                   << toString(unregister_result.error());
        return tl::unexpected(unregister_result.error());
    }
    return {};
}

int RealClient::unregister_buffer(void* buffer) {
    return to_py_ret(unregister_buffer_internal(buffer));
}

tl::expected<int64_t, ErrorCode> RealClient::get_into_internal(
    const std::string& key, void* buffer, size_t size,
    const ReadRouteConfig& config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    return client_service_->Get(key, {buffer}, {size}, config);
}

int64_t RealClient::get_into(const std::string& key, void* buffer, size_t size,
                             const ReadRouteConfig& config) {
    return to_py_ret(get_into_internal(key, buffer, size, config));
}

std::string RealClient::get_hostname() const {
    return local_ip + ":" + std::to_string(te_port);
}

std::vector<int> RealClient::batch_put_from(
    const std::vector<std::string>& keys, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const WriteConfig& config) {
    auto internal_results =
        batch_put_from_internal(keys, buffers, sizes, config);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_put_from_dummy_helper(
    const std::vector<std::string>& keys,
    const std::vector<uint64_t>& dummy_buffers,
    const std::vector<size_t>& sizes, const WriteConfig& config,
    const UUID& client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto& context = it->second;

    std::vector<void*> buffers;
    buffers.reserve(dummy_buffers.size());
    const MappedShm* last_hit_shm = nullptr;

    for (size_t i = 0; i < dummy_buffers.size(); ++i) {
        uint64_t dummy_addr = dummy_buffers[i];
        size_t size = sizes[i];
        bool found = false;

        if (last_hit_shm && dummy_addr >= last_hit_shm->dummy_base_addr &&
            dummy_addr + size <=
                last_hit_shm->dummy_base_addr + last_hit_shm->shm_size) {
            buffers.push_back(reinterpret_cast<void*>(
                dummy_addr + last_hit_shm->shm_addr_offset));
            found = true;
        } else {
            for (const auto& shm : context.mapped_shms) {
                if (dummy_addr >= shm.dummy_base_addr &&
                    dummy_addr + size <= shm.dummy_base_addr + shm.shm_size) {
                    buffers.push_back(reinterpret_cast<void*>(
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
    const std::vector<std::string>& keys, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const WriteConfig& config) {
    if (std::holds_alternative<ReplicateConfig>(config)) {
        if (std::get<ReplicateConfig>(config).prefer_alloc_in_same_node) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }
    if (!client_service_) {
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
        const std::string& key = keys[i];
        void* buffer = buffers[i];
        size_t size = sizes[i];

        std::vector<mooncake::Slice> slices;
        uint64_t offset = 0;

        while (offset < size) {
            auto chunk_size = std::min(size - offset, kMaxSliceSize);
            void* chunk_ptr = static_cast<char*>(buffer) + offset;
            slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }

        all_slices[key] = std::move(slices);
    }

    std::vector<std::vector<mooncake::Slice>> ordered_batched_slices;
    ordered_batched_slices.reserve(keys.size());
    for (const auto& key : keys) {
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
    return client_service_->BatchPut(keys, ordered_batched_slices, config);
}

tl::expected<void, ErrorCode> RealClient::put_from_internal(
    const std::string& key, void* buffer, size_t size,
    const WriteConfig& config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (std::holds_alternative<ReplicateConfig>(config)) {
        if (std::get<ReplicateConfig>(config).prefer_alloc_in_same_node) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    if (!client_service_) {
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
        void* chunk_ptr = static_cast<char*>(buffer) + offset;
        slices.emplace_back(Slice{chunk_ptr, chunk_size});
        offset += chunk_size;
    }

    auto put_result = client_service_->Put(key, slices, config);
    if (!put_result) {
        return tl::unexpected(put_result.error());
    }

    return {};
}

int RealClient::put_from(const std::string& key, void* buffer, size_t size,
                         const WriteConfig& config) {
    return to_py_ret(put_from_internal(key, buffer, size, config));
}

std::vector<int64_t> RealClient::batch_get_into(
    const std::vector<std::string>& keys, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const ReadRouteConfig& config) {
    auto internal_results =
        batch_get_into_internal(keys, buffers, sizes, config);
    std::vector<int64_t> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_dummy_helper(
    const std::vector<std::string>& keys,
    const std::vector<uint64_t>& dummy_buffers,
    const std::vector<size_t>& sizes, const ReadRouteConfig& config,
    const UUID& client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto& context = it->second;

    std::vector<void*> buffers;
    buffers.reserve(dummy_buffers.size());
    const MappedShm* last_hit_shm = nullptr;

    for (size_t i = 0; i < dummy_buffers.size(); ++i) {
        uint64_t dummy_addr = dummy_buffers[i];
        size_t size = sizes[i];
        bool found = false;

        if (last_hit_shm && dummy_addr >= last_hit_shm->dummy_base_addr &&
            dummy_addr + size <=
                last_hit_shm->dummy_base_addr + last_hit_shm->shm_size) {
            buffers.push_back(reinterpret_cast<void*>(
                dummy_addr + last_hit_shm->shm_addr_offset));
            found = true;
        } else {
            for (const auto& shm : context.mapped_shms) {
                if (dummy_addr >= shm.dummy_base_addr &&
                    dummy_addr + size <= shm.dummy_base_addr + shm.shm_size) {
                    buffers.push_back(reinterpret_cast<void*>(
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
    return batch_get_into_internal(keys, buffers, sizes, config);
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_internal(const std::vector<std::string>& keys,
                                    const std::vector<void*>& buffers,
                                    const std::vector<size_t>& sizes,
                                    const ReadRouteConfig& config) {
    // Validate preconditions
    if (!client_service_) {
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

    if (keys.empty()) {
        return {};
    }

    std::vector<std::vector<void*>> all_buffers(keys.size());
    std::vector<std::vector<size_t>> all_sizes(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        all_buffers[i] = {buffers[i]};
        all_sizes[i] = {sizes[i]};
    }
    return client_service_->BatchGet(keys, all_buffers, all_sizes, config);
}

std::vector<tl::expected<bool, ErrorCode>> RealClient::batchIsExist_internal(
    const std::vector<std::string>& keys) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.empty()) {
        LOG(WARNING) << "Empty keys vector provided to batchIsExist_internal";
        return std::vector<tl::expected<bool, ErrorCode>>();
    }

    // Call client BatchIsExist and return the vector<expected> directly
    return client_service_->BatchIsExist(keys);
}

int RealClient::put_from_with_metadata(const std::string& key, void* buffer,
                                       void* metadata_buffer, size_t size,
                                       size_t metadata_size,
                                       const WriteConfig& config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (std::holds_alternative<ReplicateConfig>(config)) {
        if (std::get<ReplicateConfig>(config).prefer_alloc_in_same_node) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
            return -1;
        }
    }
    if (!client_service_) {
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
        void* metadata_chunk_ptr =
            static_cast<char*>(metadata_buffer) + metadata_offset;
        slices.emplace_back(Slice{metadata_chunk_ptr, metadata_chunk_size});
        metadata_offset += metadata_chunk_size;
    }

    uint64_t offset = 0;
    while (offset < size) {
        auto chunk_size = std::min(size - offset, kMaxSliceSize);
        void* chunk_ptr = static_cast<char*>(buffer) + offset;
        slices.emplace_back(Slice{chunk_ptr, chunk_size});
        offset += chunk_size;
    }
    auto put_result = client_service_->Put(key, slices, config);
    if (!put_result) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return -toInt(put_result.error());
    }
    return 0;
}

std::vector<int> RealClient::batch_put_from_multi_buffers(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& sizes, const WriteConfig& config) {
    auto start = std::chrono::steady_clock::now();

    auto internal_results =
        batch_put_from_multi_buffers_internal(keys, all_buffers, sizes, config);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
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
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const WriteConfig& config) {
    if (!client_service_) {
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
        const auto& buffers = all_buffers[i];
        const auto& sizes = all_sizes[i];
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
    return client_service_->BatchPut(keys, batched_slices, config);
}

std::vector<int> RealClient::batch_get_into_multi_buffers(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    bool prefer_alloc_in_same_node, const ReadRouteConfig& config) {
    auto start = std::chrono::steady_clock::now();
    auto internal_results = batch_get_into_multi_buffers_internal(
        keys, all_buffers, all_sizes, prefer_alloc_in_same_node, config);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto& result : internal_results) {
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
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    bool prefer_alloc_in_same_node, const ReadRouteConfig& config) {
    // Validate preconditions
    if (!client_service_) {
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

    if (keys.empty()) {
        return {};
    }

    return client_service_->BatchGet(keys, all_buffers, all_sizes, config,
                                     prefer_alloc_in_same_node);
}

tl::expected<HeartbeatResponse, ErrorCode> RealClient::ping(
    const UUID& client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    ClientStatus client_status = ClientStatus::HEALTH;

    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!dummy_client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=dummy_client_ping_queue_";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    HeartbeatResponse resp;
    resp.status = client_status;
    resp.view_version = view_version_;
    return resp;
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
            for (auto& client_id : expired_clients) {
                // Unmap mapped_shms associated with this client
                unmap_shm_internal(client_id);
            }
        }

        std::unique_lock<std::mutex> lock(dummy_client_monitor_cv_mutex_);
        dummy_client_monitor_cv_.wait_for(
            lock, std::chrono::milliseconds(kDummyClientMonitorSleepMs),
            [this] { return !dummy_client_monitor_running_.load(); });
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
            connect(sock, (struct sockaddr*)&addr,
                    sizeof(sa_family_t) + strlen(&addr.sun_path[1]) + 1);
            close(sock);
        }
    }
    // jthread will join on destruction or we can explicitly join if needed
    // But recvmsg/accept might block. The logic above attempts to unblock.
    return 0;
}

int RealClient::stop_dummy_client_monitor() {
    {
        std::lock_guard<std::mutex> lock(dummy_client_monitor_cv_mutex_);
        dummy_client_monitor_running_ = false;
    }
    dummy_client_monitor_cv_.notify_all();
    if (dummy_client_monitor_thread_.joinable()) {
        dummy_client_monitor_thread_.join();
    }
    return 0;
}

static int recv_fd(int socket, void* data, size_t data_len) {
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

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
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

    if (bind(server_sock, (struct sockaddr*)&addr,
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
    const std::string& key) {
    auto query_result = client_service_->Query(key);
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
    const std::vector<Replica::Descriptor>& replica_list =
        query_result.value()->replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
    }
    return replica_list;
}

std::map<std::string, std::vector<Replica::Descriptor>>
RealClient::batch_get_replica_desc(const std::vector<std::string>& keys) {
    auto query_results = client_service_->BatchQuery(keys);
    std::map<std::string, std::vector<Replica::Descriptor>> replica_map;
    if (query_results.size() != keys.size()) {
        LOG(ERROR) << "Batch query response size mismatch in "
                      "batch_get_allocated_buffer_desc: expected "
                   << keys.size() << ", got " << query_results.size() << ".";
        return replica_map;
    }

    for (size_t i = 0; i < query_results.size(); ++i) {
        if (query_results[i]) {
            replica_map[keys[i]] = query_results[i].value()->replicas;
        } else {
            LOG(ERROR) << "batch_get_replica failed for key: " << keys[i]
                       << " with error: " << toString(query_results[i].error());
        }
    }
    return replica_map;
}

}  // namespace mooncake
