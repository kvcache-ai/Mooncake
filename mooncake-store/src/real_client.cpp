#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/un.h>
#include <unistd.h>
#include <numa.h>
#include <pthread.h>
#include <signal.h>
#include <thread>
#include <chrono>
#include <stop_token>

#include <cstdlib>  // for atexit
#include <algorithm>
#include <cctype>
#include <optional>
#include <exception>
#include <vector>

#if defined(MOONCAKE_STORE_CLIENT_P2P)
#include "p2p/client/p2p_client_config_builder.h"
#else
#include "centralized_client_config_builder.h"
#endif
#include "real_client.h"
#include "client_buffer.hpp"
#include "mutex.h"
#include "types.h"
#include "utils.h"
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

tl::expected<DeploymentMode, ErrorCode> RealClient::service_ready_internal() {
    const auto state = state_.load(std::memory_order_acquire);
    if (state != State::READY) {
        const auto error = state == State::CLOSING || state == State::CLOSED
                               ? ErrorCode::SHUTTING_DOWN
                               : ErrorCode::INTERNAL_ERROR;
        LOG(ERROR) << "Real client is not ready, error=" << error;
        return tl::unexpected(error);
    }
    return deployment_mode();
}

std::shared_ptr<ClientBufferAllocator> RealClient::GetBufferAllocator() const {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return nullptr;
    }
    return client_service_->GetBufferAllocator();
}

int RealClient::setup(const ClientConfig& config) {
    return to_py_ret(setup_internal(config));
}

tl::expected<void, ErrorCode> RealClient::setup_internal(
    const ClientConfig& config, bool enable_dummy_service,
    size_t rpc_thread_count, uint16_t rpc_port) {
    auto startup_result = ErrorCode::OK;
    {
        std::lock_guard lock(lifecycle_mutex_);
        if (state_.load(std::memory_order_acquire) != State::UNINITIALIZED) {
            LOG(ERROR) << "Cannot setup a closed or initialized RealClient";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto client = CreateService(config);
        if (!client) {
            LOG(ERROR) << "Failed to create native client: " << client.error();
            return tl::unexpected(client.error());
        }
        client_service_ = std::move(*client);
        protocol_ = config.protocol;
        ipc_socket_path_ = config.ipc_socket_path;
        if (!ipc_socket_path_.empty()) {
            startup_result = start_ipc_server();
        }
        if (startup_result != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start IPC server: " << startup_result;
        } else if (enable_dummy_service &&
                   (startup_result = start_dummy_client_monitor()) !=
                       ErrorCode::OK) {
            LOG(ERROR) << "Failed to start Dummy monitor: " << startup_result;
        } else if (enable_dummy_service &&
                   (startup_result = start_dummy_rpc_server(
                        rpc_thread_count, rpc_port)) != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start Dummy RPC server: "
                       << startup_result;
        } else {
            state_.store(State::READY, std::memory_order_release);
            return {};
        }
    }

    // failed path
    state_.store(State::CLOSING, std::memory_order_release);
    auto cleanup = tearDownAll_internal();
    if (!cleanup) {
        LOG(ERROR) << "Deployment startup cleanup failed: " << cleanup.error();
    }
    return tl::unexpected(startup_result);
}

ErrorCode RealClient::start_dummy_rpc_server(size_t thread_count,
                                             uint16_t port) {
    try {
        dummy_rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            thread_count, port, "127.0.0.1");
        register_dummy_rpc_handlers();
        // async_start() binds and listens before returning the exit future.
        dummy_rpc_exit_ = dummy_rpc_server_->async_start();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start Dummy RPC server: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
    if (dummy_rpc_exit_.hasResult()) {
        LOG(ERROR) << "Dummy RPC server failed to start on port " << port
                   << ": " << dummy_rpc_exit_.result().value().message();
        return ErrorCode::RPC_FAIL;
    }
    LOG(INFO) << "Dummy RPC server is listening at 127.0.0.1:"
              << dummy_rpc_server_->port();
    return ErrorCode::OK;
}

// Must call this exactly once after successful setup with Dummy RPC
// enabled. Concurrent or repeated waits are not supported.
int RealClient::wait_dummy_rpc_server() {
    if (!dummy_rpc_exit_.valid()) {
        LOG(ERROR) << "Cannot wait for Dummy RPC server: no exit future";
        return toInt(ErrorCode::INVALID_PARAMS);
    }

    auto error = std::move(dummy_rpc_exit_).get();
    int result = 0;
    if (state_.load(std::memory_order_acquire) == State::READY) {
        LOG(ERROR) << "Dummy RPC server exited unexpectedly: "
                   << error.message();
        result = toInt(ErrorCode::INTERNAL_ERROR);
    }
    auto cleanup = tearDownAll_internal();
    if (!cleanup) {
        LOG(ERROR) << "Failed to close real client: " << cleanup.error();
        return toInt(cleanup.error());
    }
    return result;
}

int RealClient::initAll(const std::string& protocol_,
                        const std::string& device_name,
                        size_t mount_segment_size) {
    return to_py_ret(
        initAll_internal(protocol_, device_name, mount_segment_size));
}

tl::expected<void, ErrorCode> RealClient::tearDownAll_internal() {
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_acquire) == State::CLOSED) {
        return {};
    }
    state_.store(State::CLOSING, std::memory_order_release);

    // Keep Dummy connections and SHM alive while native calls drain.
    if (client_service_) {
        client_service_->Stop();
    }
    if (dummy_rpc_server_) {
        dummy_rpc_server_->stop();
    }

    // Handlers have finished. Stop SHM reclamation and registration before
    // releasing mappings or their native owner.
    auto monitor_result = stop_dummy_client_monitor();
    auto ipc_result = stop_ipc_server();
    if (ipc_result != ErrorCode::OK || monitor_result != ErrorCode::OK) {
        LOG(ERROR) << "Failed to stop deployment workers: IPC=" << ipc_result
                   << ", Dummy monitor=" << monitor_result;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    if (!client_service_) {
        state_.store(State::CLOSED, std::memory_order_release);
        return {};
    }

    client_service_.reset();
    protocol_ = "";
    // Detach the map, then tear down each context under its own lock.
    std::unordered_map<UUID, std::shared_ptr<ShmContext>, boost::hash<UUID>>
        contexts;
    {
        SharedMutexLocker lock(&dummy_client_mutex_);
        contexts.swap(shm_contexts_);
    }
    for (auto& entry : contexts) {
        ShmContext& context = *entry.second;
        SharedMutexLocker lock(&context.mutex_);
        context.client_buffer_allocator_.reset();

        for (auto& seg : context.mapped_shms_) {
            if (seg.shm_buffer_) {
                // Memory mapped from memfd needs munmap
                if (munmap(seg.shm_buffer_, seg.shm_size_) != 0) {
                    LOG(ERROR) << "Failed to unmap shm: " << seg.shm_name_
                               << ", error: " << strerror(errno);
                }
                seg.shm_buffer_ = nullptr;
            }
        }
        context.mapped_shms_.clear();
    }
    state_.store(State::CLOSED, std::memory_order_release);
    return {};
}

int RealClient::tearDownAll() { return to_py_ret(tearDownAll_internal()); }

tl::expected<void, ErrorCode> RealClient::put_internal(
    const std::string& key, std::span<const char> value,
    const std::optional<WriteConfig>& config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    auto config_result = ValidateWriteEntryConfig(config);
    if (!config_result) {
        LOG(ERROR) << "the write config is invalid";
        return tl::unexpected(config_result.error());
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
        LOG(ERROR) << "Put failed, key=" << key
                   << ", error=" << put_result.error();
        return tl::unexpected(put_result.error());
    }

    return {};
}

std::shared_ptr<RealClient::ShmContext> RealClient::find_shm_context(
    const UUID& client_id) {
    SharedMutexLocker lock(&dummy_client_mutex_, shared_lock);
    auto it = shm_contexts_.find(client_id);
    return it == shm_contexts_.end() ? nullptr : it->second;
}

tl::expected<void, ErrorCode> RealClient::put_dummy_helper(
    const std::string& key, std::span<const char> value,
    const std::optional<WriteConfig>& config, const UUID& client_id) {
    auto context_ptr = find_shm_context(client_id);
    if (!context_ptr) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& context = *context_ptr;
    SharedMutexLocker lock(&context.mutex_, shared_lock);
    return put_internal(key, value, config, context.client_buffer_allocator_);
}

int RealClient::put(const std::string& key, std::span<const char> value,
                    const std::optional<WriteConfig>& config) {
    return to_py_ret(put_internal(key, value, config, GetBufferAllocator()));
}

tl::expected<void, ErrorCode> RealClient::put_batch_internal(
    const std::vector<std::string>& keys,
    const std::vector<std::span<const char>>& values,
    const std::optional<WriteConfig>& config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    auto config_result = ValidateWriteEntryConfig(config);
    if (!config_result) {
        LOG(ERROR) << "the write config is invalid";
        return tl::unexpected(config_result.error());
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
            LOG(ERROR) << "BatchPut failed, key=" << keys[i]
                       << ", error=" << results[i].error();
            return tl::unexpected(results[i].error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::put_batch_dummy_helper(
    const std::vector<std::string>& keys,
    const std::vector<std::span<const char>>& values,
    const std::optional<WriteConfig>& config, const UUID& client_id) {
    auto context_ptr = find_shm_context(client_id);
    if (!context_ptr) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& context = *context_ptr;
    SharedMutexLocker lock(&context.mutex_, shared_lock);
    return put_batch_internal(keys, values, config,
                              context.client_buffer_allocator_);
}

int RealClient::put_batch(const std::vector<std::string>& keys,
                          const std::vector<std::span<const char>>& values,
                          const std::optional<WriteConfig>& config) {
    return to_py_ret(
        put_batch_internal(keys, values, config, GetBufferAllocator()));
}

tl::expected<void, ErrorCode> RealClient::put_parts_internal(
    const std::string& key, std::vector<std::span<const char>> values,
    const std::optional<WriteConfig>& config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator,
    WriteOperation operation) {
    auto operation_result = ValidateWriteOperation(operation, config);
    if (!operation_result) {
        LOG(ERROR) << "the write config is invalid";
        return tl::unexpected(operation_result.error());
    }
    auto config_result = ValidateWriteEntryConfig(config);
    if (!config_result) {
        LOG(ERROR) << "the write config is invalid";
        return tl::unexpected(config_result.error());
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
    const std::optional<WriteConfig>& config, const UUID& client_id,
    WriteOperation operation) {
    auto context_ptr = find_shm_context(client_id);
    if (!context_ptr) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& context = *context_ptr;
    SharedMutexLocker lock(&context.mutex_, shared_lock);
    return put_parts_internal(key, values, config,
                              context.client_buffer_allocator_, operation);
}

int RealClient::put_parts(const std::string& key,
                          std::vector<std::span<const char>> values,
                          const std::optional<WriteConfig>& config,
                          WriteOperation operation) {
    return to_py_ret(put_parts_internal(key, values, config,
                                        GetBufferAllocator(), operation));
}

tl::expected<void, ErrorCode> RealClient::remove_internal(
    const std::string& key, bool force) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto remove_result = client_service_->Remove(key, force);
    if (!remove_result) {
        LOG(ERROR) << "Remove failed, key=" << key
                   << ", error=" << remove_result.error();
        return tl::unexpected(remove_result.error());
    }
    return {};
}

int RealClient::remove(const std::string& key, bool force) {
    return to_py_ret(remove_internal(key, force));
}

tl::expected<long, ErrorCode> RealClient::removeByRegex_internal(
    const std::string& str, bool force) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = client_service_->RemoveByRegex(str, force);
    if (!result) {
        LOG(ERROR) << "removeByRegex_internal failed"
                   << ", error=" << result.error();
    }
    return result;
}

long RealClient::removeByRegex(const std::string& str, bool force) {
    return to_py_ret(removeByRegex_internal(str, force));
}

tl::expected<int64_t, ErrorCode> RealClient::removeAll_internal(bool force) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = client_service_->RemoveAll(force);
    if (!result) {
        LOG(ERROR) << "removeAll_internal failed"
                   << ", error=" << result.error();
    }
    return result;
}

long RealClient::removeAll(bool force) {
    return to_py_ret(removeAll_internal(force));
}

long RealClient::removeAllLocal() {
    return to_py_ret(removeAllLocal_internal());
}

int RealClient::removeLocal(const std::string& key) {
    return to_py_ret(removeLocal_internal(key));
}

tl::expected<bool, ErrorCode> RealClient::isExist_internal(
    const std::string& key) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = client_service_->IsExist(key);
    if (!result) {
        LOG(ERROR) << "isExist_internal failed" << ", key=" << key
                   << ", error=" << result.error();
    }
    return result;
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

int64_t RealClient::getSize(const std::string& key) {
    return to_py_ret(getSize_internal(key));
}

tl::expected<void, ErrorCode> RealClient::map_shm_internal(
    int fd, uint64_t dummy_base_addr, size_t shm_size, bool is_local_buffer,
    const UUID& client_id) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "map_shm_internal failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        if (fd >= 0 && close(fd) != 0) {
            LOG(ERROR) << "Failed to close rejected SHM descriptor: "
                       << strerror(errno);
        }
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    std::stringstream addr_stream;
    addr_stream << "0x" << std::hex << dummy_base_addr;

    std::string shm_name =
        std::string(MOONCAKE_SHM_NAME) + "_" + std::to_string(client_id.first) +
        "_" + std::to_string(client_id.second) + "_" + addr_stream.str();

    // Skip the mmap/registration below if this segment is already mapped.
    if (auto context_ptr = find_shm_context(client_id)) {
        auto& context = *context_ptr;
        SharedMutexLocker lock(&context.mutex_, shared_lock);
        for (const auto& shm : context.mapped_shms_) {
            if (shm.dummy_base_addr_ ==
                static_cast<uintptr_t>(dummy_base_addr)) {
                LOG(WARNING)
                    << "Segment already mapped: " << shm_name
                    << ", client_id=" << client_id << ", dummy_base_addr=0x"
                    << std::hex << dummy_base_addr << std::dec
                    << ", shm_size=" << shm.shm_size_;
                if (fd >= 0) {
                    close(fd);
                }
                return {};
            }
        }
    }

    if (fd < 0) {
        LOG(ERROR) << "Invalid file descriptor: " << fd
                   << ", client_id=" << client_id << ", name=" << shm_name;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Map shared memory from FD
    void* shm_buffer =
        mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (shm_buffer == MAP_FAILED) {
        LOG(ERROR) << "Failed to map shared memory from fd: " << fd
                   << ", name: " << shm_name << ", client_id=" << client_id
                   << ", error: " << strerror(errno);
        close(fd);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    close(fd);  // Close FD after mapping

    MappedShm shm;
    shm.shm_name_ = shm_name;
    shm.shm_buffer_ = shm_buffer;
    shm.shm_size_ = shm_size;
    shm.dummy_base_addr_ = static_cast<uintptr_t>(dummy_base_addr);
    shm.shm_addr_offset_ = reinterpret_cast<uintptr_t>(shm_buffer) -
                           reinterpret_cast<uintptr_t>(dummy_base_addr);

    // Build the (potentially expensive) allocator BEFORE taking the global
    // lock.
    std::shared_ptr<ClientBufferAllocator> new_allocator;
    if (is_local_buffer) {
        auto allocator = ClientBufferAllocator::create(shm_buffer, shm_size,
                                                       this->protocol_);
        if (!allocator) {
            LOG(ERROR) << "Failed to create SHM allocator: "
                       << allocator.error();
            if (munmap(shm_buffer, shm_size) != 0) {
                LOG(ERROR) << "Failed to unmap SHM after allocation failure: "
                           << strerror(errno);
            }
            return tl::unexpected(allocator.error());
        }
        new_allocator = std::move(*allocator);
    }

    auto registration = client_service_->RegisterLocalMemory(
        shm_buffer, shm_size, kWildcardLocation, false, true);
    if (!registration) {
        LOG(ERROR) << "Failed to register SHM: " << registration.error();
        if (munmap(shm_buffer, shm_size) != 0) {
            LOG(ERROR) << "Failed to unmap SHM after registration failure: "
                       << strerror(errno);
        }
        return tl::unexpected(registration.error());
    }

    {
        SharedMutexLocker map_lock(&dummy_client_mutex_);
        auto& slot = shm_contexts_[client_id];
        if (!slot) {
            slot = std::make_shared<ShmContext>();
        }
        ShmContext& context = *slot;
        SharedMutexLocker ctx_lock(&context.mutex_);
        if (is_local_buffer) {
            if (context.client_buffer_allocator_) {
                // A client may map at most one local buffer; roll back ours.
                LOG(ERROR)
                    << "A local buffer is already mapped for this client "
                       "shared memory, client_id="
                    << client_id;
                if (shm_size > 0) {
                    client_service_->unregisterLocalMemory(shm_buffer, true);
                }
                if (munmap(shm_buffer, shm_size) != 0) {
                    LOG(ERROR) << "Failed to unmap duplicate local SHM: "
                               << strerror(errno);
                }
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
            context.client_buffer_allocator_ = std::move(new_allocator);
        }
        context.mapped_shms_.push_back(std::move(shm));

        LOG(INFO) << "Mapped new shared memory: " << shm_name
                  << ", client_id=" << client_id << ", dummy_base_addr=0x"
                  << std::hex << dummy_base_addr << std::dec
                  << ", shm_buffer=" << shm_buffer << ", shm_size=" << shm_size;
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::unmap_shm_internal(
    const UUID& client_id) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "unmap_shm_internal failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    std::shared_ptr<ShmContext> context_ptr;
    {
        SharedMutexLocker lock(&dummy_client_mutex_);
        auto it = shm_contexts_.find(client_id);
        if (it == shm_contexts_.end()) {
            LOG(INFO) << "client_id=" << client_id << ", shm already unmapped";
            return {};
        }
        context_ptr = it->second;
        shm_contexts_.erase(it);
    }

    ShmContext& context = *context_ptr;
    SharedMutexLocker lock(&context.mutex_);
    context.client_buffer_allocator_.reset();

    bool had_error = false;
    for (auto& shm : context.mapped_shms_) {
        if (!shm.shm_buffer_) {
            continue;
        }
        if (shm.shm_size_ > 0) {
            auto rc =
                client_service_->unregisterLocalMemory(shm.shm_buffer_, true);
            if (!rc) {
                LOG(WARNING) << "Failed to unregister memory for "
                             << shm.shm_name_ << ", client_id=" << client_id
                             << ", error=" << toString(rc.error())
                             << ", proceeding with cleanup";
                had_error = true;
            }
        }
        if (munmap(shm.shm_buffer_, shm.shm_size_) != 0) {
            LOG(WARNING) << "munmap failed for " << shm.shm_name_
                         << ", client_id=" << client_id << ": "
                         << strerror(errno);
            had_error = true;
        } else {
            LOG(INFO) << "Unmapped shared memory: " << shm.shm_name_
                      << ", client_id=" << client_id << ", dummy_base_addr=0x"
                      << std::hex << shm.dummy_base_addr_ << std::dec
                      << ", shm_buffer=" << shm.shm_buffer_
                      << ", shm_size=" << shm.shm_size_;
        }
    }
    context.mapped_shms_.clear();
    if (had_error) {
        LOG(ERROR) << "Failed to unmap shared memory for client_id="
                   << client_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::unregister_shm_buffer_internal(
    uint64_t dummy_base_addr, const UUID& client_id) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "unregister_shm_buffer_internal failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto context_ptr = find_shm_context(client_id);
    if (!context_ptr) {
        LOG(INFO) << "client_id=" << client_id << ", shm already unmapped";
        return {};
    }
    ShmContext& context = *context_ptr;
    SharedMutexLocker lock(
        &context.mutex_);  // exclusive: modifies mapped_shms_

    // Find the shm corresponding to this dummy address
    auto shm_it = context.mapped_shms_.end();
    for (auto sit = context.mapped_shms_.begin();
         sit != context.mapped_shms_.end(); ++sit) {
        if (dummy_base_addr == sit->dummy_base_addr_) {
            shm_it = sit;
            break;
        }
    }

    if (shm_it == context.mapped_shms_.end()) {
        std::stringstream addr_stream;
        addr_stream << "0x" << std::hex << dummy_base_addr;
        LOG(ERROR) << "Share memory not found for dummy address: "
                   << addr_stream.str() << " (client_id: " << client_id << ")";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (context.client_buffer_allocator_ &&
        context.client_buffer_allocator_->getBase() == shm_it->shm_buffer_) {
        context.client_buffer_allocator_.reset();
    }
    // Unmap and clean up the shm
    tl::expected<void, ErrorCode> cleanup_result;
    if (shm_it->shm_buffer_) {
        // Unregister from transfer engine if it was registered
        if (shm_it->shm_size_ > 0) {
            // Matching the usage in unmap_shm_internal
            auto res = client_service_->unregisterLocalMemory(
                shm_it->shm_buffer_, true);
            if (!res) {
                LOG(WARNING)
                    << "Failed to unregister local memory for shared memory: "
                    << shm_it->shm_name_
                    << ", error: " << toString(res.error());
                cleanup_result = res;
            }
        }

        if (munmap(shm_it->shm_buffer_, shm_it->shm_size_) != 0) {
            LOG(ERROR) << "Failed to munmap shared memory: "
                       << shm_it->shm_name_ << ", error: " << strerror(errno);
            cleanup_result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
        } else {
            LOG(INFO) << "Unmapped and cleaned up shared memory: "
                      << shm_it->shm_name_ << ", client_id=" << client_id
                      << ", dummy_base_addr=0x" << std::hex
                      << shm_it->dummy_base_addr_ << std::dec
                      << ", shm_buffer=" << shm_it->shm_buffer_
                      << ", shm_size=" << shm_it->shm_size_;
        }
    }

    // Remove shm from list
    context.mapped_shms_.erase(shm_it);

    return cleanup_result;
}

std::shared_ptr<BufferHandle> RealClient::get_buffer(
    const std::string& key, const std::optional<ReadConfig>& config) {
    return get_buffer_internal(key, GetBufferAllocator(), config);
}

std::tuple<uint64_t, size_t> RealClient::get_buffer_info(
    const std::string& key, const std::optional<ReadConfig>& config) {
    auto buffer_handle = get_buffer_internal(key, GetBufferAllocator(), config);
    if (!buffer_handle) {
        LOG(ERROR) << "Failed to get buffer for key: " << key;
        return std::make_tuple(0, 0);
    }
    uint64_t buffer_base = reinterpret_cast<uint64_t>(buffer_handle->ptr());
    size_t buffer_size = buffer_handle->size();
    return std::make_tuple(buffer_base, buffer_size);
}

tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>
RealClient::get_buffer_info_dummy_helper(
    const std::string& key, const std::optional<ReadConfig>& config,
    const UUID& client_id) {
    auto context_ptr = find_shm_context(client_id);
    if (!context_ptr) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    ShmContext& context = *context_ptr;
    SharedMutexLocker lock(&context.mutex_, shared_lock);

    auto buffer_handle =
        get_buffer_internal(key, context.client_buffer_allocator_, config);
    if (!buffer_handle) {
        LOG(ERROR) << "Failed to get buffer for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t buffer_base = reinterpret_cast<uint64_t>(buffer_handle->ptr());
    size_t buffer_size = buffer_handle->size();

    for (const auto& shm : context.mapped_shms_) {
        uint64_t shm_start = reinterpret_cast<uint64_t>(shm.shm_buffer_);
        uint64_t shm_end = shm_start + shm.shm_size_;

        if (buffer_base >= shm_start && buffer_base < shm_end) {
            // Convert real address to dummy address.
            return std::make_tuple(buffer_base - shm.shm_addr_offset_,
                                   buffer_size);
        }
    }

    LOG(ERROR) << "Buffer allocated at " << buffer_base
               << " not found in any shared memory for client " << client_id;
    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
}

std::vector<std::shared_ptr<BufferHandle>> RealClient::batch_get_buffer(
    const std::vector<std::string>& keys,
    const std::optional<ReadConfig>& config) {
    return batch_get_buffer_internal(keys, config);
}

tl::expected<void, ErrorCode> RealClient::register_buffer_internal(
    void* buffer, size_t size) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = client_service_->RegisterLocalMemory(
        buffer, size, kWildcardLocation, false, true);
    if (!result) {
        LOG(ERROR) << "register_buffer_internal failed"
                   << ", error=" << result.error();
    }
    return result;
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

int64_t RealClient::get_into(const std::string& key, void* buffer, size_t size,
                             const std::optional<ReadConfig>& config) {
    return to_py_ret(get_into_internal(key, buffer, size, config));
}

std::vector<int> RealClient::batch_put_from(
    const std::vector<std::string>& keys, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const std::optional<WriteConfig>& config,
    WriteOperation operation) {
    auto internal_results =
        batch_put_from_internal(keys, buffers, sizes, config, operation);
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
    const std::vector<size_t>& sizes, const std::optional<WriteConfig>& config,
    const UUID& client_id, WriteOperation operation) {
    if (keys.size() != dummy_buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << dummy_buffers.size()
                   << ", sizes=" << sizes.size();
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (keys.empty()) {
        return {};
    }
    auto context_ptr = find_shm_context(client_id);
    if (!context_ptr) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    // Hold the context lock across translation and the transfer so a concurrent
    // unmap can't munmap the segments mid-transfer.
    ShmContext& context = *context_ptr;
    SharedMutexLocker lock(&context.mutex_, shared_lock);

    std::vector<void*> buffers;
    buffers.reserve(dummy_buffers.size());
    const MappedShm* last_hit_shm = nullptr;

    for (size_t i = 0; i < dummy_buffers.size(); ++i) {
        uint64_t dummy_addr = dummy_buffers[i];
        size_t size = sizes[i];
        bool found = false;

        if (last_hit_shm && dummy_addr >= last_hit_shm->dummy_base_addr_ &&
            dummy_addr + size <=
                last_hit_shm->dummy_base_addr_ + last_hit_shm->shm_size_) {
            buffers.push_back(reinterpret_cast<void*>(
                dummy_addr + last_hit_shm->shm_addr_offset_));
            found = true;
        } else {
            for (const auto& shm : context.mapped_shms_) {
                if (dummy_addr >= shm.dummy_base_addr_ &&
                    dummy_addr + size <= shm.dummy_base_addr_ + shm.shm_size_) {
                    buffers.push_back(reinterpret_cast<void*>(
                        dummy_addr + shm.shm_addr_offset_));
                    found = true;
                    last_hit_shm = &shm;
                    break;
                }
            }
        }

        if (!found) {
            LOG(ERROR) << "Dummy buffer at " << dummy_addr << " (0x" << std::hex
                       << dummy_addr << std::dec << ", size " << size << ") "
                       << "not found in any mapped shared memory for client "
                       << client_id;
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }
    return batch_put_from_internal(keys, buffers, sizes, config, operation);
}

std::vector<tl::expected<void, ErrorCode>> RealClient::batch_put_from_internal(
    const std::vector<std::string>& keys, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const std::optional<WriteConfig>& config,
    WriteOperation operation) {
    auto operation_result = ValidateWriteOperation(operation, config);
    if (!operation_result) {
        LOG(ERROR) << "the write config is invalid";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(operation_result.error()));
    }
    auto config_result = ValidateWriteEntryConfig(config);
    if (!config_result) {
        LOG(ERROR) << "the write config is invalid";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(config_result.error()));
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
    auto results =
        client_service_->BatchPut(keys, ordered_batched_slices, config);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            LOG(ERROR) << "BatchPut failed, key=" << keys[i]
                       << ", error=" << results[i].error();
        }
    }
    return results;
}

tl::expected<void, ErrorCode> RealClient::put_from_internal(
    const std::string& key, void* buffer, size_t size,
    const std::optional<WriteConfig>& config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    auto config_result = ValidateWriteEntryConfig(config);
    if (!config_result) {
        LOG(ERROR) << "the write config is invalid";
        return tl::unexpected(config_result.error());
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
        LOG(ERROR) << "put_from_internal failed, key=" << key
                   << ", error=" << put_result.error();
        return tl::unexpected(put_result.error());
    }

    return {};
}

int RealClient::put_from(const std::string& key, void* buffer, size_t size,
                         const std::optional<WriteConfig>& config) {
    return to_py_ret(put_from_internal(key, buffer, size, config));
}

std::vector<int64_t> RealClient::batch_get_into(
    const std::vector<std::string>& keys, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const std::optional<ReadConfig>& config) {
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
    const std::vector<size_t>& sizes, const std::optional<ReadConfig>& config,
    const UUID& client_id) {
    if (keys.size() != dummy_buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << dummy_buffers.size()
                   << ", sizes=" << sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (keys.empty()) {
        return {};
    }
    auto context_ptr = find_shm_context(client_id);
    if (!context_ptr) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    // Hold the context lock across translation and the transfer so a concurrent
    // unmap can't munmap the segments mid-transfer.
    ShmContext& context = *context_ptr;
    SharedMutexLocker lock(&context.mutex_, shared_lock);

    std::vector<void*> buffers;
    buffers.reserve(dummy_buffers.size());
    const MappedShm* last_hit_shm = nullptr;

    for (size_t i = 0; i < dummy_buffers.size(); ++i) {
        uint64_t dummy_addr = dummy_buffers[i];
        size_t size = sizes[i];
        bool found = false;

        if (last_hit_shm && dummy_addr >= last_hit_shm->dummy_base_addr_ &&
            dummy_addr + size <=
                last_hit_shm->dummy_base_addr_ + last_hit_shm->shm_size_) {
            buffers.push_back(reinterpret_cast<void*>(
                dummy_addr + last_hit_shm->shm_addr_offset_));
            found = true;
        } else {
            for (const auto& shm : context.mapped_shms_) {
                if (dummy_addr >= shm.dummy_base_addr_ &&
                    dummy_addr + size <= shm.dummy_base_addr_ + shm.shm_size_) {
                    buffers.push_back(reinterpret_cast<void*>(
                        dummy_addr + shm.shm_addr_offset_));
                    found = true;
                    last_hit_shm = &shm;
                    break;
                }
            }
        }

        if (!found) {
            LOG(ERROR) << "Dummy buffer at " << dummy_addr << " (0x" << std::hex
                       << dummy_addr << std::dec << ", size " << size << ") "
                       << "not found in any mapped shared memory for client "
                       << client_id;
            return std::vector<tl::expected<int64_t, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }
    return batch_get_into_internal(keys, buffers, sizes, config);
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
    auto results = client_service_->BatchIsExist(keys);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            LOG(ERROR) << "BatchIsExist failed, key=" << keys[i]
                       << ", error=" << results[i].error();
        }
    }
    return results;
}

int RealClient::put_from_with_metadata(
    const std::string& key, void* buffer, void* metadata_buffer, size_t size,
    size_t metadata_size, const std::optional<WriteConfig>& config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    auto config_result = ValidateWriteEntryConfig(config);
    if (!config_result) {
        LOG(ERROR) << "the write config is invalid";
        return -1;
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
    const std::vector<std::vector<size_t>>& sizes,
    const std::optional<WriteConfig>& config) {
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
    const std::optional<WriteConfig>& config) {
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
    auto results = client_service_->BatchPut(keys, batched_slices, config);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            LOG(ERROR) << "BatchPut failed, key=" << keys[i]
                       << ", error=" << results[i].error();
        }
    }
    return results;
}

std::vector<int> RealClient::batch_get_into_multi_buffers(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    bool prefer_alloc_in_same_node, const std::optional<ReadConfig>& config) {
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

tl::expected<DummyHeartbeatResponse, ErrorCode> RealClient::ping(
    const UUID& client_id) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "ping failed, error=" << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    DummyHeartbeatResponse resp;
    auto context = find_shm_context(client_id);
    if (!context) {
        resp.status = DummyClientStatus::DISCONNECTION;
        resp.mapped_shm_count = 0;
        LOG(WARNING) << "receive heartbeat from a disconnected client"
                     << ", client_id=" << client_id;
        return resp;
    }
    resp.status = DummyClientStatus::HEALTH;
    {
        SharedMutexLocker lock(&context->mutex_, shared_lock);
        resp.mapped_shm_count = context->mapped_shms_.size();
    }

    // Record the heartbeat (lock-free queue) to refresh this client's TTL.
    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!dummy_client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(WARNING) << "client_id=" << client_id
                     << ", error=dummy_client_ping_queue_";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

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
            UUID client_id = {pod_client_id.first_, pod_client_id.second_};
            client_ttl[client_id] =
                now + std::chrono::seconds(dummy_client_live_ttl_sec_);
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            if (it->second < now) {
                LOG(WARNING)
                    << "client_id=" << it->first << ", action=client_expired"
                    << ", reason=heartbeat_ttl_timeout"
                    << ", ttl_sec=" << dummy_client_live_ttl_sec_
                    << ", will unmap all its shm segments";
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                ++it;
            }
        }

        // Unmap expired clients
        for (auto& client_id : expired_clients) {
            unmap_shm_internal(client_id);
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kDummyClientMonitorSleepMs));
    }
}

ErrorCode RealClient::start_dummy_client_monitor() {
    dummy_client_monitor_running_ = true;
    try {
        dummy_client_monitor_thread_ =
            std::thread(&RealClient::dummy_client_monitor_func, this);
    } catch (const std::exception& e) {
        dummy_client_monitor_running_ = false;
        LOG(ERROR) << "Failed to start Dummy monitor: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
    LOG(INFO) << "start dummy_client_monitor_thread";
    return ErrorCode::OK;
}

ErrorCode RealClient::start_ipc_server() {
    int server_sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_sock < 0) {
        LOG(ERROR) << "Failed to create IPC socket: " << strerror(errno);
        return ErrorCode::INTERNAL_ERROR;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    // Using abstract namespace, so we don't need to unlink.
    strncpy(&addr.sun_path[1], ipc_socket_path_.c_str(),
            sizeof(addr.sun_path) - 2);

    if (bind(server_sock, (struct sockaddr*)&addr,
             sizeof(sa_family_t) + strlen(&addr.sun_path[1]) + 1) < 0) {
        LOG(ERROR) << "Failed to bind IPC socket: " << strerror(errno);
        if (close(server_sock) != 0) {
            LOG(ERROR) << "Failed to close IPC listener: " << strerror(errno);
        }
        return ErrorCode::INTERNAL_ERROR;
    }
    if (listen(server_sock, 5) < 0) {
        LOG(ERROR) << "Failed to listen on IPC socket: " << strerror(errno);
        if (close(server_sock) != 0) {
            LOG(ERROR) << "Failed to close IPC listener: " << strerror(errno);
        }
        return ErrorCode::INTERNAL_ERROR;
    }

    ipc_running_ = true;
    try {
        ipc_thread_ =
            std::jthread(&RealClient::ipc_server_func, this, server_sock);
    } catch (const std::exception& e) {
        ipc_running_ = false;
        LOG(ERROR) << "Failed to start IPC worker: " << e.what();
        if (close(server_sock) != 0) {
            LOG(ERROR) << "Failed to close IPC listener: " << strerror(errno);
        }
        return ErrorCode::INTERNAL_ERROR;
    }
    LOG(INFO) << "IPC server is listening at " << ipc_socket_path_;
    return ErrorCode::OK;
}

ErrorCode RealClient::stop_ipc_server() {
    ipc_running_ = false;
    if (ipc_thread_.joinable()) {
        // Connect to self to unblock accept, as in the original IPC server.
        if (!ipc_socket_path_.empty()) {
            int sock = socket(AF_UNIX, SOCK_STREAM, 0);
            if (sock >= 0) {
                struct sockaddr_un addr;
                memset(&addr, 0, sizeof(addr));
                addr.sun_family = AF_UNIX;
                strncpy(&addr.sun_path[1], ipc_socket_path_.c_str(),
                        sizeof(addr.sun_path) - 2);
                if (connect(sock, (struct sockaddr*)&addr,
                            sizeof(sa_family_t) + strlen(&addr.sun_path[1]) +
                                1) != 0) {
                    LOG(ERROR)
                        << "Failed to wake IPC listener: " << strerror(errno);
                }
                if (close(sock) != 0) {
                    LOG(ERROR) << "Failed to close IPC wakeup socket: "
                               << strerror(errno);
                }
            } else {
                LOG(ERROR) << "Failed to create IPC wakeup socket: "
                           << strerror(errno);
            }
        }
        // Finish registrations before releasing SHM and the native object.
        try {
            ipc_thread_.join();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to join IPC worker: " << e.what();
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    return ErrorCode::OK;
}

ErrorCode RealClient::stop_dummy_client_monitor() {
    dummy_client_monitor_running_ = false;
    if (dummy_client_monitor_thread_.joinable()) {
        try {
            dummy_client_monitor_thread_.join();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to join Dummy monitor: " << e.what();
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    return ErrorCode::OK;
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

    ssize_t received = recvmsg(socket, &msg, MSG_WAITALL);
    if (received < 0) {
        LOG(ERROR) << "Failed to receive IPC registration: " << strerror(errno);
        return -1;
    }

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_level == SOL_SOCKET &&
        cmsg->cmsg_type == SCM_RIGHTS) {
        int fd;
        memcpy(&fd, CMSG_DATA(cmsg), sizeof(int));
        if (received != static_cast<ssize_t>(data_len)) {
            LOG(ERROR) << "Incomplete IPC registration: " << received << "/"
                       << data_len;
            close(fd);
            return -1;
        }
        return fd;
    }
    LOG(ERROR) << "IPC registration is missing an SCM_RIGHTS descriptor";
    return -1;
}

void RealClient::ipc_server_func(int server_sock) {
    while (ipc_running_) {
        int client_sock = accept(server_sock, nullptr, nullptr);
        if (client_sock < 0) {
            if (ipc_running_) {
                LOG(ERROR) << "Accept failed: " << strerror(errno);
            }
            continue;
        }

        if (!ipc_running_) {
            if (close(client_sock) != 0) {
                LOG(ERROR) << "Failed to close IPC connection: "
                           << strerror(errno);
            }
            break;
        }

        // Bound an incomplete request so close can join this worker.
        const timeval receive_timeout{5, 0};
        if (setsockopt(client_sock, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout,
                       sizeof(receive_timeout)) != 0) {
            LOG(ERROR) << "Failed to set IPC receive timeout: "
                       << strerror(errno);
            if (close(client_sock) != 0) {
                LOG(ERROR) << "Failed to close IPC connection: "
                           << strerror(errno);
            }
            continue;
        }

        ShmRegisterRequest req{};
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
        if (send(client_sock, &status, sizeof(status), MSG_NOSIGNAL) !=
            static_cast<ssize_t>(sizeof(status))) {
            LOG(ERROR) << "Failed to send response to client";
        }
        if (close(client_sock) != 0) {
            LOG(ERROR) << "Failed to close IPC connection: " << strerror(errno);
        }
    }

    if (close(server_sock) != 0) {
        LOG(ERROR) << "Failed to close IPC listener: " << strerror(errno);
    }
    LOG(INFO) << "IPC server stopped";
}

}  // namespace mooncake
