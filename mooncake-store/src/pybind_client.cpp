#include "pybind_client.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <numa.h>
#include <pthread.h>
#include <signal.h>
#include <thread>
#include <stop_token>

#include <cstdlib>  // for atexit
#include <optional>

#include "client_buffer.hpp"
#include "config.h"
#include "mutex.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

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

PyClient::PyClient() {
    // Initialize logging severity (leave as before)
    easylog::set_min_severity(easylog::Severity::WARN);
}

PyClient::~PyClient() {
    // Ensure resources are cleaned even if not explicitly closed
    tearDownAll_internal();
}

std::shared_ptr<PyClient> PyClient::create() {
    auto sp = std::shared_ptr<PyClient>(new PyClient());
    ResourceTracker::getInstance().registerInstance(sp);
    return sp;
}

tl::expected<void, ErrorCode> PyClient::setup_internal(
    const std::string &local_hostname, const std::string &metadata_server,
    size_t global_segment_size, size_t local_buffer_size,
    const std::string &protocol, const std::string &rdma_devices,
    const std::string &master_server_addr) {
    this->protocol = protocol;

    // Remove port if hostname already contains one
    std::string hostname = local_hostname;
    size_t colon_pos = hostname.find(":");
    if (colon_pos == std::string::npos) {
        // Create port binder to hold a port
        port_binder_ = std::make_unique<AutoPortBinder>();
        int port = port_binder_->getPort();
        if (port < 0) {
            LOG(ERROR) << "Failed to bind available port";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        this->local_hostname = hostname + ":" + std::to_string(port);
    } else {
        this->local_hostname = local_hostname;
    }

    std::optional<std::string> device_name =
        (rdma_devices.empty() ? std::nullopt
                              : std::make_optional(rdma_devices));

    auto client_opt =
        mooncake::Client::Create(this->local_hostname, metadata_server,
                                 protocol, device_name, master_server_addr);
    if (!client_opt) {
        LOG(ERROR) << "Failed to create client";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    client_ = *client_opt;

    // Local_buffer_size is allowed to be 0, but we only register memory when
    // local_buffer_size > 0. Invoke ibv_reg_mr() with size=0 is UB, and may
    // fail in some rdma implementations.
    client_buffer_allocator_ =
        ClientBufferAllocator::create(local_buffer_size, this->protocol);
    if (local_buffer_size > 0) {
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
    // segments.
    auto max_mr_size = globalConfig().max_mr_size;     // Max segment size
    uint64_t total_glbseg_size = global_segment_size;  // For logging
    uint64_t current_glbseg_size = 0;                  // For logging
    while (global_segment_size > 0) {
        size_t segment_size = std::min(global_segment_size, max_mr_size);
        global_segment_size -= segment_size;
        current_glbseg_size += segment_size;
        LOG(INFO) << "Mounting segment: " << segment_size << " bytes, "
                  << current_glbseg_size << " of " << total_glbseg_size;
        void *ptr =
            allocate_buffer_allocator_memory(segment_size, this->protocol);
        if (!ptr) {
            LOG(ERROR) << "Failed to allocate segment memory";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (this->protocol == "ascend") {
            ascend_segment_ptrs_.emplace_back(ptr);
        } else {
            segment_ptrs_.emplace_back(ptr);
        }
        auto mount_result = client_->MountSegment(ptr, segment_size);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount segment: "
                       << toString(mount_result.error());
            return tl::unexpected(mount_result.error());
        }
    }
    if (total_glbseg_size == 0) {
        LOG(INFO) << "Global segment size is 0, skip mounting segment";
    }

    return {};
}

int PyClient::setup(const std::string &local_hostname,
                    const std::string &metadata_server,
                    size_t global_segment_size, size_t local_buffer_size,
                    const std::string &protocol,
                    const std::string &rdma_devices,
                    const std::string &master_server_addr) {
    return to_py_ret(setup_internal(
        local_hostname, metadata_server, global_segment_size, local_buffer_size,
        protocol, rdma_devices, master_server_addr));
}

tl::expected<void, ErrorCode> PyClient::initAll_internal(
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

int PyClient::initAll(const std::string &protocol_,
                      const std::string &device_name,
                      size_t mount_segment_size) {
    return to_py_ret(
        initAll_internal(protocol_, device_name, mount_segment_size));
}

tl::expected<void, ErrorCode> PyClient::tearDownAll_internal() {
    // Ensure cleanup executes once across destructor/close/signal paths
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
        return {};
    }
    if (!client_) {
        // Not initialized or already cleaned; treat as success for idempotence
        return {};
    }
    // Reset all resources
    client_.reset();
    client_buffer_allocator_.reset();
    port_binder_.reset();
    segment_ptrs_.clear();
    local_hostname = "";
    device_name = "";
    protocol = "";
    return {};
}

int PyClient::tearDownAll() { return to_py_ret(tearDownAll_internal()); }

tl::expected<void, ErrorCode> PyClient::put_internal(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto alloc_result = client_buffer_allocator_->allocate(value.size_bytes());
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
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return tl::unexpected(put_result.error());
    }

    return {};
}

int PyClient::put(const std::string &key, std::span<const char> value,
                  const ReplicateConfig &config) {
    return to_py_ret(put_internal(key, value, config));
}

tl::expected<void, ErrorCode> PyClient::put_batch_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (keys.size() != values.size()) {
        LOG(ERROR) << "Key and value size mismatch";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<BufferHandle> buffer_handles;
    std::unordered_map<std::string, std::vector<Slice>> batched_slices;
    batched_slices.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        auto &key = keys[i];
        auto &value = values[i];
        auto alloc_result =
            client_buffer_allocator_->allocate(value.size_bytes());
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
            LOG(ERROR) << "BatchPut operation failed for key '" << keys[i]
                       << "' with error: " << toString(results[i].error());
            return tl::unexpected(results[i].error());
        }
    }
    return {};
}

int PyClient::put_batch(const std::vector<std::string> &keys,
                        const std::vector<std::span<const char>> &values,
                        const ReplicateConfig &config) {
    return to_py_ret(put_batch_internal(keys, values, config));
}

tl::expected<void, ErrorCode> PyClient::put_parts_internal(
    const std::string &key, std::vector<std::span<const char>> values,
    const ReplicateConfig &config) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
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
    auto alloc_result = client_buffer_allocator_->allocate(total_size);
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

int PyClient::put_parts(const std::string &key,
                        std::vector<std::span<const char>> values,
                        const ReplicateConfig &config) {
    return to_py_ret(put_parts_internal(key, values, config));
}

tl::expected<void, ErrorCode> PyClient::remove_internal(
    const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto remove_result = client_->Remove(key);
    if (!remove_result) {
        return tl::unexpected(remove_result.error());
    }
    return {};
}

int PyClient::remove(const std::string &key) {
    return to_py_ret(remove_internal(key));
}

tl::expected<long, ErrorCode> PyClient::removeByRegex_internal(
    const std::string &str) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_->RemoveByRegex(str);
}

long PyClient::removeByRegex(const std::string &str) {
    return to_py_ret(removeByRegex_internal(str));
}

tl::expected<int64_t, ErrorCode> PyClient::removeAll_internal() {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_->RemoveAll();
}

long PyClient::removeAll() { return to_py_ret(removeAll_internal()); }

tl::expected<bool, ErrorCode> PyClient::isExist_internal(
    const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_->IsExist(key);
}

int PyClient::isExist(const std::string &key) {
    auto result = isExist_internal(key);

    if (result.has_value()) {
        return *result ? 1 : 0;  // 1 if exists, 0 if not
    } else {
        return toInt(result.error());
    }
}

std::vector<int> PyClient::batchIsExist(const std::vector<std::string> &keys) {
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

tl::expected<int64_t, ErrorCode> PyClient::getSize_internal(
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

int64_t PyClient::getSize(const std::string &key) {
    return to_py_ret(getSize_internal(key));
}

// Implementation of get_buffer method
std::shared_ptr<BufferHandle> PyClient::get_buffer(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return nullptr;
    }

    // Query the object info
    auto query_result = client_->Query(key);
    if (!query_result) {
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND) {
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

    const auto &replica = replica_list[0];
    uint64_t total_length = calculate_total_size(replica);

    if (total_length == 0) {
        return nullptr;
    }

    // Allocate buffer using the new allocator
    auto alloc_result = client_buffer_allocator_->allocate(total_length);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get_buffer, key: " << key;
        return nullptr;
    }

    auto &buffer_handle = *alloc_result;

    // Create slices for the allocated buffer
    std::vector<Slice> slices;
    allocateSlices(slices, replica, buffer_handle);

    // Get the object data
    auto get_result = client_->Get(key, query_result.value(), slices);
    if (!get_result) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(get_result.error());
        return nullptr;
    }

    // Create BufferHandle with the allocated memory
    // The buffer will be managed by the BufferHandle's shared_ptr
    return std::make_shared<BufferHandle>(std::move(buffer_handle));
}

// Implementation of batch_get_buffer_internal method
std::vector<std::shared_ptr<BufferHandle>> PyClient::batch_get_buffer_internal(
    const std::vector<std::string> &keys) {
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
            if (query_results[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
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
        allocateSlices(slices, replica, *buffer_handle);

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

    return final_results;
}

// Implementation of batch_get_buffer method
std::vector<std::shared_ptr<BufferHandle>> PyClient::batch_get_buffer(
    const std::vector<std::string> &keys) {
    return batch_get_buffer_internal(keys);
}

tl::expected<void, ErrorCode> PyClient::register_buffer_internal(void *buffer,
                                                                 size_t size) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_->RegisterLocalMemory(buffer, size, kWildcardLocation, false,
                                        true);
}

int PyClient::register_buffer(void *buffer, size_t size) {
    return to_py_ret(register_buffer_internal(buffer, size));
}

tl::expected<void, ErrorCode> PyClient::unregister_buffer_internal(
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

int PyClient::unregister_buffer(void *buffer) {
    return to_py_ret(unregister_buffer_internal(buffer));
}

tl::expected<int64_t, ErrorCode> PyClient::get_into_internal(
    const std::string &key, void *buffer, size_t size) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Step 1: Get object info
    auto query_result = client_->Query(key);
    if (!query_result) {
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND) {
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

    auto &replica = replica_list[0];
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
    uint64_t offset = 0;

    if (replica.is_memory_replica() == false) {
        while (offset < total_size) {
            auto chunk_size = std::min(total_size - offset, kMaxSliceSize);
            void *chunk_ptr = static_cast<char *>(buffer) + offset;
            slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }
    } else {
        for (auto &handle :
             replica.get_memory_descriptor().buffer_descriptors) {
            void *chunk_ptr = static_cast<char *>(buffer) + offset;
            slices.emplace_back(Slice{chunk_ptr, handle.size_});
            offset += handle.size_;
        }
    }

    // Step 3: Read data directly into user buffer
    auto get_result = client_->Get(key, query_result.value(), slices);
    if (!get_result) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(get_result.error());
        return tl::unexpected(get_result.error());
    }

    return static_cast<int64_t>(total_size);
}

int PyClient::get_into(const std::string &key, void *buffer, size_t size) {
    return to_py_ret(get_into_internal(key, buffer, size));
}

std::string PyClient::get_hostname() const { return local_hostname; }

std::vector<int> PyClient::batch_put_from(const std::vector<std::string> &keys,
                                          const std::vector<void *> &buffers,
                                          const std::vector<size_t> &sizes,
                                          const ReplicateConfig &config) {
    auto internal_results =
        batch_put_from_internal(keys, buffers, sizes, config);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>> PyClient::batch_put_from_internal(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config) {
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

tl::expected<void, ErrorCode> PyClient::put_from_internal(
    const std::string &key, void *buffer, size_t size,
    const ReplicateConfig &config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
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
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return tl::unexpected(put_result.error());
    }

    return {};
}

int PyClient::put_from(const std::string &key, void *buffer, size_t size,
                       const ReplicateConfig &config) {
    return to_py_ret(put_from_internal(key, buffer, size, config));
}

std::vector<int> PyClient::batch_get_into(const std::vector<std::string> &keys,
                                          const std::vector<void *> &buffers,
                                          const std::vector<size_t> &sizes) {
    auto internal_results = batch_get_into_internal(keys, buffers, sizes);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>> PyClient::batch_get_into_internal(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes) {
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

        // Validate buffer capacity
        if (sizes[i] < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << key
                       << "': required=" << total_size
                       << ", available=" << sizes[i];
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        // Create slices for this key's buffer
        std::vector<Slice> key_slices;
        uint64_t offset = 0;
        if (replica.is_memory_replica() == false) {
            while (offset < total_size) {
                auto chunk_size = std::min(total_size - offset, kMaxSliceSize);
                void *chunk_ptr = static_cast<char *>(buffers[i]) + offset;
                key_slices.emplace_back(Slice{chunk_ptr, chunk_size});
                offset += chunk_size;
            }
        } else {
            for (auto &handle :
                 replica.get_memory_descriptor().buffer_descriptors) {
                void *chunk_ptr = static_cast<char *>(buffers[i]) + offset;
                key_slices.emplace_back(Slice{chunk_ptr, handle.size_});
                offset += handle.size_;
            }
        }

        // Store operation info for batch processing
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

    for (const auto &op : valid_operations) {
        batch_keys.push_back(op.key);
        batch_query_results.push_back(op.query_result);
        batch_slices[op.key] = op.slices;
    }

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

    return results;
}

std::vector<tl::expected<bool, ErrorCode>> PyClient::batchIsExist_internal(
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

int PyClient::put_from_with_metadata(const std::string &key, void *buffer,
                                     void *metadata_buffer, size_t size,
                                     size_t metadata_size,
                                     const ReplicateConfig &config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
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

}  // namespace mooncake
