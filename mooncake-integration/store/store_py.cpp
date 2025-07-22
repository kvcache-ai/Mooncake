#include "store_py.h"

#include <netinet/in.h>
#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>  // for atexit
#include <random>

#include "client_buffer.hpp"
#include "types.h"
#include "utils.h"
#include "config.h"

namespace py = pybind11;

namespace mooncake {

// ResourceTracker implementation using singleton pattern
ResourceTracker &ResourceTracker::getInstance() {
    static ResourceTracker instance;
    return instance;
}

ResourceTracker::ResourceTracker() {
    // Set up signal handlers
    struct sigaction sa;
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    // Register for common termination signals
    sigaction(SIGINT, &sa, nullptr);   // Ctrl+C
    sigaction(SIGTERM, &sa, nullptr);  // kill command
    sigaction(SIGHUP, &sa, nullptr);   // Terminal closed

    // Register exit handler
    std::atexit(exitHandler);
}

ResourceTracker::~ResourceTracker() {
    // Cleanup is handled by exitHandler
}

void ResourceTracker::registerInstance(DistributedObjectStore *instance) {
    std::lock_guard<std::mutex> lock(mutex_);
    instances_.insert(instance);
}

void ResourceTracker::unregisterInstance(DistributedObjectStore *instance) {
    std::lock_guard<std::mutex> lock(mutex_);
    instances_.erase(instance);
}

void ResourceTracker::cleanupAllResources() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Perform cleanup outside the lock to avoid potential deadlocks
    for (void *instance : instances_) {
        DistributedObjectStore *store =
            static_cast<DistributedObjectStore *>(instance);
        if (store) {
            LOG(INFO) << "Cleaning up DistributedObjectStore instance";
            store->tearDownAll();
        }
    }
}

void ResourceTracker::signalHandler(int signal) {
    LOG(INFO) << "Received signal " << signal << ", cleaning up resources";
    getInstance().cleanupAllResources();

    // Re-raise the signal with default handler to allow normal termination
    struct sigaction sa;
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(signal, &sa, nullptr);
    raise(signal);
}

void ResourceTracker::exitHandler() { getInstance().cleanupAllResources(); }

static bool isPortAvailable(int port) {
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

// Get a random available port between min_port and max_port
static int getRandomAvailablePort(int min_port = 12300, int max_port = 14300) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(min_port, max_port);

    for (int attempts = 0; attempts < 10; attempts++) {
        int port = dis(gen);
        if (isPortAvailable(port)) {
            return port;
        }
    }
    return -1;  // Failed to find available port
}

DistributedObjectStore::DistributedObjectStore() {
    // Register this instance with the global tracker
    easylog::set_min_severity(easylog::Severity::WARN);
    ResourceTracker::getInstance().registerInstance(this);
}

DistributedObjectStore::~DistributedObjectStore() {
    // Unregister from the tracker before cleanup
    ResourceTracker::getInstance().unregisterInstance(this);
}

int DistributedObjectStore::setup(const std::string &local_hostname,
                                  const std::string &metadata_server,
                                  size_t global_segment_size,
                                  size_t local_buffer_size,
                                  const std::string &protocol,
                                  const std::string &rdma_devices,
                                  const std::string &master_server_addr) {
    this->protocol = protocol;

    // Remove port if hostname already contains one
    std::string hostname = local_hostname;
    size_t colon_pos = hostname.find(":");
    if (colon_pos == std::string::npos) {
        // Get a random available port
        int port = getRandomAvailablePort();
        if (port < 0) {
            LOG(ERROR) << "Failed to find available port";
            return 1;
        }
        // Combine hostname with port
        this->local_hostname = hostname + ":" + std::to_string(port);
    } else {
        this->local_hostname = local_hostname;
    }

    void **args = (protocol == "rdma") ? rdma_args(rdma_devices) : nullptr;
    auto client_opt =
        mooncake::Client::Create(this->local_hostname, metadata_server,
                                 protocol, args, master_server_addr);
    if (!client_opt) {
        LOG(ERROR) << "Failed to create client";
        return 1;
    }
    client_ = *client_opt;

    client_buffer_allocator_ = ClientBufferAllocator::create(local_buffer_size);
    auto result = client_->RegisterLocalMemory(
        client_buffer_allocator_->getBase(), local_buffer_size,
        kWildcardLocation, false, true);
    if (!result.has_value()) {
        LOG(ERROR) << "Failed to register local memory: "
                   << toString(result.error());
        return 1;
    }

    // If global_segment_size is 0, skip mount segment;
    // If global_segment_size is larger than max_mr_size, split to multiple
    // segments.
    auto max_mr_size = globalConfig().max_mr_size; // Max segment size
    uint64_t total_glbseg_size = global_segment_size;  // For logging
    uint64_t current_glbseg_size = 0;                  // For logging
    while (global_segment_size > 0) {
        size_t segment_size = std::min(global_segment_size, max_mr_size);
        global_segment_size -= segment_size;
        current_glbseg_size += segment_size;
        LOG(INFO) << "Mounting segment: " << segment_size << " bytes, "
                  << current_glbseg_size << " of " << total_glbseg_size;
        void *ptr = allocate_buffer_allocator_memory(segment_size);
        if (!ptr) {
            LOG(ERROR) << "Failed to allocate segment memory";
            return 1;
        }
        segment_ptrs_.emplace_back(ptr);
        auto mount_result = client_->MountSegment(ptr, segment_size);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount segment: "
                       << toString(mount_result.error());
            return 1;
        }
    }

    return 0;
}

int DistributedObjectStore::initAll(const std::string &protocol_,
                                    const std::string &device_name,
                                    size_t mount_segment_size) {
    if (client_) {
        LOG(ERROR) << "Client is already initialized";
        return 1;
    }
    uint64_t buffer_allocator_size = 1024 * 1024 * 1024;
    return setup("localhost:12345", "127.0.0.1:2379", mount_segment_size,
                 buffer_allocator_size, protocol_, device_name);
}

int DistributedObjectStore::tearDownAll() {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    // Reset all resources
    client_.reset();
    client_buffer_allocator_.reset();
    segment_ptrs_.clear();
    local_hostname = "";
    device_name = "";
    protocol = "";
    return 0;
}

std::vector<Slice> split_into_slices(BufferHandle &handle) {
    std::vector<Slice> slices;
    auto base = static_cast<uint8_t *>(handle.ptr());
    size_t offset = 0;

    while (offset < handle.size()) {
        size_t chunk_size = std::min(handle.size() - offset, kMaxSliceSize);
        slices.push_back({base + offset, chunk_size});
        offset += chunk_size;
    }
    return slices;
}

int DistributedObjectStore::put(const std::string &key,
                                std::span<const char> value,
                                const ReplicateConfig &config) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    auto alloc_result = client_buffer_allocator_->allocate(value.size_bytes());
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for put operation, key: "
                   << key << ", value size: " << value.size();
        return 1;
    }
    auto &buffer_handle = *alloc_result;
    memcpy(buffer_handle.ptr(), value.data(), value.size_bytes());

    std::vector<Slice> slices = split_into_slices(buffer_handle);

    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return toInt(put_result.error());
    }

    return 0;
}

int DistributedObjectStore::put_batch(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    if (keys.size() != values.size()) {
        LOG(ERROR) << "Key and value size mismatch";
        return 1;
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
            return 1;
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
            return 1;
        }
    }

    auto results = client_->BatchPut(keys, ordered_batched_slices, config);

    // Check if any operations failed
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            LOG(ERROR) << "BatchPut operation failed for key '" << keys[i]
                       << "' with error: " << toString(results[i].error());
            return toInt(results[i].error());
        }
    }
    return 0;
}

int DistributedObjectStore::put_parts(const std::string &key,
                                      std::vector<std::span<const char>> values,
                                      const ReplicateConfig &config) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }

    // Calculate total size needed
    size_t total_size = 0;
    for (const auto &value : values) {
        total_size += value.size_bytes();
    }

    if (total_size == 0) {
        LOG(WARNING) << "Attempting to put empty data for key: " << key;
        return 0;
    }

    // Allocate buffer using the new allocator
    auto alloc_result = client_buffer_allocator_->allocate(total_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for put_parts operation, key: "
                   << key << ", total size: " << total_size;
        return 1;
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
        return toInt(put_result.error());
    }

    return 0;
}

pybind11::bytes DistributedObjectStore::get(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return pybind11::bytes("\0", 0);
    }

    const auto kNullString = pybind11::bytes("\0", 0);

    {
        py::gil_scoped_release release_gil;

        auto query_result = client_->Query(key);
        if (!query_result) {
            py::gil_scoped_acquire acquire_gil;
            return kNullString;
        }

        auto replica_list = query_result.value();
        if (replica_list.empty()) {
            py::gil_scoped_acquire acquire_gil;
            return kNullString;
        }

        // Calculate total size from memory descriptors (assuming memory
        // replica)
        uint64_t total_size = 0;
        auto &memory_descriptors = replica_list[0].get_memory_descriptor();
        for (auto &handle : memory_descriptors.buffer_descriptors) {
            total_size += handle.size_;
        }

        if (total_size == 0) {
            py::gil_scoped_acquire acquire_gil;
            return pybind11::bytes("", 0);
        }

        // Allocate buffer using the new allocator
        auto alloc_result = client_buffer_allocator_->allocate(total_size);
        if (!alloc_result) {
            py::gil_scoped_acquire acquire_gil;
            LOG(ERROR) << "Failed to allocate buffer for get operation, key: "
                       << key << ", size: " << total_size;
            return kNullString;
        }

        auto &buffer_handle = *alloc_result;

        // Create slices for the allocated buffer based on memory descriptors
        std::vector<Slice> slices;
        uint64_t offset = 0;

        for (auto &handle : memory_descriptors.buffer_descriptors) {
            void *chunk_ptr = static_cast<char *>(buffer_handle.ptr()) + offset;
            slices.emplace_back(Slice{chunk_ptr, handle.size_});
            offset += handle.size_;
        }

        // Get the object data
        auto get_result = client_->Get(key, replica_list, slices);
        if (!get_result) {
            py::gil_scoped_acquire acquire_gil;
            LOG(ERROR) << "Get operation failed with error: "
                       << toString(get_result.error());
            return kNullString;
        }

        py::gil_scoped_acquire acquire_gil;

        // Create Python bytes object - buffer_handle will be released
        // automatically
        return pybind11::bytes(static_cast<char *>(buffer_handle.ptr()),
                               total_size);
    }
}

std::vector<pybind11::bytes> DistributedObjectStore::get_batch(
    const std::vector<std::string> &keys) {
    const auto kNullString = pybind11::bytes("\0", 0);
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        py::gil_scoped_acquire acquire_gil;
        return {kNullString};
    }

    std::unordered_set<std::string> seen;
    for (const auto &key : keys) {
        if (!seen.insert(key).second) {
            LOG(ERROR) << "Duplicate key not supported for Batch API, key: "
                       << key;
            py::gil_scoped_acquire acquire_gil;
            return {kNullString};
        }
    }

    {
        py::gil_scoped_release release_gil;
        auto query_results = client_->BatchQuery(keys);

        // Extract successful replica lists
        std::vector<std::vector<mooncake::Replica::Descriptor>> replica_lists;
        replica_lists.reserve(keys.size());
        for (size_t i = 0; i < query_results.size(); ++i) {
            if (!query_results[i]) {
                py::gil_scoped_acquire acquire_gil;
                LOG(ERROR) << "Query failed for key '" << keys[i]
                           << "': " << toString(query_results[i].error());
                return {kNullString};
            }
            replica_lists.emplace_back(query_results[i].value());
        }

        // Prepare buffers and slices for each key
        std::vector<std::unique_ptr<BufferHandle>> buffer_handles;
        std::vector<std::vector<Slice>> all_slices;
        std::vector<uint64_t> total_sizes;

        buffer_handles.reserve(keys.size());
        all_slices.reserve(keys.size());
        total_sizes.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            const auto &replica_list = replica_lists[i];
            if (replica_list.empty()) {
                py::gil_scoped_acquire acquire_gil;
                LOG(ERROR) << "Empty replica list for key: " << keys[i];
                return {kNullString};
            }

            // Calculate total size (memory replica assumption)
            uint64_t total_size = 0;
            auto &memory_descriptors = replica_list[0].get_memory_descriptor();
            for (auto &handle : memory_descriptors.buffer_descriptors) {
                total_size += handle.size_;
            }

            // Allocate buffer
            auto alloc_result = client_buffer_allocator_->allocate(total_size);
            if (!alloc_result) {
                py::gil_scoped_acquire acquire_gil;
                LOG(ERROR) << "Failed to allocate buffer for key: " << keys[i];
                return {kNullString};
            }

            auto &buffer_handle = *alloc_result;

            // Create slices
            std::vector<Slice> slices;
            uint64_t offset = 0;
            for (auto &handle : memory_descriptors.buffer_descriptors) {
                void *chunk_ptr =
                    static_cast<char *>(buffer_handle.ptr()) + offset;
                slices.emplace_back(Slice{chunk_ptr, handle.size_});
                offset += handle.size_;
            }

            buffer_handles.emplace_back(
                std::make_unique<BufferHandle>(std::move(buffer_handle)));
            all_slices.emplace_back(std::move(slices));
            total_sizes.emplace_back(total_size);
        }

        // Prepare batch transfer data structures
        std::vector<std::string> batch_keys = keys;
        std::unordered_map<std::string, std::vector<Slice>> batch_slices;

        for (size_t i = 0; i < keys.size(); ++i) {
            batch_slices[keys[i]] = all_slices[i];
        }

        // Execute batch transfer
        auto batch_get_results =
            client_->BatchGet(batch_keys, replica_lists, batch_slices);

        py::gil_scoped_acquire acquire_gil;
        std::vector<pybind11::bytes> results;
        results.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            if (!batch_get_results[i]) {
                LOG(ERROR) << "BatchGet failed for key '" << keys[i]
                           << "': " << toString(batch_get_results[i].error());
                return {kNullString};
            }

            // Create Python bytes object from buffer
            results.emplace_back(pybind11::bytes(
                static_cast<char *>(buffer_handles[i]->ptr()), total_sizes[i]));
        }

        return results;
    }
}

int DistributedObjectStore::remove(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    auto remove_result = client_->Remove(key);
    if (!remove_result) return toInt(remove_result.error());
    return 0;
}

long DistributedObjectStore::removeAll() {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }
    auto result = client_->RemoveAll();
    if (!result) {
        LOG(ERROR) << "RemoveAll failed: " << result.error();
        return -1;
    }
    return result.value();
}

int DistributedObjectStore::isExist(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }
    auto exist_result = client_->IsExist(key);
    if (!exist_result) {
        if (exist_result.error() == ErrorCode::OBJECT_NOT_FOUND)
            return 0;                        // No
        return toInt(exist_result.error());  // Error
    }
    return exist_result.value() ? 1 : 0;  // Yes/No
}

std::vector<int> DistributedObjectStore::batchIsExist(
    const std::vector<std::string> &keys) {
    std::vector<int> results;

    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        results.resize(keys.size(), -1);  // Fill with error codes
        return results;
    }

    if (keys.empty()) {
        LOG(WARNING) << "Empty keys vector provided to batchIsExist";
        return results;  // Return empty vector
    }

    auto batch_exist_results = client_->BatchIsExist(keys);

    results.resize(keys.size());

    // Convert tl::expected results to int results
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!batch_exist_results[i]) {
            if (batch_exist_results[i].error() == ErrorCode::OBJECT_NOT_FOUND) {
                results[i] = 0;  // Does not exist
            } else {
                results[i] = toInt(batch_exist_results[i].error());  // Error
            }
        } else {
            results[i] =
                batch_exist_results[i].value() ? 1 : 0;  // Exists/Not exists
        }
    }

    return results;
}

int64_t DistributedObjectStore::getSize(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }

    auto query_result = client_->Query(key);

    if (!query_result) {
        return toInt(query_result.error());
    }

    auto replica_list = query_result.value();

    // Calculate total size from all replicas' handles
    int64_t total_size = 0;
    if (!replica_list.empty()) {
        auto &replica = replica_list[0];
        if (replica.is_memory_replica() == false) {
            auto &disk_descriptor = replica.get_disk_descriptor();
            total_size = disk_descriptor.file_size;
        } else {
            auto &memory_descriptors = replica.get_memory_descriptor();
            for (auto &handle : memory_descriptors.buffer_descriptors) {
                total_size += handle.size_;
            }
        }
    } else {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return -1;  // Internal error
    }

    return total_size;
}

// SliceBuffer implementation
SliceBuffer::SliceBuffer(BufferHandle handle) : handle_(std::move(handle)) {}

void *SliceBuffer::ptr() const { return handle_.ptr(); }

uint64_t SliceBuffer::size() const { return handle_.size(); }

// Implementation of get_buffer method
std::shared_ptr<SliceBuffer> DistributedObjectStore::get_buffer(
    const std::string &key) {
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

    auto replica_list = query_result.value();
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return nullptr;
    }

    // Calculate total size (memory replica assumption)
    uint64_t total_length = 0;
    auto &memory_descriptors = replica_list[0].get_memory_descriptor();
    for (auto &handle : memory_descriptors.buffer_descriptors) {
        total_length += handle.size_;
    }

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
    uint64_t offset = 0;
    for (auto &handle : memory_descriptors.buffer_descriptors) {
        void *chunk_ptr = static_cast<char *>(buffer_handle.ptr()) + offset;
        slices.emplace_back(Slice{chunk_ptr, handle.size_});
        offset += handle.size_;
    }

    // Get the object data
    auto get_result = client_->Get(key, replica_list, slices);
    if (!get_result) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(get_result.error());
        return nullptr;
    }

    // Create SliceBuffer with the allocated memory
    // The buffer will be managed by the SliceBuffer's shared_ptr
    return std::make_shared<SliceBuffer>(std::move(buffer_handle));
}

int DistributedObjectStore::register_buffer(void *buffer, size_t size) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    auto register_result = client_->RegisterLocalMemory(
        buffer, size, kWildcardLocation, false, true);
    if (!register_result) {
        LOG(ERROR) << "Register buffer failed with error: "
                   << toString(register_result.error());
        return toInt(register_result.error());
    }
    return 0;
}

int DistributedObjectStore::unregister_buffer(void *buffer) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    auto unregister_result = client_->unregisterLocalMemory(buffer, true);
    if (!unregister_result) {
        LOG(ERROR) << "Unregister buffer failed with error: "
                   << toString(unregister_result.error());
        return toInt(unregister_result.error());
    }
    return 0;
}

int DistributedObjectStore::get_into(const std::string &key, void *buffer,
                                     size_t size) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }

    // Step 1: Get object info
    auto query_result = client_->Query(key);
    if (!query_result) {
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND) {
            VLOG(1) << "Object not found for key: " << key;
            return -toInt(query_result.error());
        }
        LOG(ERROR) << "Query failed for key: " << key
                   << " with error: " << toString(query_result.error());
        return -toInt(query_result.error());
    }

    auto replica_list = query_result.value();

    // Calculate total size from replica list
    uint64_t total_size = 0;
    if (replica_list.empty()) {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return -1;
    }

    auto &replica = replica_list[0];
    if (replica.is_memory_replica() == false) {
        auto &disk_descriptor = replica.get_disk_descriptor();
        total_size = disk_descriptor.file_size;
    } else {
        for (auto &handle :
             replica.get_memory_descriptor().buffer_descriptors) {
            total_size += handle.size_;
        }
    }

    // Check if user buffer is large enough
    if (size < total_size) {
        LOG(ERROR) << "User buffer too small. Required: " << total_size
                   << ", provided: " << size;
        return -1;
    }

    // Step 2: Split user buffer according to object info and create slices
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
    auto get_result = client_->Get(key, replica_list, slices);
    if (!get_result) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(get_result.error());
        return -toInt(get_result.error());
    }

    return static_cast<int>(total_size);
}

std::string DistributedObjectStore::get_hostname() const {
    return local_hostname;
}

std::vector<int> DistributedObjectStore::batch_put_from(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<int>(keys.size(), -1);
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Mismatched sizes for keys, buffers, and sizes";
        return std::vector<int>(keys.size(), -1);
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
            return std::vector<int>(keys.size(), -1);
        }
    }

    auto batch_put_results =
        client_->BatchPut(keys, ordered_batched_slices, config);

    std::vector<int> results(keys.size());

    // Check if any operations failed
    for (size_t i = 0; i < batch_put_results.size(); ++i) {
        if (!batch_put_results[i]) {
            LOG(ERROR) << "BatchPut operation failed for key '" << keys[i]
                       << "' with error: "
                       << toString(batch_put_results[i].error());
            results[i] = -toInt(batch_put_results[i].error());
        } else {
            results[i] = 0;
        }
    }

    return results;
}

std::vector<int> DistributedObjectStore::batch_get_into(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes) {
    // Validate preconditions
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<int>(keys.size(), -1);
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << buffers.size()
                   << ", sizes=" << sizes.size();
        return std::vector<int>(keys.size(), -1);
    }

    const size_t num_keys = keys.size();
    std::vector<int> results(num_keys, -1);

    if (num_keys == 0) {
        return results;
    }

    // Query metadata for all keys
    const auto query_results = client_->BatchQuery(keys);

    // Process each key individually and prepare for batch transfer
    struct ValidKeyInfo {
        std::string key;
        size_t original_index;
        std::vector<Replica::Descriptor> replica_list;
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
            results[i] = (error == ErrorCode::OBJECT_NOT_FOUND)
                             ? -toInt(ErrorCode::OBJECT_NOT_FOUND)
                             : -toInt(error);

            if (error != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(error);
            }
            continue;
        }

        // Validate replica list
        auto replica_list = query_results[i].value();
        if (replica_list.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            results[i] = -1;
            // TODO: We could early return here for prefix match case
            continue;
        }

        // Calculate required buffer size
        const auto &replica = replica_list[0];
        uint64_t total_size = 0;
        if (replica.is_memory_replica() == false) {
            auto &disk_descriptor = replica.get_disk_descriptor();
            total_size = disk_descriptor.file_size;
        } else {
            for (auto &handle :
                 replica.get_memory_descriptor().buffer_descriptors) {
                total_size += handle.size_;
            }
        }

        // Validate buffer capacity
        if (sizes[i] < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << key
                       << "': required=" << total_size
                       << ", available=" << sizes[i];
            results[i] = -1;
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
        valid_operations.push_back({.key = key,
                                    .original_index = i,
                                    .replica_list = std::move(replica_list),
                                    .slices = std::move(key_slices),
                                    .total_size = total_size});

        // Set success result (actual bytes transferred)
        results[i] = static_cast<int>(total_size);
    }

    // Early return if no valid operations
    if (valid_operations.empty()) {
        return results;
    }

    // Prepare batch transfer data structures
    std::vector<std::string> batch_keys;
    std::vector<std::vector<Replica::Descriptor>> batch_replica_lists;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;

    batch_keys.reserve(valid_operations.size());
    batch_replica_lists.reserve(valid_operations.size());

    for (const auto &op : valid_operations) {
        batch_keys.push_back(op.key);
        batch_replica_lists.push_back(op.replica_list);
        batch_slices[op.key] = op.slices;
    }

    // Execute batch transfer
    const auto batch_get_results =
        client_->BatchGet(batch_keys, batch_replica_lists, batch_slices);

    // Process transfer results
    for (size_t j = 0; j < batch_get_results.size(); ++j) {
        const auto &op = valid_operations[j];

        if (!batch_get_results[j]) {
            const auto error = batch_get_results[j].error();
            LOG(ERROR) << "BatchGet failed for key '" << op.key
                       << "': " << toString(error);
            results[op.original_index] = -toInt(error);
        }
    }

    return results;
}

int DistributedObjectStore::put_from(const std::string &key, void *buffer,
                                     size_t size,
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

template <typename T>
py::array create_typed_array(char *exported_data, size_t total_length) {
    py::capsule free_when_done(exported_data,
                               [](void *p) { delete[] static_cast<T *>(p); });

    return py::array_t<T>({static_cast<ssize_t>(total_length / sizeof(T))},
                          (T *)exported_data, free_when_done);
}

pybind11::object DistributedObjectStore::get_tensor(const std::string &key,
                                                    const std::string dtype) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return pybind11::none();
    }

    try {
        // Query object info first
        auto query_result = client_->Query(key);
        if (!query_result) {
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        auto replica_list = query_result.value();
        if (replica_list.empty()) {
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        // Calculate total size (memory replica assumption)
        uint64_t total_length = 0;
        auto &memory_descriptors = replica_list[0].get_memory_descriptor();
        for (auto &handle : memory_descriptors.buffer_descriptors) {
            total_length += handle.size_;
        }

        if (total_length == 0) {
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        // Allocate buffer using the new allocator
        auto alloc_result = client_buffer_allocator_->allocate(total_length);
        if (!alloc_result) {
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        auto &buffer_handle = *alloc_result;

        // Create slices for the allocated buffer
        std::vector<Slice> slices;
        uint64_t offset = 0;
        for (auto &handle : memory_descriptors.buffer_descriptors) {
            void *chunk_ptr = static_cast<char *>(buffer_handle.ptr()) + offset;
            slices.emplace_back(Slice{chunk_ptr, handle.size_});
            offset += handle.size_;
        }

        // Get the object data
        auto get_result = client_->Get(key, replica_list, slices);
        if (!get_result) {
            py::gil_scoped_acquire acquire_gil;
            LOG(ERROR) << "Get failed for key: " << key
                       << " with error: " << toString(get_result.error());
            return pybind11::none();
        }

        // Create contiguous buffer and copy data
        char *exported_data = new char[total_length];
        if (!exported_data) {
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        // Copy data from buffer to contiguous memory
        memcpy(exported_data, buffer_handle.ptr(), total_length);

        // Convert bytes to tensor using torch.from_numpy
        py::gil_scoped_acquire acquire_gil;

        py::object py_buffer =
            py::memoryview::from_memory(exported_data, total_length);
        pybind11::object np_array;
        if (dtype == "float32") {
            np_array = create_typed_array<float>(exported_data, total_length);
        } else if (dtype == "float64") {
            np_array = create_typed_array<double>(exported_data, total_length);
        } else if (dtype == "int8") {
            np_array = create_typed_array<int8_t>(exported_data, total_length);
        } else if (dtype == "uint8") {
            np_array = create_typed_array<uint8_t>(exported_data, total_length);
        } else if (dtype == "int16") {
            np_array = create_typed_array<int16_t>(exported_data, total_length);
        } else if (dtype == "uint16") {
            np_array =
                create_typed_array<uint16_t>(exported_data, total_length);
        } else if (dtype == "int32") {
            np_array = create_typed_array<int32_t>(exported_data, total_length);
        } else if (dtype == "uint32") {
            np_array =
                create_typed_array<uint32_t>(exported_data, total_length);
        } else if (dtype == "int64") {
            np_array = create_typed_array<int64_t>(exported_data, total_length);
        } else if (dtype == "uint64") {
            np_array =
                create_typed_array<uint64_t>(exported_data, total_length);
        } else if (dtype == "bool") {
            np_array = create_typed_array<bool>(exported_data, total_length);
        }

        // Create tensor from numpy array
        pybind11::object tensor = torch.attr("from_numpy")(np_array);
        return tensor;
    } catch (const pybind11::error_already_set &e) {
        LOG(ERROR) << "Failed to convert bytes to tensor: " << e.what();
        return pybind11::none();
    }
}

int DistributedObjectStore::put_tensor(const std::string &key,
                                       pybind11::object tensor) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }

    try {
        // Import torch module
        // Check if the object is a tensor
        if (!(tensor.attr("__class__")
                  .attr("__name__")
                  .cast<std::string>()
                  .find("Tensor") != std::string::npos)) {
            LOG(ERROR) << "Input is not a PyTorch tensor";
            return -1;
        }
        // Get the data pointer and size directly from the tensor
        uintptr_t data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
        size_t numel = tensor.attr("numel")().cast<size_t>();
        size_t element_size = tensor.attr("element_size")().cast<size_t>();
        size_t buffer_size = numel * element_size;

        this->register_buffer(reinterpret_cast<void *>(data_ptr), buffer_size);
        // Use put_from for direct memory access (zero-copy)
        int result = this->put_from(key, reinterpret_cast<void *>(data_ptr),
                                    buffer_size);
        this->unregister_buffer(reinterpret_cast<void *>(data_ptr));
        return result;
    } catch (const pybind11::error_already_set &e) {
        LOG(ERROR) << "Failed to access tensor data: " << e.what();
        return -1;
    }
}

PYBIND11_MODULE(store, m) {
    // Define the ReplicateConfig class
    py::class_<ReplicateConfig>(m, "ReplicateConfig")
        .def(py::init<>())
        .def_readwrite("replica_num", &ReplicateConfig::replica_num)
        .def_readwrite("with_soft_pin", &ReplicateConfig::with_soft_pin)
        .def_readwrite("preferred_segment", &ReplicateConfig::preferred_segment)
        .def("__str__", [](const ReplicateConfig &config) {
            std::ostringstream oss;
            oss << config;
            return oss.str();
        });

    // Define the SliceBuffer class
    py::class_<SliceBuffer, std::shared_ptr<SliceBuffer>>(m, "SliceBuffer",
                                                          py::buffer_protocol())
        .def("ptr",
             [](const SliceBuffer &self) {
                 // Return the pointer as an integer for Python
                 return reinterpret_cast<uintptr_t>(self.ptr());
             })
        .def("size", &SliceBuffer::size)
        .def("__len__", &SliceBuffer::size)
        .def_buffer([](SliceBuffer &self) -> py::buffer_info {
            // SliceBuffer now always contains contiguous memory
            if (self.size() > 0) {
                return py::buffer_info(
                    self.ptr(),   /* Pointer to buffer */
                    sizeof(char), /* Size of one scalar */
                    py::format_descriptor<
                        char>::format(),   /* Python struct-style
                                              format descriptor */
                    1,                     /* Number of dimensions */
                    {(size_t)self.size()}, /* Buffer dimensions */
                    {sizeof(char)} /* Strides (in bytes) for each index */
                );
            } else {
                // Empty buffer
                return py::buffer_info(
                    nullptr,      /* Pointer to buffer */
                    sizeof(char), /* Size of one scalar */
                    py::format_descriptor<
                        char>::format(), /* Python struct-style
                                            format descriptor */
                    1,                   /* Number of dimensions */
                    {0},                 /* Buffer dimensions */
                    {sizeof(char)}       /* Strides (in bytes) for each index */
                );
            }
        });

    // Define the DistributedObjectStore class
    py::class_<DistributedObjectStore>(m, "MooncakeDistributedStore")
        .def(py::init<>())
        .def("setup", &DistributedObjectStore::setup)
        .def("init_all", &DistributedObjectStore::initAll)
        .def("get", &DistributedObjectStore::get)
        .def("get_batch", &DistributedObjectStore::get_batch)
        .def("get_buffer", &DistributedObjectStore::get_buffer,
             py::call_guard<py::gil_scoped_release>(),
             py::return_value_policy::take_ownership)
        .def("remove", &DistributedObjectStore::remove,
             py::call_guard<py::gil_scoped_release>())
        .def("remove_all", &DistributedObjectStore::removeAll,
             py::call_guard<py::gil_scoped_release>())
        .def("is_exist", &DistributedObjectStore::isExist,
             py::call_guard<py::gil_scoped_release>())
        .def("batch_is_exist", &DistributedObjectStore::batchIsExist,
             py::call_guard<py::gil_scoped_release>(), py::arg("keys"),
             "Check if multiple objects exist. Returns list of results: 1 if "
             "exists, 0 if not exists, -1 if error")
        .def("close", &DistributedObjectStore::tearDownAll)
        .def("get_size", &DistributedObjectStore::getSize,
             py::call_guard<py::gil_scoped_release>())
        .def("get_tensor", &DistributedObjectStore::get_tensor, py::arg("key"),
             py::arg("dtype"), "Get a PyTorch tensor from the store")
        .def("put_tensor", &DistributedObjectStore::put_tensor, py::arg("key"),
             py::arg("tensor"), "Put a PyTorch tensor into the store")
        .def(
            "register_buffer",
            [](DistributedObjectStore &self, uintptr_t buffer_ptr,
               size_t size) {
                // Register memory buffer for RDMA operations
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                return self.register_buffer(buffer, size);
            },
            py::arg("buffer_ptr"), py::arg("size"),
            "Register a memory buffer for direct access operations")
        .def(
            "unregister_buffer",
            [](DistributedObjectStore &self, uintptr_t buffer_ptr) {
                // Unregister memory buffer
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                return self.unregister_buffer(buffer);
            },
            py::arg("buffer_ptr"),
            "Unregister a previously registered memory "
            "buffer for direct access operations")
        .def(
            "get_into",
            [](DistributedObjectStore &self, const std::string &key,
               uintptr_t buffer_ptr, size_t size) {
                // Get data directly into user-provided buffer
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                return self.get_into(key, buffer, size);
            },
            py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
            "Get object data directly into a pre-allocated buffer")
        .def(
            "batch_get_into",
            [](DistributedObjectStore &self,
               const std::vector<std::string> &keys,
               const std::vector<uintptr_t> &buffer_ptrs,
               const std::vector<size_t> &sizes) {
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release;
                return self.batch_get_into(keys, buffers, sizes);
            },
            py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
            "Get object data directly into pre-allocated buffers for multiple "
            "keys")
        .def(
            "put_from",
            [](DistributedObjectStore &self, const std::string &key,
               uintptr_t buffer_ptr, size_t size,
               const ReplicateConfig &config = ReplicateConfig{}) {
                // Put data directly from user-provided buffer
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                return self.put_from(key, buffer, size, config);
            },
            py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
            py::arg("config") = ReplicateConfig{},
            "Put object data directly from a pre-allocated buffer")
        .def(
            "batch_put_from",
            [](DistributedObjectStore &self,
               const std::vector<std::string> &keys,
               const std::vector<uintptr_t> &buffer_ptrs,
               const std::vector<size_t> &sizes,
               const ReplicateConfig &config = ReplicateConfig{}) {
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release;
                return self.batch_put_from(keys, buffers, sizes, config);
            },
            py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
            py::arg("config") = ReplicateConfig{},
            "Put object data directly from pre-allocated buffers for multiple "
            "keys")
        .def(
            "put",
            [](DistributedObjectStore &self, const std::string &key,
               py::buffer buf,
               const ReplicateConfig &config = ReplicateConfig{}) {
                py::buffer_info info = buf.request(/*writable=*/false);
                py::gil_scoped_release release;
                return self.put(
                    key,
                    std::span<const char>(static_cast<char *>(info.ptr),
                                          static_cast<size_t>(info.size)),
                    config);
            },
            py::arg("key"), py::arg("value"),
            py::arg("config") = ReplicateConfig{})
        .def(
            "put_parts",
            [](DistributedObjectStore &self, const std::string &key,
               py::args parts,
               const ReplicateConfig &config = ReplicateConfig{}) {
                // 1) Python buffer → span
                std::vector<py::buffer_info> infos;
                std::vector<std::span<const char>> spans;
                infos.reserve(parts.size());
                spans.reserve(parts.size());

                for (auto &obj : parts) {
                    py::buffer buf = py::reinterpret_borrow<py::buffer>(obj);
                    infos.emplace_back(buf.request(false));
                    const auto &info = infos.back();
                    if (info.ndim != 1 || info.itemsize != 1)
                        throw std::runtime_error(
                            "parts must be 1-D bytes-like");

                    spans.emplace_back(static_cast<const char *>(info.ptr),
                                       static_cast<size_t>(info.size));
                }

                // 2) Call C++ function
                py::gil_scoped_release unlock;
                return self.put_parts(key, spans, config);
            },
            py::arg("key"), py::arg("config") = ReplicateConfig{})
        .def(
            "put_batch",
            [](DistributedObjectStore &self,
               const std::vector<std::string> &keys,
               const std::vector<py::bytes> &py_values,
               const ReplicateConfig &config = ReplicateConfig{}) {
                std::vector<std::string> temp_values;
                temp_values.reserve(py_values.size());
                for (const auto &value : py_values) {
                    temp_values.emplace_back(value.cast<std::string>());
                }

                std::vector<std::span<const char>> spans;
                spans.reserve(temp_values.size());
                for (const auto &s : temp_values) {
                    spans.emplace_back(s.data(), s.size());
                }

                return self.put_batch(keys, spans, config);
            },
            py::arg("keys"), py::arg("values"),
            py::arg("config") = ReplicateConfig{})
        .def("get_hostname", &DistributedObjectStore::get_hostname);
}

}  // namespace mooncake
