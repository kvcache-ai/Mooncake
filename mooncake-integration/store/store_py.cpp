#include "store_py.h"

#include <netinet/in.h>
#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>  // for atexit
#include <random>

#include "types.h"

namespace py = pybind11;
using namespace mooncake;

// RAII container that automatically frees slices on destruction
class SliceGuard {
   public:
    explicit SliceGuard(DistributedObjectStore &store) : store_(store) {}

    ~SliceGuard() { store_.freeSlices(slices_); }

    // Prevent copying
    SliceGuard(const SliceGuard &) = delete;
    SliceGuard &operator=(const SliceGuard &) = delete;

    // Access the underlying slices
    std::vector<Slice> &slices() { return slices_; }
    const std::vector<Slice> &slices() const { return slices_; }

   private:
    DistributedObjectStore &store_;
    std::vector<Slice> slices_;
};

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

    client_buffer_allocator_ =
        std::make_unique<SimpleAllocator>(local_buffer_size);
    ErrorCode error_code = client_->RegisterLocalMemory(
        client_buffer_allocator_->getBase(), local_buffer_size,
        kWildcardLocation, false, false);
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Failed to register local memory: "
                   << toString(error_code);
        return 1;
    }
    // Skip mount segment if global_segment_size is 0
    if (global_segment_size == 0) {
        return 0;
    }
    void *ptr = allocate_buffer_allocator_memory(global_segment_size);
    if (!ptr) {
        LOG(ERROR) << "Failed to allocate segment memory";
        return 1;
    }
    segment_ptr_.reset(ptr);
    error_code = client_->MountSegment(segment_ptr_.get(), global_segment_size);
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Failed to mount segment: " << toString(error_code);
        return 1;
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

int DistributedObjectStore::allocateSlices(std::vector<Slice> &slices,
                                           const std::string &value) {
    uint64_t offset = 0;
    while (offset < value.size()) {
        auto chunk_size = std::min(value.size() - offset, kMaxSliceSize);
        auto ptr = client_buffer_allocator_->allocate(chunk_size);
        if (!ptr) {
            return 1;  // SliceGuard will handle cleanup
        }
        memcpy(ptr, value.data() + offset, chunk_size);
        slices.emplace_back(Slice{ptr, chunk_size});
        offset += chunk_size;
    }
    return 0;
}

int DistributedObjectStore::allocateSlices(std::vector<Slice> &slices,
                                           std::span<const char> value) {
    uint64_t offset = 0;
    while (offset < value.size()) {
        auto chunk_size = std::min(value.size() - offset, kMaxSliceSize);
        auto ptr = client_buffer_allocator_->allocate(chunk_size);
        if (!ptr) {
            return 1;  // SliceGuard will handle cleanup
        }
        memcpy(ptr, value.data() + offset, chunk_size);
        slices.emplace_back(Slice{ptr, chunk_size});
        offset += chunk_size;
    }
    return 0;
}

int DistributedObjectStore::allocateSlicesPacked(
    std::vector<mooncake::Slice> &slices,
    const std::vector<std::span<const char>> &parts) {
    size_t total = 0;
    for (auto p : parts) total += p.size();

    if (total == 0) return 0;

    size_t n_slice = (total + kMaxSliceSize - 1) / kMaxSliceSize;
    slices.reserve(n_slice);

    size_t remaining = total;
    for (size_t i = 0; i < n_slice; ++i) {
        size_t sz = std::min(remaining, (size_t)kMaxSliceSize);
        void *ptr = client_buffer_allocator_->allocate(sz);
        if (!ptr) {
            return 1;  // SliceGuard will handle cleanup
        }
        slices.emplace_back(mooncake::Slice{ptr, sz});
        remaining -= sz;
    }

    size_t idx = 0;
    char *dst = static_cast<char *>(slices[0].ptr);
    size_t dst_left = slices[0].size;

    for (auto part : parts) {
        const char *src = part.data();
        size_t n = part.size();

        while (n > 0) {
            if (dst_left == 0) {
                dst = static_cast<char *>(slices[++idx].ptr);
                dst_left = slices[idx].size;
            }
            size_t chunk = std::min(n, dst_left);
            memcpy(dst, src, chunk);
            dst += chunk;
            dst_left -= chunk;
            src += chunk;
            n -= chunk;
        }
    }
    return 0;
}

int DistributedObjectStore::allocateSlices(
    std::vector<mooncake::Slice> &slices,
    const mooncake::Client::ObjectInfo &object_info, uint64_t &length) {
    length = 0;
    if (object_info.replica_list.empty()) return -1;
    auto &replica = object_info.replica_list[0];
    for (auto &handle : replica.buffer_descriptors) {
        auto chunk_size = handle.size_;
        assert(chunk_size <= kMaxSliceSize);
        auto ptr = client_buffer_allocator_->allocate(chunk_size);
        if (!ptr) {
            return 1;  // SliceGuard will handle cleanup
        }
        slices.emplace_back(Slice{ptr, chunk_size});
        length += chunk_size;
    }
    return 0;
}

int DistributedObjectStore::allocateBatchedSlices(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    std::unordered_map<std::string, std::vector<mooncake::Slice>>
        &batched_slices) {
    for (size_t i = 0; i < keys.size(); ++i) {
        uint64_t offset = 0;
        const auto &value = values[i];
        std::vector<Slice> slices;
        while (offset < value.size()) {
            auto chunk_size = std::min(value.size() - offset, kMaxSliceSize);
            auto ptr = client_buffer_allocator_->allocate(chunk_size);
            if (!ptr) {
                return 1;
            }
            memcpy(ptr, value.data() + offset, chunk_size);
            slices.emplace_back(Slice{ptr, chunk_size});
            offset += chunk_size;
        }
        batched_slices.emplace(keys[i], std::move(slices));
    }
    return 0;
}

int DistributedObjectStore::allocateBatchedSlices(
    const std::vector<std::string> &keys,
    std::unordered_map<std::string, std::vector<mooncake::Slice>>
        &batched_slices,
    const mooncake::Client::BatchObjectInfo &batched_object_info,
    std::unordered_map<std::string, uint64_t> &str_length_map) {
    if (batched_object_info.batch_replica_list.empty()) return -1;
    for (const auto &key : keys) {
        auto object_info_it = batched_object_info.batch_replica_list.find(key);
        if (object_info_it == batched_object_info.batch_replica_list.end()) {
            LOG(ERROR) << "Key not found: " << key;
            return 1;
        }
        // Get first replica
        auto &replica = object_info_it->second[0];
        uint64_t length = 0;
        for (auto &handle : replica.buffer_descriptors) {
            auto chunk_size = handle.size_;
            assert(chunk_size <= kMaxSliceSize);
            auto ptr = client_buffer_allocator_->allocate(chunk_size);
            if (!ptr) {
                return 1;
            }
            batched_slices[key].emplace_back(Slice{ptr, chunk_size});
            length += chunk_size;
        }
        str_length_map.emplace(key, length);
    }
    return 0;
}

char *DistributedObjectStore::exportSlices(
    const std::vector<mooncake::Slice> &slices, uint64_t length) {
    char *buf = new char[length + 1];
    buf[length] = '\0';
    uint64_t offset = 0;
    for (auto slice : slices) {
        memcpy(buf + offset, slice.ptr, slice.size);
        offset += slice.size;
    }
    return buf;
}

int DistributedObjectStore::freeSlices(
    const std::vector<mooncake::Slice> &slices) {
    for (auto slice : slices) {
        client_buffer_allocator_->deallocate(slice.ptr, slice.size);
    }
    return 0;
}

int DistributedObjectStore::tearDownAll() {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    // Reset all resources
    client_.reset();
    client_buffer_allocator_.reset();
    segment_ptr_.reset();
    local_hostname = "";
    device_name = "";
    protocol = "";
    return 0;
}

int DistributedObjectStore::put(const std::string &key,
                                std::span<const char> value) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    SliceGuard slices(*this);
    int ret = allocateSlices(slices.slices(), value);
    if (ret) {
        LOG(ERROR) << "Failed to allocate slices for put operation, key: "
                   << key << ", value size: " << value.size();
        return ret;
    }

    ReplicateConfig config;
    config.replica_num = 1;  // Make configurable
    config.preferred_segment = this->local_hostname;
    ErrorCode error_code = client_->Put(key, slices.slices(), config);
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(error_code);
        return toInt(error_code);
    }

    return 0;
}

int DistributedObjectStore::put_batch(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    if (keys.size() != values.size()) {
        LOG(ERROR) << "Key and value size mismatch";
    }
    std::unordered_map<std::string, std::vector<mooncake::Slice>>
        batched_slices;
    int ret = allocateBatchedSlices(keys, values, batched_slices);
    if (ret) {
        LOG(ERROR) << "Failed to allocate slices for put_batch operation";
        return ret;
    }

    ReplicateConfig config;
    config.replica_num = 1;
    ErrorCode error_code = client_->BatchPut(keys, batched_slices, config);
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "BatchPut operation failed with error: "
                   << toString(error_code);
        return toInt(error_code);
    }

    for (auto &slice : batched_slices) {
        freeSlices(slice.second);
    }
    return 0;
}

int DistributedObjectStore::put_parts(
    const std::string &key, std::vector<std::span<const char>> values) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    SliceGuard slices(*this);
    int ret = allocateSlicesPacked(slices.slices(), values);
    if (ret) {
        LOG(ERROR) << "Failed to allocate slices for put operation, key: "
                   << key << ", values size: " << values.size();
        return ret;
    }
    ReplicateConfig config;
    config.replica_num = 1;  // Make configurable
    config.preferred_segment = this->local_hostname;
    ErrorCode error_code = client_->Put(key, slices.slices(), config);
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(error_code);
        return toInt(error_code);
    }
    return 0;
}

pybind11::bytes DistributedObjectStore::get(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return pybind11::bytes("\0", 0);
    }

    mooncake::Client::ObjectInfo object_info;
    SliceGuard guard(*this);  // Use SliceGuard for RAII
    uint64_t str_length = 0;
    ErrorCode error_code;
    char *exported_str_ptr = nullptr;
    bool use_exported_str = false;

    const auto kNullString = pybind11::bytes("\0", 0);

    {
        py::gil_scoped_release release_gil;

        error_code = client_->Query(key, object_info);
        if (error_code != ErrorCode::OK) {
            py::gil_scoped_acquire acquire_gil;
            return kNullString;
        }

        int ret = allocateSlices(guard.slices(), object_info, str_length);
        if (ret) {
            py::gil_scoped_acquire acquire_gil;
            return kNullString;
        }

        error_code = client_->Get(key, object_info, guard.slices());
        if (error_code != ErrorCode::OK) {
            py::gil_scoped_acquire acquire_gil;
            return kNullString;
        }

        if (guard.slices().size() == 1 &&
            guard.slices()[0].size == str_length) {
        } else {
            exported_str_ptr = exportSlices(guard.slices(), str_length);
            if (!exported_str_ptr) {
                py::gil_scoped_acquire acquire_gil;
                return kNullString;
            }
            use_exported_str = true;
        }
    }

    py::gil_scoped_acquire acquire_gil;

    pybind11::bytes result;
    if (use_exported_str) {
        result = pybind11::bytes(exported_str_ptr, str_length);
        delete[] exported_str_ptr;
    } else if (!guard.slices().empty()) {
        result = pybind11::bytes(static_cast<char *>(guard.slices()[0].ptr),
                                 str_length);
    } else {
        result = kNullString;
    }

    return result;
}

std::vector<pybind11::bytes> DistributedObjectStore::get_batch(
    const std::vector<std::string> &keys) {
    const auto kNullString = pybind11::bytes("\0", 0);
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return {kNullString};
    }
    std::unordered_set<std::string> seen;
    for (const auto &key : keys) {
        if (!seen.insert(key).second) {
            LOG(ERROR) << "Duplicate key not supported for Batch API, key: "
                       << key;
            return {kNullString};
        }
    }

    std::vector<pybind11::bytes> results;
    mooncake::Client::BatchObjectInfo batched_object_info;
    std::unordered_map<std::string, std::vector<mooncake::Slice>>
        batched_slices;
    std::unordered_map<std::string, uint64_t> str_length_map;
    {
        py::gil_scoped_release release_gil;
        ErrorCode error_code = client_->BatchQuery(keys, batched_object_info);
        if (error_code != ErrorCode::OK) {
            py::gil_scoped_acquire acquire_gil;
            return {kNullString};
        } else {
            int ret = allocateBatchedSlices(
                keys, batched_slices, batched_object_info, str_length_map);
            if (ret) {
                py::gil_scoped_acquire acquire_gil;
                return {kNullString};
            }
            error_code =
                client_->BatchGet(keys, batched_object_info, batched_slices);
            if (error_code != ErrorCode::OK) {
                py::gil_scoped_acquire acquire_gil;
                return {kNullString};
            }
        }
        for (const auto &key : keys) {
            if (batched_slices[key].size() == 1 &&
                batched_slices[key][0].size == str_length_map[key]) {
                results.push_back(pybind11::bytes(
                    static_cast<char *>(batched_slices[key][0].ptr),
                    str_length_map[key]));
            } else {
                char *exported_str_ptr =
                    exportSlices(batched_slices[key], str_length_map[key]);
                if (!exported_str_ptr) {
                    return {kNullString};
                } else {
                    results.push_back(
                        pybind11::bytes(exported_str_ptr, str_length_map[key]));
                    delete[] exported_str_ptr;
                }
            }
        }
        if (results.size() != keys.size()) {
            LOG(ERROR) << "Results size does not match keys size";
            return {kNullString};
        }
        for (auto &slice : batched_slices) {
            freeSlices(slice.second);
        }
        return results;
    }
}

int DistributedObjectStore::remove(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    ErrorCode error_code = client_->Remove(key);
    if (error_code != ErrorCode::OK) return toInt(error_code);
    return 0;
}

long DistributedObjectStore::removeAll() {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }
    return client_->RemoveAll();
}

int DistributedObjectStore::isExist(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }
    ErrorCode err = client_->IsExist(key);
    if (err == ErrorCode::OK) return 1;                // Yes
    if (err == ErrorCode::OBJECT_NOT_FOUND) return 0;  // No
    return toInt(err);                                 // Error
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

    std::vector<ErrorCode> exist_results;
    ErrorCode batch_err = client_->BatchIsExist(keys, exist_results);

    results.resize(keys.size());

    if (batch_err != ErrorCode::OK) {
        LOG(ERROR) << "BatchIsExist operation failed with error: "
                   << toString(batch_err);
        // Fill all results with error code
        std::fill(results.begin(), results.end(), toInt(batch_err));
        return results;
    }

    // Convert ErrorCode results to int results
    for (size_t i = 0; i < keys.size(); ++i) {
        if (exist_results[i] == ErrorCode::OK) {
            results[i] = 1;  // Exists
        } else if (exist_results[i] == ErrorCode::OBJECT_NOT_FOUND) {
            results[i] = 0;  // Does not exist
        } else {
            results[i] = toInt(exist_results[i]);  // Error
        }
    }

    return results;
}

int64_t DistributedObjectStore::getSize(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }

    mooncake::Client::ObjectInfo object_info;
    ErrorCode error_code = client_->Query(key, object_info);

    if (error_code != ErrorCode::OK) {
        return toInt(error_code);
    }

    // Calculate total size from all replicas' handles
    int64_t total_size = 0;
    if (!object_info.replica_list.empty()) {
        auto &replica = object_info.replica_list[0];
        for (auto &handle : replica.buffer_descriptors) {
            total_size += handle.size_;
        }
    } else {
        LOG(ERROR) << "Internal error: object_info.replica_list_size() is 0";
        return -1;  // Internal error
    }

    return total_size;
}

// SliceBuffer implementation
SliceBuffer::SliceBuffer(DistributedObjectStore &store, void *buffer,
                         uint64_t size, bool use_allocator_free)
    : store_(store),
      buffer_(buffer),
      size_(size),
      use_allocator_free_(use_allocator_free) {}

SliceBuffer::~SliceBuffer() {
    if (buffer_) {
        if (use_allocator_free_) {
            // Use SimpleAllocator to deallocate memory
            store_.client_buffer_allocator_->deallocate(buffer_, size_);
        } else {
            // Use delete[] for memory allocated with new[]
            delete[] static_cast<char *>(buffer_);
        }
        buffer_ = nullptr;
    }
}

void *SliceBuffer::ptr() const { return buffer_; }

uint64_t SliceBuffer::size() const { return size_; }

// Implementation of get_buffer method
std::shared_ptr<SliceBuffer> DistributedObjectStore::get_buffer(
    const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return nullptr;
    }

    mooncake::Client::ObjectInfo object_info;
    SliceGuard guard(*this);  // Use SliceGuard for RAII
    uint64_t total_length = 0;
    ErrorCode error_code;
    std::shared_ptr<SliceBuffer> result = nullptr;

    // Query the object info
    error_code = client_->Query(key, object_info);
    if (error_code == ErrorCode::OBJECT_NOT_FOUND) {
        return nullptr;
    }
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Query failed for key: " << key
                   << " with error: " << toString(error_code);
        return nullptr;
    }

    // Allocate slices for the object using the guard
    int ret = allocateSlices(guard.slices(), object_info, total_length);
    if (ret) {
        LOG(ERROR) << "Failed to allocate slices for key: " << key;
        return nullptr;
    }

    // Get the object data
    error_code = client_->Get(key, object_info, guard.slices());
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(error_code);
        return nullptr;
    }

    if (guard.slices().size() == 1) {
        auto ptr = guard.slices()[0].ptr;
        guard.slices().clear();
        // Use SimpleAllocator for deallocation (default behavior)
        result = std::make_shared<SliceBuffer>(*this, ptr, total_length, true);
    } else {
        auto contiguous_buffer = exportSlices(guard.slices(), total_length);
        // Use delete[] for deallocation since exportSlices uses new char[]
        result = std::make_shared<SliceBuffer>(*this, contiguous_buffer,
                                               total_length, false);
    }

    return result;
}

int DistributedObjectStore::register_buffer(void *buffer, size_t size) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    ErrorCode error_code =
        client_->RegisterLocalMemory(buffer, size, kWildcardLocation);
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Register buffer failed with error: "
                   << toString(error_code);
        return toInt(error_code);
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

    mooncake::Client::ObjectInfo object_info;
    ErrorCode error_code;

    // Step 1: Get object info
    error_code = client_->Query(key, object_info);
    if (error_code == ErrorCode::OBJECT_NOT_FOUND) {
        VLOG(1) << "Object not found for key: " << key;
        return -toInt(error_code);
    }
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Query failed for key: " << key
                   << " with error: " << toString(error_code);
        return -toInt(error_code);
    }

    // Calculate total size from object info
    uint64_t total_size = 0;
    if (object_info.replica_list.empty()) {
        LOG(ERROR) << "Internal error: object_info.replica_list is empty";
        return -1;
    }

    auto &replica = object_info.replica_list[0];
    for (auto &handle : replica.buffer_descriptors) {
        total_size += handle.size_;
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

    for (auto &handle : replica.buffer_descriptors) {
        auto chunk_size = handle.size_;
        void *chunk_ptr = static_cast<char *>(buffer) + offset;
        slices.emplace_back(Slice{chunk_ptr, chunk_size});
        offset += chunk_size;
    }

    // Step 3: Read data directly into user buffer
    error_code = client_->Get(key, object_info, slices);
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(error_code);
        return -toInt(error_code);
    }

    return static_cast<int>(total_size);
}

std::vector<int> DistributedObjectStore::batch_put_from(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<int>(keys.size(), -1);
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Mismatched sizes for keys, buffers, and sizes";
        return std::vector<int>(keys.size(), -1);
    }

    std::unordered_map<std::string, std::vector<mooncake::Slice>> all_slices;
    ReplicateConfig config;
    config.replica_num = 1;  // Make configurable
    config.preferred_segment = this->local_hostname;

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto &key = keys[i];
        void *buffer = buffers[i];
        size_t size = sizes[i];

        if (size == 0) {
            LOG(WARNING) << "Attempting to put empty data for key: " << key;
            continue;
        }

        std::vector<mooncake::Slice> key_slices;
        uint64_t offset = 0;
        while (offset < size) {
            auto chunk_size = std::min(size - offset, kMaxSliceSize);
            void *chunk_ptr = static_cast<char *>(buffer) + offset;
            key_slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }
        all_slices[key] = key_slices;
    }

    ErrorCode batch_put_err = client_->BatchPut(keys, all_slices, config);

    std::vector<int> results(keys.size());
    if (batch_put_err != ErrorCode::OK) {
        LOG(ERROR) << "BatchPut failed with error: " << toString(batch_put_err);
        std::fill(results.begin(), results.end(), toInt(batch_put_err));
    } else {
        std::fill(results.begin(), results.end(), 0);
    }

    return results;
}

std::vector<int> DistributedObjectStore::batch_get_into(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes) {
    auto start_time = std::chrono::steady_clock::now();
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<int>(keys.size(), -1);
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Mismatched sizes for keys, buffers, and sizes";
        return std::vector<int>(keys.size(), -1);
    }

    std::vector<int> results(keys.size());
    mooncake::Client::BatchObjectInfo
        object_infos;  // This is BatchGetReplicaListResponse

    // Step 1: Batch query object info
    ErrorCode batch_query_err = client_->BatchQuery(keys, object_infos);
    if (batch_query_err != ErrorCode::OK) {
        LOG(ERROR) << "BatchQuery failed with error: "
                   << toString(batch_query_err);
        std::fill(results.begin(), results.end(), toInt(batch_query_err));
        return results;
    }

    // Step 2: Prepare slices for each key
    std::unordered_map<std::string, std::vector<mooncake::Slice>> all_slices;
    std::vector<std::string> valid_keys;
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto &key = keys[i];
        auto it = object_infos.batch_replica_list.find(key);

        if (it == object_infos.batch_replica_list.end()) {
            results[i] = -toInt(ErrorCode::OBJECT_NOT_FOUND);
            continue;
        }

        auto &replica_list = it->second;
        if (replica_list.empty()) {
            LOG(ERROR) << "Internal error: replica_list is empty for key: "
                       << key;
            results[i] = -1;
            continue;
        }

        auto &replica = replica_list[0];
        uint64_t total_size = 0;
        for (auto &handle : replica.buffer_descriptors) {
            total_size += handle.size_;
        }

        if (sizes[i] < total_size) {
            LOG(ERROR) << "User buffer too small for key: " << key
                       << ". Required: " << total_size
                       << ", provided: " << sizes[i];
            results[i] = -1;
            continue;
        }

        uint64_t offset = 0;
        std::vector<mooncake::Slice> key_slices;
        for (auto &handle : replica.buffer_descriptors) {
            void *chunk_ptr = static_cast<char *>(buffers[i]) + offset;
            key_slices.emplace_back(Slice{chunk_ptr, handle.size_});
            offset += handle.size_;
        }
        all_slices[key] = key_slices;
        results[i] = static_cast<int>(total_size);
        valid_keys.push_back(key);
    }

    if (valid_keys.empty()) {
        return results;
    }

    // Step 3: Batch get data
    ErrorCode batch_get_err =
        client_->BatchGet(valid_keys, object_infos, all_slices);
    if (batch_get_err != ErrorCode::OK) {
        LOG(ERROR) << "BatchGet failed with error: " << toString(batch_get_err);
        for (const auto &key : valid_keys) {
            auto it = std::find(keys.begin(), keys.end(), key);
            if (it != keys.end()) {
                size_t i = std::distance(keys.begin(), it);
                results[i] = toInt(batch_get_err);
            }
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time)
                            .count();
    LOG(INFO) << "Time taken for batch_get_into: " << elapsed_time << "us";

    return results;
}

int DistributedObjectStore::put_from(const std::string &key, void *buffer,
                                     size_t size) {
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

    ReplicateConfig config;
    config.replica_num = 1;  // Make configurable
    config.preferred_segment = this->local_hostname;

    ErrorCode error_code = client_->Put(key, slices, config);
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(error_code);
        return -toInt(error_code);
    }

    return 0;
}

PYBIND11_MODULE(store, m) {
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
               uintptr_t buffer_ptr, size_t size) {
                // Put data directly from user-provided buffer
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                return self.put_from(key, buffer, size);
            },
            py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
            "Put object data directly from a pre-allocated buffer")
        .def(
            "batch_put_from",
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
                return self.batch_put_from(keys, buffers, sizes);
            },
            py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
            "Put object data directly from pre-allocated buffers for multiple "
            "keys")
        .def("put",
             [](DistributedObjectStore &self, const std::string &key,
                py::buffer buf) {
                 py::buffer_info info = buf.request(/*writable=*/false);
                 py::gil_scoped_release release;
                 return self.put(key, std::span<const char>(
                                          static_cast<char *>(info.ptr),
                                          static_cast<size_t>(info.size)));
             })
        .def("put_parts",
             [](DistributedObjectStore &self, const std::string &key,
                py::args parts) {
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
                 return self.put_parts(key, spans);
             })
        .def(
            "put_batch",
            [](DistributedObjectStore &self,
               const std::vector<std::string> &keys,
               const std::vector<py::bytes> &py_values) {
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

                return self.put_batch(keys, spans);
            },
            py::arg("keys"), py::arg("values"));
}
