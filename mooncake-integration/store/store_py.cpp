#include "store_py.h"

#include <netinet/in.h>
#include <pybind11/gil.h>  // For GIL management
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
    ErrorCode error_code =
        client_->RegisterLocalMemory(client_buffer_allocator_->getBase(),
                                     local_buffer_size, "cpu:0", false, false);
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
    error_code = client_->MountSegment(this->local_hostname, segment_ptr_.get(),
                                       global_segment_size);
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

PYBIND11_MODULE(store, m) {
    /*
     * Mooncake Store Python Bindings
     */

    // Define the SliceBuffer class for efficient memory management
    py::class_<SliceBuffer, std::shared_ptr<SliceBuffer>>(m, "SliceBuffer",
                                                          py::buffer_protocol(),
                                                          R"(
        A buffer class that holds contiguous data retrieved from the distributed store.

        This class provides RAII-style memory management and implements the Python
        buffer protocol for efficient data access without copying.

        Note: SliceBuffer automatically manages memory deallocation when destroyed.
        )")
        .def(
            "ptr",
            [](const SliceBuffer &self) {
                // Return the pointer as an integer for Python
                return reinterpret_cast<uintptr_t>(self.ptr());
            },
            R"(
             Get the memory address of the buffer as an integer.

             Returns:
                 int: Memory address as an integer (for advanced use cases)
             )")
        .def("size", &SliceBuffer::size,
             R"(
             Get the size of the buffer in bytes.

             Returns:
                 int: Size of the buffer in bytes
             )")
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

    // Define the DistributedObjectStore class - Main interface for Mooncake
    // Store
    py::class_<DistributedObjectStore>(m, "MooncakeDistributedStore",
                                       R"(
        Example:
            store = MooncakeDistributedStore()
            store.setup("localhost:12345", "127.0.0.1:2379")
            store.put("my_key", b"my_data")
            data = store.get("my_key")
            store.close()
        )")
        .def(py::init<>(),
             R"(
             Initialize a new MooncakeDistributedStore instance.

             Note: You must call setup() or init_all() before using the store.
             )")
        .def("setup", &DistributedObjectStore::setup, py::arg("local_hostname"),
             py::arg("metadata_server"),
             py::arg("global_segment_size") = 1024 * 1024 * 16,
             py::arg("local_buffer_size") = 1024 * 1024 * 16,
             py::arg("protocol") = "tcp", py::arg("rdma_devices") = "",
             py::arg("master_server_addr") = "127.0.0.1:50051",
             R"(
             Configure and initialize the distributed object store.

             This method sets up the connection to the metadata server, initializes
             the transfer engine, and prepares the local memory segments for storage.

             Args:
                 local_hostname (str): Local host address in format "IP:Port"
                                     (e.g., "192.168.1.100:12345")
                 metadata_server (str): Metadata server address in format "IP:Port"
                                      (e.g., "127.0.0.1:2379" for etcd)
                 global_segment_size (int, optional): Size of global memory segment in bytes.
                                                    Defaults to 16MB. Set to 0 to skip mounting.
                 local_buffer_size (int, optional): Size of local buffer for data transfer in bytes.
                                                   Defaults to 16MB.
                 protocol (str, optional): Transport protocol ("tcp" or "rdma"). Defaults to "tcp".
                 rdma_devices (str, optional): RDMA device specification for RDMA protocol.
                                              Defaults to empty string.
                 master_server_addr (str, optional): Master server address. Defaults to "127.0.0.1:50051".

             Returns:
                 int: 0 on success, non-zero error code on failure

             Note:
                 For RDMA protocol, ensure proper RDMA devices are available and configured.
             )")
        .def("init_all", &DistributedObjectStore::initAll, py::arg("protocol"),
             py::arg("device_name"),
             py::arg("mount_segment_size") = 1024 * 1024 * 16,
             R"(
             Simplified initialization with default settings.

             This is a convenience method that calls setup() with predefined defaults
             suitable for local testing and development.

             Args:
                 protocol (str): Transport protocol ("tcp" or "rdma")
                 device_name (str): Device name for the protocol
                 mount_segment_size (int, optional): Memory segment size in bytes. Defaults to 16MB.

             Returns:
                 int: 0 on success, non-zero error code on failure
             )")
        .def("get", &DistributedObjectStore::get, py::arg("key"),
             R"(
             Retrieve data for the specified key.

             This method fetches the complete object data associated with the given key
             and returns it as Python bytes. The operation is thread-safe and releases
             the GIL during network operations.

             Args:
                 key (str): Object key to retrieve

             Returns:
                 bytes: Object data as bytes, or empty bytes if key doesn't exist or error occurs

             Note:
                 For large objects, consider using get_buffer() for more efficient memory usage.
             )")
        .def("get_buffer", &DistributedObjectStore::get_buffer,
             py::call_guard<py::gil_scoped_release>(),
             py::return_value_policy::take_ownership, py::arg("key"),
             R"(
             Retrieve data as a SliceBuffer for efficient memory access.

             This method returns a SliceBuffer object that implements the Python buffer
             protocol, allowing zero-copy access to the retrieved data. This is more
             memory-efficient than get() for large objects.

             Args:
                 key (str): Object key to retrieve

             Returns:
                 SliceBuffer or None: Buffer containing the data, or None if key doesn't exist or error occurs

             Example:
                 buffer = store.get_buffer("my_key")
                 if buffer:
                     data = bytes(buffer)  # Convert to bytes if needed
                     print(f"Retrieved {buffer.size()} bytes")
             )")
        .def("remove", &DistributedObjectStore::remove,
             py::call_guard<py::gil_scoped_release>(), py::arg("key"),
             R"(
             Remove a single object from the store.

             This method deletes the object and all its replicas from the Mooncake store.

             Args:
                 key (str): Object key to remove

             Returns:
                 int: 0 on success, non-zero error code on failure
             )")
        .def("remove_all", &DistributedObjectStore::removeAll,
             py::call_guard<py::gil_scoped_release>(),
             R"(
             Remove all objects from the store.

             This method deletes all objects and their replicas from the distributed store.
             Use with caution as this operation cannot be undone.

             Returns:
                 int: Number of objects removed, or negative value on error
             )")
        .def("is_exist", &DistributedObjectStore::isExist,
             py::call_guard<py::gil_scoped_release>(), py::arg("key"),
             R"(
             Check if an object exists in the store.

             This method queries the metadata to determine if the specified key exists
             without transferring the actual data.

             Args:
                 key (str): Object key to check

             Returns:
                 int: 1 if exists, 0 if not exists, -1 on error
             )")
        .def("close", &DistributedObjectStore::tearDownAll,
             R"(
             Clean up and close the distributed store connection.

             This method properly shuts down all connections, unmounts memory segments,
             and releases resources. Always call this method when done using the store.

             Returns:
                 int: 0 on success, non-zero error code on failure

             Note:
                 The store cannot be used after calling close() unless setup() is called again.
             )")
        .def("get_size", &DistributedObjectStore::getSize,
             py::call_guard<py::gil_scoped_release>(), py::arg("key"),
             R"(
             Get the size of an object without retrieving its data.

             This method queries the metadata to determine the total size of the object
             in bytes without transferring the actual data.

             Args:
                 key (str): Object key to query

             Returns:
                 int: Size of the object in bytes, or -1 if object doesn't exist or error occurs
             )")
        .def(
            "put",
            [](DistributedObjectStore &self, const std::string &key,
               py::buffer buf) {
                py::buffer_info info = buf.request(/*writable=*/false);
                py::gil_scoped_release release;
                return self.put(
                    key, std::span<const char>(static_cast<char *>(info.ptr),
                                               static_cast<size_t>(info.size)));
            },
            py::arg("key"), py::arg("value"),
            R"(
             Store data in the distributed object store.

             This method stores the provided data under the specified key with automatic
             replication. The data is automatically split into slices if it exceeds the
             maximum slice size.

             Args:
                 key (str): Unique identifier for the object
                 value (bytes-like): Data to store (bytes, bytearray, memoryview, etc.)

             Returns:
                 int: 0 on success, non-zero error code on failure

             Example:
                 result = store.put("my_key", b"Hello, World!")
                 if result == 0:
                     print("Data stored successfully")

             Note:
                 The operation is atomic - either all data is stored or none is stored.
             )")
        .def(
            "put_parts",
            [](DistributedObjectStore &self, const std::string &key,
               py::args parts) {
                // Convert Python buffer objects to C++ spans
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

                // Call C++ function with GIL released for performance
                py::gil_scoped_release unlock;
                return self.put_parts(key, spans);
            },
            py::arg("key"),
            R"(
        Store multiple data parts as a single object in the distributed store.

        This method efficiently stores multiple data parts (e.g., list of bytes objects)
        as a single contiguous object. The parts are packed together before storage,
        which can be more efficient than concatenating them in Python.

        Note:
        Data parts are efficiently packed and automatically sliced for optimal storage.

        Args:
            key (str): Unique identifier for the object
            *parts: Variable number of bytes-like objects to store as parts

        Returns:
            int: 0 on success, non-zero error code on failure

        Example:
            part1 = b"Hello, "
            part2 = b"World!"
            result = store.put_parts("greeting", part1, part2)
            # Equivalent to: store.put("greeting", b"Hello, World!")

        Note:
            All parts must be 1-dimensional bytes-like objects (bytes, bytearray, etc.).
            The operation is atomic - either all parts are stored or none are stored.
        )");
}
