#include "distributed_object_store.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>  // for atexit
#include <random>

#include "types.h"

using namespace mooncake;

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

void ResourceTracker::exitHandler() {
    LOG(INFO) << "Process exiting, cleaning up resources";
    getInstance().cleanupAllResources();
}

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
        std::cout << "port is " << port << std::endl;
        if (isPortAvailable(port)) {
            return port;
        }
    }
    return -1;  // Failed to find available port
}

DistributedObjectStore::DistributedObjectStore() {
    // Register this instance with the global tracker
    ResourceTracker::getInstance().registerInstance(this);
}

DistributedObjectStore::~DistributedObjectStore() {
    // Unregister from the tracker before cleanup
    ResourceTracker::getInstance().unregisterInstance(this);

    if (client_ && segment_ptr_) {
        // Try to unmount the segment using saved local_hostname
        ErrorCode rc = client_->UnInit();
        if (rc != ErrorCode::OK) {
            LOG(ERROR) << "Failed to unmount segment in destructor: "
                       << toString(rc);
        }
        // The unique_ptr will automatically free the memory when reset
        segment_ptr_.reset();
        client_.reset();
    }
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

    client_ = std::make_unique<mooncake::Client>();

    void **args = (protocol == "rdma") ? rdma_args(rdma_devices) : nullptr;
    ErrorCode rc = client_->Init(this->local_hostname, metadata_server,
                                 protocol, args, master_server_addr);
    if (rc != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize client: " << toString(rc);
        return 1;
    }

    client_buffer_allocator_ =
        std::make_unique<SimpleAllocator>(local_buffer_size);
    rc = client_->RegisterLocalMemory(client_buffer_allocator_->getBase(),
                                      local_buffer_size, "cpu:0", false, false);
    if (rc != ErrorCode::OK) {
        LOG(ERROR) << "Failed to register local memory: " << toString(rc);
        return 1;
    }
    void *ptr = allocate_buffer_allocator_memory(global_segment_size);
    if (!ptr) {
        LOG(ERROR) << "Failed to allocate segment memory";
        return 1;
    }
    segment_ptr_.reset(ptr);
    rc = client_->MountSegment(this->local_hostname, segment_ptr_.get(),
                               global_segment_size);
    if (rc != ErrorCode::OK) {
        LOG(ERROR) << "Failed to mount segment: " << toString(rc);
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
            // Deallocate any previously allocated slices
            for (auto &slice : slices) {
                client_buffer_allocator_->deallocate(slice.ptr, slice.size);
            }
            slices.clear();
            return 1;
        }
        memcpy(ptr, value.data() + offset, chunk_size);
        slices.emplace_back(Slice{ptr, chunk_size});
        offset += chunk_size;
    }
    return 0;
}

int DistributedObjectStore::allocateSlices(
    std::vector<mooncake::Slice> &slices,
    const mooncake::Client::ObjectInfo &object_info, uint64_t &length) {
    length = 0;
    if (!object_info.replica_list_size()) return -1;
    auto &replica = object_info.replica_list(0);
    for (auto &handle : replica.handles()) {
        auto chunk_size = handle.size();
        assert(chunk_size <= kMaxSliceSize);
        auto ptr = client_buffer_allocator_->allocate(chunk_size);
        if (!ptr) return 1;
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
    ErrorCode rc = client_->UnInit();
    if (rc != ErrorCode::OK) {
        LOG(ERROR) << "Failed to unmount segment: " << toString(rc);
        return 1;
    }
    client_.reset();
    client_buffer_allocator_.reset();
    segment_ptr_.reset();
    local_hostname = "";
    device_name = "";
    protocol = "";
    return 0;
}

int DistributedObjectStore::put(const std::string &key,
                                const std::string &value) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    ReplicateConfig config;
    config.replica_num = 1;  // TODO

    std::vector<Slice> slices;
    int ret = allocateSlices(slices, value);
    if (ret) return ret;
    ErrorCode error_code = client_->Put(std::string(key), slices, config);
    freeSlices(slices);
    if (error_code != ErrorCode::OK) return toInt(error_code);
    return 0;
}

pybind11::bytes DistributedObjectStore::get(const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return pybind11::bytes("\0", 0);
    }
    mooncake::Client::ObjectInfo object_info;
    std::vector<Slice> slices;

    const auto kNullString = pybind11::bytes("\0", 0);
    ErrorCode error_code = client_->Query(key, object_info);
    if (error_code != ErrorCode::OK) return kNullString;

    uint64_t str_length = 0;
    int ret = allocateSlices(slices, object_info, str_length);
    if (ret) return kNullString;

    error_code = client_->Get(key, object_info, slices);
    if (error_code != ErrorCode::OK) {
        freeSlices(slices);
        return kNullString;
    }

    if (slices.size() == 1 && slices[0].size == str_length) {
        auto result = pybind11::bytes((char *)slices[0].ptr, str_length);
        freeSlices(slices);
        return result;
    }

    const char *str = exportSlices(slices, str_length);
    freeSlices(slices);
    if (!str) return kNullString;

    pybind11::bytes result(str, str_length);
    delete[] str;
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
    if (object_info.replica_list_size() > 0) {
        auto &replica = object_info.replica_list(0);
        for (auto &handle : replica.handles()) {
            total_size += handle.size();
        }
    } else {
        LOG(ERROR) << "Internal error: object_info.replica_list_size() is 0";
        return -1;  // Internal error
    }

    return total_size;
}
