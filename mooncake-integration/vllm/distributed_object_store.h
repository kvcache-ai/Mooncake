#pragma once

#include <pybind11/pybind11.h>

#include <csignal>
#include <mutex>
#include <string>
#include <unordered_set>

#include "allocator.h"
#include "client.h"
#include "utils.h"

class DistributedObjectStore;

// Global resource tracker to handle cleanup on abnormal termination
class ResourceTracker {
   public:
    // Get the singleton instance
    static ResourceTracker &getInstance();

    // Register a DistributedObjectStore instance for cleanup
    void registerInstance(DistributedObjectStore *instance);

    // Unregister a DistributedObjectStore instance
    void unregisterInstance(DistributedObjectStore *instance);

   private:
    ResourceTracker();
    ~ResourceTracker();

    // Prevent copying
    ResourceTracker(const ResourceTracker &) = delete;
    ResourceTracker &operator=(const ResourceTracker &) = delete;

    // Cleanup all registered resources
    void cleanupAllResources();

    // Signal handler function
    static void signalHandler(int signal);

    // Exit handler function
    static void exitHandler();

    std::mutex mutex_;
    std::unordered_set<DistributedObjectStore *> instances_;
};

class DistributedObjectStore {
   public:
    DistributedObjectStore();
    ~DistributedObjectStore();

    int setup(const std::string &local_hostname,
              const std::string &metadata_server,
              size_t global_segment_size = 1024 * 1024 * 16,
              size_t local_buffer_size = 1024 * 1024 * 16,
              const std::string &protocol = "tcp",
              const std::string &rdma_devices = "",
              const std::string &master_server_addr = "127.0.0.1:50051",
              const std::string &storage_root_path = "");

    int initAll(const std::string &protocol, const std::string &device_name,
                size_t mount_segment_size = 1024 * 1024 * 16, // Default 16MB
                const std::string &storage_root_path = "");

    int put(const std::string &key, const std::string &value);

    pybind11::bytes get(const std::string &key);

    int remove(const std::string &key);

    int tearDownAll();

    /**
     * @brief Check if an object exists
     * @param key Key to check
     * @return 1 if exists, 0 if not exists, -1 if error
     */
    int isExist(const std::string &key);

    /**
     * @brief Get the size of an object
     * @param key Key of the object
     * @return Size of the object in bytes, or -1 if error or object doesn't
     * exist
     */
    int64_t getSize(const std::string &key);

   private:
    int allocateSlices(std::vector<mooncake::Slice> &slices,
                       const std::string &value);

    int allocateSlices(std::vector<mooncake::Slice> &slices,
                       const mooncake::Client::ObjectInfo &object_info,
                       uint64_t &length);

    char *exportSlices(const std::vector<mooncake::Slice> &slices,
                       uint64_t length);

    int freeSlices(const std::vector<mooncake::Slice> &slices);

   public:
    std::shared_ptr<mooncake::Client> client_ = nullptr;
    std::unique_ptr<mooncake::SimpleAllocator> client_buffer_allocator_ =
        nullptr;
    struct SegmentDeleter {
        void operator()(void *ptr) {
            if (ptr) {
                free(ptr);
            }
        }
    };

    std::unique_ptr<void, SegmentDeleter> segment_ptr_;
    std::string protocol;
    std::string device_name;
    std::string local_hostname;
};
