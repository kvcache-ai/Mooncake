#pragma once

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>

#include <csignal>
#include <mutex>
#include <string>
#include <unordered_set>

#include "client.h"
#include "client_buffer.hpp"

namespace mooncake {

class DistributedObjectStore;

// Forward declarations
class SliceBuffer;

template <class T>
constexpr bool is_supported_return_type_v =
    std::is_void_v<T> || std::is_integral_v<T>;

template <class T>
    requires is_supported_return_type_v<T>
int64_t to_py_ret(const tl::expected<T, ErrorCode> &exp) noexcept {
    if (!exp) {
        return static_cast<int64_t>(toInt(exp.error()));
    }

    if constexpr (std::is_void_v<T>) {
        return 0;
    } else if constexpr (std::is_integral_v<T>) {
        return static_cast<int64_t>(exp.value());
    } else {
        static_assert(!sizeof(T), "Unsupported payload type in to_py_ret()");
    }
}

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

/**
 * @brief A class that holds a contiguous buffer of data
 * This class is responsible for freeing the buffer when it's destroyed (RAII)
 */
class SliceBuffer {
   public:
    SliceBuffer(BufferHandle handle);

    void *ptr() const;
    uint64_t size() const;

   private:
    BufferHandle handle_;
};

class DistributedObjectStore {
   public:
    friend class SliceBuffer;  // Allow SliceBuffer to access private members
    DistributedObjectStore();
    ~DistributedObjectStore();

    int setup(const std::string &local_hostname,
              const std::string &metadata_server,
              size_t global_segment_size = 1024 * 1024 * 16,
              size_t local_buffer_size = 1024 * 1024 * 16,
              const std::string &protocol = "tcp",
              const std::string &rdma_devices = "",
              const std::string &master_server_addr = "127.0.0.1:50051");

    int initAll(const std::string &protocol, const std::string &device_name,
                size_t mount_segment_size = 1024 * 1024 * 16);  // Default 16MB

    int put(const std::string &key, std::span<const char> value,
            const ReplicateConfig &config = ReplicateConfig{});

    int register_buffer(void *buffer, size_t size);

    int unregister_buffer(void *buffer);

    /**
     * @brief Get object data directly into a pre-allocated buffer
     * @param key Key of the object to get
     * @param buffer Pointer to the pre-allocated buffer (must be registered
     * with register_buffer)
     * @param size Size of the buffer
     * @return Number of bytes read on success, negative value on error
     * @note The buffer address must be previously registered with
     * register_buffer() for zero-copy operations
     */
    int get_into(const std::string &key, void *buffer, size_t size);

    /**
     * @brief Get object data directly into pre-allocated buffers for multiple
     * keys (batch version)
     * @param keys Vector of keys of the objects to get
     * @param buffers Vector of pointers to the pre-allocated buffers
     * @param sizes Vector of sizes of the buffers
     * @return Vector of integers, where each element is the number of bytes
     * read on success, or a negative value on error
     * @note The buffer addresses must be previously registered with
     * register_buffer() for zero-copy operations
     */
    std::vector<int> batch_get_into(const std::vector<std::string> &keys,
                                    const std::vector<void *> &buffers,
                                    const std::vector<size_t> &sizes);

    /**
     * @brief Put object data directly from a pre-allocated buffer
     * @param key Key of the object to put
     * @param buffer Pointer to the buffer containing data (must be registered
     * with register_buffer)
     * @param size Size of the data to put
     * @return 0 on success, negative value on error
     * @note The buffer address must be previously registered with
     * register_buffer() for zero-copy operations
     */
    int put_from(const std::string &key, void *buffer, size_t size,
                 const ReplicateConfig &config = ReplicateConfig{});

    /**
     * @brief Put object data directly from pre-allocated buffers for multiple
     * keys(metadata version, better not be directly used in Python)
     * @param keys Vector of keys of the objects to put
     * @param buffers Vector of pointers to the pre-allocated buffers
     * @param metadata_buffers Vector of pointers to the pre-allocated metadata
     * buffers
     * @param size Number of sizes of the buffers
     * @param metadata_size Number of sizes of the metadata buffers
     * @param config Replication configuration
     * @return Vector of integers, where each element is 0 on success, or a
     * negative value on error
     * @note The buffer addresses must be previously registered with
     * register_buffer() for zero-copy operations
     */
    int put_from_with_metadata(
        const std::string &key, void *buffer, void *metadata_buffer,
        size_t size, size_t metadata_size,
        const ReplicateConfig &config = ReplicateConfig{});

    /**
     * @brief Put object data directly from pre-allocated buffers for multiple
     * keys (batch version)
     * @param keys Vector of keys of the objects to put
     * @param buffers Vector of pointers to the pre-allocated buffers
     * @param sizes Vector of sizes of the buffers
     * @param config Replication configuration
     * @return Vector of integers, where each element is 0 on success, or a
     * negative value on error
     * @note The buffer addresses must be previously registered with
     * register_buffer() for zero-copy operations
     */

    std::vector<int> batch_put_from(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    int put_parts(const std::string &key,
                  std::vector<std::span<const char>> values,
                  const ReplicateConfig &config = ReplicateConfig{});

    int put_batch(const std::vector<std::string> &keys,
                  const std::vector<std::span<const char>> &values,
                  const ReplicateConfig &config = ReplicateConfig{});

    [[nodiscard]] std::string get_hostname() const;

    pybind11::bytes get(const std::string &key);

    std::vector<pybind11::bytes> get_batch(
        const std::vector<std::string> &keys);

    /**
     * @brief Get a buffer containing the data for a key
     * @param key Key to get data for
     * @return std::shared_ptr<SliceBuffer> Buffer containing the data, or
     * nullptr if error
     */
    std::shared_ptr<SliceBuffer> get_buffer(const std::string &key);

    /**
     * @brief Get buffers containing the data for multiple keys (batch version)
     * @param keys Vector of keys to get data for
     * @return Vector of std::shared_ptr<SliceBuffer> buffers containing the
     * data, or nullptr for each key if error
     */
    std::vector<std::shared_ptr<SliceBuffer>> batch_get_buffer(
        const std::vector<std::string> &keys);

    int remove(const std::string &key);

    long removeAll();

    int tearDownAll();

    /**
     * @brief Check if an object exists
     * @param key Key to check
     * @return 1 if exists, 0 if not exists, -1 if error
     */
    int isExist(const std::string &key);

    /**
     * @brief Check if multiple objects exist
     * @param keys Vector of keys to check
     * @return Vector of existence results: 1 if exists, 0 if not exists, -1 if
     * error
     */
    std::vector<int> batchIsExist(const std::vector<std::string> &keys);

    /**
     * @brief Get the size of an object
     * @param key Key of the object
     * @return Size of the object in bytes, or -1 if error or object doesn't
     * exist
     */
    int64_t getSize(const std::string &key);

    /**
     * @brief Get a PyTorch tensor from the store
     * @param key Key of the tensor to get
     * @return PyTorch tensor, or nullptr if error or tensor doesn't exist
     */
    pybind11::object get_tensor(const std::string &key);
    /**
     * @brief Put a PyTorch tensor into the store
     * @param key Key for the tensor
     * @param tensor PyTorch tensor to store
     * @return 0 on success, negative value on error
     */
    int put_tensor(const std::string &key, pybind11::object tensor);

    // Internal versions that return tl::expected
    tl::expected<void, ErrorCode> setup_internal(
        const std::string &local_hostname, const std::string &metadata_server,
        size_t global_segment_size = 1024 * 1024 * 16,
        size_t local_buffer_size = 1024 * 1024 * 16,
        const std::string &protocol = "tcp",
        const std::string &rdma_devices = "",
        const std::string &master_server_addr = "127.0.0.1:50051");

    tl::expected<void, ErrorCode> initAll_internal(
        const std::string &protocol, const std::string &device_name,
        size_t mount_segment_size = 1024 * 1024 * 16);

    tl::expected<void, ErrorCode> unregister_buffer_internal(void *buffer);

    tl::expected<void, ErrorCode> put_internal(
        const std::string &key, std::span<const char> value,
        const ReplicateConfig &config = ReplicateConfig{});

    tl::expected<void, ErrorCode> register_buffer_internal(void *buffer,
                                                           size_t size);

    tl::expected<int64_t, ErrorCode> get_into_internal(const std::string &key,
                                                       void *buffer,
                                                       size_t size);

    std::vector<tl::expected<int64_t, ErrorCode>> batch_get_into_internal(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes);

    tl::expected<void, ErrorCode> put_from_internal(
        const std::string &key, void *buffer, size_t size,
        const ReplicateConfig &config = ReplicateConfig{});

    std::vector<tl::expected<void, ErrorCode>> batch_put_from_internal(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    tl::expected<void, ErrorCode> put_parts_internal(
        const std::string &key, std::vector<std::span<const char>> values,
        const ReplicateConfig &config = ReplicateConfig{});

    tl::expected<void, ErrorCode> put_batch_internal(
        const std::vector<std::string> &keys,
        const std::vector<std::span<const char>> &values,
        const ReplicateConfig &config = ReplicateConfig{});

    tl::expected<void, ErrorCode> put_batch_internal(
        const std::vector<std::string> &keys,
        const std::vector<pybind11::buffer> &buffers,
        const ReplicateConfig &config = ReplicateConfig{});

    tl::expected<void, ErrorCode> remove_internal(const std::string &key);

    tl::expected<int64_t, ErrorCode> removeAll_internal();

    tl::expected<void, ErrorCode> tearDownAll_internal();

    tl::expected<bool, ErrorCode> isExist_internal(const std::string &key);

    std::vector<tl::expected<bool, ErrorCode>> batchIsExist_internal(
        const std::vector<std::string> &keys);

    tl::expected<int64_t, ErrorCode> getSize_internal(const std::string &key);

    tl::expected<void, ErrorCode> put_tensor_internal(const std::string &key,
                                                      pybind11::object tensor);

    std::vector<std::shared_ptr<SliceBuffer>> batch_get_buffer_internal(
        const std::vector<std::string> &keys);

    std::shared_ptr<mooncake::Client> client_ = nullptr;
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator_ = nullptr;

    struct SegmentDeleter {
        void operator()(void *ptr) {
            if (ptr) {
                free(ptr);
            }
        }
    };

    std::vector<std::unique_ptr<void, SegmentDeleter>> segment_ptrs_;
    std::string protocol;
    std::string device_name;
    std::string local_hostname;
};

}  // namespace mooncake
