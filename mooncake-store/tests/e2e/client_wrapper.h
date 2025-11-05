#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "allocator.h"
#include "client.h"
#include "types.h"

namespace mooncake {
namespace testing {

struct SegmentInfo {
    void* base;
    size_t size;
};

/*
 * @brief A wrapper for the client.
 *
 * This class is used to wrap the client and provide a more convenient interface
 * for the tests.
 */
class ClientTestWrapper {
   public:
    /**
     * @brief Constructor.
     *
     * @param client The client instance.
     * @param allocator Allocate slice memory for get and put operations.
     */
    ClientTestWrapper(std::shared_ptr<Client> client,
                      std::shared_ptr<SimpleAllocator> allocator);
    ~ClientTestWrapper();

    // The client wrapper is not copyable.
    ClientTestWrapper(const ClientTestWrapper&) = delete;
    ClientTestWrapper& operator=(const ClientTestWrapper&) = delete;

    /**
     * @brief Create a client wrapper.
     *
     * @param hostname The hostname of the client.
     * @param metadata_connstring Transfer engine metadata server url.
     * @param protocol Transfer protocol: rdma|tcp.
     * @param device_name The device name (used in transfer engine).
     * @param master_server_entry The master server entry.
     * @param local_buffer_size The local buffer size that will be used for get
     * and put operations.
     * @return The client wrapper.
     */
    static std::optional<std::shared_ptr<ClientTestWrapper>>
    CreateClientWrapper(const std::string& hostname,
                        const std::string& metadata_connstring,
                        const std::string& protocol,
                        const std::string& device_name,
                        const std::string& master_server_entry,
                        size_t local_buffer_size = 1024 * 1024 * 128);

    // Mount a segment. The buffer will be used to unmount the segment.
    ErrorCode Mount(const size_t size, void*& buffer);

    // Unmount a segment. The buffer is the one returned by Mount.
    ErrorCode Unmount(const void* buffer);

    ErrorCode Get(const std::string& key, std::string& value);
    ErrorCode Put(const std::string& key, const std::string& value);
    ErrorCode Delete(const std::string& key);

   private:
    struct SliceGuard {
        std::vector<Slice> slices_;
        std::shared_ptr<SimpleAllocator> allocator_;

        // Allocate memory according to the descriptors.
        SliceGuard(const std::vector<AllocatedBuffer::Descriptor>& descriptors,
                   std::shared_ptr<SimpleAllocator> allocator);

        // Allocate memory with a given size.
        SliceGuard(size_t size, std::shared_ptr<SimpleAllocator> allocator);

        // Prevent copying
        SliceGuard(const SliceGuard&) = delete;
        SliceGuard& operator=(const SliceGuard&) = delete;

        ~SliceGuard();
    };

    // The client instance.
    std::shared_ptr<Client> client_;
    // The segments that are mounted by the client.
    std::unordered_map<uintptr_t, SegmentInfo> segments_;
    // Manage the memory allocation for get and put operations.
    std::shared_ptr<SimpleAllocator> allocator_;
};

}  // namespace testing
}  // namespace mooncake