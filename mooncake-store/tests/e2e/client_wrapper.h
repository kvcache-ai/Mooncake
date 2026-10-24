#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "allocator.h"
#include "common_types.h"

namespace mooncake {

#if defined(MOONCAKE_STORE_CLIENT_P2P)
class P2PClientService;
#else
class Client;
#endif

namespace testing {

#if defined(MOONCAKE_STORE_CLIENT_P2P)
using NativeTestClient = P2PClientService;
#else
using NativeTestClient = Client;
#endif

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
    ClientTestWrapper(std::shared_ptr<NativeTestClient> client,
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
     * @param redis_cluster_id Optional Redis HA cluster ID for tests using
     * redis:// master discovery.
     * @param enable_http_server Whether to start the P2P client's HTTP metrics
     * server. Centralized metrics use native environment controls.
     * @return The client wrapper.
     */
    static std::optional<std::shared_ptr<ClientTestWrapper>>
    CreateClientWrapper(const std::string& hostname,
                        const std::string& metadata_connstring,
                        const std::string& protocol,
                        const std::string& device_name,
                        const std::string& master_server_entry,
                        size_t local_buffer_size = 1024 * 1024 * 128,
                        const std::string& redis_cluster_id = "",
                        bool enable_http_server = true,
                        const std::string& p2p_local_transfer_mode = "memcpy",
                        uint16_t p2p_client_rpc_port = 0,
                        const std::string& redis_username = "",
                        const std::string& redis_password = "");

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

        // Allocate memory with a given size.
        SliceGuard(size_t size, std::shared_ptr<SimpleAllocator> allocator)
            : allocator_(allocator) {
            try {
                if (size != 0) {
                    slices_.reserve(1 + (size - 1) / kMaxSliceSize);
                }
                while (size != 0) {
                    auto chunk_size = std::min(size, kMaxSliceSize);
                    auto ptr = allocator_->allocate(chunk_size);
                    if (!ptr) {
                        throw std::runtime_error(
                            "Failed to allocate memory for slice");
                    }
                    slices_.emplace_back(Slice{ptr, chunk_size});
                    size -= chunk_size;
                }
            } catch (...) {
                LOG(ERROR)
                    << "Slice allocation failed; releasing partial buffers";
                for (const auto& slice : slices_) {
                    allocator_->deallocate(slice.ptr, slice.size);
                }
                throw;
            }
        }

        // Prevent copying
        SliceGuard(const SliceGuard&) = delete;
        SliceGuard& operator=(const SliceGuard&) = delete;

        ~SliceGuard() {
            for (auto& slice : slices_) {
                allocator_->deallocate(slice.ptr, slice.size);
            }
        }
    };

    // The native client type is fixed by the build architecture.
    std::shared_ptr<NativeTestClient> client_;
    // The segments that are mounted by the client.
    std::unordered_map<uintptr_t, SegmentInfo> segments_;
    // Manage the memory allocation for get and put operations.
    std::shared_ptr<SimpleAllocator> allocator_;
};

}  // namespace testing
}  // namespace mooncake
