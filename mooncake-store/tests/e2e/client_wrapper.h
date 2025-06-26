#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.h"
#include "types.h"
#include "utils.h"
#include "allocator.h"

namespace mooncake {
namespace testing {

struct SegmentInfo {
    void* base;
    size_t size;
};

class ClientTestWrapper {
   private:
    std::shared_ptr<Client> client_;
    std::unordered_map<uintptr_t, SegmentInfo> segments_;
    std::shared_ptr<SimpleAllocator> allocator_;

   public:
    ClientTestWrapper(std::shared_ptr<Client> client, std::shared_ptr<SimpleAllocator> allocator)
        : client_(client), allocator_(allocator) {}

    ~ClientTestWrapper() {
        for (auto& [base, segment] : segments_) {
            free(segment.base);
        }
    }

    static std::optional<std::shared_ptr<ClientTestWrapper>> CreateClientWrapper(
        const std::string& hostname, const std::string& metadata_connstring,
        const std::string& protocol, const std::string& device_name,
        const std::string& master_server_entry, size_t local_buffer_size = 1024 * 1024 * 128) {
        void** args = (protocol == "rdma") ? rdma_args(device_name) : nullptr;

        auto client_opt = Client::Create(hostname,  // Local hostname
                                         metadata_connstring, protocol, args,
                                         master_server_entry);

        if (!client_opt.has_value()) {
            return std::nullopt;
        }

        std::shared_ptr<SimpleAllocator> allocator =
            std::make_shared<SimpleAllocator>(local_buffer_size);
        if (!allocator) {
            LOG(ERROR) << "Failed to create allocator";
            return std::nullopt;
        }

        ErrorCode error_code = client_opt.value()->RegisterLocalMemory(
            allocator->getBase(), local_buffer_size, "cpu:0",
            false, false);
        if (error_code != ErrorCode::OK) {
            LOG(ERROR) << "register_local_memory_failed base=" << allocator->getBase()
                    << " size=" << local_buffer_size << ", error=" << error_code;
            return std::nullopt;
        }
        return std::make_shared<ClientTestWrapper>(
            ClientTestWrapper(client_opt.value(), allocator));
    }

    ErrorCode Mount(const size_t size, void*& buffer) {
        buffer = allocate_buffer_allocator_memory(size);
        if (!buffer) {
            LOG(ERROR) << " Failed to allocate memory for segment";
            return ErrorCode::INTERNAL_ERROR;
        }

        ErrorCode error_code = client_->MountSegment(buffer, size);
        if (error_code != ErrorCode::OK) {
            free(buffer);
            return error_code;
        } else {
            segments_.emplace(reinterpret_cast<uintptr_t>(buffer),
                              SegmentInfo{buffer, size});
            return ErrorCode::OK;
        }
    }

    ErrorCode Unmount(const void* base) {
        auto it = segments_.find(reinterpret_cast<uintptr_t>(base));
        if (it == segments_.end()) {
            return ErrorCode::INVALID_PARAMS;
        }
        SegmentInfo& segment = it->second;
        ErrorCode error_code =
            client_->UnmountSegment(segment.base, segment.size);
        if (error_code != ErrorCode::OK) {
            return error_code;
        } else {
            // Clear the memory, so any further read will get wrong data
            memset(segment.base, 0, segment.size);
            free(segment.base);
            segments_.erase(it);
            return ErrorCode::OK;
        }
    }

    ErrorCode Get(const std::string& key, std::string& value) {
        Client::ObjectInfo object_info;
        ErrorCode error_code = client_->Query(key, object_info);
        if (error_code != ErrorCode::OK) {
            return error_code;
        }

        // Create slices
        std::vector<AllocatedBuffer::Descriptor>& descriptors =
            object_info.replica_list[0].buffer_descriptors;
        SliceGuard slice_guard(descriptors, allocator_);

        // Perform get operation
        error_code = client_->Get(key, object_info, slice_guard.slices_);
        if (error_code != ErrorCode::OK) {
            return error_code;
        }

        // Fill value
        value.clear();
        for (const auto& slice : slice_guard.slices_) {
            value.append(static_cast<const char*>(slice.ptr), slice.size);
        }
        return ErrorCode::OK;
    }

    ErrorCode Put(const std::string& key, const std::string& value) {
        // Create slices
        SliceGuard slice_guard(value.size(), allocator_);
        size_t offset = 0;
        for (const auto& slice : slice_guard.slices_) {
            memcpy(slice.ptr, value.data() + offset, slice.size);
            offset += slice.size;
        }

        // Configure replication
        ReplicateConfig config;
        config.replica_num = 1;

        // Perform put operation
        ErrorCode error_code = client_->Put(key, slice_guard.slices_, config);

        return error_code;
    }

    ErrorCode Delete(const std::string& key) {
        return client_->Remove(key);
    }

   private:
    struct SliceGuard {
        std::vector<Slice> slices_;
        std::shared_ptr<SimpleAllocator> allocator_;
        SliceGuard(std::vector<AllocatedBuffer::Descriptor>& descriptors, std::shared_ptr<SimpleAllocator> allocator)
            : allocator_(allocator) {
            slices_.resize(descriptors.size());
            for (size_t i = 0; i < descriptors.size(); i++) {
                void* buffer = allocator_->allocate(descriptors[i].size_);
                if (!buffer) {
                    LOG(ERROR) << "Failed to allocate memory for slice";
                    throw std::runtime_error("Failed to allocate memory for slice");
                }
                slices_[i] = Slice{buffer, descriptors[i].size_};
            }
        }
        SliceGuard(size_t size, std::shared_ptr<SimpleAllocator> allocator)
            : allocator_(allocator) {
            slices_.resize(1);
            void* buffer = allocator_->allocate(size);
            if (!buffer) {
                LOG(ERROR) << "Failed to allocate memory for slice";
                throw std::runtime_error("Failed to allocate memory for slice");
            }
            slices_[0] = Slice{buffer, size};
        }

        // Prevent copying
        SliceGuard(const SliceGuard &) = delete;
        SliceGuard &operator=(const SliceGuard &) = delete;
        ~SliceGuard() {
            for (auto& slice : slices_) {
                allocator_->deallocate(slice.ptr, slice.size);
            }
        }
    };
};

}  // namespace testing
}  // namespace mooncake