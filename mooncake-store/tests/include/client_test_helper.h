#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.h"
#include "types.h"
#include "utils.h"

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

   public:
    ClientTestWrapper(std::shared_ptr<Client> client) : client_(client) {}

    ~ClientTestWrapper() {
        for (auto& [base, segment] : segments_) {
            free(segment.base);
        }
    }

    static std::optional<std::shared_ptr<ClientTestWrapper>> CreateClient(
        const std::string& hostname, const std::string& metadata_connstring,
        const std::string& protocol, const std::string& device_name,
        const std::string& master_server_entry) {
        void** args = (protocol == "rdma") ? rdma_args(device_name) : nullptr;

        auto client_opt = Client::Create(hostname,  // Local hostname
                                         metadata_connstring, protocol, args,
                                         master_server_entry);

        if (!client_opt.has_value()) {
            return std::nullopt;
        }

        return std::make_shared<ClientTestWrapper>(ClientTestWrapper(client_opt.value()));
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
        SliceGuard slice_guard(descriptors);

        // Perform get operation
        error_code = client_->Get(key, object_info, slice_guard.slices);
        if (error_code != ErrorCode::OK) {
            return error_code;
        }

        // Fill value
        value.clear();
        for (const auto& slice : slice_guard.slices) {
            value.append(static_cast<const char*>(slice.ptr), slice.size);
        }
        return ErrorCode::OK;
    }

    ErrorCode Put(const std::string& key, const std::string& value) {
        // Allocate buffer for the value
        void* buffer = malloc(value.size());
        if (!buffer) {
            return ErrorCode::INTERNAL_ERROR;
        }

        // Copy value to buffer
        memcpy(buffer, value.data(), value.size());

        // Create slices
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buffer, value.size()});

        // Configure replication
        ReplicateConfig config;
        config.replica_num = 1;

        // Perform put operation
        ErrorCode error_code = client_->Put(key, slices, config);

        // Free the buffer
        free(buffer);

        return error_code;
    }

    ErrorCode Delete(const std::string& key) {
        return client_->Remove(key);
    }

   private:
    struct SliceGuard {
        std::vector<Slice> slices;
        SliceGuard(std::vector<AllocatedBuffer::Descriptor>& descriptors) {
            slices.resize(descriptors.size());
            for (size_t i = 0; i < descriptors.size(); i++) {
                void* buffer = malloc(descriptors[i].size_);
                slices[i] = Slice{buffer, descriptors[i].size_};
            }
        }
        ~SliceGuard() {
            for (auto& slice : slices) {
                free(slice.ptr);
            }
        }
    };
};

}  // namespace testing
}  // namespace mooncake