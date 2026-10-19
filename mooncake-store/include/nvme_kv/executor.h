#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct FileStorageConfig;

class NvmeKvCommandExecutor {
   public:
    using PhysicalKey = std::array<uint8_t, 16>;

    struct StoreRequest {
        PhysicalKey key;
        std::string_view value;
        tl::expected<void, ErrorCode> result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    };

    struct RetrievedBuffer {
        std::shared_ptr<void> owner;
        const char *data = nullptr;
        uint32_t size = 0;

        std::string ToString() const {
            return data == nullptr ? std::string()
                                   : std::string(data, data + size);
        }
    };

    struct RetrieveBufferRequest {
        PhysicalKey key;
        uint32_t size_hint = 0;
        tl::expected<RetrievedBuffer, ErrorCode> result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    };

    struct RetrieveIntoRequest {
        PhysicalKey key;
        char *data = nullptr;
        uint32_t size = 0;
        tl::expected<uint32_t, ErrorCode> result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    };

    struct Capabilities {
        uint32_t effective_max_value_size = UINT32_MAX;
        uint32_t queue_depth = 1;
    };

    virtual ~NvmeKvCommandExecutor() = default;

    virtual tl::expected<void, ErrorCode> Store(const PhysicalKey &key,
                                                std::string value) = 0;
    // Batch calls are synchronous: implementations must finish accessing all
    // request-owned buffers and populate every result before returning.
    virtual void StoreBatch(std::vector<StoreRequest> &requests) {
        for (auto &request : requests) {
            request.result = Store(request.key, std::string(request.value));
        }
    }
    virtual tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey &key, uint32_t size_hint = 0) const = 0;
    virtual void RetrieveBufferBatch(
        std::vector<RetrieveBufferRequest> &requests) const {
        for (auto &request : requests) {
            auto result = Retrieve(request.key, request.size_hint);
            if (!result) {
                request.result = tl::make_unexpected(result.error());
                continue;
            }
            auto owner =
                std::make_shared<std::string>(std::move(result.value()));
            RetrievedBuffer buffer;
            buffer.owner = owner;
            buffer.data = owner->data();
            buffer.size = static_cast<uint32_t>(owner->size());
            request.result = std::move(buffer);
        }
    }
    virtual void RetrieveIntoBatch(
        std::vector<RetrieveIntoRequest> &requests) const {
        std::vector<RetrieveBufferRequest> buffer_requests;
        buffer_requests.reserve(requests.size());
        for (const auto &request : requests) {
            RetrieveBufferRequest buffer_request;
            buffer_request.key = request.key;
            buffer_request.size_hint = request.size;
            buffer_requests.push_back(std::move(buffer_request));
        }
        RetrieveBufferBatch(buffer_requests);
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto &buffer_result = buffer_requests[index].result;
            if (!buffer_result) {
                requests[index].result =
                    tl::make_unexpected(buffer_result.error());
                continue;
            }
            const auto &buffer = buffer_result.value();
            if (requests[index].data == nullptr ||
                buffer.size != requests[index].size) {
                requests[index].result =
                    tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                continue;
            }
            std::memcpy(requests[index].data, buffer.data, buffer.size);
            requests[index].result = buffer.size;
        }
    }
    virtual tl::expected<void, ErrorCode> Delete(const PhysicalKey &key) = 0;
    virtual const Capabilities &GetCapabilities() const = 0;
};

using NvmeKvExecutorResult =
    tl::expected<std::unique_ptr<NvmeKvCommandExecutor>, ErrorCode>;

NvmeKvExecutorResult CreateNvmeKvIoUringExecutor(
    std::string device_path, uint32_t nsid, uint32_t queue_depth,
    uint32_t runtime_transfer_limit);

NvmeKvExecutorResult CreateNvmeKvIoctlExecutor(std::string device_path,
                                               uint32_t nsid,
                                               uint32_t queue_depth,
                                               uint32_t runtime_transfer_limit);

}  // namespace mooncake
