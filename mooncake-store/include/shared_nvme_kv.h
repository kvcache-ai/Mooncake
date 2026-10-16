#pragma once

#include <span>

#include "nvme_kv/executor.h"

namespace mooncake {

// Logical keys identify immutable payloads; callers must enforce this contract.
class SharedNvmeKv final {
   public:
    struct WriteRequest {
        std::string key;
        uint64_t allocation_id = 0;
        std::span<const char> payload;
        tl::expected<void, ErrorCode> result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    };

    struct ReadRequest {
        std::string key;
        uint64_t allocation_id = 0;
        // size() is the expected payload length, excluding header and identity.
        std::span<char> data;
        tl::expected<size_t, ErrorCode> result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    };

    explicit SharedNvmeKv(std::unique_ptr<NvmeKvCommandExecutor> executor);

    const char* GetName() const { return "shared_nvme_kv"; }

    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      uint64_t allocation_id,
                                      std::span<const char> payload);
    tl::expected<size_t, ErrorCode> Get(const std::string& key,
                                        uint64_t allocation_id, void* buf,
                                        size_t len);

    // Batches are synchronous and populate every result before returning.
    // Failed items do not roll back successful items.
    void PutBatch(std::vector<WriteRequest>& requests);
    void GetBatch(std::vector<ReadRequest>& requests);

    tl::expected<uint32_t, ErrorCode> EncodedSize(const std::string& key,
                                                  uint64_t payload_size) const;

   private:
    tl::expected<void, ErrorCode> EncodeInto(const std::string& key,
                                             uint64_t allocation_id,
                                             std::span<const char> payload,
                                             std::span<char> encoded) const;
    tl::expected<uint32_t, ErrorCode> EncodeMetadata(
        const std::string& key, uint64_t allocation_id,
        std::span<const char> payload, std::span<char> encoded) const;
    tl::expected<std::string_view, ErrorCode> Validate(
        const std::string& key, uint64_t allocation_id,
        std::string_view encoded) const;
    tl::expected<size_t, ErrorCode> Decode(const std::string& key,
                                           uint64_t allocation_id,
                                           std::string_view encoded,
                                           std::span<char> output) const;

    std::unique_ptr<NvmeKvCommandExecutor> executor_;
};

}  // namespace mooncake
