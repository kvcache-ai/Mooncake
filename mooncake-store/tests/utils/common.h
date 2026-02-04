#pragma once
#include "file_storage.h"
#include "storage_backend.h"
namespace mooncake {
namespace fs = std::filesystem;
inline tl::expected<void, ErrorCode> BatchOffloadUtil(
    StorageBackendInterface& storage_backend, std::vector<std::string>& keys,
    std::vector<int64_t>& sizes,
    std::unordered_map<std::string, std::string>& batched_data,
    std::vector<int64_t>& buckets) {
    storage_backend.Init();
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    size_t bucket_sz = 10;
    size_t batch_sz = 10;
    size_t data_sz = 10;
    for (size_t i = 0; i < bucket_sz; i++) {
        std::unordered_map<std::string, std::vector<Slice>> batched_slices;
        for (size_t j = 0; j < batch_sz; j++) {
            std::string key =
                "test_key_i_" + std::to_string(i) + "_j_" + std::to_string(j);
            std::string all_data;
            std::vector<Slice> slices;
            for (size_t k = 0; k < data_sz; k++) {
                std::string data = "test_data_i_" + std::to_string(i) + "_j_" +
                                   std::to_string(j) + "_k_" +
                                   std::to_string(k);
                all_data += data;
                void* buffer = client_buffer_allocator->allocate(data.size());
                memcpy(buffer, data.data(), data.size());
                slices.emplace_back(Slice{buffer, data.size()});
            }
            batched_slices.emplace(key, slices);
            batched_data.emplace(key, all_data);
            keys.emplace_back(key);
            sizes.emplace_back(all_data.size());
        }
        auto batch_store_object_one_result = storage_backend.BatchOffload(
            batched_slices,
            [&](const std::vector<std::string>& keys,
                const std::vector<StorageObjectMetadata>& metadatas) {
                if (keys.size() != batched_slices.size()) {
                    return ErrorCode::INVALID_KEY;
                }
                for (const auto& key : keys) {
                    if (batched_slices.find(key) == batched_slices.end()) {
                        return ErrorCode::INVALID_KEY;
                    }
                }
                return ErrorCode::OK;
            });
        if (!batch_store_object_one_result) {
            return tl::make_unexpected(batch_store_object_one_result.error());
        }
        buckets.emplace_back(batch_store_object_one_result.value());
    }
    return {};
}
}  // namespace mooncake