#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <vector>

#include "replica.h"
#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

tl::expected<std::shared_ptr<ObjectStorageAdapter>, ErrorCode>
CreateObjectStorageAdapter(const ObjectStorageConfig& config);

class ObjectStorageBackend {
   public:
    using Copy = std::function<bool(void*, const void*, size_t)>;
    ObjectStorageBackend(ObjectStorageConfig config,
                         std::shared_ptr<ObjectStorageAdapter> adapter);
    ErrorCode Write(const ObjectStorageDescriptor& descriptor,
                    const std::vector<Slice>& slices, const Copy& to_host,
                    bool* upload_started = nullptr);
    ErrorCode Read(const ObjectStorageDescriptor& descriptor,
                   const std::vector<Slice>& slices, uint64_t offset,
                   const Copy& from_host);
    const ObjectStorageConfig& config() const { return config_; }

   private:
    bool Reserve(uint64_t bytes);
    ObjectStorageConfig config_;
    std::shared_ptr<ObjectStorageAdapter> adapter_;
    std::atomic<uint64_t> staging_bytes_{0};
};

}  // namespace mooncake
