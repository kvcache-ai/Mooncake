#pragma once

#include "nvme_kv/object_store.h"
#include "replica.h"

namespace mooncake {

// Device addresses are allocated, not hashed. ID zero is reserved for absence.
NvmeKvPhysicalKey EncodeSharedNvmeKvPhysicalKey(uint64_t allocation_id,
                                                uint64_t part = 0);

struct SharedKvIORequest {
    std::string key;
    SharedKvDescriptor descriptor;
    const std::vector<Slice>* slice_lists;
};

class SharedNvmeKvBackend {
   public:
    static std::shared_ptr<NvmeKvObjectStore> CreateObjectStore(
        std::unique_ptr<NvmeKvConnector> connector);

    explicit SharedNvmeKvBackend(
        std::vector<std::shared_ptr<NvmeKvObjectStore>> object_stores);

    std::vector<tl::expected<void, ErrorCode>> BatchWrite(
        const std::vector<SharedKvIORequest>& requests);

    std::vector<tl::expected<void, ErrorCode>> BatchRead(
        const std::vector<SharedKvIORequest>& requests);

   private:
    std::vector<tl::expected<void, ErrorCode>> BatchTransfer(
        const std::vector<SharedKvIORequest>& requests, bool write);

    const std::vector<std::shared_ptr<NvmeKvObjectStore>> object_stores_;
};

}  // namespace mooncake
