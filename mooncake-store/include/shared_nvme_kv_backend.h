#pragma once

#include "replica.h"
#include "shared_nvme_kv.h"

namespace mooncake {

struct SharedKvIORequest {
    std::string key;
    SharedKvDescriptor descriptor;
    const std::vector<Slice>* slice_lists;
};

class SharedNvmeKvBackend {
   public:
    explicit SharedNvmeKvBackend(
        std::vector<std::shared_ptr<SharedNvmeKv>> namespaces);

    std::vector<tl::expected<void, ErrorCode>> BatchWrite(
        const std::vector<SharedKvIORequest>& requests);
    std::vector<tl::expected<void, ErrorCode>> BatchRead(
        const std::vector<SharedKvIORequest>& requests);

   private:
    std::vector<tl::expected<void, ErrorCode>> BatchTransfer(
        const std::vector<SharedKvIORequest>& requests, bool write);
    const std::vector<std::shared_ptr<SharedNvmeKv>> namespaces_;
};
}  // namespace mooncake
