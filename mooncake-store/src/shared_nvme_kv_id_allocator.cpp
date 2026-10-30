#include "shared_nvme_kv_id_allocator.h"

namespace mooncake {

tl::expected<uint64_t, ErrorCode> SharedNvmeKvIdAllocator::Allocate() {
    std::lock_guard lock(mutex_);
    if (next_ == 0) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return next_++;
}

}  // namespace mooncake
