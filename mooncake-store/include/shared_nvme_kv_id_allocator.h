#pragma once

#include <cstdint>
#include <mutex>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

class SharedNvmeKvIdAllocator {
   public:
    tl::expected<uint64_t, ErrorCode> Allocate();

   private:
    std::mutex mutex_;
    uint64_t next_ = 1;
};

}  // namespace mooncake
