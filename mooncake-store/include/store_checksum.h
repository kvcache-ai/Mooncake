#pragma once

#include <cstddef>
#include <cstdint>

namespace mooncake {

bool StoreChecksumEnabled();

class StoreChecksum {
   public:
    void Update(const void* data, size_t size);
    uint64_t Finalize() const { return crc_; }

   private:
    uint64_t crc_{0};
};

uint64_t ComputeStoreChecksum(const void* data, size_t size);

}  // namespace mooncake
