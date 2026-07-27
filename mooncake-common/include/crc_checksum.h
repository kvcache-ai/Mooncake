#pragma once

#include <cstddef>
#include <cstdint>

namespace mooncake {

class CrcChecksum {
   public:
    void Update(const void* data, size_t size);
    uint64_t Finalize() const { return crc_; }

   private:
    uint64_t crc_{0};
};

uint64_t ComputeCrcChecksum(const void* data, size_t size);

}  // namespace mooncake
