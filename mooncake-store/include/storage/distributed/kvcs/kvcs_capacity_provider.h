#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "segment/usage.h"
#include "types.h"

namespace mooncake {

class KvcsDriver;
struct KvcsEfcTarget;

class KvcsCapacityProvider {
   public:
    KvcsCapacityProvider(uint32_t mountpoint_index, std::string mountpoint_id,
                         std::unique_ptr<KvcsDriver> driver);
    ~KvcsCapacityProvider();

    tl::expected<DfsUsageSnapshot, ErrorCode> GetCapacity();

   private:
    uint32_t mountpoint_index_;
    std::string mountpoint_id_;
    std::unique_ptr<KvcsDriver> driver_;
};

tl::expected<std::unique_ptr<KvcsCapacityProvider>, ErrorCode>
CreateKvcsCapacityProvider(const KvcsEfcTarget& target);

}  // namespace mooncake
