#pragma once
#include "types.h"
#include "replica.h"
#include <vector>
#include <string>

namespace mooncake {

struct CacheStats {
    uint64_t cache_hit = 0;
    uint64_t cache_miss = 0;
};

class MasterClientInterface {
public:
    virtual ~MasterClientInterface() = default;
    virtual tl::expected<void, ErrorCode> Connect(const std::string& master_addr) = 0;
    virtual tl::expected<std::vector<std::string>, ErrorCode> BatchQueryIp(
        const std::vector<UUID>& client_ids) = 0;
    virtual tl::expected<std::vector<Replica>, ErrorCode> GetReplicaListByRegex(
        const std::string& str) = 0;
    virtual CacheStats CalcCacheStats() = 0;
};

}  // namespace mooncake