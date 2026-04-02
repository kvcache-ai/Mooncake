#pragma once

#ifdef HAVE_REDIS

#include "connectors/data_connector.h"
#include <hiredis/hiredis.h>

namespace mooncake {

class RedisConnector : public DataConnector {
   public:
    RedisConnector();
    ~RedisConnector() override;

    tl::expected<void, std::string> ListObjects(
        const std::string& prefix,
        std::vector<ExternalObject>& objects) override;

    tl::expected<void, std::string> DownloadObject(
        const std::string& key, std::vector<uint8_t>& buffer) override;

    std::string GetConnectionInfo() const override;

   private:
    redisContext* context_;
    std::string host_;
    int port_;
};

}  // namespace mooncake

#endif  // HAVE_REDIS
