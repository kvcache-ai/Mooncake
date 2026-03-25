#pragma once

#include <memory>
#include <string>

#include "connectors/data_connector.h"
#include "client_service.h"
#include "types.h"

namespace mooncake {

class ConnectorImporter {
   public:
    ConnectorImporter(std::shared_ptr<Client> client,
                      std::unique_ptr<DataConnector> connector);

    tl::expected<void, ErrorCode> ImportObject(
        const std::string& external_key, const std::string& store_key,
        const ReplicateConfig& config = ReplicateConfig{});

    tl::expected<size_t, ErrorCode> ImportByPrefix(
        const std::string& prefix,
        const ReplicateConfig& config = ReplicateConfig{});

   private:
    std::shared_ptr<Client> client_;
    std::unique_ptr<DataConnector> connector_;
};

}  // namespace mooncake
