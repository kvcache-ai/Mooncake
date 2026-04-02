#include "connectors/data_connector.h"

#include <stdexcept>

#ifdef HAVE_AWS_SDK
#include "connectors/oss_connector.h"
#endif

#include "connectors/huggingface_connector.h"

#ifdef HAVE_REDIS
#include "connectors/redis_connector.h"
#endif

namespace mooncake {

std::unique_ptr<DataConnector> DataConnector::Create(ConnectorType type) {
    switch (type) {
#ifdef HAVE_AWS_SDK
        case ConnectorType::OSS:
            return std::make_unique<OSSConnector>();
#endif
        case ConnectorType::HUGGINGFACE:
            return std::make_unique<HuggingFaceConnector>();
#ifdef HAVE_REDIS
        case ConnectorType::REDIS:
            return std::make_unique<RedisConnector>();
#endif
        default:
            throw std::invalid_argument("Unsupported connector type");
    }
}

}  // namespace mooncake
