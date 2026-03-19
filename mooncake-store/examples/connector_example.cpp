// Example: Using Mooncake Store Connectors

#include "connectors/data_connector.h"
#include "connectors/connector_importer.h"
#include "client_service.h"

using namespace mooncake;

int main() {
    // Example 1: Import from Hugging Face
    auto hf_connector = DataConnector::Create(ConnectorType::HUGGINGFACE);

    // Example 2: Import from OSS (Alibaba Cloud)
    // Set environment variables first:
    // export MOONCAKE_OSS_ENDPOINT="oss-cn-hangzhou.aliyuncs.com"
    // export MOONCAKE_OSS_BUCKET="my-bucket"
    // export MOONCAKE_OSS_ACCESS_KEY_ID="your-key"
    // export MOONCAKE_OSS_SECRET_ACCESS_KEY="your-secret"

#ifdef HAVE_AWS_SDK
    auto oss_connector = DataConnector::Create(ConnectorType::OSS);

    // Create client and importer using factory
    auto client_opt = Client::Create();
    if (!client_opt) {
        // Failed to create client; abort example
        return 1;
    }
    ConnectorImporter importer(client_opt.value(), std::move(oss_connector));

    // Import single object
    ReplicateConfig config;
    config.replica_num = 2;
    auto result =
        importer.ImportObject("models/bert.bin", "bert_model", config);

    // Import by prefix
    auto count = importer.ImportByPrefix("models/", config);
#endif

    return 0;
}
