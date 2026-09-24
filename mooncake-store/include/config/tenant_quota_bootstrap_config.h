#pragma once

#include <string>

namespace mooncake {

struct TenantQuotaBootstrapConfig {
    bool enable_multi_tenants = false;
    std::string tenant_quota_connector_type = "file";
    std::string tenant_quota_connector_uri;
};

}  // namespace mooncake
