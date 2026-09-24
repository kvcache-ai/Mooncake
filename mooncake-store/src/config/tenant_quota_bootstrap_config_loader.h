#pragma once

#include <optional>
#include <string>

#include "config/tenant_quota_bootstrap_config.h"

namespace mooncake {

class DefaultConfig;

struct TenantQuotaCommandLineOverrides {
    std::optional<bool> enable_multi_tenants;
    std::optional<std::string> tenant_quota_connector_type;
    std::optional<std::string> tenant_quota_connector_uri;
};

TenantQuotaBootstrapConfig ResolveTenantQuotaBootstrapConfig(
    const DefaultConfig* file_config,
    const TenantQuotaCommandLineOverrides& command_line);

}  // namespace mooncake
