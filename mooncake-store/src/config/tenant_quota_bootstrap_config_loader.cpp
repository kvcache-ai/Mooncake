#include "tenant_quota_bootstrap_config_loader.h"

#include "default_config.h"

namespace mooncake {

TenantQuotaBootstrapConfig ResolveTenantQuotaBootstrapConfig(
    const DefaultConfig* file_config,
    const TenantQuotaCommandLineOverrides& command_line) {
    TenantQuotaBootstrapConfig result;

    if (file_config != nullptr) {
        if (file_config->Contains("enable_multi_tenants")) {
            file_config->GetBool("enable_multi_tenants",
                                 &result.enable_multi_tenants);
        }
        if (file_config->Contains("tenant_quota_connector_type")) {
            file_config->GetString("tenant_quota_connector_type",
                                   &result.tenant_quota_connector_type);
        }
        if (file_config->Contains("tenant_quota_connector_uri")) {
            file_config->GetString("tenant_quota_connector_uri",
                                   &result.tenant_quota_connector_uri);
        }
    }

    if (command_line.enable_multi_tenants.has_value()) {
        result.enable_multi_tenants = *command_line.enable_multi_tenants;
    }
    if (command_line.tenant_quota_connector_type.has_value()) {
        result.tenant_quota_connector_type =
            *command_line.tenant_quota_connector_type;
    }
    if (command_line.tenant_quota_connector_uri.has_value()) {
        result.tenant_quota_connector_uri =
            *command_line.tenant_quota_connector_uri;
    }

    return result;
}

}  // namespace mooncake
