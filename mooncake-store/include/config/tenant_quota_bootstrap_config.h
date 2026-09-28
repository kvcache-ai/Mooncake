#pragma once

#include <string>
#include <string_view>

namespace mooncake {

struct TenantQuotaBootstrapConfig {
    // The flag definitions in master.cpp reuse these defaults, so the file
    // default, the config default, and the flag default cannot drift apart.
    static constexpr bool kDefaultEnableMultiTenants = false;
    static constexpr std::string_view kDefaultConnectorType = "file";
    static constexpr std::string_view kDefaultConnectorUri = "";

    bool enable_multi_tenants{kDefaultEnableMultiTenants};
    std::string tenant_quota_connector_type{std::string(kDefaultConnectorType)};
    std::string tenant_quota_connector_uri{std::string(kDefaultConnectorUri)};
};

}  // namespace mooncake
