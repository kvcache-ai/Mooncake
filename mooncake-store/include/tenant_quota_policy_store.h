#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

struct TenantQuotaPolicySnapshot {
    std::map<std::string, uint64_t> tenant_quotas;
};

tl::expected<uint64_t, std::string> ParseTenantQuotaBytes(
    const std::string& value);

tl::expected<TenantQuotaPolicySnapshot, std::string> ParseTenantQuotaPolicyYaml(
    const std::string& yaml);

std::string FormatTenantQuotaPolicyYaml(
    const TenantQuotaPolicySnapshot& snapshot);

class TenantQuotaPolicyStore {
   public:
    virtual ~TenantQuotaPolicyStore() = default;

    virtual tl::expected<TenantQuotaPolicySnapshot, std::string> Load() = 0;
    virtual tl::expected<void, std::string> Save(
        const TenantQuotaPolicySnapshot& snapshot) = 0;
};

class YamlTenantQuotaPolicyStore final : public TenantQuotaPolicyStore {
   public:
    explicit YamlTenantQuotaPolicyStore(std::string path);

    tl::expected<TenantQuotaPolicySnapshot, std::string> Load() override;
    tl::expected<void, std::string> Save(
        const TenantQuotaPolicySnapshot& snapshot) override;

   private:
    std::string path_;
    std::mutex mutex_;
};

tl::expected<std::unique_ptr<TenantQuotaPolicyStore>, std::string>
CreateTenantQuotaPolicyStore(const std::string& type, const std::string& uri);

}  // namespace mooncake
