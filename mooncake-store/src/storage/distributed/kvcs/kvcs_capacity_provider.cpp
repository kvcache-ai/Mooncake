#include "storage/distributed/kvcs/kvcs_capacity_provider.h"

#include <array>
#include <utility>

#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "storage/distributed/kvcs/kvcs_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_target.h"

namespace mooncake {

KvcsCapacityProvider::KvcsCapacityProvider(
    uint32_t mountpoint_index, std::string mountpoint_id,
    std::unique_ptr<KvcsDriver> driver)
    : mountpoint_index_(mountpoint_index),
      mountpoint_id_(std::move(mountpoint_id)),
      driver_(std::move(driver)) {}

KvcsCapacityProvider::~KvcsCapacityProvider() = default;

tl::expected<DfsUsageSnapshot, ErrorCode>
KvcsCapacityProvider::GetCapacity() {
    if (!driver_ || mountpoint_index_ == 0 || mountpoint_id_.empty())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto initialized = driver_->Init();
    if (!initialized) return tl::make_unexpected(initialized.error());
    auto stats = driver_->GetMountpointStats();
    if (!stats) return tl::make_unexpected(stats.error());

    const KvcsMountpointStat* match = nullptr;
    for (const auto& stat : *stats) {
        if (stat.index != mountpoint_index_ || stat.id != mountpoint_id_)
            continue;
        if (match) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        match = &stat;
    }
    if (!match || match->total_bytes == 0 ||
        match->used_bytes > match->total_bytes)
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);

    return DfsUsageSnapshot{.enabled = true,
                            .used_bytes = match->used_bytes,
                            .capacity_bytes = match->total_bytes,
                            .file_count = 0};
}

tl::expected<void, ErrorCode> KvcsCapacityProvider::DeleteObject(
    const std::string& logical_key) {
    if (!driver_) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto initialized = driver_->Init();
    if (!initialized) return tl::make_unexpected(initialized.error());
    auto encoded = EncodeKvcsKey(logical_key);
    if (!IsKvcsKeyStringSafe(encoded, driver_->MaxKeySize()))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const std::array<ObjectKey, 1> keys{std::move(encoded)};
    auto deleted = driver_->BatchDelete(keys);
    if (deleted.size() != 1)
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    return std::move(deleted.front());
}

tl::expected<std::unique_ptr<KvcsCapacityProvider>, ErrorCode>
CreateKvcsCapacityProvider(const KvcsEfcTarget& target) {
    auto driver = CreateKvcsLowLevelDriver(target.mountpoint_index);
    if (!driver) return tl::make_unexpected(driver.error());
    return std::make_unique<KvcsCapacityProvider>(
        target.mountpoint_index, target.mountpoint_id, std::move(*driver));
}

}  // namespace mooncake
