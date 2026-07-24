#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "nvme_kv_executor.h"
#include "types.h"

namespace mooncake {

namespace test {
class StorageBackendTest;
}

struct FileStorageConfig;

class NvmeKvConnector {
   public:
    friend class FileStorageTest;
    friend class test::StorageBackendTest;
    using PhysicalKey = NvmeKvCommandExecutor::PhysicalKey;
    using Capabilities = NvmeKvCommandExecutor::Capabilities;

    enum class DeviceSelectorType {
        kPath,
        kUuid,
    };

    struct ResolvedDeviceSelector {
        std::string configured_selector;
        DeviceSelectorType selector_type = DeviceSelectorType::kPath;
        std::string resolved_device_path;
        std::string resolved_uuid;
    };

    struct DiscoveredDeviceIdentity {
        std::string device_path;
        std::vector<std::string> stable_ids;
    };

    using DeviceIdentityEnumerator =
        std::function<std::vector<DiscoveredDeviceIdentity>()>;

    struct DeviceMetadata {
        std::string backend_type = "stub";
        std::string uuid;
        std::string device_path;
        uint32_t nsid = 0;
        uint32_t queue_depth = 1;
        int64_t capacity_total = 0;
        std::string health_state = "unknown";
        std::string last_error;
    };

    explicit NvmeKvConnector(const FileStorageConfig& file_storage_config,
                             std::string device_id = "default");

    tl::expected<void, ErrorCode> Init();
    tl::expected<void, ErrorCode> Store(
        const PhysicalKey& key, std::string value,
        NvmeKvCommandExecutor::StoreOptions options = {});
    tl::expected<std::string, ErrorCode> Retrieve(const PhysicalKey& key,
                                                  uint32_t size_hint = 0) const;
    tl::expected<void, ErrorCode> Delete(const PhysicalKey& key);
    tl::expected<void, ErrorCode> Iterate(
        const std::function<tl::expected<void, ErrorCode>(
            const PhysicalKey& key)>& visitor) const;
    tl::expected<void, ErrorCode> Probe();
    const Capabilities& GetCapabilities() const;
    const DeviceMetadata& GetMetadata() const { return metadata_; }
    const std::string& GetDeviceId() const { return device_id_; }
    const std::filesystem::path& GetStoragePath() const {
        return storage_path_;
    }
    static void SetTestDeviceIdentityEnumerator(
        DeviceIdentityEnumerator enumerator);
    static void ClearTestDeviceIdentityEnumerator();
    tl::expected<ResolvedDeviceSelector, ErrorCode> ResolveDeviceSelector();

   private:
    tl::expected<void, ErrorCode> InitRealExecutor();
    tl::expected<void, ErrorCode> InitStubExecutor();
    bool ShouldUseStubFallback() const;

    std::string storage_root_;
    std::string device_id_;
    std::filesystem::path storage_path_;
    DeviceMetadata metadata_;
    std::unique_ptr<NvmeKvCommandExecutor> executor_;
};

}  // namespace mooncake
