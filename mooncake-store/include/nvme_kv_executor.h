#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct FileStorageConfig;

class NvmeKvCommandExecutor {
   public:
    using PhysicalKey = std::array<uint8_t, 16>;

    struct StoreOptions {
        bool if_not_exists;

        StoreOptions() : if_not_exists(false) {}
        explicit StoreOptions(bool if_not_exists_value)
            : if_not_exists(if_not_exists_value) {}
    };

    struct Capabilities {
        uint32_t max_key_size = 16;
        uint32_t max_value_size = UINT32_MAX;
        uint32_t runtime_transfer_limit = UINT32_MAX;
        uint32_t effective_max_value_size = UINT32_MAX;
        uint32_t queue_depth = 1;
        bool supports_iterate = false;
        bool supports_batch_submit = false;
        bool supports_conditional_store = false;
    };

    struct CommandAuditInfo {
        std::string transport;
        std::string operation;
        int observed_return = 0;
        int observed_errno = 0;
        uint32_t observed_status = 0;
        bool has_observed_status = false;
        uint32_t observed_result = 0;
        bool has_observed_result = false;
        bool success = false;
        std::string interpretation;
    };

    virtual ~NvmeKvCommandExecutor() = default;

    virtual tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                                std::string value,
                                                StoreOptions options = {}) = 0;
    virtual tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey& key, uint32_t size_hint = 0) const = 0;
    virtual tl::expected<void, ErrorCode> Delete(const PhysicalKey& key) = 0;
    virtual tl::expected<void, ErrorCode> Iterate(
        const std::function<tl::expected<void, ErrorCode>(
            const PhysicalKey& key)>& visitor) const = 0;
    virtual const Capabilities& GetCapabilities() const = 0;
    virtual std::string GetBackendType() const = 0;
    virtual std::optional<CommandAuditInfo> GetLastCommandAuditInfo() const = 0;
};

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvStubExecutor(
    std::filesystem::path storage_path);

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvIoUringExecutor(
    const std::string& device_id, std::filesystem::path storage_path,
    std::string device_path, uint32_t nsid, uint32_t queue_depth,
    uint32_t runtime_transfer_limit,
    tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>& capabilities);

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvIoctlExecutor(
    const std::string& device_id, std::filesystem::path storage_path,
    std::string device_path, uint32_t nsid, uint32_t queue_depth,
    uint32_t runtime_transfer_limit,
    tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>& capabilities);

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvLibnvmeExecutor(
    const std::string& device_id, std::filesystem::path storage_path,
    std::string device_path, uint32_t nsid, uint32_t queue_depth,
    uint32_t runtime_transfer_limit,
    tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>& capabilities);

}  // namespace mooncake
