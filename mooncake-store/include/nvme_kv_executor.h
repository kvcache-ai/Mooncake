#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

class NvmeKvCommandExecutor {
   public:
    using PhysicalKey = std::array<uint8_t, 16>;

    struct Capabilities {
        uint32_t max_key_size = 16;
        uint32_t max_value_size = UINT32_MAX;
        uint32_t effective_max_value_size = UINT32_MAX;
        bool probed = false;
    };

    virtual ~NvmeKvCommandExecutor() = default;

    virtual tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                                std::string value) = 0;
    virtual tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey& key) const = 0;
    // Retrieve directly into a caller-provided buffer, avoiding an extra copy.
    // Returns the actual number of bytes written to `buffer`.
    // Default implementation falls back to Retrieve() + memcpy.
    virtual tl::expected<uint32_t, ErrorCode> RetrieveInto(
        const PhysicalKey& key, void* buffer, uint32_t buffer_size) const {
        auto res = Retrieve(key);
        if (!res) {
            return tl::make_unexpected(res.error());
        }
        if (res.value().size() > buffer_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        std::memcpy(buffer, res.value().data(), res.value().size());
        return static_cast<uint32_t>(res.value().size());
    }
    virtual tl::expected<bool, ErrorCode> Exists(
        const PhysicalKey& key) const = 0;
    virtual const Capabilities& GetCapabilities() const = 0;
    virtual std::string GetBackendType() const = 0;
};

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvIoctlExecutor(
    std::string device_path, uint32_t nsid,
    tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>& capabilities);

// Create an executor by type string.
// Supported types: "ioctl" (default). Extensible for "sim", "virtio", etc.
// Returns nullptr and sets capabilities to an error on unknown type.
std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvExecutor(
    const std::string& type, const std::string& device_path, uint32_t nsid,
    tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>& capabilities);

}  // namespace mooncake
