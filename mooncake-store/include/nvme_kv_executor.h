#pragma once

#include <array>
#include <cstdint>
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
    virtual tl::expected<bool, ErrorCode> Exists(
        const PhysicalKey& key) const = 0;
    virtual const Capabilities& GetCapabilities() const = 0;
    virtual std::string GetBackendType() const = 0;
};

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvIoctlExecutor(
    std::string device_path, uint32_t nsid,
    tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>& capabilities);

}  // namespace mooncake
