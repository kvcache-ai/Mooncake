#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "nvme_kv_executor.h"
#include "types.h"

namespace mooncake {

class NvmeKvConnector {
   public:
    using PhysicalKey = NvmeKvCommandExecutor::PhysicalKey;
    using Capabilities = NvmeKvCommandExecutor::Capabilities;

    NvmeKvConnector(std::string device_id, std::string device_path,
                    uint32_t nsid);

    tl::expected<void, ErrorCode> Init();
    tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                        std::string value);
    tl::expected<std::string, ErrorCode> Retrieve(const PhysicalKey& key) const;
    tl::expected<bool, ErrorCode> Exists(const PhysicalKey& key) const;
    const Capabilities& GetCapabilities() const;
    const std::string& GetDeviceId() const { return device_id_; }

   private:
    tl::expected<void, ErrorCode> InitExecutor();

    std::string device_id_;
    std::string device_path_;
    uint32_t nsid_ = 1;
    std::unique_ptr<NvmeKvCommandExecutor> executor_;
};

}  // namespace mooncake
