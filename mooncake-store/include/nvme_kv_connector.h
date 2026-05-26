#pragma once

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

    // Injection constructor: takes ownership of an externally-created executor.
    NvmeKvConnector(std::string device_id,
                    std::unique_ptr<NvmeKvCommandExecutor> executor);

    tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                        std::string value);
    tl::expected<std::string, ErrorCode> Retrieve(const PhysicalKey& key) const;
    tl::expected<bool, ErrorCode> Exists(const PhysicalKey& key) const;
    const Capabilities& GetCapabilities() const;
    const std::string& GetDeviceId() const { return device_id_; }

   private:
    std::string device_id_;
    std::unique_ptr<NvmeKvCommandExecutor> executor_;
};

}  // namespace mooncake
