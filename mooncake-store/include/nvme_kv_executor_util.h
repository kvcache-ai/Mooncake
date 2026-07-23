#pragma once

#include <cstdint>

#include "nvme_kv_executor.h"
#include "types.h"

namespace mooncake {

struct NvmeKvPackedKeyFields {
    uint32_t cdw2 = 0;
    uint32_t cdw3 = 0;
    uint32_t cdw14 = 0;
    uint32_t cdw15 = 0;
};

constexpr uint32_t kNvmeKvTransferUnitBytes = 512;
constexpr uint32_t kNvmeKvMaxKeySizeBytes = 16;

uint32_t RoundUpToNvmeKvTransferBytes(uint32_t bytes);
uint32_t RoundDownToNvmeKvTransferBytes(uint32_t bytes);

NvmeKvPackedKeyFields PackNvmeKvPhysicalKey(
    const NvmeKvCommandExecutor::PhysicalKey& key);

ErrorCode MapNvmeKvStatus(uint32_t status, bool is_write);
ErrorCode MapNvmeKvTransportError(int err, bool is_write);

}  // namespace mooncake
