#include "nvme_kv_executor_util.h"

#include <cerrno>
#include <cstdlib>
#include <limits>

namespace mooncake {
namespace {

constexpr uint32_t kNvmeStatusCodeMask = 0xFFu;
constexpr uint32_t kNvmeKvStatusCapacityExceeded = 0x81;
constexpr uint32_t kNvmeKvStatusInvalidValueSize = 0x85;
constexpr uint32_t kNvmeKvStatusInvalidKeySize = 0x86;
constexpr uint32_t kNvmeKvStatusKeyNotFound = 0x87;
constexpr uint32_t kNvmeKvStatusUnrecoveredRead = 0x88;
constexpr uint32_t kNvmeKvStatusKeyExists = 0x89;

uint32_t ReadLe32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

uint32_t ParseU32EnvOr(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    char* end = nullptr;
    errno = 0;
    unsigned long parsed = std::strtoul(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' ||
        parsed > std::numeric_limits<uint32_t>::max()) {
        return fallback;
    }
    return static_cast<uint32_t>(parsed);
}

}  // namespace

uint32_t RoundUpToNvmeKvTransferBytes(uint32_t bytes) {
    if (bytes == 0) {
        return 0;
    }
    return ((bytes + kNvmeKvTransferUnitBytes - 1) / kNvmeKvTransferUnitBytes) *
           kNvmeKvTransferUnitBytes;
}

uint32_t RoundDownToNvmeKvTransferBytes(uint32_t bytes) {
    return (bytes / kNvmeKvTransferUnitBytes) * kNvmeKvTransferUnitBytes;
}

NvmeKvPackedKeyFields PackNvmeKvPhysicalKey(
    const NvmeKvCommandExecutor::PhysicalKey& key) {
    NvmeKvPackedKeyFields fields;
    fields.cdw2 = ReadLe32(key.data());
    fields.cdw3 = ReadLe32(key.data() + 4);
    fields.cdw14 = ReadLe32(key.data() + 8);
    fields.cdw15 = ReadLe32(key.data() + 12);
    return fields;
}

ErrorCode MapNvmeKvStatus(uint32_t status, bool is_write) {
    const uint32_t status_code = status & kNvmeStatusCodeMask;
    switch (status_code) {
        case kNvmeKvStatusCapacityExceeded:
            return ErrorCode::KEYS_ULTRA_LIMIT;
        case kNvmeKvStatusInvalidValueSize:
        case kNvmeKvStatusInvalidKeySize:
            return ErrorCode::INVALID_PARAMS;
        case kNvmeKvStatusKeyNotFound:
            return ErrorCode::OBJECT_NOT_FOUND;
        case kNvmeKvStatusUnrecoveredRead:
            return ErrorCode::FILE_READ_FAIL;
        case kNvmeKvStatusKeyExists:
            return ErrorCode::OBJECT_ALREADY_EXISTS;
        default:
            return is_write ? ErrorCode::FILE_WRITE_FAIL
                            : ErrorCode::FILE_READ_FAIL;
    }
}

ErrorCode MapNvmeKvTransportError(int err, bool is_write) {
    const uint32_t raw_err = static_cast<uint32_t>(err);
    const uint32_t already_exists_status =
        ParseU32EnvOr("MOONCAKE_NVME_KV_STATUS_KEY_ALREADY_EXISTS", 0);
    const uint32_t key_not_found_status =
        ParseU32EnvOr("MOONCAKE_NVME_KV_STATUS_KEY_NOT_FOUND", 0x4087);
    const uint32_t device_full_status =
        ParseU32EnvOr("MOONCAKE_NVME_KV_STATUS_DEVICE_FULL", 0);

    if (already_exists_status != 0 && raw_err == already_exists_status) {
        return ErrorCode::OBJECT_ALREADY_EXISTS;
    }
    if (key_not_found_status != 0 && raw_err == key_not_found_status) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    if (device_full_status != 0 && raw_err == device_full_status) {
        return ErrorCode::KEYS_ULTRA_LIMIT;
    }

    switch (err) {
        case ENOENT:
            return ErrorCode::OBJECT_NOT_FOUND;
        case ENOSPC:
            return ErrorCode::KEYS_ULTRA_LIMIT;
        case EINVAL:
            return ErrorCode::INVALID_PARAMS;
        case ENOMEM:
            return ErrorCode::BUFFER_OVERFLOW;
        default:
            return MapNvmeKvStatus(raw_err, is_write);
    }
}

}  // namespace mooncake
