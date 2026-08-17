#include "nvme_kv_executor_util.h"
#include "nvme_kv_object_layout.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <iomanip>
#include <limits>

namespace mooncake {
namespace {

constexpr uint32_t kNvmeStatusCodeMask = 0xFFu;
constexpr char kNvmeKvTransferAlignmentEnv[] =
    "MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES";
constexpr uint32_t kNvmeKvStatusCapacityExceeded = 0x81;
constexpr uint32_t kNvmeKvStatusInvalidValueSize = 0x85;
constexpr uint32_t kNvmeKvStatusInvalidKeySize = 0x86;
constexpr uint32_t kNvmeKvStatusKeyNotFound = 0x87;
constexpr uint32_t kNvmeKvStatusUnrecoveredRead = 0x88;
constexpr uint32_t kNvmeKvStatusKeyExists = 0x89;

uint32_t ReadLe32(const uint8_t *p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

}  // namespace

uint32_t ParseNvmeKvU32EnvOr(const char *name, uint32_t fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    char *end = nullptr;
    errno = 0;
    unsigned long parsed = std::strtoul(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' ||
        parsed > std::numeric_limits<uint32_t>::max()) {
        return fallback;
    }
    return static_cast<uint32_t>(parsed);
}

NvmeKvAlignedBuffer AllocateNvmeKvAlignedBuffer(size_t size) {
    void *ptr = nullptr;
    const size_t alignment = std::max<size_t>(
        kDefaultNvmeKvTransferAlignmentBytes, NvmeKvTransferAlignmentBytes());
    if (posix_memalign(&ptr, alignment, size) != 0) {
        return nullptr;
    }
    return NvmeKvAlignedBuffer(static_cast<char *>(ptr));
}

uint32_t RoundUpToNvmeKvTransferBytes(uint32_t bytes) {
    if (bytes == 0) {
        return 0;
    }
    const uint32_t alignment = NvmeKvTransferAlignmentBytes();
    const uint64_t rounded =
        ((static_cast<uint64_t>(bytes) + alignment - 1) / alignment) *
        alignment;
    if (rounded > std::numeric_limits<uint32_t>::max()) {
        return std::numeric_limits<uint32_t>::max();
    }
    return static_cast<uint32_t>(rounded);
}

uint32_t RoundDownToNvmeKvTransferBytes(uint32_t bytes) {
    const uint32_t alignment = NvmeKvTransferAlignmentBytes();
    return (bytes / alignment) * alignment;
}

uint32_t NvmeKvTransferAlignmentBytes() {
    const uint32_t configured = ParseNvmeKvU32EnvOr(
        kNvmeKvTransferAlignmentEnv, kDefaultNvmeKvTransferAlignmentBytes);
    return configured == 0 ? kDefaultNvmeKvTransferAlignmentBytes : configured;
}

uint32_t NvmeKvValueBlockUnitBytes() {
    const uint32_t configured =
        ParseNvmeKvU32EnvOr("MOONCAKE_NVME_KV_VALUE_BLOCK_UNIT_BYTES",
                            kDefaultNvmeKvValueBlockUnitBytes);
    return configured == 0 ? kDefaultNvmeKvValueBlockUnitBytes : configured;
}

uint32_t ResolveNvmeKvStoreSubmissionBytes(uint32_t logical_bytes) {
    return RoundUpToNvmeKvTransferBytes(logical_bytes);
}

uint32_t ResolveNvmeKvInitialRetrieveBytes(uint32_t size_hint,
                                           uint32_t effective_max_value_size) {
    const uint32_t requested_bytes =
        size_hint == 0 ? std::max<uint32_t>(sizeof(NvmeKvObjectHeader),
                                            NvmeKvTransferAlignmentBytes())
                       : size_hint;
    return std::min(effective_max_value_size,
                    RoundUpToNvmeKvTransferBytes(requested_bytes));
}

uint32_t ResolveNvmeKvRetrievedValueSize(const char *buffer,
                                         uint32_t returned_size,
                                         uint32_t max_size,
                                         uint32_t size_hint) {
    const uint32_t resolved_size =
        ResolveNvmeKvObjectBlobSize(buffer, returned_size, max_size);
    if (resolved_size != 0) {
        return resolved_size;
    }
    if (returned_size == 0 && size_hint != 0 && size_hint <= max_size) {
        return size_hint;
    }
    return 0;
}

bool ShouldRetryNvmeKvRetrieveWithMaxBuffer(ErrorCode error, uint32_t size_hint,
                                            uint32_t request_bytes,
                                            uint32_t effective_max_value_size) {
    return error == ErrorCode::INVALID_PARAMS && size_hint == 0 &&
           request_bytes < effective_max_value_size;
}

NvmeKvCommandExecutor::Capabilities BuildNvmeKvCapabilities(
    uint32_t default_queue_depth, uint32_t queue_depth,
    uint32_t runtime_transfer_limit) {
    NvmeKvCommandExecutor::Capabilities caps;
    const uint32_t protocol_max_value_size =
        ParseNvmeKvU32EnvOr("MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE",
                            kDefaultNvmeKvProtocolMaxValueSize);
    const uint32_t effective_runtime_limit =
        runtime_transfer_limit == 0 ? kDefaultNvmeKvRuntimeTransferLimit
                                    : runtime_transfer_limit;
    caps.effective_max_value_size = RoundDownToNvmeKvTransferBytes(
        std::min(protocol_max_value_size, effective_runtime_limit));
    caps.queue_depth = queue_depth == 0 ? default_queue_depth : queue_depth;
    return caps;
}

uint32_t ComputeNvmeKvValueBlockCountMinusOne(uint32_t bytes) {
    if (bytes == 0) {
        return 0;
    }
    const uint32_t rounded_bytes = RoundUpToNvmeKvTransferBytes(bytes);
    const uint32_t block_unit = NvmeKvValueBlockUnitBytes();
    const uint64_t block_count =
        (static_cast<uint64_t>(rounded_bytes) + block_unit - 1) / block_unit;
    return block_count == 0 ? 0 : block_count - 1;
}

NvmeKvPackedKeyFields PackNvmeKvPhysicalKey(
    const NvmeKvCommandExecutor::PhysicalKey &key) {
    NvmeKvPackedKeyFields fields;
    fields.cdw2 = ReadLe32(key.data());
    fields.cdw3 = ReadLe32(key.data() + 4);
    fields.cdw14 = (static_cast<uint32_t>(kNvmeKvCommandSetIdentifier) << 24) |
                   (ReadLe32(key.data() + 8) & 0x00FFFFFFu);
    fields.cdw15 = ReadLe32(key.data() + 12);
    return fields;
}

std::string NvmeKvPhysicalKeyToHex(
    const NvmeKvCommandExecutor::PhysicalKey &key) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (uint8_t byte : key) {
        oss << std::setw(2) << static_cast<int>(byte);
    }
    return oss.str();
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
            return MapNvmeKvStatus(static_cast<uint32_t>(err), is_write);
    }
}

bool IsNvmeKvControlFlowError(ErrorCode error) {
    switch (error) {
        case ErrorCode::OBJECT_NOT_FOUND:
        case ErrorCode::OBJECT_ALREADY_EXISTS:
            return true;
        default:
            return false;
    }
}

}  // namespace mooncake
