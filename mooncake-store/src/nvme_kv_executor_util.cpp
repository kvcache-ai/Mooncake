#include "nvme_kv_executor_util.h"
#include "nvme_kv_object_layout.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>
#include <unordered_map>

#include <glog/logging.h>

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
constexpr uint32_t kNvmeStatusCodeTypeGeneric = 0x0;
constexpr char kNvmeKvSysfsRootEnv[] = "MOONCAKE_NVME_KV_SYSFS_ROOT";
constexpr char kNvmeKvDevRootEnv[] = "MOONCAKE_NVME_KV_DEV_ROOT";
constexpr char kDefaultNvmeKvSysfsRoot[] = "/sys/class/nvme";
constexpr char kDefaultNvmeKvDevRoot[] = "/dev";

using EndpointFields = std::unordered_map<std::string, std::string>;

uint32_t ReadLe32(const uint8_t *p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

std::string Trim(std::string value) {
    const auto is_space = [](unsigned char ch) {
        return std::isspace(ch) != 0;
    };
    value.erase(value.begin(),
                std::find_if(value.begin(), value.end(),
                             [&](char ch) { return !is_space(ch); }));
    value.erase(std::find_if(value.rbegin(), value.rend(),
                             [&](char ch) { return !is_space(ch); })
                    .base(),
                value.end());
    return value;
}

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    return value;
}

std::string GetEnvOrDefault(const char *name, const char *fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') return fallback;
    return value;
}

bool FileExists(const std::filesystem::path &path) {
    std::error_code ec;
    return std::filesystem::exists(path, ec);
}

std::optional<std::string> ReadFirstLine(const std::filesystem::path &path) {
    std::ifstream stream(path);
    if (!stream.is_open()) return std::nullopt;
    std::string line;
    if (!std::getline(stream, line)) return std::nullopt;
    return Trim(line);
}

EndpointFields ParseFields(const std::string &text) {
    EndpointFields fields;
    std::string normalized = text;
    std::replace(normalized.begin(), normalized.end(), ',', ' ');

    std::istringstream stream(normalized);
    std::string token;
    while (stream >> token) {
        auto separator = token.find('=');
        if (separator == std::string::npos) separator = token.find(':');
        if (separator == std::string::npos || separator == 0) continue;

        std::string key = ToLower(Trim(token.substr(0, separator)));
        std::string value = Trim(token.substr(separator + 1));
        if (!key.empty() && !value.empty()) fields[key] = value;
    }
    return fields;
}

bool LooksLikeNofEndpoint(const EndpointFields &fields) {
    return fields.find("traddr") != fields.end() &&
           fields.find("subnqn") != fields.end();
}

bool HasNamespaceIdField(const EndpointFields &fields) {
    return fields.find("ns") != fields.end() ||
           fields.find("nsid") != fields.end();
}

std::optional<uint32_t> ParseNamespaceId(const EndpointFields &fields) {
    auto it = fields.find("ns");
    if (it == fields.end()) it = fields.find("nsid");
    if (it == fields.end()) return std::nullopt;

    char *end_ptr = nullptr;
    errno = 0;
    const unsigned long value = std::strtoul(it->second.c_str(), &end_ptr, 10);
    if (errno != 0 || end_ptr == it->second.c_str() || *end_ptr != '\0' ||
        value == 0 || value > std::numeric_limits<uint32_t>::max()) {
        return std::nullopt;
    }
    return static_cast<uint32_t>(value);
}

bool FieldEquals(const EndpointFields &expected, const EndpointFields &actual,
                 const char *key, bool case_insensitive) {
    const auto expected_it = expected.find(key);
    if (expected_it == expected.end()) return true;

    const auto actual_it = actual.find(key);
    if (actual_it == actual.end()) return false;
    if (case_insensitive) {
        return ToLower(expected_it->second) == ToLower(actual_it->second);
    }
    return expected_it->second == actual_it->second;
}

bool AddressMatches(const EndpointFields &endpoint,
                    const EndpointFields &address) {
    return FieldEquals(endpoint, address, "traddr", false) &&
           FieldEquals(endpoint, address, "trsvcid", false) &&
           FieldEquals(endpoint, address, "host_traddr", false) &&
           FieldEquals(endpoint, address, "host_iface", false) &&
           FieldEquals(endpoint, address, "trtype", true) &&
           FieldEquals(endpoint, address, "adrfam", true);
}

std::optional<std::string> NamespaceDeviceName(
    const std::filesystem::path &controller_path, uint32_t expected_nsid) {
    std::error_code ec;
    for (std::filesystem::directory_iterator it(controller_path, ec), end;
         !ec && it != end; it.increment(ec)) {
        const std::string name = it->path().filename().string();
        if (name.rfind("nvme", 0) != 0 ||
            name.find('n', 4) == std::string::npos) {
            continue;
        }

        auto nsid = ReadFirstLine(it->path() / "nsid");
        if (!nsid.has_value()) continue;

        char *end_ptr = nullptr;
        errno = 0;
        const unsigned long value = std::strtoul(nsid->c_str(), &end_ptr, 10);
        if (errno != 0 || end_ptr == nsid->c_str() || *end_ptr != '\0' ||
            value != expected_nsid) {
            continue;
        }
        return name;
    }
    return std::nullopt;
}

std::optional<NvmeKvResolvedDevicePath> ResolveNofEndpoint(
    const EndpointFields &endpoint, NvmeKvDevicePathType type,
    uint32_t configured_nsid) {
    const std::filesystem::path sysfs_root =
        GetEnvOrDefault(kNvmeKvSysfsRootEnv, kDefaultNvmeKvSysfsRoot);
    const std::filesystem::path dev_root =
        GetEnvOrDefault(kNvmeKvDevRootEnv, kDefaultNvmeKvDevRoot);
    const auto parsed_nsid = ParseNamespaceId(endpoint);
    if (!parsed_nsid.has_value() && HasNamespaceIdField(endpoint)) {
        return std::nullopt;
    }
    const uint32_t effective_nsid = parsed_nsid.value_or(configured_nsid);
    if (effective_nsid == 0) return std::nullopt;

    std::error_code ec;
    for (std::filesystem::directory_iterator it(sysfs_root, ec), end;
         !ec && it != end; it.increment(ec)) {
        const std::filesystem::path controller_path = it->path();
        const std::string controller_name = controller_path.filename().string();
        if (controller_name.rfind("nvme", 0) != 0) continue;

        auto subsysnqn = ReadFirstLine(controller_path / "subsysnqn");
        if (!subsysnqn.has_value() || *subsysnqn != endpoint.at("subnqn")) {
            continue;
        }

        auto address = ReadFirstLine(controller_path / "address");
        if (!address.has_value() ||
            !AddressMatches(endpoint, ParseFields(*address))) {
            continue;
        }

        auto namespace_name =
            NamespaceDeviceName(controller_path, effective_nsid);
        if (!namespace_name.has_value()) continue;

        std::string device_name = *namespace_name;
        if (type == NvmeKvDevicePathType::kGenericCharacter) {
            device_name = "ng" + device_name.substr(4);
        }
        const std::filesystem::path device_path = dev_root / device_name;
        if (FileExists(device_path)) {
            return NvmeKvResolvedDevicePath{device_path.string(),
                                            effective_nsid};
        }
    }
    return std::nullopt;
}

std::string ResolveGenericCharacterDevicePath(const std::string &device_path) {
    const std::filesystem::path path(device_path);
    const std::string filename = path.filename().string();
    if (filename.rfind("ng", 0) == 0) return device_path;
    if (filename.rfind("nvme", 0) != 0) return device_path;

    const std::filesystem::path generic_path =
        path.parent_path() / ("ng" + filename.substr(4));
    if (FileExists(generic_path)) {
        LOG(INFO) << "[NvmeKvExecutor] using NVMe generic char device "
                  << generic_path << " for namespace block device "
                  << device_path;
        return generic_path.string();
    }
    return device_path;
}

}  // namespace

std::string NvmeKvTransportIdWithNsid(const std::string &configured_path,
                                      uint32_t configured_nsid) {
    const auto fields = ParseFields(configured_path);
    if (!LooksLikeNofEndpoint(fields) || HasNamespaceIdField(fields)) {
        return configured_path;
    }
    if (configured_nsid == 0) {
        return configured_path;
    }

    std::ostringstream stream;
    stream << configured_path << " ns:" << configured_nsid;
    return stream.str();
}

tl::expected<NvmeKvResolvedDevicePath, ErrorCode> ResolveNvmeKvDevicePath(
    const std::string &configured_path, NvmeKvDevicePathType type,
    uint32_t configured_nsid) {
    const auto fields = ParseFields(configured_path);
    if (LooksLikeNofEndpoint(fields)) {
        auto resolved = ResolveNofEndpoint(fields, type, configured_nsid);
        if (!resolved.has_value()) {
            LOG(ERROR) << "[NvmeKvExecutor] failed to resolve NVMe-oF endpoint "
                       << "to a local NVMe device: " << configured_path;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return resolved.value();
    }

    if (type == NvmeKvDevicePathType::kGenericCharacter) {
        return NvmeKvResolvedDevicePath{
            ResolveGenericCharacterDevicePath(configured_path),
            configured_nsid};
    }
    return NvmeKvResolvedDevicePath{configured_path, configured_nsid};
}

uint32_t ParseNvmeKvU32EnvOr(const char *name, uint32_t fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    char *end_ptr = nullptr;
    errno = 0;
    unsigned long parsed = std::strtoul(value, &end_ptr, 0);
    if (errno != 0 || end_ptr == value || *end_ptr != '\0' ||
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

ErrorCode MapNvmeKvCompletionStatus(uint32_t status_code_type,
                                    uint32_t status_code, bool is_write) {
    if (status_code_type == kNvmeStatusCodeTypeGeneric) {
        if (status_code == 0) {
            return ErrorCode::OK;
        }
        return MapNvmeKvStatus(status_code, is_write);
    }
    return is_write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL;
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
