#include "storage/distributed/kvcs/kvcs_capi_driver.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "ascii_string.h"
#include "integer_parser.h"
#include "storage/distributed/kvcs/kvcs_driver.h"

#ifdef MOONCAKE_HAVE_KVCS_SDK
#include <kvcs_capi.h>
#endif

namespace mooncake {
namespace {

#ifdef MOONCAKE_HAVE_KVCS_SDK

constexpr uint64_t kStandardDefaultMaxValueSize = 4ULL * 1024 * 1024;
constexpr uint64_t kDefaultOperationTimeoutMs = 30'000;
constexpr uint64_t kDefaultQueryOperationTimeoutMs = 50;
constexpr std::string_view kDefaultEfcSocket = "/var/run/kvcs/efc-grpc.sock";
constexpr std::string_view kLowLevelPartSuffixPrefix = ".part_";
constexpr std::string_view kLowLevelManifestSuffix = ".manifest";

uint32_t DecimalDigits(uint32_t value) {
    uint32_t digits = 1;
    while (value >= 10) {
        value /= 10;
        ++digits;
    }
    return digits;
}

uint32_t LowLevelPhysicalKeySuffixSize(uint32_t total_shard) {
    if (total_shard <= 1) return 0;
    const uint32_t part_suffix_size =
        static_cast<uint32_t>(kLowLevelPartSuffixPrefix.size()) +
        DecimalDigits(total_shard - 1);
    return std::max(
        part_suffix_size,
        static_cast<uint32_t>(kLowLevelManifestSuffix.size()));
}

uint32_t LowLevelLogicalKeyLimit(uint32_t physical_key_limit,
                                 uint32_t total_shard) {
    const uint32_t suffix_size =
        LowLevelPhysicalKeySuffixSize(total_shard);
    return physical_key_limit > suffix_size
               ? physical_key_limit - suffix_size
               : 0;
}

std::string LowLevelChunkKey(std::string_view logical_key, uint32_t shard_id,
                             uint32_t total_shard) {
    if (total_shard <= 1) return std::string(logical_key);
    return std::string(logical_key) +
           std::string(kLowLevelPartSuffixPrefix) +
           std::to_string(shard_id);
}

std::string LowLevelManifestKey(std::string_view logical_key) {
    return std::string(logical_key) + std::string(kLowLevelManifestSuffix);
}

struct KvcsManifestPayload {
    uint32_t total_shard{1};
    uint64_t total_size{0};
    uint64_t chunk_size{0};
};

constexpr std::array<uint8_t, 4> kManifestMagic{'M', 'C', 'M', 'F'};
constexpr uint16_t kManifestVersion = 1;
constexpr uint16_t kManifestHeaderSize = 32;
constexpr size_t kManifestPayloadSize = kManifestHeaderSize;

template <typename Integer>
void StoreLittleEndian(uint8_t* output, Integer value) {
    for (size_t i = 0; i < sizeof(value); ++i) {
        output[i] = static_cast<uint8_t>(value >> (i * 8));
    }
}

template <typename Integer>
Integer LoadLittleEndian(const uint8_t* input) {
    Integer value = 0;
    for (size_t i = 0; i < sizeof(value); ++i) {
        value |= static_cast<Integer>(input[i]) << (i * 8);
    }
    return value;
}

inline std::array<uint8_t, kManifestPayloadSize> EncodeManifest(
    uint64_t total_size, uint64_t chunk_size, uint32_t total_shard) {
    std::array<uint8_t, kManifestPayloadSize> bytes{};
    std::copy(kManifestMagic.begin(), kManifestMagic.end(), bytes.begin());
    StoreLittleEndian(bytes.data() + 4, kManifestVersion);
    StoreLittleEndian(bytes.data() + 6, kManifestHeaderSize);
    StoreLittleEndian(bytes.data() + 8, total_shard);
    StoreLittleEndian(bytes.data() + 12, uint32_t{0});
    StoreLittleEndian(bytes.data() + 16, total_size);
    StoreLittleEndian(bytes.data() + 24, chunk_size);
    return bytes;
}

inline std::optional<KvcsManifestPayload> DecodeManifest(
    std::span<const uint8_t> data) {
    if (data.size() != kManifestPayloadSize ||
        !std::equal(kManifestMagic.begin(), kManifestMagic.end(),
                    data.begin()) ||
        LoadLittleEndian<uint16_t>(data.data() + 4) != kManifestVersion ||
        LoadLittleEndian<uint16_t>(data.data() + 6) != kManifestHeaderSize ||
        LoadLittleEndian<uint32_t>(data.data() + 12) != 0) {
        return std::nullopt;
    }
    KvcsManifestPayload payload{
        .total_shard = LoadLittleEndian<uint32_t>(data.data() + 8),
        .total_size = LoadLittleEndian<uint64_t>(data.data() + 16),
        .chunk_size = LoadLittleEndian<uint64_t>(data.data() + 24),
    };
    if (payload.total_shard == 0) {
        return std::nullopt;
    }
    return payload;
}

struct LowLevelConfig {
    std::string efc_socket;
    uint32_t mountpoint_index = 0;
    // Safe defaults for a 4 MiB value limit: about 96 MiB of ring/shm
    // capacity with one worker per operation class and eight keys per batch.
    uint64_t max_value_size = kStandardDefaultMaxValueSize;
    int max_keys_per_batch = 8;
    int get_workers = 1;
    int set_workers = 1;
    int simple_workers = 1;
    uint32_t max_key_size = kKvcsDefaultMaxKeySize;
    uint64_t operation_timeout_ms = kDefaultOperationTimeoutMs;
    uint64_t query_operation_timeout_ms = kDefaultQueryOperationTimeoutMs;
};

struct StandardConfig {
    std::string efc_socket;
    std::vector<std::string> redis_endpoints;
    std::string redis_password;
    std::string name_space = "mooncake";
    uint32_t max_key_size = kKvcsDefaultMaxKeySize;
    uint64_t max_value_size = kStandardDefaultMaxValueSize;
};

struct CommonConfig {
    std::string efc_socket;
    uint32_t max_key_size = kKvcsDefaultMaxKeySize;
    uint64_t max_value_size = 0;
};

tl::expected<CommonConfig, ErrorCode> ParseCommonConfig(
    std::optional<uint64_t> default_max_value_size) {
    CommonConfig config;
    if (const char* value = std::getenv("MOONCAKE_KVCS_EFC_SOCKET");
        value != nullptr && *value != '\0') {
        config.efc_socket = value;
    } else {
        config.efc_socket = kDefaultEfcSocket;
    }
    if (config.efc_socket.find('\0') != std::string::npos) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (const char* value = std::getenv("MOONCAKE_KVCS_MAX_KEY_SIZE");
        value != nullptr && *value != '\0') {
        auto parsed = TryParseInteger<uint64_t>(value);
        if (!parsed || *parsed == 0 || *parsed > kKvcsMaxKeySize) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.max_key_size = static_cast<uint32_t>(*parsed);
    }

    if (const char* value = std::getenv("MOONCAKE_KVCS_MAX_VALUE_SIZE");
        value != nullptr && *value != '\0') {
        auto parsed = TryParseInteger<uint64_t>(value);
        if (!parsed || *parsed == 0 ||
            *parsed >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.max_value_size = *parsed;
    } else if (default_max_value_size) {
        config.max_value_size = *default_max_value_size;
    } else {
        LOG(ERROR) << "KVCS Low Level mode requires "
                      "MOONCAKE_KVCS_MAX_VALUE_SIZE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return config;
}

tl::expected<int, ErrorCode> ParseNonnegativeIntEnvironment(const char* name,
                                                            int default_value) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return default_value;
    auto parsed = TryParseInteger<uint64_t>(value);
    if (!parsed || *parsed > static_cast<uint64_t>(INT_MAX)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return static_cast<int>(*parsed);
}

tl::expected<LowLevelConfig, ErrorCode> ParseLowLevelConfig() {
    auto common = ParseCommonConfig(kStandardDefaultMaxValueSize);
    if (!common) {
        return tl::make_unexpected(common.error());
    }

    LowLevelConfig config;
    config.efc_socket = std::move(common->efc_socket);
    config.max_key_size = common->max_key_size;
    config.max_value_size = common->max_value_size;
    if (config.max_value_size < kManifestPayloadSize) {
        LOG(ERROR) << "KVCS Low Level max value size must be >= "
                   << kManifestPayloadSize << " for its manifest";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto max_keys = ParseNonnegativeIntEnvironment(
        "MOONCAKE_KVCS_MAX_KEYS_PER_BATCH", config.max_keys_per_batch);
    auto get_workers = ParseNonnegativeIntEnvironment(
        "MOONCAKE_KVCS_GET_WORKERS", config.get_workers);
    auto set_workers = ParseNonnegativeIntEnvironment(
        "MOONCAKE_KVCS_SET_WORKERS", config.set_workers);
    auto simple_workers = ParseNonnegativeIntEnvironment(
        "MOONCAKE_KVCS_SIMPLE_WORKERS", config.simple_workers);
    const char* timeout_value =
        std::getenv("MOONCAKE_KVCS_OPERATION_TIMEOUT_MS");
    if (timeout_value != nullptr && *timeout_value != '\0') {
        auto parsed = TryParseInteger<uint64_t>(timeout_value);
        if (!parsed || *parsed == 0 ||
            *parsed > static_cast<uint64_t>(
                          std::numeric_limits<int64_t>::max() / 1'000'000)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.operation_timeout_ms = *parsed;
    }
    const char* query_timeout_value =
        std::getenv("MOONCAKE_KVCS_QUERY_TIMEOUT_MS");
    if (query_timeout_value != nullptr && *query_timeout_value != '\0') {
        auto parsed = TryParseInteger<uint64_t>(query_timeout_value);
        if (!parsed || *parsed == 0 ||
            *parsed > static_cast<uint64_t>(
                          std::numeric_limits<int64_t>::max() / 1'000'000)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.query_operation_timeout_ms = *parsed;
    }
    if (!max_keys || !get_workers || !set_workers || !simple_workers) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    config.max_keys_per_batch = *max_keys;
    config.get_workers = *get_workers;
    config.set_workers = *set_workers;
    config.simple_workers = *simple_workers;
    return config;
}

tl::expected<StandardConfig, ErrorCode> ParseStandardConfig() {
    auto common = ParseCommonConfig(kStandardDefaultMaxValueSize);
    if (!common) {
        return tl::make_unexpected(common.error());
    }

    StandardConfig config;
    config.efc_socket = std::move(common->efc_socket);
    config.max_key_size = common->max_key_size;
    config.max_value_size = common->max_value_size;

    const char* endpoints = std::getenv("MOONCAKE_KVCS_REDIS_ENDPOINTS");
    if (endpoints == nullptr || *endpoints == '\0') {
        endpoints = std::getenv("KVCS_REDIS_ENDPOINTS");
    }
    if (endpoints == nullptr || *endpoints == '\0') {
        LOG(ERROR) << "KVCS Standard mode requires "
                      "MOONCAKE_KVCS_REDIS_ENDPOINTS";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::string_view remaining(endpoints);
    while (!remaining.empty()) {
        const size_t delimiter = remaining.find(',');
        const auto endpoint =
            TrimAsciiWhitespace(remaining.substr(0, delimiter));
        if (endpoint.empty() || endpoint.find('\0') != std::string_view::npos) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.redis_endpoints.emplace_back(endpoint);
        if (delimiter == std::string_view::npos) break;
        remaining.remove_prefix(delimiter + 1);
    }
    if (config.redis_endpoints.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const char* password = std::getenv("MOONCAKE_KVCS_REDIS_PASSWORD");
    if (password == nullptr) {
        password = std::getenv("KVCS_REDIS_PASSWORD");
    }
    if (password != nullptr) {
        config.redis_password = password;
    }
    if (const char* name_space = std::getenv("MOONCAKE_KVCS_NAMESPACE");
        name_space != nullptr && *name_space != '\0') {
        config.name_space = name_space;
    }
    if (config.name_space.empty() || config.name_space.size() >= 128 ||
        config.name_space.find('\0') != std::string::npos) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return config;
}

ErrorCode MapCallStatus(int status) {
    if (status >= 0) {
        return ErrorCode::OK;
    }
    switch (-status) {
        case KVCS_NOT_FOUND:
            return ErrorCode::OBJECT_NOT_FOUND;
        case KVCS_INCOMPLETE:
            return ErrorCode::KVCS_INCOMPLETE;
        case KVCS_UNAVAILABLE:
            return ErrorCode::KVCS_UNAVAILABLE;
        case KVCS_RESOURCE_EXHAUSTED:
        case KVCS_BUFFER_TOO_SMALL:
            return ErrorCode::KVCS_RESOURCE_EXHAUSTED;
        case KVCS_INVALID_ARGUMENT:
            return ErrorCode::INVALID_PARAMS;
        case KVCS_NAMESPACE_NOT_FOUND:
            return ErrorCode::OBJECT_NOT_FOUND;
        case KVCS_ALREADY_EXISTS:
            return ErrorCode::OBJECT_ALREADY_EXISTS;
        case KVCS_INVALID_MOUNT_POINT:
            return ErrorCode::KVCS_UNAVAILABLE;
        case ETIMEDOUT:
            return ErrorCode::RPC_TIMEOUT;
        default:
            LOG(WARNING) << "Unmapped KVCS call status: " << status;
            return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode MapItemStatus(int status) {
    if (status >= 0) {
        return ErrorCode::OK;
    }
    switch (-status) {
        case ENOENT:
            return ErrorCode::OBJECT_NOT_FOUND;
        case EEXIST:
            return ErrorCode::OBJECT_ALREADY_EXISTS;
        case ENOSPC:
        case ENOMEM:
            return ErrorCode::KVCS_RESOURCE_EXHAUSTED;
        case ENODEV:
            return ErrorCode::KVCS_UNAVAILABLE;
        case ETIMEDOUT:
            return ErrorCode::RPC_TIMEOUT;
        case EINVAL:
        case E2BIG:
        case ENAMETOOLONG:
            return ErrorCode::INVALID_PARAMS;
        default:
            LOG(WARNING) << "Unmapped KVCS item status: " << status;
            return ErrorCode::INTERNAL_ERROR;
    }
}

bool IsDefinitePerItemPutRejection(int status) {
    if (status >= 0) return false;
    switch (-status) {
        case EEXIST:
        case ENOSPC:
        case EINVAL:
        case E2BIG:
        case ENAMETOOLONG:
            return true;
        default:
            return false;
    }
}

bool IsDefinitePerItemDeleteRejection(int status) {
    if (status >= 0) return false;
    switch (-status) {
        case ENOENT:
        case EINVAL:
        case E2BIG:
        case ENAMETOOLONG:
            return true;
        default:
            return false;
    }
}

uint64_t RemainingTimeoutMs(std::chrono::steady_clock::time_point deadline,
                            uint64_t cap_ms) {
    if (deadline == std::chrono::steady_clock::time_point::max()) {
        return cap_ms;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) return 0;
    const auto remaining_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now)
            .count();
    constexpr int64_t kNanosPerMillisecond = 1'000'000;
    const uint64_t remaining_ms = static_cast<uint64_t>(
        (remaining_ns + kNanosPerMillisecond - 1) / kNanosPerMillisecond);
    return std::min(cap_ms, std::max<uint64_t>(1, remaining_ms));
}

int64_t OperationDeadlineNs(uint64_t timeout_ms) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    const auto now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    constexpr int64_t kNanosPerMillisecond = 1'000'000;
    const uint64_t timeout_ns = timeout_ms * kNanosPerMillisecond;
    if (timeout_ns >=
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max() - now_ns)) {
        return std::numeric_limits<int64_t>::max();
    }
    return now_ns + static_cast<int64_t>(timeout_ns);
}

template <typename Results>
Results MakeErrors(size_t count, ErrorCode error) {
    Results results;
    results.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        results.emplace_back(tl::make_unexpected(error));
    }
    return results;
}

template <typename Request>
bool ValidateShardRequest(const Request& request, uint32_t max_key_size,
                          uint64_t max_value_size = 0) {
    if (!IsKvcsKeyStringSafe(request.logical_key, max_key_size) ||
        request.total_shard == 0 || request.shard_id >= request.total_shard ||
        request.size == 0 || request.slices.empty() ||
        request.slices.size() > static_cast<size_t>(INT_MAX) ||
        (max_value_size > 0 && request.size > max_value_size)) {
        return false;
    }
    uint64_t size = 0;
    for (const auto& slice : request.slices) {
        if (slice.ptr == nullptr || slice.size == 0 ||
            slice.size > std::numeric_limits<uint64_t>::max() - size) {
            return false;
        }
        size += slice.size;
    }
    return size == request.size;
}

tl::expected<std::vector<const char*>, ErrorCode> BuildKeyPointers(
    std::span<const ObjectKey> keys, uint32_t max_key_size) {
    if (keys.size() > static_cast<size_t>(INT_MAX)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<const char*> pointers;
    pointers.reserve(keys.size());
    for (const auto& key : keys) {
        if (!IsKvcsKeyStringSafe(key, max_key_size)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        pointers.push_back(key.c_str());
    }
    return pointers;
}

class BatchGetBuffers {
   public:
    explicit BatchGetBuffers(std::span<const KvcsShardGetRequest> requests)
        : staging_(requests.size()) {
        buffers_.reserve(requests.size());
        capacities_.reserve(requests.size());
        for (size_t i = 0; i < requests.size(); ++i) {
            if (requests[i].slices.size() == 1) {
                buffers_.push_back(requests[i].slices[0].ptr);
            } else {
                staging_[i].resize(static_cast<size_t>(requests[i].size));
                buffers_.push_back(staging_[i].data());
            }
            capacities_.push_back(static_cast<size_t>(requests[i].size));
        }
    }

    void** buffers() { return buffers_.data(); }
    size_t* capacities() { return capacities_.data(); }

    KvcsGetShardResults Finish(std::span<const KvcsShardGetRequest> requests,
                               std::span<const int> statuses,
                               std::span<const size_t> lengths) {
        KvcsGetShardResults results;
        results.reserve(requests.size());
        for (size_t i = 0; i < requests.size(); ++i) {
            if (statuses[i] > 0 && lengths[i] == requests[i].size) {
                size_t offset = 0;
                for (const auto& slice : requests[i].slices) {
                    if (!staging_[i].empty()) {
                        std::memcpy(slice.ptr, staging_[i].data() + offset,
                                    slice.size);
                    }
                    offset += slice.size;
                }
                results.emplace_back();
            } else {
                const ErrorCode error =
                    statuses[i] == 0
                        ? ErrorCode::OBJECT_NOT_FOUND
                        : (statuses[i] < 0 ? MapItemStatus(statuses[i])
                                           : ErrorCode::INTERNAL_ERROR);
                results.emplace_back(tl::make_unexpected(error));
            }
        }
        return results;
    }

   private:
    std::vector<std::vector<char>> staging_;
    std::vector<void*> buffers_;
    std::vector<size_t> capacities_;
};

class QueryResults {
   public:
    explicit QueryResults(size_t count) : values_(count) {}

    ~QueryResults() {
        if (initialized_ > 0) {
            kvcs_query_result_free(values_.data(), initialized_);
        }
    }

    kvcs_query_result_t* data() { return values_.data(); }
    const kvcs_query_result_t& operator[](size_t index) const {
        return values_[index];
    }

    tl::expected<void, ErrorCode> Complete(int returned) {
        if (returned < 0) {
            const auto error = MapCallStatus(returned);
            LOG(WARNING) << "KVCS query call failed, status=" << returned
                         << ", mapped_error=" << toString(error);
            return tl::make_unexpected(error);
        }
        const int requested = static_cast<int>(values_.size());
        initialized_ =
            returned == 0 ? requested : std::min(returned, requested);
        if (returned != 0 && returned != requested) {
            LOG(WARNING) << "KVCS query returned an unexpected result count, "
                         << "requested=" << requested
                         << ", returned=" << returned;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return {};
    }

   private:
    std::vector<kvcs_query_result_t> values_;
    int initialized_ = 0;
};

std::string_view BoundedString(const char* value, size_t capacity) {
    return std::string_view(value, strnlen(value, capacity));
}

class KvcsStandardCapiDriver final : public KvcsDriver {
   public:
    explicit KvcsStandardCapiDriver(StandardConfig config)
        : efc_socket_(std::move(config.efc_socket)),
          redis_endpoints_(std::move(config.redis_endpoints)),
          redis_password_(std::move(config.redis_password)),
          namespace_(std::move(config.name_space)),
          max_key_size_(config.max_key_size),
          max_value_size_(config.max_value_size) {}

    ~KvcsStandardCapiDriver() override {
        if (client_ != nullptr) {
            kvcs_client_destroy(client_);
        }
    }

    tl::expected<void, ErrorCode> Init() override {
        if (client_ != nullptr) return {};

        std::vector<const char*> endpoint_pointers;
        endpoint_pointers.reserve(redis_endpoints_.size());
        for (const auto& endpoint : redis_endpoints_) {
            endpoint_pointers.push_back(endpoint.c_str());
        }

        kvcs_client_config_t config{};
        config.efc_socket = efc_socket_.c_str();
        config.redis_endpoints = endpoint_pointers.data();
        config.redis_count = static_cast<int>(endpoint_pointers.size());
        config.redis_password =
            redis_password_.empty() ? nullptr : redis_password_.c_str();
        config.max_key_size = static_cast<int>(max_key_size_);
        config.max_value_size = static_cast<int64_t>(max_value_size_);
        config.enable_metrics = 1;
        client_ = kvcs_client_create(&config);
        if (client_ == nullptr) {
            return tl::make_unexpected(errno == EINVAL
                                           ? ErrorCode::INVALID_PARAMS
                                           : ErrorCode::KVCS_UNAVAILABLE);
        }

        kvcs_namespace_info_t namespace_info{};
        const int get_status =
            kvcs_get_namespace(client_, namespace_.c_str(), &namespace_info);
        if (get_status == -KVCS_NAMESPACE_NOT_FOUND) {
            kvcs_create_ns_opts_t options{};
            const int create_status =
                kvcs_create_namespace(client_, namespace_.c_str(), &options);
            if (create_status < 0 && create_status != -KVCS_ALREADY_EXISTS) {
                const ErrorCode error = MapCallStatus(create_status);
                kvcs_client_destroy(client_);
                client_ = nullptr;
                return tl::make_unexpected(error);
            }
        } else if (get_status < 0) {
            const ErrorCode error = MapCallStatus(get_status);
            kvcs_client_destroy(client_);
            client_ = nullptr;
            return tl::make_unexpected(error);
        }
        return {};
    }

    uint64_t MaxValueSize() const override { return max_value_size_; }
    uint32_t MaxKeySize() const override { return max_key_size_; }

    KvcsPutShardResults BatchPut(
        std::span<const KvcsShardPutRequest> requests) override {
        if (client_ == nullptr) {
            return MakeErrors<KvcsPutShardResults>(requests.size(),
                                                   ErrorCode::INTERNAL_ERROR);
        }
        if (requests.empty()) return {};
        if (requests.size() > static_cast<size_t>(INT_MAX)) {
            return MakeErrors<KvcsPutShardResults>(requests.size(),
                                                   ErrorCode::INVALID_PARAMS);
        }

        std::vector<std::vector<const void*>> segment_pointers(requests.size());
        std::vector<std::vector<size_t>> segment_lengths(requests.size());
        std::vector<kvcs_put_item_t> items;
        items.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            if (!ValidateRequest(request)) {
                return MakeErrors<KvcsPutShardResults>(
                    requests.size(), ErrorCode::INVALID_PARAMS);
            }
            auto& pointers = segment_pointers[index];
            auto& lengths = segment_lengths[index];
            pointers.reserve(request.slices.size());
            lengths.reserve(request.slices.size());
            for (const auto& slice : request.slices) {
                pointers.push_back(slice.ptr);
                lengths.push_back(slice.size);
            }
            items.push_back(kvcs_put_item_t{
                .key = request.logical_key.c_str(),
                .value_segs = pointers.data(),
                .seg_lens = lengths.data(),
                .seg_count = static_cast<int>(pointers.size()),
                .shard_id = static_cast<int32_t>(request.shard_id),
                .total_shard = static_cast<int32_t>(request.total_shard),
                .location = nullptr,
                .meta = nullptr,
                .meta_count = 0,
            });
        }

        std::vector<kvcs_put_result_t> native_results(requests.size());
        const int count = static_cast<int>(requests.size());
        const int call_status =
            kvcs_batch_put(client_, namespace_.c_str(), items.data(), count,
                           native_results.data(), count, 0);
        if (call_status < 0) {
            return MakeErrors<KvcsPutShardResults>(requests.size(),
                                                   MapCallStatus(call_status));
        }

        KvcsPutShardResults results;
        results.reserve(requests.size());
        for (const auto& native : native_results) {
            if (native.status >= 0) {
                results.emplace_back();
            } else {
                results.emplace_back(
                    tl::make_unexpected(MapItemStatus(native.status)));
            }
        }

        // Standard mode reports per-shard results, but a multi-shard object
        // must not be left partially published when one shard is rejected.
        // Roll back only accepted shards belonging to a failed logical key:
        // another object in the same SDK batch may still have completed
        // successfully.
        std::unordered_set<ObjectKey> failed_keys;
        for (size_t index = 0; index < results.size(); ++index) {
            if (!results[index]) {
                failed_keys.insert(requests[index].logical_key);
            }
        }
        std::vector<kvcs_delete_item_t> rollback_items;
        std::vector<size_t> rollback_request_indices;
        rollback_items.reserve(requests.size());
        rollback_request_indices.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            if (native_results[index].status >= 0 &&
                failed_keys.contains(requests[index].logical_key)) {
                rollback_items.push_back(kvcs_delete_item_t{
                    .key = requests[index].logical_key.c_str(),
                    .shard_id = static_cast<int32_t>(requests[index].shard_id),
                    .location = nullptr,
                });
                rollback_request_indices.push_back(index);
            }
        }
        if (!rollback_items.empty()) {
            std::vector<kvcs_delete_result_t> rollback_results(
                rollback_items.size());
            const int rollback_count = static_cast<int>(rollback_items.size());
            const int rollback_status = kvcs_batch_delete_items(
                client_, namespace_.c_str(), rollback_items.data(),
                rollback_count, rollback_results.data(), rollback_count, 0);
            if (rollback_status < 0) {
                LOG(WARNING) << "KVCS Standard rollback failed, status="
                             << rollback_status << ", mapped_error="
                             << toString(MapCallStatus(rollback_status));
            } else {
                for (size_t index = 0; index < rollback_results.size();
                     ++index) {
                    if (rollback_results[index].status < 0) {
                        const size_t request_index =
                            rollback_request_indices[index];
                        LOG(WARNING)
                            << "KVCS Standard rollback item failed, status="
                            << rollback_results[index].status
                            << ", key=" << requests[request_index].logical_key
                            << ", shard_id=" << requests[request_index].shard_id
                            << ", mapped_error="
                            << toString(MapItemStatus(
                                   rollback_results[index].status));
                    }
                }
            }
        }
        return results;
    }

    KvcsDriverQueryResults BatchQuery(
        std::span<const ObjectKey> logical_keys) override {
        if (client_ == nullptr) {
            return MakeErrors<KvcsDriverQueryResults>(
                logical_keys.size(), ErrorCode::INTERNAL_ERROR);
        }
        if (logical_keys.empty()) return {};
        auto key_pointers = BuildKeyPointers(logical_keys, max_key_size_);
        if (!key_pointers)
            return MakeErrors<KvcsDriverQueryResults>(logical_keys.size(),
                                                      key_pointers.error());

        QueryResults native(logical_keys.size());
        kvcs_query_opts_t options{
            .renew = 1, .with_shards = 1, .kvstore_fallback = 1};
        const int count = static_cast<int>(logical_keys.size());
        const int returned =
            kvcs_batch_query(client_, namespace_.c_str(), key_pointers->data(),
                             count, &options, native.data(), count);
        auto completion = native.Complete(returned);
        if (!completion) {
            return MakeErrors<KvcsDriverQueryResults>(logical_keys.size(),
                                                      completion.error());
        }

        KvcsDriverQueryResults results;
        results.reserve(logical_keys.size());
        for (size_t index = 0; index < logical_keys.size(); ++index) {
            const auto& result = native[index];
            const auto status =
                BoundedString(result.status, sizeof(result.status));
            if (status == "not_found") {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
                continue;
            }
            if (status == "incomplete") {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE));
                continue;
            }
            if (status != "ok" || result.total_shard <= 0 ||
                result.total_size <= 0 || result.shard_count <= 0 ||
                result.shards == nullptr) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
                continue;
            }

            KvcsManifest manifest{
                .logical_key = logical_keys[index],
                .total_shard = static_cast<uint32_t>(result.total_shard),
                .total_size = static_cast<uint64_t>(result.total_size),
                .shards = {},
            };
            manifest.shards.reserve(static_cast<size_t>(result.shard_count));
            bool valid = result.shard_count == result.total_shard;
            for (int shard_index = 0; valid && shard_index < result.shard_count;
                 ++shard_index) {
                const auto& shard = result.shards[shard_index];
                if (shard.shard_id < 0 || shard.size <= 0) {
                    valid = false;
                    break;
                }
                manifest.shards.push_back(KvcsShardLocation{
                    .shard_id = static_cast<uint32_t>(shard.shard_id),
                    .size = static_cast<uint64_t>(shard.size),
                });
            }
            if (valid) {
                results.emplace_back(std::move(manifest));
            } else {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
            }
        }
        return results;
    }

    KvcsGetShardResults BatchGet(
        std::span<const KvcsShardGetRequest> requests) override {
        if (client_ == nullptr) {
            return MakeErrors<KvcsGetShardResults>(requests.size(),
                                                   ErrorCode::INTERNAL_ERROR);
        }
        if (requests.empty()) return {};
        if (requests.size() > static_cast<size_t>(INT_MAX)) {
            return MakeErrors<KvcsGetShardResults>(requests.size(),
                                                   ErrorCode::INVALID_PARAMS);
        }

        std::vector<kvcs_get_item_t> items;
        items.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            if (!ValidateRequest(request)) {
                return MakeErrors<KvcsGetShardResults>(
                    requests.size(), ErrorCode::INVALID_PARAMS);
            }
            items.push_back(kvcs_get_item_t{
                .key = request.logical_key.c_str(),
                .shard_id = static_cast<int32_t>(request.shard_id),
                // Standard-mode replica metadata stays authoritative in
                // Redis. An unhinted read lets the SDK select a healthy
                // replica.
                .location = nullptr,
                .expected_value_size = static_cast<size_t>(request.size),
                .meta = nullptr,
                .meta_count = 0,
            });
        }

        BatchGetBuffers output(requests);
        std::vector<int> statuses(requests.size());
        std::vector<size_t> lengths(requests.size());
        const int count = static_cast<int>(requests.size());
        const int call_status = kvcs_batch_get_into(
            client_, namespace_.c_str(), items.data(), count, output.buffers(),
            output.capacities(), statuses.data(), lengths.data(), 0);
        if (call_status < 0) {
            return MakeErrors<KvcsGetShardResults>(requests.size(),
                                                   MapCallStatus(call_status));
        }
        return output.Finish(requests, statuses, lengths);
    }

    KvcsDeleteResults BatchDelete(
        std::span<const ObjectKey> logical_keys) override {
        if (client_ == nullptr) {
            return MakeErrors<KvcsDeleteResults>(logical_keys.size(),
                                                 ErrorCode::INTERNAL_ERROR);
        }
        if (logical_keys.empty()) return {};

        auto queries = BatchQuery(logical_keys);
        if (queries.size() != logical_keys.size()) {
            return MakeErrors<KvcsDeleteResults>(logical_keys.size(),
                                                 ErrorCode::INTERNAL_ERROR);
        }
        KvcsDeleteResults results(logical_keys.size());
        std::vector<kvcs_delete_item_t> items;
        std::vector<size_t> owners;
        for (size_t index = 0; index < queries.size(); ++index) {
            const auto& query = queries[index];
            if (!query) {
                if (query.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    results[index] = tl::make_unexpected(query.error());
                }
                continue;
            }
            for (const auto& shard : query->shards) {
                items.push_back(kvcs_delete_item_t{
                    .key = logical_keys[index].c_str(),
                    .shard_id = static_cast<int32_t>(shard.shard_id),
                    // An unhinted Standard delete resolves and removes every
                    // replica recorded in Redis.
                    .location = nullptr,
                });
                owners.push_back(index);
            }
        }
        if (items.empty()) return results;
        if (items.size() > static_cast<size_t>(INT_MAX)) {
            return MakeErrors<KvcsDeleteResults>(logical_keys.size(),
                                                 ErrorCode::INVALID_PARAMS);
        }

        std::vector<kvcs_delete_result_t> native_results(items.size());
        const int count = static_cast<int>(items.size());
        const int call_status =
            kvcs_batch_delete_items(client_, namespace_.c_str(), items.data(),
                                    count, native_results.data(), count, 0);
        if (call_status < 0) {
            return MakeErrors<KvcsDeleteResults>(logical_keys.size(),
                                                 MapCallStatus(call_status));
        }
        for (size_t index = 0; index < native_results.size(); ++index) {
            if (native_results[index].status >= 0) continue;
            const ErrorCode error = MapItemStatus(native_results[index].status);
            if (error != ErrorCode::OBJECT_NOT_FOUND &&
                results[owners[index]]) {
                results[owners[index]] = tl::make_unexpected(error);
            }
        }
        return results;
    }

   private:
    template <typename Req>
    bool ValidateRequest(const Req& request) const {
        return ValidateShardRequest(request, max_key_size_);
    }

    std::string efc_socket_;
    std::vector<std::string> redis_endpoints_;
    std::string redis_password_;
    std::string namespace_;
    uint32_t max_key_size_ = kKvcsDefaultMaxKeySize;
    uint64_t max_value_size_ = kStandardDefaultMaxValueSize;
    kvcs_client_t* client_ = nullptr;
};

class KvcsLowLevelClientState {
   public:
    explicit KvcsLowLevelClientState(const LowLevelConfig& config)
        : efc_socket_(config.efc_socket),
          max_key_size_(config.max_key_size),
          max_value_size_(config.max_value_size),
          max_keys_per_batch_(config.max_keys_per_batch),
          get_workers_(config.get_workers),
          set_workers_(config.set_workers),
          simple_workers_(config.simple_workers) {}

    ~KvcsLowLevelClientState() {
        if (client_ != nullptr) kvcs_ll_client_destroy(client_);
    }

    tl::expected<kvcs_ll_client_t*, ErrorCode> Init() {
        std::lock_guard lock(mutex_);
        if (client_ != nullptr) return client_;

        kvcs_ll_config_t config{};
        config.efc_socket = efc_socket_.c_str();
        config.max_key_size = static_cast<int>(max_key_size_);
        config.max_value_size = static_cast<int64_t>(max_value_size_);
        config.max_keys_per_batch = max_keys_per_batch_;
        config.get_workers = get_workers_;
        config.set_workers = set_workers_;
        config.simple_workers = simple_workers_;
        client_ = kvcs_ll_create(&config);
        if (client_ == nullptr) {
            return tl::make_unexpected(errno == EINVAL
                                           ? ErrorCode::INVALID_PARAMS
                                           : ErrorCode::KVCS_UNAVAILABLE);
        }
        return client_;
    }

   private:
    std::string efc_socket_;
    uint32_t max_key_size_ = kKvcsDefaultMaxKeySize;
    uint64_t max_value_size_ = 0;
    int max_keys_per_batch_ = 0;
    int get_workers_ = 0;
    int set_workers_ = 0;
    int simple_workers_ = 0;
    std::mutex mutex_;
    kvcs_ll_client_t* client_ = nullptr;
};

class KvcsCapiDriver final : public KvcsDriver {
   public:
    KvcsCapiDriver(LowLevelConfig config,
                   std::shared_ptr<KvcsLowLevelClientState> client_state)
        : mountpoint_index_(config.mountpoint_index),
          max_key_size_(config.max_key_size),
          max_value_size_(config.max_value_size),
          query_batch_size_(config.max_keys_per_batch > 0
                                ? static_cast<size_t>(config.max_keys_per_batch)
                                : 0),
          operation_timeout_ms_(config.operation_timeout_ms),
          query_operation_timeout_ms_(config.query_operation_timeout_ms),
          client_state_(std::move(client_state)) {}

    ~KvcsCapiDriver() override = default;

    tl::expected<void, ErrorCode> Init() override {
        if (client_ != nullptr) {
            return {};
        }
        if (!client_state_)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        auto client = client_state_->Init();
        if (!client) return tl::make_unexpected(client.error());
        client_ = client.value();
        return {};
    }

    uint64_t MaxValueSize() const override { return max_value_size_; }
    uint32_t MaxKeySize() const override { return max_key_size_; }

    KvcsPutShardResults BatchPut(
        std::span<const KvcsShardPutRequest> requests) override {
        if (client_ == nullptr) {
            return MakeErrors<KvcsPutShardResults>(requests.size(),
                                                   ErrorCode::INTERNAL_ERROR);
        }
        if (requests.empty()) {
            return {};
        }

        std::vector<PutGroup> groups;
        if (!ValidatePutRequests(requests, groups)) {
            return MakeErrors<KvcsPutShardResults>(requests.size(),
                                                   ErrorCode::INVALID_PARAMS);
        }

        std::vector<RawPutRecord> records;
        std::vector<uint8_t> reuse_matching_existing;
        std::vector<uint8_t> eligible(requests.size(), 1);
        records.reserve(requests.size());
        reuse_matching_existing.reserve(requests.size());
        for (const auto& request : requests) {
            records.push_back(RawPutRecord{
                .logical_key = request.logical_key,
                .key = LowLevelChunkKey(
                    request.logical_key, request.shard_id, request.total_shard),
                .slices = request.slices,
                .size = request.size,
            });
            reuse_matching_existing.push_back(request.total_shard > 1);
        }
        for (const auto& group : groups) {
            bool quarantined =
                IsLogicalKeyQuarantined(group.logical_key) ||
                (group.total_shard > 1 &&
                 IsPhysicalRecordQuarantined(
                     LowLevelManifestKey(group.logical_key)));
            for (const size_t request_index : group.request_indices) {
                quarantined =
                    quarantined ||
                    IsPhysicalRecordQuarantined(records[request_index].key);
            }
            if (!quarantined) continue;
            for (const size_t request_index : group.request_indices) {
                eligible[request_index] = 0;
            }
            LOG(ERROR)
                << "KVCS Low Level Put rejected because a physical key has "
                   "an indeterminate earlier operation, key="
                << group.logical_key
                << ", mountpoint_index=" << mountpoint_index_;
        }
        auto chunk_put =
            PutRecords(records, reuse_matching_existing, eligible);
        KvcsPutShardResults results = std::move(chunk_put.results);

        // Low-Level batches are not atomic. If one chunk fails, remove only
        // sibling chunks this call definitely inserted. Deterministic
        // physical keys do not identify write ownership, so deleting failed or
        // ambiguous keys could destroy an existing or concurrent object.
        for (const auto& group : groups) {
            const bool failed = std::any_of(
                group.request_indices.begin(), group.request_indices.end(),
                [&results](size_t index) { return !results[index]; });
            if (!failed) continue;

            std::optional<ErrorCode> group_error;
            const bool rollback_safe = std::all_of(
                group.request_indices.begin(), group.request_indices.end(),
                [&chunk_put](size_t index) {
                    return chunk_put.outcome_known[index];
                });
            for (size_t request_index : group.request_indices) {
                if (!results[request_index]) {
                    const ErrorCode error = results[request_index].error();
                    if (!group_error || (IsKvcsTransientError(*group_error) &&
                                         !IsKvcsTransientError(error))) {
                        group_error = error;
                    }
                }
            }

            ErrorCode put_error =
                rollback_safe
                    ? group_error.value_or(ErrorCode::INTERNAL_ERROR)
                    : ErrorCode::KVCS_INCOMPLETE;
            if (rollback_safe) {
                std::vector<std::string> rollback_keys;
                rollback_keys.reserve(group.request_indices.size());
                for (size_t request_index : group.request_indices) {
                    if (chunk_put.inserted[request_index]) {
                        rollback_keys.push_back(records[request_index].key);
                    }
                }
                auto cleanup = DeletePhysicalRecords(rollback_keys,
                                                     group.logical_key);
                if (!cleanup) {
                    put_error = ErrorCode::KVCS_INCOMPLETE;
                    LOG(WARNING)
                        << "KVCS Low Level chunk rollback failed, key="
                        << group.logical_key
                        << ", put_error=" << toString(put_error)
                        << ", cleanup_error=" << toString(cleanup.error())
                        << ", mountpoint_index=" << mountpoint_index_;
                }
            } else {
                const size_t inserted_chunks = std::count_if(
                    group.request_indices.begin(), group.request_indices.end(),
                    [&chunk_put](size_t index) {
                        return chunk_put.inserted[index];
                    });
                LOG(WARNING)
                    << "KVCS Low Level chunk rollback skipped because a Put "
                       "outcome is ambiguous, key="
                    << group.logical_key
                    << ", put_error=" << toString(put_error)
                    << ", inserted_chunks=" << inserted_chunks
                    << ", mountpoint_index=" << mountpoint_index_;
            }
            for (size_t request_index : group.request_indices) {
                results[request_index] = tl::make_unexpected(put_error);
            }
        }

        std::vector<size_t> manifest_groups;
        for (size_t group_index = 0; group_index < groups.size();
             ++group_index) {
            const auto& group = groups[group_index];
            if (group.total_shard == 1) {
                continue;
            }
            const bool data_ok = std::all_of(
                group.request_indices.begin(), group.request_indices.end(),
                [&results](size_t index) {
                    return results[index].has_value();
                });
            if (data_ok) {
                manifest_groups.push_back(group_index);
            }
        }
        if (manifest_groups.empty()) {
            return results;
        }

        std::vector<std::array<uint8_t, kManifestPayloadSize>> manifest_values(
            manifest_groups.size());
        std::vector<Slice> manifest_slices(manifest_groups.size());
        std::vector<RawPutRecord> manifest_records;
        manifest_records.reserve(manifest_groups.size());
        for (size_t index = 0; index < manifest_groups.size(); ++index) {
            const auto& group = groups[manifest_groups[index]];
            manifest_values[index] = EncodeManifest(
                group.total_size, max_value_size_, group.total_shard);
            manifest_slices[index] = Slice{manifest_values[index].data(),
                                           manifest_values[index].size()};
            manifest_records.push_back(RawPutRecord{
                .logical_key = group.logical_key,
                .key = LowLevelManifestKey(group.logical_key),
                .slices = std::span<const Slice>(&manifest_slices[index], 1),
                .size = manifest_values[index].size(),
            });
        }

        auto manifest_put = PutRecords(manifest_records);
        auto& manifest_results = manifest_put.results;
        for (size_t index = 0; index < manifest_results.size(); ++index) {
            if (manifest_results[index]) {
                continue;
            }
            const auto& group = groups[manifest_groups[index]];
            ErrorCode put_error = manifest_results[index].error();
            if (manifest_put.outcome_known[index]) {
                std::vector<std::string> rollback_keys;
                rollback_keys.reserve(group.request_indices.size());
                for (size_t request_index : group.request_indices) {
                    if (chunk_put.inserted[request_index]) {
                        rollback_keys.push_back(records[request_index].key);
                    }
                }
                auto cleanup = DeletePhysicalRecords(rollback_keys,
                                                     group.logical_key);
                if (!cleanup) {
                    put_error = ErrorCode::KVCS_INCOMPLETE;
                    LOG(WARNING)
                        << "KVCS Low Level manifest rollback failed, key="
                        << group.logical_key
                        << ", put_error=" << toString(put_error)
                        << ", cleanup_error=" << toString(cleanup.error())
                        << ", mountpoint_index=" << mountpoint_index_;
                }
            } else {
                put_error = ErrorCode::KVCS_INCOMPLETE;
                LOG(WARNING)
                    << "KVCS Low Level manifest rollback skipped because the "
                       "manifest Put outcome is ambiguous, key="
                    << group.logical_key
                    << ", put_error=" << toString(put_error)
                    << ", inserted_chunks="
                    << std::count_if(
                           group.request_indices.begin(),
                           group.request_indices.end(),
                           [&chunk_put](size_t request_index) {
                               return chunk_put.inserted[request_index];
                           })
                    << ", mountpoint_index=" << mountpoint_index_;
            }
            for (size_t request_index : group.request_indices) {
                results[request_index] = tl::make_unexpected(put_error);
            }
        }
        return results;
    }

    KvcsDriverQueryResults BatchQuery(
        std::span<const ObjectKey> logical_keys) override {
        return BatchQueryUntil(
            logical_keys,
            std::chrono::steady_clock::now() +
                std::chrono::milliseconds(query_operation_timeout_ms_));
    }

    KvcsDriverQueryResults BatchQueryUntil(
        std::span<const ObjectKey> logical_keys,
        std::chrono::steady_clock::time_point deadline) override {
        if (client_ == nullptr) {
            return MakeErrors<KvcsDriverQueryResults>(
                logical_keys.size(), ErrorCode::INTERNAL_ERROR);
        }
        if (logical_keys.empty()) return {};

        KvcsDriverQueryResults results = MakeErrors<KvcsDriverQueryResults>(
            logical_keys.size(), ErrorCode::INTERNAL_ERROR);
        std::vector<ObjectKey> active_keys;
        std::vector<size_t> active_indices;
        active_keys.reserve(logical_keys.size());
        active_indices.reserve(logical_keys.size());
        for (size_t index = 0; index < logical_keys.size(); ++index) {
            if (IsLogicalKeyQuarantined(logical_keys[index])) {
                results[index] =
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE);
                continue;
            }
            active_keys.push_back(logical_keys[index]);
            active_indices.push_back(index);
        }
        if (active_keys.empty()) return results;

        const size_t batch_size =
            query_batch_size_ == 0
                ? active_keys.size()
                : std::min(query_batch_size_, active_keys.size());
        for (size_t begin = 0; begin < active_keys.size();
             begin += batch_size) {
            const size_t count =
                std::min(batch_size, active_keys.size() - begin);
            auto chunk = QueryChunkUntil(
                std::span<const ObjectKey>(active_keys).subspan(begin, count),
                deadline);
            if (chunk.size() != count) {
                return MakeErrors<KvcsDriverQueryResults>(
                    logical_keys.size(), ErrorCode::INTERNAL_ERROR);
            }
            for (size_t offset = 0; offset < count; ++offset) {
                results[active_indices[begin + offset]] =
                    std::move(chunk[offset]);
            }
        }
        return results;
    }

    KvcsGetShardResults BatchGet(
        std::span<const KvcsShardGetRequest> requests) override {
        if (client_ == nullptr) {
            return MakeErrors<KvcsGetShardResults>(requests.size(),
                                                   ErrorCode::INTERNAL_ERROR);
        }
        if (requests.empty()) {
            return {};
        }
        if (requests.size() > static_cast<size_t>(INT_MAX)) {
            return MakeErrors<KvcsGetShardResults>(requests.size(),
                                                   ErrorCode::INVALID_PARAMS);
        }

        KvcsGetShardResults results(
            requests.size(), tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        std::vector<size_t> active_indices;
        std::vector<KvcsShardGetRequest> active_requests;
        active_indices.reserve(requests.size());
        active_requests.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            if (IsLogicalKeyQuarantined(request.logical_key)) {
                results[index] =
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE);
                continue;
            }
            if (!ValidateGetRequest(request)) {
                return MakeErrors<KvcsGetShardResults>(
                    requests.size(), ErrorCode::INVALID_PARAMS);
            }
            active_indices.push_back(index);
            active_requests.push_back(request);
        }
        if (active_requests.empty()) return results;

        std::vector<std::string> keys;
        std::vector<kvcs_ll_get_item_t> items;
        keys.reserve(active_requests.size());
        items.reserve(active_requests.size());
        for (const auto& request : active_requests) {
            keys.push_back(LowLevelChunkKey(
                request.logical_key, request.shard_id, request.total_shard));
            if (!IsKvcsKeyStringSafe(keys.back(), max_key_size_)) {
                return MakeErrors<KvcsGetShardResults>(
                    requests.size(), ErrorCode::INVALID_PARAMS);
            }
        }
        for (size_t index = 0; index < active_requests.size(); ++index) {
            items.push_back(kvcs_ll_get_item_t{.key = keys[index].c_str()});
        }

        BatchGetBuffers output(active_requests);
        std::vector<int> statuses(active_requests.size());
        std::vector<size_t> lengths(active_requests.size());
        const int count = static_cast<int>(active_requests.size());
        const int call_status = kvcs_ll_batch_get_into(
            client_, items.data(), count, output.buffers(), output.capacities(),
            statuses.data(), lengths.data(),
            OperationDeadlineNs(operation_timeout_ms_), &batch_options_);
        if (call_status < 0) {
            return MakeErrors<KvcsGetShardResults>(requests.size(),
                                                   MapCallStatus(call_status));
        }
        auto active_results = output.Finish(active_requests, statuses, lengths);
        for (size_t index = 0; index < active_results.size(); ++index) {
            results[active_indices[index]] = std::move(active_results[index]);
        }
        return results;
    }

    KvcsDeleteResults BatchDelete(
        std::span<const ObjectKey> logical_keys) override {
        if (client_ == nullptr) {
            return MakeErrors<KvcsDeleteResults>(logical_keys.size(),
                                                 ErrorCode::INTERNAL_ERROR);
        }
        if (logical_keys.empty()) {
            return {};
        }
        if (logical_keys.size() > static_cast<size_t>(INT_MAX)) {
            return MakeErrors<KvcsDeleteResults>(logical_keys.size(),
                                                 ErrorCode::INVALID_PARAMS);
        }

        auto query_results = BatchQuery(logical_keys);
        if (query_results.size() != logical_keys.size()) {
            return MakeErrors<KvcsDeleteResults>(logical_keys.size(),
                                                 ErrorCode::INTERNAL_ERROR);
        }
        KvcsDeleteResults results(logical_keys.size());
        std::vector<std::string> data_keys;
        std::vector<size_t> data_owners;
        std::vector<std::string> manifest_keys;
        std::vector<size_t> manifest_owners;
        for (size_t index = 0; index < query_results.size(); ++index) {
            if (IsLogicalKeyQuarantined(logical_keys[index])) {
                results[index] =
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE);
                continue;
            }
            const auto& query = query_results[index];
            if (!query) {
                if (query.error() == ErrorCode::OBJECT_NOT_FOUND) {
                    continue;
                }
                results[index] = tl::make_unexpected(query.error());
                continue;
            }

            const auto& manifest = *query;
            if (!manifest.layout_known || manifest.total_shard <= 1) {
                data_keys.push_back(logical_keys[index]);
                data_owners.push_back(index);
                continue;
            }
            auto validation = ValidateKvcsManifest(manifest);
            if (!validation) {
                results[index] = tl::make_unexpected(validation.error());
                continue;
            }
            for (uint32_t shard = 0; shard < manifest.total_shard; ++shard) {
                data_keys.push_back(LowLevelChunkKey(
                    logical_keys[index], shard, manifest.total_shard));
                data_owners.push_back(index);
            }
            manifest_keys.push_back(
                LowLevelManifestKey(logical_keys[index]));
            manifest_owners.push_back(index);
        }

        auto delete_physical = [&](std::span<const std::string> keys,
                                   std::span<const size_t> owners) {
            if (keys.empty()) return;
            if (keys.size() != owners.size() ||
                keys.size() > static_cast<size_t>(INT_MAX)) {
                for (size_t owner : owners) {
                    if (results[owner]) {
                        results[owner] =
                            tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                }
                return;
            }
            const bool invalid_key =
                std::any_of(keys.begin(), keys.end(), [this](const auto& key) {
                    return !IsKvcsKeyStringSafe(key, max_key_size_);
                });
            if (invalid_key) {
                for (const size_t owner : owners) {
                    if (results[owner]) {
                        results[owner] =
                            tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                }
                return;
            }

            std::vector<const char*> key_pointers;
            key_pointers.reserve(keys.size());
            for (const auto& key : keys) {
                key_pointers.push_back(key.c_str());
            }
            std::vector<int> statuses(keys.size());
            const int count = static_cast<int>(keys.size());
            const int call_status = kvcs_ll_batch_delete(
                client_, key_pointers.data(), count, statuses.data(), count,
                OperationDeadlineNs(operation_timeout_ms_), &batch_options_);
            if (call_status < 0) {
                const ErrorCode error = MapCallStatus(call_status);
                QuarantinePhysicalRecords(keys, "delete-call", error);
                for (const size_t owner : owners) {
                    QuarantineLogicalKey(logical_keys[owner], "delete-call",
                                         error);
                }
                for (size_t owner : owners) {
                    if (results[owner]) {
                        results[owner] = tl::make_unexpected(error);
                    }
                }
                return;
            }

            for (size_t index = 0; index < statuses.size(); ++index) {
                if (statuses[index] >= 0) continue;
                const ErrorCode error = MapItemStatus(statuses[index]);
                if (error == ErrorCode::OBJECT_NOT_FOUND) continue;
                if (!IsDefinitePerItemDeleteRejection(statuses[index])) {
                    const std::array<std::string, 1> ambiguous_key{
                        keys[index]};
                    QuarantinePhysicalRecords(ambiguous_key, "delete-item",
                                              error);
                    QuarantineLogicalKey(logical_keys[owners[index]],
                                         "delete-item", error);
                }
                const size_t owner = owners[index];
                if (results[owner]) {
                    results[owner] = tl::make_unexpected(error);
                }
            }
        };

        // Keep the manifest until every chunk is gone. A failed delete can
        // then be retried without losing the metadata needed to locate the
        // remaining chunks.
        delete_physical(data_keys, data_owners);

        std::vector<std::string> ready_manifest_keys;
        std::vector<size_t> ready_manifest_owners;
        ready_manifest_keys.reserve(manifest_keys.size());
        ready_manifest_owners.reserve(manifest_owners.size());
        for (size_t index = 0; index < manifest_keys.size(); ++index) {
            if (results[manifest_owners[index]]) {
                ready_manifest_keys.push_back(std::move(manifest_keys[index]));
                ready_manifest_owners.push_back(manifest_owners[index]);
            }
        }
        delete_physical(ready_manifest_keys, ready_manifest_owners);
        return results;
    }

   private:
    struct PutGroup {
        ObjectKey logical_key;
        uint32_t total_shard = 0;
        uint64_t total_size = 0;
        std::vector<size_t> request_indices;
    };

    struct RawPutRecord {
        ObjectKey logical_key;
        std::string key;
        std::span<const Slice> slices;
        uint64_t size = 0;
    };

    struct RawPutResults {
        KvcsPutShardResults results;
        std::vector<bool> inserted;
        std::vector<bool> outcome_known;
    };

    bool IsPhysicalRecordQuarantined(std::string_view key) const {
        std::lock_guard lock(quarantine_mutex_);
        return quarantined_physical_keys_.contains(std::string(key));
    }

    bool IsLogicalKeyQuarantined(std::string_view key) const {
        std::lock_guard lock(quarantine_mutex_);
        return quarantined_logical_keys_.contains(std::string(key));
    }

    void QuarantineLogicalKey(std::string_view key, std::string_view operation,
                              ErrorCode error) const {
        if (key.empty()) return;
        bool inserted = false;
        {
            std::lock_guard lock(quarantine_mutex_);
            inserted = quarantined_logical_keys_.insert(std::string(key)).second;
        }
        if (inserted) {
            LOG(ERROR)
                << "KVCS Low Level quarantined logical key after an "
                   "indeterminate operation, operation="
                << operation << ", mapped_error=" << toString(error)
                << ", key=" << key
                << ", mountpoint_index=" << mountpoint_index_
                << "; all reads, writes, and deletes for this logical key "
                   "remain fail-closed until process restart";
        }
    }

    void QuarantinePhysicalRecords(std::span<const std::string> keys,
                                   std::string_view operation,
                                   ErrorCode error) const {
        if (keys.empty()) return;
        size_t inserted = 0;
        {
            std::lock_guard lock(quarantine_mutex_);
            for (const auto& key : keys) {
                inserted += quarantined_physical_keys_.insert(key).second;
            }
        }
        if (inserted != 0) {
            LOG(ERROR)
                << "KVCS Low Level quarantined physical keys after an "
                   "indeterminate operation, operation="
                << operation << ", mapped_error=" << toString(error)
                << ", new_keys=" << inserted
                << ", mountpoint_index=" << mountpoint_index_
                << "; writes using these keys remain fail-closed until "
                   "process restart";
        }
    }

    struct RecordMatchContext {
        const RawPutRecord* record = nullptr;
        bool matched = false;
        bool completed = false;
        ErrorCode error = ErrorCode::INTERNAL_ERROR;
    };

    static void MatchRecord(int count, const void* const* data,
                            const size_t* lengths, const int* statuses,
                            void* opaque) {
        auto* context = static_cast<RecordMatchContext*>(opaque);
        if (count != 1 || context == nullptr || context->record == nullptr) {
            return;
        }
        context->completed = true;
        if (statuses[0] <= 0) {
            context->error = statuses[0] == 0
                                 ? ErrorCode::OBJECT_NOT_FOUND
                                 : MapItemStatus(statuses[0]);
            return;
        }
        context->error = ErrorCode::OK;
        if (data[0] == nullptr || lengths[0] != context->record->size) return;

        const auto* source = static_cast<const uint8_t*>(data[0]);
        size_t offset = 0;
        for (const auto& slice : context->record->slices) {
            if (slice.size != 0 &&
                std::memcmp(source + offset, slice.ptr, slice.size) != 0) {
                return;
            }
            offset += slice.size;
        }
        context->matched = true;
    }

    tl::expected<bool, ErrorCode> ExistingRecordMatches(
        const RawPutRecord& record,
        std::chrono::steady_clock::time_point deadline) const {
        const uint64_t timeout_ms =
            RemainingTimeoutMs(deadline, operation_timeout_ms_);
        if (timeout_ms == 0) {
            return tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
        }
        kvcs_ll_get_item_t item{.key = record.key.c_str()};
        RecordMatchContext context{.record = &record};
        const int call_status = kvcs_ll_batch_get(
            client_, &item, 1, MatchRecord, &context,
            OperationDeadlineNs(timeout_ms), &batch_options_);
        if (call_status < 0) {
            return tl::make_unexpected(MapCallStatus(call_status));
        }
        if (!context.completed || context.error == ErrorCode::OBJECT_NOT_FOUND) {
            // The existence check and value read observed different states.
            // Do not classify a concurrent change as a reusable record.
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (context.error != ErrorCode::OK) {
            return tl::make_unexpected(context.error);
        }
        return context.matched;
    }

    bool ValidatePutRequests(std::span<const KvcsShardPutRequest> requests,
                             std::vector<PutGroup>& groups) const {
        if (requests.size() > static_cast<size_t>(INT_MAX)) return false;
        std::unordered_map<ObjectKey, size_t> group_by_key;
        group_by_key.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            if (request.total_shard == 0 ||
                request.total_shard > kKvcsMaxMaterializedShards) {
                return false;
            }
            const uint32_t key_limit = LowLevelLogicalKeyLimit(
                max_key_size_, request.total_shard);
            if (!ValidateShardRequest(request, key_limit, max_value_size_)) {
                return false;
            }
            auto [position, inserted] =
                group_by_key.emplace(request.logical_key, groups.size());
            if (inserted) {
                groups.push_back(PutGroup{
                    .logical_key = request.logical_key,
                    .total_shard = request.total_shard,
                    .total_size = 0,
                    .request_indices = {},
                });
            }
            auto& group = groups[position->second];
            if (group.total_shard != request.total_shard ||
                request.size >
                    std::numeric_limits<uint64_t>::max() - group.total_size) {
                return false;
            }
            group.total_size += request.size;
            group.request_indices.push_back(index);
        }

        for (const auto& group : groups) {
            if (group.request_indices.size() != group.total_shard) return false;
            std::vector<bool> seen(group.total_shard, false);
            for (size_t request_index : group.request_indices) {
                const auto& request = requests[request_index];
                if (seen[request.shard_id]) return false;
                seen[request.shard_id] = true;
                if (group.total_shard > 1 &&
                    ((request.shard_id + 1 < group.total_shard &&
                      request.size != max_value_size_) ||
                     (request.shard_id + 1 == group.total_shard &&
                      request.size > max_value_size_))) {
                    return false;
                }
            }
        }
        return true;
    }

    bool ValidateGetRequest(const KvcsShardGetRequest& request) const {
        if (request.total_shard == 0 ||
            request.total_shard > kKvcsMaxMaterializedShards) {
            return false;
        }
        const uint32_t key_limit =
            LowLevelLogicalKeyLimit(max_key_size_, request.total_shard);
        return ValidateShardRequest(request, key_limit, max_value_size_) &&
               (request.total_shard != 1 || request.shard_id == 0);
    }

    RawPutResults PutRecords(
        std::span<const RawPutRecord> records,
        std::span<const uint8_t> reuse_matching_existing = {},
        std::span<const uint8_t> eligible = {}) const {
        if (records.empty()) {
            return {
                .results = {},
                .inserted = {},
                .outcome_known = {},
            };
        }
        if (!reuse_matching_existing.empty() &&
            reuse_matching_existing.size() != records.size()) {
            return {
                .results = MakeErrors<KvcsPutShardResults>(
                    records.size(), ErrorCode::INVALID_PARAMS),
                .inserted = std::vector<bool>(records.size(), false),
                .outcome_known = std::vector<bool>(records.size(), true),
            };
        }
        if (!eligible.empty() && eligible.size() != records.size()) {
            return {
                .results = MakeErrors<KvcsPutShardResults>(
                    records.size(), ErrorCode::INVALID_PARAMS),
                .inserted = std::vector<bool>(records.size(), false),
                .outcome_known = std::vector<bool>(records.size(), true),
            };
        }

        std::vector<uint8_t> effective_eligible(records.size(), 1);
        if (!eligible.empty()) {
            std::copy(eligible.begin(), eligible.end(),
                      effective_eligible.begin());
        }
        for (size_t index = 0; index < records.size(); ++index) {
            if (IsPhysicalRecordQuarantined(records[index].key) ||
                IsLogicalKeyQuarantined(records[index].logical_key)) {
                effective_eligible[index] = 0;
            }
        }
        if (std::any_of(effective_eligible.begin(), effective_eligible.end(),
                        [](uint8_t value) { return value == 0; })) {
            RawPutResults combined{
                .results = MakeErrors<KvcsPutShardResults>(
                    records.size(), ErrorCode::KVCS_INCOMPLETE),
                .inserted = std::vector<bool>(records.size(), false),
                .outcome_known = std::vector<bool>(records.size(), true),
            };
            std::vector<RawPutRecord> active_records;
            std::vector<uint8_t> active_reuse;
            std::vector<size_t> active_indices;
            active_records.reserve(records.size());
            active_indices.reserve(records.size());
            if (!reuse_matching_existing.empty()) {
                active_reuse.reserve(records.size());
            }
            for (size_t index = 0; index < records.size(); ++index) {
                if (effective_eligible[index] == 0) continue;
                active_records.push_back(records[index]);
                active_indices.push_back(index);
                if (!reuse_matching_existing.empty()) {
                    active_reuse.push_back(reuse_matching_existing[index]);
                }
            }
            if (active_records.empty()) return combined;

            auto active_results =
                PutRecords(active_records, active_reuse);
            for (size_t index = 0; index < active_indices.size(); ++index) {
                const size_t output_index = active_indices[index];
                combined.results[output_index] =
                    std::move(active_results.results[index]);
                combined.inserted[output_index] =
                    active_results.inserted[index];
                combined.outcome_known[output_index] =
                    active_results.outcome_known[index];
            }
            return combined;
        }

        KvcsPutShardResults results(
            records.size(),
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        std::vector<bool> inserted(records.size(), false);
        std::vector<bool> outcome_known(records.size(), true);
        std::vector<const char*> key_pointers;
        key_pointers.reserve(records.size());
        for (size_t index = 0; index < records.size(); ++index) {
            if (!IsKvcsKeyStringSafe(records[index].key, max_key_size_)) {
                return {
                    .results = MakeErrors<KvcsPutShardResults>(
                        records.size(), ErrorCode::INVALID_PARAMS),
                    .inserted = std::vector<bool>(records.size(), false),
                    .outcome_known = std::vector<bool>(records.size(), true),
                };
            }
            key_pointers.push_back(records[index].key.c_str());
        }

        const auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(operation_timeout_ms_);
        const int record_count = static_cast<int>(records.size());
        QueryResults existing(records.size());
        int call_status = kvcs_ll_batch_query(
            client_, key_pointers.data(), record_count, existing.data(),
            record_count, OperationDeadlineNs(operation_timeout_ms_),
            &batch_options_);
        auto completion = existing.Complete(call_status);
        if (!completion) {
            return {
                .results = MakeErrors<KvcsPutShardResults>(
                    records.size(), completion.error()),
                .inserted = std::move(inserted),
                // The existence check failed before any Put was submitted.
                .outcome_known = std::move(outcome_known),
            };
        }

        std::vector<size_t> put_indices;
        put_indices.reserve(records.size());
        for (size_t index = 0; index < records.size(); ++index) {
            const std::string_view status = BoundedString(
                existing[index].status, sizeof(existing[index].status));
            if (status == "not_found") {
                put_indices.push_back(index);
                continue;
            }
            if (status == "ok") {
                const bool may_reuse =
                    !reuse_matching_existing.empty() &&
                    reuse_matching_existing[index] != 0;
                if (!may_reuse) {
                    results[index] = tl::make_unexpected(
                        ErrorCode::OBJECT_ALREADY_EXISTS);
                    continue;
                }
                auto matches = ExistingRecordMatches(records[index], deadline);
                if (!matches) {
                    LOG(WARNING)
                        << "KVCS Low Level existing-record verification failed"
                        << ", mapped_error=" << toString(matches.error())
                        << ", mountpoint_index=" << mountpoint_index_
                        << ", key=" << records[index].key;
                    results[index] =
                        tl::make_unexpected(matches.error());
                } else if (*matches) {
                    // A previous partial Put may have left a physical record
                    // behind after rollback cleanup failed. Reuse it only when
                    // its bytes exactly match this Put; it is not owned by this
                    // call and must not be rolled back.
                    results[index] = {};
                } else {
                    results[index] = tl::make_unexpected(
                        ErrorCode::OBJECT_ALREADY_EXISTS);
                }
                continue;
            }

            ErrorCode error = ErrorCode::INTERNAL_ERROR;
            if (status == "incomplete") {
                error = ErrorCode::KVCS_INCOMPLETE;
            } else if (status == "unavailable") {
                error = ErrorCode::KVCS_UNAVAILABLE;
            } else if (status == "timeout") {
                error = ErrorCode::RPC_TIMEOUT;
            }
            LOG(WARNING) << "KVCS Low Level pre-Put query failed, status="
                         << status << ", mapped_error=" << toString(error)
                         << ", mountpoint_index=" << mountpoint_index_
                         << ", key=" << records[index].key;
            results[index] = tl::make_unexpected(error);
        }

        if (put_indices.empty()) {
            return {
                .results = std::move(results),
                .inserted = std::move(inserted),
                .outcome_known = std::move(outcome_known),
            };
        }

        const uint64_t timeout_ms =
            RemainingTimeoutMs(deadline, operation_timeout_ms_);
        if (timeout_ms == 0) {
            for (size_t index : put_indices) {
                results[index] =
                    tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
            }
            return {
                .results = std::move(results),
                .inserted = std::move(inserted),
                // No Put was submitted after the preflight timed out.
                .outcome_known = std::move(outcome_known),
            };
        }

        std::vector<std::vector<const void*>> pointers(put_indices.size());
        std::vector<std::vector<size_t>> lengths(put_indices.size());
        std::vector<kvcs_ll_put_item_t> items;
        items.reserve(put_indices.size());
        for (size_t item_index = 0; item_index < put_indices.size();
             ++item_index) {
            const size_t record_index = put_indices[item_index];
            pointers[item_index].reserve(
                records[record_index].slices.size());
            lengths[item_index].reserve(
                records[record_index].slices.size());
            for (const auto& slice : records[record_index].slices) {
                pointers[item_index].push_back(slice.ptr);
                lengths[item_index].push_back(slice.size);
            }
            items.push_back(kvcs_ll_put_item_t{
                .key = records[record_index].key.c_str(),
                .value_segs = pointers[item_index].data(),
                .seg_lens = lengths[item_index].data(),
                .seg_count =
                    static_cast<int>(pointers[item_index].size()),
            });
        }

        std::vector<kvcs_put_result_t> native_results(put_indices.size());
        const int count = static_cast<int>(put_indices.size());
        call_status = kvcs_ll_batch_put(
            client_, items.data(), count, native_results.data(), count,
            OperationDeadlineNs(timeout_ms), &batch_options_);
        if (call_status < 0) {
            const auto error = MapCallStatus(call_status);
            std::vector<std::string> ambiguous_keys;
            ambiguous_keys.reserve(put_indices.size());
            for (const size_t index : put_indices) {
                ambiguous_keys.push_back(records[index].key);
            }
            QuarantinePhysicalRecords(ambiguous_keys, "put-call", error);
            for (const size_t index : put_indices) {
                QuarantineLogicalKey(records[index].logical_key, "put-call",
                                     error);
            }
            LOG(WARNING) << "KVCS Low Level batch put failed, status="
                         << call_status << ", mapped_error=" << toString(error)
                         << ", mountpoint_index=" << mountpoint_index_
                         << ", items=" << put_indices.size();
            for (size_t index : put_indices) {
                results[index] = tl::make_unexpected(error);
                outcome_known[index] = false;
            }
            return {
                .results = std::move(results),
                .inserted = std::move(inserted),
                .outcome_known = std::move(outcome_known),
            };
        }

        for (size_t item_index = 0; item_index < put_indices.size();
             ++item_index) {
            const size_t index = put_indices[item_index];
            const int native_status = native_results[item_index].status;
            const ErrorCode error = MapItemStatus(native_status);
            if (error == ErrorCode::OK) {
                results[index] = {};
                inserted[index] = true;
            } else if (error == ErrorCode::OBJECT_ALREADY_EXISTS &&
                       !reuse_matching_existing.empty() &&
                       reuse_matching_existing[index] != 0) {
                auto matches = ExistingRecordMatches(records[index], deadline);
                if (!matches) {
                    LOG(WARNING)
                        << "KVCS Low Level existing-record verification failed"
                        << ", mapped_error=" << toString(matches.error())
                        << ", mountpoint_index=" << mountpoint_index_
                        << ", key=" << records[index].key;
                    results[index] =
                        tl::make_unexpected(matches.error());
                } else if (*matches) {
                    results[index] = {};
                } else {
                    results[index] = tl::make_unexpected(error);
                }
            } else {
                LOG(WARNING) << "KVCS Low Level put item failed, status="
                             << native_status
                             << ", mapped_error=" << toString(error)
                             << ", mountpoint_index=" << mountpoint_index_
                             << ", key=" << records[index].key;
                results[index] = tl::make_unexpected(error);
            }
            if (!results[index]) {
                outcome_known[index] =
                    IsDefinitePerItemPutRejection(native_status);
                if (!outcome_known[index]) {
                    const std::array<std::string, 1> ambiguous_key{
                        records[index].key};
                    QuarantinePhysicalRecords(ambiguous_key, "put-item",
                                              results[index].error());
                    QuarantineLogicalKey(records[index].logical_key,
                                         "put-item", results[index].error());
                }
            }
        }
        return {
            .results = std::move(results),
            .inserted = std::move(inserted),
            .outcome_known = std::move(outcome_known),
        };
    }

    tl::expected<void, ErrorCode> DeletePhysicalRecords(
        std::span<const std::string> keys, std::string_view logical_key) const {
        if (keys.empty()) return {};
        if (keys.size() > static_cast<size_t>(INT_MAX)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (std::any_of(keys.begin(), keys.end(), [this](const auto& key) {
                return !IsKvcsKeyStringSafe(key, max_key_size_);
            })) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        std::vector<const char*> key_pointers;
        key_pointers.reserve(keys.size());
        for (const auto& key : keys) key_pointers.push_back(key.c_str());
        std::vector<int> statuses(keys.size());
        const int count = static_cast<int>(keys.size());
        // KVCS does not guarantee that a timeout means the delete was not
        // applied. Retrying could delete data inserted by a concurrent writer.
        const int call_status = kvcs_ll_batch_delete(
            client_, key_pointers.data(), count, statuses.data(), count,
            OperationDeadlineNs(operation_timeout_ms_), &batch_options_);
        if (call_status < 0) {
            const ErrorCode error = MapCallStatus(call_status);
            QuarantinePhysicalRecords(keys, "rollback-delete-call", error);
            QuarantineLogicalKey(logical_key, "rollback-delete-call", error);
            return tl::make_unexpected(error);
        }
        std::optional<ErrorCode> first_error;
        for (size_t index = 0; index < statuses.size(); ++index) {
            const int status = statuses[index];
            const ErrorCode error = MapItemStatus(status);
            if (error != ErrorCode::OK &&
                error != ErrorCode::OBJECT_NOT_FOUND) {
                if (!IsDefinitePerItemDeleteRejection(status)) {
                    const std::array<std::string, 1> ambiguous_key{
                        keys[index]};
                    QuarantinePhysicalRecords(
                        ambiguous_key, "rollback-delete-item", error);
                    QuarantineLogicalKey(logical_key, "rollback-delete-item",
                                         error);
                }
                if (!first_error) first_error = error;
            }
        }
        if (first_error) return tl::make_unexpected(*first_error);
        return {};
    }

    KvcsDriverQueryResults QueryChunkUntil(
        std::span<const ObjectKey> logical_keys,
        std::chrono::steady_clock::time_point deadline) const {
        if (logical_keys.size() > static_cast<size_t>(INT_MAX)) {
            return MakeErrors<KvcsDriverQueryResults>(
                logical_keys.size(), ErrorCode::INVALID_PARAMS);
        }
        const uint64_t timeout_ms =
            RemainingTimeoutMs(deadline, query_operation_timeout_ms_);
        if (timeout_ms == 0) {
            return MakeErrors<KvcsDriverQueryResults>(logical_keys.size(),
                                                      ErrorCode::RPC_TIMEOUT);
        }
        auto key_pointers = BuildKeyPointers(logical_keys, max_key_size_);
        if (!key_pointers) {
            return MakeErrors<KvcsDriverQueryResults>(logical_keys.size(),
                                                      key_pointers.error());
        }

        QueryResults native(logical_keys.size());
        const int count = static_cast<int>(logical_keys.size());
        const int returned = kvcs_ll_batch_query(
            client_, key_pointers->data(), count, native.data(), count,
            OperationDeadlineNs(timeout_ms), &batch_options_);
        auto completion = native.Complete(returned);
        if (!completion) {
            return MakeErrors<KvcsDriverQueryResults>(logical_keys.size(),
                                                      completion.error());
        }

        KvcsDriverQueryResults results(
            logical_keys.size(),
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
        std::vector<size_t> missing;
        for (size_t index = 0; index < logical_keys.size(); ++index) {
            const auto& result = native[index];
            const std::string_view status =
                BoundedString(result.status, sizeof(result.status));
            if (status == "ok") {
                // Low-Level query is an existence check only. It does not
                // describe value size or chunk layout.
                results[index] = KvcsManifest{
                    .logical_key = logical_keys[index],
                    .total_shard = 1,
                    .total_size = 0,
                    .shards = {},
                    .layout_known = false,
                };
            } else if (status == "not_found") {
                missing.push_back(index);
            } else if (status == "incomplete") {
                results[index] =
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE);
            } else if (status == "unavailable") {
                results[index] =
                    tl::make_unexpected(ErrorCode::KVCS_UNAVAILABLE);
            } else if (status == "timeout") {
                results[index] = tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
            } else {
                LOG(WARNING)
                    << "KVCS Low Level query returned unexpected status, key="
                    << logical_keys[index] << ", status=" << status
                    << ", mountpoint_index=" << mountpoint_index_;
            }
        }
        ResolveChunkManifests(logical_keys, missing, results, deadline);
        return results;
    }

    void ResolveChunkManifests(
        std::span<const ObjectKey> logical_keys,
        const std::vector<size_t>& missing, KvcsDriverQueryResults& results,
        std::chrono::steady_clock::time_point deadline) const {
        if (missing.empty()) {
            return;
        }
        std::vector<size_t> manifest_requests;
        std::vector<std::string> keys;
        std::vector<kvcs_ll_get_item_t> items;
        manifest_requests.reserve(missing.size());
        keys.reserve(missing.size());
        for (size_t request_index : missing) {
            auto key =
                LowLevelManifestKey(logical_keys[request_index]);
            if (!IsKvcsKeyStringSafe(key, max_key_size_)) {
                results[request_index] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
            manifest_requests.push_back(request_index);
            keys.push_back(std::move(key));
        }
        if (manifest_requests.empty()) {
            return;
        }

        std::vector<std::array<uint8_t, kManifestPayloadSize>> manifests(
            manifest_requests.size());
        std::vector<void*> buffers;
        std::vector<size_t> capacities(manifest_requests.size(),
                                       kManifestPayloadSize);
        items.reserve(manifest_requests.size());
        buffers.reserve(manifest_requests.size());
        for (size_t index = 0; index < manifest_requests.size(); ++index) {
            items.push_back(kvcs_ll_get_item_t{.key = keys[index].c_str()});
            buffers.push_back(manifests[index].data());
        }

        const uint64_t timeout_ms =
            RemainingTimeoutMs(deadline, operation_timeout_ms_);
        if (timeout_ms == 0) {
            for (size_t request_index : manifest_requests) {
                results[request_index] =
                    tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
            }
            return;
        }
        std::vector<int> statuses(manifest_requests.size());
        std::vector<size_t> lengths(manifest_requests.size());
        const int count = static_cast<int>(manifest_requests.size());
        const int call_status = kvcs_ll_batch_get_into(
            client_, items.data(), count, buffers.data(), capacities.data(),
            statuses.data(), lengths.data(), OperationDeadlineNs(timeout_ms),
            &batch_options_);
        if (call_status < 0) {
            LOG(WARNING) << "KVCS Low Level manifest read failed, status="
                         << call_status << ", mapped_error="
                         << toString(MapCallStatus(call_status))
                         << ", mountpoint_index=" << mountpoint_index_;
            for (size_t request_index : manifest_requests) {
                results[request_index] =
                    tl::make_unexpected(MapCallStatus(call_status));
            }
            return;
        }

        for (size_t index = 0; index < manifest_requests.size(); ++index) {
            const size_t request_index = manifest_requests[index];
            if (statuses[index] == 0) {
                results[request_index] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
            if (statuses[index] < 0) {
                const ErrorCode error = MapItemStatus(statuses[index]);
                if (error != ErrorCode::OBJECT_NOT_FOUND) {
                    LOG(WARNING)
                        << "KVCS Low Level manifest item failed, key="
                        << keys[index] << ", status=" << statuses[index]
                        << ", mapped_error=" << toString(error)
                        << ", mountpoint_index=" << mountpoint_index_;
                }
                results[request_index] = tl::make_unexpected(error);
                continue;
            }
            if (lengths[index] != kManifestPayloadSize) {
                LOG(WARNING)
                    << "KVCS Low Level manifest payload has unexpected length, "
                       "key="
                    << keys[index] << ", status=" << statuses[index]
                    << ", length=" << lengths[index]
                    << ", expected_length=" << kManifestPayloadSize
                    << ", mountpoint_index=" << mountpoint_index_;
                results[request_index] =
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                continue;
            }
            auto payload = DecodeManifest(std::span<const uint8_t>(
                manifests[index].data(), kManifestPayloadSize));
            if (!payload) {
                LOG(WARNING)
                    << "KVCS Low Level manifest payload is invalid, key="
                    << keys[index] << ", status=" << statuses[index]
                    << ", length=" << lengths[index]
                    << ", expected_length=" << kManifestPayloadSize
                    << ", mountpoint_index=" << mountpoint_index_;
                results[request_index] =
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                continue;
            }

            if (payload->total_size == 0 || payload->chunk_size == 0 ||
                payload->chunk_size > max_value_size_ ||
                payload->total_shard < 2 ||
                payload->total_shard > kKvcsMaxMaterializedShards ||
                logical_keys[request_index].size() >
                    LowLevelLogicalKeyLimit(max_key_size_,
                                            payload->total_shard)) {
                LOG(WARNING)
                    << "KVCS Low Level manifest metadata is invalid, key="
                    << keys[index] << ", total_size=" << payload->total_size
                    << ", chunk_size=" << payload->chunk_size
                    << ", total_shard=" << payload->total_shard
                    << ", max_value_size=" << max_value_size_
                    << ", mountpoint_index=" << mountpoint_index_;
                results[request_index] =
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                continue;
            }
            auto manifest =
                BuildKvcsManifest(logical_keys[request_index],
                                  payload->total_size, payload->chunk_size);
            if (!manifest || manifest->total_shard != payload->total_shard) {
                LOG(WARNING)
                    << "KVCS Low Level manifest layout is invalid, key="
                    << keys[index] << ", total_size=" << payload->total_size
                    << ", chunk_size=" << payload->chunk_size
                    << ", chunk_count=" << payload->total_shard
                    << ", mountpoint_index=" << mountpoint_index_;
                results[request_index] =
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                continue;
            }
            results[request_index] = std::move(*manifest);
        }
    }

    uint32_t mountpoint_index_ = 0;
    uint32_t max_key_size_ = kKvcsDefaultMaxKeySize;
    uint64_t max_value_size_ = 0;
    size_t query_batch_size_ = 0;
    uint64_t operation_timeout_ms_ = kDefaultOperationTimeoutMs;
    uint64_t query_operation_timeout_ms_ = kDefaultQueryOperationTimeoutMs;
    std::shared_ptr<KvcsLowLevelClientState> client_state_;
    kvcs_ll_client_t* client_ = nullptr;
    kvcs_ll_batch_opts_t batch_options_{.mountpoint_index = mountpoint_index_};
    mutable std::mutex quarantine_mutex_;
    mutable std::unordered_set<std::string> quarantined_physical_keys_;
    mutable std::unordered_set<std::string> quarantined_logical_keys_;
};

#endif  // MOONCAKE_HAVE_KVCS_SDK

}  // namespace

tl::expected<std::unique_ptr<KvcsDriver>, ErrorCode>
CreateKvcsStandardDriver() {
#ifdef MOONCAKE_HAVE_KVCS_SDK
    auto parsed = ParseStandardConfig();
    if (!parsed) {
        return tl::make_unexpected(parsed.error());
    }
    std::unique_ptr<KvcsDriver> driver =
        std::make_unique<KvcsStandardCapiDriver>(std::move(parsed.value()));
    return driver;
#else
    LOG(ERROR) << "KVCS Standard mode requested, but the KVCS SDK was not "
                  "found at build time";
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
}

tl::expected<std::vector<std::unique_ptr<KvcsDriver>>, ErrorCode>
CreateKvcsLowLevelDrivers(std::span<const uint32_t> mountpoint_indices) {
#ifdef MOONCAKE_HAVE_KVCS_SDK
    if (mountpoint_indices.empty())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto parsed = ParseLowLevelConfig();
    if (!parsed) {
        return tl::make_unexpected(parsed.error());
    }
    auto client_state =
        std::make_shared<KvcsLowLevelClientState>(parsed.value());
    std::vector<std::unique_ptr<KvcsDriver>> drivers;
    drivers.reserve(mountpoint_indices.size());
    for (const uint32_t mountpoint_index : mountpoint_indices) {
        auto driver_config = parsed.value();
        driver_config.mountpoint_index = mountpoint_index;
        drivers.push_back(std::make_unique<KvcsCapiDriver>(
            std::move(driver_config), client_state));
    }
    return drivers;
#else
    (void)mountpoint_indices;
    LOG(ERROR) << "KVCS low-level mode requested, but the KVCS SDK was not "
                  "found when Mooncake was built";
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
}

}  // namespace mooncake
