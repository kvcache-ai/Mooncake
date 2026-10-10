#include "storage/distributed/kvcs/kvcs_capi_driver.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "integer_parser.h"
#include "storage/distributed/kvcs/kvcs_driver.h"

#ifdef MOONCAKE_HAVE_KVCS_SDK
#include <kvcs_capi.h>
#endif

namespace mooncake {
namespace {
#ifdef MOONCAKE_HAVE_KVCS_SDK

struct Config {
    std::string socket = "/var/run/kvcs/efc-grpc.sock";
    uint64_t max_value_size = 4ULL * 1024 * 1024;
    uint32_t max_key_size = kKvcsDefaultMaxKeySize;
    int max_keys_per_batch = 8;
    int get_workers = 1;
    int set_workers = 1;
    int simple_workers = 1;
    uint64_t operation_timeout_ms = 30000;
    uint64_t query_timeout_ms = 50;
};

tl::expected<Config, ErrorCode> ReadConfig() {
    Config config;
    if (const char* value = std::getenv("MOONCAKE_KVCS_EFC_SOCKET");
        value && *value)
        config.socket = value;
    auto read_uint = [](const char* name, uint64_t fallback, uint64_t minimum,
                        uint64_t maximum) -> tl::expected<uint64_t, ErrorCode> {
        const char* value = std::getenv(name);
        if (!value || !*value) return fallback;
        auto parsed = TryParseInteger<uint64_t>(value);
        if (!parsed || *parsed < minimum || *parsed > maximum)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        return *parsed;
    };
    const uint64_t timeout_limit =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max() / 1000000);
    auto value =
        read_uint("MOONCAKE_KVCS_MAX_VALUE_SIZE", config.max_value_size, 1,
                  std::numeric_limits<int64_t>::max());
    auto key = read_uint("MOONCAKE_KVCS_MAX_KEY_SIZE", config.max_key_size, 1,
                         kKvcsMaxKeySize);
    auto batch = read_uint("MOONCAKE_KVCS_MAX_KEYS_PER_BATCH",
                           config.max_keys_per_batch, 1, INT_MAX);
    auto get =
        read_uint("MOONCAKE_KVCS_GET_WORKERS", config.get_workers, 0, INT_MAX);
    auto set =
        read_uint("MOONCAKE_KVCS_SET_WORKERS", config.set_workers, 0, INT_MAX);
    auto simple = read_uint("MOONCAKE_KVCS_SIMPLE_WORKERS",
                            config.simple_workers, 0, INT_MAX);
    auto timeout = read_uint("MOONCAKE_KVCS_OPERATION_TIMEOUT_MS",
                             config.operation_timeout_ms, 1, timeout_limit);
    auto query = read_uint("MOONCAKE_KVCS_QUERY_TIMEOUT_MS",
                           config.query_timeout_ms, 1, timeout_limit);
    if (!value || !key || !batch || !get || !set || !simple || !timeout ||
        !query || config.socket.empty())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    config.max_value_size = *value;
    config.max_key_size = static_cast<uint32_t>(*key);
    config.max_keys_per_batch = static_cast<int>(*batch);
    config.get_workers = static_cast<int>(*get);
    config.set_workers = static_cast<int>(*set);
    config.simple_workers = static_cast<int>(*simple);
    config.operation_timeout_ms = *timeout;
    config.query_timeout_ms = *query;
    return config;
}

ErrorCode CallError(int status) {
    switch (-status) {
        case KVCS_NOT_FOUND:
        case KVCS_NAMESPACE_NOT_FOUND:
            return ErrorCode::OBJECT_NOT_FOUND;
        case KVCS_INCOMPLETE:
            return ErrorCode::KVCS_INCOMPLETE;
        case KVCS_UNAVAILABLE:
        case KVCS_INVALID_MOUNT_POINT:
            return ErrorCode::KVCS_UNAVAILABLE;
        case KVCS_RESOURCE_EXHAUSTED:
        case KVCS_BUFFER_TOO_SMALL:
            return ErrorCode::KVCS_RESOURCE_EXHAUSTED;
        case KVCS_INVALID_ARGUMENT:
            return ErrorCode::INVALID_PARAMS;
        case KVCS_ALREADY_EXISTS:
            return ErrorCode::OBJECT_ALREADY_EXISTS;
        case ETIMEDOUT:
            return ErrorCode::RPC_TIMEOUT;
        default:
            return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode ItemError(int status) {
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
            return ErrorCode::INTERNAL_ERROR;
    }
}

bool DefinitePutRejection(int status) {
    return status == -EEXIST || status == -ENOSPC || status == -EINVAL ||
           status == -E2BIG || status == -ENAMETOOLONG;
}

bool DefiniteDeleteRejection(int status) {
    return status == -ENOENT || status == -EINVAL || status == -E2BIG ||
           status == -ENAMETOOLONG;
}

int64_t DeadlineNs(uint64_t timeout_ms) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    const int64_t ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    const uint64_t increment = timeout_ms * 1000000;
    return increment >= static_cast<uint64_t>(INT64_MAX - ns)
               ? INT64_MAX
               : ns + static_cast<int64_t>(increment);
}

uint64_t RemainingMs(std::chrono::steady_clock::time_point deadline,
                     uint64_t cap) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) return 0;
    const auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now)
            .count();
    return std::min(cap,
                    static_cast<uint64_t>(ns / 1000000 + (ns % 1000000 != 0)));
}

template <typename Results>
Results Errors(size_t count, ErrorCode error) {
    Results results;
    results.reserve(count);
    for (size_t i = 0; i < count; ++i)
        results.emplace_back(tl::make_unexpected(error));
    return results;
}

class QueryResultBuffer {
   public:
    kvcs_query_result_t result{};
    ~QueryResultBuffer() {
        if (initialized_) kvcs_query_result_free(&result, 1);
    }
    tl::expected<std::string_view, ErrorCode> Complete(int returned) {
        if (returned < 0) return tl::make_unexpected(CallError(returned));
        // The SDK returns either 0 or the requested item count on success.
        if (returned != 0 && returned != 1)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        initialized_ = true;
        return std::string_view(result.status,
                                strnlen(result.status, sizeof(result.status)));
    }

   private:
    bool initialized_ = false;
};

class SingleValueDriver final : public KvcsDriver {
   public:
    SingleValueDriver(Config config, uint32_t index)
        : config_(std::move(config)), options_{.mountpoint_index = index} {}
    ~SingleValueDriver() override {
        if (client_) kvcs_ll_client_destroy(client_);
    }

    tl::expected<void, ErrorCode> Init() override {
        if (client_) return {};
        kvcs_ll_config_t config{};
        config.efc_socket = config_.socket.c_str();
        config.max_key_size = static_cast<int>(config_.max_key_size);
        config.max_value_size = static_cast<int64_t>(config_.max_value_size);
        config.max_keys_per_batch = config_.max_keys_per_batch;
        config.get_workers = config_.get_workers;
        config.set_workers = config_.set_workers;
        config.simple_workers = config_.simple_workers;
        client_ = kvcs_ll_create(&config);
        if (!client_)
            return tl::make_unexpected(errno == EINVAL
                                           ? ErrorCode::INVALID_PARAMS
                                           : ErrorCode::KVCS_UNAVAILABLE);
        return {};
    }
    uint64_t MaxValueSize() const override { return config_.max_value_size; }
    uint32_t MaxKeySize() const override { return config_.max_key_size; }

    KvcsDriverQueryResults BatchQuery(
        std::span<const ObjectKey> keys) override {
        return BatchQueryUntil(
            keys, std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(config_.query_timeout_ms));
    }

    KvcsDriverQueryResults BatchQueryUntil(
        std::span<const ObjectKey> keys,
        std::chrono::steady_clock::time_point deadline) override {
        if (!client_)
            return Errors<KvcsDriverQueryResults>(keys.size(),
                                                  ErrorCode::INTERNAL_ERROR);
        KvcsDriverQueryResults results;
        results.reserve(keys.size());
        for (const auto& key : keys) {
            if (!IsKvcsKeyStringSafe(key, config_.max_key_size)) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INVALID_PARAMS));
                continue;
            }
            if (Quarantined(key)) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE));
                continue;
            }
            const auto timeout =
                RemainingMs(deadline, config_.operation_timeout_ms);
            if (!timeout) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::RPC_TIMEOUT));
                continue;
            }
            QueryResultBuffer native;
            const char* pointer = key.c_str();
            const int returned =
                kvcs_ll_batch_query(client_, &pointer, 1, &native.result, 1,
                                    DeadlineNs(timeout), &options_);
            auto status = native.Complete(returned);
            if (!status) {
                results.emplace_back(tl::make_unexpected(status.error()));
            } else if (*status == "ok") {
                results.emplace_back();
            } else if (*status == "not_found") {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
            } else if (*status == "incomplete") {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE));
            } else if (*status == "unavailable") {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_UNAVAILABLE));
            } else if (*status == "timeout") {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::RPC_TIMEOUT));
            } else {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
            }
        }
        return results;
    }

    KvcsPutResults BatchPut(std::span<const KvcsPutRequest> requests) override {
        if (!client_)
            return Errors<KvcsPutResults>(requests.size(),
                                          ErrorCode::INTERNAL_ERROR);
        KvcsPutResults results;
        results.reserve(requests.size());
        for (const auto& request : requests) {
            if (!Valid(request) || Quarantined(request.logical_key)) {
                results.emplace_back(
                    tl::make_unexpected(Quarantined(request.logical_key)
                                            ? ErrorCode::KVCS_INCOMPLETE
                                            : ErrorCode::INVALID_PARAMS));
                continue;
            }
            // SDK Put is insert-only; still preflight to avoid overwriting
            // when the EFC has an existing record from a previous process.
            const std::array<ObjectKey, 1> key{request.logical_key};
            const auto deadline =
                std::chrono::steady_clock::now() +
                std::chrono::milliseconds(config_.operation_timeout_ms);
            auto existing = BatchQueryUntil(key, deadline);
            if (existing[0]) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS));
                continue;
            }
            if (existing[0].error() != ErrorCode::OBJECT_NOT_FOUND) {
                results.emplace_back(tl::make_unexpected(existing[0].error()));
                continue;
            }
            const uint64_t remaining =
                RemainingMs(deadline, config_.operation_timeout_ms);
            if (remaining == 0) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::RPC_TIMEOUT));
                continue;
            }
            std::vector<const void*> pointers;
            std::vector<size_t> sizes;
            for (const auto& slice : request.slices) {
                pointers.push_back(slice.ptr);
                sizes.push_back(slice.size);
            }
            kvcs_ll_put_item_t item{
                .key = request.logical_key.c_str(),
                .value_segs = pointers.data(),
                .seg_lens = sizes.data(),
                .seg_count = static_cast<int>(sizes.size())};
            kvcs_put_result_t native{};
            const int call =
                kvcs_ll_batch_put(client_, &item, 1, &native, 1,
                                  DeadlineNs(remaining), &options_);
            if (call < 0) {
                Quarantine(request.logical_key, "put", CallError(call));
                results.emplace_back(tl::make_unexpected(CallError(call)));
            } else if (native.status < 0) {
                const auto error = ItemError(native.status);
                if (!DefinitePutRejection(native.status))
                    Quarantine(request.logical_key, "put-item", error);
                results.emplace_back(tl::make_unexpected(error));
            } else {
                results.emplace_back();
            }
        }
        return results;
    }

    KvcsGetResults BatchGet(std::span<const KvcsGetRequest> requests) override {
        if (!client_)
            return Errors<KvcsGetResults>(requests.size(),
                                          ErrorCode::INTERNAL_ERROR);
        KvcsGetResults results;
        results.reserve(requests.size());
        for (const auto& request : requests) {
            if (!Valid(request) || Quarantined(request.logical_key)) {
                results.emplace_back(
                    tl::make_unexpected(Quarantined(request.logical_key)
                                            ? ErrorCode::KVCS_INCOMPLETE
                                            : ErrorCode::INVALID_PARAMS));
                continue;
            }
            std::vector<char> staging;
            void* buffer = request.slices[0].ptr;
            if (request.slices.size() > 1) {
                staging.resize(static_cast<size_t>(request.size));
                buffer = staging.data();
            }
            size_t capacity = request.size;
            size_t length = 0;
            int status = 0;
            kvcs_ll_get_item_t item{.key = request.logical_key.c_str()};
            const int call = kvcs_ll_batch_get_into(
                client_, &item, 1, &buffer, &capacity, &status, &length,
                DeadlineNs(config_.operation_timeout_ms), &options_);
            if (call < 0) {
                results.emplace_back(tl::make_unexpected(CallError(call)));
            } else if (status == 0) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
            } else if (status < 0) {
                results.emplace_back(tl::make_unexpected(ItemError(status)));
            } else if (length != request.size) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE));
            } else {
                size_t offset = 0;
                for (const auto& slice : request.slices) {
                    if (!staging.empty())
                        std::memcpy(slice.ptr, staging.data() + offset,
                                    slice.size);
                    offset += slice.size;
                }
                results.emplace_back();
            }
        }
        return results;
    }

    KvcsDeleteResults BatchDelete(std::span<const ObjectKey> keys) override {
        if (!client_)
            return Errors<KvcsDeleteResults>(keys.size(),
                                             ErrorCode::INTERNAL_ERROR);
        KvcsDeleteResults results;
        results.reserve(keys.size());
        for (const auto& key : keys) {
            if (!IsKvcsKeyStringSafe(key, config_.max_key_size) ||
                Quarantined(key)) {
                results.emplace_back(tl::make_unexpected(
                    Quarantined(key) ? ErrorCode::KVCS_INCOMPLETE
                                     : ErrorCode::INVALID_PARAMS));
                continue;
            }
            const std::array<ObjectKey, 1> probe{key};
            auto existing = BatchQuery(probe);
            if (!existing[0]) {
                if (existing[0].error() == ErrorCode::OBJECT_NOT_FOUND)
                    results.emplace_back();
                else
                    results.emplace_back(
                        tl::make_unexpected(existing[0].error()));
                continue;
            }
            const char* pointer = key.c_str();
            int status = 0;
            const int call = kvcs_ll_batch_delete(
                client_, &pointer, 1, &status, 1,
                DeadlineNs(config_.operation_timeout_ms), &options_);
            if (call < 0) {
                Quarantine(key, "delete", CallError(call));
                results.emplace_back(tl::make_unexpected(CallError(call)));
            } else if (status < 0 && status != -ENOENT) {
                const auto error = ItemError(status);
                if (!DefiniteDeleteRejection(status))
                    Quarantine(key, "delete-item", error);
                results.emplace_back(tl::make_unexpected(error));
            } else {
                results.emplace_back();
            }
        }
        return results;
    }

   private:
    template <typename Request>
    bool Valid(const Request& request) const {
        if (!IsKvcsKeyStringSafe(request.logical_key, config_.max_key_size) ||
            request.size == 0 || request.size > config_.max_value_size ||
            request.slices.empty() || request.slices.size() > INT_MAX)
            return false;
        uint64_t size = 0;
        for (const auto& slice : request.slices) {
            if (!slice.ptr || !slice.size || slice.size > request.size - size)
                return false;
            size += slice.size;
        }
        return size == request.size;
    }

    bool Quarantined(std::string_view key) const {
        std::lock_guard lock(mutex_);
        return uncertain_keys_.contains(std::string(key));
    }
    void Quarantine(std::string_view key, const char* operation,
                    ErrorCode error) {
        std::lock_guard lock(mutex_);
        if (uncertain_keys_.insert(std::string(key)).second)
            LOG(ERROR) << "KVCS ambiguous " << operation << " key=" << key
                       << ", error=" << toString(error)
                       << "; blocked until process restart";
    }

    Config config_;
    kvcs_ll_batch_opts_t options_;
    kvcs_ll_client_t* client_ = nullptr;
    mutable std::mutex mutex_;
    std::unordered_set<std::string> uncertain_keys_;
};

#endif  // MOONCAKE_HAVE_KVCS_SDK
}  // namespace

tl::expected<std::unique_ptr<KvcsDriver>, ErrorCode> CreateKvcsLowLevelDriver(
    uint32_t mountpoint_index) {
#ifdef MOONCAKE_HAVE_KVCS_SDK
    if (mountpoint_index == 0)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto config = ReadConfig();
    if (!config) return tl::make_unexpected(config.error());
    return std::unique_ptr<KvcsDriver>(
        std::make_unique<SingleValueDriver>(*config, mountpoint_index));
#else
    (void)mountpoint_index;
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
}

}  // namespace mooncake
