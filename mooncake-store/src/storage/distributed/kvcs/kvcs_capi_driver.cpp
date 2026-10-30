#include "storage/distributed/kvcs/kvcs_capi_driver.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "environ.h"
#include "environment_variables.h"
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
};

tl::expected<Config, ErrorCode> ReadConfig() {
    Config config;
    using Variables = KvcsEnvironmentVariables;
    config.socket = Environ::ReadOr(Variables::MOONCAKE_KVCS_EFC_SOCKET,
                                    std::move(config.socket));
    config.max_value_size = Environ::ReadOr(
        Variables::MOONCAKE_KVCS_MAX_VALUE_SIZE, config.max_value_size);
    const int max_key_size =
        Environ::ReadOr(Variables::MOONCAKE_KVCS_MAX_KEY_SIZE,
                        static_cast<int>(config.max_key_size));
    config.max_keys_per_batch = Environ::ReadOr(
        Variables::MOONCAKE_KVCS_MAX_KEYS_PER_BATCH, config.max_keys_per_batch);
    config.get_workers = Environ::ReadOr(Variables::MOONCAKE_KVCS_GET_WORKERS,
                                         config.get_workers);
    config.set_workers = Environ::ReadOr(Variables::MOONCAKE_KVCS_SET_WORKERS,
                                         config.set_workers);
    config.simple_workers = Environ::ReadOr(
        Variables::MOONCAKE_KVCS_SIMPLE_WORKERS, config.simple_workers);
    config.operation_timeout_ms =
        Environ::ReadOr(Variables::MOONCAKE_KVCS_OPERATION_TIMEOUT_MS,
                        config.operation_timeout_ms);

    const uint64_t timeout_limit =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max() / 1000000);
    if (config.socket.empty() || config.max_value_size == 0 ||
        config.max_value_size >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        max_key_size <= 0 || max_key_size > static_cast<int>(kKvcsMaxKeySize) ||
        config.max_keys_per_batch <= 0 || config.get_workers < 0 ||
        config.set_workers < 0 || config.simple_workers < 0 ||
        config.operation_timeout_ms == 0 ||
        config.operation_timeout_ms > timeout_limit) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    config.max_key_size = static_cast<uint32_t>(max_key_size);
    return config;
}

ErrorCode CallError(int status) {
    const auto code = status < 0 ? -static_cast<int64_t>(status) : status;
    switch (code) {
        case KVCS_NOT_FOUND:
        case KVCS_NAMESPACE_NOT_FOUND:
            return ErrorCode::OBJECT_NOT_FOUND;
        case KVCS_INCOMPLETE:
        case KVCS_UNAVAILABLE:
        case KVCS_INVALID_MOUNT_POINT:
            return ErrorCode::DFS_SERVICE_UNAVAILABLE;
        case KVCS_RESOURCE_EXHAUSTED:
            return ErrorCode::NO_AVAILABLE_HANDLE;
        case KVCS_BUFFER_TOO_SMALL:
            return ErrorCode::FILE_READ_FAIL;
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
    const auto code = status < 0 ? -static_cast<int64_t>(status) : status;
    switch (code) {
        case ENOENT:
            return ErrorCode::OBJECT_NOT_FOUND;
        case EEXIST:
            return ErrorCode::OBJECT_ALREADY_EXISTS;
        case ENOSPC:
        case ENOMEM:
            return ErrorCode::NO_AVAILABLE_HANDLE;
        case ENODEV:
            return ErrorCode::DFS_SERVICE_UNAVAILABLE;
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

int64_t DeadlineNs(uint64_t timeout_ms) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    const int64_t ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    const uint64_t increment = timeout_ms * 1000000;
    return increment >= static_cast<uint64_t>(INT64_MAX - ns)
               ? INT64_MAX
               : ns + static_cast<int64_t>(increment);
}

template <typename Results>
Results Errors(size_t count, ErrorCode error) {
    Results results;
    results.reserve(count);
    for (size_t i = 0; i < count; ++i)
        results.emplace_back(tl::make_unexpected(error));
    return results;
}

template <typename T, typename IsValid>
std::vector<size_t> CollectValidIndices(std::span<const T> items,
                                        IsValid&& is_valid,
                                        KvcsIoResults& results) {
    std::vector<size_t> indices;
    indices.reserve(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
        if (is_valid(items[i]))
            indices.push_back(i);
        else
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return indices;
}

void SetBatchError(KvcsIoResults& results, const std::vector<size_t>& indices,
                   size_t begin, size_t count, ErrorCode error) {
    for (size_t i = 0; i < count; ++i)
        results[indices[begin + i]] = tl::make_unexpected(error);
}

class KvcsLowLevelDriver final : public KvcsDriver {
   public:
    KvcsLowLevelDriver(Config config, uint32_t index)
        : config_(std::move(config)), options_{.mountpoint_index = index} {}
    ~KvcsLowLevelDriver() override {
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
            return tl::make_unexpected(
                errno == EINVAL ? ErrorCode::INVALID_PARAMS
                                : ErrorCode::DFS_SERVICE_UNAVAILABLE);
        return {};
    }
    uint64_t MaxValueSize() const override { return config_.max_value_size; }
    uint32_t MaxKeySize() const override { return config_.max_key_size; }

    tl::expected<std::vector<KvcsMountpointStat>, ErrorCode>
    GetMountpointStats() override {
        if (!client_)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        std::vector<kvcs_mountpoint_stat_t> native(KVCS_MOUNTPOINT_MAX);
        int count = 0;
        const int call = kvcs_ll_mp_stat(
            client_, native.data(), static_cast<int>(native.size()), &count,
            DeadlineNs(config_.operation_timeout_ms));
        if (call < 0) return tl::make_unexpected(CallError(call));
        if (count < 0 || static_cast<size_t>(count) > native.size())
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

        std::vector<KvcsMountpointStat> stats;
        stats.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; ++i) {
            const auto& stat = native[static_cast<size_t>(i)];
            const auto* end = static_cast<const char*>(
                std::memchr(stat.id, '\0', KVCS_MOUNTPOINT_ID_MAX));
            const size_t id_size = end ? static_cast<size_t>(end - stat.id)
                                       : KVCS_MOUNTPOINT_ID_MAX;
            stats.push_back({.index = stat.index,
                             .id = std::string(stat.id, id_size),
                             .total_bytes = stat.total_bytes,
                             .used_bytes = stat.used_bytes});
        }
        return stats;
    }

    KvcsIoResults BatchPut(std::span<const KvcsIoRequest> requests) override {
        if (!client_)
            return Errors<KvcsIoResults>(requests.size(),
                                         ErrorCode::INTERNAL_ERROR);
        KvcsIoResults results(requests.size());
        auto valid_indices = CollectValidIndices(
            requests, [this](const auto& request) { return Valid(request); },
            results);

        const size_t limit = static_cast<size_t>(config_.max_keys_per_batch);
        for (size_t begin = 0; begin < valid_indices.size(); begin += limit) {
            const size_t count = std::min(limit, valid_indices.size() - begin);
            std::vector<std::vector<const void*>> pointers(count);
            std::vector<std::vector<size_t>> sizes(count);
            std::vector<kvcs_ll_put_item_t> items(count);
            std::vector<kvcs_put_result_t> native(count);
            for (size_t i = 0; i < count; ++i) {
                const auto& request = requests[valid_indices[begin + i]];
                pointers[i].reserve(request.slices.size());
                sizes[i].reserve(request.slices.size());
                for (const auto& slice : request.slices) {
                    pointers[i].push_back(slice.ptr);
                    sizes[i].push_back(slice.size);
                }
                items[i] = {.key = request.logical_key.c_str(),
                            .value_segs = pointers[i].data(),
                            .seg_lens = sizes[i].data(),
                            .seg_count = static_cast<int>(sizes[i].size())};
            }
            const int call = kvcs_ll_batch_put(
                client_, items.data(), static_cast<int>(count), native.data(),
                static_cast<int>(count),
                DeadlineNs(config_.operation_timeout_ms), &options_);
            if (call < 0) {
                SetBatchError(results, valid_indices, begin, count,
                              CallError(call));
                continue;
            }
            for (size_t i = 0; i < count; ++i) {
                const size_t result_index = valid_indices[begin + i];
                results[result_index] =
                    native[i].status != 0
                        ? tl::expected<void, ErrorCode>(
                              tl::make_unexpected(ItemError(native[i].status)))
                        : tl::expected<void, ErrorCode>();
            }
        }
        return results;
    }

    KvcsIoResults BatchGet(std::span<const KvcsIoRequest> requests) override {
        if (!client_)
            return Errors<KvcsIoResults>(requests.size(),
                                         ErrorCode::INTERNAL_ERROR);
        KvcsIoResults results(requests.size());
        auto valid_indices = CollectValidIndices(
            requests,
            [this](const auto& request) {
                return request.slices.size() == 1 && Valid(request);
            },
            results);

        const size_t limit = static_cast<size_t>(config_.max_keys_per_batch);
        for (size_t begin = 0; begin < valid_indices.size(); begin += limit) {
            const size_t count = std::min(limit, valid_indices.size() - begin);
            std::vector<kvcs_ll_get_item_t> items(count);
            std::vector<void*> buffers(count);
            std::vector<size_t> capacities(count);
            std::vector<int> statuses(count);
            std::vector<size_t> lengths(count);
            for (size_t i = 0; i < count; ++i) {
                const auto& request = requests[valid_indices[begin + i]];
                items[i] = {.key = request.logical_key.c_str()};
                buffers[i] = request.slices.front().ptr;
                capacities[i] = static_cast<size_t>(request.size);
            }
            const int call = kvcs_ll_batch_get_into(
                client_, items.data(), static_cast<int>(count), buffers.data(),
                capacities.data(), statuses.data(), lengths.data(),
                DeadlineNs(config_.operation_timeout_ms), &options_);
            if (call < 0) {
                SetBatchError(results, valid_indices, begin, count,
                              CallError(call));
                continue;
            }
            for (size_t i = 0; i < count; ++i) {
                const size_t result_index = valid_indices[begin + i];
                const auto& request = requests[result_index];
                if (statuses[i] == 0) {
                    results[result_index] =
                        tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    continue;
                }
                if (statuses[i] < 0) {
                    results[result_index] =
                        tl::make_unexpected(ItemError(statuses[i]));
                    continue;
                }
                if (lengths[i] != request.size) {
                    results[result_index] =
                        tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                    continue;
                }
                results[result_index] = {};
            }
        }
        return results;
    }

    KvcsIoResults BatchDelete(std::span<const ObjectKey> keys) override {
        if (!client_)
            return Errors<KvcsIoResults>(keys.size(),
                                         ErrorCode::INTERNAL_ERROR);
        KvcsIoResults results(keys.size());
        auto valid_indices = CollectValidIndices(
            keys,
            [this](const ObjectKey& key) {
                return IsKvcsKeyStringSafe(key, config_.max_key_size);
            },
            results);

        const size_t limit = static_cast<size_t>(config_.max_keys_per_batch);
        for (size_t begin = 0; begin < valid_indices.size(); begin += limit) {
            const size_t count = std::min(limit, valid_indices.size() - begin);
            std::vector<const char*> pointers(count);
            std::vector<int> statuses(count);
            for (size_t i = 0; i < count; ++i)
                pointers[i] = keys[valid_indices[begin + i]].c_str();
            const int call = kvcs_ll_batch_delete(
                client_, pointers.data(), static_cast<int>(count),
                statuses.data(), static_cast<int>(count),
                DeadlineNs(config_.operation_timeout_ms), &options_);
            if (call < 0) {
                SetBatchError(results, valid_indices, begin, count,
                              CallError(call));
                continue;
            }
            for (size_t i = 0; i < count; ++i) {
                const size_t result_index = valid_indices[begin + i];
                const auto error = ItemError(statuses[i]);
                results[result_index] =
                    statuses[i] != 0 && error != ErrorCode::OBJECT_NOT_FOUND
                        ? tl::expected<void, ErrorCode>(
                              tl::make_unexpected(error))
                        : tl::expected<void, ErrorCode>();
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

    Config config_;
    kvcs_ll_batch_opts_t options_;
    kvcs_ll_client_t* client_ = nullptr;
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
        std::make_unique<KvcsLowLevelDriver>(*config, mountpoint_index));
#else
    (void)mountpoint_index;
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
}

}  // namespace mooncake
