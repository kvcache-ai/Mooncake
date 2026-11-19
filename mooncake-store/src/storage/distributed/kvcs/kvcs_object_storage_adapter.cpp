#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <future>
#include <limits>
#include <mutex>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_target.h"
#include "thread_pool.h"

namespace mooncake {
namespace {

template <typename T>
std::vector<tl::expected<T, ErrorCode>> Errors(size_t count, ErrorCode error) {
    std::vector<tl::expected<T, ErrorCode>> results;
    results.reserve(count);
    for (size_t i = 0; i < count; ++i)
        results.emplace_back(tl::make_unexpected(error));
    return results;
}

template <typename T, typename Call, typename Convert>
void RunKvcsBatch(const std::vector<size_t>& request_indices,
                  std::vector<tl::expected<T, ErrorCode>>& results, Call&& call,
                  Convert&& convert) {
    if (request_indices.empty()) return;
    try {
        auto driver_results = call();
        if (driver_results.size() != request_indices.size()) {
            for (size_t index : request_indices)
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            return;
        }
        for (size_t i = 0; i < driver_results.size(); ++i) {
            results[request_indices[i]] =
                convert(std::move(driver_results[i]), request_indices[i]);
        }
    } catch (...) {
        for (size_t index : request_indices)
            results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

}  // namespace

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(std::string efc_config_path)
    : efc_config_path_(std::move(efc_config_path)) {}

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    std::unique_ptr<KvcsDriver> driver)
    : driver_(std::move(driver)) {}

KvcsObjectStorageAdapter::~KvcsObjectStorageAdapter() = default;

tl::expected<ObjectKey, ErrorCode> KvcsObjectStorageAdapter::EncodeKey(
    std::string_view logical_key) const {
    if (logical_key.empty() || !driver_)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto encoded = EncodeKvcsKey(logical_key);
    if (!IsKvcsKeyStringSafe(encoded, driver_->MaxKeySize()))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return encoded;
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Init() {
    if (initialized_) return {};
    uint32_t mountpoint_index = 1;
    std::string mountpoint_id = "injected";
    if (!driver_) {
        auto target = LoadKvcsEfcTarget(efc_config_path_);
        if (!target) return tl::make_unexpected(target.error());
        auto driver = CreateKvcsLowLevelDriver(target->mountpoint_index);
        if (!driver) return tl::make_unexpected(driver.error());
        mountpoint_id = std::move(target->mountpoint_id);
        mountpoint_index = target->mountpoint_index;
        driver_ = std::move(*driver);
    }
    if (!driver_) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    auto init = driver_->Init();
    if (!init) return init;
    if (driver_->MaxValueSize() == 0 || driver_->MaxKeySize() == 0 ||
        driver_->MaxKeySize() > kKvcsMaxKeySize)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    try {
        if (driver_->ReadConcurrency() > 1)
            read_workers_ =
                std::make_unique<ThreadPool>(driver_->ReadConcurrency());
    } catch (...) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    initialized_ = true;
    LOG(INFO) << "KVCS Low Level mountpoint_id=" << mountpoint_id
              << ", mountpoint_index=" << mountpoint_index;
    return {};
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Put(
    const std::string& key, std::span<const char> data) {
    iovec iov{const_cast<char*>(data.data()), data.size()};
    return PutV(key, &iov, 1);
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::PutV(
    const std::string& key, const iovec* iov, int iovcnt) {
    auto results = PutBatch({{key, iov, iovcnt}});
    return std::move(results.front());
}

std::vector<tl::expected<void, ErrorCode>> KvcsObjectStorageAdapter::PutBatch(
    const std::vector<ObjectPutRequest>& requests) {
    if (!initialized_)
        return Errors<void>(requests.size(), ErrorCode::INTERNAL_ERROR);
    ObjectStorageIoResults results(requests.size());
    std::vector<KvcsIoRequest> puts;
    std::vector<size_t> put_indices;
    puts.reserve(requests.size());
    put_indices.reserve(requests.size());
    std::unordered_set<std::string> keys;
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (!request.iov || request.iovcnt <= 0) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        auto key = EncodeKey(request.logical_key);
        if (!key) {
            results[i] = tl::make_unexpected(key.error());
            continue;
        }
        std::vector<Slice> slices;
        slices.reserve(static_cast<size_t>(request.iovcnt));
        uint64_t size = 0;
        bool valid = true;
        for (int j = 0; j < request.iovcnt; ++j) {
            const auto& iov = request.iov[j];
            if (!iov.iov_base || iov.iov_len == 0 ||
                iov.iov_len > driver_->MaxValueSize() - size) {
                valid = false;
                break;
            }
            size += iov.iov_len;
            slices.push_back({iov.iov_base, iov.iov_len});
        }
        if (!valid || size == 0) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        if (!keys.insert(*key).second) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        puts.push_back({std::move(*key), std::move(slices), size});
        put_indices.push_back(i);
    }

    std::vector<ObjectKey> encoded_keys;
    encoded_keys.reserve(puts.size());
    for (const auto& put : puts) encoded_keys.push_back(put.logical_key);
    auto deleted = driver_->BatchDelete(encoded_keys);
    if (deleted.size() != puts.size())
        return Errors<void>(requests.size(), ErrorCode::INTERNAL_ERROR);

    std::vector<KvcsIoRequest> pending;
    std::vector<size_t> pending_indices;
    pending.reserve(puts.size());
    pending_indices.reserve(put_indices.size());
    for (size_t i = 0; i < deleted.size(); ++i) {
        if (!deleted[i]) {
            results[put_indices[i]] = tl::make_unexpected(deleted[i].error());
            continue;
        }
        pending.push_back(std::move(puts[i]));
        pending_indices.push_back(put_indices[i]);
    }

    RunKvcsBatch<void>(
        pending_indices, results, [&] { return driver_->BatchPut(pending); },
        [](auto result, size_t) { return result; });
    return results;
}

tl::expected<size_t, ErrorCode> KvcsObjectStorageAdapter::Get(
    const std::string& key, void* buf, size_t len) {
    auto results = GetBatch({{key, buf, len}});
    return std::move(results.front());
}

std::vector<tl::expected<size_t, ErrorCode>> KvcsObjectStorageAdapter::GetBatch(
    const std::vector<ObjectGetRequest>& requests) {
    return GetBatchWithCompletion(requests, {});
}

std::vector<tl::expected<size_t, ErrorCode>>
KvcsObjectStorageAdapter::GetBatchWithCompletion(
    const std::vector<ObjectGetRequest>& requests,
    const ReadCompletion& completed) {
    auto results = Errors<size_t>(requests.size(), ErrorCode::INTERNAL_ERROR);
    auto notify = [&](size_t i) {
        if (completed) completed(i, results[i]);
    };
    if (!initialized_) {
        for (size_t i = 0; i < requests.size(); ++i) notify(i);
        return results;
    }
    const int64_t deadline_ns = driver_->ReadDeadlineNs();
    std::vector<KvcsIoRequest> gets;
    std::vector<size_t> indices;
    std::vector<std::pair<uintptr_t, uintptr_t>> ranges;
    gets.reserve(requests.size());
    indices.reserve(requests.size());
    ranges.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto key = EncodeKey(request.logical_key);
        const auto begin = reinterpret_cast<uintptr_t>(request.buffer);
        if (!key || !request.buffer || request.size == 0 ||
            request.size > driver_->MaxValueSize() ||
            request.size > std::numeric_limits<uintptr_t>::max() - begin) {
            results[i] = tl::make_unexpected(key ? ErrorCode::INVALID_PARAMS
                                                 : key.error());
            notify(i);
            continue;
        }
        gets.push_back(
            {std::move(*key), {{request.buffer, request.size}}, request.size});
        indices.push_back(i);
        ranges.emplace_back(begin, begin + request.size);
    }
    if (gets.empty()) return results;
    // Keep ordinary ordered I/O for aliased destinations, including callers
    // that bypass DFS singleflight because they have no published source ID.
    std::sort(ranges.begin(), ranges.end());
    bool aliased = false;
    for (size_t i = 1; i < ranges.size(); ++i)
        aliased |= ranges[i].first < ranges[i - 1].second;
    const size_t concurrency = read_workers_ ? read_workers_->size() : 1;
    const size_t cap = driver_->MaxBatchKeys();
    const size_t limit =
        !aliased && cap
            ? std::min(cap, (gets.size() + concurrency - 1) / concurrency)
            : gets.size();
    auto fetch = [&](size_t begin) {
        const size_t count = std::min(limit, gets.size() - begin);
        try {
            const auto now =
                std::chrono::steady_clock::now().time_since_epoch();
            if (std::chrono::duration_cast<std::chrono::nanoseconds>(now)
                    .count() >= deadline_ns)
                return Errors<void>(count, ErrorCode::RPC_TIMEOUT);
            auto read = driver_->BatchGetUntil(
                std::span<const KvcsIoRequest>(gets).subspan(begin, count),
                deadline_ns);
            if (read.size() == count) return read;
        } catch (...) {
        }
        return Errors<void>(count, ErrorCode::INTERNAL_ERROR);
    };
    auto finish = [&](size_t begin, const KvcsIoResults& read) {
        for (size_t j = 0; j < read.size(); ++j) {
            const size_t i = indices[begin + j];
            results[i] = read[j]
                             ? tl::expected<size_t, ErrorCode>(requests[i].size)
                             : tl::make_unexpected(read[j].error());
            notify(i);
        }
    };
    if (!read_workers_) {
        for (size_t begin = 0; begin < gets.size(); begin += limit)
            finish(begin, fetch(begin));
        return results;
    }
    struct Chunk {
        size_t begin;
        KvcsIoResults results;
    };
    struct Slot {
        std::future<Chunk> future;
        bool ready = false;
    };
    std::vector<Slot> slots(
        std::min(concurrency, (gets.size() - 1) / limit + 1));
    std::mutex mutex;
    std::condition_variable changed;
    struct Drain {
        std::vector<Slot>& slots;
        ~Drain() {
            for (auto& slot : slots)
                if (slot.future.valid()) slot.future.wait();
        }
    } drain{slots};
    size_t next = 0, active = 0;
    auto submit = [&](Slot& slot) {
        const size_t begin = next;
        auto task = std::make_shared<std::packaged_task<Chunk()>>([&, begin] {
            struct Notify {
                Slot& slot;
                std::mutex& mutex;
                std::condition_variable& changed;
                ~Notify() {
                    std::lock_guard lock(mutex);
                    slot.ready = true;
                    changed.notify_one();
                }
            } ready{slot, mutex, changed};
            return Chunk{begin, fetch(begin)};
        });
        slot.future = task->get_future();
        read_workers_->enqueue([task] { (*task)(); });
        next += limit;
        ++active;
    };
    for (auto& slot : slots) submit(slot);
    while (active) {
        size_t i = 0;
        {
            std::unique_lock lock(mutex);
            changed.wait(lock, [&] {
                for (size_t j = 0; j < slots.size(); ++j)
                    if (slots[j].ready) {
                        i = j;
                        return true;
                    }
                return false;
            });
            slots[i].ready = false;
        }
        // The ready notification precedes packaged_task publication. Get this
        // task's future to cover that gap and finish all borrowed-buffer use.
        auto chunk = slots[i].future.get();
        --active;
        if (next < gets.size()) submit(slots[i]);
        finish(chunk.begin, chunk.results);
    }
    return results;
}

tl::expected<bool, ErrorCode> KvcsObjectStorageAdapter::Exists(
    const std::string&) {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Delete(
    const std::string& key) {
    const std::array<std::string, 1> keys{key};
    auto result = DeleteBatch(keys);
    return std::move(result.front());
}

ObjectStorageIoResults KvcsObjectStorageAdapter::DeleteBatch(
    std::span<const std::string> keys) {
    if (!initialized_)
        return Errors<void>(keys.size(), ErrorCode::INTERNAL_ERROR);
    ObjectStorageIoResults results(keys.size());
    std::vector<ObjectKey> encoded_keys;
    std::vector<size_t> delete_indices;
    encoded_keys.reserve(keys.size());
    delete_indices.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto encoded = EncodeKey(keys[i]);
        if (!encoded) {
            results[i] = tl::make_unexpected(encoded.error());
            continue;
        }
        encoded_keys.push_back(std::move(*encoded));
        delete_indices.push_back(i);
    }
    RunKvcsBatch<void>(
        delete_indices, results,
        [&] { return driver_->BatchDelete(encoded_keys); },
        [](auto result, size_t) { return result; });
    return results;
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
KvcsObjectStorageAdapter::ListKeys() {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

}  // namespace mooncake
