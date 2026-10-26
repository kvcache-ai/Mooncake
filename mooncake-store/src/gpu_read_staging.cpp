#include "gpu_read_staging.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <mutex>

#include <glog/logging.h>

#include "environ.h"
#include "environment_variables.h"
#include "client_buffer.h"
#ifdef USE_CUDA
#include "gpu_read_staging_kernel.h"
#endif

namespace mooncake {

#ifdef USE_CUDA
namespace {

struct DeviceGuard {
    int previous = -1;
    bool valid = false;
    explicit DeviceGuard(int selected) {
        if (cudaGetDevice(&previous) != cudaSuccess) return;
        valid = previous == selected || cudaSetDevice(selected) == cudaSuccess;
    }
    ~DeviceGuard() {
        if (previous >= 0) cudaSetDevice(previous);
    }
};

struct ReadKey {
    size_t bytes;
    size_t first_fragment;
    size_t fragment_count;
};

struct ReadPlan {
    int device = -1;
    size_t max_batch_fragments = 0;
    std::vector<ReadKey> keys;
    std::vector<GpuReadFragment> fragments;
    // Half-open key ranges, including the final end index.
    std::vector<size_t> boundaries{0};
};

}  // namespace
#endif

struct GpuReadStagingImpl {
    using ResolveRegion = GpuReadStaging::ResolveRegion;
    using Results = GpuReadStaging::Results;
    GpuReadStagingImpl(std::shared_ptr<Client> client, size_t bytes)
        : capacity(bytes), owner(std::move(client)) {}

    bool enabled = true;
    const size_t capacity;
    const std::shared_ptr<Client> owner;
#ifdef USE_CUDA
    static constexpr size_t kMaxFragments = 1024 * 1024;
    std::mutex mutex;
    int device = -1;
    cudaStream_t stream = nullptr;
    void* payload = nullptr;
    GpuReadFragment* host_fragments = nullptr;
    GpuReadFragment* device_fragments = nullptr;
    size_t descriptor_capacity = 0;
    bool registered = false;

    ~GpuReadStagingImpl() {
        if (device < 0) return;
        DeviceGuard guard(device);
        if (!guard.valid) {
            LOG(ERROR) << "GPU staging cleanup could not select device; "
                          "retaining allocations";
            return;
        }
        if (stream) cudaStreamSynchronize(stream);
        if (registered && !owner->unregisterLocalMemory(payload, true)) {
            // Never free an allocation while TE still owns its MR.
            LOG(ERROR) << "GPU staging unregister failed; retaining payload";
            payload = nullptr;
        }
        if (payload) cudaFree(payload);
        if (host_fragments) cudaFreeHost(host_fragments);
        if (device_fragments) cudaFree(device_fragments);
        if (stream) cudaStreamDestroy(stream);
    }

    bool Initialize(int selected) {
        if (device >= 0) return device == selected && enabled;
        device = selected;
        if (cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking) !=
            cudaSuccess)
            return false;
        if (cudaMalloc(&payload, capacity) != cudaSuccess) return false;
        if (!owner->RegisterLocalMemory(payload, capacity, kWildcardLocation,
                                        false, true)) {
            return false;
        }
        registered = true;
        LOG(INFO) << "GPU read staging initialized: device=" << device
                  << " bytes=" << capacity;
        return true;
    }

    bool ReserveDescriptors(size_t count) {
        if (count <= descriptor_capacity) return true;
        if (host_fragments) cudaFreeHost(host_fragments);
        if (device_fragments) cudaFree(device_fragments);
        host_fragments = nullptr;
        device_fragments = nullptr;
        descriptor_capacity = 0;
        const size_t bytes = count * sizeof(GpuReadFragment);
        if (cudaMallocHost(&host_fragments, bytes) != cudaSuccess ||
            cudaMalloc(&device_fragments, bytes) != cudaSuccess)
            return false;
        descriptor_capacity = count;
        return true;
    }

    bool ScatterAndWait(size_t count) {
        if (count == 0) return true;
        cudaError_t error = cudaMemcpyAsync(device_fragments, host_fragments,
                                            count * sizeof(GpuReadFragment),
                                            cudaMemcpyHostToDevice, stream);
        if (error == cudaSuccess)
            error =
                LaunchGpuReadScatter(payload, device_fragments, count, stream);
        // Drain the descriptor copy even if the subsequent launch failed.
        const cudaError_t completion = cudaStreamSynchronize(stream);
        if (error == cudaSuccess) error = completion;
        if (error == cudaSuccess) return true;
        LOG(ERROR) << "GPU staging scatter failed: "
                   << cudaGetErrorString(error);
        return false;
    }

    std::optional<ReadPlan> BuildPlan(
        const std::vector<std::string>& keys,
        const std::vector<QueryResult>& queries,
        const std::unordered_map<std::string, std::vector<Slice>>& slices,
        const ResolveRegion& resolve) const {
        if (queries.size() != keys.size()) return std::nullopt;
        ReadPlan plan;
        std::unordered_map<void*, int> devices;
        std::vector<std::pair<uintptr_t, uintptr_t>> destinations;
        size_t batch_bytes = 0, batch_fragments = 0;
        for (size_t index = 0; index < keys.size(); ++index) {
            const auto found = slices.find(keys[index]);
            if (found == slices.end() || found->second.empty())
                return std::nullopt;
            const auto& source_slices = found->second;
            if (source_slices.size() > kMaxFragments - plan.fragments.size())
                return std::nullopt;
            size_t bytes = 0;
            for (const auto& slice : source_slices) {
                if (slice.size == 0 || slice.size > capacity - bytes) {
                    VLOG(1)
                        << "GPU staging bypass: key exceeds buffer capacity";
                    return std::nullopt;  // A complete key must fit the buffer.
                }
                bytes += slice.size;
                const auto region = resolve(slice.ptr);
                if (!region) {
                    VLOG(1) << "GPU staging bypass: unregistered destination";
                    return std::nullopt;
                }
                const auto begin = reinterpret_cast<uintptr_t>(slice.ptr);
                const auto base = reinterpret_cast<uintptr_t>(region->base);
                if (begin < base || begin - base > region->size ||
                    slice.size > region->size - (begin - base))
                    return std::nullopt;
                auto [it, inserted] = devices.try_emplace(region->base, -1);
                if (inserted) {
                    cudaPointerAttributes attr{};
                    if (cudaPointerGetAttributes(&attr, region->base) !=
                        cudaSuccess) {
                        cudaGetLastError();
                        return std::nullopt;
                    }
                    if (attr.type != cudaMemoryTypeDevice) {
                        VLOG(1) << "GPU staging bypass: destination type="
                                << attr.type;
                        return std::nullopt;
                    }
                    it->second = attr.device;
                }
                if (plan.device < 0) plan.device = it->second;
                if (plan.device != it->second) return std::nullopt;
                if (slice.size > std::numeric_limits<uintptr_t>::max() - begin)
                    return std::nullopt;
                destinations.emplace_back(begin, begin + slice.size);
            }
            // Only complete memory objects with exact destination coverage.
            if (queries[index].replicas.size() != 1 ||
                !queries[index].replicas.front().is_memory_replica())
                return std::nullopt;
            const auto& replica = queries[index].replicas.front();
            const auto& descriptor =
                replica.get_memory_descriptor().buffer_descriptor;
            if (descriptor.protocol_ == "cxl") return std::nullopt;
            if (calculate_total_size(replica) != bytes) {
                VLOG(1) << "GPU staging bypass: object_bytes="
                        << calculate_total_size(replica)
                        << " destination_bytes=" << bytes;
                return std::nullopt;
            }
            if (batch_bytes > capacity - bytes) {
                plan.boundaries.push_back(index);
                batch_bytes = batch_fragments = 0;
            }
            plan.keys.push_back(
                {bytes, plan.fragments.size(), source_slices.size()});
            for (const auto& slice : source_slices) {
                plan.fragments.push_back(
                    {reinterpret_cast<uintptr_t>(slice.ptr), batch_bytes,
                     slice.size});
                batch_bytes += slice.size;
            }
            batch_fragments += source_slices.size();
            plan.max_batch_fragments =
                std::max(plan.max_batch_fragments, batch_fragments);
        }
        if (plan.fragments.size() == keys.size()) return std::nullopt;
        std::sort(destinations.begin(), destinations.end());
        for (size_t i = 1; i < destinations.size(); ++i) {
            if (destinations[i].first < destinations[i - 1].second)
                return std::nullopt;
        }
        plan.boundaries.push_back(keys.size());
        return plan;
    }

    bool Prepare(const ReadPlan& plan) {
        if (Initialize(plan.device) &&
            ReserveDescriptors(plan.max_batch_fragments))
            return true;
        enabled = false;
        cudaGetLastError();
        LOG(WARNING)
            << "GPU staging allocation/registration failed; using direct";
        return false;
    }

    Results ReadBatches(const ReadPlan& plan,
                        const std::vector<std::string>& keys,
                        const std::vector<QueryResult>& queries,
                        bool group_by_target) {
        Results results(keys.size(), tl::unexpected(ErrorCode::TRANSFER_FAIL));
        try {
            for (size_t b = 1; b < plan.boundaries.size(); ++b) {
                const size_t first = plan.boundaries[b - 1],
                             end = plan.boundaries[b];
                std::vector<std::string> batch_keys(keys.begin() + first,
                                                    keys.begin() + end);
                std::vector<QueryResult> batch_queries(queries.begin() + first,
                                                       queries.begin() + end);
                std::unordered_map<std::string, std::vector<Slice>> targets;
                for (size_t i = first; i < end; ++i) {
                    const auto& key = plan.keys[i];
                    const size_t offset =
                        plan.fragments[key.first_fragment].offset;
                    targets.emplace(
                        keys[i],
                        std::vector<Slice>{
                            {static_cast<char*>(payload) + offset, key.bytes}});
                }
                auto received = owner->BatchGet(batch_keys, batch_queries,
                                                targets, group_by_target);
                if (received.size() != end - first) {
                    LOG(ERROR) << "GPU staging incomplete batch result";
                    enabled = false;
                    return Results(keys.size(),
                                   tl::unexpected(ErrorCode::TRANSFER_FAIL));
                }
                size_t used = 0;
                for (size_t i = first; i < end; ++i) {
                    results[i] = received[i - first];
                    if (!results[i]) continue;
                    const auto& key = plan.keys[i];
                    std::memcpy(host_fragments + used,
                                plan.fragments.data() + key.first_fragment,
                                key.fragment_count * sizeof(GpuReadFragment));
                    used += key.fragment_count;
                }
                if (!ScatterAndWait(used)) {
                    enabled = false;
                    return Results(keys.size(),
                                   tl::unexpected(ErrorCode::TRANSFER_FAIL));
                }
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "GPU staging read failed: " << error.what();
            enabled = false;
            return Results(keys.size(),
                           tl::unexpected(ErrorCode::TRANSFER_FAIL));
        }
        VLOG(1) << "GPU staging read: keys=" << keys.size()
                << " fragments=" << plan.fragments.size()
                << " batches=" << plan.boundaries.size() - 1;
        return results;
    }

#endif
};

std::unique_ptr<GpuReadStaging> GpuReadStaging::Create(
    const std::shared_ptr<Client>& client) {
    if (!Environ::ReadOr(
            GpuReadStagingEnvironmentVariables::MC_STORE_GPU_STAGING, false))
        return nullptr;
#ifndef USE_CUDA
    LOG(WARNING) << "GPU read staging requires a CUDA-enabled build";
    return nullptr;
#else
    // Async hot-cache admission may retain staging slices after BatchGet.
    if (!client) return nullptr;
    if (client->IsHotCacheEnabled()) {
        LOG(WARNING) << "GPU read staging requires hot cache disabled; "
                        "using direct reads";
        return nullptr;
    }
    const size_t capacity = Environ::ReadOr(
        GpuReadStagingEnvironmentVariables::MC_STORE_GPU_STAGING_BUFFER_BYTES,
        size_t{256} * 1024 * 1024);
    if (capacity == 0) {
        LOG(WARNING)
            << "GPU read staging buffer size is zero; using direct reads";
        return nullptr;
    }
    return std::unique_ptr<GpuReadStaging>(
        new GpuReadStaging(client, capacity));
#endif
}

GpuReadStaging::GpuReadStaging(std::shared_ptr<Client> client, size_t capacity)
    : impl_(std::make_unique<GpuReadStagingImpl>(std::move(client), capacity)) {
}
GpuReadStaging::~GpuReadStaging() = default;

std::optional<GpuReadStaging::Results> GpuReadStaging::Read(
    const std::vector<std::string>& keys,
    const std::vector<QueryResult>& queries,
    const std::unordered_map<std::string, std::vector<Slice>>& slices,
    bool group_by_target, const ResolveRegion& resolve) {
#ifndef USE_CUDA
    return std::nullopt;
#else
    auto& state = *impl_;
    std::unique_lock lock(state.mutex, std::try_to_lock);
    if (!lock.owns_lock() || !state.enabled || keys.empty())
        return std::nullopt;

    const auto plan = state.BuildPlan(keys, queries, slices, resolve);
    if (!plan || (state.device >= 0 && state.device != plan->device))
        return std::nullopt;
    DeviceGuard guard(plan->device);
    if (!guard.valid) {
        cudaGetLastError();
        return std::nullopt;
    }
    if (!state.Prepare(*plan)) return std::nullopt;
    return state.ReadBatches(*plan, keys, queries, group_by_target);
#endif
}

}  // namespace mooncake
