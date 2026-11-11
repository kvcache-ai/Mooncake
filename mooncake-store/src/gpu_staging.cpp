#include "gpu_staging.h"

#include <algorithm>
#include <chrono>
#include <iterator>
#include <map>
#include <mutex>

#include <glog/logging.h>

#include "client_service.h"
#include "environ.h"
#include "environment_variables.h"
#ifdef USE_CUDA
#include "gpu_staging_kernel.h"
#endif

namespace mooncake {
namespace {

// Keys start at this alignment inside a half of the buffer.
constexpr size_t kAlignment = 256;

size_t AlignUp(size_t bytes) {
    return (bytes + kAlignment - 1) / kAlignment * kAlignment;
}

}  // namespace

#ifdef USE_CUDA
namespace {

using Clock = std::chrono::steady_clock;

struct DeviceGuard {
    int previous = -1;
    bool valid = false;
    explicit DeviceGuard(int selected) {
        valid =
            cudaGetDevice(&previous) == cudaSuccess &&
            (previous == selected || cudaSetDevice(selected) == cudaSuccess);
        if (!valid) cudaGetLastError();
    }
    ~DeviceGuard() {
        if (previous >= 0) cudaSetDevice(previous);
    }
};

// The device that owns `ptr`, or -1 when it is not device memory.
int PointerDevice(const void* ptr) {
    cudaPointerAttributes attr{};
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    return attr.type == cudaMemoryTypeDevice ? attr.device : -1;
}

// The keys of one call that go through the buffer, all on one device.
struct Plan {
    int device = -1;
    std::vector<size_t> staged;  // positions in the call, in call order
    std::vector<size_t> bytes;   // object size of each staged key
    std::vector<size_t> direct;  // positions left to the per-part path
};

// Staged keys packed into one half of the buffer.
struct Batch {
    int half = -1;
    std::vector<size_t> keys;                // positions in the call
    std::vector<std::vector<Slice>> slices;  // each key's staging slice
};

struct Half {
    char* base = nullptr;
    cudaEvent_t done = nullptr;  // end of the half's last kernel
    GpuStagingCopy* host_copies = nullptr;
    GpuStagingCopy* device_copies = nullptr;
    size_t copy_capacity = 0;
    Clock::time_point quarantined_until{};
};

// A registered buffer and the device that owns it.
struct KnownRegion {
    uintptr_t base = 0;
    size_t size = 0;
    int device = -1;  // -1: not device memory

    bool Holds(uintptr_t begin, size_t bytes) const {
        return begin >= base && begin - base < size &&
               bytes <= size - (begin - base);
    }
};

}  // namespace
#endif

struct GpuStagingImpl {
    using Results = GpuStaging::Results;
    using Transfer = GpuStaging::Transfer;
    using ResolveRegion = GpuStaging::ResolveRegion;

    GpuStagingImpl(std::shared_ptr<Client> client, GpuStagingOptions opts)
        : owner(std::move(client)),
          options(opts),
          half_bytes(opts.buffer_bytes / 2 / kAlignment * kAlignment) {}

    const std::shared_ptr<Client> owner;
    const GpuStagingOptions options;
    const size_t half_bytes;

#ifdef USE_CUDA
    // Calls are serialized; RealClient keeps one instance per direction.
    std::mutex mutex;
    bool enabled = true;
    int device = -1;
    cudaStream_t stream = nullptr;
    void* buffer = nullptr;
    bool registered = false;
    Half halves[2];

    ~GpuStagingImpl() {
        if (device < 0) return;
        DeviceGuard guard(device);
        if (!guard.valid) {
            LOG(ERROR) << "GPU staging cleanup could not select device "
                       << device << "; retaining allocations";
            return;
        }
        if (stream) cudaStreamSynchronize(stream);
        const auto now = Clock::now();
        const bool in_flight = std::any_of(
            std::begin(halves), std::end(halves),
            [&](const Half& half) { return half.quarantined_until > now; });
        if (in_flight) {
            // A timed-out transfer may still copy into or out of the buffer.
            LOG(WARNING) << "GPU staging buffer retained: a quarantined "
                            "transfer may still be in flight";
        } else if (registered && !Unregister()) {
            LOG(ERROR) << "GPU staging unregister failed; retaining buffer";
        } else if (buffer) {
            cudaFree(buffer);
        }
        for (auto& half : halves) {
            if (half.host_copies) cudaFreeHost(half.host_copies);
            if (half.device_copies) cudaFree(half.device_copies);
            if (half.done) cudaEventDestroy(half.done);
        }
        if (stream) cudaStreamDestroy(stream);
    }

    // The NVLink transport keeps no record of plain device memory (it is a
    // local buffer only), so it reports such a buffer as not registered.
    bool Unregister() {
        const auto result = owner->unregisterLocalMemory(buffer, true);
        return result || result.error() == ErrorCode::NOT_REGISTERED;
    }

    // Allocates and registers the buffer on first use. If that fails,
    // staging stays off for this client.
    bool Prepare(int selected) {
        if (device >= 0) return device == selected;
        device = selected;
        auto error = cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
        for (auto& half : halves) {
            if (error == cudaSuccess)
                error = cudaEventCreateWithFlags(&half.done,
                                                 cudaEventDisableTiming);
        }
        if (error == cudaSuccess) error = cudaMalloc(&buffer, 2 * half_bytes);
        bool ok = error == cudaSuccess;
        if (ok && owner) {
            registered = static_cast<bool>(owner->RegisterLocalMemory(
                buffer, 2 * half_bytes, kWildcardLocation, false, true));
            ok = registered;
        }
        if (!ok) {
            enabled = false;
            cudaGetLastError();
            LOG(WARNING) << "GPU staging allocation or registration failed; "
                            "using the per-part path";
            return false;
        }
        halves[0].base = static_cast<char*>(buffer);
        halves[1].base = halves[0].base + half_bytes;
        LOG(INFO) << "GPU staging initialized: device=" << device
                  << " bytes=" << 2 * half_bytes;
        return true;
    }

    // Device of the registered region that holds all of `part`, or -1.
    // `recent` is the region of the same part position in the previous key:
    // with one buffer per layer, it holds this part too. `known` holds every
    // region resolved so far in the call.
    static int DeviceOf(const Slice& part, const ResolveRegion& resolve,
                        std::map<uintptr_t, KnownRegion>& known,
                        KnownRegion& recent) {
        const auto begin = reinterpret_cast<uintptr_t>(part.ptr);
        if (!recent.Holds(begin, part.size)) {
            const auto it = known.upper_bound(begin);
            if (it != known.begin() &&
                std::prev(it)->second.Holds(begin, part.size)) {
                recent = std::prev(it)->second;
            } else if (const auto region = resolve(part.ptr)) {
                recent = {reinterpret_cast<uintptr_t>(region->base),
                          region->size, PointerDevice(region->base)};
                known.insert_or_assign(recent.base, recent);
            } else {
                return -1;
            }
        }
        return recent.Holds(begin, part.size) ? recent.device : -1;
    }

    // A key is staged when it has more than one part, every part lies in a
    // registered device buffer on the staging device, and the object fits in
    // half of the buffer (and, for reads, matches the stored size).
    // Overlapping destinations are not checked: their final bytes are
    // unspecified on the per-part path too, whose copies are unordered.
    std::optional<Plan> BuildPlan(const std::vector<std::vector<Slice>>& parts,
                                  const std::vector<size_t>* object_bytes,
                                  const ResolveRegion& resolve) const {
        Plan plan;
        plan.device = device;
        std::map<uintptr_t, KnownRegion> known;
        std::vector<KnownRegion> recent;  // by part position
        for (size_t i = 0; i < parts.size(); ++i) {
            bool stage = parts[i].size() > 1 &&
                         (!object_bytes || (*object_bytes)[i] > 0);
            if (recent.size() < parts[i].size()) recent.resize(parts[i].size());
            size_t bytes = 0;
            for (size_t j = 0; stage && j < parts[i].size(); ++j) {
                const auto& part = parts[i][j];
                stage = part.size > 0 && part.size <= half_bytes - bytes;
                if (!stage) break;
                bytes += part.size;
                const int part_device =
                    DeviceOf(part, resolve, known, recent[j]);
                if (plan.device < 0) plan.device = part_device;
                stage = part_device >= 0 && part_device == plan.device;
            }
            if (stage && object_bytes) stage = (*object_bytes)[i] == bytes;
            if (!stage) {
                plan.direct.push_back(i);
                continue;
            }
            plan.staged.push_back(i);
            plan.bytes.push_back(bytes);
        }
        if (plan.staged.empty()) return std::nullopt;
        return plan;
    }

    // `preferred` unless it is quarantined, else the other half, else -1.
    int PickHalf(int preferred) const {
        const auto now = Clock::now();
        for (int half : {preferred, preferred ^ 1}) {
            if (halves[half].quarantined_until <= now) return half;
        }
        return -1;
    }

    // Packs staged keys, from plan.staged[next] on, into `half`.
    Batch Pack(const Plan& plan, size_t& next, int half) const {
        Batch batch;
        batch.half = half;
        size_t used = 0;
        while (next < plan.staged.size()) {
            const size_t offset = AlignUp(used);
            const size_t bytes = plan.bytes[next];
            if (offset > half_bytes || bytes > half_bytes - offset) break;
            batch.keys.push_back(plan.staged[next]);
            batch.slices.push_back({Slice{halves[half].base + offset, bytes}});
            used = offset + bytes;
            ++next;
        }
        return batch;
    }

    bool Reserve(Half& half, size_t count) {
        if (count <= half.copy_capacity) return true;
        if (half.host_copies) cudaFreeHost(half.host_copies);
        if (half.device_copies) cudaFree(half.device_copies);
        half.host_copies = nullptr;
        half.device_copies = nullptr;
        half.copy_capacity = 0;
        const size_t capacity = std::max(count, size_t{4096});
        if (cudaMallocHost(&half.host_copies,
                           capacity * sizeof(GpuStagingCopy)) != cudaSuccess ||
            cudaMalloc(&half.device_copies,
                       capacity * sizeof(GpuStagingCopy)) != cudaSuccess)
            return false;
        half.copy_capacity = capacity;
        return true;
    }

    // Launches one kernel that copies the parts of the batch's keys marked
    // `ok`: into the staging slices when `gather`, out of them otherwise.
    bool Launch(const Batch& batch,
                const std::vector<std::vector<Slice>>& parts,
                const std::vector<bool>& ok, bool gather) {
        Half& half = halves[batch.half];
        // The half's previous kernel may still read the descriptors.
        if (cudaEventSynchronize(half.done) != cudaSuccess) return false;
        size_t count = 0;
        for (size_t k = 0; k < batch.keys.size(); ++k) {
            if (ok[k]) count += parts[batch.keys[k]].size();
        }
        if (count == 0) return true;
        if (!Reserve(half, count)) return false;
        size_t n = 0;
        for (size_t k = 0; k < batch.keys.size(); ++k) {
            if (!ok[k]) continue;
            auto staging =
                reinterpret_cast<uintptr_t>(batch.slices[k].front().ptr);
            for (const auto& part : parts[batch.keys[k]]) {
                const auto user = reinterpret_cast<uintptr_t>(part.ptr);
                half.host_copies[n++] =
                    gather ? GpuStagingCopy{user, staging, part.size}
                           : GpuStagingCopy{staging, user, part.size};
                staging += part.size;
            }
        }
        cudaError_t error = cudaMemcpyAsync(
            half.device_copies, half.host_copies,
            count * sizeof(GpuStagingCopy), cudaMemcpyHostToDevice, stream);
        if (error == cudaSuccess)
            error = LaunchGpuStagingCopies(half.device_copies, count, stream);
        if (error == cudaSuccess) error = cudaEventRecord(half.done, stream);
        if (error == cudaSuccess) return true;
        LOG(ERROR) << "GPU staging kernel launch failed: "
                   << cudaGetErrorString(error);
        return false;
    }

    static Results Call(const Transfer& transfer,
                        const std::vector<size_t>& indices,
                        const std::vector<std::vector<Slice>>& slices) {
        auto results = transfer(indices, slices);
        if (results.size() == indices.size()) return results;
        LOG(ERROR) << "GPU staging: transfer returned " << results.size()
                   << " results for " << indices.size() << " keys";
        return Results(indices.size(),
                       tl::unexpected(ErrorCode::TRANSFER_FAIL));
    }

    // Transfers a batch through its half. A transfer that fails after the
    // transfer engine's wait timeout may still be copying, so its half stays
    // out of use for a while.
    Results Send(const Transfer& transfer, const Batch& batch) {
        const auto start = Clock::now();
        auto results = Call(transfer, batch.keys, batch.slices);
        const auto elapsed = Clock::now() - start;
        if (elapsed >= std::chrono::milliseconds(options.late_failure_ms) &&
            std::any_of(results.begin(), results.end(),
                        [](const auto& result) { return !result; })) {
            halves[batch.half].quarantined_until =
                Clock::now() + std::chrono::milliseconds(options.quarantine_ms);
            LOG(WARNING)
                << "GPU staging: a transfer failed after "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       elapsed)
                       .count()
                << " ms and may still be in flight; half " << batch.half
                << " is unused for " << options.quarantine_ms << " ms";
        }
        return results;
    }

    // Transfers `positions` with their own slices (the per-part path).
    static void Direct(const std::vector<size_t>& positions,
                       const std::vector<std::vector<Slice>>& parts,
                       const Transfer& transfer, Results& results) {
        if (positions.empty()) return;
        std::vector<std::vector<Slice>> slices;
        slices.reserve(positions.size());
        for (size_t i : positions) slices.push_back(parts[i]);
        auto got = Call(transfer, positions, slices);
        for (size_t n = 0; n < positions.size(); ++n)
            results[positions[n]] = std::move(got[n]);
    }

    // Runs one call. Keys that cannot be staged take the per-part path;
    // `stage` moves the staged keys from plan.staged[next] on and returns
    // false on a CUDA error. Keys it leaves (both halves quarantined) take
    // the per-part path too. After a CUDA error the keys in `unverified`
    // fail, since their kernel may not have run, and staging turns off.
    template <typename Stage>
    std::optional<Results> Coalesce(
        const std::vector<std::vector<Slice>>& parts,
        const std::vector<size_t>* object_bytes, const ResolveRegion& resolve,
        const Transfer& transfer, Stage stage) {
        std::lock_guard<std::mutex> lock(mutex);
        if (!enabled) return std::nullopt;
        const auto plan = BuildPlan(parts, object_bytes, resolve);
        if (!plan) return std::nullopt;
        DeviceGuard guard(plan->device);
        if (!guard.valid || !Prepare(plan->device)) return std::nullopt;

        Results results(parts.size(), tl::unexpected(ErrorCode::TRANSFER_FAIL));
        Direct(plan->direct, parts, transfer, results);
        size_t next = 0;
        std::vector<size_t> unverified;
        bool failed = !stage(*plan, next, results, unverified);
        for (auto& half : halves) {
            failed |= cudaEventSynchronize(half.done) != cudaSuccess;
        }
        if (failed) {
            LOG(ERROR) << "GPU staging failed: "
                       << cudaGetErrorString(cudaGetLastError())
                       << "; using the per-part path from now on";
            enabled = false;
            for (size_t i : unverified)
                results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            return results;
        }
        Direct(std::vector<size_t>(plan->staged.begin() + next,
                                   plan->staged.end()),
               parts, transfer, results);
        return results;
    }

    std::optional<Results> Read(const std::vector<std::vector<Slice>>& parts,
                                const std::vector<size_t>& object_bytes,
                                const ResolveRegion& resolve,
                                const Transfer& read) {
        if (object_bytes.size() != parts.size()) return std::nullopt;
        return Coalesce(
            parts, &object_bytes, resolve, read,
            [&](const Plan& plan, size_t& next, Results& results,
                std::vector<size_t>& scattered) {
                for (int preferred = 0; next < plan.staged.size();) {
                    const int half = PickHalf(preferred);
                    if (half < 0) return true;
                    // Nothing may land in a half its last kernel still reads.
                    if (cudaEventSynchronize(halves[half].done) != cudaSuccess)
                        return false;
                    const Batch batch = Pack(plan, next, half);
                    auto got = Send(read, batch);
                    std::vector<bool> ok(batch.keys.size());
                    for (size_t k = 0; k < batch.keys.size(); ++k) {
                        ok[k] = got[k].has_value();
                        if (ok[k]) scattered.push_back(batch.keys[k]);
                        results[batch.keys[k]] = std::move(got[k]);
                    }
                    if (!Launch(batch, parts, ok, /*gather=*/false))
                        return false;
                    preferred = half ^ 1;
                }
                return true;
            });
    }

    std::optional<Results> Write(const std::vector<std::vector<Slice>>& parts,
                                 const ResolveRegion& resolve,
                                 const Transfer& write) {
        // Keys already written keep their results after a CUDA error.
        return Coalesce(
            parts, nullptr, resolve, write,
            [&](const Plan& plan, size_t& next, Results& results,
                std::vector<size_t>&) {
                std::optional<Batch> ready;  // gathered, not yet written
                bool ok = true;
                const auto gather = [&](int preferred) {
                    const int half = PickHalf(preferred);
                    if (half < 0) return;
                    Batch batch = Pack(plan, next, half);
                    ok = Launch(batch, parts,
                                std::vector<bool>(batch.keys.size(), true),
                                /*gather=*/true);
                    if (ok) ready = std::move(batch);
                };
                gather(0);
                while (ready && ok) {
                    const Batch batch = std::move(*ready);
                    ready.reset();
                    // The next batch gathers into the other half meanwhile.
                    const int other = batch.half ^ 1;
                    if (next < plan.staged.size() && PickHalf(other) == other)
                        gather(other);
                    if (!ok || cudaEventSynchronize(halves[batch.half].done) !=
                                   cudaSuccess)
                        return false;
                    auto got = Send(write, batch);
                    for (size_t k = 0; k < batch.keys.size(); ++k)
                        results[batch.keys[k]] = std::move(got[k]);
                    if (!ready && next < plan.staged.size()) gather(batch.half);
                }
                return ok;
            });
    }
#endif
};

std::unique_ptr<GpuStaging> GpuStaging::Create(
    const std::shared_ptr<Client>& client) {
    if (!Environ::ReadOr(NvlinkCoalesceEnvironmentVariables::MC_NVLINK_COALESCE,
                         false))
        return nullptr;
#ifndef USE_CUDA
    LOG(WARNING) << "MC_NVLINK_COALESCE requires a CUDA build; ignored";
    return nullptr;
#else
    if (!client) return nullptr;
    // The hot cache may still read a transfer's slices after it returns, by
    // which time the staging buffer holds another batch.
    if (client->IsHotCacheEnabled()) {
        LOG(WARNING) << "MC_NVLINK_COALESCE is ignored while the local hot "
                        "cache is enabled";
        return nullptr;
    }
    GpuStagingOptions options;
    options.buffer_bytes =
        Environ::ReadOr(
            NvlinkCoalesceEnvironmentVariables::MC_NVLINK_COALESCE_STAGING_MB,
            uint64_t{options.buffer_bytes >> 20})
        << 20;
    if (options.buffer_bytes == 0) {
        LOG(WARNING) << "MC_NVLINK_COALESCE_STAGING_MB is 0; coalescing off";
        return nullptr;
    }
    return std::make_unique<GpuStaging>(client, options);
#endif
}

GpuStaging::GpuStaging(std::shared_ptr<Client> client,
                       GpuStagingOptions options)
    : impl_(std::make_unique<GpuStagingImpl>(std::move(client), options)) {}

GpuStaging::~GpuStaging() = default;

std::optional<GpuStaging::Results> GpuStaging::Read(
    const std::vector<std::vector<Slice>>& parts,
    const std::vector<size_t>& object_bytes, const ResolveRegion& resolve,
    const Transfer& read) {
#ifdef USE_CUDA
    return impl_->Read(parts, object_bytes, resolve, read);
#else
    return std::nullopt;
#endif
}

std::optional<GpuStaging::Results> GpuStaging::Write(
    const std::vector<std::vector<Slice>>& parts, const ResolveRegion& resolve,
    const Transfer& write) {
#ifdef USE_CUDA
    return impl_->Write(parts, resolve, write);
#else
    return std::nullopt;
#endif
}

}  // namespace mooncake
