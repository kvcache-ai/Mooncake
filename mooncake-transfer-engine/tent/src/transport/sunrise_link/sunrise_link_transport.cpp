// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/transport/sunrise_link/sunrise_link_transport.h"

#include <dlfcn.h>
#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/topology.h"
#include "tent/common/utils/string_builder.h"

#include <tang_runtime_api.h>
#include <ptml.h>

namespace mooncake {
namespace tent {

namespace {

// Default matches typical Tang RT install; override root and/or arch via env.
static std::string TangRtSharedObjectPath(const char* soname) {
    const char* root = std::getenv("MC_TANGRT_ROOT");
    std::string r = (root && root[0]) ? std::string(root)
                                      : std::string("/usr/local/tangrt");
    while (!r.empty() && r.back() == '/') r.pop_back();
    const char* arch = std::getenv("MC_TANGRT_LIB_ARCH");
    std::string arch_dir =
        (arch && arch[0]) ? std::string(arch) : std::string("linux-x86_64");
    return r + "/lib/" + arch_dir + "/" + soname;
}

constexpr int kMaxPhytopoPorts = 10;

static tangMemcpyKind MemcpyKindForPointers(tangMemoryType st,
                                            tangMemoryType dt) {
    if (st == tangMemoryTypeDevice && dt == tangMemoryTypeHost) {
        return tangMemcpyDeviceToHost;
    }
    if (st == tangMemoryTypeHost && dt == tangMemoryTypeDevice) {
        return tangMemcpyHostToDevice;
    }
    if (st == tangMemoryTypeDevice && dt == tangMemoryTypeDevice) {
        return tangMemcpyDeviceToDevice;
    }
    return tangMemcpyHostToHost;
}

// Sunrise S2 path may allocate mapped GPU memory via tangHostAlloc.
// For kernel/device-copy style paths, convert mapped host pointer into its
// corresponding device pointer when available.
static void NormalizeMappedHostPointer(void** ptr,
                                       tangPointerAttributes* attr) {
    if (!ptr || !*ptr || !attr) return;
    if (attr->type != tangMemoryTypeHost) return;

    void* dev_ptr = nullptr;
    tangError_t ret = tangHostGetDevicePointer(&dev_ptr, *ptr, 0);
    if (ret != tangSuccess || !dev_ptr) return;

    tangPointerAttributes dev_attr = {};
    tangPointerGetAttributes(&dev_attr, dev_ptr);
    if (dev_attr.type == tangMemoryTypeDevice) {
        *ptr = dev_ptr;
        *attr = dev_attr;
    }
}

static tangError_t QueryPointerAttrsBestEffort(void* ptr,
                                               tangPointerAttributes* attr,
                                               int preferred_dev) {
    if (!ptr || !attr) return tangErrorInvalidValue;
    tangError_t ret = tangPointerGetAttributes(attr, ptr);
    if (ret == tangSuccess) return ret;

    if (preferred_dev >= 0) {
        tangSetDevice(preferred_dev);
        ret = tangPointerGetAttributes(attr, ptr);
        if (ret == tangSuccess) return ret;
    }

    int dev_count = 0;
    if (tangGetDeviceCount(&dev_count) != tangSuccess || dev_count <= 0) {
        return ret;
    }
    for (int d = 0; d < dev_count; ++d) {
        tangSetDevice(d);
        ret = tangPointerGetAttributes(attr, ptr);
        if (ret == tangSuccess) return ret;
    }
    return ret;
}

// Fallback when tangMemcpyPeer_v2 fails: map peer device memory through PTLink.
static tangError_t C2cMemcpyWithPeerMap(int src_dev, int dst_dev,
                                        const void* src, void* dst, size_t len,
                                        int port_src_to_dst,
                                        int port_dst_to_src) {
    tangError_t ret = tangErrorUnknown;
    void* mapped = nullptr;

    if (port_src_to_dst >= 0) {
        ret = tangDeviceGetPeerPointer(src_dev, port_src_to_dst, dst, &mapped);
        if (ret != tangSuccess) {
            ret = tangSetDevice(src_dev);
            if (ret != tangSuccess) return ret;
            ret = tangDeviceGetPeerPointer(src_dev, port_src_to_dst, dst,
                                           &mapped);
        }
        if (ret == tangSuccess) {
            ret = tangMemcpy(mapped, src, len, tangMemcpyDeviceToDevice);
        }
        return ret;
    }
    if (port_dst_to_src >= 0) {
        ret = tangDeviceGetPeerPointer(dst_dev, port_dst_to_src,
                                       const_cast<void*>(src), &mapped);
        if (ret != tangSuccess) {
            ret = tangSetDevice(dst_dev);
            if (ret != tangSuccess) return ret;
            ret = tangDeviceGetPeerPointer(dst_dev, port_dst_to_src,
                                           const_cast<void*>(src), &mapped);
        }
        if (ret == tangSuccess) {
            ret = tangMemcpy(dst, mapped, len, tangMemcpyDeviceToDevice);
        }
        return ret;
    }
    return tangErrorInvalidValue;
}

}  // namespace

static ptmlReturn_t (*ptmlInit)() = nullptr;
static ptmlReturn_t (*ptmlPtlinkEnableAll)() = nullptr;
static ptmlReturn_t (*ptmlPtlinkPhytopoDetect)(ptmlDevice_t dev, int len,
                                               void* data) = nullptr;
static ptmlReturn_t (*ptmlDeviceGetCount)(int* devCount) = nullptr;

// Tang memcpy must NOT use one process-wide lock: it serialized every
// tangMemcpyPeer_v2 and made multi-threaded C2C benches ~3 orders of magnitude
// slower than PCCL (which runs many concurrent device copies). We only guard
// tangSetDevice + non-peer tangMemcpy with per-device mutexes; peer copies use
// device ids passed into tangMemcpyPeer_v2 and run without that serialization.

namespace {

constexpr int kMaxTangDevLocks = 32;

static std::mutex& devMutex(int dev) {
    static std::mutex table[kMaxTangDevLocks];
    if (dev < 0 || dev >= kMaxTangDevLocks) {
        static std::mutex fallback;
        return fallback;
    }
    return table[dev];
}

// Serialize tangSetDevice + context-bound ops for a device; for peer pairs,
// lock both devices in a fixed order to avoid deadlock.
struct ScopedTangDeviceOps {
    int lo{-1};
    int hi{-1};

    explicit ScopedTangDeviceOps(int src_dev, int dst_dev) {
        if (src_dev < 0 && dst_dev < 0) {
            return;
        }
        if (src_dev < 0 || dst_dev < 0) {
            int d = src_dev >= 0 ? src_dev : dst_dev;
            devMutex(d).lock();
            lo = d;
            return;
        }
        if (src_dev == dst_dev) {
            devMutex(src_dev).lock();
            lo = src_dev;
            return;
        }
        lo = std::min(src_dev, dst_dev);
        hi = std::max(src_dev, dst_dev);
        devMutex(lo).lock();
        devMutex(hi).lock();
    }

    ~ScopedTangDeviceOps() {
        if (hi >= 0) {
            devMutex(hi).unlock();
            devMutex(lo).unlock();
        } else if (lo >= 0) {
            devMutex(lo).unlock();
        }
    }
};

}  // namespace

class ThreadLocalTangStreamPool {
   public:
    tangStream_t getOrCreate(int device_id) {
        if (device_id < 0) return nullptr;
        auto it = streams_by_device_.find(device_id);
        if (it != streams_by_device_.end()) return it->second;

        ScopedTangDeviceOps ctx_lock(device_id, device_id);
        int saved_dev = -1;
        tangGetDevice(&saved_dev);
        tangError_t set_ret = tangSetDevice(device_id);
        if (set_ret != tangSuccess) {
            LOG(ERROR) << "tangSetDevice(" << device_id
                       << ") failed for stream creation: " << set_ret << " "
                       << tangGetErrorString(set_ret);
            return nullptr;
        }

        tangStream_t stream = nullptr;
        tangError_t create_ret =
            tangStreamCreateWithFlags(&stream, tangStreamNonBlocking);
        if (saved_dev >= 0) tangSetDevice(saved_dev);
        if (create_ret != tangSuccess) {
            LOG(ERROR) << "tangStreamCreateWithFlags failed: " << create_ret
                       << " " << tangGetErrorString(create_ret)
                       << ", device=" << device_id;
            return nullptr;
        }
        streams_by_device_[device_id] = stream;
        return stream;
    }

    ~ThreadLocalTangStreamPool() {
        for (const auto& kv : streams_by_device_) {
            if (kv.second) tangStreamDestroy(kv.second);
        }
    }

   private:
    std::unordered_map<int, tangStream_t> streams_by_device_;
};

thread_local ThreadLocalTangStreamPool tl_stream_pool;

SunriseLinkTransport::SunriseLinkTransport() : installed_(false) {
    // Match NVLink-style caps: routing may see MTYPE_CPU if pointer probe fails
    // while the remote segment is VRAM; dram_to_gpu must still be available.
    caps.dram_to_dram = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;
}

SunriseLinkTransport::~SunriseLinkTransport() { uninstall(); }

Status SunriseLinkTransport::install(std::string& local_segment_name,
                                     std::shared_ptr<ControlService> metadata,
                                     std::shared_ptr<Topology> local_topology,
                                     std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "SunriseLink transport has been installed" LOC_MARK);
    }

    conf_ = conf;
    metadata_ = metadata;

    auto status = initSunriseLink();
    if (!status.ok()) {
        LOG(WARNING) << "SunriseLink transport initSunriseLink fail";
        return status;
    }

    status = detectTopology();
    if (!status.ok()) {
        LOG(WARNING) << "SunriseLink transport detectTopology fail";
        return status;
    }

    installed_ = true;
    async_memcpy_threshold_ =
        conf_
            ? conf_->get("transports/sunrise_link/async_memcpy_threshold", 0) *
                  1024
            : 0;
    LOG(INFO) << "SunriseLink transport installed successfully";

    return Status::OK();
}

Status SunriseLinkTransport::initSunriseLink() {
    ptml_handle_ =
        dlopen(TangRtSharedObjectPath("libptml_shared.so").c_str(), RTLD_NOW);
    if (!ptml_handle_) {
        char* error = dlerror();
        LOG(ERROR) << "Failed to load libptml_shared.so: "
                   << (error ? error : "unknown error");
        return Status::InternalError("Failed to load PTML library");
    }

#define LOAD_PTML_SYM(handle, symbol, funcptr)                       \
    do {                                                             \
        void* tmp = dlsym(handle, symbol);                           \
        if (tmp == nullptr) {                                        \
            LOG(ERROR) << "dlsym failed on " << symbol;              \
            return Status::InternalError("Failed to load " #symbol); \
        }                                                            \
        *(void**)&funcptr = tmp;                                     \
    } while (0)

    LOAD_PTML_SYM(ptml_handle_, "ptmlInit", ptmlInit);
    LOAD_PTML_SYM(ptml_handle_, "ptmlPtlinkEnableAll", ptmlPtlinkEnableAll);
    LOAD_PTML_SYM(ptml_handle_, "ptmlPtlinkPhytopoDetect",
                  ptmlPtlinkPhytopoDetect);
    LOAD_PTML_SYM(ptml_handle_, "ptmlDeviceGetCount", ptmlDeviceGetCount);

    ptmlReturn_t ret = ptmlInit();
    if (ret != PTML_SUCCESS) {
        LOG(ERROR) << "ptmlInit failed, error code: " << ret;
        return Status::InternalError("Failed to initialize PTML");
    }

    ret = ptmlPtlinkEnableAll();
    if (ret != PTML_SUCCESS) {
        LOG(WARNING) << "ptmlPtlinkEnableAll failed, error code: " << ret;
    }

    runtime_lib_handle_ =
        dlopen(TangRtSharedObjectPath("libtangrt_shared.so").c_str(), RTLD_NOW);
    if (!runtime_lib_handle_) {
        char* error = dlerror();
        LOG(ERROR) << "Failed to load libtangrt_shared.so: "
                   << (error ? error : "unknown error");
        return Status::InternalError("Failed to load Tang library");
    }

    LOG(INFO) << "SunriseLink runtime initialized successfully (PTML + Tang)";
    return Status::OK();
}

Status SunriseLinkTransport::detectTopology() {
    topology_map_.clear();
    c2c_port_by_pair_.clear();
    local_devices_.clear();

    int device_count = 0;
    ptmlReturn_t pret = ptmlDeviceGetCount(&device_count);
    if (pret != PTML_SUCCESS || device_count <= 0) {
        LOG(ERROR) << "ptmlDeviceGetCount failed, ret=" << pret
                   << " count=" << device_count;
        return Status::InternalError("ptmlDeviceGetCount failed");
    }

    std::vector<ptPhyTopo_t> phytopo(static_cast<size_t>(device_count) *
                                     kMaxPhytopoPorts);

    for (int dev = 0; dev < device_count; ++dev) {
        memset(phytopo.data() + static_cast<size_t>(dev) * kMaxPhytopoPorts, 0,
               kMaxPhytopoPorts * sizeof(ptPhyTopo_t));

        pret = ptmlPtlinkPhytopoDetect(
            dev, kMaxPhytopoPorts * sizeof(ptPhyTopo_t),
            phytopo.data() + static_cast<size_t>(dev) * kMaxPhytopoPorts);
        if (pret != PTML_SUCCESS) {
            LOG(WARNING) << "ptmlPtlinkPhytopoDetect failed for device " << dev
                         << " ret=" << pret;
            continue;
        }

        SunriseLinkDeviceInfo info;
        info.device_id = dev;

        ptPhyTopo_t* row =
            phytopo.data() + static_cast<size_t>(dev) * kMaxPhytopoPorts;
        for (int p = 0; p < kMaxPhytopoPorts; ++p) {
            if (!row[p].link_status) {
                continue;
            }
            int lc = static_cast<int>(row[p].local_chipid);
            int rc = static_cast<int>(row[p].remote_chipid);
            int lp = static_cast<int>(row[p].local_port);
            auto key = std::make_pair(lc, rc);
            if (c2c_port_by_pair_.find(key) == c2c_port_by_pair_.end()) {
                c2c_port_by_pair_[key] = lp;
            }

            SunriseLinkPeerInfo peer;
            peer.peer_device_id = rc;
            peer.peer_port = lp;
            peer.is_active = true;
            topology_map_[dev].push_back(peer);
            info.active_ports.push_back(lp);

            LOG(INFO) << "PTLink topo dev=" << dev << " port=" << lp << " link "
                      << lc << " -> " << rc << " (remote_port="
                      << static_cast<int>(row[p].remote_port) << ")";
        }

        local_devices_.push_back(std::move(info));
    }

    LOG(INFO) << "SunriseLink topology: " << device_count << " device(s), "
              << c2c_port_by_pair_.size() << " C2C port mapping(s)";
    return Status::OK();
}

Status SunriseLinkTransport::allocateSubBatch(SubBatchRef& batch,
                                              size_t max_size) {
    auto shm_batch = Slab<SunriseLinkSubBatch>::Get().allocate();
    if (!shm_batch) {
        return Status::InternalError(
            "Unable to allocate SunriseLink sub-batch");
    }
    batch = shm_batch;
    // Slab reuse can leave stale tasks; sub_task_id must index a fresh list.
    shm_batch->task_list.clear();
    shm_batch->task_list.reserve(max_size);
    shm_batch->max_size = max_size;
    // Synchronous copy path (can_async=false in startTransfer) does not rely on
    // stream completion; avoid creating thread-local Tang stream here to reduce
    // device-context coupling in worker threads.
    shm_batch->stream = nullptr;
    return Status::OK();
}

Status SunriseLinkTransport::freeSubBatch(SubBatchRef& batch) {
    auto shm_batch = dynamic_cast<SunriseLinkSubBatch*>(batch);
    if (!shm_batch) {
        return Status::InvalidArgument(
            "Invalid SunriseLink sub-batch" LOC_MARK);
    }
    shm_batch->task_list.clear();
    Slab<SunriseLinkSubBatch>::Get().deallocate(shm_batch);
    batch = nullptr;
    return Status::OK();
}

Status SunriseLinkTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto shm_batch = dynamic_cast<SunriseLinkSubBatch*>(batch);
    if (!shm_batch) {
        return Status::InvalidArgument(
            "Invalid SunriseLink sub-batch" LOC_MARK);
    }

    if (request_list.size() + shm_batch->task_list.size() >
        shm_batch->max_size) {
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    }

    auto infer_local_dev = [&](void* ptr) -> int {
        uint64_t p = reinterpret_cast<uint64_t>(ptr);
        std::lock_guard<std::mutex> guard(registered_memory_mutex_);
        for (const auto& entry : registered_memory_) {
            uint64_t base = reinterpret_cast<uint64_t>(entry.first);
            if (base <= p && p < base + entry.second) {
                auto it = registered_memory_gpu_id_.find(entry.first);
                if (it != registered_memory_gpu_id_.end()) return it->second;
            }
        }
        return -1;
    };
    auto infer_local_dev_best_effort = [&](void* ptr) -> int {
        int dev = infer_local_dev(ptr);
        if (dev >= 0) return dev;

        tangPointerAttributes attr = {};
        int saved_dev = -1;
        tangGetDevice(&saved_dev);
        tangError_t ret = QueryPointerAttrsBestEffort(ptr, &attr, -1);
        if (saved_dev >= 0) tangSetDevice(saved_dev);
        if (ret != tangSuccess) return -1;

        NormalizeMappedHostPointer(&ptr, &attr);
        if (attr.type == tangMemoryTypeDevice) return attr.device;
        return -1;
    };

    for (auto& request : request_list) {
        shm_batch->task_list.push_back(SunriseLinkTask{});
        auto& task = shm_batch->task_list[shm_batch->task_list.size() - 1];

        uint64_t target_addr = request.target_offset;
        int target_gpu_id = -1;
        int local_gpu_id = infer_local_dev_best_effort(request.source);
        if (local_gpu_id >= 0) {
            tangError_t sd = tangSetDevice(local_gpu_id);
            if (sd != tangSuccess) {
                return Status::InternalError(
                    "Failed to set local Sunrise device before transfer");
            }
        }
        if (request.target_id != LOCAL_SEGMENT_ID) {
            auto status = relocateRemoteAddress(
                target_addr, request.length, request.target_id, &target_gpu_id);
            if (!status.ok()) return status;
        }
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;
        task.is_tang_ipc = (request.target_id != LOCAL_SEGMENT_ID);
        task.tang_gpu_id = target_gpu_id;

        auto status = startTransfer(&task, shm_batch);
        if (!status.ok()) return status;
    }

    return Status::OK();
}

Status SunriseLinkTransport::startTransfer(SunriseLinkTask* task,
                                           SunriseLinkSubBatch* batch) {
    void* src = nullptr;
    void* dst = nullptr;

    const uint64_t resolved_target = task->target_addr;

    if (task->request.opcode == Request::READ) {
        dst = task->request.source;
        src = reinterpret_cast<void*>(resolved_target);
    } else {
        src = task->request.source;
        dst = reinterpret_cast<void*>(resolved_target);
    }

    const size_t len = task->request.length;
    tangStream_t stream = nullptr;
    int current_dev = -1;
    tangGetDevice(&current_dev);
    auto infer_local_dev = [&](void* ptr) -> int {
        uint64_t p = reinterpret_cast<uint64_t>(ptr);
        std::lock_guard<std::mutex> guard(registered_memory_mutex_);
        for (const auto& entry : registered_memory_) {
            uint64_t base = reinterpret_cast<uint64_t>(entry.first);
            if (base <= p && p < base + entry.second) {
                auto it = registered_memory_gpu_id_.find(entry.first);
                if (it != registered_memory_gpu_id_.end()) return it->second;
            }
        }
        return -1;
    };
    auto infer_local_dev_best_effort = [&](void* ptr) -> int {
        int dev = infer_local_dev(ptr);
        if (dev >= 0) return dev;

        tangPointerAttributes attr = {};
        int saved_dev = -1;
        tangGetDevice(&saved_dev);
        tangError_t ret = QueryPointerAttrsBestEffort(ptr, &attr, -1);
        if (saved_dev >= 0) tangSetDevice(saved_dev);
        if (ret != tangSuccess) return -1;

        NormalizeMappedHostPointer(&ptr, &attr);
        if (attr.type == tangMemoryTypeDevice) return attr.device;
        return -1;
    };

    // Follow ipcC2cTest style: for IPC path, prefer explicit gpu ids and
    // tangMemcpyPeer directly, instead of relying on pointer attributes.
    if (task->is_tang_ipc && task->tang_gpu_id >= 0) {
        int remote_dev = task->tang_gpu_id;
        int local_dev = -1;
        if (task->request.opcode == Request::WRITE) {
            local_dev = infer_local_dev_best_effort(src);
        } else {
            local_dev = infer_local_dev_best_effort(dst);
        }
        if (local_dev < 0) local_dev = current_dev;

        int src_dev =
            (task->request.opcode == Request::WRITE) ? local_dev : remote_dev;
        int dst_dev =
            (task->request.opcode == Request::WRITE) ? remote_dev : local_dev;

        // Same GPU id on initiator and target (typical single-card P2P bench):
        // pointers are IPC handles into the same device context; tangMemcpyPeer
        // is not valid when src_dev == dst_dev.
        if (src_dev == dst_dev) {
            ScopedTangDeviceOps ctx_lock(src_dev, dst_dev);
            tangSetDevice(src_dev);
            tangError_t ret =
                tangMemcpy(dst, src, len, tangMemcpyDeviceToDevice);
            if (ret != tangSuccess) {
                LOG(ERROR)
                    << "SunriseLink IPC same-device memcpy failed, tang ret="
                    << ret << " " << tangGetErrorString(ret)
                    << " dev=" << src_dev;
                task->status_word = TransferStatusEnum::FAILED;
                return Status::InternalError("SunriseLink transfer failed");
            }
            task->transferred_bytes = len;
            task->status_word = TransferStatusEnum::COMPLETED;
            return Status::OK();
        }

        tangError_t ret = tangSuccess;
        // Prefer ipcC2cTest ordering first (current=dst_dev), then retry with
        // current=src_dev since some Tang stacks are context-sensitive for peer
        // copy direction.
        auto try_peer_copy = [&]() -> tangError_t {
            tangError_t r = tangMemcpyPeer(dst, dst_dev, src, src_dev, len);
            if (r != tangSuccess) {
                r = tangMemcpyPeer_v2(dst, dst_dev, src, src_dev, len);
            }
            return r;
        };
        // ipcC2cTest uses synchronous tangMemcpyPeer only.
        ret = try_peer_copy();
        if (ret != tangSuccess) {
            int p_sd = -1;
            int p_ds = -1;
            auto it_sd =
                c2c_port_by_pair_.find(std::make_pair(src_dev, dst_dev));
            if (it_sd != c2c_port_by_pair_.end()) p_sd = it_sd->second;
            auto it_ds =
                c2c_port_by_pair_.find(std::make_pair(dst_dev, src_dev));
            if (it_ds != c2c_port_by_pair_.end()) p_ds = it_ds->second;
            ret = C2cMemcpyWithPeerMap(src_dev, dst_dev, src, dst, len, p_sd,
                                       p_ds);
        }
        if (ret != tangSuccess) {
            LOG(ERROR) << "SunriseLink IPC peer copy failed, tang ret=" << ret
                       << " " << tangGetErrorString(ret)
                       << " src_dev=" << src_dev << " dst_dev=" << dst_dev
                       << " remote_dev=" << remote_dev
                       << " local_dev=" << local_dev;
            task->status_word = TransferStatusEnum::FAILED;
            return Status::InternalError("SunriseLink transfer failed");
        }
        task->transferred_bytes = len;
        task->status_word = TransferStatusEnum::COMPLETED;
        return Status::OK();
    }

    tangPointerAttributes sa = {};
    tangPointerAttributes da = {};
    if (task->tang_gpu_id >= 0) tangSetDevice(task->tang_gpu_id);

    // Intentionally no mutex here: a global lock on pointer-attribute queries
    // serialized all peer copies across threads and capped throughput at a few
    // MiB/s in multi-threaded benches. Tang is expected to tolerate concurrent
    // attribute reads for different pointers.
    tangError_t src_attr_ret =
        QueryPointerAttrsBestEffort(src, &sa, task->tang_gpu_id);
    tangError_t dst_attr_ret =
        QueryPointerAttrsBestEffort(dst, &da, task->tang_gpu_id);
    NormalizeMappedHostPointer(&src, &sa);
    NormalizeMappedHostPointer(&dst, &da);

    int src_dev = (sa.type == tangMemoryTypeDevice) ? sa.device : -1;
    int dst_dev = (da.type == tangMemoryTypeDevice) ? da.device : -1;
    int remote_dev_hint = task->tang_gpu_id;
    if (task->request.opcode == Request::WRITE) {
        if (dst_dev < 0 && remote_dev_hint >= 0) dst_dev = remote_dev_hint;
        if (src_dev < 0 && current_dev >= 0) src_dev = current_dev;
    } else {
        if (src_dev < 0 && remote_dev_hint >= 0) src_dev = remote_dev_hint;
        if (dst_dev < 0 && current_dev >= 0) dst_dev = current_dev;
    }
    if (task->request.opcode == Request::WRITE && src_dev < 0) {
        int hinted = infer_local_dev(src);
        if (hinted >= 0) src_dev = hinted;
    } else if (task->request.opcode == Request::READ && dst_dev < 0) {
        int hinted = infer_local_dev(dst);
        if (hinted >= 0) dst_dev = hinted;
    }

    tangError_t ret = tangSuccess;
    const int stream_dev = (dst_dev >= 0) ? dst_dev : src_dev;
    if (stream_dev >= 0) {
        stream = tl_stream_pool.getOrCreate(stream_dev);
    }
    const bool can_async =
        stream && async_memcpy_threshold_ > 0 && len >= async_memcpy_threshold_;
    batch->stream = reinterpret_cast<void*>(stream);

    if (task->is_tang_ipc && src_dev >= 0 && dst_dev >= 0 &&
        src_dev == dst_dev) {
        ScopedTangDeviceOps ctx_lock(src_dev, dst_dev);
        tangSetDevice(src_dev);
        if (can_async) {
            ret = tangMemcpyAsync(dst, src, len, tangMemcpyDeviceToDevice,
                                  stream);
            if (ret == tangSuccess) {
                return Status::OK();
            }
        }
        ret = tangMemcpy(dst, src, len, tangMemcpyDeviceToDevice);
        if (ret != tangSuccess) {
            LOG(ERROR)
                << "SunriseLink IPC local-device memcpy failed, tang ret="
                << ret << " " << tangGetErrorString(ret) << " dev=" << src_dev;
            task->status_word = TransferStatusEnum::FAILED;
            return Status::InternalError("SunriseLink transfer failed");
        }
        task->transferred_bytes = len;
        task->status_word = TransferStatusEnum::COMPLETED;
        return Status::OK();
    }

    if (src_dev >= 0 && dst_dev >= 0 &&
        (src_dev != dst_dev || task->is_tang_ipc)) {
        auto collect_ports = [&](int local, int peer) {
            std::vector<int> ports;
            auto it = topology_map_.find(local);
            if (it == topology_map_.end()) return ports;
            for (const auto& p : it->second) {
                if (p.peer_device_id == peer && p.is_active) {
                    ports.push_back(p.peer_port);
                }
            }
            std::sort(ports.begin(), ports.end());
            ports.erase(std::unique(ports.begin(), ports.end()), ports.end());
            return ports;
        };

        auto ports_sd = collect_ports(src_dev, dst_dev);
        auto ports_ds = collect_ports(dst_dev, src_dev);
        if (ports_sd.empty()) {
            auto it = c2c_port_by_pair_.find(std::make_pair(src_dev, dst_dev));
            if (it != c2c_port_by_pair_.end()) ports_sd.push_back(it->second);
        }
        if (ports_ds.empty()) {
            auto it = c2c_port_by_pair_.find(std::make_pair(dst_dev, src_dev));
            if (it != c2c_port_by_pair_.end()) ports_ds.push_back(it->second);
        }
        constexpr size_t kMultiPortThreshold = 4ull << 20;  // 4 MiB
        const bool enable_port_striping =
            conf_ ? conf_->get("transports/sunrise_link/enable_port_striping",
                               false)
                  : false;
        if (enable_port_striping && !ports_sd.empty() && ports_sd.size() > 1 &&
            len >= kMultiPortThreshold) {
            size_t lanes = ports_sd.size();
            size_t offset = 0;
            const size_t base = len / lanes;
            const size_t rem = len % lanes;
            for (size_t lane = 0; lane < lanes; ++lane) {
                size_t piece = base + (lane < rem ? 1 : 0);
                size_t lane_offset = offset;
                offset += piece;
                int p_sd = ports_sd[lane];
                ret = C2cMemcpyWithPeerMap(
                    src_dev, dst_dev, static_cast<char*>(src) + lane_offset,
                    static_cast<char*>(dst) + lane_offset, piece, p_sd, -1);
                if (ret != tangSuccess) {
                    break;
                }
            }
            if (ret == tangSuccess) {
                task->transferred_bytes = len;
                task->status_word = TransferStatusEnum::COMPLETED;
                return Status::OK();
            }
        }

        auto peer_sync = [&]() -> tangError_t {
            tangError_t r = tangMemcpyPeer(dst, dst_dev, src, src_dev, len);
            if (r != tangSuccess) {
                r = tangMemcpyPeer_v2(dst, dst_dev, src, src_dev, len);
            }
            if (r != tangSuccess) {
                int p_sd = !ports_sd.empty() ? ports_sd.front() : -1;
                int p_ds = !ports_ds.empty() ? ports_ds.front() : -1;
                r = C2cMemcpyWithPeerMap(src_dev, dst_dev, src, dst, len, p_sd,
                                         p_ds);
            }
            return r;
        };

        // Synchronous tangMemcpyPeer only (see can_async above).
        (void)can_async;
        ret = peer_sync();
    } else if (src_dev >= 0 && dst_dev >= 0 && src_dev == dst_dev) {
        ScopedTangDeviceOps ctx_lock(src_dev, dst_dev);
        tangSetDevice(src_dev);
        if (can_async) {
            ret = tangMemcpyAsync(dst, src, len, tangMemcpyDeviceToDevice,
                                  stream);
            if (ret == tangSuccess) {
                return Status::OK();
            }
        }
        ret = tangMemcpy(dst, src, len, tangMemcpyDeviceToDevice);
    } else {
        ScopedTangDeviceOps ctx_lock(src_dev, dst_dev);
        if (src_dev >= 0) {
            tangSetDevice(src_dev);
        } else if (dst_dev >= 0) {
            tangSetDevice(dst_dev);
        }
        tangMemcpyKind kind = MemcpyKindForPointers(sa.type, da.type);
        if (can_async) {
            ret = tangMemcpyAsync(dst, src, len, kind, stream);
            if (ret == tangSuccess) {
                return Status::OK();
            }
        }
        ret = tangMemcpy(dst, src, len, kind);
    }

    if (ret != tangSuccess) {
        LOG(ERROR) << "SunriseLink transfer failed, tang ret=" << ret << " "
                   << tangGetErrorString(ret) << " src_dev=" << src_dev
                   << " dst_dev=" << dst_dev << " src_attr_ret=" << src_attr_ret
                   << " dst_attr_ret=" << dst_attr_ret
                   << " task.tang_gpu_id=" << task->tang_gpu_id;
        task->status_word = TransferStatusEnum::FAILED;
        return Status::InternalError("SunriseLink transfer failed");
    }

    task->transferred_bytes = len;
    task->status_word = TransferStatusEnum::COMPLETED;
    return Status::OK();
}

Status SunriseLinkTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                               TransferStatus& status) {
    auto shm_batch = dynamic_cast<SunriseLinkSubBatch*>(batch);
    if (!shm_batch) {
        return Status::InvalidArgument(
            "Invalid SunriseLink sub-batch" LOC_MARK);
    }
    if (task_id < 0 || task_id >= (int)shm_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto& task = shm_batch->task_list[task_id];
    if (task.status_word == TransferStatusEnum::PENDING) {
        tangStream_t stream = reinterpret_cast<tangStream_t>(shm_batch->stream);
        if (stream) {
            tangError_t err = tangStreamQuery(stream);
            if (err == tangSuccess) {
                tangStreamSynchronize(stream);
                task.transferred_bytes = task.request.length;
                task.status_word = TransferStatusEnum::COMPLETED;
            } else if (err != tangErrorNotReady) {
                task.status_word = TransferStatusEnum::FAILED;
            }
        } else {
            LOG(ERROR)
                << "SunriseLink getTransferStatus: PENDING with null stream; "
                   "cannot complete async bookkeeping";
            task.status_word = TransferStatusEnum::FAILED;
        }
    }
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status SunriseLinkTransport::addMemoryBuffer(BufferDesc& desc,
                                             const MemoryOptions& options) {
    LocationParser location(desc.location);
    auto status =
        registerMemory(reinterpret_cast<void*>(desc.addr), desc.length);
    if (!status.ok()) return status;
    if (location.type() == "cuda" || location.type() == "npu") {
        {
            std::lock_guard<std::mutex> guard(registered_memory_mutex_);
            registered_memory_gpu_id_[reinterpret_cast<void*>(desc.addr)] =
                location.index();
        }
        int saved_dev = -1;
        tangGetDevice(&saved_dev);
        if (location.index() >= 0) {
            tangError_t sd = tangSetDevice(location.index());
            if (sd != tangSuccess) {
                LOG(ERROR)
                    << "tangSetDevice before tangIpcGetMemHandle failed: " << sd
                    << " " << tangGetErrorString(sd);
                return Status::InternalError("tangSetDevice failed");
            }
        }
        tangIpcMemHandle_t handle;
        auto ret =
            tangIpcGetMemHandle(&handle, reinterpret_cast<void*>(desc.addr));
        if (saved_dev >= 0) {
            tangSetDevice(saved_dev);
        }
        if (ret != tangSuccess) {
            LOG(ERROR) << "tangIpcGetMemHandle failed: " << ret << " "
                       << tangGetErrorString(ret);
            return Status::InternalError("tangIpcGetMemHandle failed");
        }
        desc.shm_path =
            serializeBinaryData(&handle, sizeof(tangIpcMemHandle_t));
    } else if (location.type() == "cpu" ||
               location.type() == kWildcardLocation) {
        std::lock_guard<std::mutex> guard(registered_memory_mutex_);
        registered_memory_gpu_id_[reinterpret_cast<void*>(desc.addr)] = -1;
    }
    desc.transports.push_back(TransportType::SUNRISE_LINK);
    return Status::OK();
}

Status SunriseLinkTransport::registerMemory(void* addr, size_t size) {
    std::lock_guard<std::mutex> guard(registered_memory_mutex_);
    registered_memory_[addr] = size;
    return Status::OK();
}

Status SunriseLinkTransport::deregisterMemory(void* addr) {
    std::lock_guard<std::mutex> guard(registered_memory_mutex_);
    auto it = registered_memory_.find(addr);
    if (it != registered_memory_.end()) {
        registered_memory_.erase(it);
        registered_memory_gpu_id_.erase(addr);
    }
    return Status::OK();
}

Status SunriseLinkTransport::removeMemoryBuffer(BufferDesc& desc) {
    desc.shm_path.clear();
    return deregisterMemory(reinterpret_cast<void*>(desc.addr));
}

Status SunriseLinkTransport::relocateRemoteAddress(uint64_t& dest_addr,
                                                   uint64_t length,
                                                   uint64_t target_id,
                                                   int* target_gpu_id) {
    thread_local IpcRelocateMap tl_relocate_map;
    if (tl_relocate_map.empty()) {
        RWSpinlock::ReadGuard guard(relocate_lock_);
        tl_relocate_map = relocate_map_;
    }

    for (auto& entry : tl_relocate_map[target_id]) {
        if (entry.first <= dest_addr &&
            dest_addr + length <= entry.first + entry.second.length) {
            dest_addr = dest_addr - entry.first +
                        reinterpret_cast<uint64_t>(entry.second.dev_ptr);
            if (target_gpu_id) *target_gpu_id = entry.second.gpu_id;
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);
    SegmentDesc* desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;

    auto buffer = desc->findBuffer(dest_addr, length);
    if (!buffer || buffer->shm_path.empty()) {
        // Cache may be stale right after remote registration; refresh once.
        metadata_->segmentManager().invalidateRemote(target_id);
        status = metadata_->segmentManager().getRemoteCached(desc, target_id);
        if (!status.ok()) return status;
        buffer = desc->findBuffer(dest_addr, length);
    }
    if (!buffer || buffer->shm_path.empty()) {
        return Status::InvalidArgument(
            "Requested address is not in registered Sunrise buffer" LOC_MARK);
    }

    if (!relocate_map_[target_id].count(buffer->addr)) {
        LocationParser location(buffer->location);
        if (location.type() != "cuda" && location.type() != "npu") {
            return Status::InvalidArgument(
                "Requested address is not in registered CUDA/NPU "
                "buffer" LOC_MARK);
        }
        std::vector<unsigned char> decoded;
        deserializeBinaryData(buffer->shm_path, decoded);
        if (decoded.size() != sizeof(tangIpcMemHandle_t)) {
            return Status::InvalidArgument(
                "Invalid Sunrise IPC handle size" LOC_MARK);
        }
        tangIpcMemHandle_t handle;
        memcpy(&handle, decoded.data(), sizeof(handle));

        void* opened_ptr = nullptr;
        int saved_dev = -1;
        tangGetDevice(&saved_dev);
        tangError_t sd = tangSetDevice(location.index());
        if (sd != tangSuccess) {
            LOG(ERROR) << "tangSetDevice before tangIpcOpenMemHandle failed: "
                       << sd << " " << tangGetErrorString(sd)
                       << ", target_gpu=" << location.index()
                       << ", saved_dev=" << saved_dev;
            return Status::InternalError("tangSetDevice failed");
        }
        tangError_t ret = tangIpcOpenMemHandle(&opened_ptr, handle,
                                               tangIpcMemLazyEnablePeerAccess);
        if (saved_dev >= 0) {
            tangSetDevice(saved_dev);
        }
        if (ret != tangSuccess) {
            LOG(ERROR) << "tangIpcOpenMemHandle failed: " << ret << " "
                       << tangGetErrorString(ret);
            return Status::InternalError("tangIpcOpenMemHandle failed");
        }

        OpenedIpcEntry ipc_entry;
        ipc_entry.dev_ptr = opened_ptr;
        ipc_entry.length = buffer->length;
        ipc_entry.gpu_id = location.index();
        relocate_map_[target_id][buffer->addr] = ipc_entry;
        tl_relocate_map = relocate_map_;
    }

    auto opened_ptr = relocate_map_[target_id][buffer->addr].dev_ptr;
    if (target_gpu_id) {
        *target_gpu_id = relocate_map_[target_id][buffer->addr].gpu_id;
    }
    dest_addr =
        dest_addr - buffer->addr + reinterpret_cast<uint64_t>(opened_ptr);
    return Status::OK();
}

Status SunriseLinkTransport::uninstall() {
    if (installed_) {
        std::vector<void*> reg_addrs;
        {
            std::lock_guard<std::mutex> guard(registered_memory_mutex_);
            reg_addrs.reserve(registered_memory_.size());
            for (auto& entry : registered_memory_) {
                reg_addrs.push_back(entry.first);
            }
        }
        for (void* addr : reg_addrs) {
            auto status = deregisterMemory(addr);
            if (!status.ok()) return status;
        }
        {
            std::lock_guard<std::mutex> guard(registered_memory_mutex_);
            registered_memory_.clear();
            registered_memory_gpu_id_.clear();
        }

        if (runtime_lib_handle_) {
            dlclose(runtime_lib_handle_);
            runtime_lib_handle_ = nullptr;
        }

        for (auto& seg_entry : relocate_map_) {
            for (auto& entry : seg_entry.second) {
                if (entry.second.dev_ptr) {
                    tangIpcCloseMemHandle(entry.second.dev_ptr);
                }
            }
        }
        relocate_map_.clear();

        if (ptml_handle_) {
            dlclose(ptml_handle_);
            ptml_handle_ = nullptr;
        }

        metadata_.reset();
        installed_ = false;
        LOG(INFO) << "SunriseLink transport uninstalled";
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
