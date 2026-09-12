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

#include <dlfcn.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/time.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <set>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "common/base/status.h"
#include "transfer_engine.h"
#include "transport/transport.h"

#ifdef USE_EFA
// For neuronProbeAddress(), which answers which /dev/neuron<N> a pointer
// belongs to -- the same lookup the transport uses to name a buffer's location.
#include "transport/efa_transport/efa_neuron.h"
#endif

#ifdef USE_TENT
#include "tent/transfer_engine.h"
#include "tent/common/config.h"
#endif

#include "cuda_alike.h"
#ifdef USE_CUDA
#ifdef USE_NVMEOF
#include <cufile.h>
#endif
#endif

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_UBSHMEM) || defined(USE_SUPA) || defined(USE_SUNRISE)
#include <cassert>

#if defined(USE_MNNVL) || defined(USE_MUSA) || defined(USE_UBSHMEM)
#include "gpu_vendor/mnnvl.h"
#endif

#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif

#if defined(USE_UBSHMEM)
static void checkAclError(aclError result, const char* message) {
    if (result != ACL_ERROR_NONE) {
        const char* errMsg = aclGetRecentErrMsg();
        LOG(ERROR) << message << " (Error code: " << result << " - " << errMsg
                   << ")";
        exit(EXIT_FAILURE);
    }
}
#else
static void checkCudaError(cudaError_t result, const char* message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        exit(EXIT_FAILURE);
    }
}
#endif
#endif

const static int NR_SOCKETS =
    numa_available() == 0 ? numa_num_configured_nodes() : 1;

static int buffer_num = NR_SOCKETS;

DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "192.168.3.77:2379", "etcd server host address");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");
DEFINE_string(operation, "read", "Operation type: read or write");

DEFINE_string(protocol, "rdma",
              "Transfer protocol: "
              "rdma|barex|tcp|efa|nvlink|musa|nvlink_intra|hip|sunrise_link");

DEFINE_string(device_name, "mlx5_2",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(nic_priority_matrix, "",
              "Path to RDMA NIC priority matrix file (Advanced)");

DEFINE_string(segment_id, "192.168.3.76", "Segment ID to access data");
DEFINE_uint64(buffer_size, 1ull << 30, "total size of data buffer");
DEFINE_int32(batch_size, 128, "Batch size");
DEFINE_uint64(block_size, 65536, "Block size for each transfer request");
DEFINE_int32(duration, 10, "Test duration in seconds");
DEFINE_int32(threads, 12, "Task submission threads");
DEFINE_bool(auto_discovery, false, "Enable auto discovery");
DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");
DEFINE_string(backend, "classic", "Backend to use: classic|tent");

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_UBSHMEM) || defined(USE_SUPA) || defined(USE_SUNRISE)
DEFINE_bool(use_vram, true, "Allocate memory from GPU/NPU VRAM");
DEFINE_bool(init_mem, true, "Initialize allocated memory");
DEFINE_int32(gpu_id, 0,
             "GPU/NPU ID to use, -1 for all GPUs, not supported for NPUs");
#endif

#ifdef USE_EFA
DEFINE_int32(neuron_device, -1,
             "Allocate buffers from AWS Neuron (Trainium/Inferentia) HBM on "
             "this logical NeuronCore instead of DRAM; -1 disables. Valid if "
             "protocol=efa");
DEFINE_int32(
    neuron_device_count, 1,
    "Number of Neuron devices to spread buffers over, starting at the "
    "one holding --neuron_device. One buffer per device, since only two "
    "EFA NICs sit on any one device's PCIe switch and a single device "
    "therefore cannot use the whole fabric");
DEFINE_int32(
    neuron_fill_byte, 0xCC,
    "Byte to prefill Neuron HBM with. Give the two sides different "
    "values and --mode=target reports, on shutdown, how much of its "
    "HBM the peer actually overwrote -- the only end-to-end check that "
    "the bytes landed in device memory, since the host cannot memcmp() "
    "it");
DEFINE_int32(neuron_core_stride, 0,
             "Logical NeuronCores to step between the buffers of "
             "--neuron_device_count. 0 (the default) does not step a fixed "
             "amount at all: it finds one logical core per physical device by "
             "probing, which needs no assumption about core fusion. Set it "
             "only to reproduce a particular placement. Either way the "
             "\"Registering Neuron HBM ... as neuron:N\" lines say where the "
             "buffers landed");
#endif

using namespace mooncake;

#ifdef USE_EFA
// Neuron HBM allocation for the benchmark.
//
// libnrt is reached through dlopen() rather than linked, so a stock build of
// this benchmark still starts on hosts with no Neuron SDK -- the same reason
// the transport itself takes no build flag for Neuron (see efa_neuron.h).  Note
// that no location string is derived from FLAGS_neuron_device here: that is a
// *logical* NeuronCore index, which NEURON_RT_VISIBLE_CORES renumbers and which
// several cores share per physical device.  Naming is left to the transport,
// which reads the real device ordinal back out of the pointer.
namespace {
struct nrt_tensor;

struct NeuronRuntime {
    int (*init)(int, const char*, const char*) = nullptr;
    int (*tensor_allocate)(int, int, size_t, const char*,
                           struct nrt_tensor**) = nullptr;
    void* (*tensor_get_va)(struct nrt_tensor*) = nullptr;
    void (*tensor_free)(struct nrt_tensor**) = nullptr;
    int (*tensor_write)(struct nrt_tensor*, const void*, size_t,
                        size_t) = nullptr;
    int (*tensor_read)(struct nrt_tensor*, void*, size_t, size_t) = nullptr;
};

// Tensor handles are kept keyed by VA because allocateMemoryPool() hands its
// caller a bare pointer and freeMemoryPool() only ever gets that pointer back.
std::unordered_map<void*, struct nrt_tensor*> neuron_tensors;

const NeuronRuntime* neuronRuntime() {
    static NeuronRuntime rt;
    static bool ok = [&]() -> bool {
        // Prefer an already-loaded libnrt over a second independent copy of the
        // device runtime in one process.
        void* handle = dlopen("libnrt.so.1", RTLD_LAZY | RTLD_NOLOAD);
        if (!handle) handle = dlopen("libnrt.so.1", RTLD_LAZY);
        if (!handle)
            handle = dlopen("/opt/aws/neuron/lib/libnrt.so.1", RTLD_LAZY);
        if (!handle) {
            // dlerror() may return NULL; streaming a NULL const char* is UB.
            const char* err = dlerror();
            LOG(ERROR) << "--neuron_device requires libnrt: "
                       << (err ? err : "no dlerror() message");
            return false;
        }
        rt.init = (decltype(rt.init))dlsym(handle, "nrt_init");
        rt.tensor_allocate =
            (decltype(rt.tensor_allocate))dlsym(handle, "nrt_tensor_allocate");
        rt.tensor_get_va =
            (decltype(rt.tensor_get_va))dlsym(handle, "nrt_tensor_get_va");
        rt.tensor_free =
            (decltype(rt.tensor_free))dlsym(handle, "nrt_tensor_free");
        rt.tensor_write =
            (decltype(rt.tensor_write))dlsym(handle, "nrt_tensor_write");
        rt.tensor_read =
            (decltype(rt.tensor_read))dlsym(handle, "nrt_tensor_read");
        if (!rt.init || !rt.tensor_allocate || !rt.tensor_get_va ||
            !rt.tensor_free) {
            LOG(ERROR) << "libnrt is missing the tensor API";
            return false;
        }
        // NRT_FRAMEWORK_TYPE_NO_FW.  Idempotent, so calling it here is safe
        // even when a framework in the same process has already initialised the
        // runtime.
        int rc = rt.init(1, "", "");
        if (rc != 0) {
            LOG(ERROR) << "nrt_init failed with NRT_STATUS " << rc
                       << "; check NEURON_RT_VISIBLE_CORES (the range must be "
                          "contiguous) and that no other process owns the core";
            return false;
        }
        return true;
    }();
    return ok ? &rt : nullptr;
}

// The logical core index of the first core of each distinct physical device,
// starting the scan at --neuron_device, discovered by allocating a page on each
// core in turn and asking the transport which device the pointer came from.
//
// This exists because there is no arithmetic that gets it right on every
// instance type.  Stepping by a fixed number of cores requires knowing the
// core-fusion factor (LNC), which is a runtime setting with no clean query --
// nrt_get_visible_vnc_count() segfaults when called with the signature its
// symbol suggests -- and guessing it wrong is silent: on trn1.32xlarge, where
// Trainium1 does not fuse cores at all, a step derived from trn2's LNC=2 puts
// two buffers on each of half the devices, leaves the other half idle, and only
// shows up as ~10% less throughput.  Asking where a pointer actually landed
// needs no such guess and cannot be silently wrong.
const std::vector<int>& neuronFirstCorePerDevice() {
    // Bounded by the largest instance today (trn2.48xlarge: 16 devices x 4
    // logical cores), with room to spare; the scan also stops at the first core
    // the runtime refuses, which is the usual way out.
    constexpr int kMaxCoreScan = 128;
    static const std::vector<int> cores = []() {
        std::vector<int> cores;
        const NeuronRuntime* rt = neuronRuntime();
        if (!rt) return cores;

        // Every probe is held until the scan is over, and only then freed.  The
        // runtime hands the *same* virtual address straight back after a free,
        // so freeing as we went made every core look like the one before it and
        // the whole scan reported a single device.
        std::vector<struct nrt_tensor*> probes;
        std::set<int> seen_devices;
        const int first = std::max(0, FLAGS_neuron_device);
        for (int core = first; core < first + kMaxCoreScan; ++core) {
            struct nrt_tensor* tensor = nullptr;
            // A page is enough: the probe reads the device ordinal out of the
            // /dev/neuron<N> mapping the allocation sits in, not out of its
            // size.  Anything the runtime refuses ends the scan -- that is how
            // the last visible core is found (NRT_STATUS 2, invalid, on the
            // first core past the visible range).
            int rc =
                rt->tensor_allocate(0, core, 4096, "mooncake_probe", &tensor);
            if (rc != 0) {
                VLOG(1) << "Neuron: logical core " << core
                        << " refused a probe allocation (NRT_STATUS " << rc
                        << "), ending the scan";
                break;
            }
            probes.push_back(tensor);
            void* va = rt->tensor_get_va(tensor);
            int device_index = -1;
            const bool found =
                va && neuronProbeAddress(va, 4096, &device_index);
            VLOG(1) << "Neuron: logical core " << core << " at " << va
                    << " belongs to device "
                    << (found ? std::to_string(device_index)
                              : std::string("(unknown)"));
            if (found && seen_devices.insert(device_index).second) {
                cores.push_back(core);
            }
        }
        for (auto& tensor : probes) rt->tensor_free(&tensor);
        if (!cores.empty()) {
            LOG(INFO) << "Neuron: " << cores.size()
                      << " distinct device(s) found among logical cores "
                      << first << ".." << (first + kMaxCoreScan - 1)
                      << ", one buffer will go on each";
        }
        return cores;
    }();
    return cores;
}

// How far to step, in nrt_tensor_allocate() core indices, to land on the next
// physical device.  The fallback for when the probe above comes up short, and
// what --neuron_core_stride overrides.
int neuronCoresPerDevice() {
    static const int stride = []() -> int {
        if (FLAGS_neuron_core_stride > 0) return FLAGS_neuron_core_stride;

        int physical_cores = 0;
        std::ifstream f(
            "/sys/devices/virtual/neuron_device/neuron0/core_count");
        if (!(f >> physical_cores) || physical_cores <= 0) {
            LOG(WARNING)
                << "Cannot read Neuron core_count from sysfs, stepping "
                   "one core per buffer; pass --neuron_core_stride if "
                   "the buffers land on the same device";
            return 1;
        }
        // LNC=2 is the trn2 default and the only fusion factor shipped so far.
        const int stride = physical_cores >= 2 ? physical_cores / 2 : 1;
        LOG(INFO) << "Neuron: " << physical_cores
                  << " physical cores per device, stepping " << stride
                  << " logical core(s) per buffer";
        return stride;
    }();
    return stride;
}

void* allocateNeuronMemory(size_t size, int buffer_id) {
    const NeuronRuntime* rt = neuronRuntime();
    if (!rt) return nullptr;

    // One buffer per device.  The probed core list is used only when it covers
    // every buffer asked for: a probe that came up short (an old libfabric, so
    // neuronProbeAddress() answers false for everything, or mappings the
    // runtime had not published yet) would otherwise quietly stack the
    // remaining buffers onto devices already in use, which is the failure this
    // replaced.  Falling back to the arithmetic keeps such a host exactly where
    // it was.
    const auto& probed = neuronFirstCorePerDevice();
    const bool use_probed = FLAGS_neuron_core_stride <= 0 && !probed.empty() &&
                            (int)probed.size() >= FLAGS_neuron_device_count;
    if (FLAGS_neuron_core_stride <= 0 && !use_probed && buffer_id == 0) {
        LOG(WARNING) << "Neuron: probed " << probed.size() << " device(s) for "
                     << FLAGS_neuron_device_count
                     << " buffer(s), falling back to a fixed core step of "
                     << neuronCoresPerDevice()
                     << "; check the \"as neuron:N\" ordinals below and pass "
                        "--neuron_core_stride if they repeat";
    }
    const int core =
        use_probed ? probed[buffer_id % probed.size()]
                   : FLAGS_neuron_device + buffer_id * neuronCoresPerDevice();

    struct nrt_tensor* tensor = nullptr;
    // 0 == NRT_TENSOR_PLACEMENT_DEVICE, i.e. HBM rather than pinned host
    // memory, which is the whole point of the exercise.
    int rc = rt->tensor_allocate(0, core, size, "mooncake_bench", &tensor);
    if (rc != 0) {
        LOG(ERROR) << "nrt_tensor_allocate(" << size << " bytes on NeuronCore "
                   << core << ") failed with NRT_STATUS " << rc;
        return nullptr;
    }
    void* va = rt->tensor_get_va(tensor);
    if (!va) {
        rt->tensor_free(&tensor);
        LOG(ERROR) << "nrt_tensor_get_va returned NULL";
        return nullptr;
    }
    // The buffer is filled through the runtime, never with memset(): host
    // stores into device HBM are exactly what the transport's Neuron path
    // exists to avoid.  Skipped silently if libnrt predates nrt_tensor_write.
    if (rt->tensor_write) {
        std::vector<uint8_t> pattern(std::min<size_t>(size, 1ull << 20),
                                     (uint8_t)FLAGS_neuron_fill_byte);
        for (size_t off = 0; off < size; off += pattern.size()) {
            size_t len = std::min(pattern.size(), size - off);
            rc = rt->tensor_write(tensor, pattern.data(), off, len);
            if (rc != 0) {
                LOG(WARNING) << "nrt_tensor_write at offset " << off
                             << " failed with NRT_STATUS " << rc
                             << ", buffer left uninitialized";
                break;
            }
        }
    }
    LOG(INFO) << "Allocated " << size << " bytes of Neuron HBM at " << va
              << " on logical NeuronCore " << core;
    neuron_tensors[va] = tensor;
    return va;
}

// Reports how much of each Neuron buffer no longer holds the fill byte, i.e.
// how much of it the peer's transfers actually landed in HBM.
//
// This is the only end-to-end data check available on the Neuron path: the
// buffer is device memory, so the validator's memcmp() approach cannot look at
// it, and a completion queue entry only says the NIC believed it wrote
// somewhere.  Reading it back through the runtime is what distinguishes "the
// transport reported success" from "the bytes are in HBM".
void reportNeuronOverwrite(const std::vector<void*>& addr) {
    const NeuronRuntime* rt = neuronRuntime();
    if (!rt || !rt->tensor_read) {
        LOG(WARNING) << "libnrt has no nrt_tensor_read, cannot check what the "
                        "peer wrote into Neuron HBM";
        return;
    }

    const size_t kWindow = 1ull << 20;
    for (size_t i = 0; i < addr.size(); ++i) {
        auto it = neuron_tensors.find(addr[i]);
        if (it == neuron_tensors.end()) continue;

        std::vector<uint8_t> readback(kWindow);
        int rc = rt->tensor_read(it->second, readback.data(), 0, kWindow);
        if (rc != 0) {
            LOG(WARNING) << "nrt_tensor_read on buffer " << i
                         << " failed with NRT_STATUS " << rc;
            continue;
        }
        size_t changed = 0;
        int first_new_value = -1;
        for (size_t off = 0; off < kWindow; ++off) {
            if (readback[off] == (uint8_t)FLAGS_neuron_fill_byte) continue;
            if (first_new_value < 0) first_new_value = readback[off];
            ++changed;
        }
        LOG(INFO) << "Neuron buffer " << i << " at " << addr[i] << ": "
                  << changed << "/" << kWindow
                  << " bytes of the first MiB differ from the fill byte 0x"
                  << std::hex << (FLAGS_neuron_fill_byte & 0xff)
                  << ", first new value 0x"
                  << (first_new_value < 0 ? 0 : first_new_value) << std::dec;
    }
}

void freeNeuronMemory(void* addr) {
    const NeuronRuntime* rt = neuronRuntime();
    auto it = neuron_tensors.find(addr);
    if (!rt || it == neuron_tensors.end()) return;
    rt->tensor_free(&it->second);
    neuron_tensors.erase(it);
}
}  // namespace
#endif  // USE_EFA

static void* allocateMemoryPool(size_t size, int buffer_id,
                                bool from_vram = false) {
#ifdef USE_EFA
    if (FLAGS_neuron_device >= 0) return allocateNeuronMemory(size, buffer_id);
#endif
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_UBSHMEM) || defined(USE_SUPA) || defined(USE_SUNRISE)
    if (from_vram) {
        int gpu_id;
        if (FLAGS_gpu_id == -1) {
            gpu_id = buffer_id;
        } else {
            gpu_id = FLAGS_gpu_id;
        }
        void* d_buf;
#if defined(USE_UBSHMEM)
        LOG(INFO) << "Allocating memory on NPU " << gpu_id;
        checkAclError(aclrtSetDevice(gpu_id), "Failed to set device");
#else
        LOG(INFO) << "Allocating memory on GPU " << gpu_id;
        checkCudaError(cudaSetDevice(gpu_id), "Failed to set device");
#endif
        if (FLAGS_protocol == "nvlink" || FLAGS_protocol == "musa" ||
            FLAGS_protocol == "hip") {
#if defined(USE_MNNVL) || defined(USE_MUSA)
            d_buf = allocateFabricMemory(size);
            LOG(INFO) << "Using GPU fabric/IPC memory allocation";
#else
            LOG(ERROR) << "--protocol=nvlink/musa/hip requires USE_MNNVL=ON or "
                          "USE_MUSA=ON";
            return nullptr;
#endif
        } else if (FLAGS_protocol == "nvlink_intra") {
#ifdef USE_INTRA_NVLINK
            d_buf = allocateFabricMemory_intra(size);
            LOG(INFO) << "Using intra-NVLink memory allocation";
#else
            LOG(ERROR)
                << "--protocol=nvlink_intra requires USE_INTRA_NVLINK=ON";
            return nullptr;
#endif
        } else if (FLAGS_protocol == "ubshmem") {
#ifdef USE_UBSHMEM
            d_buf = allocateFabricMemory(size);
            LOG(INFO) << "Using UBShmem fabric memory allocation";
#else
            LOG(ERROR) << "--protocol=ubshmem requires USE_UBSHMEM=ON";
            return nullptr;
#endif
        } else {
#ifndef USE_UBSHMEM
            checkCudaError(cudaMalloc(&d_buf, size),
                           "Failed to allocate device memory");
#endif
        }
#if defined(USE_UBSHMEM)
        if (FLAGS_init_mem) {
            checkAclError(aclrtMemset(d_buf, size, 0xCC, size),
                          "Failed to initialize device memory");
        }
        return d_buf;
#else
        if (FLAGS_init_mem) {
            checkCudaError(cudaMemset(d_buf, 0xCC, size),
                           "Failed to initialize device memory");
            // Ensure memory initialization is done from CPU standpoint
            checkCudaError(cudaStreamSynchronize(0), "Failed to synchronize");
        }

        return d_buf;
#endif
    }
#endif
    return numa_alloc_onnode(size, buffer_id);
}

static void freeMemoryPool(void* addr, size_t size) {
#ifdef USE_EFA
    if (FLAGS_neuron_device >= 0) {
        freeNeuronMemory(addr);
        return;
    }
#endif
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_UBSHMEM) || defined(USE_SUPA) || defined(USE_SUNRISE)
    if (FLAGS_protocol == "nvlink" || FLAGS_protocol == "musa" ||
        FLAGS_protocol == "hip") {
#if defined(USE_MNNVL) || defined(USE_MUSA)
        if (FLAGS_use_vram) {
            freeFabricMemory(addr);
            return;
        }
#endif  // USE_MNNVL || USE_MUSA
    } else if (FLAGS_protocol == "nvlink_intra") {
#ifdef USE_INTRA_NVLINK
        if (FLAGS_use_vram) {
            freeFabricMemory_intra(addr);
            return;
        }
#endif
    } else if (FLAGS_protocol == "ubshmem") {
#ifdef USE_UBSHMEM
        if (FLAGS_use_vram) {
            freeFabricMemory(addr);
            return;
        }
#endif
    } else {
#ifndef USE_UBSHMEM
        // Check FLAGS_use_vram first to avoid unnecessary CUDA calls in non-GPU
        // environments
        if (!FLAGS_use_vram) {
            // Memory was allocated via numa_alloc_onnode, free it directly
            numa_free(addr, size);
        } else {
            // Memory may be on GPU, check pointer attributes
            // Use graceful error handling like transfer engine core
            // (memory_location.cpp)
            cudaPointerAttributes attributes;
            cudaError_t result = cudaPointerGetAttributes(&attributes, addr);

            if (result != cudaSuccess) {
                // CUDA call failed when FLAGS_use_vram is true - this is an
                // error
                LOG(ERROR) << "cudaPointerGetAttributes failed (Error code: "
                           << result << " - " << cudaGetErrorString(result)
                           << ")";
                numa_free(addr, size);
            } else if (attributes.type == cudaMemoryTypeDevice) {
                cudaFree(addr);
            } else if (attributes.type == cudaMemoryTypeHost ||
                       attributes.type == cudaMemoryTypeUnregistered) {
                numa_free(addr, size);
            } else {
                LOG(ERROR) << "Unknown memory type, " << addr << " "
                           << attributes.type << ", assuming CPU memory";
                numa_free(addr, size);
            }
        }
#endif
    }
#else
    if (FLAGS_protocol == "ub") {
        munmap(addr, size);  // for urma
    }
    numa_free(addr, size);
#endif
}

const static std::unordered_map<std::string, uint64_t> RATE_UNIT_MP = {
    {"GB", 1000ull * 1000ull * 1000ull},
    {"GiB", 1ull << 30},
    {"Gb", 1000ull * 1000ull * 1000ull / 8},
    {"MB", 1000ull * 1000ull},
    {"MiB", 1ull << 20},
    {"Mb", 1000ull * 1000ull / 8},
    {"KB", 1000ull},
    {"KiB", 1ull << 10},
    {"Kb", 1000ull / 8}};

static inline std::string calculateRate(uint64_t data_bytes, double duration) {
    if (std::fabs(duration) < 1e-10) {
        LOG(ERROR) << "Invalid args: duration shouldn't be 0";
        return "";
    }
    if (!RATE_UNIT_MP.count(FLAGS_report_unit)) {
        LOG(WARNING) << "Invalid flag: report_unit only support "
                        "GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb, not support "
                     << FLAGS_report_unit
                     << " . Now use GB(default) as report_unit";
        FLAGS_report_unit = "GB";
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(FLAGS_report_precision)
        << 1.0 * data_bytes / duration / RATE_UNIT_MP.at(FLAGS_report_unit)
        << " " << FLAGS_report_unit << "/s";
    return oss.str();
}

volatile bool running = true;
std::atomic<size_t> total_batch_count(0);

// Ensure each worker thread has a valid GPU context before issuing transfers.
static inline void setWorkerDeviceIfNeeded() {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_SUPA) || defined(USE_SUNRISE)
    if (FLAGS_use_vram && FLAGS_gpu_id >= 0) {
        checkCudaError(cudaSetDevice(FLAGS_gpu_id),
                       "Failed to set device in worker");
    }
#endif
}

// Common helper to determine buffer count based on GPU/NUMA configuration
static int determineBufferCount() {
#ifdef USE_EFA
    if (FLAGS_neuron_device >= 0) {
        LOG(INFO) << "Neuron HBM is used, " << FLAGS_neuron_device_count
                  << " device(s) starting at logical NeuronCore "
                  << FLAGS_neuron_device;
        return std::max(1, FLAGS_neuron_device_count);
    }
#endif
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_SUPA) || defined(USE_SUNRISE)
    if (FLAGS_use_vram) {
        int gpu_num;
        LOG(INFO) << "VRAM is used";
        if (FLAGS_gpu_id == -1 && cudaGetDeviceCount(&gpu_num) == cudaSuccess) {
            LOG(INFO) << "GPU ID is not specified, found " << gpu_num
                      << " GPUs to use";
            return gpu_num;
        } else {
            LOG(INFO) << "GPU ID is specified or failed to get GPU count, use "
                      << FLAGS_gpu_id << " GPU";
            return 1;
        }
    }
#endif
#if defined(USE_UBSHMEM)
    if (FLAGS_use_vram) {
        LOG(INFO) << "VRAM is used";
        LOG(INFO) << "NPU ID is specified, use NPU:" << FLAGS_gpu_id;
        return 1;
    }
#endif
    LOG(INFO) << "DRAM is used, numa node num: " << NR_SOCKETS;
    return NR_SOCKETS;
}

// Common helper to allocate memory buffers
static std::vector<void*> allocateBuffers() {
    buffer_num = determineBufferCount();
    std::vector<void*> addr(buffer_num);
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_UBSHMEM) || defined(USE_SUPA) || defined(USE_SUNRISE)
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, FLAGS_use_vram);
    }
#else
    for (int i = 0; i < buffer_num; ++i) {
        addr[i] = allocateMemoryPool(FLAGS_buffer_size, i, false);
    }
#endif
    return addr;
}

// Common helper to free memory buffers
static void freeBuffers(std::vector<void*>& addr) {
    for (int i = 0; i < buffer_num; ++i) {
        freeMemoryPool(addr[i], FLAGS_buffer_size);
    }
    addr.clear();
}

// Helper to get location name for classic backend
static std::string getLocationName(int buffer_id) {
#ifdef USE_EFA
    // Deliberately unnamed: FLAGS_neuron_device is a logical NeuronCore, not
    // the physical device ordinal the location string needs.  The transport
    // resolves the real one from the pointer.
    if (FLAGS_neuron_device >= 0) return kWildcardLocation;
#endif
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_UBSHMEM) || defined(USE_SUPA) || defined(USE_SUNRISE)
    if (FLAGS_use_vram) {
        int name_suffix = (FLAGS_gpu_id == -1) ? buffer_id : FLAGS_gpu_id;
        return std::string(GPU_PREFIX) + std::to_string(name_suffix);
    }
#endif
    return "cpu:" + std::to_string(buffer_id);
}

Status initiatorWorker(TransferEngine* engine, SegmentID segment_id,
                       int thread_id, void* addr) {
    bindToSocket(thread_id % NR_SOCKETS);
    TransferRequest::OpCode opcode;
    if (FLAGS_operation == "read")
        opcode = TransferRequest::READ;
    else if (FLAGS_operation == "write")
        opcode = TransferRequest::WRITE;
    else {
        LOG(ERROR) << "Unsupported operation: must be 'read' or 'write'";
        exit(EXIT_FAILURE);
    }

    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    if (!segment_desc) {
        LOG(ERROR) << "Unable to get target segment ID, please recheck";
        exit(EXIT_FAILURE);
    }
    uint64_t remote_base =
        (uint64_t)segment_desc->buffers[thread_id % buffer_num].addr;

    size_t batch_count = 0;
    while (running) {
        auto batch_id = engine->allocateBatchID(FLAGS_batch_size);
        Status s;
        std::vector<TransferRequest> requests;
        for (int i = 0; i < FLAGS_batch_size; ++i) {
            TransferRequest entry;
            entry.opcode = opcode;
            entry.length = FLAGS_block_size;
            entry.source = (uint8_t*)(addr) +
                           FLAGS_block_size * (i * FLAGS_threads + thread_id);
            entry.target_id = segment_id;
            entry.target_offset =
                remote_base +
                FLAGS_block_size * (i * FLAGS_threads + thread_id);
            requests.emplace_back(entry);
        }

        s = engine->submitTransfer(batch_id, requests);
        if (!s.ok()) LOG(ERROR) << s.ToString();
        LOG_ASSERT(s.ok());
        for (int task_id = 0; task_id < FLAGS_batch_size; ++task_id) {
            bool completed = false;
            TransferStatus status;
            while (!completed) {
                Status s = engine->getTransferStatus(batch_id, task_id, status);
                LOG_ASSERT(s.ok());
                if (status.s == TransferStatusEnum::COMPLETED)
                    completed = true;
                else if (status.s == TransferStatusEnum::FAILED) {
                    LOG(INFO) << "FAILED";
                    completed = true;
                    exit(EXIT_FAILURE);
                }
            }
        }

        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
        batch_count++;
    }
    LOG(INFO) << "Worker " << thread_id << " stopped!";
    total_batch_count.fetch_add(batch_count);
    return Status::OK();
}

std::string formatDeviceNames(const std::string& device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::string loadNicPriorityMatrix() {
    if (!FLAGS_nic_priority_matrix.empty()) {
        std::ifstream file(FLAGS_nic_priority_matrix);
        // Falling through to the fabricated matrix below on a typo would be
        // silent and wrong: that matrix names --device_name (mlx5_2 by
        // default), which exists on no Trainium host, so the run comes up with
        // no usable NIC instead of with the ones the file asked for.
        if (!file.is_open()) {
            LOG(FATAL) << "Cannot open --nic_priority_matrix="
                       << FLAGS_nic_priority_matrix;
        }
        std::string content((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
        file.close();
        return content;
    }
    // Build JSON Data
    auto device_names = formatDeviceNames(FLAGS_device_name);
    return "{\"cpu:0\": [[" + device_names +
           "], []], "
           " \"cpu:1\": [[" +
           device_names +
           "], []], "
           " \"cuda:0\": [[" +
           device_names +
           "], []], "
           " \"musa:0\": [[" +
           device_names +
           "], []], "
           " \"maca:0\": [[" +
           device_names + "], []]}";
}

// Common helper to install transport based on protocol flag
// Returns the installed transport, or nullptr if auto_discovery is enabled
static Transport* installTransportFromFlags(TransferEngine* engine) {
    if (FLAGS_auto_discovery) {
        return nullptr;
    }

    Transport* xport = nullptr;
    std::string nic_priority_matrix;
    std::unique_ptr<void*, decltype(&free)> args(nullptr, free);

    if (FLAGS_protocol == "rdma" || FLAGS_protocol == "barex") {
        nic_priority_matrix = loadNicPriorityMatrix();
        args.reset(static_cast<void**>(malloc(2 * sizeof(void*))));
        args.get()[0] = const_cast<char*>(nic_priority_matrix.c_str());
        args.get()[1] = nullptr;
        xport = engine->installTransport(FLAGS_protocol.c_str(), args.get());
    } else if (FLAGS_protocol == "ub") {
        engine->getLocalTopology()->discover({FLAGS_device_name});
        xport = engine->installTransport(FLAGS_protocol, nullptr);
    } else if (FLAGS_protocol == "efa") {
        // EFA needs topology discovery to find devices, but auto_discovery
        // would auto-install RDMA transport. Manually discover instead --
        // unless an explicit matrix was given, which is then the only way to
        // pin EFA transfers to a subset of the NICs (discovery always takes
        // all of them).
        if (FLAGS_nic_priority_matrix.empty()) {
            engine->getLocalTopology()->discover({});
        } else {
            int rc = engine->getLocalTopology()->parse(loadNicPriorityMatrix());
            if (rc) {
                LOG(FATAL) << "Cannot parse --nic_priority_matrix="
                           << FLAGS_nic_priority_matrix << ", rc " << rc;
            }
        }
        xport = engine->installTransport("efa", nullptr);
    } else if (FLAGS_protocol == "tcp" || FLAGS_protocol == "nvlink" ||
               FLAGS_protocol == "musa" || FLAGS_protocol == "hip" ||
               FLAGS_protocol == "nvlink_intra" ||
               FLAGS_protocol == "ubshmem" ||
               FLAGS_protocol == "sunrise_link" || FLAGS_protocol == "flagcx") {
        xport = engine->installTransport(FLAGS_protocol.c_str(), nullptr);
    } else {
        LOG(ERROR) << "Unsupported protocol: " << FLAGS_protocol;
    }

    return xport;
}

int initiator() {
    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    if (!FLAGS_auto_discovery) {
        Transport* xport = installTransportFromFlags(engine.get());
        LOG_ASSERT(xport);
    }

    auto addr = allocateBuffers();
    for (int i = 0; i < buffer_num; ++i) {
        int rc = engine->registerLocalMemory(addr[i], FLAGS_buffer_size,
                                             getLocationName(i));
        LOG_ASSERT(!rc);
    }

    auto segment_id = engine->openSegment(FLAGS_segment_id.c_str());

    std::vector<std::thread> workers(FLAGS_threads);

    struct timeval start_tv, stop_tv;
    gettimeofday(&start_tv, nullptr);

    for (int i = 0; i < FLAGS_threads; ++i)
        workers[i] = std::thread(initiatorWorker, engine.get(), segment_id, i,
                                 addr[i % buffer_num]);

    sleep(FLAGS_duration);
    running = false;

    for (int i = 0; i < FLAGS_threads; ++i) workers[i].join();

    gettimeofday(&stop_tv, nullptr);
    auto duration = (stop_tv.tv_sec - start_tv.tv_sec) +
                    (stop_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    auto batch_count = total_batch_count.load();

    LOG(INFO) << "Test completed: duration " << std::fixed
              << std::setprecision(2) << duration << ", batch count "
              << batch_count << ", throughput "
              << calculateRate(
                     batch_count * FLAGS_batch_size * FLAGS_block_size,
                     duration);

    for (int i = 0; i < buffer_num; ++i) {
        engine->unregisterLocalMemory(addr[i]);
    }
    freeBuffers(addr);

    return 0;
}

static std::atomic<bool> target_running(true);

void signalHandler(int /* signum */) {
    target_running.store(false, std::memory_order_relaxed);
}

static void setupSignalHandler() {
    struct sigaction sa;
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

int target() {
    setupSignalHandler();

    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(FLAGS_auto_discovery);

    auto hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hostname_port.first.c_str(), hostname_port.second);

    installTransportFromFlags(engine.get());

    auto addr = allocateBuffers();
    for (int i = 0; i < buffer_num; ++i) {
        int rc = engine->registerLocalMemory(addr[i], FLAGS_buffer_size,
                                             getLocationName(i));
        LOG_ASSERT(!rc);
    }

    while (target_running) sleep(1);

#ifdef USE_EFA
    if (FLAGS_neuron_device >= 0) reportNeuronOverwrite(addr);
#endif

    for (int i = 0; i < buffer_num; ++i) {
        engine->unregisterLocalMemory(addr[i]);
    }
    freeBuffers(addr);

    return 0;
}

#ifdef USE_TENT
// TENT backend implementation
namespace tent_backend {

// Helper function to register buffers for TENT backend
static void registerBuffers(mooncake::tent::TransferEngine* engine,
                            std::vector<void*>& addr) {
    for (int i = 0; i < buffer_num; ++i) {
        auto status = engine->registerLocalMemory(addr[i], FLAGS_buffer_size);
        LOG_ASSERT(status.ok())
            << "Failed to register memory: " << status.ToString();
    }
}

// Helper function to unregister buffers for TENT backend
static void unregisterBuffers(mooncake::tent::TransferEngine* engine,
                              std::vector<void*>& addr) {
    for (int i = 0; i < buffer_num; ++i) {
        engine->unregisterLocalMemory(addr[i], FLAGS_buffer_size);
    }
}

std::shared_ptr<mooncake::tent::Config> createTentConfig() {
    auto config = std::make_shared<mooncake::tent::Config>();

    // Parse metadata_server to determine type and address
    // Format: "redis://host:port" or "etcd://host:port" or "host:port" (default
    // p2p)
    std::string metadata_type = "p2p";
    std::string metadata_servers = "";

    if (FLAGS_metadata_server.find("redis://") == 0) {
        metadata_type = "redis";
        metadata_servers = FLAGS_metadata_server.substr(sizeof("redis://") - 1);
    } else if (FLAGS_metadata_server.find("etcd://") == 0) {
        metadata_type = "etcd";
        metadata_servers = FLAGS_metadata_server.substr(sizeof("etcd://") - 1);
    } else if (FLAGS_metadata_server == P2PHANDSHAKE) {
        metadata_type = "p2p";
        metadata_servers = P2PHANDSHAKE;
    } else if (!FLAGS_metadata_server.empty()) {
        // Default to etcd for backward compatibility
        metadata_type = "etcd";
        metadata_servers = FLAGS_metadata_server;
    }

    config->set("metadata_type", metadata_type);
    config->set("metadata_servers", metadata_servers);
    config->set("local_segment_name", FLAGS_local_server_name);
    config->set("verbose", true);

    return config;
}

void initiatorWorker(mooncake::tent::TransferEngine* engine,
                     mooncake::tent::SegmentID segment_id, int thread_id,
                     void* addr,
                     const mooncake::tent::SegmentInfo& segment_info) {
    bindToSocket(thread_id % NR_SOCKETS);
    setWorkerDeviceIfNeeded();
    mooncake::tent::Request::OpCode opcode;
    if (FLAGS_operation == "read")
        opcode = mooncake::tent::Request::READ;
    else if (FLAGS_operation == "write")
        opcode = mooncake::tent::Request::WRITE;
    else {
        LOG(ERROR) << "Unsupported operation: must be 'read' or 'write'";
        exit(EXIT_FAILURE);
    }

    // Buffer boundary check
    size_t buffer_index = thread_id % buffer_num;
    if (buffer_index >= segment_info.buffers.size()) {
        LOG(ERROR) << "Remote segment has fewer buffers ("
                   << segment_info.buffers.size() << ") than expected ("
                   << buffer_num << ")";
        exit(EXIT_FAILURE);
    }
    uint64_t remote_base = segment_info.buffers[buffer_index].base;

    size_t batch_count = 0;
    while (running) {
        auto batch_id = engine->allocateBatch(FLAGS_batch_size);
        std::vector<mooncake::tent::Request> requests;
        for (int i = 0; i < FLAGS_batch_size; ++i) {
            mooncake::tent::Request entry;
            entry.opcode = opcode;
            entry.length = FLAGS_block_size;
            entry.source = (uint8_t*)(addr) +
                           FLAGS_block_size * (i * FLAGS_threads + thread_id);
            entry.target_id = segment_id;
            entry.target_offset =
                remote_base +
                FLAGS_block_size * (i * FLAGS_threads + thread_id);
            requests.emplace_back(entry);
        }

        auto s = engine->submitTransfer(batch_id, requests);
        LOG_ASSERT(s.ok()) << "submitTransfer failed: " << s.ToString();

        while (true) {
            mooncake::tent::TransferStatus overall_status;
            s = engine->getTransferStatus(batch_id, overall_status);
            LOG_ASSERT(s.ok()) << "getTransferStatus failed: " << s.ToString();
            if (overall_status.s ==
                mooncake::tent::TransferStatusEnum::COMPLETED) {
                break;
            } else if (overall_status.s ==
                       mooncake::tent::TransferStatusEnum::FAILED) {
                LOG(ERROR) << "Transfer failed";
                exit(EXIT_FAILURE);
            }
        }

        s = engine->freeBatch(batch_id);
        LOG_ASSERT(s.ok()) << "freeBatch failed: " << s.ToString();
        batch_count++;
    }
    LOG(INFO) << "Worker " << thread_id << " stopped!";
    total_batch_count.fetch_add(batch_count);
}

int initiator() {
    auto config = createTentConfig();
    auto engine = std::make_unique<mooncake::tent::TransferEngine>(config);

    if (!engine->available()) {
        LOG(ERROR) << "Failed to initialize TENT TransferEngine";
        return EXIT_FAILURE;
    }

    auto addr = allocateBuffers();
    registerBuffers(engine.get(), addr);

    mooncake::tent::SegmentID segment_id;
    auto status = engine->openSegment(segment_id, FLAGS_segment_id);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to open segment: " << status.ToString();
        unregisterBuffers(engine.get(), addr);
        freeBuffers(addr);
        return EXIT_FAILURE;
    }

    mooncake::tent::SegmentInfo segment_info;
    status = engine->getSegmentInfo(segment_id, segment_info);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to get segment info: " << status.ToString();
        unregisterBuffers(engine.get(), addr);
        freeBuffers(addr);
        return EXIT_FAILURE;
    }

    std::vector<std::thread> workers(FLAGS_threads);

    struct timeval start_tv, stop_tv;
    gettimeofday(&start_tv, nullptr);

    for (int i = 0; i < FLAGS_threads; ++i)
        workers[i] = std::thread(initiatorWorker, engine.get(), segment_id, i,
                                 addr[i % buffer_num], std::ref(segment_info));

    sleep(FLAGS_duration);
    running = false;

    for (int i = 0; i < FLAGS_threads; ++i) workers[i].join();

    gettimeofday(&stop_tv, nullptr);
    auto duration = (stop_tv.tv_sec - start_tv.tv_sec) +
                    (stop_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    auto batch_count = total_batch_count.load();

    LOG(INFO) << "Test completed: duration " << std::fixed
              << std::setprecision(2) << duration << ", batch count "
              << batch_count << ", throughput "
              << calculateRate(
                     batch_count * FLAGS_batch_size * FLAGS_block_size,
                     duration);

    unregisterBuffers(engine.get(), addr);
    freeBuffers(addr);

    return 0;
}

int target() {
    setupSignalHandler();

    auto config = createTentConfig();
    auto engine = std::make_unique<mooncake::tent::TransferEngine>(config);

    if (!engine->available()) {
        LOG(ERROR) << "Failed to initialize TENT TransferEngine";
        return EXIT_FAILURE;
    }

    LOG(INFO) << "TENT target started, segment name: "
              << engine->getSegmentName();

    auto addr = allocateBuffers();
    registerBuffers(engine.get(), addr);

    while (target_running) sleep(1);

    unregisterBuffers(engine.get(), addr);
    freeBuffers(addr);

    return 0;
}

}  // namespace tent_backend
#endif  // USE_TENT

void check_total_buffer_size() {
    uint64_t require_size = FLAGS_block_size * FLAGS_batch_size * FLAGS_threads;
    if (FLAGS_buffer_size < require_size) {
        FLAGS_buffer_size = require_size;
        LOG(WARNING) << "Invalid flag: buffer size is smaller than "
                        "require_size, adjust to "
                     << require_size;
    }
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    check_total_buffer_size();

#ifdef USE_EFA
    // Only the EFA transport can register Neuron HBM.  Any other protocol
    // would take these buffers down a host path -- and since they are
    // registered under the wildcard location, nothing downstream would stop
    // preTouchMemory() from writing to device memory from the CPU.
    if (FLAGS_neuron_device >= 0 && FLAGS_protocol != "efa") {
        LOG(FATAL) << "--neuron_device requires --protocol=efa, got --protocol="
                   << FLAGS_protocol;
    }
#endif

#if defined(USE_UBSHMEM)
    if (FLAGS_gpu_id != -1) {
        checkAclError(aclrtSetDevice(FLAGS_gpu_id), "Failed to set device");
        LOG(INFO) << "Set device to " << FLAGS_gpu_id;
    } else {
        LOG(ERROR) << "-1 is not supported for NPUs";
    }
    if (FLAGS_use_vram == false) {
        LOG(ERROR) << "UBShmem transport only supports vram.";
    }
#endif
    if (FLAGS_backend == "classic") {
        if (FLAGS_mode == "initiator")
            return initiator();
        else if (FLAGS_mode == "target")
            return target();
    } else if (FLAGS_backend == "tent") {
#ifdef USE_TENT
        if (FLAGS_mode == "initiator")
            return tent_backend::initiator();
        else if (FLAGS_mode == "target")
            return tent_backend::target();
#else
        LOG(ERROR)
            << "TENT backend is not enabled. Please rebuild with -DUSE_TENT=ON";
        exit(EXIT_FAILURE);
#endif
    } else {
        LOG(ERROR) << "Unsupported backend: must be 'classic' or 'tent'";
        exit(EXIT_FAILURE);
    }

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    exit(EXIT_FAILURE);
}
