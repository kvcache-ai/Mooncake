// Copyright 2025 KVCache.AI
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

#include "tent_backend.h"
#include "utils.h"

#include <cctype>

#include "tent/common/types.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/topology.h"
#include "tent/runtime/transport_selector.h"

#if defined(USE_CUDA) || defined(USE_SUNRISE)
#include "cuda_alike.h"
#endif

#ifdef USE_HIP
#include <hip/hip_runtime.h>
#endif

namespace mooncake {
namespace tent {

volatile bool g_tent_running = true;
volatile bool g_tent_triggered_sig = false;

void signalHandlerV1(int signum) {
    if (g_tent_triggered_sig) {
        LOG(ERROR) << "Received signal " << signum
                   << " again, forcefully terminating...";
        std::exit(EXIT_FAILURE);
    }
    LOG(INFO) << "Received signal " << signum << ", stopping target server...";
    g_tent_running = false;
    g_tent_triggered_sig = true;
}

std::shared_ptr<Config> loadConfig() {
    auto config = std::make_shared<Config>();
    config->set("local_segment_name", XferBenchConfig::seg_name);
    config->set("metadata_type", XferBenchConfig::metadata_type);
    config->set("metadata_servers", XferBenchConfig::metadata_url_list);
    config->set("rpc_server_port", XferBenchConfig::rpc_server_port);

    // Configure transport types based on xport_type parameter
    if (!XferBenchConfig::xport_type.empty()) {
        // Map of transport names to their config keys (handle name mismatches)
        std::unordered_map<std::string, std::string> transport_map = {
            {"rdma", "rdma"},
            {"tcp", "tcp"},
            {"shm", "shm"},
            {"iouring", "io_uring"},  // Note: iouring -> io_uring
            {"gds", "gds"},
            {"mnnvl", "mnnvl"},
            {"nvlink", "nvlink"},
            {"sunrise_link", "sunrise_link"}};

        // Disable all transports by default
        for (const auto& entry : transport_map) {
            config->set("transports/" + entry.second + "/enable", false);
        }

        // Enable only the specified transport
        auto it = transport_map.find(XferBenchConfig::xport_type);
        if (it != transport_map.end()) {
            config->set("transports/" + it->second + "/enable", true);
        }
    }

    return config;
}

static TransportType getTransportType(const std::string& xport_type) {
    if (xport_type == "rdma") return RDMA;
    if (xport_type == "shm") return SHM;
    if (xport_type == "gds") return GDS;
    if (xport_type == "mnnvl") return MNNVL;
    if (xport_type == "nvlink") return NVLINK;
    if (xport_type == "tcp") return TCP;
    if (xport_type == "iouring") return IOURING;
    if (xport_type == "sunrise_link") return SUNRISE_LINK;
    return UNSPEC;
}

int TENTBenchRunner::allocateBuffers() {
    const auto total_buffer_size = XferBenchConfig::total_buffer_size;
    const auto& seg_type = XferBenchConfig::seg_type;
    const auto& xport_type = XferBenchConfig::xport_type;

    // Resolve device prefix, start index, and buffer count per seg_type
    std::string device_prefix;
    int start_idx = 0, num_buffers = 0;

    if (seg_type == "DRAM") {
        device_prefix = "cpu";
        num_buffers = numa_num_configured_nodes();
#if defined(USE_CUDA) || defined(USE_SUNRISE)
    } else if (seg_type == "VRAM") {
        device_prefix = "cuda";
        int gpu_count = 0;
        auto err = cudaGetDeviceCount(&gpu_count);
        LOG_ASSERT(err == cudaSuccess && gpu_count > 0)
            << "cudaGetDeviceCount failed: " << cudaGetErrorString(err);
        start_idx = 0;
        num_buffers = gpu_count;
        if (XferBenchConfig::local_gpu_id != -1) {
            start_idx = XferBenchConfig::local_gpu_id;
            num_buffers = 1;
            LOG_ASSERT(start_idx >= 0 && start_idx < gpu_count)
                << "local_gpu_id " << start_idx << " out of range [0, "
                << gpu_count << ")";
        }
#elif defined(USE_HIP)
    } else if (seg_type == "VRAM") {
        device_prefix = "rocm";
        int gpu_count = 0;
        hipGetDeviceCount(&gpu_count);
        start_idx = 0;
        num_buffers = gpu_count;
        if (XferBenchConfig::local_gpu_id != -1) {
            start_idx = XferBenchConfig::local_gpu_id;
            num_buffers = 1;
            LOG_ASSERT(start_idx >= 0 && start_idx < gpu_count)
                << "local_gpu_id " << start_idx << " out of range [0, "
                << gpu_count << ")";
        }
#endif
    } else {
        LOG(ERROR) << "Unknown seg_type: " << seg_type;
        return -1;
    }

    pinned_buffer_list_.resize(num_buffers, nullptr);
    uint64_t alloc_ns = 0, reg_ns = 0;
    for (int i = 0; i < num_buffers; ++i) {
        auto location = device_prefix + ":" + std::to_string(start_idx + i);
        MemoryOptions options;
        if (!xport_type.empty()) {
            options.type = getTransportType(xport_type);
            options.location = location;
        }

        auto t0 = getCurrentTimeInNano();
        if (!xport_type.empty()) {
            options.location = location;
            CHECK_FAIL(engine_->allocateLocalMemory(
                &pinned_buffer_list_[i], total_buffer_size, options));
        } else {
            CHECK_FAIL(engine_->allocateLocalMemory(
                &pinned_buffer_list_[i], total_buffer_size, location));
        }
        auto t1 = getCurrentTimeInNano();

#ifdef USE_SUNRISE
        if (seg_type == "VRAM") {
            auto err = cudaSetDevice(start_idx + i);
            CHECK_FAIL(err == cudaSuccess ? Status::OK()
                                          : Status::InternalError(
                                                "Failed to set Sunrise device "
                                                "before registerLocalMemory"));
        }
#endif
        CHECK_FAIL(engine_->registerLocalMemory(pinned_buffer_list_[i],
                                                total_buffer_size, options));
        auto t2 = getCurrentTimeInNano();

        alloc_ns += (t1 - t0);
        reg_ns += (t2 - t1);
    }

    LOG(INFO) << "Allocated " << total_buffer_size * num_buffers << " bytes "
              << seg_type << " buffers in " << alloc_ns / 1e6
              << " ms, registered in " << reg_ns / 1e6 << " ms";
    return 0;
}

int TENTBenchRunner::freeBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    for (size_t i = 0; i < pinned_buffer_list_.size(); ++i) {
        CHECK_FAIL(engine_->unregisterLocalMemory(pinned_buffer_list_[i],
                                                  total_buffer_size));
        CHECK_FAIL(engine_->freeLocalMemory(pinned_buffer_list_[i]));
    }
    pinned_buffer_list_.clear();
    return 0;
}

TENTBenchRunner::TENTBenchRunner() {
    signal(SIGINT, signalHandlerV1);
    signal(SIGTERM, signalHandlerV1);
    engine_ = std::make_unique<TransferEngine>(loadConfig());
    transport_hint_ = TransportSelector::parseTransportType(
        XferBenchConfig::tent_transport_hint);
    allocateBuffers();
}

TENTBenchRunner::~TENTBenchRunner() { freeBuffers(); }

int TENTBenchRunner::runTarget() {
    while (g_tent_running) sleep(1);
    return 0;
}

int TENTBenchRunner::startInitiator(int num_threads) {
    CHECK_FAIL(engine_->openSegment(handle_, XferBenchConfig::target_seg_name));
    info_.buffers.clear();
    CHECK_FAIL(engine_->getSegmentInfo(handle_, info_));
    std::sort(info_.buffers.begin(), info_.buffers.end(),
              [](const SegmentInfo::Buffer& a, const SegmentInfo::Buffer& b) {
                  return a.location < b.location;
              });
    threads_.resize(num_threads);
    current_task_.resize(threads_.size());
    g_tent_running = true;
    for (size_t i = 0; i < threads_.size(); ++i)
        threads_[i] = std::thread(&TENTBenchRunner::runner, this, i);
    return 0;
}

int TENTBenchRunner::stopInitiator() {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        g_tent_running = false;
        cv_task_.notify_all();
        cv_done_.notify_all();
    }
    for (auto& thread : threads_) {
        thread.join();
    }
    return 0;
}

static inline int getNumaNodeFromPciDevice(const std::string& pci_bdf) {
    std::string sysfs_path = "/sys/bus/pci/devices/" + pci_bdf + "/numa_node";
    std::ifstream numa_file(sysfs_path);
    if (!numa_file.is_open()) return -1;
    int numa_node = -1;
    numa_file >> numa_node;
    if (numa_file.fail()) return -1;
    return numa_node;
}

#if defined(USE_CUDA) || defined(USE_SUNRISE)
static inline int getGpuDeviceNumaID(int gpu_id) {
    char pci_bus_id[20];
    auto err = cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), gpu_id);
    if (err != cudaSuccess) {
        LOG(WARNING) << "cudaDeviceGetPCIBusId: " << cudaGetErrorString(err);
        return 0;
    }
    for (char* ch = pci_bus_id;
         (*ch =
              static_cast<char>(std::tolower(static_cast<unsigned char>(*ch))));
         ch++);
    return getNumaNodeFromPciDevice(pci_bus_id);
}
#elif defined(USE_HIP)
static inline int getGpuDeviceNumaID(int gpu_id) {
    hipDeviceProp_t prop;
    if (hipGetDeviceProperties(&prop, gpu_id) != hipSuccess) return 0;
    char pci_bus_id[20];
    snprintf(pci_bus_id, sizeof(pci_bus_id), "%04x:%02x:%02x.0",
             prop.pciDomainID, prop.pciBusID, prop.pciDeviceID);
    return getNumaNodeFromPciDevice(pci_bus_id);
}
#else
static inline int getGpuDeviceNumaID(int gpu_id) { return 0; }
#endif

void TENTBenchRunner::pinThread(int thread_id) {
#ifdef USE_SUNRISE
    if (XferBenchConfig::seg_type == "VRAM" && !pinned_buffer_list_.empty()) {
        int base_gpu = std::max(0, XferBenchConfig::local_gpu_id);
        int device_id =
            base_gpu +
            (thread_id % static_cast<int>(pinned_buffer_list_.size()));
        auto err = cudaSetDevice(device_id);
        LOG_ASSERT(err == cudaSuccess)
            << "cudaSetDevice failed before getLocation: "
            << cudaGetErrorString(err) << " device_id=" << device_id;
        bindToSocket(getGpuDeviceNumaID(device_id));
        return;
    }
#endif
    uint64_t addr =
        (uint64_t)pinned_buffer_list_[thread_id % pinned_buffer_list_.size()];
    auto result = Platform::getLoader().getLocation((void*)addr, 1);
    LocationParser location(result[0].location);
    if (location.type() == "cpu") {
        auto socket_id = location.index();
        bindToSocket(socket_id);
    } else if (location.type() == "cuda" || location.type() == "rocm") {
        auto device_id = location.index();
        auto socket_id = getGpuDeviceNumaID(device_id);
        bindToSocket(socket_id);
    }
}

int TENTBenchRunner::runner(int thread_id) {
    while (g_tent_running) {
        std::function<int(int)> task;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_task_.wait(lk, [&] {
                return !g_tent_running || current_task_[thread_id];
            });
            if (!g_tent_running) break;
            std::swap(task, current_task_[thread_id]);
        }
        if (task) task(thread_id);
        {
            std::unique_lock<std::mutex> lk(mtx_);
            if (--pending_ == 0) cv_done_.notify_all();
        }
    }
    return 0;
}

int TENTBenchRunner::runInitiatorTasks(
    const std::function<int(int /* thread_id */)>& func) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (size_t id = 0; id < current_task_.size(); ++id)
        current_task_[id] = func;
    pending_ = (int)threads_.size();
    cv_task_.notify_all();
    cv_done_.wait(lk, [&] { return !g_tent_running || pending_ == 0; });
    return g_tent_running ? 0 : -1;
}

double TENTBenchRunner::runSingleTransfer(uint64_t local_addr,
                                          uint64_t target_addr,
                                          uint64_t block_size,
                                          uint64_t batch_size, OpCode opcode) {
    auto batch_id = engine_->allocateBatch(batch_size);
    std::vector<Request> requests;
    for (uint64_t i = 0; i < batch_size; ++i) {
        Request entry;
        entry.opcode = opcode == READ ? Request::READ : Request::WRITE;
        entry.length = block_size;
        entry.source = (void*)(local_addr + block_size * i);
        entry.target_id = handle_;
        entry.target_offset = target_addr + block_size * i;
        entry.transport_hint = transport_hint_;
        requests.emplace_back(entry);
    }
    XferBenchTimer timer;
    if (XferBenchConfig::notifi) {
        // Use target_addr as msg for verification by peer
        Notification notifi{"benchmark", std::to_string(target_addr)};
        CHECK_FAIL(engine_->submitTransfer(batch_id, requests, notifi));
    } else {
        CHECK_FAIL(engine_->submitTransfer(batch_id, requests));
    }
    while (true) {
        TransferStatus overall_status;
        CHECK_FAIL(engine_->getTransferStatus(batch_id, overall_status));
        if (overall_status.s == TransferStatusEnum::COMPLETED) {
            break;
        } else if (overall_status.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "Failed transfer detected";
            exit(EXIT_FAILURE);
        }
    }
    auto duration = timer.lap_us();
    CHECK_FAIL(engine_->freeBatch(batch_id));
    return duration;
}

}  // namespace tent
}  // namespace mooncake
