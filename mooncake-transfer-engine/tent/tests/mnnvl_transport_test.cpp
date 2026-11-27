// Copyright 2026 KVCache.AI
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

#include <gtest/gtest.h>
#include "cuda_memory_test.h"
#include <glog/logging.h>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <cuda.h>
#include <cuda_runtime.h>

#include "cuda_memcpy_batch_compat.h"
#include "tent/common/config.h"
#include "tent/metrics/tent_metrics.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/mnnvl/mnnvl_transport.h"

#ifdef MNNVL_TEST_CUDA_WRAPPERS
namespace {
std::atomic<bool> fail_completion_record{false};
std::atomic<cudaEvent_t> fail_completion_query{nullptr};
std::atomic<bool> hide_fabric_support{false};
std::atomic<int> fabric_queries{0};
}  // namespace
extern "C" CUresult CUDAAPI __real_cuDeviceGetAttribute(int*,
                                                        CUdevice_attribute,
                                                        CUdevice);
extern "C" CUresult CUDAAPI __wrap_cuDeviceGetAttribute(
    int* value, CUdevice_attribute attribute, CUdevice device) {
    if (hide_fabric_support.load() &&
        attribute == CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED) {
        ++fabric_queries;
        *value = 0;
        return CUDA_SUCCESS;
    }
    return __real_cuDeviceGetAttribute(value, attribute, device);
}
extern "C" cudaError_t CUDARTAPI __real_cudaEventRecord(cudaEvent_t,
                                                        cudaStream_t);
extern "C" cudaError_t CUDARTAPI __real_cudaEventQuery(cudaEvent_t);
extern "C" cudaError_t CUDARTAPI __wrap_cudaEventRecord(cudaEvent_t event,
                                                        cudaStream_t stream) {
    if (stream != cudaStreamPerThread && fail_completion_record.exchange(false))
        return cudaErrorInvalidResourceHandle;
    return __real_cudaEventRecord(event, stream);
}
extern "C" cudaError_t CUDARTAPI __wrap_cudaEventQuery(cudaEvent_t event) {
    auto expected = event;
    if (fail_completion_query.compare_exchange_strong(expected, nullptr))
        return cudaErrorInvalidResourceHandle;
    return __real_cudaEventQuery(event);
}

// CUDA 12 resolves batch memcpy through dlsym, so wrapping the exported CUDA
// function alone would not intercept the transport's call.
#if CUDART_VERSION >= 12080 && CUDART_VERSION < 13000
namespace {
std::atomic<mooncake::CudaMemcpyBatchAsyncFn> real_batch_copy{nullptr};
std::atomic<bool> inject_copy_faults{false};
std::atomic<cudaError_t> batch_copy_error{cudaSuccess};
std::atomic<int> batch_copy_calls{0}, async_copy_calls{0}, stream_drains{0};
std::atomic<int> fail_async_copy_on{0};

class ScopedCopyFaults {
   public:
    explicit ScopedCopyFaults(cudaError_t error, int fail_copy = 0) {
        batch_copy_error.store(error);
        fail_async_copy_on.store(fail_copy);
        batch_copy_calls.store(0);
        async_copy_calls.store(0);
        stream_drains.store(0);
        inject_copy_faults.store(true);
    }
    ~ScopedCopyFaults() { inject_copy_faults.store(false); }
};

cudaError_t interceptedBatchCopy(void** dsts, void** srcs, size_t* sizes,
                                 size_t count, cudaMemcpyAttributes* attrs,
                                 size_t* indices, size_t num_attrs,
                                 size_t* fail_index, cudaStream_t stream) {
    if (inject_copy_faults.load()) {
        ++batch_copy_calls;
        return batch_copy_error.load();
    }
    return real_batch_copy.load()(dsts, srcs, sizes, count, attrs, indices,
                                  num_attrs, fail_index, stream);
}
}  // namespace
#endif
extern "C" void* __real_dlsym(void*, const char*);
extern "C" void* __wrap_dlsym(void* handle, const char* name) {
    void* symbol = __real_dlsym(handle, name);
#if CUDART_VERSION >= 12080 && CUDART_VERSION < 13000
    if (symbol && std::strcmp(name, "cudaMemcpyBatchAsync") == 0) {
        real_batch_copy.store(
            reinterpret_cast<mooncake::CudaMemcpyBatchAsyncFn>(symbol));
        return reinterpret_cast<void*>(&interceptedBatchCopy);
    }
#endif
    return symbol;
}
extern "C" cudaError_t CUDARTAPI __real_cudaMemcpyAsync(void*, const void*,
                                                        size_t, cudaMemcpyKind,
                                                        cudaStream_t);
extern "C" cudaError_t CUDARTAPI __wrap_cudaMemcpyAsync(void* dst,
                                                        const void* src,
                                                        size_t size,
                                                        cudaMemcpyKind kind,
                                                        cudaStream_t stream) {
#if CUDART_VERSION >= 12080 && CUDART_VERSION < 13000
    if (inject_copy_faults.load() &&
        ++async_copy_calls == fail_async_copy_on.load())
        return cudaErrorInvalidValue;
#endif
    return __real_cudaMemcpyAsync(dst, src, size, kind, stream);
}
extern "C" cudaError_t CUDARTAPI __real_cudaStreamSynchronize(cudaStream_t);
extern "C" cudaError_t CUDARTAPI
__wrap_cudaStreamSynchronize(cudaStream_t stream) {
#if CUDART_VERSION >= 12080 && CUDART_VERSION < 13000
    if (inject_copy_faults.load()) ++stream_drains;
#endif
    return __real_cudaStreamSynchronize(stream);
}
#endif

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static std::shared_ptr<tent::ControlService> metadata(
        tent::TransferEngineImpl& engine) {
        return engine.metadata_;
    }
    static void requireFabric(tent::TransferEngineImpl& engine) {
        engine.mnnvl_config_.require_fabric = true;
        engine.mnnvl_config_.egm_enabled = false;
    }
};

namespace tent {

class MnnvlTransportTestPeer {
   public:
    static std::shared_ptr<CudaSharedMemory> mapping(MnnvlTransport& transport,
                                                     SegmentID peer,
                                                     uint64_t address) {
        return transport.relocate_map_.at(peer).at(address).memory;
    }

    static Status relocate(MnnvlTransport& transport, uint64_t& address,
                           uint64_t length, SegmentID target_id) {
        return transport.relocateSharedMemoryAddress(address, length,
                                                     target_id);
    }
    static Status mapFixed(MnnvlTransport& transport, uint64_t address,
                           uint64_t length, SegmentID target_id) {
        return transport.mapVmmMemory(address, length, target_id, true);
    }
    static void configureLocalCopies(MnnvlTransport& transport,
                                     std::shared_ptr<ControlService> metadata,
                                     bool gpu_wait, size_t capacity = 1,
                                     bool completion_event_pool = true) {
        transport.metadata_ = std::move(metadata);
        transport.platform_ =
            dynamic_cast<CudaPlatform*>(&Platform::getLoader());
        transport.options_.require_fabric = true;
        transport.handle_type_ = CU_MEM_HANDLE_TYPE_FABRIC;
        transport.options_.gpu_wait = gpu_wait;
        transport.caps.gpu_to_gpu = true;
        transport.options_.completion_event_pool = completion_event_pool;
        transport.completion_pool_ = std::make_shared<MnnvlCompletionEventPool>(
            completion_event_pool ? capacity : 0);
        transport.installed_ = true;
    }
    static void configureRegistration(
        MnnvlTransport& transport, std::shared_ptr<ControlService> metadata) {
        configureLocalCopies(transport, std::move(metadata), false);
        transport.options_.require_fabric = false;
        transport.handle_type_ = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    }

    // Seed cache-only tests without creating a CUDA context or importing a
    // real mapping. The transport stays uninstalled, so teardown never tries
    // to unmap these synthetic addresses.
    static void seed(MnnvlTransport& transport,
                     std::shared_ptr<ControlService> metadata,
                     uint64_t remote_addr, size_t length,
                     const std::string& handle, uint64_t mapped_addr) {
        transport.metadata_ = std::move(metadata);
        transport.handle_type_ = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
        MnnvlTransport::OpenedMnnvlEntry entry;
        entry.mnnvl_addr = reinterpret_cast<void*>(mapped_addr);
        entry.length = length;
        entry.mapped_length = length;
        entry.mnnvl_handle = handle;
        transport.relocate_map_[LOCAL_SEGMENT_ID][remote_addr] =
            std::move(entry);
        // Production epochs count up from 1, so the high half is unique.
        static std::atomic<uint64_t> seeded_epoch{1ull << 63};
        transport.relocate_epoch_.store(seeded_epoch.fetch_add(1));
    }

    // Own ordinary pipe FDs without CUDA initialization. No batches, host
    // registrations or mappings exist, so uninstall only closes these FDs.
    static void seedExportedFd(MnnvlTransport& transport,
                               const BufferDesc& desc, int fd) {
        transport.installed_ = true;
        transport.exported_fds_.emplace(desc.mnnvl_handle, fd);
    }

    static Status importHandle(MnnvlTransport& transport,
                               CUmemAllocationHandleType type,
                               const std::string& handle) {
        transport.handle_type_ = type;
        BufferDesc buffer;
        buffer.length = 2 << 20;
        buffer.mnnvl_handle = handle;
        MnnvlTransport::OpenedMnnvlEntry entry;
        auto status = transport.importRemoteBuffer(buffer, entry);
        if (status.ok()) transport.releaseImportedEntry(entry);
        return status;
    }

    static Status ensureContext() {
        return MnnvlTransport::ensureCudaContext();
    }

    static Status setupPeerAccess(MnnvlTransport& transport) {
        return transport.setPeerAccess();
    }

    // Synthetic allocation records exercise bounds checks without CUDA.
    // The uninstalled transport never attempts to free these addresses.
    static void seedAllocation(MnnvlTransport& transport, uint64_t base,
                               size_t size, bool mapped = true) {
        auto* addr = reinterpret_cast<void*>(base);
        transport.allocate_set_[addr] = {addr, size, 0, mapped};
    }

    static Status allocationRange(MnnvlTransport& transport, uint64_t address,
                                  size_t length, uint64_t& base, size_t& size) {
        return transport.getLocalAllocationRange(address, length, base, size);
    }

    static size_t idle(MnnvlCompletionEventPool& pool, int device) {
        std::lock_guard<std::mutex> lock(pool.mutex_);
        return pool.idle_[device].size();
    }
};

namespace {

constexpr uint64_t kRemoteAddress = 0x10000000;
constexpr size_t kBufferLength = 4096;
constexpr uint32_t kPattern = 0x5a3c69e7;

class MnnvlLogSink : public google::LogSink {
   public:
    explicit MnnvlLogSink(int verbosity)
        : old_v_(google::SetVLOGLevel("mnnvl_transport", verbosity)),
          old_minloglevel_(FLAGS_minloglevel) {
        FLAGS_minloglevel = google::INFO;
        google::AddLogSink(this);
    }
    ~MnnvlLogSink() override {
        google::RemoveLogSink(this);
        google::SetVLOGLevel("mnnvl_transport", old_v_);
        FLAGS_minloglevel = old_minloglevel_;
    }
    void send(google::LogSeverity, const char*, const char*, int,
              const std::tm*, const char* message, size_t length) override {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.emplace_back(message, length);
    }
    size_t count(const std::string& text) {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::count_if(messages_.begin(), messages_.end(),
                             [&](const std::string& message) {
                                 return message.find(text) != std::string::npos;
                             });
    }

   private:
    int old_v_;
    int old_minloglevel_;
    std::mutex mutex_;
    std::vector<std::string> messages_;
};

// POSIX-FD handles are imported through pidfd_getfd(), so the exporter must
// be another process: the child allocates a VMM buffer with the
// POSIX_FILE_DESCRIPTOR handle type, fills it, exports the fd and stays
// alive (keeping the allocation and the fd open) until the parent is done.
//
// The parent must not touch CUDA before fork(); its first CUDA call happens
// in MnnvlTransport::install(), after the child has been spawned.
class PeerFdExporter {
   public:
    explicit PeerFdExporter(bool subrange = false) {
        if (pipe(ready_pipe_) != 0 || pipe(stop_pipe_) != 0) return;

        auto ready_arg = std::to_string(ready_pipe_[1]);
        auto stop_arg = std::to_string(stop_pipe_[0]);
        pid_t child = fork();
        if (child < 0) return;
        if (child == 0) {
            close(ready_pipe_[0]);
            close(stop_pipe_[1]);
            execl("/proc/self/exe", "mnnvl_transport_test", "--export-posix-fd",
                  ready_arg.c_str(), stop_arg.c_str(), subrange ? "1" : "0",
                  nullptr);
            _exit(127);
        }

        close(ready_pipe_[1]);
        close(stop_pipe_[0]);
        child_pid_ = child;
        valid_ = readExportResult();
    }

    ~PeerFdExporter() {
        if (child_pid_ > 0) {
            char done = 1;
            (void)write(stop_pipe_[1], &done, sizeof(done));
            (void)waitpid(child_pid_, nullptr, 0);
        }
        if (ready_pipe_[0] >= 0) close(ready_pipe_[0]);
        if (stop_pipe_[1] >= 0) close(stop_pipe_[1]);
    }

    static int runChild(int ready, int stop, bool subrange) {
        return childMain(ready, stop, subrange);
    }
    bool forkOk() const { return child_pid_ > 0; }
    bool valid() const { return valid_; }
    // 0 = exported fine; 1 = no CUDA device or no POSIX-FD VMM support.
    int childStatus() const { return child_status_; }
    // "<pid>-<fd>", the format MnnvlTransport publishes in mnnvl_handle.
    std::string mnnvlHandle() const {
        return std::to_string(child_pid_) + "-" + std::to_string(exported_fd_);
    }
    // Granularity-rounded allocation extent, distinct from a logical subrange.
    // cuMemMap on the importing side requires a granularity multiple.
    size_t mappedSize() const { return mapped_size_; }

   private:
    static int childMain(int ready_fd, int stop_fd, bool subrange) {
        int32_t status = 1;
        int device_count = 0;
        if (cudaGetDeviceCount(&device_count) != cudaSuccess ||
            device_count <= 0) {
            (void)write(ready_fd, &status, sizeof(status));
            return 0;
        }
        if (cudaSetDevice(0) != cudaSuccess || cudaFree(0) != cudaSuccess)
            return 2;

        CUdevice device;
        if (cuDeviceGet(&device, 0) != CUDA_SUCCESS) return 3;
        int fd_supported = 0;
        cuDeviceGetAttribute(
            &fd_supported,
            CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED,
            device);
        if (!fd_supported) {
            (void)write(ready_fd, &status, sizeof(status));
            return 0;
        }

        CUmemAllocationProp prop = {};
        prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
        prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        prop.location.id = device;
        prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
        size_t granularity = 0;
        if (cuMemGetAllocationGranularity(&granularity, &prop,
                                          CU_MEM_ALLOC_GRANULARITY_MINIMUM) !=
            CUDA_SUCCESS)
            return 4;
        size_t size =
            ((kBufferLength + granularity - 1) / granularity) * granularity;
        if (subrange) size = 2 * size;
        const size_t offset = subrange ? size / 2 + 64 : 0;

        CUmemGenericAllocationHandle handle;
        CUdeviceptr ptr = 0;
        if (cuMemCreate(&handle, size, &prop, 0) != CUDA_SUCCESS) return 5;
        if (cuMemAddressReserve(&ptr, size, 0, 0, 0) != CUDA_SUCCESS) return 6;
        if (cuMemMap(ptr, size, 0, handle, 0) != CUDA_SUCCESS) return 7;
        CUmemAccessDesc access = {};
        access.location = prop.location;
        access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        if (cuMemSetAccess(ptr, size, &access, 1) != CUDA_SUCCESS) return 8;

        std::vector<uint32_t> pattern(kBufferLength / sizeof(uint32_t),
                                      kPattern);
        if (cudaMemset(reinterpret_cast<void*>(ptr), 0x19, size) != cudaSuccess)
            return 9;
        if (cudaMemcpy(reinterpret_cast<void*>(ptr + offset), pattern.data(),
                       kBufferLength, cudaMemcpyHostToDevice) != cudaSuccess)
            return 9;

        int shared_fd = -1;
        if (cuMemExportToShareableHandle(
                &shared_fd, handle, CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                0) != CUDA_SUCCESS)
            return 10;

        status = 0;
        if (write(ready_fd, &status, sizeof(status)) != sizeof(status))
            return 11;
        if (write(ready_fd, &shared_fd, sizeof(shared_fd)) != sizeof(shared_fd))
            return 12;
        if (write(ready_fd, &size, sizeof(size)) != sizeof(size)) return 13;

        // Hold the allocation and the fd until the parent is done.
        char done = 0;
        while (read(stop_fd, &done, sizeof(done)) < 0 && errno == EINTR) {
        }
        return 0;
    }

    bool readExportResult() {
        int32_t status = -1;
        if (read(ready_pipe_[0], &status, sizeof(status)) !=
            (ssize_t)sizeof(status))
            return false;
        child_status_ = status;
        if (status != 0) return true;  // unsupported in child; skip, not fail
        if (read(ready_pipe_[0], &exported_fd_, sizeof(exported_fd_)) !=
            (ssize_t)sizeof(exported_fd_))
            return false;
        if (read(ready_pipe_[0], &mapped_size_, sizeof(mapped_size_)) !=
            (ssize_t)sizeof(mapped_size_))
            return false;
        return exported_fd_ >= 0 && mapped_size_ >= kBufferLength;
    }

    int ready_pipe_[2]{-1, -1};
    int stop_pipe_[2]{-1, -1};
    pid_t child_pid_{-1};
    int child_status_{-1};
    int exported_fd_{-1};
    size_t mapped_size_{0};
    bool valid_{false};
};

Status installLocalSegmentWithMnnvlHandle(ControlService& metadata,
                                          const std::string& mnnvl_handle,
                                          uint64_t remote_addr, size_t length) {
    return metadata.segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            segment.name = "mnnvl_test_segment";
            segment.machine_id = "mnnvl_test_machine";
            segment.type = SegmentType::Memory;
            auto& memory = std::get<MemorySegmentDesc>(segment.detail);
            memory.buffers.clear();
            BufferDesc buffer;
            buffer.addr = remote_addr;
            buffer.length = length;
            buffer.location = "cuda:0";
            buffer.mnnvl_handle = mnnvl_handle;
            buffer.transports = {MNNVL};
            memory.buffers.push_back(std::move(buffer));
            return Status::OK();
        });
}

std::shared_ptr<Config> makeRegistrationEngineConfig() {
    auto conf = std::make_shared<Config>();
    conf->set("metadata_type", "p2p");
    conf->set("rpc_server_hostname", "127.0.0.1");
    conf->set("rpc_server_port", "0");
    conf->set("log_level", "warning");
    for (const auto* name : {"tcp", "shm", "rdma", "io_uring", "nvlink",
                             "mnnvl", "gds", "ascend_direct"})
        conf->set(std::string("transports/") + name + "/enable", false);
    return conf;
}

std::shared_ptr<Config> makePosixFdConfig() {
    auto conf = std::make_shared<Config>();
    conf->set("transports/nvlink/handle_type", std::string("posix_fd"));
    return conf;
}

// pid of a process that has already exited (reaped, so the pid is dead).
pid_t exitedChildPid() {
    pid_t child = fork();
    if (child == 0) _exit(0);
    if (child > 0) (void)waitpid(child, nullptr, 0);
    return child;
}

}  // namespace

TEST(MnnvlTransportTest, OwnedVmmRangeIncludesSubranges) {
    MnnvlTransport transport;
    constexpr size_t mapped_size = 48 << 20;
    MnnvlTransportTestPeer::seedAllocation(transport, kRemoteAddress,
                                           mapped_size);
    for (uint64_t address : {kRemoteAddress, kRemoteAddress + 64,
                             kRemoteAddress + mapped_size - 64}) {
        uint64_t base = 0;
        size_t size = 0;
        ASSERT_TRUE(MnnvlTransportTestPeer::allocationRange(transport, address,
                                                            64, base, size)
                        .ok());
        EXPECT_EQ(base, kRemoteAddress);
        EXPECT_EQ(size, mapped_size);
    }
}

TEST(MnnvlTransportTest, OwnedVmmRangeRejectsOverflowAndOverrun) {
    MnnvlTransport transport;
    MnnvlTransportTestPeer::seedAllocation(transport, kRemoteAddress,
                                           kBufferLength);
    uint64_t base = 0;
    size_t size = 0;
    EXPECT_FALSE(
        MnnvlTransportTestPeer::allocationRange(
            transport, kRemoteAddress + kBufferLength - 32, 64, base, size)
            .ok());
    EXPECT_EQ(base, 0u);
    EXPECT_EQ(size, 0u);
    EXPECT_FALSE(MnnvlTransportTestPeer::allocationRange(
                     transport, UINT64_MAX - 31, 64, base, size)
                     .ok());
    EXPECT_FALSE(MnnvlTransportTestPeer::allocationRange(
                     transport, kRemoteAddress, 0, base, size)
                     .ok());
    EXPECT_FALSE(
        MnnvlTransportTestPeer::allocationRange(transport, 0, 64, base, size)
            .ok());

    // Reject before exporting or initializing CUDA, even in non-strict mode.
    BufferDesc desc;
    desc.addr = kRemoteAddress + kBufferLength - 32;
    desc.length = 64;
    desc.location = "cpu:0";
    MemoryOptions options;
    options.type = MNNVL;
    EXPECT_FALSE(transport.addMemoryBuffer(desc, options).ok());
    EXPECT_TRUE(desc.mnnvl_handle.empty());
    EXPECT_TRUE(desc.transports.empty());
}

TEST(MnnvlTransportTest, OwnedVmmRangeIgnoresUnknownAndUnmappedBuffers) {
    MnnvlTransport transport;
    MnnvlTransportTestPeer::seedAllocation(transport, kRemoteAddress,
                                           kBufferLength);
    for (uint64_t address :
         {kRemoteAddress - 64, kRemoteAddress + kBufferLength}) {
        uint64_t base = 1;
        size_t size = 1;
        EXPECT_TRUE(MnnvlTransportTestPeer::allocationRange(transport, address,
                                                            64, base, size)
                        .ok());
        EXPECT_EQ(base, 0u);
        EXPECT_EQ(size, 0u);
    }
    MnnvlTransportTestPeer::seedAllocation(transport, kRemoteAddress,
                                           kBufferLength, false);
    uint64_t base = 1;
    size_t size = 1;
    EXPECT_TRUE(MnnvlTransportTestPeer::allocationRange(
                    transport, kRemoteAddress, 64, base, size)
                    .ok());
    EXPECT_EQ(base, 0u);
    EXPECT_EQ(size, 0u);
    MnnvlTransportTestPeer::seedAllocation(transport, UINT64_MAX - 31, 64);
    EXPECT_TRUE(MnnvlTransportTestPeer::allocationRange(
                    transport, UINT64_MAX - 31, 16, base, size)
                    .ok());
    EXPECT_EQ(base, 0u);
    EXPECT_EQ(size, 0u);
}

TEST(MnnvlTransportTest, RemovingSameBaseRegistrationKeepsOtherExportOpen) {
    for (int removed_index : {0, 1}) {
        SCOPED_TRACE(removed_index);
        int fds[2];
        ASSERT_EQ(pipe(fds), 0);
        MnnvlTransport transport;
        BufferDesc descs[2];
        for (int i = 0; i < 2; ++i) {
            descs[i].addr = kRemoteAddress;
            descs[i].length = kBufferLength;
            descs[i].mnnvl_handle =
                std::to_string(getpid()) + "-" + std::to_string(fds[i]);
            MnnvlTransportTestPeer::seedExportedFd(transport, descs[i], fds[i]);
        }
        const int survivor_index = 1 - removed_index;
        const auto removed_desc = descs[removed_index];
        const auto survivor_handle = descs[survivor_index].mnnvl_handle;
        EXPECT_TRUE(transport.removeMemoryBuffer(descs[removed_index]).ok());
        EXPECT_TRUE(descs[removed_index].mnnvl_handle.empty());
        EXPECT_EQ(fcntl(fds[removed_index], F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
        EXPECT_GE(fcntl(fds[survivor_index], F_GETFD), 0);
        EXPECT_EQ(descs[survivor_index].mnnvl_handle, survivor_handle);

        // Failed/foreign registration cleanup at the same base owns no FD.
        auto unowned = descs[survivor_index];
        unowned.mnnvl_handle.clear();
        EXPECT_TRUE(transport.removeMemoryBuffer(unowned).ok());
        unowned.mnnvl_handle = "unowned-handle";
        EXPECT_TRUE(transport.removeMemoryBuffer(unowned).ok());
        EXPECT_GE(fcntl(fds[survivor_index], F_GETFD), 0);

        // A repeated removal must not close an unrelated reused FD number.
        int reused_fd = dup(fds[survivor_index]);
        ASSERT_GE(reused_fd, 0);
        auto stale_desc = removed_desc;
        EXPECT_TRUE(transport.removeMemoryBuffer(stale_desc).ok());
        EXPECT_GE(fcntl(reused_fd, F_GETFD), 0);
        EXPECT_EQ(close(reused_fd), 0);
        EXPECT_TRUE(transport.removeMemoryBuffer(descs[removed_index]).ok());
        EXPECT_GE(fcntl(fds[survivor_index], F_GETFD), 0);

        EXPECT_TRUE(transport.removeMemoryBuffer(descs[survivor_index]).ok());
        EXPECT_EQ(fcntl(fds[survivor_index], F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
    }
}

TEST(MnnvlTransportTest, UninstallClosesAllSameBaseExports) {
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);
    MnnvlTransport transport;
    for (int fd : fds) {
        BufferDesc desc;
        desc.addr = kRemoteAddress;
        desc.length = kBufferLength;
        desc.mnnvl_handle = std::to_string(getpid()) + "-" + std::to_string(fd);
        MnnvlTransportTestPeer::seedExportedFd(transport, desc, fd);
    }
    EXPECT_TRUE(transport.uninstall().ok());
    for (int fd : fds) {
        EXPECT_EQ(fcntl(fd, F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
    }
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, MalformedHandlesReturnErrorsWithoutThrowing) {
    MnnvlTransport transport;
    for (const std::string handle : {"not-a-pid-fd", "123-fd", "0-3",
                                     "123-3junk", "99999999999999999999-3"}) {
        SCOPED_TRACE(handle);
        EXPECT_FALSE(
            MnnvlTransportTestPeer::importHandle(
                transport, CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR, handle)
                .ok());
    }
    for (const std::string handle : {"f", "zz", "00"}) {
        SCOPED_TRACE(handle);
        EXPECT_FALSE(MnnvlTransportTestPeer::importHandle(
                         transport, CU_MEM_HANDLE_TYPE_FABRIC, handle)
                         .ok());
    }
}

TEST(MnnvlTransportTest, RelocationCacheDetectsChangedPeerHandle) {
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, "cached-handle", kRemoteAddress, kBufferLength)
                    .ok());
    MnnvlTransport transport;
    constexpr uint64_t mapped_addr = 0x20000000;
    MnnvlTransportTestPeer::seed(transport, metadata, kRemoteAddress,
                                 kBufferLength, "cached-handle", mapped_addr);
    uint64_t address = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(transport, address, 64,
                                                 LOCAL_SEGMENT_ID)
                    .ok());
    EXPECT_EQ(address, mapped_addr + 64);

    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, "not-a-pid-fd", kRemoteAddress, kBufferLength)
                    .ok());
    address = kRemoteAddress + 64;
    EXPECT_FALSE(MnnvlTransportTestPeer::relocate(transport, address, 64,
                                                  LOCAL_SEGMENT_ID)
                     .ok());
    EXPECT_EQ(address, kRemoteAddress + 64);
}

TEST(MnnvlTransportTest, RelocationCacheIsIsolatedBetweenInstances) {
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, "cached-handle", kRemoteAddress, kBufferLength)
                    .ok());
    MnnvlTransport first, second;
    MnnvlTransportTestPeer::seed(first, metadata, kRemoteAddress, kBufferLength,
                                 "cached-handle", 0x20000000);
    MnnvlTransportTestPeer::seed(second, metadata, kRemoteAddress,
                                 kBufferLength, "cached-handle", 0x30000000);
    for (auto* transport : {&first, &second, &first}) {
        uint64_t address = kRemoteAddress;
        ASSERT_TRUE(MnnvlTransportTestPeer::relocate(*transport, address, 64,
                                                     LOCAL_SEGMENT_ID)
                        .ok());
        EXPECT_EQ(address, transport == &first ? 0x20000000u : 0x30000000u);
    }
}

// Regression guard: cuMemImportFromShareableHandle() takes the POSIX fd
// value itself cast to void*, not a pointer to it. Passing &fd failed with
// CUDA_ERROR_UNKNOWN, so the POSIX-FD handle type could never import a peer
// buffer.
TEST(MnnvlTransportTest, RelocateImportsPosixFdHandle) {
    PeerFdExporter exporter;
    if (!exporter.forkOk() || !exporter.valid()) {
        GTEST_SKIP() << "Peer export failed (fork/pipe error)";
    }
    if (exporter.childStatus() != 0) {
        GTEST_SKIP() << "No CUDA device with POSIX-FD VMM support";
    }

    // First CUDA touch in the parent, after fork(). install() alone does not
    // create a context, and the driver calls in relocate need one.
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, exporter.mnnvlHandle(), kRemoteAddress,
                    exporter.mappedSize())
                    .ok());

    MnnvlTransport transport;
    std::string local_segment_name = "mnnvl_test_segment";
    auto status = transport.install(local_segment_name, metadata, nullptr,
                                    makePosixFdConfig());
    if (!status.ok()) {
        GTEST_SKIP() << "MnnvlTransport::install failed: " << status.ToString();
    }

    uint64_t dest_addr = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport, dest_addr, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());

    // dest_addr now aliases the child's allocation at offset 64.
    std::vector<uint32_t> readback((kBufferLength - 64) / sizeof(uint32_t));
    ASSERT_EQ(cudaMemcpy(readback.data(), reinterpret_cast<void*>(dest_addr),
                         kBufferLength - 64, cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (uint32_t value : readback) {
        EXPECT_EQ(value, kPattern);
    }
    // The same thread must not reuse a mapping after uninstall/reinstall.
    const auto old_mapping = dest_addr;
    ASSERT_TRUE(transport.uninstall().ok());
    unsigned long long flags = 0;
    CUmemLocation location{};
    location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    location.id = 0;
    EXPECT_NE(cuMemGetAccess(&flags, &location, (CUdeviceptr)old_mapping),
              CUDA_SUCCESS);
    ASSERT_TRUE(
        transport
            .install(local_segment_name, metadata, nullptr, makePosixFdConfig())
            .ok());
    dest_addr = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport, dest_addr, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());
    ASSERT_EQ(cudaMemcpy(readback.data(), reinterpret_cast<void*>(dest_addr),
                         kBufferLength - 64, cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (uint32_t value : readback) EXPECT_EQ(value, kPattern);
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, RelocateImportsLogicalSubrangeAndBoundsCache) {
    PeerFdExporter exporter(true);
    if (!exporter.forkOk() || !exporter.valid())
        GTEST_SKIP() << "Peer export failed (fork/pipe error)";
    if (exporter.childStatus() != 0)
        GTEST_SKIP() << "No CUDA device with POSIX-FD VMM support";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);
    const uint64_t offset = exporter.mappedSize() / 2 + 64;
    const auto handle = json{
        {"version", 1},
        {"handle", exporter.mnnvlHandle()},
        {"mapped_length", exporter.mappedSize()},
        {"offset", offset}}.dump();
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, handle, kRemoteAddress, kBufferLength)
                    .ok());
    MnnvlTransport transport;
    std::string name = "mnnvl_test_segment";
    const auto installed =
        transport.install(name, metadata, nullptr, makePosixFdConfig());
    if (!installed.ok())
        GTEST_SKIP() << "MnnvlTransport::install failed: "
                     << installed.ToString();
    for (uint64_t within_range : {uint64_t{0}, uint64_t{64}}) {
        uint64_t address = kRemoteAddress + within_range;
        ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                        transport, address, kBufferLength - within_range,
                        LOCAL_SEGMENT_ID)
                        .ok());
        std::vector<uint32_t> output((kBufferLength - within_range) / 4);
        ASSERT_EQ(cudaMemcpy(output.data(), reinterpret_cast<void*>(address),
                             output.size() * 4, cudaMemcpyDeviceToHost),
                  cudaSuccess);
        for (auto value : output) EXPECT_EQ(value, kPattern);
    }
    uint64_t outside = kRemoteAddress + kBufferLength;
    EXPECT_FALSE(MnnvlTransportTestPeer::relocate(transport, outside, 1,
                                                  LOCAL_SEGMENT_ID)
                     .ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, FixedMappingPreservesLogicalSubrangeOffset) {
    PeerFdExporter exporter(true);
    ASSERT_TRUE(exporter.forkOk());
    ASSERT_TRUE(exporter.valid());
    if (exporter.childStatus() != 0)
        GTEST_SKIP() << "No CUDA device with POSIX-FD VMM support";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(nullptr), cudaSuccess);
    const uint64_t offset = exporter.mappedSize() / 2 + 64;
    const uint64_t region = cuda_memory_test::kAddress + offset;
    const auto handle = json{
        {"version", 1},
        {"handle", exporter.mnnvlHandle()},
        {"mapped_length", exporter.mappedSize()},
        {"offset", offset}}.dump();
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.type = SegmentType::Memory;
                        BufferDesc buffer;
                        buffer.addr = region;
                        buffer.length = kBufferLength;
                        buffer.location = "cuda:0";
                        buffer.transports = {MNNVL};
                        buffer.mnnvl_handle = handle;
                        std::get<MemorySegmentDesc>(segment.detail).buffers = {
                            buffer};
                        return Status::OK();
                    })
                    .ok());
    MnnvlTransport transport;
    std::string name = "fixed_subrange";
    ASSERT_TRUE(
        transport.install(name, metadata, nullptr, makePosixFdConfig()).ok());
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto status = MnnvlTransportTestPeer::mapFixed(
            transport, region, kBufferLength, LOCAL_SEGMENT_ID);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }
    auto mapping =
        MnnvlTransportTestPeer::mapping(transport, LOCAL_SEGMENT_ID, region);
    EXPECT_EQ(mapping->address(), cuda_memory_test::kAddress);
    uint32_t value = 0;
    ASSERT_EQ(cudaMemcpy(&value, reinterpret_cast<void*>(region), sizeof(value),
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(value, kPattern);
    uint64_t relocated = region + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport, relocated, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());
    EXPECT_EQ(relocated, region + 64);
    uint64_t outside = region + kBufferLength;
    EXPECT_FALSE(MnnvlTransportTestPeer::relocate(transport, outside, 1,
                                                  LOCAL_SEGMENT_ID)
                     .ok());
    ASSERT_TRUE(transport
                    .unmapRemoteMemory(LOCAL_SEGMENT_ID,
                                       reinterpret_cast<void*>(region))
                    .ok());
    EXPECT_FALSE(mapping->mapped());
    ASSERT_TRUE(transport.uninstall().ok());
}

// A peer that is gone (or never exported the fd) must surface as a failed
// relocate, not a crash: pidfd_open fails on the dead pid and
// importFdFromProcess returns -1.
TEST(MnnvlTransportTest, RelocateFailsCleanlyWhenPeerIsGone) {
    pid_t dead_pid = exitedChildPid();
    if (dead_pid <= 0) {
        GTEST_SKIP() << "fork failed";
    }
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    std::string handle = std::to_string(dead_pid) + "-3";
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(*metadata, handle,
                                                   kRemoteAddress, 2 << 20)
                    .ok());

    MnnvlTransport transport;
    std::string local_segment_name = "mnnvl_test_segment";
    auto status = transport.install(local_segment_name, metadata, nullptr,
                                    makePosixFdConfig());
    if (!status.ok()) {
        GTEST_SKIP() << "MnnvlTransport::install failed: " << status.ToString();
    }

    uint64_t dest_addr = kRemoteAddress;
    EXPECT_FALSE(MnnvlTransportTestPeer::relocate(
                     transport, dest_addr, kBufferLength, LOCAL_SEGMENT_ID)
                     .ok());
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, PosixSharedMemoryCreationMappingAndTeardown) {
    PeerFdExporter exporter;
    if (!exporter.forkOk() || !exporter.valid() || exporter.childStatus() != 0)
        GTEST_SKIP() << "Requires CUDA POSIX-FD shared memory";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(nullptr), cudaSuccess);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, exporter.mnnvlHandle(), kRemoteAddress,
                    exporter.mappedSize())
                    .ok());
    MnnvlTransport transport;
    std::string name = "mnnvl_test_segment";
    ASSERT_TRUE(
        transport.install(name, metadata, nullptr, makePosixFdConfig()).ok());
    MemoryOptions options;
    options.type = MNNVL;
    options.location = "cuda:0";
    void* local = nullptr;
    auto status =
        transport.allocateLocalMemory(&local, kBufferLength + 1, options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    CUmemGenericAllocationHandle allocation;
    ASSERT_EQ(cuMemRetainAllocationHandle(&allocation, local), CUDA_SUCCESS);
    ASSERT_EQ(cuMemRelease(allocation), CUDA_SUCCESS);
    ASSERT_EQ(cudaMemset(local, 0x35, kBufferLength + 1), cudaSuccess);
    ASSERT_TRUE(transport.freeLocalMemory(local, kBufferLength + 1).ok());

    uint64_t address = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport, address, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());
    uint32_t value = 0;
    ASSERT_EQ(cudaMemcpy(&value, reinterpret_cast<void*>(address),
                         sizeof(value), cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(value, kPattern);
    auto previous = MnnvlTransportTestPeer::mapping(transport, LOCAL_SEGMENT_ID,
                                                    kRemoteAddress);
    ASSERT_TRUE(previous->mapped());
    ASSERT_TRUE(transport.uninstall().ok());
    EXPECT_FALSE(previous->mapped());

    // Thread-local views can outlive uninstall; they must not reuse a released
    // mapping when the same transport is installed again.
    ASSERT_TRUE(
        transport.install(name, metadata, nullptr, makePosixFdConfig()).ok());
    address = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport, address, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());
    auto current = MnnvlTransportTestPeer::mapping(transport, LOCAL_SEGMENT_ID,
                                                   kRemoteAddress);
    EXPECT_NE(current.get(), previous.get());
    EXPECT_TRUE(current->mapped());
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlVmmMemoryTest, SameAddressKernelAccessAndReuse) {
    cuda_memory_test::roundTrip(MNNVL);
}

#ifdef MNNVL_TEST_CUDA_WRAPPERS
TEST(MnnvlTransportTest, PosixFdInstallDoesNotRequireFabricSupport) {
    int devices = 0;
    if (cudaGetDeviceCount(&devices) != cudaSuccess || devices == 0)
        GTEST_SKIP() << "No CUDA device available";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    struct FabricUnavailable {
        FabricUnavailable() {
            fabric_queries.store(0);
            hide_fabric_support.store(true);
        }
        ~FabricUnavailable() { hide_fabric_support.store(false); }
    } unavailable;
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    MnnvlTransport transport;
    std::string name = "handle_capabilities";
    auto config = makePosixFdConfig();
    auto status = transport.install(name, metadata, nullptr, config);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(fabric_queries.load(), 0);
    ASSERT_TRUE(transport.uninstall().ok());

    // Strict mode must still reject POSIX-FD and require actual FABRIC support.
    config->set("transports/mnnvl/require_fabric", true);
    EXPECT_TRUE(
        transport.install(name, metadata, nullptr, config).IsInvalidArgument());
    config->set("transports/nvlink/handle_type", "fabric");
    EXPECT_TRUE(
        transport.install(name, metadata, nullptr, config).IsInvalidArgument());
    EXPECT_GT(fabric_queries.load(), 0);
}
#endif

TEST(MnnvlVmmMemoryTest, ExplicitUninstallPreservesLiveGvaAllocation) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    auto conf = cuda_memory_test::config(MNNVL);
    MnnvlTransport transport;
    std::string name = "allocation_lifecycle";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());
    MemoryOptions options;
    options.type = MNNVL;
    options.location = "cuda:0";
    options.global_address = cuda_memory_test::kAddress;
    void* memory = nullptr;
    auto status = transport.allocateLocalMemory(&memory, 4096, options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_FALSE(transport.uninstall().ok());
    ASSERT_EQ(cudaMemset(memory, 0x35, 4096), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
    ASSERT_TRUE(transport.freeLocalMemory(memory, 4096).ok());
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlVmmMemoryTest, OrdinaryAllocationSurvivesExplicitUninstall) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    MnnvlTransport transport;
    std::string name = "ordinary_lifecycle";
    ASSERT_TRUE(
        transport
            .install(name, metadata, nullptr, cuda_memory_test::config(MNNVL))
            .ok());
    MemoryOptions options;
    options.location = "cuda:0";
    options.type = MNNVL;
    void* memory = nullptr;
    ASSERT_TRUE(transport.allocateLocalMemory(&memory, 4096, options).ok());
    ASSERT_TRUE(transport.uninstall().ok());
    ASSERT_EQ(cudaMemset(memory, 0x35, 4096), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
    ASSERT_TRUE(transport.freeLocalMemory(memory, 4096).ok());
}

TEST(MnnvlVmmMemoryTest, ExternalCudaMallocIsSkippedFromDifferentDevice) {
    int devices = 0;
    ASSERT_EQ(cudaGetDeviceCount(&devices), cudaSuccess);
    ASSERT_GT(devices, 0);
    ASSERT_EQ(cudaSetDevice(devices > 1 ? 1 : 0), cudaSuccess);
    void* memory = nullptr;
    ASSERT_EQ(cudaMalloc(&memory, 4096), cudaSuccess);
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    MnnvlTransport transport;
    BufferDesc desc;
    desc.addr = (uint64_t)memory;
    desc.length = 4096;
    desc.location = devices > 1 ? "cuda:1" : "cuda:0";
    MemoryOptions options;
    auto status = transport.addMemoryBuffer(desc, options);
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(desc.transports.empty());
    EXPECT_TRUE(desc.mnnvl_handle.empty());
    ASSERT_EQ(cudaFree(memory), cudaSuccess);
}

// The relocation cache is thread-local. A new transport, or a reinstalled one,
// must not reach a peer address through mappings imported before it.
TEST(MnnvlTransportTest, RelocateCacheIsScopedToTransportInstallation) {
    PeerFdExporter exporter;
    pid_t dead_pid = exitedChildPid();
    if (!exporter.forkOk() || !exporter.valid() || dead_pid <= 0)
        GTEST_SKIP() << "Peer export failed (fork/pipe error)";
    if (exporter.childStatus() != 0)
        GTEST_SKIP() << "No CUDA device with POSIX-FD VMM support";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto live = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *live, exporter.mnnvlHandle(), kRemoteAddress,
                    exporter.mappedSize())
                    .ok());
    auto dead = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *dead, std::to_string(dead_pid) + "-3", kRemoteAddress,
                    exporter.mappedSize())
                    .ok());

    std::string name = "mnnvl_test_segment";
    {
        MnnvlTransport probe;
        const auto installed =
            probe.install(name, live, nullptr, makePosixFdConfig());
        if (!installed.ok())
            GTEST_SKIP() << "MnnvlTransport::install failed: "
                         << installed.ToString();
        ASSERT_TRUE(probe.uninstall().ok());
    }
    auto relocate_once = [&](MnnvlTransport& transport,
                             std::shared_ptr<ControlService> metadata) {
        auto status =
            transport.install(name, metadata, nullptr, makePosixFdConfig());
        if (!status.ok()) return status;
        uint64_t address = kRemoteAddress;
        status = MnnvlTransportTestPeer::relocate(
            transport, address, kBufferLength, LOCAL_SEGMENT_ID);
        EXPECT_TRUE(transport.uninstall().ok());
        return status;
    };

    MnnvlTransport first, second;
    EXPECT_TRUE(relocate_once(first, live).ok());
    EXPECT_FALSE(relocate_once(second, dead).ok());  // new instance
    EXPECT_TRUE(relocate_once(second, live).ok());
    EXPECT_FALSE(relocate_once(second, dead).ok());  // reinstalled instance
}

// A malformed POSIX-FD handle published by a peer must fail the relocate
// instead of escaping parsePidFd() as an exception.
TEST(MnnvlTransportTest, RelocateRejectsMalformedPosixFdHandle) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(*metadata, "not-a-pid-fd",
                                                   kRemoteAddress, 2 << 20)
                    .ok());

    MnnvlTransport transport;
    std::string local_segment_name = "mnnvl_test_segment";
    auto status = transport.install(local_segment_name, metadata, nullptr,
                                    makePosixFdConfig());
    if (!status.ok()) {
        GTEST_SKIP() << "MnnvlTransport::install failed: " << status.ToString();
    }

    uint64_t dest_addr = kRemoteAddress;
    EXPECT_FALSE(MnnvlTransportTestPeer::relocate(
                     transport, dest_addr, kBufferLength, LOCAL_SEGMENT_ID)
                     .ok());
    EXPECT_EQ(dest_addr, kRemoteAddress);
    ASSERT_TRUE(transport.uninstall().ok());
}

// transports/mnnvl/egm_strict without transports/mnnvl/egm must refuse host
// allocations instead of silently handing out memory peers cannot address.
TEST(MnnvlTransportTest, StrictEgmRejectsHostAllocationWithoutEgm) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    auto conf = makePosixFdConfig();
    conf->set("transports/mnnvl/egm", false);
    conf->set("transports/mnnvl/egm_strict", true);

    MnnvlTransport transport;
    std::string local_segment_name = "mnnvl_test_segment";
    auto status =
        transport.install(local_segment_name, metadata, nullptr, conf);
    if (!status.ok()) {
        GTEST_SKIP() << "MnnvlTransport::install failed: " << status.ToString();
    }

    MemoryOptions options;
    options.location = "cpu:0";
    void* addr = nullptr;
    EXPECT_FALSE(transport.allocateLocalMemory(&addr, 2 << 20, options).ok());
    EXPECT_EQ(addr, nullptr);
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, StrictRegistrationCleanupPreservesCallerPinning) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    auto conf = makePosixFdConfig();
    conf->set("transports/mnnvl/egm", true);
    conf->set("transports/mnnvl/egm_strict", true);
    MnnvlTransport transport;
    std::string segment_name = "mnnvl_test_segment";
    auto status = transport.install(segment_name, metadata, nullptr, conf);
    if (!status.ok()) GTEST_SKIP() << status.ToString();

    std::vector<uint8_t> buffer(kBufferLength);
    ASSERT_EQ(
        cudaHostRegister(buffer.data(), buffer.size(), cudaHostRegisterDefault),
        cudaSuccess);
    BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(buffer.data());
    desc.length = buffer.size();
    desc.location = "cpu:0";
    MemoryOptions options;
    options.type = MNNVL;
    EXPECT_FALSE(transport.addMemoryBuffer(desc, options).ok());
    EXPECT_TRUE(desc.mnnvl_handle.empty());
    EXPECT_FALSE(transport.tracksLocalBuffer(desc));
    EXPECT_TRUE(transport.removeMemoryBuffer(desc).ok());
    // Caller ownership remains intact after defensive engine cleanup.
    EXPECT_EQ(cudaHostUnregister(buffer.data()), cudaSuccess);
    EXPECT_TRUE(transport.uninstall().ok());
}

// Exercise actual export bookkeeping for two subranges of one VMM
// allocation. Unlike the pipe tests, this requires a CUDA-capable host.
static void checkIndependentExports(uint64_t global_address) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);
    CUdevice device;
    ASSERT_EQ(cuDeviceGet(&device, 0), CUDA_SUCCESS);
    int fd_supported = 0;
    ASSERT_EQ(
        cuDeviceGetAttribute(
            &fd_supported,
            CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED,
            device),
        CUDA_SUCCESS);
    if (!fd_supported) GTEST_SKIP() << "No POSIX-FD VMM support";

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    MnnvlTransport transport;
    std::string segment_name = "mnnvl_test_segment";
    auto conf = makePosixFdConfig();
    conf->set("transports/mnnvl/egm_strict", true);
    auto status = transport.install(segment_name, metadata, nullptr, conf);
    if (!status.ok()) GTEST_SKIP() << status.ToString();

    MemoryOptions options;
    options.type = MNNVL;
    options.location = "cuda:0";
    options.global_address = global_address;
    void* ptr = nullptr;
    ASSERT_TRUE(
        transport.allocateLocalMemory(&ptr, kBufferLength, options).ok());
    auto free_buffer = [&transport](void* addr) {
        EXPECT_TRUE(transport.freeLocalMemory(addr, kBufferLength).ok());
    };
    std::unique_ptr<void, decltype(free_buffer)> allocation(ptr, free_buffer);
    BufferDesc first;
    first.addr = reinterpret_cast<uint64_t>(ptr);
    first.length = kBufferLength / 2;
    first.location = options.location;
    if (global_address) {
        BufferDesc overrun = first;
        overrun.addr += kBufferLength - 1;
        overrun.length = 2;
        EXPECT_FALSE(transport.addMemoryBuffer(overrun, options).ok());
        EXPECT_TRUE(overrun.mnnvl_handle.empty());
    }
    BufferDesc second = first;
    second.addr += kBufferLength / 2;
    uint64_t mapped_base = 0;
    size_t mapped_size = 0;
    ASSERT_TRUE(
        MnnvlTransportTestPeer::allocationRange(
            transport, first.addr, first.length, mapped_base, mapped_size)
            .ok());
    ASSERT_EQ(mapped_base, reinterpret_cast<uint64_t>(ptr));
    ASSERT_GE(mapped_size, kBufferLength);
    ASSERT_TRUE(transport.addMemoryBuffer(first, options).ok());
    ASSERT_FALSE(first.mnnvl_handle.empty());
    std::thread registration_thread([&] {
        CUcontext context = nullptr;
        ASSERT_EQ(cuCtxGetCurrent(&context), CUDA_SUCCESS);
        ASSERT_EQ(context, nullptr);
        auto registered = transport.addMemoryBuffer(second, options);
        ASSERT_TRUE(registered.ok()) << registered.ToString();
        ASSERT_EQ(cuCtxGetCurrent(&context), CUDA_SUCCESS);
        EXPECT_NE(context, nullptr);
    });
    registration_thread.join();
    ASSERT_FALSE(second.mnnvl_handle.empty());
    // Both keep their logical ranges; envelopes describe the shared mapping.
    EXPECT_EQ(first.addr, mapped_base);
    EXPECT_EQ(first.length, kBufferLength / 2);
    EXPECT_EQ(second.addr, mapped_base + kBufferLength / 2);
    EXPECT_EQ(second.length, kBufferLength / 2);
    EXPECT_NE(first.mnnvl_handle, second.mnnvl_handle);
    const auto first_export = json::parse(first.mnnvl_handle);
    const auto second_export = json::parse(second.mnnvl_handle);
    EXPECT_EQ(first_export.at("mapped_length").get<size_t>(), mapped_size);
    EXPECT_EQ(second_export.at("mapped_length").get<size_t>(), mapped_size);
    EXPECT_EQ(first_export.at("offset").get<size_t>(), 0u);
    EXPECT_EQ(second_export.at("offset").get<size_t>(), kBufferLength / 2);
    auto exported_fd = [](const json& envelope) {
        const auto handle = envelope.at("handle").get<std::string>();
        return std::stoi(handle.substr(handle.find_last_of('-') + 1));
    };
    const int first_fd = exported_fd(first_export);
    const int second_fd = exported_fd(second_export);
    EXPECT_GE(fcntl(first_fd, F_GETFD), 0);
    EXPECT_GE(fcntl(second_fd, F_GETFD), 0);

    EXPECT_TRUE(transport.removeMemoryBuffer(first).ok());
    EXPECT_EQ(fcntl(first_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_GE(fcntl(second_fd, F_GETFD), 0);
    // The surviving published FD must remain importable, not merely open.
    CUmemGenericAllocationHandle imported;
    ASSERT_EQ(cuMemImportFromShareableHandle(
                  &imported,
                  reinterpret_cast<void*>(static_cast<uintptr_t>(second_fd)),
                  CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR),
              CUDA_SUCCESS);
    EXPECT_EQ(cuMemRelease(imported), CUDA_SUCCESS);
    EXPECT_TRUE(transport.removeMemoryBuffer(second).ok());
    EXPECT_EQ(fcntl(second_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    allocation.reset();
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, SameVmmBaseRegistrationsKeepIndependentExports) {
    checkIndependentExports(0);
}

TEST(MnnvlVmmMemoryTest, GvaSubrangesKeepIndependentExportsAndLogicalBounds) {
    checkIndependentExports(cuda_memory_test::kAddress);
}

TEST(MnnvlTransportTest, PeerAccessInitializesCudaContextOnFreshThread) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    ASSERT_EQ(cuInit(0), CUDA_SUCCESS);
    std::thread worker([] {
        CUcontext context = nullptr;
        ASSERT_EQ(cuCtxGetCurrent(&context), CUDA_SUCCESS);
        ASSERT_EQ(context, nullptr);
        MnnvlTransport transport;
        auto status = MnnvlTransportTestPeer::setupPeerAccess(transport);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(cuCtxGetCurrent(&context), CUDA_SUCCESS);
        EXPECT_NE(context, nullptr);
    });
    worker.join();
}

TEST(MnnvlTransportTest, PreservesCurrentCudaContext) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    // Also exercise a nonzero selected device when more than one is visible.
    ASSERT_EQ(cudaSetDevice(device_count - 1), cudaSuccess);
    ASSERT_EQ(cudaFree(nullptr), cudaSuccess);
    CUcontext before = nullptr;
    ASSERT_EQ(cuCtxGetCurrent(&before), CUDA_SUCCESS);
    ASSERT_NE(before, nullptr);
    auto status = MnnvlTransportTestPeer::ensureContext();
    ASSERT_TRUE(status.ok()) << status.ToString();
    CUcontext after = nullptr;
    ASSERT_EQ(cuCtxGetCurrent(&after), CUDA_SUCCESS);
    EXPECT_EQ(after, before);
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
}

namespace {

// Completion and caller ordering need a CUDA device, but not an IMEX domain.
// Only the FABRIC allocation/import cases need actual MNNVL hardware.
class MnnvlCompletionTest : public ::testing::Test {
   protected:
    void SetUp() override {
        int count = 0;
        if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0)
            GTEST_SKIP() << "No CUDA device available";
        ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
        ASSERT_EQ(cudaMalloc(&source_, kBufferLength), cudaSuccess);
        ASSERT_EQ(cudaMalloc(&target_, kBufferLength), cudaSuccess);
        ASSERT_EQ(cudaMalloc(&other_, kBufferLength), cudaSuccess);
        metadata_ = std::make_shared<ControlService>("p2p", "", nullptr);
        ASSERT_TRUE(
            metadata_->segmentManager()
                .updateLocal([&](SegmentDesc& segment) {
                    segment.type = SegmentType::Memory;
                    auto& memory = std::get<MemorySegmentDesc>(segment.detail);
                    memory.buffers.clear();
                    for (void* ptr : {source_, target_, other_}) {
                        BufferDesc buffer;
                        buffer.addr = reinterpret_cast<uint64_t>(ptr);
                        buffer.length = kBufferLength;
                        buffer.location = "cuda:0";
                        buffer.permission = kLocalReadWrite;
                        memory.buffers.push_back(buffer);
                    }
                    std::sort(memory.buffers.begin(), memory.buffers.end(),
                              [](const auto& a, const auto& b) {
                                  return a.addr < b.addr;
                              });
                    return Status::OK();
                })
                .ok());
        MnnvlTransportTestPeer::configureLocalCopies(transport_, metadata_,
                                                     true);
        ASSERT_TRUE(transport_.allocateSubBatch(batch_, 16).ok());
#if TENT_METRICS_ENABLED
        auto& metrics = TentMetrics::instance();
        metrics.shutdown();
        MetricsConfig config;
        config.enabled = true;
        config.http_port = 19876;
        config.report_interval_seconds = 0;
        ASSERT_TRUE(metrics.initialize(config).ok());
        TentMetrics::setEnabled(true);
#endif
    }
    void TearDown() override {
        release_.store(true);
        if (watchdog_.joinable()) watchdog_.join();
        if (batch_) EXPECT_TRUE(transport_.freeSubBatch(batch_).ok());
        if (source_) (void)cudaFree(source_);
        if (target_) (void)cudaFree(target_);
        if (other_) (void)cudaFree(other_);
#if TENT_METRICS_ENABLED
        TentMetrics::instance().shutdown();
#endif
    }
    static void CUDART_CB waitForRelease(void* opaque) {
        auto* release = static_cast<std::atomic<bool>*>(opaque);
        while (!release->load())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    void delayProducer(cudaStream_t stream = cudaStreamPerThread) {
        release_.store(false);
        // Bounded even if a regression accidentally makes gpu_wait
        // CPU-blocking.
        watchdog_ = std::thread([this] {
            for (int i = 0; i < 2000 && !release_.load(); ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            release_.store(true);
        });
        ASSERT_EQ(cudaLaunchHostFunc(stream, waitForRelease, &release_),
                  cudaSuccess);
    }
    Request request(Request::OpCode op = Request::WRITE,
                    void* target = nullptr) {
        Request req;
        req.opcode = op;
        req.source = source_;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset =
            reinterpret_cast<uint64_t>(target ? target : target_);
        req.length = kBufferLength;
        return req;
    }
    TransferStatus finish(int task) {
        TransferStatus status;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        do {
            EXPECT_TRUE(
                transport_.getTransferStatus(batch_, task, status).ok());
            if (status.s != PENDING) return status;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        ADD_FAILURE() << "MNNVL completion timed out";
        return status;
    }
    void expectPattern(void* ptr, uint8_t pattern) {
        std::vector<uint8_t> output(kBufferLength);
        ASSERT_EQ(cudaMemcpy(output.data(), ptr, output.size(),
                             cudaMemcpyDeviceToHost),
                  cudaSuccess);
        for (auto byte : output) EXPECT_EQ(byte, pattern);
    }
    void* source_{nullptr};
    void* target_{nullptr};
    void* other_{nullptr};
    std::shared_ptr<ControlService> metadata_;
    MnnvlTransport transport_;
    Transport::SubBatchRef batch_{nullptr};
    std::atomic<bool> release_{true};
    std::thread watchdog_;
};

TEST_F(MnnvlCompletionTest, DefaultPlatformSupportsTopologyDiscovery) {
    // SetUp() initializes the loader without a config, as direct transport
    // users do. Topology probing must still be safe when an engine later
    // reuses that singleton.
    auto& platform = Platform::getLoader();
    EXPECT_EQ(&platform, &Platform::getLoader(makeRegistrationEngineConfig()));
    Topology topology;
    ASSERT_TRUE(topology.discover({&platform}).ok());
    EXPECT_FALSE(topology.empty());
}

TEST_F(MnnvlCompletionTest, VerboseLogsDescribeEachSubmissionAndCompletion) {
    MnnvlLogSink logs(1);
    delayProducer();
    ASSERT_EQ(
        cudaMemsetAsync(source_, 0x5a, kBufferLength, cudaStreamPerThread),
        cudaSuccess);
    ASSERT_TRUE(transport_
                    .submitTransferTasks(
                        batch_, {request(), request(Request::WRITE, other_)})
                    .ok());
    EXPECT_EQ(logs.count("MnnvlTransport: submit batch="), 1);
    EXPECT_EQ(
        logs.count("tasks=2 bytes=8192 reads=0 writes=2 local=2 remote=0"), 1);
    EXPECT_EQ(logs.count("handle_type=fabric sync_mode=gpu_wait"), 1);
    EXPECT_EQ(logs.count("MnnvlTransport: copy stream ready batch="), 1);
    TransferStatus pending;
    ASSERT_TRUE(transport_.getTransferStatus(batch_, 0, pending).ok());
    EXPECT_EQ(pending.s, PENDING);
    ASSERT_TRUE(transport_.getTransferStatus(batch_, 1, pending).ok());
    EXPECT_EQ(pending.s, PENDING);
    EXPECT_EQ(logs.count("MnnvlTransport: complete batch="), 0);
    release_.store(true);
    EXPECT_EQ(finish(0).s, COMPLETED);
    EXPECT_EQ(finish(1).s, COMPLETED);
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(finish(0).s, COMPLETED);
        EXPECT_EQ(finish(1).s, COMPLETED);
    }
    EXPECT_EQ(logs.count("MnnvlTransport: complete batch="), 1);
    EXPECT_EQ(logs.count("group=0 device=0 status=COMPLETED"), 1);
    ASSERT_TRUE(
        transport_.submitTransferTasks(batch_, {request(Request::READ)}).ok());
    EXPECT_EQ(finish(2).s, COMPLETED);
    EXPECT_EQ(logs.count("MnnvlTransport: submit batch="), 2);
    EXPECT_EQ(
        logs.count("tasks=1 bytes=4096 reads=1 writes=0 local=1 remote=0"), 1);
    EXPECT_EQ(logs.count("MnnvlTransport: complete batch="), 2);
    EXPECT_EQ(logs.count("group=1 device=0 status=COMPLETED"), 1);
    EXPECT_EQ(logs.count("MnnvlTransport: copy stream ready batch="), 1);
    expectPattern(source_, 0x5a);
    expectPattern(other_, 0x5a);
}

TEST_F(MnnvlCompletionTest, TransferLogsAreQuietWhenVerboseLoggingIsDisabled) {
    MnnvlLogSink logs(0);
    ASSERT_EQ(cudaMemset(source_, 0x69, kBufferLength), cudaSuccess);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {request()}).ok());
    EXPECT_EQ(finish(0).s, COMPLETED);
    EXPECT_EQ(logs.count("MnnvlTransport: submit batch="), 0);
    EXPECT_EQ(logs.count("MnnvlTransport: complete batch="), 0);
    EXPECT_EQ(logs.count("MnnvlTransport: copy stream ready batch="), 0);
    EXPECT_EQ(logs.count("MnnvlTransport: completion event pool"), 0);
    expectPattern(target_, 0x69);
}

#ifdef MNNVL_TEST_CUDA_WRAPPERS
TEST_F(MnnvlCompletionTest, VerboseCompletionFailureIsLoggedOncePerGroup) {
    MnnvlLogSink logs(1);
    ASSERT_EQ(cudaMemset(source_, 0x12, kBufferLength), cudaSuccess);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {request()}).ok());
    auto* batch = static_cast<MnnvlSubBatch*>(batch_);
    fail_completion_query.store(batch->completion_groups[0]->event);
    EXPECT_EQ(finish(0).s, FAILED);
    EXPECT_EQ(finish(0).s, FAILED);
    EXPECT_EQ(logs.count("MnnvlTransport: complete batch="), 1);
    EXPECT_EQ(logs.count("group=0 device=0 status=FAILED"), 1);
}
#endif

TEST_F(MnnvlCompletionTest, GpuWaitOrdersDelayedCallerAndSharesPolling) {
    delayProducer();
    ASSERT_EQ(
        cudaMemsetAsync(source_, 0x5a, kBufferLength, cudaStreamPerThread),
        cudaSuccess);
    auto first = request();
    auto second = request(Request::WRITE, other_);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {first, second}).ok());
    EXPECT_FALSE(release_.load()) << "gpu_wait blocked the submitting CPU";
#if TENT_METRICS_ENABLED
    auto query_count = [] {
        return json::parse(TentMetrics::instance().getJsonMetrics())
            .value("tent_mnnvl_completion_query_total", uint64_t{0});
    };
    const auto before = query_count();
#endif
    TransferStatus status;
    ASSERT_TRUE(transport_.getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    ASSERT_TRUE(transport_.getTransferStatus(batch_, 1, status).ok());
    EXPECT_EQ(status.s, PENDING);
#if TENT_METRICS_ENABLED
    EXPECT_EQ(query_count() - before, 1);
#endif
    release_.store(true);
    EXPECT_EQ(finish(0).s,
              COMPLETED);  // Repeated/equal ordinal must query again.
    EXPECT_EQ(finish(1).s, COMPLETED);
    expectPattern(target_, 0x5a);
    expectPattern(other_, 0x5a);
}

TEST_F(MnnvlCompletionTest,
       AppendedSubmissionsAndCallerEventRerecordAreIndependent) {
    ASSERT_EQ(
        cudaMemsetAsync(source_, 0x3c, kBufferLength, cudaStreamPerThread),
        cudaSuccess);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {request()}).ok());
    EXPECT_EQ(finish(0).s, COMPLETED);
    delayProducer();
    ASSERT_EQ(
        cudaMemsetAsync(source_, 0xa7, kBufferLength, cudaStreamPerThread),
        cudaSuccess);
    ASSERT_TRUE(
        transport_
            .submitTransferTasks(batch_, {request(Request::WRITE, other_)})
            .ok());
    auto* batch = static_cast<MnnvlSubBatch*>(batch_);
    ASSERT_EQ(batch->completion_groups.size(), 2);
    EXPECT_NE(batch->completion_groups[0]->event,
              batch->completion_groups[1]->event);
    EXPECT_EQ(finish(0).s, COMPLETED);
    TransferStatus pending;
    ASSERT_TRUE(transport_.getTransferStatus(batch_, 1, pending).ok());
    EXPECT_EQ(pending.s, PENDING);
    release_.store(true);
    EXPECT_EQ(finish(1).s, COMPLETED);
    expectPattern(target_, 0x3c);
    expectPattern(other_, 0xa7);
}

TEST_F(MnnvlCompletionTest, CpuBlockAndReadKeepExistingOrdering) {
    MnnvlTransportTestPeer::configureLocalCopies(transport_, metadata_, false);
    ASSERT_EQ(
        cudaMemsetAsync(target_, 0x69, kBufferLength, cudaStreamPerThread),
        cudaSuccess);
    ASSERT_TRUE(
        transport_.submitTransferTasks(batch_, {request(Request::READ)}).ok());
    EXPECT_EQ(finish(0).s, COMPLETED);
    expectPattern(source_, 0x69);
}

TEST_F(MnnvlCompletionTest, CopyWaitsForRequestProducerEvent) {
    CUDAStreamPool pool;
    CUDAStreamHandle producer_stream;
    ASSERT_TRUE(pool.acquire(producer_stream, 0).ok());
    ASSERT_EQ(cudaMemset(source_, 0, kBufferLength), cudaSuccess);
    cudaEvent_t producer_event = nullptr;
    ASSERT_EQ(cudaEventCreateWithFlags(&producer_event, cudaEventDisableTiming),
              cudaSuccess);

    struct Gate {
        std::atomic<bool> released{false};
        cudaStream_t stream;
        ~Gate() {
            released.store(true);
            cudaStreamSynchronize(stream);
        }
    } gate{{false}, producer_stream.get()};
    ASSERT_EQ(cudaLaunchHostFunc(
                  producer_stream.get(),
                  [](void* data) {
                      auto& gate = *static_cast<Gate*>(data);
                      // Bound the wait even if the test exits unexpectedly.
                      auto deadline = std::chrono::steady_clock::now() +
                                      std::chrono::seconds(5);
                      while (!gate.released.load() &&
                             std::chrono::steady_clock::now() < deadline)
                          std::this_thread::yield();
                  },
                  &gate),
              cudaSuccess);
    ASSERT_EQ(
        cudaMemsetAsync(source_, 0x5a, kBufferLength, producer_stream.get()),
        cudaSuccess);
    ASSERT_EQ(cudaEventRecord(producer_event, producer_stream.get()),
              cudaSuccess);

    auto req = request();
    req.producer_event = producer_event;
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {req}).ok());
    TransferStatus before_release{};
    EXPECT_TRUE(transport_.getTransferStatus(batch_, 0, before_release).ok());
    EXPECT_EQ(before_release.s, PENDING);
    gate.released.store(true);
    EXPECT_EQ(finish(0).s, COMPLETED);
    expectPattern(target_, 0x5a);
    EXPECT_EQ(cudaEventDestroy(producer_event), cudaSuccess);
}

TEST_F(MnnvlCompletionTest, ExternalProducerFenceRemainsEffective) {
    cudaStream_t producer;
    cudaEvent_t ready;
    ASSERT_EQ(cudaStreamCreateWithFlags(&producer, cudaStreamNonBlocking),
              cudaSuccess);
    ASSERT_EQ(cudaEventCreateWithFlags(&ready, cudaEventDisableTiming),
              cudaSuccess);
    delayProducer(producer);
    ASSERT_EQ(cudaMemsetAsync(target_, 0x42, kBufferLength, producer),
              cudaSuccess);
    ASSERT_EQ(cudaEventRecord(ready, producer), cudaSuccess);
    ASSERT_EQ(cudaStreamWaitEvent(cudaStreamPerThread, ready, 0), cudaSuccess);
    ASSERT_TRUE(
        transport_.submitTransferTasks(batch_, {request(Request::READ)}).ok());
    release_.store(true);
    EXPECT_EQ(finish(0).s, COMPLETED);
    expectPattern(source_, 0x42);
    EXPECT_EQ(cudaEventDestroy(ready), cudaSuccess);
    EXPECT_EQ(cudaStreamDestroy(producer), cudaSuccess);
}

TEST_F(MnnvlCompletionTest, FailedValidationPreservesEarlierTasks) {
    ASSERT_EQ(cudaMemset(source_, 0x12, kBufferLength), cudaSuccess);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {request()}).ok());
    auto invalid = request();
    invalid.source = reinterpret_cast<void*>(1);
    EXPECT_FALSE(
        transport_.submitTransferTasks(batch_, {request(), invalid}).ok());
    invalid = request();
    invalid.target_offset = 1;
    EXPECT_FALSE(
        transport_.submitTransferTasks(batch_, {request(), invalid}).ok());
    EXPECT_EQ(batch_->size(), 1);
    EXPECT_EQ(finish(0).s, COMPLETED);
    expectPattern(target_, 0x12);
}

TEST_F(MnnvlCompletionTest, EarlyReleaseDrainsBeforeStreamReuse) {
    delayProducer();
    ASSERT_EQ(
        cudaMemsetAsync(source_, 0x7b, kBufferLength, cudaStreamPerThread),
        cudaSuccess);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {request()}).ok());
    std::thread releaser([this] {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        release_.store(true);
    });
    const auto status = transport_.freeSubBatch(batch_);
    EXPECT_TRUE(release_.load())
        << "SubBatch release did not drain pending copies";
    releaser.join();
    EXPECT_TRUE(status.ok());
    expectPattern(target_, 0x7b);
}

TEST_F(MnnvlCompletionTest, PoolBoundsDevicesAndNeverRetainsPendingEvents) {
    auto pool = std::make_shared<MnnvlCompletionEventPool>(1);
    MnnvlCompletionEvent first, second;
    ASSERT_TRUE(pool->acquire(0, first).ok());
    ASSERT_TRUE(pool->acquire(0, second).ok());
    ASSERT_EQ(cudaEventRecord(first.event, cudaStreamPerThread), cudaSuccess);
    ASSERT_EQ(cudaEventRecord(second.event, cudaStreamPerThread), cudaSuccess);
    ASSERT_EQ(cudaEventSynchronize(second.event), cudaSuccess);
    pool->release(0, first, true);
    pool->release(0, second, true);
    EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 1);
    MnnvlCompletionEvent reused;
    ASSERT_TRUE(pool->acquire(0, reused).ok());
    EXPECT_EQ(first.event, reused.event);
    delayProducer();
    {
        MnnvlCompletion pending;
        pending.pool = pool;
        pending.device = 0;
        static_cast<MnnvlCompletionEvent&>(pending) = reused;
        ASSERT_EQ(cudaEventRecord(reused.event, cudaStreamPerThread),
                  cudaSuccess);
        pending.recorded = true;
    }
    EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 0);
    release_.store(true);
    ASSERT_EQ(cudaStreamSynchronize(cudaStreamPerThread), cudaSuccess);
    int count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&count), cudaSuccess);
    if (count > 1) {
        MnnvlCompletionEvent device_one;
        ASSERT_TRUE(pool->acquire(1, device_one).ok());
        int caller;
        ASSERT_EQ(cudaGetDevice(&caller), cudaSuccess);
        EXPECT_EQ(caller, 0);
        ASSERT_EQ(cudaSetDevice(1), cudaSuccess);
        ASSERT_EQ(cudaEventRecord(device_one.event, cudaStreamPerThread),
                  cudaSuccess);
        ASSERT_EQ(cudaEventSynchronize(device_one.event), cudaSuccess);
        ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
        pool->release(1, device_one, true);
        EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 1), 1);
        EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 0);
        MnnvlCompletionEvent device_one_reused;
        ASSERT_TRUE(pool->acquire(1, device_one_reused).ok());
        EXPECT_EQ(device_one.event, device_one_reused.event);
        ASSERT_EQ(cudaGetDevice(&caller), cudaSuccess);
        EXPECT_EQ(caller, 0);
        pool->release(1, device_one_reused, true);
    }
    auto zero = std::make_shared<MnnvlCompletionEventPool>(0);
    MnnvlCompletionEvent discarded;
    ASSERT_TRUE(zero->acquire(0, discarded).ok());
    ASSERT_EQ(cudaEventRecord(discarded.event, cudaStreamPerThread),
              cudaSuccess);
    ASSERT_EQ(cudaEventSynchronize(discarded.event), cudaSuccess);
    zero->release(0, discarded, true);
    EXPECT_EQ(MnnvlTransportTestPeer::idle(*zero, 0), 0);
}

TEST_F(MnnvlCompletionTest, DestroyedContextsDoNotConsumePoolCapacity) {
#if CUDA_VERSION < 12000
    GTEST_SKIP() << "Context lifetime detection requires CUDA 12+ headers";
#else
    CUcontext caller = nullptr;
    ASSERT_EQ(cuCtxGetCurrent(&caller), CUDA_SUCCESS);
    CUdevice device;
    ASSERT_EQ(cuDeviceGet(&device, 0), CUDA_SUCCESS);
    auto destroy_context = [caller](CUcontext context) {
        EXPECT_EQ(cuCtxDestroy(context), CUDA_SUCCESS);
        EXPECT_EQ(cuCtxSetCurrent(caller), CUDA_SUCCESS);
    };
    for (bool acquire_before_destroy : {false, true}) {
        SCOPED_TRACE(::testing::Message()
                     << "acquire_before_destroy=" << acquire_before_destroy);
        CUcontext raw = nullptr;
#if CUDA_VERSION >= 13000
        ASSERT_EQ(cuCtxCreate(&raw, nullptr, 0, device), CUDA_SUCCESS);
#else
        ASSERT_EQ(cuCtxCreate(&raw, 0, device), CUDA_SUCCESS);
#endif
        std::unique_ptr<CUctx_st, decltype(destroy_context)> context_a(
            raw, destroy_context);
        auto pool = std::make_shared<MnnvlCompletionEventPool>(1);
        {
            MnnvlCompletion cached;
            cached.pool = pool;
            cached.device = 0;
            ASSERT_TRUE(pool->acquire(0, cached).ok());
            ASSERT_EQ(cached.context, context_a.get());
            ASSERT_EQ(cudaEventRecord(cached.event, cudaStreamPerThread),
                      cudaSuccess);
            cached.recorded = true;
            ASSERT_EQ(cudaEventSynchronize(cached.event), cudaSuccess);
        }
        ASSERT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 1);
        ASSERT_EQ(cuCtxSetCurrent(caller), CUDA_SUCCESS);

        // Cover both a fresh acquisition after A dies and an event already
        // in flight when A dies. Acquisition-only pruning misses the latter.
        cudaEvent_t retained = nullptr;
        {
            MnnvlCompletion live;
            live.pool = pool;
            live.device = 0;
            if (!acquire_before_destroy) context_a.reset();
            ASSERT_TRUE(pool->acquire(0, live).ok());
            ASSERT_EQ(live.context, caller);
            ASSERT_EQ(cudaEventRecord(live.event, cudaStreamPerThread),
                      cudaSuccess);
            live.recorded = true;
            ASSERT_EQ(cudaEventSynchronize(live.event), cudaSuccess);
            if (acquire_before_destroy) context_a.reset();
            retained = live.event;
        }
        EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 1);
        CUcontext current = nullptr;
        ASSERT_EQ(cuCtxGetCurrent(&current), CUDA_SUCCESS);
        EXPECT_EQ(current, caller);
        {
            MnnvlCompletion reused;
            reused.pool = pool;
            reused.device = 0;
            ASSERT_TRUE(pool->acquire(0, reused).ok());
            EXPECT_EQ(reused.event, retained);
            EXPECT_EQ(reused.context, caller);
            // Checking the slot count distinguishes a real hit from a newly
            // allocated event that happens to reuse a destroyed event handle.
            EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 0);
            ASSERT_EQ(cudaEventRecord(reused.event, cudaStreamPerThread),
                      cudaSuccess);
            reused.recorded = true;
            ASSERT_EQ(cudaEventSynchronize(reused.event), cudaSuccess);
        }
        EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 1);
    }
#endif
}

TEST_F(MnnvlCompletionTest, OverlappingContextsNeverShareCompletionEvents) {
    CUcontext caller = nullptr;
    ASSERT_EQ(cuCtxGetCurrent(&caller), CUDA_SUCCESS);
    CUdevice device;
    ASSERT_EQ(cuDeviceGet(&device, 0), CUDA_SUCCESS);
    auto create_context = [&](CUcontext& context) {
#if CUDA_VERSION >= 13000
        return cuCtxCreate(&context, nullptr, 0, device);
#else
        return cuCtxCreate(&context, 0, device);
#endif
    };
    auto destroy_context = [caller](CUcontext context) {
        EXPECT_EQ(cuCtxDestroy(context), CUDA_SUCCESS);
        EXPECT_EQ(cuCtxSetCurrent(caller), CUDA_SUCCESS);
    };
    CUcontext raw = nullptr;
    ASSERT_EQ(create_context(raw), CUDA_SUCCESS);
    std::unique_ptr<CUctx_st, decltype(destroy_context)> context_a(
        raw, destroy_context);
    ASSERT_EQ(create_context(raw), CUDA_SUCCESS);
    std::unique_ptr<CUctx_st, decltype(destroy_context)> context_b(
        raw, destroy_context);
    ASSERT_EQ(cuCtxSetCurrent(caller), CUDA_SUCCESS);

    // Keep both contexts alive until all batches AND idle events are released.
    {
        MnnvlTransport transport;
        MnnvlTransportTestPeer::configureLocalCopies(transport, metadata_, true,
                                                     1);
        std::shared_ptr<MnnvlCompletionEventPool> pool;
        struct ContextBatch {
            MnnvlTransport& transport;
            CUcontext context;
            Transport::SubBatchRef batch{nullptr};
            void* source{nullptr};
            void* target{nullptr};
            cudaStream_t stream{nullptr};
            cudaEvent_t producer{nullptr};
            Request request;
            ~ContextBatch() {
                CUcontext saved = nullptr;
                (void)cuCtxGetCurrent(&saved);
                if (cuCtxSetCurrent(context) == CUDA_SUCCESS) {
                    if (batch) EXPECT_TRUE(transport.freeSubBatch(batch).ok());
                    if (stream)
                        EXPECT_EQ(cudaStreamDestroy(stream), cudaSuccess);
                    if (producer)
                        EXPECT_EQ(cudaEventDestroy(producer), cudaSuccess);
                    if (source) EXPECT_EQ(cudaFree(source), cudaSuccess);
                    if (target) EXPECT_EQ(cudaFree(target), cudaSuccess);
                }
                EXPECT_EQ(cuCtxSetCurrent(saved), CUDA_SUCCESS);
            }
        } a{transport, context_a.get()}, b{transport, context_b.get()};
        auto on_context = [](CUcontext context, auto action) {
            std::thread worker([&] {
                ASSERT_EQ(cuCtxSetCurrent(context), CUDA_SUCCESS);
                action();
                EXPECT_EQ(cuCtxSetCurrent(nullptr), CUDA_SUCCESS);
            });
            worker.join();
        };
        auto start = [&](ContextBatch& state) {
            ASSERT_EQ(cudaMalloc(&state.source, kBufferLength), cudaSuccess);
            ASSERT_EQ(cudaMalloc(&state.target, kBufferLength), cudaSuccess);
            ASSERT_TRUE(
                metadata_->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        auto& buffers =
                            std::get<MemorySegmentDesc>(segment.detail).buffers;
                        for (void* ptr : {state.source, state.target}) {
                            BufferDesc buffer;
                            buffer.addr = reinterpret_cast<uint64_t>(ptr);
                            buffer.length = kBufferLength;
                            buffer.location = "cuda:0";
                            buffer.permission = kLocalReadWrite;
                            buffers.push_back(buffer);
                        }
                        std::sort(buffers.begin(), buffers.end(),
                                  [](const auto& x, const auto& y) {
                                      return x.addr < y.addr;
                                  });
                        return Status::OK();
                    })
                    .ok());
            ASSERT_EQ(
                cudaStreamCreateWithFlags(&state.stream, cudaStreamNonBlocking),
                cudaSuccess);
            ASSERT_EQ(cudaEventCreateWithFlags(&state.producer,
                                               cudaEventDisableTiming),
                      cudaSuccess);
            ASSERT_TRUE(transport.allocateSubBatch(state.batch, 2).ok());
            auto* batch = static_cast<MnnvlSubBatch*>(state.batch);
            // Existing context-owned streams avoid the device-only stream
            // pool, isolating completion reuse from stream reuse limitations.
            batch->async_stream = CUDAStreamHandle(state.stream, 0, nullptr);
            batch->stream_device_id = 0;
            state.request.opcode = Request::WRITE;
            state.request.source = state.source;
            state.request.target_id = LOCAL_SEGMENT_ID;
            state.request.target_offset =
                reinterpret_cast<uint64_t>(state.target);
            state.request.length = kBufferLength;
            state.request.producer_event = state.producer;
            ASSERT_EQ(cudaMemsetAsync(state.source, 0x47, kBufferLength,
                                      state.stream),
                      cudaSuccess);
            ASSERT_EQ(cudaEventRecord(state.producer, state.stream),
                      cudaSuccess);
            ASSERT_TRUE(
                transport.submitTransferTasks(state.batch, {state.request})
                    .ok());
            ASSERT_EQ(cudaStreamSynchronize(state.stream), cudaSuccess);
            TransferStatus status;
            ASSERT_TRUE(
                transport.getTransferStatus(state.batch, 0, status).ok());
            EXPECT_EQ(status.s, COMPLETED);
        };
        on_context(a.context, [&] { start(a); });
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        auto* first = static_cast<MnnvlSubBatch*>(a.batch);
        const auto event_a = first->completion_groups[0]->event;
        pool = first->completion_groups[0]->pool;
        on_context(b.context, [&] { start(b); });
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        on_context(a.context,
                   [&] { ASSERT_TRUE(transport.freeSubBatch(a.batch).ok()); });
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 1);
        on_context(b.context, [&] {
            ASSERT_TRUE(
                transport.submitTransferTasks(b.batch, {b.request}).ok());
            auto* batch = static_cast<MnnvlSubBatch*>(b.batch);
            EXPECT_NE(batch->completion_groups.back()->event, event_a);
            EXPECT_EQ(batch->completion_groups.back()->context, b.context);
            ASSERT_EQ(cudaStreamSynchronize(b.stream), cudaSuccess);
            TransferStatus status;
            ASSERT_TRUE(transport.getTransferStatus(b.batch, 1, status).ok());
            EXPECT_EQ(status.s, COMPLETED);
            expectPattern(b.target, 0x47);
            ASSERT_TRUE(transport.freeSubBatch(b.batch).ok());
        });
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        // B's two completed events must not expand the per-device capacity.
        EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 1);
        MnnvlCompletionEvent reused;
        on_context(a.context, [&] {
            ASSERT_TRUE(pool->acquire(0, reused).ok());
            EXPECT_EQ(reused.event, event_a);
            ASSERT_EQ(cudaEventRecord(reused.event, a.stream), cudaSuccess);
            ASSERT_EQ(cudaEventSynchronize(reused.event), cudaSuccess);
        });
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        // Release from another context without relabeling the owning context.
        on_context(b.context, [&] {
            pool->release(0, reused, true);
            CUcontext current = nullptr;
            ASSERT_EQ(cuCtxGetCurrent(&current), CUDA_SUCCESS);
            EXPECT_EQ(current, b.context);
        });
    }
    // Destruction of the idle pool also restores the exact calling context.
    CUcontext after = nullptr;
    ASSERT_EQ(cuCtxGetCurrent(&after), CUDA_SUCCESS);
    EXPECT_EQ(after, caller);
}

TEST_F(MnnvlCompletionTest, StrictLocalOnlyCopiesUseDefaultEnginePolicy) {
    // Test real CUDA copies through engine routing without requiring IMEX or
    // installing a remote FABRIC transport. Cross-node tests remain separate.
    for (bool legacy : {false, true}) {
        for (bool gpu_wait : {false, true}) {
            for (bool pool : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << "legacy=" << legacy << " gpu_wait=" << gpu_wait
                             << " pool=" << pool);
                auto config = makeRegistrationEngineConfig();
                config->set("use_legacy_transport_selection", legacy);
                config->set("enable_runtime_queue", false);
                config->set("merge_requests", false);
                TransferEngineImpl engine(config);
                ASSERT_TRUE(engine.available());
                TransferEngineImplTestPeer::requireFabric(engine);
                auto metadata = TransferEngineImplTestPeer::metadata(engine);
                auto fabric = std::make_shared<MnnvlTransport>();
                MnnvlTransportTestPeer::configureLocalCopies(*fabric, metadata,
                                                             gpu_wait, 1, pool);
                engine.swapTransportForTest(MNNVL, fabric);
                MemoryOptions options;
                options.type = MNNVL;
                options.perm = kLocalReadWrite;
                ASSERT_TRUE(engine
                                .registerLocalMemory(
                                    {source_, target_},
                                    {kBufferLength, kBufferLength}, options)
                                .ok());
                auto expect_unexported = [&] {
                    auto local = metadata->segmentManager().getLocal();
                    for (auto* address : {source_, target_}) {
                        auto* buffer = local->findBuffer(
                            reinterpret_cast<uint64_t>(address), kBufferLength);
                        ASSERT_NE(buffer, nullptr);
                        EXPECT_EQ(buffer->permission, kLocalReadWrite);
                        EXPECT_TRUE(buffer->transports.empty());
                        EXPECT_TRUE(buffer->mnnvl_handle.empty());
                    }
                };
                expect_unexported();
                uint8_t pattern = 0x40;
                for (auto hint : {UNSPEC, MNNVL}) {
                    for (auto opcode : {Request::READ, Request::WRITE}) {
                        SCOPED_TRACE(::testing::Message()
                                     << "hint=" << hint
                                     << " opcode=" << opcode);
                        void* input =
                            opcode == Request::WRITE ? source_ : target_;
                        void* output =
                            opcode == Request::WRITE ? target_ : source_;
                        ASSERT_EQ(
                            cudaMemsetAsync(input, ++pattern, kBufferLength,
                                            cudaStreamPerThread),
                            cudaSuccess);
                        ASSERT_EQ(cudaMemsetAsync(output, 0, kBufferLength,
                                                  cudaStreamPerThread),
                                  cudaSuccess);
                        auto req = request(opcode);
                        req.transport_hint = hint;
                        auto batch = engine.allocateBatch(1);
                        ASSERT_NE(batch, (BatchID)0);
                        ASSERT_TRUE(engine.submitTransfer(batch, {req}).ok());
                        TransferStatus status{PENDING, 0};
                        const auto deadline = std::chrono::steady_clock::now() +
                                              std::chrono::seconds(5);
                        do {
                            ASSERT_TRUE(
                                engine.getTransferStatus(batch, 0, status)
                                    .ok());
                            if (status.s != PENDING && status.s != INITIAL)
                                break;
                            std::this_thread::yield();
                        } while (std::chrono::steady_clock::now() < deadline);
                        EXPECT_TRUE(engine.freeBatch(batch).ok());
                        ASSERT_EQ(status.s, COMPLETED)
                            << "Engine-routed local MNNVL copy failed or timed "
                               "out";
                        EXPECT_EQ(status.transferred_bytes, kBufferLength);
                        expectPattern(output, pattern);
                    }
                }
                expect_unexported();
                EXPECT_TRUE(
                    engine
                        .unregisterLocalMemory({source_, target_},
                                               {kBufferLength, kBufferLength})
                        .ok());
            }
        }
    }
}

TEST_F(MnnvlCompletionTest, StrictGpuRegistrationPreservesLocalOnlyEndpoints) {
    BufferDesc buffer;
    buffer.addr = reinterpret_cast<uint64_t>(source_);
    buffer.length = kBufferLength;
    buffer.location = "cuda:0";
    MemoryOptions options;
    options.perm = kLocalReadWrite;
    EXPECT_TRUE(transport_.addMemoryBuffer(buffer, options).ok());
    EXPECT_TRUE(buffer.mnnvl_handle.empty());
    EXPECT_TRUE(buffer.transports.empty());
    options.perm = kGlobalReadWrite;
    EXPECT_FALSE(transport_.addMemoryBuffer(buffer, options).ok());
}

#ifdef MNNVL_TEST_CUDA_WRAPPERS
#if CUDART_VERSION >= 12080 && CUDART_VERSION < 13000
TEST_F(MnnvlCompletionTest, DriverFallbackPreservesProducerEventOrdering) {
    if (!loadCudaMemcpyBatchAsync())
        GTEST_SKIP() << "Requires a CUDA runtime exposing batch memcpy";
    ASSERT_EQ(cudaMemset(source_, 0, kBufferLength), cudaSuccess);
    cudaEvent_t ready;
    ASSERT_EQ(cudaEventCreateWithFlags(&ready, cudaEventDisableTiming),
              cudaSuccess);
    delayProducer();
    ASSERT_EQ(
        cudaMemsetAsync(source_, 0x5a, kBufferLength, cudaStreamPerThread),
        cudaSuccess);
    ASSERT_EQ(cudaEventRecord(ready, cudaStreamPerThread), cudaSuccess);
    auto first = request();
    auto second = request(Request::WRITE, other_);
    first.producer_event = second.producer_event = ready;
    ScopedCopyFaults faults(cudaErrorCallRequiresNewerDriver);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {first, second}).ok());
    EXPECT_EQ(batch_copy_calls.load(), 1);
    EXPECT_EQ(async_copy_calls.load(), 2);
    EXPECT_EQ(stream_drains.load(), 0);
    EXPECT_FALSE(release_.load());
    TransferStatus pending;
    ASSERT_TRUE(transport_.getTransferStatus(batch_, 0, pending).ok());
    EXPECT_EQ(pending.s, PENDING);
    release_.store(true);
    EXPECT_EQ(finish(0).s, COMPLETED);
    EXPECT_EQ(finish(1).s, COMPLETED);
    expectPattern(target_, 0x5a);
    expectPattern(other_, 0x5a);
    EXPECT_EQ(cudaEventDestroy(ready), cudaSuccess);
}

TEST_F(MnnvlCompletionTest, BatchCopyErrorsDoNotReplayCopies) {
    if (!loadCudaMemcpyBatchAsync())
        GTEST_SKIP() << "Requires a CUDA runtime exposing batch memcpy";
    ASSERT_EQ(cudaMemset(source_, 0x5a, kBufferLength), cudaSuccess);
    ASSERT_EQ(cudaMemset(target_, 0, kBufferLength), cudaSuccess);
    ScopedCopyFaults faults(cudaErrorInvalidValue);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {request()}).ok());
    EXPECT_EQ(batch_copy_calls.load(), 1);
    EXPECT_EQ(async_copy_calls.load(), 0);
    EXPECT_EQ(stream_drains.load(), 1);
    EXPECT_EQ(finish(0).s, FAILED);
    expectPattern(target_, 0);
}

TEST_F(MnnvlCompletionTest, FailedFallbackDrainsAcceptedCopyPrefix) {
    if (!loadCudaMemcpyBatchAsync())
        GTEST_SKIP() << "Requires a CUDA runtime exposing batch memcpy";
    ASSERT_EQ(cudaMemset(source_, 0x16, kBufferLength), cudaSuccess);
    ASSERT_EQ(cudaMemset(target_, 0, kBufferLength), cudaSuccess);
    ASSERT_EQ(cudaMemset(other_, 0, kBufferLength), cudaSuccess);
    ScopedCopyFaults faults(cudaErrorCallRequiresNewerDriver, 2);
    ASSERT_TRUE(transport_
                    .submitTransferTasks(
                        batch_, {request(), request(Request::WRITE, other_)})
                    .ok());
    EXPECT_EQ(batch_copy_calls.load(), 1);
    EXPECT_EQ(async_copy_calls.load(), 2);
    EXPECT_EQ(stream_drains.load(), 1);
    EXPECT_EQ(finish(0).s, FAILED);
    EXPECT_EQ(finish(1).s, FAILED);
    expectPattern(target_, 0x16);
    expectPattern(other_, 0);
}
#endif

TEST_F(MnnvlCompletionTest, CopyAndQueryErrorsDrainBeforeTerminalFailure) {
    ASSERT_EQ(
        cudaMemsetAsync(source_, 0x16, kBufferLength, cudaStreamPerThread),
        cudaSuccess);
    fail_completion_record.store(true);
    // An injected post-copy record failure must not return a submit error.
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {request()}).ok());
    EXPECT_EQ(finish(0).s, FAILED);
    expectPattern(target_, 0x16);
    auto* batch = static_cast<MnnvlSubBatch*>(batch_);
    EXPECT_FALSE(batch->completion_groups[0]->recorded);

    ASSERT_TRUE(
        transport_
            .submitTransferTasks(batch_, {request(Request::WRITE, other_)})
            .ok());
    fail_completion_query.store(batch->completion_groups[1]->event);
    EXPECT_EQ(finish(1).s, FAILED);
    expectPattern(other_, 0x16);
    auto pool = batch->completion_groups[1]->pool;
    EXPECT_TRUE(transport_.freeSubBatch(batch_).ok());
    EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0),
              0);  // Neither error event was retained.
}
#endif

TEST_F(MnnvlCompletionTest, IndependentBatchesShareCompletedEventPool) {
    ASSERT_EQ(cudaMemset(source_, 0x35, kBufferLength), cudaSuccess);
    ASSERT_TRUE(transport_.submitTransferTasks(batch_, {request()}).ok());
    auto* first = static_cast<MnnvlSubBatch*>(batch_);
    const auto event = first->completion_groups[0]->event;
    auto pool = first->completion_groups[0]->pool;
    EXPECT_EQ(finish(0).s, COMPLETED);
    ASSERT_TRUE(transport_.freeSubBatch(batch_).ok());
    EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 1);
    ASSERT_TRUE(transport_.allocateSubBatch(batch_, 4).ok());
    ASSERT_TRUE(
        transport_
            .submitTransferTasks(batch_, {request(Request::WRITE, other_)})
            .ok());
    auto* next = static_cast<MnnvlSubBatch*>(batch_);
    EXPECT_EQ(next->completion_groups[0]->event, event);
    EXPECT_EQ(finish(0).s, COMPLETED);
    expectPattern(other_, 0x35);
}

TEST_F(MnnvlCompletionTest, ConcurrentBatchesRemainIndependent) {
    ASSERT_EQ(cudaMemset(source_, 0x53, kBufferLength), cudaSuccess);
    Transport::SubBatchRef second = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(second, 1).ok());
    auto worker = [&](Transport::SubBatchRef batch, void* target) {
        ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
        ASSERT_TRUE(
            transport_
                .submitTransferTasks(batch, {request(Request::WRITE, target)})
                .ok());
        TransferStatus status;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        do {
            ASSERT_TRUE(transport_.getTransferStatus(batch, 0, status).ok());
            if (status.s != PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        EXPECT_EQ(status.s, COMPLETED);
    };
    std::thread first_worker(worker, batch_, target_);
    std::thread second_worker(worker, second, other_);
    first_worker.join();
    second_worker.join();
    EXPECT_TRUE(transport_.freeSubBatch(second).ok());
    expectPattern(target_, 0x53);
    expectPattern(other_, 0x53);
}

TEST_F(MnnvlCompletionTest, MalformedFabricHandlesReturnValidationErrors) {
    for (const auto& handle :
         {std::string("bad"),
          std::string(2 * sizeof(CUmemFabricHandle), 'z')}) {
        ASSERT_TRUE(metadata_->segmentManager()
                        .updateLocal([&](SegmentDesc& segment) {
                            auto* buffer = segment.findBuffer(
                                reinterpret_cast<uint64_t>(target_),
                                kBufferLength);
                            buffer->permission = kGlobalReadWrite;
                            buffer->transports = {MNNVL};
                            buffer->mnnvl_handle = handle;
                            return Status::OK();
                        })
                        .ok());
        uint64_t address = reinterpret_cast<uint64_t>(target_);
        EXPECT_TRUE(MnnvlTransportTestPeer::relocate(
                        transport_, address, kBufferLength, LOCAL_SEGMENT_ID)
                        .IsInvalidArgument());
    }
}

TEST_F(MnnvlCompletionTest, StrictOwnedVmmAllocationExportsTrackedExtent) {
    int supported = 0;
    if (cuDeviceGetAttribute(&supported,
                             CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                             0) != CUDA_SUCCESS ||
        !supported)
        GTEST_SKIP() << "No CUDA FABRIC allocation support";
    MemoryOptions options;
    options.location = "cuda:0";
    void* address = nullptr;
    const auto allocation =
        transport_.allocateLocalMemory(&address, kBufferLength, options);
    if (!allocation.ok())
        GTEST_SKIP() << "FABRIC/IMEX allocation unavailable: "
                     << allocation.ToString();
    BufferDesc buffer;
    buffer.addr = reinterpret_cast<uint64_t>(address);
    buffer.length = kBufferLength;
    buffer.location = "cuda:0";
    buffer.permission = kGlobalReadWrite;
    const auto registration = transport_.addMemoryBuffer(buffer, options);
    EXPECT_TRUE(registration.ok()) << registration.ToString();
    EXPECT_FALSE(buffer.mnnvl_handle.empty());
    EXPECT_EQ(buffer.addr, reinterpret_cast<uint64_t>(address));
    EXPECT_EQ(buffer.length, kBufferLength);
    if (registration.ok() && !buffer.mnnvl_handle.empty() &&
        buffer.mnnvl_handle.front() == '{') {
        const auto envelope = json::parse(buffer.mnnvl_handle);
        EXPECT_GE(envelope.at("mapped_length").get<size_t>(), kBufferLength);
        EXPECT_EQ(envelope.at("offset").get<uint64_t>(), 0);
    }
    EXPECT_TRUE(transport_.removeMemoryBuffer(buffer).ok());
    EXPECT_TRUE(transport_.freeLocalMemory(address, kBufferLength).ok());
}

TEST_F(MnnvlCompletionTest,
       OwnedVmmSubrangesPreserveEngineRegistrationIdentity) {
    CUmemAllocationProp prop = {};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = 0;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    size_t granularity = 0;
    if (cuMemGetAllocationGranularity(&granularity, &prop,
                                      CU_MEM_ALLOC_GRANULARITY_MINIMUM) !=
        CUDA_SUCCESS)
        GTEST_SKIP() << "No CUDA POSIX-FD VMM allocation support";
    TransferEngineImpl engine(makeRegistrationEngineConfig());
    ASSERT_TRUE(engine.available());
    auto fabric = std::make_shared<MnnvlTransport>();
    auto metadata = TransferEngineImplTestPeer::metadata(engine);
    MnnvlTransportTestPeer::configureRegistration(*fabric, metadata);
    engine.swapTransportForTest(MNNVL, fabric);
    MemoryOptions options;
    options.type = MNNVL;
    options.location = "cuda:0";
    void* base = nullptr;
    const auto allocated =
        engine.allocateLocalMemory(&base, 4 * granularity, options);
    if (!allocated.ok()) GTEST_SKIP() << allocated.ToString();
    BufferDesc oversized;
    oversized.addr = reinterpret_cast<uint64_t>(base) + 64;
    oversized.length = 4 * granularity;
    oversized.location = "cuda:0";
    EXPECT_TRUE(
        fabric->addMemoryBuffer(oversized, options).IsInvalidArgument());
    EXPECT_TRUE(oversized.mnnvl_handle.empty());
    oversized.addr = UINT64_MAX - 63;
    oversized.length = 128;
    EXPECT_TRUE(
        fabric->addMemoryBuffer(oversized, options).IsInvalidArgument());
    auto* interior = static_cast<uint8_t*>(base) + granularity + 64;
    const size_t interior_length = granularity - 128;
    ASSERT_TRUE(engine
                    .registerLocalMemory({base, interior},
                                         {granularity, interior_length},
                                         options)
                    .ok());
    ASSERT_TRUE(
        engine.registerLocalMemory({base}, {granularity}, options).ok());
    auto snapshot = metadata->segmentManager().getLocal();
    ASSERT_EQ(snapshot->getMemory().buffers.size(), 2);
    auto* prefix =
        snapshot->findBuffer(reinterpret_cast<uint64_t>(base), granularity);
    ASSERT_NE(prefix, nullptr);
    EXPECT_EQ(prefix->addr, reinterpret_cast<uint64_t>(base));
    EXPECT_EQ(prefix->length, granularity);
    EXPECT_EQ(prefix->ref_count, 2);
    auto* middle = snapshot->findBuffer(reinterpret_cast<uint64_t>(interior),
                                        interior_length);
    ASSERT_NE(middle, nullptr);
    EXPECT_EQ(middle->addr, reinterpret_cast<uint64_t>(interior));
    EXPECT_EQ(middle->length, interior_length);
    const auto envelope = json::parse(middle->mnnvl_handle);
    EXPECT_EQ(envelope.at("offset").get<size_t>(), granularity + 64);
    EXPECT_EQ(envelope.at("mapped_length").get<size_t>(), 4 * granularity);
    EXPECT_EQ(
        snapshot->findBuffer(reinterpret_cast<uint64_t>(base) + granularity, 1),
        nullptr);
    EXPECT_EQ(snapshot->findBuffer(
                  reinterpret_cast<uint64_t>(base) + 3 * granularity, 1),
              nullptr);
    EXPECT_TRUE(engine.unregisterLocalMemory(base, granularity).ok());
    EXPECT_EQ(metadata->segmentManager().getLocal()->getMemory().buffers.size(),
              2);
    EXPECT_TRUE(engine.unregisterLocalMemory(base, granularity).ok());
    EXPECT_EQ(metadata->segmentManager().getLocal()->getMemory().buffers.size(),
              1);
    EXPECT_TRUE(engine.unregisterLocalMemory(interior, interior_length).ok());
    EXPECT_TRUE(
        metadata->segmentManager().getLocal()->getMemory().buffers.empty());
    EXPECT_TRUE(engine.freeLocalMemory(base).ok());
}

TEST_F(MnnvlCompletionTest, FailedHostPinRollbackPreservesEarlierRegistration) {
    std::unique_ptr<void, decltype(&std::free)> host(
        std::aligned_alloc(4096, 16384), &std::free);
    ASSERT_NE(host.get(), nullptr);
    TransferEngineImpl engine(makeRegistrationEngineConfig());
    ASSERT_TRUE(engine.available());
    auto fabric = std::make_shared<MnnvlTransport>();
    auto metadata = TransferEngineImplTestPeer::metadata(engine);
    MnnvlTransportTestPeer::configureRegistration(*fabric, metadata);
    engine.swapTransportForTest(MNNVL, fabric);
    MemoryOptions options;
    options.type = MNNVL;
    options.perm = kLocalReadWrite;
    ASSERT_TRUE(engine.registerLocalMemory({host.get()}, {8192}, options).ok());
    auto snapshot = metadata->segmentManager().getLocal();
    auto* registered =
        snapshot->findBuffer(reinterpret_cast<uint64_t>(host.get()), 8192);
    ASSERT_NE(registered, nullptr);
    auto original = *registered;
    EXPECT_TRUE(fabric->tracksLocalBuffer(original));
    EXPECT_FALSE(json(original).contains("mnnvl_host_pin"));
    auto* new_buffer = static_cast<uint8_t*>(host.get()) + 8192;
    // One new pin succeeds before a conflicting registration fails. Defensive
    // cleanup must undo only the new pin and leave the existing descriptor
    // live.
    EXPECT_FALSE(engine
                     .registerLocalMemory({new_buffer, host.get()},
                                          {4096, 4096}, options)
                     .ok());
    EXPECT_TRUE(fabric->tracksLocalBuffer(original));
    EXPECT_EQ(metadata->segmentManager().getLocal()->getMemory().buffers.size(),
              1);
    cudaPointerAttributes attributes = {};
    ASSERT_EQ(cudaPointerGetAttributes(&attributes, host.get()), cudaSuccess);
    EXPECT_EQ(attributes.type, cudaMemoryTypeHost);
    ASSERT_TRUE(engine.registerLocalMemory({new_buffer}, {4096}, options).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(new_buffer, 4096).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(host.get(), 8192).ok());
    EXPECT_FALSE(fabric->tracksLocalBuffer(original));
    // A stale snapshot must not remove a later owner's pin at the same address.
    ASSERT_TRUE(engine.registerLocalMemory({host.get()}, {8192}, options).ok());
    EXPECT_TRUE(fabric->removeMemoryBuffer(original).ok());
    ASSERT_EQ(cudaPointerGetAttributes(&attributes, host.get()), cudaSuccess);
    EXPECT_EQ(attributes.type, cudaMemoryTypeHost);
    EXPECT_TRUE(engine.unregisterLocalMemory(host.get(), 8192).ok());
}

TEST_F(MnnvlCompletionTest, MalformedExportEnvelopesRejectInvalidRanges) {
    const auto raw = std::string(2 * sizeof(CUmemFabricHandle), '0');
    for (auto envelope : {json{{"version", 2},
                               {"handle", raw},
                               {"mapped_length", 8192},
                               {"offset", 0}},
                          json{{"version", 1},
                               {"handle", raw},
                               {"mapped_length", 4096},
                               {"offset", 1}},
                          json{{"version", 1},
                               {"handle", raw},
                               {"mapped_length", 8192},
                               {"offset", -1}},
                          json{{"version", 1},
                               {"handle", raw},
                               {"mapped_length", "8192"},
                               {"offset", 0}}}) {
        ASSERT_TRUE(
            installLocalSegmentWithMnnvlHandle(*metadata_, envelope.dump(),
                                               kRemoteAddress, kBufferLength)
                .ok());
        uint64_t address = kRemoteAddress;
        EXPECT_TRUE(MnnvlTransportTestPeer::relocate(
                        transport_, address, kBufferLength, LOCAL_SEGMENT_ID)
                        .IsInvalidArgument());
    }
}

TEST_F(MnnvlCompletionTest,
       ConcurrentAppendsAndPollingAreSerializedPerSubBatch) {
    ASSERT_EQ(cudaMemset(source_, 0x24, kBufferLength), cudaSuccess);
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&, i] {
            EXPECT_EQ(cudaSetDevice(0), cudaSuccess);
            auto req = request();
            req.source = static_cast<uint8_t*>(source_) + i * 1024;
            req.target_offset += i * 1024;
            req.length = 1024;
            EXPECT_TRUE(transport_.submitTransferTasks(batch_, {req}).ok());
        });
    }
    std::thread poller([&] {
        EXPECT_EQ(cudaSetDevice(0), cudaSuccess);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (batch_->size() < 4 &&
               std::chrono::steady_clock::now() < deadline) {
            if (batch_->size() != 0) {
                TransferStatus status;
                EXPECT_TRUE(
                    transport_.getTransferStatus(batch_, 0, status).ok());
            }
            std::this_thread::yield();
        }
    });
    for (auto& thread : threads) thread.join();
    poller.join();
    EXPECT_EQ(batch_->size(), 4);
    for (int i = 0; i < 4; ++i) EXPECT_EQ(finish(i).s, COMPLETED);
    expectPattern(target_, 0x24);
}

bool hasTwoCudaDevices() {
    int device_count = 0;
    return cudaGetDeviceCount(&device_count) == cudaSuccess &&
           device_count >= 2;
}

// Drains a batch whose stream and completion event live on device 0 while
// the caller is bound to the last device, optionally through a non-primary
// context. Stream and event are created first because the stream pool
// selects devices by ordinal.
template <typename Drain>
void expectCrossDeviceDrainKeepsCallerContext(bool custom_context,
                                              Drain drain) {
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    const int caller_device = device_count - 1;
    CUDAStreamPool pool;
    MnnvlTransport transport;
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    auto* mnnvl_batch = dynamic_cast<MnnvlSubBatch*>(batch);
    ASSERT_NE(mnnvl_batch, nullptr);

    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_TRUE(pool.acquire(mnnvl_batch->async_stream, 0).ok());
    mnnvl_batch->stream_device_id = 0;
    auto stream = mnnvl_batch->async_stream.get();
    auto completion = std::make_unique<MnnvlCompletion>();
    completion->pool = std::make_shared<MnnvlCompletionEventPool>(0);
    completion->device = 0;
    ASSERT_TRUE(completion->pool->acquire(0, *completion).ok());
    auto event = completion->event;
    mnnvl_batch->completion_groups.push_back(std::move(completion));
    void* buffer = nullptr;
    ASSERT_EQ(cudaMalloc(&buffer, kBufferLength), cudaSuccess);
    ASSERT_EQ(cudaMemsetAsync(buffer, 0, kBufferLength, stream), cudaSuccess);
    ASSERT_EQ(cudaEventRecord(event, stream), cudaSuccess);
    mnnvl_batch->completion_groups.back()->recorded = true;

    CUcontext custom = nullptr;
    if (custom_context) {
        CUdevice device;
        ASSERT_EQ(cuDeviceGet(&device, caller_device), CUDA_SUCCESS);
#if CUDA_VERSION >= 13000
        ASSERT_EQ(cuCtxCreate(&custom, nullptr, 0, device), CUDA_SUCCESS);
#else
        ASSERT_EQ(cuCtxCreate(&custom, 0, device), CUDA_SUCCESS);
#endif
    } else {
        ASSERT_EQ(cudaSetDevice(caller_device), cudaSuccess);
        ASSERT_EQ(cudaFree(nullptr), cudaSuccess);
    }
    CUcontext before = nullptr;
    ASSERT_EQ(cuCtxGetCurrent(&before), CUDA_SUCCESS);
    ASSERT_NE(before, nullptr);

    auto status = drain(transport, batch);
    CUcontext after = nullptr;
    auto query = cuCtxGetCurrent(&after);
    int device_after = -1;
    auto device_query = cudaGetDevice(&device_after);

    // Clean up before asserting so a failure cannot leak the context.
    if (custom) cuCtxDestroy(custom);
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    if (batch) EXPECT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_EQ(cudaFree(buffer), cudaSuccess);

    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(query, CUDA_SUCCESS);
    EXPECT_EQ(after, before);
    ASSERT_EQ(device_query, cudaSuccess);
    EXPECT_EQ(device_after, caller_device);
}

}  // namespace

TEST(MnnvlTransportTest, QuiescePreservesCustomContextAcrossDevices) {
    if (!hasTwoCudaDevices()) GTEST_SKIP() << "Requires two CUDA devices";
    expectCrossDeviceDrainKeepsCallerContext(
        true, [](MnnvlTransport& transport, Transport::SubBatchRef&) {
            return transport.quiesce();
        });
}

// Covers drainMnnvlBatch(), ~MnnvlSubBatch() and completion event
// destruction.
TEST(MnnvlTransportTest, FreeSubBatchPreservesCustomContextAcrossDevices) {
    if (!hasTwoCudaDevices()) GTEST_SKIP() << "Requires two CUDA devices";
    expectCrossDeviceDrainKeepsCallerContext(
        true, [](MnnvlTransport& transport, Transport::SubBatchRef& batch) {
            return transport.freeSubBatch(batch);
        });
}

TEST(MnnvlTransportTest, QuiescePreservesPrimaryContextAcrossDevices) {
    if (!hasTwoCudaDevices()) GTEST_SKIP() << "Requires two CUDA devices";
    expectCrossDeviceDrainKeepsCallerContext(
        false, [](MnnvlTransport& transport, Transport::SubBatchRef&) {
            return transport.quiesce();
        });
}

}  // namespace tent
}  // namespace mooncake

int main(int argc, char** argv) {
    if (argc == 5 && !std::strcmp(argv[1], "--export-posix-fd"))
        return mooncake::tent::PeerFdExporter::runChild(
            std::atoi(argv[2]), std::atoi(argv[3]), std::atoi(argv[4]) != 0);
    int child = mooncake::tent::cuda_memory_test::childMain(argc, argv);
    if (child >= 0) return child;
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
