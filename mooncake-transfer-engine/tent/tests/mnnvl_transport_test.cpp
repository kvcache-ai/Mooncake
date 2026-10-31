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
#include <glog/logging.h>

#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/config.h"
#include "tent/metrics/tent_metrics.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/mnnvl/mnnvl_transport.h"

#ifdef MNNVL_TEST_CUDA_WRAPPERS
namespace {
std::atomic<bool> fail_completion_record{false};
std::atomic<cudaEvent_t> fail_completion_query{nullptr};
}  // namespace
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
    static Status relocate(MnnvlTransport& transport, uint64_t& address,
                           uint64_t length, SegmentID target_id) {
        return transport.relocateSharedMemoryAddress(address, length,
                                                     target_id);
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
    }
    static void configureRegistration(
        MnnvlTransport& transport, std::shared_ptr<ControlService> metadata) {
        configureLocalCopies(transport, std::move(metadata), false);
        transport.options_.require_fabric = false;
        transport.handle_type_ = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
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

        pid_t child = fork();
        if (child < 0) return;
        if (child == 0) {
            close(ready_pipe_[0]);
            close(stop_pipe_[1]);
            _exit(childMain(ready_pipe_[1], stop_pipe_[0], subrange));
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
    cudaEvent_t first, second;
    ASSERT_TRUE(pool->acquire(0, first).ok());
    ASSERT_TRUE(pool->acquire(0, second).ok());
    ASSERT_EQ(cudaEventRecord(first, cudaStreamPerThread), cudaSuccess);
    ASSERT_EQ(cudaEventRecord(second, cudaStreamPerThread), cudaSuccess);
    ASSERT_EQ(cudaEventSynchronize(second), cudaSuccess);
    pool->release(0, first, true);
    pool->release(0, second, true);
    EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 1);
    cudaEvent_t reused;
    ASSERT_TRUE(pool->acquire(0, reused).ok());
    EXPECT_EQ(first, reused);
    delayProducer();
    {
        MnnvlCompletion pending;
        pending.pool = pool;
        pending.device = 0;
        pending.event = reused;
        ASSERT_EQ(cudaEventRecord(reused, cudaStreamPerThread), cudaSuccess);
        pending.recorded = true;
    }
    EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 0);
    release_.store(true);
    ASSERT_EQ(cudaStreamSynchronize(cudaStreamPerThread), cudaSuccess);
    int count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&count), cudaSuccess);
    if (count > 1) {
        cudaEvent_t device_one;
        ASSERT_TRUE(pool->acquire(1, device_one).ok());
        int caller;
        ASSERT_EQ(cudaGetDevice(&caller), cudaSuccess);
        EXPECT_EQ(caller, 0);
        ASSERT_EQ(cudaSetDevice(1), cudaSuccess);
        ASSERT_EQ(cudaEventRecord(device_one, cudaStreamPerThread),
                  cudaSuccess);
        ASSERT_EQ(cudaEventSynchronize(device_one), cudaSuccess);
        ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
        pool->release(1, device_one, true);
        EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 1), 1);
        EXPECT_EQ(MnnvlTransportTestPeer::idle(*pool, 0), 0);
        cudaEvent_t device_one_reused;
        ASSERT_TRUE(pool->acquire(1, device_one_reused).ok());
        EXPECT_EQ(device_one, device_one_reused);
        ASSERT_EQ(cudaGetDevice(&caller), cudaSuccess);
        EXPECT_EQ(caller, 0);
        pool->release(1, device_one_reused, true);
    }
    auto zero = std::make_shared<MnnvlCompletionEventPool>(0);
    cudaEvent_t discarded;
    ASSERT_TRUE(zero->acquire(0, discarded).ok());
    ASSERT_EQ(cudaEventRecord(discarded, cudaStreamPerThread), cudaSuccess);
    ASSERT_EQ(cudaEventSynchronize(discarded), cudaSuccess);
    zero->release(0, discarded, true);
    EXPECT_EQ(MnnvlTransportTestPeer::idle(*zero, 0), 0);
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

}  // namespace
}  // namespace tent
}  // namespace mooncake
