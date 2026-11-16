#include "real_client.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <thread>

#include "device/accelerator_registry.h"
#include "tent/platform/cpu.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#include "test_server_helpers.h"

namespace mooncake {
namespace {
std::array<char, 4096> source;
std::atomic<size_t> staged_bytes{0};

bool isDevice(const void* ptr) {
    auto address = reinterpret_cast<uintptr_t>(ptr);
    auto base = reinterpret_cast<uintptr_t>(source.data());
    return address >= base && address - base < source.size();
}

// CPU-backed CUDA range: exercise Store's actual admission, staging and
// registration paths without a CUDA driver or an RDMA device.
class TestDevice : public device::AcceleratorDevice {
   public:
    device::AcceleratorVendor Vendor() const override {
        return device::AcceleratorVendor::kNvidia;
    }
    bool Available(bool = false) const override { return true; }
    device::PointerInfo QueryPointer(const void* ptr) const override {
        return isDevice(ptr)
                   ? device::PointerInfo{device::MemoryKind::kDevice, 0}
                   : device::PointerInfo{device::MemoryKind::kHost, -1};
    }
    int32_t CurrentDeviceId() const override { return 0; }
    void SetContext(int32_t) const override {}
    bool Copy(void* dst, const void* src, size_t size,
              device::CopyDirection) const override {
        if (isDevice(src)) staged_bytes += size;
        std::memcpy(dst, src, size);
        return true;
    }
    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        return {std::malloc(size), size, std::free};
    }
};
TestDevice test_device;
}  // namespace

namespace tent {
class TestPlatform : public CpuPlatform {
   public:
    TestPlatform() : CpuPlatform(std::make_shared<Config>()) {}
    MemoryType getMemoryType(void* ptr) override {
        return isDevice(ptr) ? MTYPE_CUDA : MTYPE_CPU;
    }
    const std::vector<RangeLocation> getLocation(void* ptr, size_t size,
                                                 bool = false) override {
        return {{reinterpret_cast<uint64_t>(ptr), size,
                 isDevice(ptr) ? "cuda:0" : kWildcardLocation}};
    }
};
Platform& wrappedPlatformLoader(std::shared_ptr<Config>) asm(
    "__wrap_" TENT_TEST_PLATFORM_LOADER);
Platform& wrappedPlatformLoader(std::shared_ptr<Config>) {
    static TestPlatform platform;
    return platform;
}
}  // namespace tent

class TransferEngineImplTestPeer {
   public:
    static void attach(TransferEngine& engine,
                       std::shared_ptr<tent::TransferEngine> backend,
                       std::shared_ptr<tent::Transport> rdma,
                       std::shared_ptr<tent::Transport> nvlink) {
        backend->impl_->swapTransportForTest(tent::RDMA, std::move(rdma));
        backend->impl_->swapTransportForTest(tent::NVLINK, std::move(nvlink));
        engine.impl_tent_ = std::move(backend);
        engine.use_tent_ = true;
    }
};

namespace {
using namespace std::chrono_literals;

class TestTransport : public tent::Transport {
   public:
    explicit TestTransport(tent::TransportType type) : type_(type) {
        // NVLINK only provides registration; the peer offers an RDMA route.
        if (type == tent::RDMA) {
            caps.dram_to_dram = caps.gpu_to_dram = caps.dram_to_gpu = true;
        }
    }
    struct Batch : SubBatch {
        std::vector<tent::Request> requests;
        bool copied = false;
        size_t size() const override { return requests.size(); }
    };
    bool fail_device_registration = false;
    std::atomic<bool> device_registered{false};
    bool timeout = false;
    std::atomic<bool> release{false};
    std::atomic<int> timeout_polls{0};
    std::atomic<int> late_copies{0};
    std::atomic<int> source_unregistrations{0};
    std::atomic<int> premature_frees{0};

    tent::Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new Batch;
        return tent::Status::OK();
    }
    tent::Status freeSubBatch(SubBatchRef& batch) override {
        auto* b = static_cast<Batch*>(batch);
        if (timeout && !b->copied) ++premature_frees;
        delete b;
        batch = nullptr;
        return tent::Status::OK();
    }
    void copy(Batch& batch) {
        if (batch.copied) return;
        for (const auto& req : batch.requests) {
            auto* target = reinterpret_cast<void*>(req.target_offset);
            if (req.opcode == tent::Request::WRITE)
                std::memcpy(target, req.source, req.length);
            else
                std::memcpy(req.source, target, req.length);
        }
        batch.copied = true;
    }
    tent::Status submitTransferTasks(
        SubBatchRef batch,
        const std::vector<tent::Request>& requests) override {
        for (const auto& req : requests) {
            if (type_ == tent::RDMA && isDevice(req.source) &&
                !device_registered.load())
                return tent::Status::InternalError(
                    "source has no RDMA registration");
        }
        auto& b = *static_cast<Batch*>(batch);
        b.copied = false;
        b.requests.insert(b.requests.end(), requests.begin(), requests.end());
        if (!timeout) copy(b);
        return tent::Status::OK();
    }
    tent::Status getTransferStatus(SubBatchRef batch, int id,
                                   tent::TransferStatus& status) override {
        if (timeout) ++timeout_polls;
        status = {
            timeout ? tent::TIMEOUT : tent::COMPLETED,
            timeout ? 0 : static_cast<Batch*>(batch)->requests[id].length};
        return tent::Status::OK();
    }
    tent::Status isSubBatchDrained(SubBatchRef batch, bool& drained) override {
        drained = !timeout || release.load();
        if (drained && timeout && !static_cast<Batch*>(batch)->copied) {
            copy(*static_cast<Batch*>(batch));
            ++late_copies;
        }
        return tent::Status::OK();
    }
    tent::Status addMemoryBuffer(tent::BufferDesc& desc,
                                 const tent::MemoryOptions&) override {
        if (fail_device_registration &&
            isDevice(reinterpret_cast<void*>(desc.addr)))
            return tent::Status::InternalError("injected RDMA MR failure");
        if (isDevice(reinterpret_cast<void*>(desc.addr)))
            device_registered = true;
        desc.transports.push_back(type_);
        return tent::Status::OK();
    }
    tent::Status removeMemoryBuffer(tent::BufferDesc& desc) override {
        if (isDevice(reinterpret_cast<void*>(desc.addr))) {
            ++source_unregistrations;
            device_registered = false;
        }
        return tent::Status::OK();
    }

   private:
    tent::TransportType type_;
};

class DirectWriteTentTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        device::GetAcceleratorRegistry();
        static const device::AcceleratorDeviceRegistrar registered(test_device);
    }
    std::shared_ptr<TransferEngine> makeEngine(
        std::shared_ptr<TestTransport> rdma) {
        auto config = std::make_shared<tent::Config>();
        config->set("metadata_type", "p2p");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("rpc_server_port", "0");
        config->set("merge_requests", false);
        config->set("enable_auto_failover_on_poll", false);
        for (const auto* name :
             {"tcp", "rdma", "shm", "io_uring", "nvlink", "mnnvl", "gds", "mlu",
              "xpu", "ub", "ascend_direct", "memfabric"})
            config->set(std::string("transports/") + name + "/enable", false);
        auto backend = std::make_shared<tent::TransferEngine>(config);
        EXPECT_TRUE(backend->available());
        auto engine = std::make_shared<TransferEngine>(false);
        TransferEngineImplTestPeer::attach(
            *engine, backend, std::move(rdma),
            std::make_shared<TestTransport>(tent::NVLINK));
        return engine;
    }
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        rdma_ = std::make_shared<TestTransport>(tent::RDMA);
        engine_ = makeEngine(rdma_);
        provider_engine_ =
            makeEngine(std::make_shared<TestTransport>(tent::RDMA));
        provider_ = RealClient::create();
        client_ = RealClient::create();
        ASSERT_EQ(provider_->setup_real(
                      "127.0.0.1:0", "P2PHANDSHAKE", 16 << 20, 1 << 20, "rdma",
                      "", master_.master_address(), provider_engine_),
                  0);
        ASSERT_EQ(
            client_->setup_real("127.0.0.1:0", "P2PHANDSHAKE", 0, 1 << 20,
                                "rdma", "", master_.master_address(), engine_),
            0);
        source.fill('x');
        staged_bytes = 0;
    }
    void TearDown() override {
        if (client_) client_->tearDownAll();
        if (provider_) provider_->tearDownAll();
        client_.reset();
        provider_.reset();
        engine_.reset();
        provider_engine_.reset();
        master_.Stop();
    }
    std::vector<int> write(bool upsert) {
        return upsert ? client_->batch_upsert_from_multi_buffers(
                            {"direct-test"}, {{source.data()}},
                            {{source.size()}}, {}, true)
                      : client_->batch_put_from_multi_buffers(
                            {"direct-test"}, {{source.data()}},
                            {{source.size()}}, {}, true);
    }
    testing::InProcMaster master_;
    std::shared_ptr<TestTransport> rdma_;
    std::shared_ptr<TransferEngine> engine_, provider_engine_;
    std::shared_ptr<RealClient> client_, provider_;
};

TEST_F(DirectWriteTentTest,
       TimeoutKeepsPutUpsertAndUnregisterBlockedUntilDrain) {
    for (bool upsert : {false, true}) {
        ASSERT_EQ(client_->register_buffer(source.data(), source.size()), 0);
        rdma_->timeout = true;
        rdma_->release = false;
        rdma_->timeout_polls = 0;
        const auto before = rdma_->source_unregistrations.load();
        auto write_result =
            std::async(std::launch::async, [&] { return write(upsert); });
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (!rdma_->timeout_polls.load() &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
        EXPECT_GT(rdma_->timeout_polls.load(), 0);
        std::promise<void> started;
        auto unregistered = std::async(std::launch::async, [&] {
            started.set_value();
            return client_->unregister_buffer(source.data());
        });
        started.get_future().wait();
        EXPECT_EQ(write_result.wait_for(20ms), std::future_status::timeout);
        EXPECT_EQ(unregistered.wait_for(20ms), std::future_status::timeout);
        EXPECT_EQ(rdma_->source_unregistrations.load(), before);
        EXPECT_EQ(staged_bytes.load(), 0u);
        rdma_->release = true;
        EXPECT_NE(write_result.get(), std::vector<int>{0});
        EXPECT_EQ(unregistered.get(), 0);
        EXPECT_EQ(rdma_->premature_frees.load(), 0);
    }
    EXPECT_EQ(rdma_->late_copies.load(), 2);
}

TEST_F(DirectWriteTentTest, PartialCudaRegistrationFallsBackForPutAndUpsert) {
    rdma_->fail_device_registration = true;
    // NVLINK succeeds, so UNSPEC registration succeeds despite the RDMA error.
    ASSERT_EQ(client_->register_buffer(source.data(), source.size()), 0);
    for (bool upsert : {false, true}) {
        source.fill(upsert ? 'y' : 'x');
        const auto before = staged_bytes.load();
        ASSERT_EQ(write(upsert), std::vector<int>{0});
        EXPECT_EQ(staged_bytes.load() - before, source.size());
        std::array<char, source.size()> actual{};
        ASSERT_EQ(client_->register_buffer(actual.data(), actual.size()), 0);
        EXPECT_EQ(
            client_->get_into("direct-test", actual.data(), actual.size()),
            actual.size());
        EXPECT_EQ(client_->unregister_buffer(actual.data()), 0);
        EXPECT_EQ(actual, source);
    }
    EXPECT_EQ(client_->unregister_buffer(source.data()), 0);
}
}  // namespace
}  // namespace mooncake
