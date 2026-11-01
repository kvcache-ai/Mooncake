#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/network.h"
#include "real_client.h"
#include "test_server_helpers.h"

namespace mooncake {
namespace {

class ScopedEnv {
   public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        if (const char* old = std::getenv(name)) old_ = old;
        setenv(name, value, 1);
    }
    ~ScopedEnv() {
        if (old_) {
            setenv(name_.c_str(), old_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> old_;
};

#ifdef MOONCAKE_TEST_WRAP_UNREGISTER
std::atomic<void*> fail_unregister{nullptr};
std::atomic<int> unregister_failures{0};

using UnregisterResult = tl::expected<void, ErrorCode>;
extern "C" UnregisterResult RealUnregister(Client*, void*, bool) asm(
    "__real__ZN8mooncake6Client21unregisterLocalMemoryEPvb");
extern "C" UnregisterResult WrapUnregister(Client*, void*, bool) asm(
    "__wrap__ZN8mooncake6Client21unregisterLocalMemoryEPvb");
extern "C" UnregisterResult WrapUnregister(Client* client, void* addr,
                                           bool update_metadata) {
    if (addr == fail_unregister.load()) {
        ++unregister_failures;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return RealUnregister(client, addr, update_metadata);
}
#endif

class OffloadTeardownTest : public ::testing::Test {
   protected:
    static constexpr size_t kOffloadSize = 16 * 1024 * 1024;
    static constexpr size_t kSegmentSize = 32 * 1024 * 1024;

    void SetUp() override {
        char path[] = "/tmp/mooncake-offload-teardown-XXXXXX";
        ASSERT_NE(mkdtemp(path), nullptr);
        path_ = path;
        ASSERT_TRUE(master_.Start(
            InProcMasterConfigBuilder().set_enable_offload(true).build()));
        engine_ = std::make_shared<TransferEngine>(false);
        endpoint_ = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        ASSERT_EQ(engine_->init("P2PHANDSHAKE", endpoint_), 0);
        ASSERT_NE(engine_->installTransport("tcp", nullptr), nullptr);
        client_ = RealClient::create();
    }

    void SetupClient(bool offload) {
        ASSERT_TRUE(client_->setup_internal(
            endpoint_, "P2PHANDSHAKE", kSegmentSize, 0, "tcp", "",
            master_.master_address(), engine_, "", getFreeTcpPort(), offload,
            true, path_));
    }

    void TearDown() override {
#ifdef MOONCAKE_TEST_WRAP_UNREGISTER
        fail_unregister = nullptr;
        unregister_failures = 0;
#endif
        if (client_) {
            client_->tearDownAll();
            // Also clean up on the unfixed baseline after a failed assertion.
            client_->file_storage_.reset();
            client_.reset();
        }
        engine_.reset();
        master_.Stop();
        std::filesystem::remove_all(path_);
    }

    std::vector<SegmentBufferInfo> Buffers() {
        std::vector<SegmentBufferInfo> result;
        const auto segment = engine_->openSegment(engine_->getLocalIpAndPort());
        EXPECT_EQ(engine_->getSegmentBuffers(segment, result), 0);
        return result;
    }

#ifdef MOONCAKE_TEST_WRAP_UNREGISTER
    void ExpectMapped(uintptr_t addr) {
        const auto page_size = static_cast<uintptr_t>(sysconf(_SC_PAGESIZE));
        unsigned char resident = 0;
        EXPECT_EQ(mincore(reinterpret_cast<void*>(addr - addr % page_size),
                          page_size, &resident),
                  0);
    }
#endif

    ScopedEnv buffer_size_{"MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES",
                           "16777216"};
    ScopedEnv uring_{"MOONCAKE_OFFLOAD_USE_URING", "0"};
    ScopedEnv heartbeat_{"MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "1"};
    ScopedEnv gc_{"MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS", "1"};
    ScopedEnv hot_cache_{"MC_STORE_LOCAL_HOT_CACHE_SIZE", "0"};
    testing::InProcMaster master_;
    std::shared_ptr<TransferEngine> engine_;
    std::shared_ptr<RealClient> client_;
    std::string endpoint_;
    std::string path_;
};

TEST_F(OffloadTeardownTest, WithoutOffloadReleasesSegment) {
    ASSERT_NO_FATAL_FAILURE(SetupClient(false));
    std::weak_ptr<Client> owner = client_->client_;
    ASSERT_EQ(Buffers().size(), 1);
    EXPECT_EQ(client_->tearDownAll(), 0);
    EXPECT_TRUE(owner.expired());
    EXPECT_TRUE(Buffers().empty());
    EXPECT_EQ(client_->tearDownAll(), 0);
}

TEST_F(OffloadTeardownTest, OffloadReleasesOwnersAndRegistrations) {
    ASSERT_NO_FATAL_FAILURE(SetupClient(true));
    std::weak_ptr<Client> owner = client_->client_;
    ASSERT_EQ(Buffers().size(), 2);
    EXPECT_EQ(client_->tearDownAll(), 0);
    EXPECT_EQ(client_->file_storage_, nullptr);
    EXPECT_TRUE(owner.expired());
    EXPECT_TRUE(Buffers().empty());
    EXPECT_EQ(client_->tearDownAll(), 0);
    EXPECT_TRUE(Buffers().empty());
}

TEST_F(OffloadTeardownTest, DestructionReleasesOffloadRegistrations) {
    ASSERT_NO_FATAL_FAILURE(SetupClient(true));
    std::weak_ptr<Client> owner = client_->client_;
    client_.reset();
    EXPECT_TRUE(owner.expired());
    EXPECT_TRUE(Buffers().empty());
}

#ifdef MOONCAKE_TEST_WRAP_UNREGISTER
TEST_F(OffloadTeardownTest, FailedUnregisterKeepsMemoryAndRetries) {
    ASSERT_NO_FATAL_FAILURE(SetupClient(true));
    const auto before = Buffers();
    ASSERT_EQ(before.size(), 2);
    for (const auto& buffer : before) {
        if (buffer.length == kOffloadSize) {
            fail_unregister = reinterpret_cast<void*>(buffer.addr);
        }
    }
    ASSERT_NE(fail_unregister.load(), nullptr);
    std::weak_ptr<Client> owner = client_->client_;
    EXPECT_NE(client_->tearDownAll(), 0);
    EXPECT_GT(unregister_failures.load(), 0);
    EXPECT_NE(client_->file_storage_, nullptr);
    EXPECT_FALSE(owner.expired());
    EXPECT_EQ(Buffers().size(), before.size());
    for (const auto& buffer : before) ExpectMapped(buffer.addr);
    EXPECT_NE(client_->tearDownAll(), 0);
    fail_unregister = nullptr;
    EXPECT_EQ(client_->tearDownAll(), 0);
    EXPECT_TRUE(owner.expired());
    EXPECT_TRUE(Buffers().empty());
    EXPECT_EQ(client_->tearDownAll(), 0);
}

TEST_F(OffloadTeardownTest, DestructorRetainsBufferIfUnregisterFails) {
    ASSERT_NO_FATAL_FAILURE(SetupClient(true));
    for (const auto& buffer : Buffers()) {
        if (buffer.length == kOffloadSize) {
            fail_unregister = reinterpret_cast<void*>(buffer.addr);
        }
    }
    ASSERT_NE(fail_unregister.load(), nullptr);
    const auto offload_addr =
        reinterpret_cast<uintptr_t>(fail_unregister.load());
    std::weak_ptr<Client> owner = client_->client_;
    client_.reset();
    EXPECT_GT(unregister_failures.load(), 0);
    EXPECT_TRUE(owner.expired());
    const auto remaining = Buffers();
    ASSERT_EQ(remaining.size(), 1);
    EXPECT_EQ(remaining.front().addr, offload_addr);
    ExpectMapped(offload_addr);
    // The destructor deliberately retains this allocation until process exit.
    fail_unregister = nullptr;
    EXPECT_EQ(
        engine_->unregisterLocalMemory(reinterpret_cast<void*>(offload_addr)),
        0);
}
#endif

}  // namespace
}  // namespace mooncake
