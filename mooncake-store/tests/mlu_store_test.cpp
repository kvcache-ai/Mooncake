#include "mlu_test_utils.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "real_client.h"
#include "mlu_test_hooks.h"
#include "test_server_helpers.h"

namespace mooncake::testing {
namespace {

// With no environment overrides, exercise a real Store master/client and
// local host-memory replicas. For remote replicas, provide a running master
// and segment provider via MC_STORE_TEST_MASTER and MC_STORE_TEST_LOCAL.
class MluStoreTest : public MluTest {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("mlu_store_test");
        FLAGS_logtostderr = 1;
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        MluTest::SetUp();
        if (HasFatalFailure() || IsSkipped()) return;
        const char* external = std::getenv("MC_STORE_TEST_MASTER");
        std::string address;
        if (external) {
            address = external;
        } else {
            ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
            address = master_.master_address();
        }
        const char* local = std::getenv("MC_STORE_TEST_LOCAL");
        const char* protocol = std::getenv("PROTOCOL");
        const char* devices = std::getenv("DEVICE_NAME");
        client_ = RealClient::create();
        ASSERT_EQ(
            client_->setup_real(local ? local : "127.0.0.1:0", "P2PHANDSHAKE",
                                external ? 0 : 64 * 1024 * 1024,
                                16 * 1024 * 1024, protocol ? protocol : "tcp",
                                devices ? devices : "", address),
            0);
    }

    void TearDown() override {
        if (client_) {
            for (const auto& key : keys_) client_->remove(key, true);
            for (auto* ptr : registered_)
                EXPECT_EQ(client_->unregister_buffer(ptr), 0);
            registered_.clear();
            client_->tearDownAll();
            client_.reset();
        }
        source_.reset();
        target_.reset();
        host_.reset();
        master_.Stop();
    }

    void Register(void* ptr, size_t size) {
        ASSERT_EQ(client_->register_buffer(ptr, size), 0);
        registered_.push_back(ptr);
    }
    void Unregister(void* ptr) {
        ASSERT_EQ(client_->unregister_buffer(ptr), 0);
        registered_.erase(
            std::remove(registered_.begin(), registered_.end(), ptr),
            registered_.end());
    }
    std::string Key() {
        static std::atomic<unsigned int> sequence{0};
        auto key = "mlu-store-" + std::to_string(getpid()) + "-" +
                   std::to_string(sequence.fetch_add(1));
        keys_.push_back(key);
        return key;
    }
    static std::vector<char> Payload(size_t size, int salt) {
        std::vector<char> data(size);
        for (size_t i = 0; i < size; ++i) data[i] = (i * 17 + salt) % 251;
        return data;
    }
    void Verify(void* ptr, const std::vector<char>& expected) {
        std::vector<char> actual(expected.size());
        ASSERT_EQ(
            cnrtMemcpy(actual.data(), ptr, actual.size(), cnrtMemcpyDevToHost),
            cnrtSuccess);
        EXPECT_EQ(actual, expected);
    }

    InProcMaster master_;
    std::shared_ptr<RealClient> client_;
    MluTestBuffer source_, target_;
    PinnedHostBuffer host_;
    std::vector<void*> registered_;
    std::vector<std::string> keys_;
};

TEST_F(MluStoreTest, BatchOffloadSurvivesSourceFreeAndReloadsIntoMlu) {
    constexpr size_t stride = 2 * 1024 * 1024;
    const std::vector<size_t> sizes{64, 4096, 65536, 1024 * 1024};
    const size_t capacity = sizes.size() * stride;
    ASSERT_EQ(source_.allocate(capacity), cnrtSuccess);
    ASSERT_NO_FATAL_FAILURE(Register(source_.data(), capacity));
    std::vector<std::string> keys;
    std::vector<void*> sources;
    std::vector<std::vector<char>> expected;
    for (size_t i = 0; i < sizes.size(); ++i) {
        keys.push_back(Key());
        sources.push_back(source_.data(i * stride + 123));
        expected.push_back(Payload(sizes[i], i + 1));
        ASSERT_EQ(cnrtMemcpy(sources.back(), expected.back().data(), sizes[i],
                             cnrtMemcpyHostToDev),
                  cnrtSuccess);
    }
    // Store's own worker threads must identify the MLU source correctly.
    auto put = client_->batch_put_from(keys, sources, sizes);
    ASSERT_EQ(put.size(), keys.size());
    for (auto result : put) ASSERT_EQ(result, 0);
    ASSERT_NO_FATAL_FAILURE(Unregister(source_.data()));
    source_.reset();

    ASSERT_EQ(target_.allocate(capacity), cnrtSuccess);
    ASSERT_NO_FATAL_FAILURE(Register(target_.data(), capacity));
    std::vector<void*> targets;
    for (size_t i = 0; i < sizes.size(); ++i)
        targets.push_back(target_.data(i * stride + 257));
    ASSERT_EQ(cnrtMemset(target_.data(), 0x5a, capacity), cnrtSuccess);
    auto get = client_->batch_get_into(keys, targets, sizes);
    ASSERT_EQ(get.size(), keys.size());
    for (size_t i = 0; i < sizes.size(); ++i) {
        ASSERT_EQ(get[i], sizes[i]);
        ASSERT_NO_FATAL_FAILURE(Verify(targets[i], expected[i]));
        char guard = 0;
        ASSERT_EQ(cnrtMemcpy(&guard, static_cast<char*>(targets[i]) - 1, 1,
                             cnrtMemcpyDevToHost),
                  cnrtSuccess);
        EXPECT_EQ(guard, 0x5a);
    }
}

TEST_F(MluStoreTest, MultiBufferTensorSlicesRoundTripOnFreshCallerThread) {
    constexpr size_t capacity = 1024 * 1024;
    ASSERT_EQ(source_.allocate(capacity), cnrtSuccess);
    ASSERT_EQ(target_.allocate(capacity), cnrtSuccess);
    ASSERT_NO_FATAL_FAILURE(Register(source_.data(), capacity));
    ASSERT_NO_FATAL_FAILURE(Register(target_.data(), capacity));
    std::vector<std::string> keys{Key(), Key()};
    const std::vector<std::vector<size_t>> sizes{{4096, 65537}, {8192, 127}};
    std::vector<std::vector<void*>> sources, targets;
    std::vector<std::vector<std::vector<char>>> expected;
    size_t offset = 123;
    for (size_t i = 0; i < sizes.size(); ++i) {
        sources.emplace_back();
        targets.emplace_back();
        expected.emplace_back();
        for (size_t size : sizes[i]) {
            sources.back().push_back(source_.data(offset));
            targets.back().push_back(target_.data(offset + 17));
            expected.back().push_back(Payload(size, offset));
            ASSERT_EQ(
                cnrtMemcpy(sources.back().back(), expected.back().back().data(),
                           size, cnrtMemcpyHostToDev),
                cnrtSuccess);
            offset += size + 256;
        }
    }
    std::thread caller([&] {
        auto put = client_->batch_put_from_multi_buffers(keys, sources, sizes);
        ASSERT_EQ(put.size(), keys.size());
        for (auto result : put) ASSERT_EQ(result, 0);
        auto get =
            client_->batch_get_into_multi_buffers(keys, targets, sizes, true);
        ASSERT_EQ(get.size(), keys.size());
        for (size_t i = 0; i < get.size(); ++i)
            ASSERT_EQ(get[i], sizes[i][0] + sizes[i][1]);
    });
    caller.join();
    for (size_t i = 0; i < targets.size(); ++i)
        for (size_t j = 0; j < targets[i].size(); ++j)
            ASSERT_NO_FATAL_FAILURE(Verify(targets[i][j], expected[i][j]));
}

TEST_F(MluStoreTest, MixedHostDeviceBatchPreservesPerKeyErrors) {
    constexpr size_t size = 4096;
    ASSERT_EQ(source_.allocate(4 * size), cnrtSuccess);
    ASSERT_EQ(target_.allocate(4 * size), cnrtSuccess);
    ASSERT_NO_FATAL_FAILURE(Register(source_.data(), 4 * size));
    ASSERT_NO_FATAL_FAILURE(Register(target_.data(), 4 * size));
    host_ = accelerator_->AllocatePinnedHost(size);
    ASSERT_NE(host_.addr, nullptr);
    ASSERT_NO_FATAL_FAILURE(Register(host_.addr, size));
    auto expected = Payload(size, 29);
    std::copy(expected.begin(), expected.end(), static_cast<char*>(host_.addr));
    ASSERT_EQ(
        cnrtMemcpy(source_.data(), expected.data(), size, cnrtMemcpyHostToDev),
        cnrtSuccess);
    const std::vector<std::string> keys{Key(), Key()};
    auto put = client_->batch_put_from(keys, {source_.data(), host_.addr},
                                       {size, size});
    EXPECT_EQ(put, std::vector<int>({0, 0}));
    ASSERT_NO_FATAL_FAILURE(Unregister(host_.addr));
    ASSERT_EQ(cnrtMemset(target_.data(), 0x5a, 4 * size), cnrtSuccess);
    auto get = client_->batch_get_into(
        {keys[0], Key(), keys[1], keys[0]},
        {target_.data(), target_.data(size), target_.data(2 * size),
         target_.data(3 * size)},
        {size, size, size, size - 1});
    ASSERT_EQ(get.size(), 4);
    EXPECT_EQ(get[0], size);
    EXPECT_LT(get[1], 0);
    EXPECT_EQ(get[2], size);
    EXPECT_LT(get[3], 0);
    ASSERT_NO_FATAL_FAILURE(Verify(target_.data(), expected));
    ASSERT_NO_FATAL_FAILURE(Verify(target_.data(2 * size), expected));
    ASSERT_NO_FATAL_FAILURE(
        Verify(target_.data(size), std::vector<char>(size, 0x5a)));
    ASSERT_NO_FATAL_FAILURE(
        Verify(target_.data(3 * size), std::vector<char>(size, 0x5a)));
}

TEST_F(MluStoreTest, StagedPutAndUpsertFamiliesReadMluSources) {
    constexpr size_t size = 65567;
    ASSERT_EQ(source_.allocate(size), cnrtSuccess);
    ASSERT_EQ(target_.allocate(size), cnrtSuccess);
    auto expected = Payload(size, 81);
    ASSERT_EQ(
        cnrtMemcpy(source_.data(), expected.data(), size, cnrtMemcpyHostToDev),
        cnrtSuccess);
    // Match the CUDA/XPU tensor PRs: caller-owned device memory is not
    // registered with Store, so writes must gather into host staging.
    const auto* ptr = static_cast<const char*>(source_.data());
    const std::span<const char> value(ptr, size);
    const std::vector<std::span<const char>> parts{{ptr, 123},
                                                   {ptr + 123, size - 123}};
    std::vector<std::string> keys;
    for (int i = 0; i < 8; ++i) keys.push_back(Key());
    ASSERT_EQ(client_->put(keys[0], value), 0);
    ASSERT_EQ(client_->upsert(keys[1], value), 0);
    ASSERT_EQ(client_->put_parts(keys[2], parts), 0);
    ASSERT_EQ(client_->upsert_parts(keys[3], parts), 0);
    ASSERT_EQ(client_->put_batch({keys[4], keys[5]}, {value, value}), 0);
    ASSERT_EQ(client_->upsert_batch({keys[6], keys[7]}, {value, value}), 0);
    source_.reset();
    for (const auto& key : keys) {
        ASSERT_EQ(client_->get_into(key, target_.data(), size), size);
        ASSERT_NO_FATAL_FAILURE(Verify(target_.data(), expected));
    }
    ASSERT_NO_FATAL_FAILURE(Register(target_.data(), size));
    auto ranges =
        client_->get_into_ranges({target_.data()}, {{keys[0]}}, {{{17, 100}}},
                                 {{{3, 301}}}, {{{31, 47}}});
    ASSERT_EQ(ranges,
              (std::vector<std::vector<std::vector<int64_t>>>{{{31, 47}}}));
    ASSERT_NO_FATAL_FAILURE(Verify(
        target_.data(17), {expected.begin() + 3, expected.begin() + 34}));
    ASSERT_NO_FATAL_FAILURE(Verify(
        target_.data(100), {expected.begin() + 301, expected.begin() + 348}));
}

TEST_F(MluStoreTest, CopyFailuresReleaseStagingAndAllowRetry) {
    constexpr size_t size = 256 * 1024;
    ASSERT_EQ(source_.allocate(size), cnrtSuccess);
    ASSERT_EQ(target_.allocate(size), cnrtSuccess);
    auto expected = Payload(size, 67);
    ASSERT_EQ(
        cnrtMemcpy(source_.data(), expected.data(), size, cnrtMemcpyHostToDev),
        cnrtSuccess);
    const auto key = Key();
    const std::span<const char> value(static_cast<char*>(source_.data()), size);
    auto failures = mlu_injected_copy_failures.load();
    {
        ScopedMluFailure failure(fail_mlu_d2h);
        // More than the 16 MiB local staging arena: a leaked handle will
        // eventually prevent the next attempt from reaching the copy.
        for (int i = 0; i < 96; ++i) EXPECT_LT(client_->put(key, value), 0);
    }
    EXPECT_EQ(mlu_injected_copy_failures.load() - failures, 96);
    auto query = client_->batch_query({key});
    ASSERT_EQ(query.size(), 1);
    EXPECT_FALSE(query[0].has_value());
    ASSERT_EQ(client_->put(key, value), 0);
    source_.reset();

    ASSERT_EQ(cnrtMemset(target_.data(), 0x5a, size), cnrtSuccess);
    failures = mlu_injected_copy_failures.load();
    {
        ScopedMluFailure failure(fail_mlu_h2d);
        for (int i = 0; i < 96; ++i)
            EXPECT_LT(client_->get_into(key, target_.data(), size), 0);
    }
    EXPECT_EQ(mlu_injected_copy_failures.load() - failures, 96);
    ASSERT_NO_FATAL_FAILURE(
        Verify(target_.data(), std::vector<char>(size, 0x5a)));
    ASSERT_EQ(client_->get_into(key, target_.data(), size), size);
    ASSERT_NO_FATAL_FAILURE(Verify(target_.data(), expected));
}

TEST_F(MluStoreTest, ConcurrentBatchesKeepKeysAndDestinationsIndependent) {
    constexpr size_t threads = 4, rounds = 8, size = 65536, stride = size + 256;
    ASSERT_EQ(source_.allocate(threads * 2 * stride), cnrtSuccess);
    ASSERT_EQ(target_.allocate(threads * 2 * stride), cnrtSuccess);
    ASSERT_NO_FATAL_FAILURE(Register(source_.data(), threads * 2 * stride));
    ASSERT_NO_FATAL_FAILURE(Register(target_.data(), threads * 2 * stride));
    std::vector<std::vector<std::string>> keys(threads);
    for (auto& worker_keys : keys)
        for (size_t i = 0; i < rounds * 2; ++i) worker_keys.push_back(Key());
    std::barrier start(threads);
    std::vector<std::thread> workers;
    for (size_t i = 0; i < threads; ++i) {
        workers.emplace_back([&, i] {
            start.arrive_and_wait();
            ASSERT_EQ(cnrtSetDevice(0), cnrtSuccess);
            std::vector<void*> sources{source_.data(2 * i * stride + 17),
                                       source_.data((2 * i + 1) * stride + 17)};
            std::vector<void*> targets{target_.data(2 * i * stride + 31),
                                       target_.data((2 * i + 1) * stride + 31)};
            for (size_t round = 0; round < rounds; ++round) {
                auto expected = Payload(size, i * rounds + round);
                for (auto* src : sources)
                    ASSERT_EQ(cnrtMemcpy(src, expected.data(), size,
                                         cnrtMemcpyHostToDev),
                              cnrtSuccess);
                const std::vector<std::string> batch{keys[i][2 * round],
                                                     keys[i][2 * round + 1]};
                ASSERT_EQ(client_->batch_put_from(batch, sources, {size, size}),
                          (std::vector<int>{0, 0}));
                ASSERT_EQ(client_->batch_get_into(batch, targets, {size, size}),
                          (std::vector<int64_t>{size, size}));
                for (auto* dst : targets)
                    ASSERT_NO_FATAL_FAILURE(Verify(dst, expected));
            }
        });
    }
    for (auto& worker : workers) worker.join();
}

}  // namespace
}  // namespace mooncake::testing
