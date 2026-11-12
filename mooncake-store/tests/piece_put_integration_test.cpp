#include "client_service.h"
#include "test_server_helpers.h"
#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <latch>
#include <cstring>
#include <random>
#include <thread>
#include <tuple>
#include <unordered_map>

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
namespace mooncake {
void tcpTransportSetSessionProgressHookForTest(
    int (*hook)(int, bool) noexcept) noexcept;
}
#endif

namespace mooncake::test {
namespace {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
// Values match SessionProgressTestEvent/Action in tcp_transport.cpp.
constexpr int kSessionWriteAckSuccessForTest = 2;
constexpr int kSessionTimeoutCommittedForTest = 3;
constexpr int kSessionCommitTimeoutBeforeProgressForTest = 1;
std::atomic<bool> ack_timeout_injected{false};
std::atomic<int> ack_timeout_commits{0};

int CommitTimeoutAtFirstWriteAck(int event, bool) noexcept {
    if (event == kSessionTimeoutCommittedForTest) {
        ack_timeout_commits.fetch_add(1, std::memory_order_relaxed);
    }
    if (event == kSessionWriteAckSuccessForTest &&
        !ack_timeout_injected.exchange(true, std::memory_order_relaxed)) {
        return kSessionCommitTimeoutBeforeProgressForTest;
    }
    return 0;
}

struct ScopedAckTimeoutRace {
    ScopedAckTimeoutRace() {
        ack_timeout_injected.store(false, std::memory_order_relaxed);
        ack_timeout_commits.store(0, std::memory_order_relaxed);
        tcpTransportSetSessionProgressHookForTest(CommitTimeoutAtFirstWriteAck);
    }
    ~ScopedAckTimeoutRace() {
        tcpTransportSetSessionProgressHookForTest(nullptr);
    }
};
#endif
std::string NewEndpoint() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr))) {
        close(fd);
        throw std::runtime_error("bind");
    }
    socklen_t size = sizeof(addr);
    getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &size);
    const auto port = ntohs(addr.sin_port);
    close(fd);
    return "127.0.0.1:" + std::to_string(port);
}

}  // namespace
class PiecePutIntegrationTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("piece_put_integration_test");
        FLAGS_logtostderr = true;
        FLAGS_minloglevel = 2;
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }
    void SetUp() override {
        InProcMasterConfig config;
        config.enable_piece_put = true;
        config.http_metadata_port = 0;
        ASSERT_TRUE(master.Start(config));
        auto make = [&] {
            const auto name = NewEndpoint();
            if (target_name.empty()) target_name = name;
            auto c = Client::Create(name, "P2PHANDSHAKE", "tcp", std::nullopt,
                                    master.master_address());
            if (!c) throw std::runtime_error("Client create");
            return *c;
        };
        target = make();
        ASSERT_TRUE(target->EnablePiecePutTarget());
        target_memory.resize(256 * 1024 * 1024);
        ASSERT_TRUE(target->MountSegment(target_memory.data(),
                                         target_memory.size(), "tcp"));
        for (size_t i = 0; i < 4; ++i) {
            producers.push_back(make());
            source[i].resize(16 * 1024 * 1024);
            std::mt19937 random(20261107 + i);
            for (auto& c : source[i]) c = static_cast<unsigned char>(random());
            ASSERT_TRUE(producers[i]->RegisterLocalMemory(
                source[i].data(), source[i].size(), "cpu:0"));
        }
        read_buffer.resize(8 * 1024 * 1024);
        ASSERT_TRUE(producers[0]->RegisterLocalMemory(
            read_buffer.data(), read_buffer.size(), "cpu:0"));
        FLAGS_minloglevel = 2;
    }
    void TearDown() override {
        if (!producers.empty())
            producers[0]->unregisterLocalMemory(read_buffer.data());
        for (size_t i = 0; i < producers.size(); ++i)
            producers[i]->unregisterLocalMemory(source[i].data());
        producers.clear();
        target.reset();  // endpoint drain precedes backing vector destruction
        master.Stop();
    }
    auto Write(size_t writer, const std::string& key, uint64_t offset,
               uint64_t length, std::string write_id = "task-1") {
        std::vector<PiecePutInput> input{
            {key, 4096, offset, std::move(write_id)}};
        std::vector<std::vector<Slice>> slices{
            {{source[writer].data(), length}}};
        return producers[writer]->BatchPiecePut(input, slices);
    }
    bool ReadAndCheck(const std::string& key, size_t object_size,
                      size_t key_index = 0) {
        std::vector<Slice> destination{{read_buffer.data(), object_size}};
        if (!producers[0]->Get(key, destination)) return false;
        const size_t piece = object_size / 4;
        for (size_t writer = 0; writer < 4; ++writer) {
            if (!std::equal(source[writer].begin() + key_index * piece,
                            source[writer].begin() + (key_index + 1) * piece,
                            read_buffer.begin() + writer * piece))
                return false;
        }
        return true;
    }
    testing::InProcMaster master;
    std::vector<unsigned char> target_memory, source[4], read_buffer;
    std::shared_ptr<Client> target;
    std::string target_name;
    std::vector<std::shared_ptr<Client>> producers;
};

TEST_F(PiecePutIntegrationTest,
       FourProducersAssembleAndWholeGetMatchesEveryByte) {
    std::vector<std::future<void>> tasks;
    for (size_t i = 0; i < 4; ++i)
        tasks.push_back(std::async(std::launch::async, [&, i] {
            auto result = Write(i, "kv-block", i * 1024, 1024);
            ASSERT_EQ(result.size(), 1);
            ASSERT_TRUE(result[0]);
        }));
    for (auto& task : tasks) task.get();
    ASSERT_TRUE(ReadAndCheck("kv-block", 4096));
}

TEST_F(PiecePutIntegrationTest,
       ConcurrentSdkWritesCrossAdmissionWindowWithoutLosingData) {
    constexpr size_t kWorkers = 4;
    constexpr size_t kBatchesPerWorker = 33;
    constexpr size_t kBatchSize = 32;
    constexpr size_t kObjectSize = 256;
    constexpr size_t kObjects = kWorkers * kBatchesPerWorker * kBatchSize;
    static_assert(kObjects > 4096);
    ASSERT_LE(kObjects * kObjectSize, source[0].size());
    auto key_for = [](size_t index) {
        return "sdk-window-" + std::to_string(index);
    };

    // A single SDK Client shares one admission window across these workers.
    // The payload stays small; each key has distinct seeded source bytes.
    std::latch ready(kWorkers), start(1);
    std::vector<std::future<void>> tasks;
    for (size_t worker = 0; worker < kWorkers; ++worker) {
        tasks.push_back(std::async(std::launch::async, [&, worker] {
            ready.count_down();
            start.wait();
            for (size_t batch = 0; batch < kBatchesPerWorker; ++batch) {
                std::vector<PiecePutInput> inputs;
                std::vector<std::vector<Slice>> slices;
                for (size_t item = 0; item < kBatchSize; ++item) {
                    const auto index =
                        (worker * kBatchesPerWorker + batch) * kBatchSize +
                        item;
                    inputs.push_back(
                        {key_for(index), kObjectSize, 0, "window-rollover"});
                    slices.push_back({{source[0].data() + index * kObjectSize,
                                       kObjectSize}});
                }
                const auto results =
                    producers[0]->BatchPiecePut(inputs, slices);
                ASSERT_EQ(results.size(), kBatchSize);
                for (size_t item = 0; item < results.size(); ++item) {
                    ASSERT_TRUE(results[item])
                        << "worker=" << worker << " batch=" << batch
                        << " item=" << item;
                    EXPECT_EQ(results[item]->round_state,
                              PiecePutRoundState::ROUND_PUBLISHED);
                    EXPECT_FALSE(results[item].recovery);
                }
            }
        }));
    }
    ready.wait();
    start.count_down();
    for (auto& task : tasks) task.get();
    if (HasFatalFailure()) return;

    for (size_t first = 0; first < kObjects; first += kBatchSize) {
        std::vector<std::string> keys;
        std::unordered_map<std::string, std::vector<Slice>> destinations;
        for (size_t item = 0; item < kBatchSize; ++item) {
            keys.push_back(key_for(first + item));
            destinations.emplace(
                keys.back(),
                std::vector<Slice>{
                    {read_buffer.data() + item * kObjectSize, kObjectSize}});
        }
        const auto read = producers[0]->BatchGet(keys, destinations);
        ASSERT_EQ(read.size(), kBatchSize);
        for (size_t item = 0; item < read.size(); ++item) {
            ASSERT_TRUE(read[item]) << keys[item];
            const auto expected =
                source[0].begin() + (first + item) * kObjectSize;
            EXPECT_TRUE(std::equal(expected, expected + kObjectSize,
                                   read_buffer.begin() + item * kObjectSize))
                << keys[item];
        }
    }
}

TEST_F(PiecePutIntegrationTest, HoleHiddenAndDifferentBatchCannotFillIt) {
    ASSERT_TRUE(Write(0, "hole", 0, 1024)[0]);
    EXPECT_FALSE(producers[0]->Query("hole"));
    EXPECT_FALSE(Write(1, "hole", 1024, 1024, "other-task")[0]);
    EXPECT_FALSE(Write(1, "hole", 512, 1024)[0]);
    for (size_t i = 1; i < 4; ++i)
        ASSERT_TRUE(Write(i, "hole", i * 1024, 1024)[0]);
    ASSERT_TRUE(ReadAndCheck("hole", 4096));
}

TEST_F(PiecePutIntegrationTest,
       BatchReturnsIndependentOutcomesAndPreservesOrdinaryIo) {
    ASSERT_TRUE(Write(0, "collision", 0, 1024)[0]);
    std::vector<PiecePutInput> inputs{{"collision", 4096, 512, "task-1"},
                                      {"complete", 4096, 0, "task-2"}};
    std::vector<std::vector<Slice>> data{{{source[1].data(), 1024}},
                                         {{source[1].data(), 4096}}};
    auto results = producers[1]->BatchPiecePut(inputs, data);
    ASSERT_EQ(results.size(), 2);
    EXPECT_FALSE(results[0]);
    ASSERT_TRUE(results[1]);
    EXPECT_EQ(results[1]->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    std::vector<Slice> ordinary{{source[2].data(), 4096}};
    ASSERT_TRUE(producers[2]->Put("ordinary", ordinary, ReplicateConfig{}));
    std::vector<Slice> dest{{read_buffer.data(), 4096}};
    ASSERT_TRUE(producers[0]->Get("ordinary", dest));
    EXPECT_TRUE(std::equal(source[2].begin(), source[2].begin() + 4096,
                           read_buffer.begin()));
}

TEST_F(PiecePutIntegrationTest, ProtectedBatchRangesKeepCapabilityAndOffsets) {
    const std::vector<std::string> keys{"range-good", "range-wrong"};
    auto grants = producers[0]->StartBatchPutForSizes(keys, {4096, 4096},
                                                      ReplicateConfig{});
    ASSERT_EQ(grants.size(), 2u);
    ASSERT_TRUE(grants[0]);
    ASSERT_TRUE(grants[1]);
    auto good = *grants[0];
    auto wrong = *grants[1];
    auto& instance = wrong[0].get_memory_descriptor().strict_tcp_instance;
    ASSERT_TRUE(instance);
    (*instance)[0] = (*instance)[0] == '0' ? '1' : '0';
    std::vector<std::vector<Slice>> data{
        {{source[0].data() + 2048, 2048}, {source[0].data(), 2048}},
        {{source[1].data(), 4096}}};
    auto written = producers[0]->BatchTransferWriteRanges({good, wrong}, data,
                                                          {{2048, 0}, {0}});
    ASSERT_EQ(written.size(), 2u);
    // Failure of one verified endpoint can make the aggregate fail; neither
    // entry is published until its own matching completion request.
    EXPECT_FALSE(written[1]);
    EXPECT_FALSE(producers[0]->Query(keys[1]));
    auto repaired =
        producers[0]->BatchTransferWriteRanges({good}, {data[0]}, {{2048, 0}});
    ASSERT_TRUE(repaired[0]);
    EXPECT_EQ(*repaired[0], 4096);
    ObjectMeta meta{keys[0], std::nullopt};
    meta.strict_write_proof = *MakeStrictWriteProof(good);
    auto ended = producers[0]->BatchPutEnd({meta}, ReplicaType::MEMORY);
    ASSERT_TRUE(ended[0]);
    std::vector<Slice> destination{{read_buffer.data(), 4096}};
    ASSERT_TRUE(producers[0]->Get(keys[0], destination));
    EXPECT_TRUE(std::equal(source[0].begin(), source[0].begin() + 4096,
                           read_buffer.begin()));
    ASSERT_TRUE(
        producers[0]->BatchPutRevoke({keys[1]}, ReplicaType::MEMORY)[0]);
}

TEST_F(PiecePutIntegrationTest,
       ProtectedOrdinaryPutUpsertCopyMoveAndBatchPreserveData) {
    std::vector<char> copy_memory(4 * 1024 * 1024),
        move_memory(4 * 1024 * 1024);
    auto make_target = [&](std::vector<char>& backing,
                           const std::string& name) {
        auto c = Client::Create(name, "P2PHANDSHAKE", "tcp", std::nullopt,
                                master.master_address());
        if (!c) throw std::runtime_error("target create");
        if (!(*c)->EnablePiecePutTarget() ||
            !(*c)->MountSegment(backing.data(), backing.size(), "tcp"))
            throw std::runtime_error("target mount");
        return *c;
    };
    const auto copy_name = NewEndpoint(), move_name = NewEndpoint();
    auto copy_target = make_target(copy_memory, copy_name);
    auto move_target = make_target(move_memory, move_name);
    ReplicateConfig config;
    config.preferred_segments = {target_name};
    std::vector<Slice> data{{source[0].data(), 4096}};
    ASSERT_TRUE(producers[0]->Put("ordinary-complete", data, config));
    std::vector<Slice> replacement{{source[1].data(), 4096}};
    ASSERT_TRUE(producers[0]->Upsert("ordinary-complete", replacement, config));
    auto await_task = [&](const UUID& id) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        do {
            auto task = producers[0]->QueryTask(id);
            if (!task || task->status == TaskStatus::FAILED) return false;
            if (task->status == TaskStatus::SUCCESS) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    };
    auto copy = producers[0]->CreateCopyTask("ordinary-complete", {copy_name});
    ASSERT_TRUE(copy);
    ASSERT_TRUE(await_task(*copy));
    auto move = producers[0]->CreateMoveTask("ordinary-complete", target_name,
                                             move_name);
    ASSERT_TRUE(move);
    ASSERT_TRUE(await_task(*move));
    std::vector<Slice> destination{{read_buffer.data(), 4096}};
    ASSERT_TRUE(producers[0]->Get("ordinary-complete", destination));
    EXPECT_TRUE(std::equal(source[1].begin(), source[1].begin() + 4096,
                           read_buffer.begin()));
    std::vector<ObjectKey> keys{"ordinary-batch-a", "ordinary-batch-b"};
    std::vector<std::vector<Slice>> batch{{{source[2].data(), 4096}},
                                          {{source[3].data(), 4096}}};
    for (const auto& result : producers[0]->BatchPut(keys, batch, config))
        ASSERT_TRUE(result);
    for (const auto& result : producers[0]->BatchUpsert(keys, batch, config))
        ASSERT_TRUE(result);
    // Locals close before backing vectors, including on an ASSERT return.
}

TEST_F(PiecePutIntegrationTest,
       LegacyMountHistoryCannotUpgradeTheSameEndpoint) {
    std::vector<char> backing(4 * 1024 * 1024);
    auto created = Client::Create(NewEndpoint(), "P2PHANDSHAKE", "tcp",
                                  std::nullopt, master.master_address());
    ASSERT_TRUE(created);
    auto client = *created;
    ASSERT_TRUE(client->MountSegment(backing.data(), backing.size(), "tcp"));
    ASSERT_TRUE(client->UnmountSegment(backing.data(), backing.size()));

    // An empty current mount table does not revoke a delayed legacy address.
    const auto upgraded = client->EnablePiecePutTarget();
    ASSERT_FALSE(upgraded);
    EXPECT_EQ(upgraded.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    // Rejection leaves the ordinary endpoint usable and does not retire it.
    ASSERT_TRUE(client->MountSegment(backing.data(), backing.size(), "tcp"));
    ASSERT_TRUE(client->UnmountSegment(backing.data(), backing.size()));

    // A fresh endpoint may opt in before publishing the same backing region.
    auto fresh = Client::Create(NewEndpoint(), "P2PHANDSHAKE", "tcp",
                                std::nullopt, master.master_address());
    ASSERT_TRUE(fresh);
    ASSERT_TRUE((*fresh)->EnablePiecePutTarget());
    EXPECT_TRUE((*fresh)->MountSegment(backing.data(), backing.size(), "tcp"));
}

TEST_F(PiecePutIntegrationTest, SharedEngineRetainsDirectRegistrationHistory) {
    std::vector<char> backing(4 * 1024 * 1024);
    auto engine = std::make_shared<TransferEngine>(false);
    const auto endpoint = NewEndpoint();
    ASSERT_EQ(engine->init("P2PHANDSHAKE", endpoint), 0);
    ASSERT_NE(engine->installTransport("tcp", nullptr), nullptr);
    // Classic TCP publishes this region even when remote_accessible is false.
    ASSERT_EQ(engine->registerLocalMemory(backing.data(), backing.size(),
                                          "cpu:0", false, true),
              0);
    ASSERT_EQ(engine->unregisterLocalMemory(backing.data()), 0);
    auto created = Client::Create(endpoint, "P2PHANDSHAKE", "tcp", std::nullopt,
                                  master.master_address(), engine);
    ASSERT_TRUE(created);
    EXPECT_FALSE((*created)->EnablePiecePutTarget());
    EXPECT_FALSE(engine->isTcpRetired());
}

TEST_F(PiecePutIntegrationTest,
       BoundSharedEngineRejectsLegacyMountBeforeExposure) {
    std::vector<char> backing(4 * 1024 * 1024);
    auto engine = std::make_shared<TransferEngine>(false);
    const auto endpoint = NewEndpoint();
    ASSERT_EQ(engine->init("P2PHANDSHAKE", endpoint), 0);
    ASSERT_NE(engine->installTransport("tcp", nullptr), nullptr);
    auto protected_client =
        Client::Create(endpoint, "P2PHANDSHAKE", "tcp", std::nullopt,
                       master.master_address(), engine);
    ASSERT_TRUE(protected_client);
    ASSERT_TRUE((*protected_client)->EnablePiecePutTarget());
    // Another Client sees no protected segment in Master yet. The TE binding
    // must reject its ordinary mount before local registration or the RPC.
    auto legacy_client =
        Client::Create(endpoint, "P2PHANDSHAKE", "tcp", std::nullopt,
                       master.master_address(), engine);
    ASSERT_TRUE(legacy_client);
    const auto mounted =
        (*legacy_client)->MountSegment(backing.data(), backing.size(), "tcp");
    ASSERT_FALSE(mounted);
    EXPECT_EQ(mounted.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_TRUE((*protected_client)
                    ->MountSegment(backing.data(), backing.size(), "tcp"));
}

TEST_F(PiecePutIntegrationTest, RetiredEndpointCannotAcceptNewPieceWrites) {
    ASSERT_TRUE(Write(0, "partial", 0, 1024)[0]);
    ASSERT_TRUE(target->RetirePiecePutEndpoint());
    auto result = Write(1, "partial", 1024, 1024);
    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result[0]);
    EXPECT_FALSE(producers[0]->Query("partial"));
}

TEST_F(PiecePutIntegrationTest, InvalidBatchInputDoesNotCancelHealthyItem) {
    std::vector<PiecePutInput> inputs{{"", 4096, 0, "task-1"},
                                      {"valid-input", 4096, 0, "task-2"}};
    std::vector<std::vector<Slice>> data{{}, {{source[1].data(), 4096}}};
    auto results = producers[1]->BatchPiecePut(inputs, data);
    ASSERT_EQ(results.size(), 2);
    EXPECT_FALSE(results[0]);
    ASSERT_TRUE(results[1]);
    EXPECT_EQ(results[1]->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    std::vector<Slice> dest{{read_buffer.data(), 4096}};
    ASSERT_TRUE(producers[0]->Get("valid-input", dest));
    EXPECT_TRUE(std::equal(source[1].begin(), source[1].begin() + 4096,
                           read_buffer.begin()));
}

TEST_F(PiecePutIntegrationTest,
       NewMasterCannotRemountOrRetargetTheOldSharedEngine) {
    std::vector<char> backing(4 * 1024 * 1024);
    auto engine = std::make_shared<TransferEngine>(false);
    const auto endpoint = NewEndpoint();
    ASSERT_EQ(engine->init("P2PHANDSHAKE", endpoint), 0);
    ASSERT_NE(engine->installTransport("tcp", nullptr), nullptr);
    auto created = Client::Create(endpoint, "P2PHANDSHAKE", "tcp", std::nullopt,
                                  master.master_address(), engine);
    ASSERT_TRUE(created);
    auto other = *created;
    ASSERT_TRUE(other->EnablePiecePutTarget());
    ASSERT_TRUE(other->MountSegment(backing.data(), backing.size(), "tcp"));
    const auto port = master.rpc_port();
    master.Stop();
    InProcMasterConfig next;
    next.rpc_port = port;
    next.http_metadata_port = 0;
    next.enable_piece_put = true;
    ASSERT_TRUE(master.Start(next));
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!engine->isTcpRetired() &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(engine->isTcpRetired());
    // A second Client cannot bind an old live listener to the new Master.
    EXPECT_FALSE(engine->bindTcpMasterInstance("different-master"));
    auto result = Write(0, "after-restart", 0, 4096, "new-task");
    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result[0]);
    other.reset();
    created.reset();
    engine.reset();
}
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
TEST_F(PiecePutIntegrationTest, AckTimeoutRaceRetiresRoundUntilEndpointDrain) {
    const std::string key = "ack-timeout-race";
    {
        ScopedAckTimeoutRace race;
        // Real TCP applies the complete 4096-byte payload and delivers its
        // ACK. The hook deterministically commits the already-queued client
        // progress timeout before accepting that ACK; it does not drop packets
        // or replay a write against a stale/released address.
        const auto result = Write(0, key, 0, 4096, "timed-out-round");
        ASSERT_EQ(result.size(), 1u);
        ASSERT_TRUE(ack_timeout_injected.load(std::memory_order_relaxed));
        ASSERT_EQ(ack_timeout_commits.load(std::memory_order_relaxed), 1);
        ASSERT_FALSE(result[0]);
        EXPECT_EQ(result[0].error(), ErrorCode::TRANSFER_FAIL);
    }
    EXPECT_FALSE(producers[0]->Query(key));
    const auto blocked = Write(1, key, 0, 4096, "replacement-round");
    ASSERT_EQ(blocked.size(), 1u);
    EXPECT_FALSE(blocked[0]);

    // The failed session did not retire the target endpoint. Its backing and
    // every registered source buffer stay alive throughout these operations.
    std::vector<Slice> healthy{{source[1].data(), 4096}};
    ASSERT_TRUE(
        producers[1]->Put("after-ack-timeout", healthy, ReplicateConfig{}));
    std::vector<Slice> destination{{read_buffer.data(), 4096}};
    ASSERT_TRUE(producers[0]->Get("after-ack-timeout", destination));
    EXPECT_TRUE(std::equal(source[1].begin(), source[1].begin() + 4096,
                           read_buffer.begin()));

    ASSERT_TRUE(target->RetirePiecePutEndpoint());
    // A retained UNCERTAIN record rejects the different write_id. Once the
    // drain sweep erases it, Start reaches allocation and finds no live target.
    // No replacement TCP write is submitted during this reclamation check.
    bool reclaimed = false;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        const auto next = Write(2, key, 0, 4096, "replacement-round");
        ASSERT_EQ(next.size(), 1u);
        ASSERT_FALSE(next[0]);
        if (next[0].error() == ErrorCode::NO_AVAILABLE_HANDLE) {
            reclaimed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_TRUE(reclaimed);
}
#endif

}  // namespace mooncake::test
// Server-only black-box RPC fault tests. The proxy forwards each request to
// the real Master and reads its real reply before deliberately dropping it.
#include "rpc_service.h"
#include <poll.h>
#include <unordered_set>

namespace mooncake::test {
namespace {
class ReplyDroppingProxy {
   public:
    explicit ReplyDroppingProxy(const std::string& backend) {
        backend_.sin_family = AF_INET;
        auto sep = backend.rfind(':');
        backend_.sin_port = htons(std::stoi(backend.substr(sep + 1)));
        if (inet_pton(AF_INET, backend.substr(0, sep).c_str(),
                      &backend_.sin_addr) != 1)
            throw std::runtime_error("backend address");
        listener_ = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (listener_ < 0 ||
            bind(listener_, reinterpret_cast<sockaddr*>(&a), sizeof(a)) ||
            listen(listener_, 64))
            throw std::runtime_error("proxy listen");
        socklen_t len = sizeof(a);
        getsockname(listener_, reinterpret_cast<sockaddr*>(&a), &len);
        address_ = "127.0.0.1:" + std::to_string(ntohs(a.sin_port));
        acceptor_ = std::thread([this] {
            while (!stop_.load()) {
                pollfd p{listener_, POLLIN, 0};
                if (poll(&p, 1, 100) <= 0) continue;
                int peer = accept(listener_, nullptr, nullptr);
                if (peer < 0) continue;
                int remote = socket(AF_INET, SOCK_STREAM, 0);
                if (remote < 0 ||
                    connect(remote, reinterpret_cast<sockaddr*>(&backend_),
                            sizeof(backend_))) {
                    if (remote >= 0) close(remote);
                    close(peer);
                    continue;
                }
                {
                    std::lock_guard l(mutex_);
                    sockets_.insert(peer);
                    sockets_.insert(remote);
                }
                workers_.emplace_back(
                    [this, peer, remote] { Serve(peer, remote); });
            }
        });
    }
    ~ReplyDroppingProxy() {
        stop_.store(true);
        acceptor_.join();
        close(listener_);
        {
            std::lock_guard l(mutex_);
            for (int fd : sockets_) shutdown(fd, SHUT_RDWR);
        }
        for (auto& t : workers_) t.join();
    }
    const std::string& address() const { return address_; }
    void Arm(uint32_t function, int count) {
        function_.store(function);
        remaining_.store(count);
        dropped_.store(0);
    }
    int dropped() const { return dropped_.load(); }
    size_t piece_put_requests() const { return piece_put_requests_.load(); }
    void InvalidateNextMountMasterInstance() { invalidate_mount_.store(true); }
    int invalidated_mounts() const { return invalidated_mounts_.load(); }
    void ReplayFirstDroppedRequest() {
        std::vector<char> bytes;
        {
            std::lock_guard l(mutex_);
            bytes = first_request_;
        }
        ASSERT_FALSE(bytes.empty());
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(connect(fd, reinterpret_cast<sockaddr*>(&backend_),
                          sizeof(backend_)),
                  0);
        ASSERT_TRUE(Send(fd, bytes.data(), bytes.size()));
        Response h{};
        ASSERT_TRUE(Receive(fd, &h, sizeof(h)));
        std::vector<char> body(h.length + h.attach_length);
        EXPECT_TRUE(Receive(fd, body.data(), body.size()));
        close(fd);
    }

   private:
    struct Request {
        uint8_t magic, version, serializer, type;
        uint32_t sequence, function, length, attach_length;
    };
    struct Response {
        uint8_t magic, version, error, type;
        uint32_t sequence, length, attach_length;
    };
    static_assert(sizeof(Request) == 20 && sizeof(Response) == 16);
    static bool Receive(int fd, void* data, size_t size) {
        auto p = static_cast<char*>(data);
        while (size) {
            auto n = recv(fd, p, size, 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            p += n;
            size -= n;
        }
        return true;
    }
    static bool Send(int fd, const void* data, size_t size) {
        auto p = static_cast<const char*>(data);
        while (size) {
            auto n = send(fd, p, size, MSG_NOSIGNAL);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            p += n;
            size -= n;
        }
        return true;
    }
    void Serve(int peer, int remote) {
        for (;;) {
            Request h{};
            if (!Receive(peer, &h, sizeof(h))) break;
            if (h.length > 8 * 1024 * 1024 || h.attach_length > 8 * 1024 * 1024)
                break;
            std::vector<char> request(sizeof(h) + h.length + h.attach_length);
            std::memcpy(request.data(), &h, sizeof(h));
            if (!Receive(peer, request.data() + sizeof(h),
                         request.size() - sizeof(h)))
                break;
            if (h.function ==
                    coro_rpc::func_id<
                        &WrappedMasterService::GetPiecePutAdmission>() ||
                h.function ==
                    coro_rpc::func_id<
                        &WrappedMasterService::BatchPiecePutStart>() ||
                h.function == coro_rpc::func_id<
                                  &WrappedMasterService::BatchPiecePutFinish>())
                piece_put_requests_.fetch_add(1);
            if (h.function ==
                    coro_rpc::func_id<
                        &WrappedMasterService::MountSegmentWithOutcome>() &&
                invalidate_mount_.exchange(false)) {
                if (h.attach_length != 0) break;
                auto args = struct_pack::deserialize<std::tuple<Segment, UUID>>(
                    std::string_view(request.data() + sizeof(h), h.length));
                if (!args) break;
                // Ask the real Master to reject before publication. Do not
                // forge a rejection reply for a request that Master may
                // actually have mounted.
                std::get<0>(*args).piece_put_master_instance = UUID{};
                auto body = struct_pack::serialize(*args);
                h.length = body.size();
                request.resize(sizeof(h) + body.size());
                std::memcpy(request.data(), &h, sizeof(h));
                std::memcpy(request.data() + sizeof(h), body.data(),
                            body.size());
                invalidated_mounts_.fetch_add(1);
            }
            if (!Send(remote, request.data(), request.size())) break;
            Response r{};
            if (!Receive(remote, &r, sizeof(r))) break;
            if (r.length > 8 * 1024 * 1024 || r.attach_length > 8 * 1024 * 1024)
                break;
            std::vector<char> response(sizeof(r) + r.length + r.attach_length);
            std::memcpy(response.data(), &r, sizeof(r));
            if (!Receive(remote, response.data() + sizeof(r),
                         response.size() - sizeof(r)))
                break;
            int remaining = remaining_.load();
            bool drop = false;
            if (h.function == function_.load()) {
                while (remaining > 0)
                    if (remaining_.compare_exchange_weak(remaining,
                                                         remaining - 1)) {
                        drop = true;
                        break;
                    }
            }
            if (drop) {
                {
                    std::lock_guard l(mutex_);
                    if (first_request_.empty()) first_request_ = request;
                }
                dropped_.fetch_add(1);
                break;
            }
            if (!Send(peer, response.data(), response.size())) break;
        }
        {
            std::lock_guard l(mutex_);
            sockets_.erase(peer);
            sockets_.erase(remote);
            close(peer);
            close(remote);
        }
    }
    sockaddr_in backend_{};
    int listener_ = -1;
    std::string address_;
    std::atomic<bool> stop_{false};
    std::atomic<uint32_t> function_{0};
    std::atomic<int> remaining_{0}, dropped_{0};
    std::atomic<size_t> piece_put_requests_{0};
    std::atomic<bool> invalidate_mount_{false};
    std::atomic<int> invalidated_mounts_{0};
    std::thread acceptor_;
    std::mutex mutex_;
    std::unordered_set<int> sockets_;
    std::vector<std::thread> workers_;
    std::vector<char> first_request_;
};
}  // namespace
class SdkRecoveryTest : public PiecePutIntegrationTest {
   protected:
    void SetUp() override {
        PiecePutIntegrationTest::SetUp();
        proxy = std::make_unique<ReplyDroppingProxy>(master.master_address());
        auto c = Client::Create(NewEndpoint(), "P2PHANDSHAKE", "tcp",
                                std::nullopt, proxy->address());
        ASSERT_TRUE(c);
        relayed = *c;
        ASSERT_TRUE(relayed->RegisterLocalMemory(source[0].data(),
                                                 source[0].size(), "cpu:0"));
    }
    void TearDown() override {
        if (relayed) relayed->unregisterLocalMemory(source[0].data());
        relayed.reset();
        proxy.reset();
        PiecePutIntegrationTest::TearDown();
    }
    void RecreateRelayedAsTarget() {
        ASSERT_TRUE(relayed->unregisterLocalMemory(source[0].data()));
        relayed.reset();
        auto c = Client::Create(NewEndpoint(), "P2PHANDSHAKE", "tcp",
                                std::nullopt, proxy->address());
        ASSERT_TRUE(c);
        relayed = *c;
        // A target must opt in before exposing even its source buffers.
        ASSERT_TRUE(relayed->EnablePiecePutTarget());
        ASSERT_TRUE(relayed->RegisterLocalMemory(source[0].data(),
                                                 source[0].size(), "cpu:0"));
    }
    auto Put(const std::string& key, uint64_t offset = 0,
             uint64_t length = 1024, std::string id = "task-1") {
        std::vector<PiecePutInput> in{{key, 4096, offset, std::move(id)}};
        std::vector<std::vector<Slice>> data{{{source[0].data(), length}}};
        return relayed->BatchPiecePut(in, data);
    }
    std::unique_ptr<ReplyDroppingProxy> proxy;
    std::shared_ptr<Client> relayed;
};
TEST_F(SdkRecoveryTest, BatchLimitRejectsLocallyAndAcceptsFullSizeBatch) {
    constexpr size_t kObjectSize = 256;
    std::vector<std::vector<Slice>> empty;
    EXPECT_TRUE(relayed->BatchPiecePut({}, empty).empty());
    EXPECT_TRUE(relayed->RetryPiecePut({}).empty());

    std::vector<PiecePutInput> mismatched{
        {"shape-first", kObjectSize, 0, "shape-task"},
        {"shape-second", kObjectSize, 0, "shape-task"}};
    std::vector<std::vector<Slice>> short_data{
        {{source[0].data(), kObjectSize}}};
    const auto shape_results = relayed->BatchPiecePut(mismatched, short_data);
    ASSERT_EQ(shape_results.size(), mismatched.size());
    for (const auto& result : shape_results) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_FALSE(result.recovery);
    }

    for (size_t count : {Client::kMaxPiecePutBatchSize + 1,
                         Client::kMaxPiecePutBatchSize * 2}) {
        std::vector<PiecePutInput> inputs;
        std::vector<std::vector<Slice>> data;
        for (size_t i = 0; i < count; ++i) {
            inputs.push_back({"oversized-" + std::to_string(i), kObjectSize, 0,
                              "batch-limit"});
            data.push_back({{source[0].data() + i * kObjectSize, kObjectSize}});
        }
        const auto rejected = relayed->BatchPiecePut(inputs, data);
        ASSERT_EQ(rejected.size(), count);
        for (const auto& result : rejected) {
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            EXPECT_FALSE(result.recovery);
        }
    }
    // Even admission was skipped: no metadata or data work began.
    EXPECT_EQ(proxy->piece_put_requests(), 0u);

    std::vector<PiecePutInput> inputs;
    std::vector<std::vector<Slice>> data;
    std::vector<std::string> keys;
    std::unordered_map<std::string, std::vector<Slice>> destinations;
    for (size_t i = 0; i < Client::kMaxPiecePutBatchSize; ++i) {
        keys.push_back("full-batch-" + std::to_string(i));
        inputs.push_back({keys.back(), kObjectSize, 0, "batch-limit"});
        data.push_back({{source[0].data() + i * kObjectSize, kObjectSize}});
        destinations.emplace(
            keys.back(),
            std::vector<Slice>{
                {read_buffer.data() + i * kObjectSize, kObjectSize}});
    }
    const auto written = relayed->BatchPiecePut(inputs, data);
    ASSERT_EQ(written.size(), Client::kMaxPiecePutBatchSize);
    for (const auto& result : written) {
        ASSERT_TRUE(result);
        EXPECT_EQ(result->round_state, PiecePutRoundState::ROUND_PUBLISHED);
        EXPECT_FALSE(result.recovery);
    }
    EXPECT_GT(proxy->piece_put_requests(), 0u);
    const auto read = producers[0]->BatchGet(keys, destinations);
    ASSERT_EQ(read.size(), keys.size());
    for (const auto& result : read) ASSERT_TRUE(result);
    EXPECT_TRUE(std::equal(source[0].begin(),
                           source[0].begin() + keys.size() * kObjectSize,
                           read_buffer.begin()));
}

TEST_F(SdkRecoveryTest, OversizedRetryPreservesHandlesWithoutSendingRpc) {
    proxy->Arm(coro_rpc::func_id<&WrappedMasterService::BatchPiecePutFinish>(),
               1);
    const auto failed = Put("retry-limit");
    ASSERT_EQ(failed.size(), 1u);
    ASSERT_FALSE(failed[0]);
    ASSERT_TRUE(failed[0].recovery);
    auto handle = *failed[0].recovery;
    const auto requests_before = proxy->piece_put_requests();
    for (size_t count : {Client::kMaxPiecePutBatchSize + 1,
                         Client::kMaxPiecePutBatchSize * 2}) {
        const auto rejected = relayed->RetryPiecePut(
            std::vector<PiecePutRecovery>(count, handle));
        ASSERT_EQ(rejected.size(), count);
        for (const auto& result : rejected) {
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            ASSERT_TRUE(result.recovery);
        }
        handle = *rejected.front().recovery;
    }
    EXPECT_EQ(proxy->piece_put_requests(), requests_before);

    // Handles returned by the local rejection still settle the original write.
    const auto settled = relayed->RetryPiecePut(
        std::vector<PiecePutRecovery>(Client::kMaxPiecePutBatchSize, handle));
    ASSERT_EQ(settled.size(), Client::kMaxPiecePutBatchSize);
    for (const auto& result : settled) {
        ASSERT_TRUE(result);
        EXPECT_EQ(result->round_state, PiecePutRoundState::OPEN);
        EXPECT_FALSE(result.recovery);
    }
    for (size_t writer = 1; writer < 4; ++writer)
        ASSERT_TRUE(Write(writer, "retry-limit", writer * 1024, 1024)[0]);
    ASSERT_TRUE(ReadAndCheck("retry-limit", 4096));
}

TEST_F(SdkRecoveryTest,
       LostAdmissionReplyAllowsNextWriteWithoutRecoveryHandle) {
    proxy->Arm(coro_rpc::func_id<&WrappedMasterService::GetPiecePutAdmission>(),
               1);
    const auto failed = Put("lost-admission", 0, 4096);
    ASSERT_EQ(failed.size(), 1u);
    ASSERT_FALSE(failed[0]);
    EXPECT_EQ(proxy->dropped(), 1);
    EXPECT_FALSE(failed[0].recovery);
    EXPECT_FALSE(producers[0]->Query("lost-admission"));

    // Master created a window, but no Start or physical write was issued.
    // Repeating admission with the last observed id must recover that window.
    const auto retried = Put("lost-admission", 0, 4096);
    ASSERT_EQ(retried.size(), 1u);
    ASSERT_TRUE(retried[0]);
    EXPECT_EQ(retried[0]->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    EXPECT_FALSE(retried[0].recovery);
    std::vector<Slice> destination{{read_buffer.data(), 4096}};
    ASSERT_TRUE(producers[0]->Get("lost-admission", destination));
    EXPECT_TRUE(std::equal(source[0].begin(), source[0].begin() + 4096,
                           read_buffer.begin()));

    // A delayed duplicate of the lost admission request must not rotate away
    // the recovered window and prevent the same SDK Client's next operation.
    proxy->ReplayFirstDroppedRequest();
    const auto next = Put("after-lost-admission", 0, 4096);
    ASSERT_EQ(next.size(), 1u);
    ASSERT_TRUE(next[0]);
    ASSERT_TRUE(producers[0]->Get("after-lost-admission", destination));
    EXPECT_TRUE(std::equal(source[0].begin(), source[0].begin() + 4096,
                           read_buffer.begin()));
}
TEST_F(SdkRecoveryTest,
       LostRejectedStartAndCloseRepliesDoNotLeavePermanentRecovery) {
    const std::string key = "rejected-cohort";
    ASSERT_TRUE(Write(0, key, 0, 1024)[0]);
    proxy->Arm(coro_rpc::func_id<&WrappedMasterService::BatchPiecePutStart>(),
               2);
    const auto rejected = Put(key, 1024, 1024, "different-task");
    ASSERT_EQ(rejected.size(), 1u);
    ASSERT_FALSE(rejected[0]);
    ASSERT_EQ(proxy->dropped(), 2);
    ASSERT_TRUE(rejected[0].recovery);

    // Neither the rejected Start nor its lost close response issued an address.
    // Recovery must finish without changing the other producers' live round.
    const auto closed = relayed->RetryPiecePut({*rejected[0].recovery});
    ASSERT_EQ(closed.size(), 1u);
    EXPECT_FALSE(closed[0]);
    EXPECT_FALSE(closed[0].recovery);
    EXPECT_FALSE(producers[0]->Query(key));
    proxy->ReplayFirstDroppedRequest();
    for (size_t writer = 1; writer < 4; ++writer)
        ASSERT_TRUE(Write(writer, key, writer * 1024, 1024)[0]);
    ASSERT_TRUE(ReadAndCheck(key, 4096));
}
TEST_F(SdkRecoveryTest,
       LostStartAndCloseRepliesRemainRecoverableAndLateStartCannotRevive) {
    proxy->Arm(coro_rpc::func_id<&WrappedMasterService::BatchPiecePutStart>(),
               2);
    auto failed = Put("lost-start");
    ASSERT_EQ(failed.size(), 1u);
    ASSERT_FALSE(failed[0]);
    ASSERT_EQ(proxy->dropped(), 2);
    ASSERT_TRUE(failed[0].recovery);
    auto settled = relayed->RetryPiecePut({*failed[0].recovery});
    ASSERT_EQ(settled.size(), 1u);
    EXPECT_FALSE(settled[0]);
    EXPECT_FALSE(settled[0].recovery);
    auto fresh = Put("lost-start", 0, 4096, "new-task");
    ASSERT_TRUE(fresh[0]);
    proxy->ReplayFirstDroppedRequest();
    std::vector<Slice> dest{{read_buffer.data(), 4096}};
    ASSERT_TRUE(producers[0]->Get("lost-start", dest));
    EXPECT_TRUE(std::equal(source[0].begin(), source[0].begin() + 4096,
                           read_buffer.begin()));
}
TEST_F(SdkRecoveryTest,
       LostFinishReplyRetriesOriginalTokenWithoutResendingData) {
    proxy->Arm(coro_rpc::func_id<&WrappedMasterService::BatchPiecePutFinish>(),
               1);
    auto failed = Put("lost-finish");
    ASSERT_FALSE(failed[0]);
    ASSERT_TRUE(failed[0].recovery);
    ASSERT_EQ(proxy->dropped(), 1);
    auto handle = *failed[0].recovery;
    auto settled = relayed->RetryPiecePut({handle, handle});
    ASSERT_EQ(settled.size(), 2u);
    for (auto& x : settled) {
        ASSERT_TRUE(x);
        EXPECT_FALSE(x.recovery);
    }
    for (size_t i = 1; i < 4; ++i)
        ASSERT_TRUE(Write(i, "lost-finish", i * 1024, 1024)[0]);
    ASSERT_TRUE(ReadAndCheck("lost-finish", 4096));
}
TEST_F(SdkRecoveryTest,
       LostBatchFinishRetainsAllIndicesAndWrongClientCannotRecover) {
    std::vector<PiecePutInput> in{{"batch-first", 4096, 0, "task-1"},
                                  {"batch-second", 4096, 0, "task-2"}};
    std::vector<std::vector<Slice>> data{{{source[0].data(), 1024}},
                                         {{source[0].data(), 4096}}};
    proxy->Arm(coro_rpc::func_id<&WrappedMasterService::BatchPiecePutFinish>(),
               1);
    auto failed = relayed->BatchPiecePut(in, data);
    ASSERT_EQ(failed.size(), 2u);
    for (auto& x : failed) {
        ASSERT_FALSE(x);
        ASSERT_TRUE(x.recovery);
    }
    auto wrong = producers[1]->RetryPiecePut({*failed[0].recovery});
    ASSERT_EQ(wrong.size(), 1u);
    EXPECT_FALSE(wrong[0]);
    EXPECT_FALSE(wrong[0].recovery);
    auto settled =
        relayed->RetryPiecePut({*failed[1].recovery, *failed[0].recovery});
    ASSERT_EQ(settled.size(), 2u);
    ASSERT_TRUE(settled[0]);
    ASSERT_TRUE(settled[1]);
    EXPECT_EQ(settled[0]->round_state, PiecePutRoundState::ROUND_PUBLISHED);
    EXPECT_EQ(settled[1]->round_state, PiecePutRoundState::OPEN);
}
TEST_F(SdkRecoveryTest, RejectedMountKeepsHealthyEndpointAndExistingRound) {
    std::vector<char> first(4 * 1024 * 1024), second(4 * 1024 * 1024);
    ASSERT_NO_FATAL_FAILURE(RecreateRelayedAsTarget());
    ASSERT_TRUE(relayed->MountSegment(first.data(), first.size(), "tcp"));
    // Allocate the open round only on the endpoint whose next mount fails.
    ASSERT_TRUE(target->RetirePiecePutEndpoint());
    ASSERT_TRUE(Write(0, "mount-rejected", 0, 1024)[0]);
    proxy->InvalidateNextMountMasterInstance();
    auto rejected = relayed->MountSegment(second.data(), second.size(), "tcp");
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), ErrorCode::INVALID_WRITE);
    ASSERT_EQ(proxy->invalidated_mounts(), 1);
    auto cleanup = relayed->unregisterLocalMemory(second.data());
    ASSERT_FALSE(cleanup);
    EXPECT_EQ(cleanup.error(), ErrorCode::NOT_REGISTERED);
    for (size_t i = 1; i < 4; ++i)
        ASSERT_TRUE(Write(i, "mount-rejected", i * 1024, 1024)[0]);
    ASSERT_TRUE(ReadAndCheck("mount-rejected", 4096));
    // The same region can be mounted after its explicit rejection; healthy
    // pre-existing registrations and the TCP instance remain usable.
    ASSERT_TRUE(relayed->MountSegment(second.data(), second.size(), "tcp"));
    relayed->unregisterLocalMemory(source[0].data());
    relayed.reset();
}

TEST_F(SdkRecoveryTest, LostMountReplyRetiresAllSegmentsAndRejectsLaterMount) {
    std::vector<char> first(4 * 1024 * 1024), second(4 * 1024 * 1024),
        third(4 * 1024 * 1024);
    ASSERT_NO_FATAL_FAILURE(RecreateRelayedAsTarget());
    ASSERT_TRUE(relayed->MountSegment(first.data(), first.size(), "tcp"));
    proxy->Arm(
        coro_rpc::func_id<&WrappedMasterService::MountSegmentWithOutcome>(), 1);
    EXPECT_FALSE(relayed->MountSegment(second.data(), second.size(), "tcp"));
    ASSERT_EQ(proxy->dropped(), 1);
    EXPECT_FALSE(relayed->MountSegment(third.data(), third.size(), "tcp"));
    EXPECT_TRUE(relayed->RetirePiecePutEndpoint());
    // Retired engine and all pending target addresses drain before backing
    // dies.
    relayed->unregisterLocalMemory(source[0].data());
    relayed.reset();
}
}  // namespace mooncake::test
