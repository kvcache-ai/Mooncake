#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "real_client.h"
#include "test_server_helpers.h"
#include "transfer_engine_impl.h"
#include "transport/shm_transport/shm_transport.h"

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    // The Store capability check queries the engine mode and installed names;
    // neither a verbs device nor a TENT backend is needed for that query.
    static std::shared_ptr<TransferEngine> makeEngine(bool use_tent,
                                                      const char *name) {
        auto engine = std::make_shared<TransferEngine>(false);
        engine->use_tent_ = use_tent;
        if (!use_tent) {
            engine->impl_ = std::make_shared<TransferEngineImpl>();
            auto &impl = *engine->impl_;
            impl.multi_transports_ = std::make_shared<MultiTransport>(
                nullptr, impl.local_server_name_);
            impl.multi_transports_->transport_map_[name] =
                std::make_shared<ShmTransport>();
        }
        return engine;
    }
};
}  // namespace mooncake

namespace mooncake::test {
namespace {

struct ShutdownProbe {
    std::promise<void> client_destructor_started;
    std::atomic<bool> worker_saw_mapping{false};
};

class TeardownClient : public Client {
   public:
    explicit TeardownClient(std::shared_ptr<ShutdownProbe> probe)
        : Client("localhost", "", "tcp"), probe_(std::move(probe)) {}

    ~TeardownClient() override {
        probe_->client_destructor_started.set_value();
    }

   private:
    std::shared_ptr<ShutdownProbe> probe_;
};

bool IsMapped(void *address) {
    unsigned char residency = 0;
    return mincore(address, static_cast<size_t>(sysconf(_SC_PAGESIZE)),
                   &residency) == 0;
}

struct SessionMaster {
    static constexpr const char *kInflight = "inflight_put";
    static constexpr const char *kFlex = "flexible_put";
    static constexpr const char *kNofOnly = "nof_only_put";
    static constexpr const char *kStarting = "starting_session";
    static constexpr const char *kDisk = "disk_session";
    static constexpr const char *kFailedDisk = "failed_disk_session";
    static constexpr const char *kReplaced = "replaced_session";
    static constexpr const char *kShortLease = "short_write_grant";
    static constexpr const char *kMissingLease = "missing_write_grant";
    static constexpr const char *kGeneration = "generation";
    static constexpr const char *kGenerationPeer = "generation_peer";
    const UUID first_incarnation = generate_uuid();
    const UUID next_incarnation = generate_uuid();
    std::atomic<bool> restarted{false};
    std::promise<void> put_started;
    std::promise<void> finish_put;
    std::promise<void> session_started;
    std::promise<void> finish_start;
    std::atomic<int> nof_revokes{0};
    std::atomic<int> disk_revokes{0};
    std::atomic<int> failed_disk_revokes{0};
    std::atomic<int> missing_lease_revokes{0};

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const UUID &, const std::string &key, uint64_t, const ReplicateConfig &,
        const std::string &) {
        if (key == kInflight) {
            put_started.set_value();
            finish_put.get_future().wait_for(std::chrono::seconds(10));
        }
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    std::vector<tl::expected<void, ErrorCode>> FinishPutSessions(
        const UUID &, const std::vector<PutSessionKey> &keys, ReplicaType type,
        bool revoke, const std::string &) {
        std::vector<tl::expected<void, ErrorCode>> results;
        for (const auto &[key, reservation] : keys) {
            if (key == kGeneration || key == kGenerationPeer) {
                results.emplace_back();
                continue;
            }
            if (!revoke) {
                if (key == kDisk && type == ReplicaType::MEMORY &&
                    disk_revokes > 0)
                    results.emplace_back();
                else
                    results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
                continue;
            }
            if (key == kReplaced) {
                results.emplace_back(tl::unexpected(ErrorCode::ILLEGAL_CLIENT));
            } else if (key == kShortLease ||
                       (key == kMissingLease && ++missing_lease_revokes > 1)) {
                results.emplace_back();
            } else if (key == kFlex && type == ReplicaType::ALL) {
                // The MEMORY replica is already complete.
                results.emplace_back(tl::unexpected(ErrorCode::INVALID_WRITE));
            } else if (key == kFlex && type == ReplicaType::NOF_SSD &&
                       ++nof_revokes > 1) {
                results.emplace_back();
            } else if (key == kDisk && type == ReplicaType::DISK) {
                ++disk_revokes;
                results.emplace_back();
            } else if (key == kFailedDisk && type == ReplicaType::DISK &&
                       ++failed_disk_revokes >= 3) {
                results.emplace_back();
            } else if (key == kFailedDisk && type == ReplicaType::ALL &&
                       failed_disk_revokes >= 3) {
                results.emplace_back();
            } else {
                results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
            }
        }
        return results;
    }

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const UUID &, const std::vector<std::string> &keys,
                  const std::vector<uint64_t> &, const ReplicateConfig &,
                  const std::string &) {
        if (keys == std::vector<std::string>{kStarting}) {
            session_started.set_value();
            finish_start.get_future().wait_for(std::chrono::seconds(10));
            return {};  // malformed reply with an unknown reservation outcome
        }
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            result;
        const UUID incarnation =
            restarted ? next_incarnation : first_incarnation;
        for (const auto &key : keys) {
            if (key == kNofOnly) {
                result.emplace_back(std::vector<Replica::Descriptor>{
                    {0, NoFDescriptor{}, ReplicaStatus::PROCESSING}});
            } else if (key == kShortLease || key == kMissingLease ||
                       key == kGeneration || key == kGenerationPeer) {
                MemoryDescriptor memory{};
                if (key != kMissingLease)
                    memory.put_session_write_grant = PutSessionWriteGrant{
                        key == kShortLease ? 1u : 30u, incarnation};
                result.emplace_back(std::vector<Replica::Descriptor>{
                    {0, std::move(memory), ReplicaStatus::PROCESSING}});
            } else if (key == kDisk || key == kFailedDisk) {
                MemoryDescriptor memory{};
                memory.put_session_write_grant =
                    PutSessionWriteGrant{30, incarnation};
                result.emplace_back(std::vector<Replica::Descriptor>{
                    {0, std::move(memory), ReplicaStatus::PROCESSING},
                    {1, DiskDescriptor{}, ReplicaStatus::PROCESSING}});
            } else {
                result.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
            }
        }
        return result;
    }
};

ErrorCode Status(const tl::expected<void, ErrorCode> &result) {
    return result ? ErrorCode::OK : result.error();
}

}  // namespace

class RealClientTeardownTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_rpc_port(0)
                                      .set_http_metrics_port(0)
                                      .set_http_metadata_port(0)
                                      .build()));

        // Exercise the real RPC client against controlled write outcomes.
        const int port = getFreeTcpPort();
        failing_master_ = std::make_unique<coro_rpc::coro_rpc_server>(
            1, port, "127.0.0.1", std::chrono::seconds(0), true);
        failing_master_->register_handler<&WrappedMasterService::ServiceReady>(
            master_.service().get());
        failing_master_->register_handler<&SessionMaster::PutStart>(
            &session_master_,
            coro_rpc::func_id<&WrappedMasterService::PutStart>());
        failing_master_->register_handler<&SessionMaster::BatchPutStart>(
            &session_master_,
            coro_rpc::func_id<&WrappedMasterService::BatchPutStart>());
        failing_master_->register_handler<&SessionMaster::BatchPutStart>(
            &session_master_,
            coro_rpc::func_id<&WrappedMasterService::StartPutSessions>());
        failing_master_->register_handler<&SessionMaster::FinishPutSessions>(
            &session_master_,
            coro_rpc::func_id<&WrappedMasterService::FinishPutSessions>());
        ASSERT_FALSE(failing_master_->async_start().hasResult());

        probe_ = std::make_shared<ShutdownProbe>();
        client_ = std::make_shared<RealClient>();
        client_->client_ = std::make_shared<TeardownClient>(probe_);
        ASSERT_EQ(client_->client_->master_client_.Connect(
                      "127.0.0.1:" + std::to_string(port)),
                  ErrorCode::OK);
        client_->real_sessions_.put_sessions.emplace(
            kPendingKey,
            RealClient::PutSessionEntry{.reservation = generate_uuid()});
        ASSERT_TRUE(client_->client_->claimSessionPut(kPendingKey));

        // Exercise the production mmap owner, using its full release alignment
        // even when huge pages are enabled in the test environment.
        mapping_size_ = std::max(static_cast<size_t>(sysconf(_SC_PAGESIZE)),
                                 get_hugepage_size_from_env());
        mapping_ = mmap(nullptr, mapping_size_, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapping_, MAP_FAILED);
        client_->hugepage_segment_ptrs_.emplace_back(
            mapping_, RealClient::HugepageSegmentDeleter{mapping_size_});
    }

    void TearDown() override {
        client_.reset();
        if (failing_master_) failing_master_->stop();
        master_.Stop();
        if (!storage_path_.empty()) {
            std::filesystem::remove_all(storage_path_);
        }
    }

    void AddFileStorage() {
        char path[] = "/tmp/mooncake_teardown_XXXXXX";
        ASSERT_NE(mkdtemp(path), nullptr);
        storage_path_ = path;
        FileStorageConfig config;
        config.storage_backend_type = StorageBackendType::kFilePerKey;
        config.storage_filepath = storage_path_;
        config.local_buffer_size = 4096;
        config.total_keys_limit = 16;
        config.total_size_limit = 4096;
        // Initialization is unnecessary: the constructor already owns Client.
        client_->file_storage_ =
            std::make_shared<FileStorage>(config, client_->client_, "");
    }

    void CheckDestructorOrdering() {
        std::promise<void> release_worker;
        auto release = release_worker.get_future().share();
        auto arrived = std::make_shared<std::promise<void>>();
        auto worker_started = arrived->get_future();
        auto client_destructor_started =
            probe_->client_destructor_started.get_future();
        client_->client_->task_thread_pool_.enqueue(
            [arrived, release, probe = probe_, mapping = mapping_] {
                arrived->set_value();
                release.wait();
                probe->worker_saw_mapping.store(IsMapped(mapping));
            });
        ASSERT_EQ(worker_started.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);

        std::thread destroyer(
            [client = std::move(client_)]() mutable { client.reset(); });
        EXPECT_EQ(client_destructor_started.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);
        // Client's destructor now has to join the gated production worker.
        // mincore detects the old ordering safely, without reading freed
        // memory.
        EXPECT_TRUE(IsMapped(mapping_));
        release_worker.set_value();
        destroyer.join();

        EXPECT_TRUE(probe_->worker_saw_mapping.load());
        EXPECT_FALSE(IsMapped(mapping_));
    }

    bool HasPendingSession() const {
        return client_->real_sessions_.put_sessions.count(kPendingKey) == 1 &&
               client_->client_->put_claims_.count(kPendingKey) == 1;
    }

    bool PendingStateEmpty() const {
        return client_->real_sessions_.put_sessions.empty();
    }

    ErrorCode ReconnectMaster() {
        return client_->client_->master_client_.Connect(
            master_.master_address());
    }

    uint64_t next_control_ = 0;
    DummyPutControl NextControl(const UUID &id) {
        return {client_->shm_contexts_.at(id).sessions->dummy_context,
                ++next_control_};
    }

    void CheckOrdinaryWriteClaims() {
        const UUID dummy = generate_uuid();
        client_->shm_contexts_.try_emplace(dummy);
        const std::string key = "uncertain_put";
        const std::vector<int> rpc_fail{toInt(ErrorCode::RPC_FAIL)};
        ASSERT_EQ(client_
                      ->batch_put_session_start_dummy_rpc_v2(
                          {key}, {1}, {}, dummy, NextControl(dummy))
                      .results,
                  rpc_fail);

        char value = 'x';
        std::vector<Slice> slices{{&value, 1}};
        auto &store = *client_->client_;
        EXPECT_EQ(Status(store.Put(key, slices, {})), ErrorCode::INVALID_WRITE);
        EXPECT_EQ(Status(store.Upsert(key, slices, {})),
                  ErrorCode::INVALID_WRITE);
        std::vector<std::vector<Slice>> batch{slices, slices};
        for (const auto &result :
             {store.BatchPut({"other", key}, batch, {}),
              store.BatchUpsert({"other", key}, batch, {})}) {
            ASSERT_EQ(result.size(), 2U);
            EXPECT_EQ(Status(result[0]), ErrorCode::RPC_FAIL);
            EXPECT_EQ(Status(result[1]), ErrorCode::INVALID_WRITE);
        }
        EXPECT_EQ(client_->batch_put_session_revoke_dummy_rpc_v2(
                      {key}, dummy, NextControl(dummy)),
                  rpc_fail);
        EXPECT_EQ(Status(store.Put(key, slices, {})), ErrorCode::INVALID_WRITE);
        ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
        ASSERT_TRUE(client_->unmap_shm_internal(dummy));
        EXPECT_NE(Status(store.Put(key, slices, {})), ErrorCode::INVALID_WRITE);
    }

    void CheckReplacedSessionClaim() {
        const UUID dummy = generate_uuid();
        client_->shm_contexts_.try_emplace(dummy);
        const std::vector<std::string> keys{SessionMaster::kReplaced};
        ASSERT_EQ(client_
                      ->batch_put_session_start_dummy_rpc_v2(
                          keys, {1}, {}, dummy, NextControl(dummy))
                      .results,
                  std::vector<int>{toInt(ErrorCode::RPC_FAIL)});
        EXPECT_EQ(client_->batch_put_session_revoke_dummy_rpc_v2(
                      keys, dummy, NextControl(dummy)),
                  std::vector<int>{toInt(ErrorCode::ILLEGAL_CLIENT)});
        EXPECT_EQ(client_->client_->put_claims_.count(keys.front()), 0U);
        EXPECT_TRUE(client_->unmap_shm_internal(dummy));
    }

    void CheckPendingStartRecord() {
        const auto dummy = generate_uuid();
        auto &sessions = *client_->shm_contexts_[dummy].sessions;
        const std::string key = SessionMaster::kStarting;
        auto started = session_master_.session_started.get_future();
        auto start = std::async(std::launch::async, [&] {
            return client_
                ->batch_put_session_start_dummy_rpc_v2(
                    {key, key}, {1, 1}, {}, dummy, NextControl(dummy))
                .results;
        });
        ASSERT_EQ(started.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);
        {
            std::lock_guard lock(sessions.mutex);
            auto entry = sessions.put_sessions.find(key);
            ASSERT_NE(entry, sessions.put_sessions.end());
            EXPECT_FALSE(entry->second.writable);
        }
        session_master_.finish_start.set_value();
        EXPECT_EQ(start.get(),
                  (std::vector<int>{toInt(ErrorCode::RPC_FAIL),
                                    toInt(ErrorCode::INVALID_PARAMS)}));
        EXPECT_EQ(client_->batch_put_session_end_dummy_rpc_v2(
                      {key}, dummy, NextControl(dummy)),
                  std::vector<int>{toInt(ErrorCode::INVALID_PARAMS)});
        ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
        EXPECT_TRUE(client_->unmap_shm_internal(dummy));
        EXPECT_EQ(client_->client_->put_claims_.count(key), 0U);
    }

    void CheckLegacyDiskCleanup() {
        const auto dummy = generate_uuid();
        auto &sessions = *client_->shm_contexts_[dummy].sessions;
        const std::string key = SessionMaster::kDisk;
        ASSERT_EQ(client_
                      ->batch_put_session_start_dummy_rpc_v2(
                          {key}, {1}, {}, dummy, NextControl(dummy))
                      .results,
                  std::vector<int>{0});
        EXPECT_EQ(session_master_.disk_revokes.load(), 1);
        EXPECT_EQ(client_->batch_put_session_end_dummy_rpc_v2(
                      {key}, dummy, NextControl(dummy)),
                  std::vector<int>{0});
        const std::string failed = SessionMaster::kFailedDisk;
        ASSERT_EQ(client_
                      ->batch_put_session_start_dummy_rpc_v2(
                          {failed}, {1}, {}, dummy, NextControl(dummy))
                      .results,
                  std::vector<int>{toInt(ErrorCode::RPC_FAIL)});
        ASSERT_EQ(sessions.put_sessions.count(failed), 1U);
        EXPECT_FALSE(sessions.put_sessions.at(failed).writable);
        EXPECT_EQ(Status(client_->unmap_shm_internal(dummy)),
                  ErrorCode::RPC_FAIL);
        EXPECT_EQ(client_->client_->put_claims_.count(failed), 1U);
        EXPECT_TRUE(client_->unmap_shm_internal(dummy));
        EXPECT_EQ(session_master_.failed_disk_revokes.load(), 3);
        EXPECT_EQ(client_->client_->put_claims_.count(failed), 0U);
    }

    void CheckSessionWriteGrant() {
        const std::string short_key = SessionMaster::kShortLease;
        const std::string missing = SessionMaster::kMissingLease;
        const int expired = toInt(ErrorCode::LEASE_EXPIRED);
        ASSERT_EQ(
            client_->batch_put_session_start({short_key, missing}, {1, 1}, {}),
            (std::vector<int>{0, expired}));
        // The automatic revoke of the missing grant failed. A subsequent end
        // must still be unable to publish its untouched allocation.
        EXPECT_EQ(client_->batch_put_session_end({missing}),
                  std::vector<int>{expired});
        const auto deadline =
            client_->real_sessions_.put_sessions.at(short_key).write_deadline;
        std::this_thread::sleep_until(deadline);
        char byte = 'x';
        EXPECT_EQ(client_->batch_put_from_multi_buffer_ranges(
                      {short_key}, {{&byte}}, {{1}}, {{0}}),
                  std::vector<int>{expired});
        EXPECT_EQ(client_->batch_put_session_end({short_key}),
                  std::vector<int>{expired});
        // Exercise the final submission gate independently of RealClient's
        // admission check. Only the live row may reach this fixture's missing
        // transfer submitter. The expired address must be omitted.
        MemoryDescriptor live{};
        live.buffer_descriptor.transport_endpoint_ = "unused";
        auto transfer = client_->client_->BatchTransferWriteRanges(
            {client_->real_sessions_.put_sessions.at(short_key).replicas,
             {{1, std::move(live), ReplicaStatus::PROCESSING}}},
            {{{&byte, 1}}, {{&byte, 0}}}, {{0}, {0}},
            TransferIntent::kUnspecified,
            {deadline, std::chrono::steady_clock::time_point::max()});
        ASSERT_EQ(transfer.size(), 2U);
        ASSERT_FALSE(transfer[0]);
        EXPECT_EQ(transfer[0].error(), ErrorCode::LEASE_EXPIRED);
        ASSERT_FALSE(transfer[1]);
        EXPECT_EQ(transfer[1].error(), ErrorCode::TRANSFER_FAIL);
        EXPECT_EQ(client_->batch_put_session_revoke({missing, short_key}),
                  (std::vector<int>{0, 0}));
    }

    void CheckObservedMasterGeneration() {
        auto &client = *client_->client_;
        const std::vector<std::string> key{SessionMaster::kGeneration};
        const std::vector<std::string> peer{SessionMaster::kGenerationPeer};
        const std::vector<int> ok{0}, stale{toInt(ErrorCode::INVALID_VERSION)};
        ASSERT_EQ(client_->batch_put_session_start(key, {1}, {}), ok);
        const auto old = client.master_generation_.load();
        session_master_.restarted = true;
        // Standalone writer with no storage heartbeat discovers the new
        // process through an ordinary start, even at the same endpoint/view.
        ASSERT_EQ(client_->batch_put_session_start(peer, {1}, {}), ok);
        const auto fresh = client.master_generation_.load();
        EXPECT_NE(old, fresh);
        EXPECT_FALSE(
            client.ObservePutMaster(old, session_master_.first_incarnation));
        EXPECT_FALSE(client.ObservePutMaster(old,
                                             session_master_.first_incarnation,
                                             ClientStatus::NEED_REMOUNT));
        EXPECT_EQ(client.master_generation_.load(), fresh);
        char byte = 'x';
        EXPECT_EQ(client_->batch_put_from_multi_buffer_ranges(key, {{&byte}},
                                                              {{1}}, {{0}}),
                  stale);
        EXPECT_EQ(client_->batch_put_session_end(key), stale);
        EXPECT_EQ(client_->batch_put_session_revoke(key), ok);
        ASSERT_EQ(client_->batch_put_session_start(key, {1}, {}), ok);
        EXPECT_EQ(client_->batch_put_session_end(key), ok);
        // An initial/remnant NEED_REMOUNT cannot invalidate fresh grants.
        EXPECT_EQ(
            client.ObservePutMaster(fresh, {}, ClientStatus::NEED_REMOUNT),
            fresh);
        EXPECT_EQ(
            client.ObservePutMaster(fresh, {}, ClientStatus::NEED_REMOUNT),
            fresh);
        EXPECT_EQ(client.ObservePutMaster(fresh, {}, ClientStatus::OK), fresh);
        EXPECT_NE(
            client.ObservePutMaster(fresh, {}, ClientStatus::NEED_REMOUNT),
            fresh);
        EXPECT_EQ(client_->batch_put_session_end(peer), stale);
        EXPECT_EQ(client_->batch_put_session_revoke(peer), ok);
        const ha::MasterView first{master_.master_address(), 7};
        const auto before_switch = client.master_generation_.load();
        ASSERT_EQ(client.SwitchLeader(first), ErrorCode::OK);
        // Warming can clear the published view without changing the leader.
        client.current_master_view_.reset();
        ASSERT_EQ(client.SwitchLeader(first), ErrorCode::OK);
        EXPECT_EQ(client.master_generation_.load(), before_switch);
        ASSERT_EQ(client.SwitchLeader({first.leader_address, 8}),
                  ErrorCode::OK);
        EXPECT_GT(client.master_generation_.load(), before_switch);
    }

    void CheckLegacyMasterRejectsSession() {
        const int port = getFreeTcpPort();
        coro_rpc::coro_rpc_server legacy(1, port, "127.0.0.1");
        legacy.register_handler<&WrappedMasterService::ServiceReady>(
            master_.service().get());
        legacy.register_handler<&SessionMaster::BatchPutStart>(
            &session_master_,
            coro_rpc::func_id<&WrappedMasterService::BatchPutStart>());
        ASSERT_FALSE(legacy.async_start().hasResult());
        ASSERT_EQ(client_->client_->master_client_.Connect(
                      "127.0.0.1:" + std::to_string(port)),
                  ErrorCode::OK);
        const std::string key = "legacy_master";
        EXPECT_EQ(client_->batch_put_session_start({key}, {1}, {}),
                  std::vector<int>{toInt(ErrorCode::NOT_SUPPORTED)});
        EXPECT_EQ(client_->client_->put_claims_.count(key), 0U);
        EXPECT_EQ(client_->real_sessions_.put_sessions.count(key), 0U);
        EXPECT_EQ(ReconnectMaster(), ErrorCode::OK);
    }

    void CheckSessionDuringOrdinaryPut() {
        const UUID dummy = generate_uuid();
        client_->shm_contexts_.try_emplace(dummy);
        auto started = session_master_.put_started.get_future();
        auto put = std::async(std::launch::async, [&] {
            char value = 'x';
            std::vector<Slice> slices{{&value, 1}};
            return client_->client_->Put(SessionMaster::kInflight, slices, {});
        });
        ASSERT_EQ(started.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);
        EXPECT_EQ(client_
                      ->batch_put_session_start_dummy_rpc_v2(
                          {SessionMaster::kInflight}, {1}, {}, dummy,
                          NextControl(dummy))
                      .results,
                  std::vector<int>{toInt(ErrorCode::INVALID_PARAMS)});
        session_master_.finish_put.set_value();
        EXPECT_EQ(Status(put.get()), ErrorCode::NO_AVAILABLE_HANDLE);
        EXPECT_EQ(client_
                      ->batch_put_session_start_dummy_rpc_v2(
                          {SessionMaster::kInflight}, {1}, {}, dummy,
                          NextControl(dummy))
                      .results,
                  std::vector<int>{toInt(ErrorCode::RPC_FAIL)});
        ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
        EXPECT_TRUE(client_->unmap_shm_internal(dummy));
    }

    void CheckQueuedDiskWriteClaim() {
        auto &store = *client_->client_;
        char dir[] = "/tmp/mooncake_disk_claim_XXXXXX";
        ASSERT_NE(mkdtemp(dir), nullptr);
        storage_path_ = dir;
        ASSERT_EQ(store.PrepareStorageBackend(storage_path_, "disk", false),
                  ErrorCode::OK);

        // Keep the disk worker queued until the caller's key has gone away.
        std::promise<void> release;
        auto gate = release.get_future().share();
        for (size_t i = 0; i < store.write_thread_pool_.size(); ++i) {
            store.write_thread_pool_.enqueue([gate] { gate.wait(); });
        }
        const std::string key = "queued_legacy_disk_write";
        char value = 'x';
        std::vector<Slice> slices{{&value, 1}};
        DiskDescriptor disk{storage_path_ + "/disk-object", 1};
        store.PutToLocalFile(std::string(key), slices, disk);
        bool acquired = store.claimSessionPut(key);
        EXPECT_FALSE(acquired);
        if (acquired) store.releaseSessionPut(key);

        release.set_value();
        store.write_thread_pool_.stop();
        ASSERT_TRUE(store.claimSessionPut(key));
        store.releaseSessionPut(key);

        // enqueue() rejects the task before it can run. Destroying its
        // closure must release the claim too.
        EXPECT_THROW(store.PutToLocalFile(key, slices, disk),
                     std::runtime_error);
        EXPECT_TRUE(store.claimSessionPut(key));
        store.releaseSessionPut(key);
    }

    void CheckFlexCleanupRetry() {
        const UUID dummy = generate_uuid();
        const auto key = SessionMaster::kFlex;
        auto &context = client_->shm_contexts_[dummy];
        auto &entry = context.sessions->put_sessions[key];
        entry.reservation = generate_uuid();
        entry.write_mode = ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA;
        entry.write_deadline =
            std::chrono::steady_clock::now() + std::chrono::minutes(1);
        entry.replicas = {{0, MemoryDescriptor{}, ReplicaStatus::PROCESSING},
                          {1, NoFDescriptor{}, ReplicaStatus::PROCESSING}};
        ASSERT_TRUE(client_->client_->claimSessionPut(key));
        // The missing MEMORY end reply leaves the outcome unknown. Cleanup
        // must retain the session when the subsequent NoF revoke also fails.
        ASSERT_EQ(client_->batch_put_session_end_dummy_rpc_v2(
                      {key}, dummy, NextControl(dummy)),
                  std::vector<int>{toInt(ErrorCode::RPC_FAIL)});
        EXPECT_EQ(Status(client_->unmap_shm_internal(dummy)),
                  ErrorCode::RPC_FAIL);
        EXPECT_EQ(session_master_.nof_revokes.load(), 1);
        EXPECT_EQ(client_->client_->put_claims_.count(key), 1U);
        EXPECT_TRUE(client_->unmap_shm_internal(dummy));
        EXPECT_EQ(session_master_.nof_revokes.load(), 2);
        EXPECT_EQ(client_->client_->put_claims_.count(key), 0U);

        client_->shm_contexts_.try_emplace(dummy);
        ReplicateConfig flexible;
        flexible.nof_replica_num = 1;
        ASSERT_EQ(client_
                      ->batch_put_session_start_dummy_rpc_v2(
                          {SessionMaster::kNofOnly}, {1}, flexible, dummy,
                          NextControl(dummy))
                      .results,
                  std::vector<int>{toInt(ErrorCode::INVALID_REPLICA)});
        EXPECT_EQ(client_->batch_put_session_end_dummy_rpc_v2(
                      {SessionMaster::kNofOnly}, dummy, NextControl(dummy)),
                  std::vector<int>{toInt(ErrorCode::INVALID_PARAMS)});
        ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
        EXPECT_TRUE(client_->unmap_shm_internal(dummy));
    }

    std::weak_ptr<ClientBufferAllocator> AddCudaAllocation(
        const UUID &id, CudaIpcBufferHandle &handle) {
        handle.has_device_uuid = true;
        handle.size = 4096;
        const std::string key(handle.device_uuid.size() + handle.handle.size(),
                              '\0');
        auto &context = client_->shm_contexts_[id];
        auto &allocation = client_->cuda_allocations_[key];
        // Exercise MR bookkeeping and the production cleanup paths without
        // requiring a CUDA device or verbs provider.
        allocation = std::make_shared<RealClient::CudaRegisteredAllocation>();
        auto allocator = ClientBufferAllocator::create(mapping_, mapping_size_);
        std::weak_ptr<ClientBufferAllocator> allocator_observer = allocator;
        context.client_buffer_allocator = std::move(allocator);
        return allocator_observer;
    }

    void CheckShmQuarantineOwner() {
        const UUID owner = generate_uuid();
        const size_t page = sysconf(_SC_PAGESIZE);
        void *const mapping = mmap(nullptr, page, PROT_READ | PROT_WRITE,
                                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapping, MAP_FAILED);
        auto &shm = client_->quarantine_shms_.emplace_back();
        shm.owner = owner;
        shm.shm_buffer = mapping;
        // A zero length deterministically fails munmap until the retry below.
        auto failed = client_->unmap_shm_internal(owner);
        ASSERT_FALSE(failed);
        EXPECT_EQ(failed.error(), ErrorCode::INTERNAL_ERROR);
        EXPECT_TRUE(IsMapped(mapping));
        EXPECT_TRUE(client_->unmap_shm_internal(generate_uuid()));
        shm.shm_size = page;
        EXPECT_TRUE(client_->unmap_shm_internal(owner));
        EXPECT_FALSE(IsMapped(mapping));
        EXPECT_TRUE(client_->unmap_shm_internal(owner));
    }

    void CheckCudaQuarantine() {
        const UUID id = generate_uuid();
        CudaIpcBufferHandle handle{};
        auto allocator = AddCudaAllocation(id, handle);
        // A second request for an allocation with an ambiguous initial
        // rollback must preserve the negative acknowledgement for this id.
        ASSERT_FALSE(client_->register_cuda_buffer_with_fd(kCudaAddress, handle,
                                                           id, -1));
        EXPECT_FALSE(client_->unmap_shm_internal(id));
        EXPECT_TRUE(allocator.expired());
        EXPECT_EQ(client_->shm_contexts_.count(id), 0U);
        EXPECT_FALSE(
            client_->unregister_cuda_buffer_internal(kCudaAddress, handle, id));
        EXPECT_FALSE(client_->unmap_shm_internal(id));
        EXPECT_TRUE(client_->unmap_shm_internal(generate_uuid()));
        ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
        EXPECT_NE(client_->tearDownAll(), 0);
        EXPECT_EQ(client_->client_, nullptr);
        EXPECT_NE(client_->tearDownAll(), 0);
        EXPECT_EQ(client_->cuda_allocations_.size(), 1U);
        // There is no actual MR or CUDA import in this synthetic test.
        client_->cuda_allocations_.clear();
    }

    void CheckCudaDeregistrationRetry() {
        const UUID id = generate_uuid();
        CudaIpcBufferHandle handle{};
        auto allocator = AddCudaAllocation(id, handle);
        auto &allocation = client_->cuda_allocations_.begin()->second;
        allocation->state = RealClient::CudaRegistrationState::kRegistered;
        client_->shm_contexts_[id].cuda_buffers.emplace(
            kCudaAddress, RealClient::CudaRegisteredBuffer{handle, allocation});
        client_->registered_buffer_sizes_[allocation->mapping.base()] = {
            handle.size};
        int attempts = 0;
        struct ResetHook {
            ~ResetHook() { RealClient::SetTeUnregisterHookForTesting(nullptr); }
        } reset_hook;
        RealClient::SetTeUnregisterHookForTesting(
            [&](void *) -> tl::expected<void, ErrorCode> {
                // The first attempt fails after removing the MR. The retry
                // confirms that TE no longer has it and clears the Store
                // record.
                return tl::unexpected(++attempts == 1
                                          ? ErrorCode::INVALID_PARAMS
                                          : ErrorCode::NOT_REGISTERED);
            });
        auto result = client_->unmap_shm_internal(id);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_TRUE(allocator.expired());
        EXPECT_EQ(allocation->state,
                  RealClient::CudaRegistrationState::kUnregisterPending);
        EXPECT_TRUE(client_->unmap_shm_internal(id));
        EXPECT_TRUE(client_->unmap_shm_internal(id));
        EXPECT_EQ(attempts, 2);
        EXPECT_TRUE(client_->cuda_allocations_.empty());
    }

    void CheckUnsupportedCudaBackend(bool tent, const char *transport) {
        client_->client_->transfer_engine_ =
            TransferEngineImplTestPeer::makeEngine(tent, transport);
        const UUID id = generate_uuid();
        client_->shm_contexts_.try_emplace(id);
        CudaIpcBufferHandle handle{};
        handle.has_device_uuid = true;
        handle.size = 4096;
        const bool previous =
            std::exchange(globalConfig().rdma_data_direct, !tent);
        auto result =
            client_->register_cuda_buffer_with_fd(kCudaAddress, handle, id, -1);
        globalConfig().rdma_data_direct = previous;
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
        EXPECT_TRUE(client_->cuda_allocations_.empty());
        EXPECT_TRUE(client_->shm_contexts_[id].cuda_buffers.empty());
        EXPECT_TRUE(
            client_->unregister_cuda_buffer_internal(kCudaAddress, handle, id));
    }

    void CheckCudaClosePending() {
        const UUID id = generate_uuid();
        CudaIpcBufferHandle handle{};
        AddCudaAllocation(id, handle);
        auto &allocation = client_->cuda_allocations_.begin()->second;
        // A failed CUDA close retains the logical registration, but its
        // Store/TE registration was already removed on the first attempt.
        allocation->state = RealClient::CudaRegistrationState::kClosePending;
        client_->shm_contexts_[id].cuda_buffers.emplace(
            kCudaAddress, RealClient::CudaRegisteredBuffer{handle, allocation});
        EXPECT_FALSE(client_->register_cuda_buffer_with_fd(kCudaAddress, handle,
                                                           id, -1));
        EXPECT_TRUE(
            client_->unregister_cuda_buffer_internal(kCudaAddress, handle, id));
        EXPECT_TRUE(client_->cuda_allocations_.empty());
        EXPECT_TRUE(client_->shm_contexts_[id].cuda_buffers.empty());
        EXPECT_TRUE(client_->unmap_shm_internal(id));
    }

    static constexpr uint64_t kCudaAddress = 0x1000;
    static constexpr const char *kPendingKey = "teardown_pending_put";
    testing::InProcMaster master_;
    SessionMaster session_master_;
    std::unique_ptr<coro_rpc::coro_rpc_server> failing_master_;
    std::shared_ptr<RealClient> client_;
    std::shared_ptr<ShutdownProbe> probe_;
    void *mapping_ = MAP_FAILED;
    size_t mapping_size_ = 0;
    std::string storage_path_;
};

TEST_F(RealClientTeardownTest, DestructorJoinsWorkersAfterSessionCleanupFails) {
    CheckDestructorOrdering();
}

TEST_F(RealClientTeardownTest,
       UncertainSessionBlocksOrdinaryWritesUntilCleanup) {
    CheckOrdinaryWriteClaims();
}

TEST_F(RealClientTeardownTest, OrdinaryPutBlocksSessionUntilWriteReturns) {
    CheckSessionDuringOrdinaryPut();
}

TEST_F(RealClientTeardownTest, ReplacedMasterReservationReleasesLocalClaim) {
    CheckReplacedSessionClaim();
}

TEST_F(RealClientTeardownTest, ExpiredSessionGrantRejectsWritesAndPublication) {
    CheckSessionWriteGrant();
}

TEST_F(RealClientTeardownTest, LegacyMasterRejectsRangedPutWithoutClaim) {
    CheckLegacyMasterRejectsSession();
}

TEST_F(RealClientTeardownTest, ObservedMasterChangeSealsOnlyOldPuts) {
    CheckObservedMasterGeneration();
}

TEST_F(RealClientTeardownTest, InflightStartHasSealedCleanupRecord) {
    CheckPendingStartRecord();
}

TEST_F(RealClientTeardownTest, LegacyDiskReservationRemovedBeforeEndOrCleanup) {
    CheckLegacyDiskCleanup();
}

TEST_F(RealClientTeardownTest, QueuedDiskWriteHoldsClaimUntilFinalization) {
    CheckQueuedDiskWriteClaim();
}

TEST_F(RealClientTeardownTest, FlexSessionCleanupRetriesOutstandingNoFReplica) {
    CheckFlexCleanupRetry();
}

TEST_F(RealClientTeardownTest,
       DestructorDropsFileStorageReferenceBeforeReleasingSegments) {
    AddFileStorage();
    ASSERT_NE(client_->file_storage_, nullptr);
    CheckDestructorOrdering();
}

TEST_F(RealClientTeardownTest, ExplicitCloseRetainsFailedSessionForRetry) {
    EXPECT_EQ(client_->tearDownAll(), static_cast<int>(ErrorCode::RPC_FAIL));
    EXPECT_EQ(client_->tearDownAll(), static_cast<int>(ErrorCode::RPC_FAIL));
    EXPECT_NE(client_->client_, nullptr);
    EXPECT_TRUE(HasPendingSession());
    EXPECT_TRUE(IsMapped(mapping_));

    ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
    // The synthetic pending key is absent, so OBJECT_NOT_FOUND confirms that
    // the retry can release its session ownership and complete shutdown.
    EXPECT_EQ(client_->tearDownAll(), 0);
    EXPECT_EQ(client_->client_, nullptr);
    EXPECT_TRUE(PendingStateEmpty());
    EXPECT_FALSE(IsMapped(mapping_));
}

TEST_F(RealClientTeardownTest, CudaQuarantineSurvivesDummyAndRealClose) {
    CheckCudaQuarantine();
}

TEST_F(RealClientTeardownTest, UnmapReportsOnlyTheCallersOutstandingMappings) {
    CheckShmQuarantineOwner();
}

TEST_F(RealClientTeardownTest, CudaDeregistrationRetriesAfterShmCleanup) {
    CheckCudaDeregistrationRetry();
}

TEST_F(RealClientTeardownTest, CudaCloseRetrySkipsCompletedMrDeregistration) {
    CheckCudaClosePending();
}

TEST_F(RealClientTeardownTest, TentRejectsPersistentCudaBeforeImport) {
    CheckUnsupportedCudaBackend(true, "rdma");
}

TEST_F(RealClientTeardownTest, DataDirectRejectsPersistentCudaBeforeImport) {
    CheckUnsupportedCudaBackend(false, "rdma");
    CheckUnsupportedCudaBackend(false, "rdma_twosided");
}

}  // namespace mooncake::test
