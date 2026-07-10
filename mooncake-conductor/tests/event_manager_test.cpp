// Tests for EventManager and KVEventHandler: service-key construction,
// initial state, subscribe/unsubscribe behaviour, the guarded services
// append, event handling, and config file loading.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/kvevent/config.h"
#include "conductor/kvevent/event_manager.h"
#include "event_manager_test_peer.h"

namespace {

using conductor::common::ServiceConfig;
using conductor::kvevent::EventManager;
using conductor::kvevent::EventManagerTestPeer;
using conductor::kvevent::KVEventHandler;
using conductor::kvevent::KVEventHandlerTestPeer;
using conductor::kvevent::MakeServiceKey;
using conductor::zmq::BlockStoredEvent;

ServiceConfig VllmService(const std::string& instance_id = "instance-1",
                          const std::string& tenant_id = "tenant-1",
                          int dp_rank = 0) {
    ServiceConfig svc;
    svc.endpoint = "tcp://127.0.0.1:5557";
    svc.type = conductor::common::kServiceTypeVLLM;
    svc.model_name = "test-model";
    svc.instance_id = instance_id;
    svc.tenant_id = tenant_id;
    svc.dp_rank = dp_rank;
    svc.block_size = 16;
    return svc;
}

// --- makeServiceKey ------------------------------------------------------

TEST(MakeServiceKey, Format) {
    EXPECT_EQ(MakeServiceKey("instance-1", "tenant-1", 0),
              "instance-1|tenant-1|0");
}

// --- constructor state ---------------------------------------------------

TEST(EventManager, InitialState) {
    EventManager mgr({}, 13333);
    EXPECT_NE(mgr.GetIndexer(), nullptr);
    EXPECT_EQ(EventManagerTestPeer::ServicesLen(mgr), 0u);
    EXPECT_FALSE(mgr.IsStopped());
}

// --- subscribeToService --------------------------------------------------

TEST(SubscribeToService, DuplicateReturnsFalse) {
    EventManager mgr({}, 0);
    const auto svc = VllmService();

    // Pre-populate subscribers to simulate an existing subscription so
    // SubscribeToService hits the early-return path.
    EventManagerTestPeer::FakeSubscriber(
        mgr, MakeServiceKey(svc.instance_id, svc.tenant_id, svc.dp_rank));

    const auto [is_new, err] = EventManagerTestPeer::Subscribe(mgr, svc);
    EXPECT_EQ(err, "");
    EXPECT_FALSE(is_new);
}

TEST(SubscribeToService, MissingEndpointErrors) {
    EventManager mgr({}, 0);
    auto svc = VllmService();
    svc.endpoint = "";
    svc.block_size = 0;

    const auto [is_new, err] = EventManagerTestPeer::Subscribe(mgr, svc);
    EXPECT_FALSE(err.empty());
    EXPECT_FALSE(is_new);
}

// ZMQ connect is async, so a subscription succeeds even against a
// nonexistent endpoint; assert the service-key discrimination directly
// rather than relying on the connect path.
TEST(SubscribeToService, DifferentDPRankIsNewKey) {
    EXPECT_NE(MakeServiceKey("instance-1", "tenant-1", 0),
              MakeServiceKey("instance-1", "tenant-1", 1));
}

TEST(SubscribeToService, DifferentTenantIsNewKey) {
    EXPECT_NE(MakeServiceKey("instance-1", "tenant-a", 0),
              MakeServiceKey("instance-1", "tenant-b", 0));
}

// --- guarded services append (no duplicate on re-register) ---------------

TEST(SubscribeToService, NoServicesDuplicateOnReRegister) {
    EventManager mgr({}, 0);
    const auto svc = VllmService();
    EventManagerTestPeer::FakeSubscriber(
        mgr, MakeServiceKey(svc.instance_id, svc.tenant_id, svc.dp_rank));

    for (int round = 0; round < 2; ++round) {
        const auto [is_new, err] = EventManagerTestPeer::Subscribe(mgr, svc);
        ASSERT_EQ(err, "");
        if (is_new) {
            EventManagerTestPeer::AppendService(mgr, svc);
        }
        EXPECT_EQ(EventManagerTestPeer::ServicesLen(mgr), 0u)
            << "round=" << round;
    }
}

// --- Stop idempotence (spec: 重复停止幂等) --------------------------------

TEST(EventManager, StopIsIdempotent) {
    EventManager mgr({}, 0);
    mgr.Stop();
    mgr.Stop();  // second call must return immediately
    EXPECT_TRUE(mgr.IsStopped());
}

// --- KVEventHandler -------------------------------------------------------

TEST(KVEventHandlerTest, BlockSizeMismatchReturnsEmpty) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 64;
    svc.model_name = "test-model";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent event;
    event.block_size = 128;  // mismatch: 128 != 64
    event.block_hashes = {100};
    event.token_ids = {1, 2, 3, 4};

    EXPECT_EQ(KVEventHandlerTestPeer::BlockStored(handler, event, 0), "");
    // Discarded: nothing reached the indexer.
    EXPECT_TRUE(mgr.GetIndexer()->GetGlobalView().model_contexts.empty());
}

TEST(KVEventHandlerTest, BlockSizeMatchProcessesEvent) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 4;
    svc.model_name = "test-model";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent event;
    event.block_size = 4;  // matches
    event.block_hashes = {100, 200};
    event.token_ids = {1, 2, 3, 4, 5, 6, 7, 8};
    event.parent_block_hash = 0;

    EXPECT_EQ(KVEventHandlerTestPeer::BlockStored(handler, event, 0), "");
    EXPECT_EQ(mgr.GetIndexer()->GetGlobalView().model_contexts.size(), 1u);
}

TEST(KVEventHandlerTest, EmptyBlockHashesWithMismatch) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 64;
    svc.model_name = "test-model";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent event;
    event.block_size = 128;  // mismatch
    EXPECT_EQ(KVEventHandlerTestPeer::BlockStored(handler, event, 0), "");
}

TEST(KVEventHandlerTest, HandleEventDispatchesBlockStored) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 64;
    svc.model_name = "test-model";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent event;
    event.block_size = 128;  // mismatch -> discarded, returns ""
    event.block_hashes = {100};
    event.token_ids = {1, 2, 3, 4};

    EXPECT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(event), 0), "");
}

TEST(KVEventHandlerTest, ManagerStoppedReturnsError) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 4;
    KVEventHandler handler(&mgr, svc);
    mgr.Stop();

    BlockStoredEvent event;
    event.block_size = 4;
    event.block_hashes = {100};
    event.token_ids = {1, 2, 3, 4};

    EXPECT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(event), 0),
              "manager stopped");
}

// Spec scenario (bug-for-bug): the handler builds RemovedEvent WITHOUT
// the Medium field even when the zmq event carries one. Verified through
// observable behavior: store GPU + cpu replicas, remove with medium set,
// query still reports GPU (mediumSet not pruned — and it never could be,
// since the handler dropped the information).
TEST(KVEventHandlerTest, RemovedEventDropsMediumBugForBug) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 4;
    svc.model_name = "test-model";
    svc.instance_id = "instance-1";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent stored;
    stored.block_size = 4;
    stored.block_hashes = {100};
    stored.token_ids = {1, 2, 3, 4};
    stored.medium = "GPU";
    ASSERT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(stored), 0), "");
    stored.medium = "cpu";
    ASSERT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(stored), 1), "");

    conductor::zmq::BlockRemovedEvent removed;
    removed.block_hashes = {100};
    removed.medium = "GPU";  // carried by the wire event...
    ASSERT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(removed), 0), "");

    conductor::prefixindex::ModelContext ctx;
    ctx.model_name = "test-model";
    ctx.block_size = 4;
    ctx.tenant_id = "default";
    ctx.instance_id = "instance-1";
    const auto result = mgr.GetIndexer()->CacheHitCompute(ctx, {1, 2, 3, 4});
    // ...but dropped by the handler: GPU still reported (dirty read).
    EXPECT_EQ(result.gpu, 4);
    EXPECT_EQ(result.cpu, 4);
}

// --- ParseConfig (config file loading) -----------------------------------

class ConfigEnvGuard {
   public:
    ConfigEnvGuard() { unsetenv("CONDUCTOR_CONFIG_PATH"); }
    ~ConfigEnvGuard() { unsetenv("CONDUCTOR_CONFIG_PATH"); }
    void SetPath(const std::string& path) {
        setenv("CONDUCTOR_CONFIG_PATH", path.c_str(), 1);
    }
};

TEST(ParseConfig, MissingFileReturnsEmptyList) {
    ConfigEnvGuard guard;
    guard.SetPath("/nonexistent/conductor_config.json");
    int port = 13333;
    const auto services = conductor::kvevent::ParseConfig(&port);
    EXPECT_TRUE(services.empty());
    EXPECT_EQ(port, 13333);  // untouched when the file is missing
}

TEST(ParseConfig, LoadsServicesAndSkipsUnknownTypes) {
    ConfigEnvGuard guard;
    const std::string path = ::testing::TempDir() + "conductor_cfg_test.json";
    {
        std::ofstream out(path);
        out << R"({
          "http_server_port": 14444,
          "kvevent_instance": {
            "inst-vllm": {"endpoint": "tcp://127.0.0.1:5557",
                           "replay_endpoint": "tcp://127.0.0.1:5558",
                           "type": "vLLM", "modelname": "m1",
                           "block_size": 16, "dp_rank": 2,
                           "tenant_id": "t1", "additionalsalt": "s1"},
            "inst-moon": {"endpoint": "tcp://127.0.0.1:6557",
                           "type": "Mooncake", "modelname": "m2",
                           "block_size": 32},
            "inst-bad":  {"endpoint": "tcp://127.0.0.1:7557",
                           "type": "SGLang", "modelname": "m3"}
          }
        })";
    }
    guard.SetPath(path);

    int port = 13333;
    auto services = conductor::kvevent::ParseConfig(&port);
    EXPECT_EQ(port, 14444);
    ASSERT_EQ(services.size(), 2u);  // SGLang entry skipped

    std::sort(services.begin(), services.end(),
              [](const auto& a, const auto& b) {
                  return a.instance_id < b.instance_id;
              });
    EXPECT_EQ(services[0].instance_id, "inst-moon");
    EXPECT_EQ(services[0].type, conductor::common::kServiceTypeMooncake);
    EXPECT_EQ(services[1].instance_id, "inst-vllm");
    EXPECT_EQ(services[1].type, conductor::common::kServiceTypeVLLM);
    EXPECT_EQ(services[1].tenant_id, "t1");
    EXPECT_EQ(services[1].block_size, 16);
    EXPECT_EQ(services[1].dp_rank, 2);
    EXPECT_EQ(services[1].additional_salt, "s1");
    std::remove(path.c_str());
}

// BUG (preserved): a parsed file with no http_server_port field sets the
// port to 0 unconditionally, overwriting any prior value.
TEST(ParseConfig, MissingPortFieldZeroesPortBugForBug) {
    ConfigEnvGuard guard;
    const std::string path = ::testing::TempDir() + "conductor_cfg_np.json";
    {
        std::ofstream out(path);
        out << R"({"kvevent_instance": {}})";
    }
    guard.SetPath(path);

    int port = 13333;
    conductor::kvevent::ParseConfig(&port);
    EXPECT_EQ(port, 0);
    std::remove(path.c_str());
}

// JSON parse failure exits — verified with a death test.
TEST(ParseConfigDeathTest, MalformedJsonExits) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ConfigEnvGuard guard;
    const std::string path = ::testing::TempDir() + "conductor_cfg_bad.json";
    {
        std::ofstream out(path);
        out << "{not json";
    }
    guard.SetPath(path);

    int port = 0;
    EXPECT_EXIT(conductor::kvevent::ParseConfig(&port),
                ::testing::ExitedWithCode(1), "");
    std::remove(path.c_str());
}

}  // namespace
