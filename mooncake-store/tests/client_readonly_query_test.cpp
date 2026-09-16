// Tests for Client::QueryReadOnly / Client::BatchQueryReadOnly, the read-only
// replica-metadata queries backed by the admin RPCs (GetReplicaListForAdmin /
// BatchGetReplicaListForAdmin, registered on the master RPC server).
//
// Covers:
//   1. Round trip returns the same replica descriptors as the lease-granting
//      Query / BatchQuery paths.
//   2. No lease is granted on the read-only path: the returned QueryResult is
//      already expired (lease timeout pinned to the query start time).
//   3. Missing keys surface OBJECT_NOT_FOUND in both variants.
//   4. The batch variant preserves input order and per-key errors.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

#include "client_service.h"
#include "common/network.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "types.h"

namespace mooncake::test {
namespace {

int GetTestPort(std::unordered_set<int>& used_ports) {
    for (int i = 0; i < 100; ++i) {
        int port = getFreeTcpPort();
        if (port > 0 && port < 65535 && !used_ports.contains(port)) {
            used_ports.insert(port);
            return port;
        }
    }
    return -1;
}

// Replica::Descriptor has no operator==; identity of a replica across queries
// is its master-side id, status and descriptor kind.
bool SameReplicas(const std::vector<Replica::Descriptor>& a,
                  const std::vector<Replica::Descriptor>& b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i].id != b[i].id || a[i].status != b[i].status ||
            a[i].descriptor_variant.index() !=
                b[i].descriptor_variant.index()) {
            return false;
        }
    }
    return true;
}

class ClientReadOnlyQueryTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ClientReadOnlyQueryTest");
        FLAGS_logtostderr = true;

        int master_rpc_port = GetTestPort(used_ports_);
        int master_http_port = GetTestPort(used_ports_);
        int client_port = GetTestPort(used_ports_);
        ASSERT_GT(master_rpc_port, 0);
        ASSERT_GT(master_http_port, 0);
        ASSERT_GT(client_port, 0);

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_rpc_port(master_rpc_port)
                                      .set_http_metrics_port(master_http_port)
                                      .set_http_metadata_port(0)
                                      .build()));

        client_ = RealClient::create();
        ConfigDict config = {
            {CONFIG_KEY_LOCAL_HOSTNAME,
             "127.0.0.1:" + std::to_string(client_port)},
            {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
            {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "16777216"},  // 16MB
            {CONFIG_KEY_LOCAL_BUFFER_SIZE, "16777216"},    // 16MB
            {CONFIG_KEY_PROTOCOL, "tcp"},
            {CONFIG_KEY_MASTER_SERVER_ADDR, master_.master_address()},
        };
        auto setup_result = client_->setup_internal(config);
        ASSERT_TRUE(setup_result.has_value())
            << toString(setup_result.error());

        // One resident object to query.
        std::string value(kValueSize, 'x');
        ASSERT_EQ(client_->put(
                      kKey, std::span<const char>(value.data(), value.size())),
                  0);
    }

    void TearDown() override {
        if (client_) {
            EXPECT_EQ(client_->tearDownAll(), 0);
        }
        google::ShutdownGoogleLogging();
    }

    static constexpr const char* kKey = "readonly_query_key";
    static constexpr size_t kValueSize = 4096;

    std::unordered_set<int> used_ports_;
    mooncake::testing::InProcMaster master_;
    std::shared_ptr<RealClient> client_;
};

TEST_F(ClientReadOnlyQueryTest, ReturnsSameReplicasAsLeaseGrantingQuery) {
    auto leasing = client_->client_->Query(kKey);
    ASSERT_TRUE(leasing.has_value());
    ASSERT_FALSE(leasing->replicas.empty());

    auto read_only = client_->client_->QueryReadOnly(kKey);
    ASSERT_TRUE(read_only.has_value());
    EXPECT_TRUE(SameReplicas(read_only->replicas, leasing->replicas));
    EXPECT_EQ(read_only->object_checksum, leasing->object_checksum);
}

TEST_F(ClientReadOnlyQueryTest, GrantsNoLease) {
    auto leasing = client_->client_->Query(kKey);
    ASSERT_TRUE(leasing.has_value());
    // Default lease TTL is 10s; a fresh lease must be far from expiring.
    EXPECT_FALSE(leasing->IsLeaseExpired());

    auto read_only = client_->client_->QueryReadOnly(kKey);
    ASSERT_TRUE(read_only.has_value());
    EXPECT_TRUE(read_only->IsLeaseExpired());
}

TEST_F(ClientReadOnlyQueryTest, MissingKeyReturnsObjectNotFound) {
    auto leasing = client_->client_->Query("readonly_query_missing_key");
    ASSERT_FALSE(leasing.has_value());
    EXPECT_EQ(leasing.error(), ErrorCode::OBJECT_NOT_FOUND);

    auto read_only =
        client_->client_->QueryReadOnly("readonly_query_missing_key");
    ASSERT_FALSE(read_only.has_value());
    EXPECT_EQ(read_only.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(ClientReadOnlyQueryTest, BatchPreservesOrderAndPerKeyErrors) {
    const std::vector<std::string> keys = {kKey,
                                           "readonly_query_missing_key"};

    auto leasing = client_->client_->BatchQuery(keys);
    ASSERT_EQ(leasing.size(), keys.size());
    ASSERT_TRUE(leasing[0].has_value());
    ASSERT_FALSE(leasing[1].has_value());

    auto read_only = client_->client_->BatchQueryReadOnly(keys);
    ASSERT_EQ(read_only.size(), keys.size());

    ASSERT_TRUE(read_only[0].has_value());
    EXPECT_TRUE(SameReplicas(read_only[0]->replicas, leasing[0]->replicas));
    EXPECT_TRUE(read_only[0]->IsLeaseExpired());

    ASSERT_FALSE(read_only[1].has_value());
    EXPECT_EQ(read_only[1].error(), ErrorCode::OBJECT_NOT_FOUND);
}

}  // namespace
}  // namespace mooncake::test
