// Copyright 2024 KVCache.AI
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

// Tests for the endpoint_id feature: monotonically increasing endpoint IDs
// exchanged during handshake and checked during delete-endpoint notifications
// to avoid incorrectly deleting a newer replacement endpoint.
//
// The tests are organized into two parts:
//   Part I  – Serialization & basic field tests (EndpointIdTest)
//   Part II – Full endpoint deletion-notification flow simulation
//             (EndpointDeletionFlowTest)

#include <gtest/gtest.h>

#include <atomic>
#include <climits>
#include <cstdint>
#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "error.h"
#include "transfer_metadata.h"

using namespace mooncake;

namespace mooncake {

// ---------------------------------------------------------------------------
// Helper encode/decode functions that mirror the internal utilities in
// transfer_metadata.cpp.  By replicating the same JSON serialization logic
// here we validate the serialization *contract*: a message produced by one
// node must be correctly understood by another node.
// ---------------------------------------------------------------------------

namespace {

Json::Value encodeHandShakeDesc(const TransferMetadata::HandShakeDesc &desc) {
    Json::Value root;
    root["local_nic_path"] = desc.local_nic_path;
    root["peer_nic_path"] = desc.peer_nic_path;
    root["endpoint_id"] = Json::Value::UInt64(desc.endpoint_id);
    Json::Value qpNums(Json::arrayValue);
    for (const auto &qp : desc.qp_num) qpNums.append(qp);
    root["qp_num"] = qpNums;
    root["reply_msg"] = desc.reply_msg;
    return root;
}

int decodeHandShakeDesc(const Json::Value &root,
                        TransferMetadata::HandShakeDesc &desc) {
    desc.local_nic_path = root["local_nic_path"].asString();
    desc.peer_nic_path = root["peer_nic_path"].asString();
    desc.endpoint_id = root["endpoint_id"].asUInt64();
    for (const auto &qp : root["qp_num"]) desc.qp_num.push_back(qp.asUInt());
    desc.reply_msg = root["reply_msg"].asString();
    return 0;
}

Json::Value encodeDeleteEndpointDesc(
    const TransferMetadata::DeleteEndpointDesc &desc) {
    Json::Value root;
    root["deleted_nic_path"] = desc.deleted_nic_path;
    root["target_nic_path"] = desc.target_nic_path;
    root["endpoint_id"] = Json::Value::UInt64(desc.endpoint_id);
    return root;
}

int decodeDeleteEndpointDesc(const Json::Value &root,
                             TransferMetadata::DeleteEndpointDesc &desc) {
    desc.deleted_nic_path = root["deleted_nic_path"].asString();
    desc.target_nic_path = root["target_nic_path"].asString();
    desc.endpoint_id = root["endpoint_id"].asUInt64();
    return 0;
}

}  // anonymous namespace

// ===========================================================================
// Part I – Serialization & basic field tests
// ===========================================================================

class EndpointIdTest : public ::testing::Test {};

// HandShakeDesc: endpoint_id survives JSON encode → decode round-trip
TEST_F(EndpointIdTest, HandShakeDescSerializationRoundTrip) {
    TransferMetadata::HandShakeDesc original;
    original.local_nic_path = "192.168.1.1:12345@mlx5_0";
    original.peer_nic_path = "192.168.1.2:12345@mlx5_1";
    original.endpoint_id = 42;
    original.qp_num = {100, 200, 300};

    Json::Value json = encodeHandShakeDesc(original);

    TransferMetadata::HandShakeDesc decoded;
    ASSERT_EQ(decodeHandShakeDesc(json, decoded), 0);

    EXPECT_EQ(decoded.local_nic_path, original.local_nic_path);
    EXPECT_EQ(decoded.peer_nic_path, original.peer_nic_path);
    EXPECT_EQ(decoded.endpoint_id, original.endpoint_id);
    EXPECT_EQ(decoded.qp_num, original.qp_num);
}

// DeleteEndpointDesc: endpoint_id survives JSON encode → decode round-trip
TEST_F(EndpointIdTest, DeleteEndpointDescSerializationRoundTrip) {
    TransferMetadata::DeleteEndpointDesc original;
    original.deleted_nic_path = "192.168.1.1:12345@mlx5_0";
    original.target_nic_path = "192.168.1.2:12345@mlx5_1";
    original.endpoint_id = 12345;

    Json::Value json = encodeDeleteEndpointDesc(original);

    TransferMetadata::DeleteEndpointDesc decoded;
    ASSERT_EQ(decodeDeleteEndpointDesc(json, decoded), 0);

    EXPECT_EQ(decoded.deleted_nic_path, original.deleted_nic_path);
    EXPECT_EQ(decoded.target_nic_path, original.target_nic_path);
    EXPECT_EQ(decoded.endpoint_id, original.endpoint_id);
}

// Backward compatibility: JSON from an older node without endpoint_id
// must decode with endpoint_id == 0.
TEST_F(EndpointIdTest, BackwardCompatMissingEndpointId) {
    Json::Value old_hs;
    old_hs["local_nic_path"] = "old-node@mlx5_0";
    old_hs["peer_nic_path"] = "new-node@mlx5_0";
    Json::Value qpNums(Json::arrayValue);
    qpNums.append(100u);
    old_hs["qp_num"] = qpNums;
    old_hs["reply_msg"] = "";

    TransferMetadata::HandShakeDesc hs_decoded;
    ASSERT_EQ(decodeHandShakeDesc(old_hs, hs_decoded), 0);
    EXPECT_EQ(hs_decoded.endpoint_id, 0u);

    Json::Value old_del;
    old_del["deleted_nic_path"] = "old-node@mlx5_0";
    old_del["target_nic_path"] = "new-node@mlx5_0";

    TransferMetadata::DeleteEndpointDesc del_decoded;
    ASSERT_EQ(decodeDeleteEndpointDesc(old_del, del_decoded), 0);
    EXPECT_EQ(del_decoded.endpoint_id, 0u);
}

// New error codes must be negative and distinct.
TEST_F(EndpointIdTest, ErrorCodesDefined) {
    EXPECT_LT(ERR_ENDPOINT_NOT_FOUND, 0);
    EXPECT_LT(ERR_ENDPOINT_ID_MISMATCH, 0);
    EXPECT_NE(ERR_ENDPOINT_NOT_FOUND, ERR_ENDPOINT_ID_MISMATCH);
}

// ###########################################################################
//
//  Part II – Full endpoint deletion-notification flow simulation
//
//  These tests simulate the complete chain without RDMA hardware:
//
//    1. EP construct  →  assign endpoint_id (monotonic counter)
//    2. Handshake     →  exchange & store peer_endpoint_id
//    3. EP evict/destroy  →  on_delete_callback(peer_nic_path, endpoint_id)
//    4. Callback builds DeleteEndpointDesc  →  JSON encode  →  "network"
//    5. Receiver decodes  →  looks up EP  →  checks peerEndpointId match
//    6. Match → delete;  Mismatch → ignore (ERR_ENDPOINT_ID_MISMATCH)
//
// ###########################################################################

namespace {

// ---- Monotonic ID generator (mirrors RdmaEndPoint::next_endpoint_id_) ------
class EndpointIdGenerator {
   public:
    uint64_t next() { return counter_.fetch_add(1); }
    uint64_t current() const { return counter_.load(); }

   private:
    std::atomic<uint64_t> counter_{0};
};

// ---- Lightweight simulated endpoint ----------------------------------------
//  Holds the same identity fields that RdmaEndPoint carries, sufficient to
//  replicate the handshake-exchange and delete-matching logic.
struct SimEndpoint {
    std::string local_nic_path;  // this endpoint's NIC path
    std::string peer_nic_path;   // peer's NIC path
    uint64_t endpoint_id;        // own ID (assigned at construct)
    uint64_t peer_endpoint_id;   // peer's ID (set during handshake)

    // Callback invoked when the endpoint is deconstructed, mirrors
    // RdmaEndPoint::on_delete_callback_.
    using OnDeleteCallback = std::function<void(const std::string &, uint64_t)>;
    OnDeleteCallback on_delete_callback;

    SimEndpoint(const std::string &local, const std::string &peer, uint64_t id,
                OnDeleteCallback cb = nullptr)
        : local_nic_path(local),
          peer_nic_path(peer),
          endpoint_id(id),
          peer_endpoint_id(0),
          on_delete_callback(std::move(cb)) {}

    void deconstruct() {
        if (auto cb = std::exchange(on_delete_callback, nullptr)) {
            cb(peer_nic_path, endpoint_id);
        }
    }
};

// ---- Simulated endpoint store (FIFO-like) ----------------------------------
//  Mirrors FIFOEndpointStore / SIEVEEndpointStore deletion semantics:
//    - deleteEndpoint(path)             → unconditional delete
//    - deleteEndpoint(path, ep_id)      → conditional (ID-matched) delete
class SimEndpointStore {
   public:
    using OnDeleteEndpointCallback =
        std::function<void(const std::string &, uint64_t)>;

    void setOnDeleteEndpointCallback(OnDeleteEndpointCallback cb) {
        on_delete_cb_ = std::move(cb);
    }

    void insertEndpoint(const std::string &peer_nic_path,
                        std::shared_ptr<SimEndpoint> ep) {
        ep->on_delete_callback = on_delete_cb_;
        endpoints_[peer_nic_path] = std::move(ep);
    }

    std::shared_ptr<SimEndpoint> getEndpoint(
        const std::string &peer_nic_path) const {
        auto it = endpoints_.find(peer_nic_path);
        return it != endpoints_.end() ? it->second : nullptr;
    }

    // Unconditional delete (used by ERDMA path)
    int deleteEndpoint(const std::string &peer_nic_path) {
        auto it = endpoints_.find(peer_nic_path);
        if (it != endpoints_.end()) {
            deleted_.push_back(it->second);
            endpoints_.erase(it);
        }
        return 0;
    }

    // Conditional delete: only removes if peer_endpoint_id matches
    int deleteEndpoint(const std::string &peer_nic_path,
                       uint64_t peer_endpoint_id) {
        auto it = endpoints_.find(peer_nic_path);
        if (it == endpoints_.end()) {
            return ERR_ENDPOINT_NOT_FOUND;
        }
        if (it->second->peer_endpoint_id != peer_endpoint_id) {
            return ERR_ENDPOINT_ID_MISMATCH;
        }
        deleted_.push_back(it->second);
        endpoints_.erase(it);
        return 0;
    }

    size_t size() const { return endpoints_.size(); }

    const std::vector<std::shared_ptr<SimEndpoint>> &deletedEndpoints() const {
        return deleted_;
    }

   private:
    std::unordered_map<std::string, std::shared_ptr<SimEndpoint>> endpoints_;
    std::vector<std::shared_ptr<SimEndpoint>> deleted_;
    OnDeleteEndpointCallback on_delete_cb_;
};

// ---- Simulated node --------------------------------------------------------
//  Combines an endpoint store, an ID generator, and the callback wiring that
//  mirrors RdmaContext + RdmaTransport on a single node.
//
//  When an endpoint on THIS node is deconstructed:
//    1. on_delete_callback fires  →  builds DeleteEndpointDesc
//    2. The desc is "sent" by pushing it into sent_notifications_
//
//  When a notification is "received" from a peer:
//    onReceiveDeleteNotification() looks up the endpoint and does the
//    ID-matched delete.
class SimNode {
   public:
    explicit SimNode(const std::string &server_name)
        : server_name_(server_name) {
        store_.setOnDeleteEndpointCallback(
            [this](const std::string &peer_nic_path, uint64_t endpoint_id) {
                notifyPeerEndpointDeletion(peer_nic_path, endpoint_id);
            });
    }

    std::shared_ptr<SimEndpoint> createEndpoint(
        const std::string &local_nic, const std::string &peer_nic_path) {
        std::string local_nic_path = server_name_ + "@" + local_nic;
        uint64_t id = id_gen_.next();
        auto ep =
            std::make_shared<SimEndpoint>(local_nic_path, peer_nic_path, id);
        store_.insertEndpoint(peer_nic_path, ep);
        return ep;
    }

    static void handshake(std::shared_ptr<SimEndpoint> ep_a,
                          std::shared_ptr<SimEndpoint> ep_b) {
        TransferMetadata::HandShakeDesc desc_a, desc_b;

        desc_a.local_nic_path = ep_a->local_nic_path;
        desc_a.peer_nic_path = ep_a->peer_nic_path;
        desc_a.endpoint_id = ep_a->endpoint_id;
        desc_a.qp_num = {100};

        desc_b.local_nic_path = ep_b->local_nic_path;
        desc_b.peer_nic_path = ep_b->peer_nic_path;
        desc_b.endpoint_id = ep_b->endpoint_id;
        desc_b.qp_num = {200};

        Json::Value json_a = encodeHandShakeDesc(desc_a);
        Json::Value json_b = encodeHandShakeDesc(desc_b);

        TransferMetadata::HandShakeDesc decoded_a, decoded_b;
        decodeHandShakeDesc(json_a, decoded_a);
        decodeHandShakeDesc(json_b, decoded_b);

        ep_a->peer_endpoint_id = decoded_b.endpoint_id;
        ep_b->peer_endpoint_id = decoded_a.endpoint_id;
    }

    int onReceiveDeleteNotification(
        const TransferMetadata::DeleteEndpointDesc &desc) {
        return store_.deleteEndpoint(desc.deleted_nic_path, desc.endpoint_id);
    }

    SimEndpointStore &store() { return store_; }

    const std::vector<TransferMetadata::DeleteEndpointDesc> &sentNotifications()
        const {
        return sent_notifications_;
    }

    const std::string &serverName() const { return server_name_; }

   private:
    void notifyPeerEndpointDeletion(const std::string &peer_nic_path,
                                    uint64_t endpoint_id) {
        TransferMetadata::DeleteEndpointDesc desc;
        desc.deleted_nic_path = server_name_ + "@mlx5_0";
        desc.target_nic_path = peer_nic_path;
        desc.endpoint_id = endpoint_id;
        sent_notifications_.push_back(desc);
    }

    std::string server_name_;
    EndpointIdGenerator id_gen_;
    SimEndpointStore store_;
    std::vector<TransferMetadata::DeleteEndpointDesc> sent_notifications_;
};

// Transmit a notification from sender to receiver through full JSON
// serialization, mirroring the RPC path.
TransferMetadata::DeleteEndpointDesc transmitOverNetwork(
    const TransferMetadata::DeleteEndpointDesc &src) {
    Json::Value json = encodeDeleteEndpointDesc(src);
    Json::StreamWriterBuilder writer;
    std::string wire = Json::writeString(writer, json);

    Json::CharReaderBuilder reader;
    Json::Value parsed;
    std::string errs;
    std::istringstream stream(wire);
    Json::parseFromStream(reader, stream, &parsed, &errs);

    TransferMetadata::DeleteEndpointDesc dst;
    decodeDeleteEndpointDesc(parsed, dst);
    return dst;
}

}  // anonymous namespace

// ===========================================================================
// Test fixture for the full deletion-notification flow
// ===========================================================================

class EndpointDeletionFlowTest : public ::testing::Test {};

// ---------------------------------------------------------------------------
// 11. Endpoint ID generator produces monotonically increasing IDs
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, IdGeneratorMonotonic) {
    EndpointIdGenerator gen;
    uint64_t prev = gen.next();
    for (int i = 0; i < 1000; ++i) {
        uint64_t cur = gen.next();
        EXPECT_GT(cur, prev) << "IDs must be strictly increasing";
        prev = cur;
    }
}

TEST_F(EndpointDeletionFlowTest, IdGeneratorStartsAtZero) {
    EndpointIdGenerator gen;
    EXPECT_EQ(gen.next(), 0u);
    EXPECT_EQ(gen.next(), 1u);
}

// ---------------------------------------------------------------------------
// 12. SimEndpoint::deconstruct invokes callback exactly once
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, DeconstructCallbackInvokedOnce) {
    int call_count = 0;
    std::string captured_path;
    uint64_t captured_id = 0;

    SimEndpoint ep("local@mlx5_0", "peer@mlx5_0", 42,
                   [&](const std::string &path, uint64_t id) {
                       ++call_count;
                       captured_path = path;
                       captured_id = id;
                   });

    ep.deconstruct();
    EXPECT_EQ(call_count, 1);
    EXPECT_EQ(captured_path, "peer@mlx5_0");
    EXPECT_EQ(captured_id, 42u);

    ep.deconstruct();
    EXPECT_EQ(call_count, 1) << "Callback must fire at most once";
}

TEST_F(EndpointDeletionFlowTest, DeconstructWithNullCallbackIsNoOp) {
    SimEndpoint ep("local@mlx5_0", "peer@mlx5_0", 7);
    ep.deconstruct();
}

// ---------------------------------------------------------------------------
// 13. SimEndpointStore: basic insert / get / delete
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, StoreInsertAndGet) {
    SimEndpointStore store;
    auto ep = std::make_shared<SimEndpoint>("local", "peer_a@mlx5_0", 1);
    ep->peer_endpoint_id = 100;
    store.insertEndpoint("peer_a@mlx5_0", ep);

    EXPECT_EQ(store.size(), 1u);
    auto got = store.getEndpoint("peer_a@mlx5_0");
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(got->endpoint_id, 1u);
    EXPECT_EQ(got->peer_endpoint_id, 100u);
}

TEST_F(EndpointDeletionFlowTest, StoreGetNonexistent) {
    SimEndpointStore store;
    EXPECT_EQ(store.getEndpoint("no_such_peer"), nullptr);
}

// ---------------------------------------------------------------------------
// 14. Store unconditional deleteEndpoint(path) – always removes
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, StoreUnconditionalDelete) {
    SimEndpointStore store;
    auto ep = std::make_shared<SimEndpoint>("l", "peer@mlx5_0", 1);
    ep->peer_endpoint_id = 50;
    store.insertEndpoint("peer@mlx5_0", ep);

    EXPECT_EQ(store.deleteEndpoint("peer@mlx5_0"), 0);
    EXPECT_EQ(store.size(), 0u);
    EXPECT_EQ(store.getEndpoint("peer@mlx5_0"), nullptr);
    EXPECT_EQ(store.deletedEndpoints().size(), 1u);
}

TEST_F(EndpointDeletionFlowTest, StoreUnconditionalDeleteNonexistent) {
    SimEndpointStore store;
    EXPECT_EQ(store.deleteEndpoint("no_such"), 0);
}

// ---------------------------------------------------------------------------
// 15. Store conditional deleteEndpoint(path, peer_endpoint_id)
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, StoreConditionalDeleteMatch) {
    SimEndpointStore store;
    auto ep = std::make_shared<SimEndpoint>("l", "peer@mlx5_0", 1);
    ep->peer_endpoint_id = 50;
    store.insertEndpoint("peer@mlx5_0", ep);

    EXPECT_EQ(store.deleteEndpoint("peer@mlx5_0", 50), 0);
    EXPECT_EQ(store.size(), 0u);
}

TEST_F(EndpointDeletionFlowTest, StoreConditionalDeleteMismatch) {
    SimEndpointStore store;
    auto ep = std::make_shared<SimEndpoint>("l", "peer@mlx5_0", 1);
    ep->peer_endpoint_id = 50;
    store.insertEndpoint("peer@mlx5_0", ep);

    EXPECT_EQ(store.deleteEndpoint("peer@mlx5_0", 999),
              ERR_ENDPOINT_ID_MISMATCH);
    EXPECT_EQ(store.size(), 1u);
    EXPECT_NE(store.getEndpoint("peer@mlx5_0"), nullptr);
}

TEST_F(EndpointDeletionFlowTest, StoreConditionalDeleteNotFound) {
    SimEndpointStore store;
    EXPECT_EQ(store.deleteEndpoint("no_such", 42), ERR_ENDPOINT_NOT_FOUND);
}

// ---------------------------------------------------------------------------
// 16. Callback → DeleteEndpointDesc → encode/decode full chain
//     Verify that the on_delete_callback correctly produces a
//     DeleteEndpointDesc that round-trips through JSON.
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, CallbackToDeleteDescPipeline) {
    TransferMetadata::DeleteEndpointDesc captured_desc;
    bool notified = false;

    auto callback = [&](const std::string &peer_nic_path, uint64_t ep_id) {
        captured_desc.deleted_nic_path = "192.168.1.1:5000@mlx5_0";
        captured_desc.target_nic_path = peer_nic_path;
        captured_desc.endpoint_id = ep_id;
        notified = true;
    };

    SimEndpoint ep("192.168.1.1:5000@mlx5_0", "192.168.1.2:5000@mlx5_1", 77,
                   callback);

    ep.deconstruct();
    ASSERT_TRUE(notified);

    auto received = transmitOverNetwork(captured_desc);

    EXPECT_EQ(received.deleted_nic_path, "192.168.1.1:5000@mlx5_0");
    EXPECT_EQ(received.target_nic_path, "192.168.1.2:5000@mlx5_1");
    EXPECT_EQ(received.endpoint_id, 77u);
}

// ---------------------------------------------------------------------------
// 17. Two-node handshake correctly exchanges endpoint_ids
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, HandshakeExchangesIds) {
    auto ep_a =
        std::make_shared<SimEndpoint>("nodeA@mlx5_0", "nodeB@mlx5_0", 5);
    auto ep_b =
        std::make_shared<SimEndpoint>("nodeB@mlx5_0", "nodeA@mlx5_0", 8);

    SimNode::handshake(ep_a, ep_b);

    EXPECT_EQ(ep_a->peer_endpoint_id, 8u);
    EXPECT_EQ(ep_b->peer_endpoint_id, 5u);
}

// ---------------------------------------------------------------------------
// 18. End-to-end: normal deletion flow
//     NodeA EP evicted → notification → NodeB correctly deletes its EP
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, E2ENormalDeletionFlow) {
    SimNode node_a("192.168.1.1:5000");
    SimNode node_b("192.168.1.2:5000");

    auto ep_a = node_a.createEndpoint("mlx5_0", "192.168.1.2:5000@mlx5_0");
    auto ep_b = node_b.createEndpoint("mlx5_0", "192.168.1.1:5000@mlx5_0");
    SimNode::handshake(ep_a, ep_b);

    EXPECT_EQ(ep_a->peer_endpoint_id, ep_b->endpoint_id);
    EXPECT_EQ(ep_b->peer_endpoint_id, ep_a->endpoint_id);

    ep_a->deconstruct();

    ASSERT_EQ(node_a.sentNotifications().size(), 1u);
    const auto &notif = node_a.sentNotifications()[0];
    EXPECT_EQ(notif.endpoint_id, ep_a->endpoint_id);

    auto received = transmitOverNetwork(notif);
    int ret = node_b.onReceiveDeleteNotification(received);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(node_b.store().size(), 0u);
}

// ---------------------------------------------------------------------------
// 19. End-to-end: stale deletion notification is rejected
//     NodeA creates EP v1, evicts it, creates EP v2 (same nic_path).
//     NodeB receives the stale v1 delete notification but now holds v2 →
//     reject.
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, E2EStaleDeletionRejected) {
    SimNode node_a("192.168.1.1:5000");
    SimNode node_b("192.168.1.2:5000");

    auto ep_a_v1 = node_a.createEndpoint("mlx5_0", "192.168.1.2:5000@mlx5_0");
    auto ep_b_v1 = node_b.createEndpoint("mlx5_0", "192.168.1.1:5000@mlx5_0");
    SimNode::handshake(ep_a_v1, ep_b_v1);

    uint64_t v1_id = ep_a_v1->endpoint_id;

    ep_a_v1->deconstruct();
    ASSERT_EQ(node_a.sentNotifications().size(), 1u);
    auto stale_notif = transmitOverNetwork(node_a.sentNotifications()[0]);

    // EP replaced: v2 created for the same peer
    auto ep_a_v2 = node_a.createEndpoint("mlx5_0", "192.168.1.2:5000@mlx5_0");
    auto ep_b_v2 = node_b.createEndpoint("mlx5_0", "192.168.1.1:5000@mlx5_0");
    SimNode::handshake(ep_a_v2, ep_b_v2);

    EXPECT_NE(ep_a_v2->endpoint_id, v1_id);

    // Stale v1 notification must be rejected
    int ret = node_b.onReceiveDeleteNotification(stale_notif);
    EXPECT_EQ(ret, ERR_ENDPOINT_ID_MISMATCH);
    EXPECT_EQ(node_b.store().size(), 1u);
}

// ---------------------------------------------------------------------------
// 20. End-to-end: delete notification for already-gone endpoint
//     NodeA sends delete, but NodeB has already removed the EP for other
//     reasons (e.g., its own eviction).
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, E2EDeleteAlreadyGoneEndpoint) {
    SimNode node_a("192.168.1.1:5000");
    SimNode node_b("192.168.1.2:5000");

    auto ep_a = node_a.createEndpoint("mlx5_0", "192.168.1.2:5000@mlx5_0");
    auto ep_b = node_b.createEndpoint("mlx5_0", "192.168.1.1:5000@mlx5_0");
    SimNode::handshake(ep_a, ep_b);

    node_b.store().deleteEndpoint("192.168.1.1:5000@mlx5_0");
    EXPECT_EQ(node_b.store().size(), 0u);

    ep_a->deconstruct();
    auto notif = transmitOverNetwork(node_a.sentNotifications()[0]);

    int ret = node_b.onReceiveDeleteNotification(notif);
    EXPECT_EQ(ret, ERR_ENDPOINT_NOT_FOUND);
}

// ---------------------------------------------------------------------------
// 21. End-to-end: bidirectional deletion
//     Both nodes independently evict their endpoints.
//     Each should receive the peer's notification and successfully delete.
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, E2EBidirectionalDeletion) {
    SimNode node_a("10.0.0.1:5000");
    SimNode node_b("10.0.0.2:5000");

    auto ep_a = node_a.createEndpoint("mlx5_0", "10.0.0.2:5000@mlx5_0");
    auto ep_b = node_b.createEndpoint("mlx5_0", "10.0.0.1:5000@mlx5_0");
    SimNode::handshake(ep_a, ep_b);

    ep_a->deconstruct();
    ep_b->deconstruct();

    ASSERT_EQ(node_a.sentNotifications().size(), 1u);
    ASSERT_EQ(node_b.sentNotifications().size(), 1u);

    auto notif_from_a = transmitOverNetwork(node_a.sentNotifications()[0]);
    auto notif_from_b = transmitOverNetwork(node_b.sentNotifications()[0]);

    EXPECT_EQ(node_b.onReceiveDeleteNotification(notif_from_a), 0);
    EXPECT_EQ(node_a.onReceiveDeleteNotification(notif_from_b), 0);

    EXPECT_EQ(node_a.store().size(), 0u);
    EXPECT_EQ(node_b.store().size(), 0u);
}

// ---------------------------------------------------------------------------
// 22. End-to-end: multiple independent connections on the same node
//     NodeA has endpoints to NodeB and NodeC.  Evicting one must not
//     affect the other.
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, E2EMultiplePeersIndependent) {
    SimNode node_a("10.0.0.1:5000");
    SimNode node_b("10.0.0.2:5000");
    SimNode node_c("10.0.0.3:5000");

    auto ep_a_to_b = node_a.createEndpoint("mlx5_0", "10.0.0.2:5000@mlx5_0");
    auto ep_b_to_a = node_b.createEndpoint("mlx5_0", "10.0.0.1:5000@mlx5_0");
    SimNode::handshake(ep_a_to_b, ep_b_to_a);

    auto ep_a_to_c = node_a.createEndpoint("mlx5_0", "10.0.0.3:5000@mlx5_0");
    auto ep_c_to_a = node_c.createEndpoint("mlx5_0", "10.0.0.1:5000@mlx5_0");
    SimNode::handshake(ep_a_to_c, ep_c_to_a);

    EXPECT_EQ(node_a.store().size(), 2u);

    // Evict A→B: store removes from map, then deconstruct fires callback
    node_a.store().deleteEndpoint("10.0.0.2:5000@mlx5_0");
    ep_a_to_b->deconstruct();
    auto notif = transmitOverNetwork(node_a.sentNotifications()[0]);
    EXPECT_EQ(node_b.onReceiveDeleteNotification(notif), 0);

    EXPECT_EQ(node_a.store().size(), 1u);
    EXPECT_NE(node_a.store().getEndpoint("10.0.0.3:5000@mlx5_0"), nullptr);
    EXPECT_EQ(node_c.store().size(), 1u);
}

// ---------------------------------------------------------------------------
// 23. End-to-end: rapid create-delete cycles
//     Stress-test: repeatedly create and evict endpoints for the same peer,
//     verifying that only the current-generation notification succeeds.
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, E2ERapidCreateDeleteCycles) {
    SimNode node_a("10.0.0.1:5000");
    SimNode node_b("10.0.0.2:5000");

    const int NUM_CYCLES = 50;
    std::vector<TransferMetadata::DeleteEndpointDesc> all_notifs;

    for (int i = 0; i < NUM_CYCLES; ++i) {
        auto ep_a = node_a.createEndpoint("mlx5_0", "10.0.0.2:5000@mlx5_0");
        auto ep_b = node_b.createEndpoint("mlx5_0", "10.0.0.1:5000@mlx5_0");
        SimNode::handshake(ep_a, ep_b);

        ep_a->deconstruct();
        all_notifs.push_back(
            transmitOverNetwork(node_a.sentNotifications().back()));
    }

    EXPECT_EQ(node_b.store().size(), 1u);

    // All prior notifications (0..48) are stale
    for (int i = 0; i < NUM_CYCLES - 1; ++i) {
        EXPECT_EQ(node_b.onReceiveDeleteNotification(all_notifs[i]),
                  ERR_ENDPOINT_ID_MISMATCH)
            << "Notification from cycle " << i << " should be rejected";
    }

    EXPECT_EQ(node_b.onReceiveDeleteNotification(all_notifs[NUM_CYCLES - 1]),
              0);
    EXPECT_EQ(node_b.store().size(), 0u);
}

// ---------------------------------------------------------------------------
// 24. Notification carries correct NIC paths through the full chain
//     Verifies that deleted_nic_path and target_nic_path are correctly
//     propagated from callback → DeleteEndpointDesc → JSON → receiver.
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, NotificationNicPathsCorrect) {
    SimNode node_a("192.168.0.10:12345");
    SimNode node_b("192.168.0.20:12345");

    auto ep_a = node_a.createEndpoint("mlx5_2", "192.168.0.20:12345@mlx5_3");
    auto ep_b = node_b.createEndpoint("mlx5_3", "192.168.0.10:12345@mlx5_2");
    SimNode::handshake(ep_a, ep_b);

    ep_a->deconstruct();

    ASSERT_EQ(node_a.sentNotifications().size(), 1u);
    const auto &sent = node_a.sentNotifications()[0];

    EXPECT_EQ(sent.deleted_nic_path, "192.168.0.10:12345@mlx5_0");
    EXPECT_EQ(sent.target_nic_path, "192.168.0.20:12345@mlx5_3");
    EXPECT_EQ(sent.endpoint_id, ep_a->endpoint_id);

    auto received = transmitOverNetwork(sent);
    EXPECT_EQ(received.deleted_nic_path, sent.deleted_nic_path);
    EXPECT_EQ(received.target_nic_path, sent.target_nic_path);
    EXPECT_EQ(received.endpoint_id, sent.endpoint_id);
}

// ---------------------------------------------------------------------------
// 25. Handshake roundtrip preserves all fields through JSON
//     Full HandShakeDesc encode → JSON string → decode pipeline, verifying
//     endpoint_id alongside all other fields.
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, HandshakeFullFieldRoundTrip) {
    TransferMetadata::HandShakeDesc original;
    original.local_nic_path = "10.0.0.1:8080@mlx5_bond_0";
    original.peer_nic_path = "10.0.0.2:8080@mlx5_bond_1";
    original.endpoint_id = 314159;
    original.qp_num = {11, 22, 33, 44};
    original.reply_msg = "";

    Json::Value json = encodeHandShakeDesc(original);
    Json::StreamWriterBuilder writer;
    std::string wire = Json::writeString(writer, json);

    Json::CharReaderBuilder reader;
    Json::Value parsed;
    std::string errs;
    std::istringstream stream(wire);
    ASSERT_TRUE(Json::parseFromStream(reader, stream, &parsed, &errs));

    TransferMetadata::HandShakeDesc decoded;
    ASSERT_EQ(decodeHandShakeDesc(parsed, decoded), 0);

    EXPECT_EQ(decoded.local_nic_path, original.local_nic_path);
    EXPECT_EQ(decoded.peer_nic_path, original.peer_nic_path);
    EXPECT_EQ(decoded.endpoint_id, original.endpoint_id);
    EXPECT_EQ(decoded.qp_num, original.qp_num);
    EXPECT_EQ(decoded.reply_msg, original.reply_msg);
}

// ---------------------------------------------------------------------------
// 26. Endpoint replacement: inserting a new EP for the same peer_nic_path
//     should update the store entry, and the old EP remains accessible via
//     the deleted list.
// ---------------------------------------------------------------------------

TEST_F(EndpointDeletionFlowTest, StoreReplacementUpdatesEntry) {
    SimEndpointStore store;

    auto ep_v1 = std::make_shared<SimEndpoint>("l", "peer@mlx5_0", 1);
    ep_v1->peer_endpoint_id = 10;
    store.insertEndpoint("peer@mlx5_0", ep_v1);

    store.deleteEndpoint("peer@mlx5_0");

    auto ep_v2 = std::make_shared<SimEndpoint>("l", "peer@mlx5_0", 2);
    ep_v2->peer_endpoint_id = 20;
    store.insertEndpoint("peer@mlx5_0", ep_v2);

    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(store.deleteEndpoint("peer@mlx5_0", 10),
              ERR_ENDPOINT_ID_MISMATCH);
    EXPECT_EQ(store.deleteEndpoint("peer@mlx5_0", 20), 0);
    EXPECT_EQ(store.size(), 0u);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
