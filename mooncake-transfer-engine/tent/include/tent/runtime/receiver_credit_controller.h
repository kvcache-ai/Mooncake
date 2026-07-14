// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_RUNTIME_RECEIVER_CREDIT_CONTROLLER_H
#define TENT_RUNTIME_RECEIVER_CREDIT_CONTROLLER_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "tent/rpc/rpc.h"
#include "tent/runtime/receiver_credit_config.h"
#include "tent/runtime/receiver_credit_dispatch.h"
#include "tent/runtime/receiver_credit_protocol.h"

namespace mooncake::tent {

// Sender-side, nonblocking pull coordinator. At most one RPC is outstanding
// for each (target, QoS class); callers only publish bounded demand and return.
// RPC callbacks mutate the separately synchronized context/ledger models and
// never call back into TransferEngineImpl, so engine teardown cannot race a
// raw `this` callback. Runtime-queue fallback progress observes the update.
class ReceiverCreditPullController
    : public std::enable_shared_from_this<ReceiverCreditPullController> {
   public:
    static Status create(
        const ReceiverCreditRuntimeConfig& config, uint64_t sender_peer,
        std::shared_ptr<CreditPeerContextTable> contexts,
        std::shared_ptr<SenderCreditLedger> ledger,
        std::shared_ptr<ReceiverCreditPullController>& controller);

    ~ReceiverCreditPullController();

    ReceiverCreditPullController(const ReceiverCreditPullController&) = delete;
    ReceiverCreditPullController& operator=(
        const ReceiverCreditPullController&) = delete;

    // Publishes the minimum complete charge needed by blocked work. The
    // desired pull window is the larger of this charge and configured batch.
    Status request(uint64_t target_id, const std::string& server_addr,
                   uint32_t qos_class, const CreditCharge& minimum_charge);

    CreditPeerState peerState(uint64_t target_id, uint32_t qos_class) const;
    size_t peerCount() const;

    // Prevents new pulls. Outstanding callbacks retain this object until they
    // finish but become no-ops, avoiding a callback-to-engine lifetime edge.
    void stop();

   private:
    struct PeerKey {
        uint64_t target_id{0};
        uint32_t qos_class{0};
        bool operator==(const PeerKey& other) const {
            return target_id == other.target_id && qos_class == other.qos_class;
        }
    };

    struct PeerKeyHash {
        size_t operator()(const PeerKey& key) const noexcept;
    };

    struct Peer {
        std::string server_addr;
        std::array<uint64_t, kCreditResourceCount> minimum{};
        uint64_t next_request_sequence{1};
        bool in_flight{false};
        bool dirty{false};
        CreditPeerState state{CreditPeerState::Negotiating};
    };

    ReceiverCreditPullController(
        ReceiverCreditRuntimeConfig config, uint64_t sender_peer,
        std::shared_ptr<CreditPeerContextTable> contexts,
        std::shared_ptr<SenderCreditLedger> ledger);

    static Status normalize(
        const CreditCharge& charge,
        std::array<uint64_t, kCreditResourceCount>& normalized);
    void launch(PeerKey key);
    void finish(PeerKey key, Status rpc_status,
                ReceiverCreditPullResponseV1 response);
    Status applyResponse(const PeerKey& key,
                         const ReceiverCreditPullResponseV1& response,
                         bool& request_again);
    bool oldGenerationDrained(const CreditPeerContextSnapshot& context) const;

    const ReceiverCreditRuntimeConfig config_;
    const uint64_t sender_peer_;
    const std::shared_ptr<CreditPeerContextTable> contexts_;
    const std::shared_ptr<SenderCreditLedger> ledger_;
    // Engine-independent client lifetime: an in-flight callback retains the
    // controller, which in turn retains this agent until the coroutine ends.
    const std::shared_ptr<CoroRpcAgent> rpc_agent_;

    mutable std::mutex mutex_;
    bool stopped_{false};
    std::unordered_map<PeerKey, Peer, PeerKeyHash> peers_;
};

}  // namespace mooncake::tent

#endif  // TENT_RUNTIME_RECEIVER_CREDIT_CONTROLLER_H
