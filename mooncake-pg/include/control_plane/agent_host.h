#ifndef MOONCAKE_PG_AGENT_HOST_H
#define MOONCAKE_PG_AGENT_HOST_H

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <torch/csrc/distributed/c10d/Store.hpp>

#include "agent.h"
#include "rpc.h"
#include "serialized_executor.h"
#include "link_manager.h"

#include "pg_utils.h"

namespace mooncake {

class RpcServer;
class RpcClient;
class MooncakeBackend;

// =========================================================================
// Control Plane Architecture (Agent side)
// =========================================================================
//
// Each rank runs one AgentHost.  It owns the AgentStateMachine (pure state
// machine) and drives it via a SerializedExecutor.
//
//   MooncakeBackend                  AgentHost
//   +-----------------+              +---------------------------+
//   | proposeActivate |-> (sync) --->| call Coordinator RPC      |
//   | registerGroup   |-> post() --->| agent_.registerGroup()    |
//   | pushObservation |-> enqueue -->| observation_queue_        |
//   +-----------------+              +---------------------------+
//                                            |
//                                    SerializedExecutor (tick)
//                                            |
//                              +--------------------------------+
//                              | AgentStateMachine              |
//                              |  (pure state machine, no I/O)  |
//                              +--------------------------------+
//                                            |
//                                    returns Effect list
//                                            |
//                              +--------------------------------+
//                              | runEffects()                   |
//                              |  EnablePeerProbe -> LinkManager|
//                              |  SendObservation -> RPC        |
//                              |  ApplyViewToBackend -> backend |
//                              |  NotifyTEUnreachable -> fanout |
//                              |              ...               |
//                              +--------------------------------+
//
//   Coordinator pushes:                LinkManager events:
//   onPeerJoined -> postPeerJoined()   LinkUp/LinkDown -> postTELinkEvent()
//   onRankStateUpdate -> post...()     (both post to executor)
//   onViewUpdate -> post...()
//
// The Agent never makes autonomous decisions about health or membership.
// It strictly follows the Coordinator's authoritative broadcasts.

// AgentInterface  - control-plane service interface exposed to MooncakeBackend.
class AgentInterface {
   public:
    virtual ~AgentInterface() = default;

    virtual bool waitUntilRegistered(std::chrono::milliseconds timeout) = 0;

    virtual GroupView waitUntilGroupReady(
        GroupId group_id, std::chrono::milliseconds timeout) = 0;

    virtual void registerGroup(GroupDeclaration declaration,
                               MooncakeBackend* backend) = 0;

    virtual void unregisterGroup(GroupId group_id) = 0;

    virtual GroupView getGroupView(GroupId group_id) = 0;

    virtual void publishLocalEndpoint(GroupEndpointPublication endpoint) = 0;

    virtual ProposeViewUpdateResponse proposeActivate(
        GroupId group_id, const std::vector<GlobalRank>& ranks) = 0;

    virtual ProposeViewUpdateResponse proposeDeactivate(
        GroupId group_id, const std::vector<GlobalRank>& ranks) = 0;

    virtual void pushTransferObservation(
        GroupId group_id, std::vector<uint8_t> attempted_ranks,
        std::vector<uint8_t> failed_ranks,
        std::vector<uint8_t> succeeded_ranks) = 0;
};

class AgentHost;

// AgentRpcServiceImpl  - thin RPC handler for Coordinator->Agent pushes.
class AgentRpcServiceImpl : public AgentRpcService {
   public:
    explicit AgentRpcServiceImpl(AgentHost& host) : host_(host) {}

    void onPeerJoined(PeerJoinedPush push) override;
    void onRankStateUpdate(RankStateUpdatePush push) override;
    void onViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                      ViewUpdatePush push) override;

   private:
    AgentHost& host_;
};

// AgentHost - execution host for the agent state machine.
class AgentHost : public AgentInterface {
   public:
    static constexpr auto kCoordinatorAddrTimeout = std::chrono::seconds(30);
    static constexpr auto kCoordinatorAddrPollInterval =
        std::chrono::milliseconds(100);

    AgentHost(c10::intrusive_ptr<c10d::Store> store, const std::string& host_ip,
              GlobalRank rank, int max_world_size, LinkManager& link_manager);

    ~AgentHost() override;

    void start();
    void shutdown();

    bool waitUntilRegistered(std::chrono::milliseconds timeout) override;
    GroupView waitUntilGroupReady(GroupId group_id,
                                  std::chrono::milliseconds timeout) override;

    void registerGroup(GroupDeclaration declaration,
                       MooncakeBackend* backend) override;
    void unregisterGroup(GroupId group_id) override;
    GroupView getGroupView(GroupId group_id) override;
    void publishLocalEndpoint(GroupEndpointPublication endpoint) override;

    ProposeViewUpdateResponse proposeActivate(
        GroupId group_id, const std::vector<GlobalRank>& ranks) override;

    ProposeViewUpdateResponse proposeDeactivate(
        GroupId group_id, const std::vector<GlobalRank>& ranks) override;

    void pushTransferObservation(GroupId group_id,
                                 std::vector<uint8_t> attempted_ranks,
                                 std::vector<uint8_t> failed_ranks,
                                 std::vector<uint8_t> succeeded_ranks) override;

    void postPeerJoined(PeerJoinedPush push);
    void postRankStateUpdate(RankStateUpdatePush push);
    void postViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                        ViewUpdatePush push);

    void postTELinkEvent(TELinkEvent event);

   private:
    AgentStateMachine agent_;
    SerializedExecutor executor_;

    // Process-level TE link manager (non-owning, owned by ProcessContext).
    LinkManager& link_manager_;

    c10::intrusive_ptr<c10d::Store> store_;
    std::string host_ip_;
    GlobalRank rank_;
    int max_world_size_;

    std::string coordinator_addr_;
    uint64_t agent_session_epoch_ = 0;

    // RPC infrastructure.
    std::unique_ptr<RpcServer> rpc_server_;
    std::unique_ptr<RpcClient> rpc_client_;
    std::unique_ptr<AgentRpcServiceImpl> rpc_impl_;

    // Bootstrap synchronization: one-shot latch with executor-managed promises.
    bool registration_done_ = false;
    std::vector<std::shared_ptr<std::promise<void>>> registration_promises_;

    // group_ready_promises_ is fulfilled when declareGroup returns and
    // the GroupView is applied.
    std::unordered_map<GroupId,
                       std::vector<std::shared_ptr<std::promise<GroupView>>>>
        group_ready_promises_;

    // Backend registry: for view application and link reset fanout.
    // Accessed only from the executor thread.
    std::unordered_map<GroupId, MooncakeBackend*> backends_;

    // Transfer observation queue: worker thread -> executor.
    ThreadSafeQueue<TransferObservationEvent> observation_queue_;

    void startRegisterRpc();
    void tick();

    void doRegisterGroup(GroupDeclaration declaration,
                         MooncakeBackend* backend);
    void doPublishLocalEndpoint(GroupEndpointPublication endpoint);

    void runEffects(const AgentApplyResult& effects);
    template <typename F>
    void forEachBackend(F&& func) {
        for (auto& [group_id, backend] : backends_) {
            func(backend);
        }
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_AGENT_HOST_H
