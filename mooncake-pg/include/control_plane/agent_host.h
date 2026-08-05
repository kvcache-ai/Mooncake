#ifndef MOONCAKE_PG_AGENT_HOST_H
#define MOONCAKE_PG_AGENT_HOST_H

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "agent.h"
#include "rpc.h"
#include "serialized_executor.h"
#include "link_manager.h"

#include "error_types.h"
namespace mooncake {

class RpcServer;
class RpcClient;
class MooncakeCommunicator;

// =========================================================================
// Control Plane Architecture (Agent side)
// =========================================================================
//
// Each rank runs one AgentHost.  It owns the AgentStateMachine (pure state
// machine) and drives it via a SerializedExecutor.
//
//   MooncakeCommunicator                      AgentHost
//   +-----------------+              +---------------------------+
//   | proposeActivate |-> (sync) --->| call Coordinator RPC      |
//   | registerGroup   |-> post() --->| agent_.registerGroup()    |
//   | pushLinkEvent   |-> post() --->| agent_.pushLinkEvent()      |
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
//                              |  SendLinkEventReport -> RPC    |
//                              | ApplyViewToCommunicator -> comm|
//                              |              ...               |
//                              +--------------------------------+

// AgentInterface - control-plane service interface exposed to
// MooncakeCommunicator.
class AgentInterface {
   public:
    virtual ~AgentInterface() = default;

    virtual PGResult<void> waitUntilRegistered(
        std::chrono::milliseconds timeout) = 0;

    virtual PGResult<GroupView> waitUntilGroupReady(
        GroupId group_id, std::chrono::milliseconds timeout) = 0;

    virtual PGResult<void> waitUntilRankActive(
        GroupId group_id, GlobalRank rank,
        std::chrono::milliseconds timeout) = 0;

    // Returns an empty GroupId when the Coordinator rejects this group. The
    // rejected group is not inserted into the process-scoped Agent state.
    virtual PGResult<GroupId> registerGroup(
        GroupBootstrapId group_bootstrap_id, int32_t max_group_size,
        std::vector<GlobalRank> rank_order,
        GroupBootstrapIdResolvePolicy resolve_policy, bool auto_deactivate,
        MooncakeCommunicator* communicator) = 0;

    virtual void detachCommunicator(GroupId group_id) = 0;

    virtual PGResult<void> unregisterGroup(GroupId group_id) = 0;

    virtual PGResult<void> confirmReadyForActivation(GroupId group_id) = 0;

    virtual PGResult<void> publishLocalEndpoint(
        GroupEndpointPublication endpoint) = 0;

    virtual PGResult<ProposeViewUpdateResponse> proposeActivate(
        GroupId group_id, const std::vector<InGroupRank>& ranks) = 0;

    virtual PGResult<ProposeViewUpdateResponse> proposeDeactivate(
        GroupId group_id, const std::vector<InGroupRank>& ranks) = 0;

    virtual void pushLinkEvent(const LinkEvent& event) = 0;

    virtual PGResult<SyncAfterFailureResponse> syncAfterFailure(
        GroupId group_id) = 0;
};

class AgentHost;

// AgentRpcServiceImpl  - thin RPC handler for Coordinator->Agent pushes.
class AgentRpcServiceImpl : public AgentRpcService {
   public:
    explicit AgentRpcServiceImpl(AgentHost& host) : host_(host) {}

    void onPeerJoined(PeerJoinedPush push) override;
    void onRankStateUpdate(RankStatePush push) override;
    void onViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                      ViewUpdatePush push) override;

   private:
    AgentHost& host_;
};

// AgentHost - execution host for the agent state machine.
class AgentHost : public AgentInterface {
   public:
    // Throttle repeated registerAgent error logs.
    static constexpr auto kAgentRegisterErrorLogInterval =
        std::chrono::seconds(5);
    static constexpr auto kHeartbeatInterval = std::chrono::seconds(1);

    AgentHost(std::string coordinator_addr, const std::string& host_ip,
              GlobalRank rank, int max_world_size, LinkManager& link_manager,
              int64_t fault_reconciliation_window_us);

    ~AgentHost() override;

    PGResult<void> start();
    void shutdown();
    void setFaultReconciliationWindow(int64_t timeout_us);

    PGResult<void> waitUntilRegistered(
        std::chrono::milliseconds timeout) override;
    PGResult<GroupView> waitUntilGroupReady(
        GroupId group_id, std::chrono::milliseconds timeout) override;
    PGResult<void> waitUntilRankActive(
        GroupId group_id, GlobalRank rank,
        std::chrono::milliseconds timeout) override;

    PGResult<GroupId> registerGroup(
        GroupBootstrapId group_bootstrap_id, int32_t max_group_size,
        std::vector<GlobalRank> rank_order,
        GroupBootstrapIdResolvePolicy resolve_policy, bool auto_deactivate,
        MooncakeCommunicator* communicator) override;
    void detachCommunicator(GroupId group_id) override;
    PGResult<void> unregisterGroup(GroupId group_id) override;
    PGResult<void> confirmReadyForActivation(GroupId group_id) override;
    PGResult<void> publishLocalEndpoint(
        GroupEndpointPublication endpoint) override;

    PGResult<ProposeViewUpdateResponse> proposeActivate(
        GroupId group_id, const std::vector<InGroupRank>& ranks) override;

    PGResult<ProposeViewUpdateResponse> proposeDeactivate(
        GroupId group_id, const std::vector<InGroupRank>& ranks) override;

    void pushLinkEvent(const LinkEvent& event) override;

    PGResult<SyncAfterFailureResponse> syncAfterFailure(
        GroupId group_id) override;

    void postPeerJoined(PeerJoinedPush push);
    void postRankStateUpdate(RankStatePush push);
    void postViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                        ViewUpdatePush push);

   private:
    AgentStateMachine agent_;
    SerializedExecutor executor_;

    LinkManager& link_manager_;

    std::string host_ip_;
    GlobalRank rank_;
    int max_world_size_;

    std::string coordinator_addr_;
    std::atomic<int64_t> fault_reconciliation_window_us_;
    uint64_t agent_session_id_ = 0;
    bool agent_session_initialized_ = false;
    std::atomic<bool> shutdown_requested_{false};
    std::chrono::steady_clock::time_point next_heartbeat_at_;

    // RPC infrastructure.
    std::unique_ptr<RpcServer> rpc_server_;
    std::unique_ptr<RpcClient> rpc_client_;
    std::unique_ptr<AgentRpcServiceImpl> rpc_impl_;

    // Bootstrap synchronization: one-shot latch with executor-managed promises.
    bool agent_registration_done_ = false;
    std::vector<std::shared_ptr<std::promise<void>>>
        agent_registration_promises_;

    // Throttling state for registerAgent error logs
    std::chrono::steady_clock::time_point last_agent_register_error_log_time_;

    // group_ready_promises_ is fulfilled when registerGroup returns and
    // the GroupView is applied.
    std::unordered_map<GroupId,
                       std::vector<std::shared_ptr<std::promise<GroupView>>>>
        group_ready_promises_;

    // rank_active_promises_[group_id][rank] is fulfilled when a ViewUpdate
    // push activates `rank` in `group_id`.  Used by extension/replacement
    // ranks to block in MooncakeCommunicator::joinGroup() until activation.
    std::unordered_map<
        GroupId,
        std::unordered_map<GlobalRank,
                           std::vector<std::shared_ptr<std::promise<void>>>>>
        rank_active_promises_;

    // Communicator registry: for view application and link reset.
    // Accessed only from the executor thread.
    std::unordered_map<GroupId, MooncakeCommunicator*> communicators_;

    void startAgentRegistration(bool start_new_session = false);
    bool shouldLogAgentRegistrationError();
    void unregisterAgent();
    void tick();

    PGResult<void> sendPublishEndpointRpc(GroupEndpointPublication endpoint);

    void sendLinkEventReport(LinkEventReport report);

    PGResult<ProposeViewUpdateResponse> proposeViewUpdateInternal(
        GroupId group_id, const std::vector<InGroupRank>& ranks,
        bool is_activation);

    void runEffects(const AgentApplyResult& effects);
    template <typename F>
    void forEachCommunicator(F&& func) {
        for (auto& [group_id, communicator] : communicators_) {
            func(communicator);
        }
    }
    template <typename F>
    void withCommunicator(GroupId group_id, F&& func) {
        auto it = communicators_.find(group_id);
        if (it != communicators_.end()) {
            func(it->second);
        }
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_AGENT_HOST_H
