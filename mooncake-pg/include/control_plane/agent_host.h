#ifndef MOONCAKE_PG_AGENT_HOST_H
#define MOONCAKE_PG_AGENT_HOST_H

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <c10/util/intrusive_ptr.h>
#include <torch/csrc/distributed/c10d/Store.hpp>

#include "agent.h"
#include "rpc.h"
#include "serialized_executor.h"
#include "link_manager.h"

#include "pg_utils.h"
#include "mooncake_backend.h"

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
//     MooncakeBackend                         AgentHost
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
//                              |  ApplyViewToBackend -> backend |
//                              |              ...               |
//                              +--------------------------------+

// AgentInterface - control-plane service interface exposed to MooncakeBackend.
class AgentInterface {
   public:
    virtual ~AgentInterface() = default;

    virtual bool waitUntilRegistered(std::chrono::milliseconds timeout) = 0;

    virtual GroupView waitUntilGroupReady(
        GroupId group_id, std::chrono::milliseconds timeout) = 0;

    virtual void waitUntilRankActive(GroupId group_id, GlobalRank rank,
                                     std::chrono::milliseconds timeout) = 0;

    virtual void registerGroup(const GroupView& group, bool auto_deactivate,
                               MooncakeBackend* backend) = 0;

    virtual void unregisterGroup(GroupId group_id) = 0;

    virtual void confirmReadyForActivation(GroupId group_id) = 0;

    virtual void publishLocalEndpoint(GroupEndpointPublication endpoint) = 0;

    virtual ProposeViewUpdateResponse proposeActivate(
        GroupId group_id, const std::vector<GlobalRank>& ranks) = 0;

    virtual ProposeViewUpdateResponse proposeDeactivate(
        GroupId group_id, const std::vector<GlobalRank>& ranks) = 0;

    virtual void pushLinkEvent(const LinkEvent& event) = 0;

    virtual SyncAfterFailureResponse syncAfterFailure(GroupId group_id) = 0;

    virtual uint64_t getAgentSessionEpoch() = 0;
};

class AgentHost;

// AgentRpcServiceImpl  - thin RPC handler for Coordinator->Agent pushes.
class AgentRpcServiceImpl : public AgentRpcService {
   public:
    explicit AgentRpcServiceImpl(AgentHost& host) : host_(host) {}

    void onPeerJoined(PeerJoinedPush push) override;
    void onRankStateUpdate(RankStatePush push) override;
    void onLinkEventReportAck(LinkEventReportAck ack) override;
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

    // Throttle repeated registerAgent error logs.
    static constexpr auto kAgentRegisterErrorLogInterval =
        std::chrono::seconds(5);
    static constexpr auto kHeartbeatInterval = std::chrono::seconds(1);

    AgentHost(c10::intrusive_ptr<c10d::Store> store, const std::string& host_ip,
              GlobalRank rank, int max_world_size, LinkManager& link_manager,
              int64_t fault_reconciliation_window_us);

    ~AgentHost() override;

    void start();
    void shutdown();

    bool waitUntilRegistered(std::chrono::milliseconds timeout) override;
    GroupView waitUntilGroupReady(GroupId group_id,
                                  std::chrono::milliseconds timeout) override;
    void waitUntilRankActive(GroupId group_id, GlobalRank rank,
                             std::chrono::milliseconds timeout) override;

    void registerGroup(const GroupView& group, bool auto_deactivate,
                       MooncakeBackend* backend) override;
    void unregisterGroup(GroupId group_id) override;
    void confirmReadyForActivation(GroupId group_id) override;
    void publishLocalEndpoint(GroupEndpointPublication endpoint) override;

    ProposeViewUpdateResponse proposeActivate(
        GroupId group_id, const std::vector<GlobalRank>& ranks) override;

    ProposeViewUpdateResponse proposeDeactivate(
        GroupId group_id, const std::vector<GlobalRank>& ranks) override;

    void pushLinkEvent(const LinkEvent& event) override;

    SyncAfterFailureResponse syncAfterFailure(GroupId group_id) override;

    uint64_t getAgentSessionEpoch() override {
        return agent_.getAgentSessionEpoch();
    }
    void postPeerJoined(PeerJoinedPush push);
    void postRankStateUpdate(RankStatePush push);
    void postLinkEventReportAck(LinkEventReportAck ack);
    void postViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                        ViewUpdatePush push);

   private:
    AgentStateMachine agent_;
    SerializedExecutor executor_;

    LinkManager& link_manager_;

    c10::intrusive_ptr<c10d::Store> store_;
    std::string host_ip_;
    GlobalRank rank_;
    int max_world_size_;

    std::string coordinator_addr_;
    uint64_t agent_session_epoch_ = 0;
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
    uint64_t agent_register_error_log_suppressed_ = 0;

    // group_ready_promises_ is fulfilled when registerGroup returns and
    // the GroupView is applied.
    std::unordered_map<GroupId,
                       std::vector<std::shared_ptr<std::promise<GroupView>>>>
        group_ready_promises_;

    // rank_active_promises_[group_id][rank] is fulfilled when a ViewUpdate
    // push activates `rank` in `group_id`.  Used by extension/replacement
    // ranks to block in MooncakeBackend::joinGroup() until recover_ranks
    // activates them.
    std::unordered_map<
        GroupId,
        std::unordered_map<GlobalRank,
                           std::vector<std::shared_ptr<std::promise<void>>>>>
        rank_active_promises_;

    // Backend registry: for view application and link reset.
    // Accessed only from the executor thread.
    std::unordered_map<GroupId, MooncakeBackend*> backends_;

    void startAgentRegistration();
    void tick();

    void sendPublishEndpointRpc(GroupEndpointPublication endpoint);

    ProposeViewUpdateResponse proposeViewUpdateInternal(
        GroupId group_id, const std::vector<GlobalRank>& ranks,
        bool is_activation);

    void runEffects(const AgentApplyResult& effects);
    template <typename F>
    void forEachBackend(F&& func) {
        for (auto& [group_id, backend] : backends_) {
            func(backend);  // backend is MooncakeBackend*
        }
    }
    template <typename F>
    void withBackend(GroupId group_id, F&& func) {
        auto it = backends_.find(group_id);
        if (it != backends_.end()) {
            func(it->second);  // it->second is MooncakeBackend*
        }
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_AGENT_HOST_H
