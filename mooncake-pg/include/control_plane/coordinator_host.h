#ifndef MOONCAKE_PG_COORDINATOR_HOST_H
#define MOONCAKE_PG_COORDINATOR_HOST_H

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>

#include "coordinator.h"
#include "rpc.h"
#include "rpc_runtime.h"
#include "serialized_executor.h"
#include "error_types.h"

namespace mooncake {

class RpcServer;
class RpcClient;

// =========================================================================
// Control Plane Architecture (Coordinator side)
// =========================================================================
//
// The CentralizedCoordinator runs inside Rank 0's process.  It is the
// authoritative source of truth for rank health (RankState) and group
// membership (GroupView).
//
//   Agent (any rank)                 CoordinatorHost (Rank 0)
//   +-----------------+              +---------------------------+
//   | registerAgent   |--- RPC ----->| postRegisterAgent()       |
//   | heartbeat       |--- RPC ----->| postHeartbeat()           |
//   | proposeViewUpd  |--- RPC ----->| postProposeViewUpdate()   |
//   | publishEndpoint |--- RPC ----->| postPublishEndpoint()     |
//   | reportLinkEvent |--- RPC ----->| postLinkEventReport()     |
//   +-----------------+              +---------------------------+
//                                            |
//                                    SerializedExecutor
//                                            |
//                              +------------------------------------+
//                              | CentralizedCoordinatorStateMachine |
//                              |  (pure state machine, no I/O)      |
//                              +------------------------------------+
//                                            |
//                                    returns Effect list
//                                            |
//                              +-----------------------------------+
//                              | runEffects()                      |
//                              |  BroadcastRankState -> broadcast  |
//                              |  PushViewUpdate -> callAsync      |
//                              |  ReplyProposal -> reply           |
//                              +-----------------------------------+
//

class CoordinatorHost;

// CoordinatorRpcServiceImpl - thin RPC handler that forwards all calls
// to CoordinatorHost::post*().
class CoordinatorRpcServiceImpl : public CoordinatorRpcService {
   public:
    explicit CoordinatorRpcServiceImpl(CoordinatorHost& host) : host_(host) {}

    void registerAgent(coro_rpc::context<RegisterAgentResponse> ctx,
                       RegisterAgentRequest req) override;

    void heartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                   HeartbeatRequest req) override;

    void unregisterAgent(coro_rpc::context<UnregisterAgentResponse> ctx,
                         UnregisterAgentRequest req) override;

    void registerGroup(coro_rpc::context<RegisterGroupResponse> ctx,
                       RegisterGroupRequest req) override;

    void unregisterGroup(coro_rpc::context<UnregisterGroupResponse> ctx,
                         UnregisterGroupRequest req) override;

    void confirmReadyForActivation(
        coro_rpc::context<ConfirmReadyForActivationResponse> ctx,
        ConfirmReadyForActivationRequest req) override;

    void publishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                         PublishEndpointRequest req) override;

    void proposeViewUpdate(coro_rpc::context<ProposeViewUpdateResponse> ctx,
                           ProposeViewUpdateRequest req) override;

    void reportLinkEvent(coro_rpc::context<LinkEventReportAck> ctx,
                         LinkEventReport req) override;

    void syncAfterFailure(coro_rpc::context<SyncAfterFailureResponse> ctx,
                          SyncAfterFailureRequest req) override;

   private:
    CoordinatorHost& host_;
};

// CoordinatorHost - execution host for the Coordinator state machine.
class CoordinatorHost {
   public:
    CoordinatorHost(const std::string& host_ip, int max_world_size,
                    int64_t fault_reconciliation_window_us);

    ~CoordinatorHost();

    PGResult<void> start();
    void shutdown();
    PGResult<void> setFaultReconciliationWindow(int64_t timeout_us);

    const std::string& getListenAddr() const { return listen_addr_; }

    void postRegisterAgent(coro_rpc::context<RegisterAgentResponse> ctx,
                           RegisterAgentRequest req);

    void postHeartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                       HeartbeatRequest req);

    void postUnregisterAgent(coro_rpc::context<UnregisterAgentResponse> ctx,
                             UnregisterAgentRequest req);

    void postRegisterGroup(coro_rpc::context<RegisterGroupResponse> ctx,
                           RegisterGroupRequest req);

    void postUnregisterGroup(coro_rpc::context<UnregisterGroupResponse> ctx,
                             UnregisterGroupRequest req);

    void postConfirmReadyForActivation(
        coro_rpc::context<ConfirmReadyForActivationResponse> ctx,
        ConfirmReadyForActivationRequest req);

    void postProposeViewUpdate(coro_rpc::context<ProposeViewUpdateResponse> ctx,
                               ProposeViewUpdateRequest req);

    void postPublishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                             PublishEndpointRequest req);

    void postLinkEventReport(coro_rpc::context<LinkEventReportAck> ctx,
                             LinkEventReport req);

    void postSyncAfterFailure(coro_rpc::context<SyncAfterFailureResponse> ctx,
                              SyncAfterFailureRequest req);

    void postViewUpdateAck(GroupId group_id, GlobalRank rank, uint64_t epoch,
                           bool applied);

   private:
    CentralizedCoordinatorStateMachine state_machine_;
    SerializedExecutor executor_;

    std::string host_ip_;
    std::string listen_addr_;
    int max_world_size_;

    // RPC infrastructure.
    std::unique_ptr<RpcServer> rpc_server_;
    std::unique_ptr<RpcClient> rpc_client_;
    std::unique_ptr<CoordinatorRpcServiceImpl> rpc_impl_;

    // Host only maintains deferred response context mapping.
    // Related states is inside CentralizedCoordinatorStateMachine;

    uint64_t next_propose_id_{1};
    std::unordered_map<uint64_t, coro_rpc::context<ProposeViewUpdateResponse>>
        pending_proposal_resps_;

    uint64_t next_sync_id_{1};
    std::unordered_map<uint64_t, coro_rpc::context<SyncAfterFailureResponse>>
        pending_sync_resps_;

    static constexpr auto kShutdownDrainTimeout = std::chrono::seconds(30);

    // The Host requests shutdown from the state machine and stops the RPC
    // server once all sessions in the shutdown snapshot have unregistered.
    std::atomic<bool> shutdown_requested_{false};
    std::promise<void> shutdown_confirmation_;

    void runEffects(const std::vector<CoordinatorEffect>& effects);
    void pushViewUpdate(const PushViewUpdate& effect);

    template <auto Method, typename Push>
    void pushToAgent(GlobalRank rank, const Push& msg) {
        const auto& addr = state_machine_.getAgentAddr(rank);
        if (addr.empty()) {
            LOG(WARNING) << "[COORD] push target rank=" << rank
                         << " has no agent_addr; skipping";
            return;
        }
        rpc_client_->send<Method>(addr, msg);
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_COORDINATOR_HOST_H
