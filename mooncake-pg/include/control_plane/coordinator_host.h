#ifndef MOONCAKE_PG_COORDINATOR_HOST_H
#define MOONCAKE_PG_COORDINATOR_HOST_H

#include <memory>
#include <string>
#include <unordered_map>

#include <torch/csrc/distributed/c10d/Store.hpp>

#include "coordinator.h"
#include "rpc.h"
#include "rpc_runtime.h"
#include "serialized_executor.h"

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
//   | reportTransfer  |--- RPC ----->| postTransferObservation() |
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
//                              |  RankStateUpdatePush -> broadcast |
//                              |  ViewUpdatePush -> callAsync      |
//                              |  ReplyProposalEffect -> reply   |
//                              +-----------------------------------+
//

class CoordinatorHost;

// CoordinatorRpcServiceImpl  - thin RPC handler that forwards all calls
// to CoordinatorHost::post*().
class CoordinatorRpcServiceImpl : public CoordinatorRpcService {
   public:
    explicit CoordinatorRpcServiceImpl(CoordinatorHost& host) : host_(host) {}

    void registerAgent(coro_rpc::context<RegisterAgentResponse> ctx,
                       RegisterAgentRequest req) override;

    void heartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                   HeartbeatRequest req) override;

    void registerGroup(coro_rpc::context<RegisterGroupResponse> ctx,
                       RegisterGroupRequest req) override;

    void unregisterGroup(UnregisterGroupRequest req) override;

    void publishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                         PublishEndpointRequest req) override;

    void proposeViewUpdate(coro_rpc::context<ProposeViewUpdateResponse> ctx,
                           ProposeViewUpdateRequest req) override;

    void reportLinkStateChange(LinkStateChangeReport req) override;

    void reportTransferObservation(TransferObservationReport req) override;

    void syncAfterFailure(coro_rpc::context<SyncAfterFailureResponse> ctx,
                          SyncAfterFailureRequest req) override;

   private:
    CoordinatorHost& host_;
};

// CoordinatorHost - execution host for the Coordinator state machine.
class CoordinatorHost {
   public:
    CoordinatorHost(c10::intrusive_ptr<c10d::Store> store,
                    const std::string& host_ip, int max_world_size,
                    int64_t fault_reconciliation_window_us = 50000);

    ~CoordinatorHost();

    void start();
    void shutdown();

    void postRegisterAgent(coro_rpc::context<RegisterAgentResponse> ctx,
                           RegisterAgentRequest req);

    void postHeartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                       HeartbeatRequest req);

    void postRegisterGroup(coro_rpc::context<RegisterGroupResponse> ctx,
                           RegisterGroupRequest req);

    void postUnregisterGroup(UnregisterGroupRequest req);

    void postProposeViewUpdate(coro_rpc::context<ProposeViewUpdateResponse> ctx,
                               ProposeViewUpdateRequest req);

    void postPublishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                             PublishEndpointRequest req);

    void postTransferObservation(TransferObservationReport req);

    void postLinkStateChange(LinkStateChangeReport req);

    void postSyncAfterFailure(coro_rpc::context<SyncAfterFailureResponse> ctx,
                              SyncAfterFailureRequest req);

    void postViewUpdateAck(GroupId group_id, GlobalRank rank, uint64_t epoch,
                           bool applied);

   private:
    CentralizedCoordinatorStateMachine state_machine_;
    SerializedExecutor executor_;

    c10::intrusive_ptr<c10d::Store> store_;
    std::string host_ip_;
    int max_world_size_;

    // RPC infrastructure.
    std::unique_ptr<RpcServer> rpc_server_;
    std::unique_ptr<RpcClient> rpc_client_;
    std::unique_ptr<CoordinatorRpcServiceImpl> rpc_impl_;

    // Host maintains the propose_id -> RPC context mapping.
    // 2PC state is inside CentralizedCoordinatorStateMachine; Host just stores
    // the RPC context for replying when the state machine emits
    // ReplyProposalEffect.
    uint64_t next_propose_id_{1};
    std::unordered_map<uint64_t, coro_rpc::context<ProposeViewUpdateResponse>>
        pending_rpcs_;

    uint64_t next_sync_id_{1};
    std::unordered_map<uint64_t, coro_rpc::context<SyncAfterFailureResponse>>
        pending_sync_ctxs_;

    void runEffects(const std::vector<CoordinatorEffect>& effects);
    void pushViewUpdate(const ViewUpdateEffect& effect);

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
