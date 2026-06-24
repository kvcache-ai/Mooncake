#ifndef MOONCAKE_PG_COORDINATOR_HOST_H
#define MOONCAKE_PG_COORDINATOR_HOST_H

#include <memory>
#include <string>
#include <unordered_map>

#include <torch/csrc/distributed/c10d/Store.hpp>

#include "coordinator.h"
#include "rpc.h"
#include "serialized_executor.h"

namespace mooncake {

// Forward declarations.
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
//   | registerAgent   |--- RPC ----->| postRegister()            |
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
//                              |  ReplyViewUpdateEffect -> reply   |
//                              +-----------------------------------+
//

class CoordinatorHost;

// CoordinatorRpcServiceImpl  - thin RPC handler that forwards all calls
// to CoordinatorHost::post*().
class CoordinatorRpcServiceImpl : public CoordinatorRpcService {
   public:
    explicit CoordinatorRpcServiceImpl(CoordinatorHost& host) : host_(host) {}

    void registerAgent(coro_rpc::context<RegisterResponse> ctx,
                       RegisterRequest req) override;

    void heartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                   HeartbeatRequest req) override;

    void declareGroup(coro_rpc::context<DeclareGroupResponse> ctx,
                      DeclareGroupRequest req) override;

    void proposeViewUpdate(coro_rpc::context<ProposeViewUpdateResponse> ctx,
                           ProposeViewUpdateRequest req) override;

    void publishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                         PublishEndpointRequest req) override;

    void reportTransferObservation(TransferObservationReport req) override;

   private:
    CoordinatorHost& host_;
};

// CoordinatorHost - execution host for the Coordinator state machine.
class CoordinatorHost {
   public:
    CoordinatorHost(c10::intrusive_ptr<c10d::Store> store,
                    const std::string& host_ip, int max_world_size);

    ~CoordinatorHost();

    void start();
    void shutdown();

    void postRegister(coro_rpc::context<RegisterResponse> ctx,
                      RegisterRequest req);

    void postHeartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                       HeartbeatRequest req);

    void postDeclareGroup(coro_rpc::context<DeclareGroupResponse> ctx,
                          DeclareGroupRequest req);

    void postProposeViewUpdate(coro_rpc::context<ProposeViewUpdateResponse> ctx,
                               ProposeViewUpdateRequest req);

    void postProposalAck(uint64_t propose_id, GlobalRank rank, uint64_t epoch,
                         bool applied);

    void postBootstrapAck(GroupId group_id, GlobalRank rank, uint64_t epoch);

    void postPublishEndpoint(coro_rpc::context<PublishEndpointResponse> ctx,
                             PublishEndpointRequest req);

    void postTransferObservation(TransferObservationReport req);

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
    // ReplyViewUpdateEffect.
    uint64_t next_propose_id_{1};
    std::unordered_map<uint64_t, coro_rpc::context<ProposeViewUpdateResponse>>
        pending_rpcs_;

    void runEffects(const std::vector<CoordinatorEffect>& effects);
    void pushToAgent(GlobalRank rank, const RankStateUpdatePush& msg);
    void pushToAgent(GlobalRank rank, const PeerJoinedPush& msg);
    void pushViewUpdate(const ViewUpdateEffect& effect);
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_COORDINATOR_HOST_H
