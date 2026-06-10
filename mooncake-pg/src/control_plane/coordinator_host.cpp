#include "control_plane/coordinator_host.h"

#include <glog/logging.h>

#include "control_plane/rpc.h"
#include "control_plane/rpc_runtime.h"
#include "pg_utils.h"

namespace mooncake {

// CoordinatorRpcServiceImpl

void CoordinatorRpcServiceImpl::registerAgent(
    coro_rpc::context<RegisterResponse> ctx, RegisterRequest req) {
    host_.postRegister(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::heartbeat(
    coro_rpc::context<HeartbeatResponse> ctx, HeartbeatRequest req) {
    host_.postHeartbeat(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::declareGroup(
    coro_rpc::context<DeclareGroupResponse> ctx, DeclareGroupRequest req) {
    host_.postDeclareGroup(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::proposeViewUpdate(
    coro_rpc::context<ProposeViewUpdateResponse> ctx,
    ProposeViewUpdateRequest req) {
    host_.postProposeViewUpdate(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::publishEndpoint(
    coro_rpc::context<PublishEndpointResponse> ctx,
    PublishEndpointRequest req) {
    host_.postPublishEndpoint(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::reportTransferObservation(
    TransferObservationReport req) {
    host_.postTransferObservation(std::move(req));
}

// CoordinatorHost

CoordinatorHost::CoordinatorHost(c10::intrusive_ptr<c10d::Store> store,
                                 const std::string& host_ip, int max_world_size)
    : state_machine_(max_world_size),
      executor_("CoordinatorHost"),
      store_(std::move(store)),
      host_ip_(host_ip),
      max_world_size_(max_world_size),
      rpc_client_(std::make_unique<RpcClient>()) {}

CoordinatorHost::~CoordinatorHost() { shutdown(); }

void CoordinatorHost::start() {
    // Start RPC server.
    rpc_server_ = std::make_unique<RpcServer>(/*port=*/0, /*thread_num=*/2);
    rpc_impl_ = std::make_unique<CoordinatorRpcServiceImpl>(*this);
    rpc_server_->registerHandler<
        &CoordinatorRpcService::registerAgent,
        &CoordinatorRpcService::heartbeat, &CoordinatorRpcService::declareGroup,
        &CoordinatorRpcService::proposeViewUpdate,
        &CoordinatorRpcService::publishEndpoint,
        &CoordinatorRpcService::reportTransferObservation>(rpc_impl_.get());

    rpc_server_->start();

    // Write Coordinator address to Store for Agent discovery.
    std::string addr = rpc_server_->getListenAddr(host_ip_);
    store_->set("coordinator_addr", addr);
    LOG(INFO) << "CoordinatorHost: published address " << addr;

    // Set up periodic tick.
    executor_.setTickCallback([this]() {
        auto result = state_machine_.checkTimeouts();
        runEffects(result.effects);
    });

    executor_.start();
}

void CoordinatorHost::shutdown() {
    // 1. Stop RPC server  - no new requests accepted.
    if (rpc_server_) rpc_server_->shutdown();
    // 2. Drain the executor  - all posted tasks complete.
    executor_.shutdown();
    // 3. Fail any pending 2PC proposals so clients don't hang.
    for (auto& [propose_id, ctx] : pending_rpcs_) {
        ctx.response_msg(ProposeViewUpdateResponse{
            ViewUpdateStatus::Rejected, 0, {}, "coordinator shutting down"});
    }
    pending_rpcs_.clear();
}

void CoordinatorHost::postRegister(coro_rpc::context<RegisterResponse> ctx,
                                   RegisterRequest req) {
    LOG(INFO) << "CoordinatorHost: postRegister rank=" << req.rank
              << " addr=" << req.agent_addr;
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            LOG(INFO) << "CoordinatorHost: handling register rank=" << req.rank;
            auto result = state_machine_.handleRegister(req);
            LOG(INFO) << "CoordinatorHost: register rank=" << req.rank
                      << " success=" << result.response.success;
            ctx.response_msg(std::move(result.response));
            runEffects(result.effects);
        });
}

void CoordinatorHost::postHeartbeat(coro_rpc::context<HeartbeatResponse> ctx,
                                    HeartbeatRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handleHeartbeat(req);
            ctx.response_msg(std::move(result.response));
            runEffects(result.effects);
        });
}

void CoordinatorHost::postDeclareGroup(
    coro_rpc::context<DeclareGroupResponse> ctx, DeclareGroupRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handleDeclareGroup(req);
            ctx.response_msg(std::move(result.response));
            runEffects(result.effects);
        });
}

void CoordinatorHost::postProposeViewUpdate(
    coro_rpc::context<ProposeViewUpdateResponse> ctx,
    ProposeViewUpdateRequest req) {
    executor_.post([this, ctx = std::move(ctx),
                    req = std::move(req)]() mutable {
        uint64_t propose_id = next_propose_id_++;
        pending_rpcs_.emplace(propose_id, std::move(ctx));
        auto result = state_machine_.handleProposeViewUpdate(propose_id, req);
        runEffects(result.effects);
    });
}

void CoordinatorHost::postProposalAck(uint64_t propose_id, GlobalRank rank,
                                      uint64_t epoch, bool applied) {
    executor_.post([this, propose_id, rank, epoch, applied]() {
        auto result =
            state_machine_.handleProposalAck(propose_id, rank, epoch, applied);
        runEffects(result.effects);
    });
}

void CoordinatorHost::postBootstrapAck(GroupId group_id, GlobalRank rank,
                                       uint64_t epoch) {
    executor_.post([this, group_id, rank, epoch]() {
        auto result = state_machine_.handleBootstrapAck(group_id, rank, epoch);
        runEffects(result.effects);
    });
}

void CoordinatorHost::postPublishEndpoint(
    coro_rpc::context<PublishEndpointResponse> ctx,
    PublishEndpointRequest req) {
    LOG(INFO) << "CoordinatorHost: postPublishEndpoint rank=" << req.rank
              << " epoch=" << req.agent_session_epoch
              << " num_endpoints=" << req.endpoints.size();
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handlePublishEndpoint(req);
            LOG(INFO) << "CoordinatorHost: publishEndpoint rank=" << req.rank
                      << " success=" << result.response.success;
            ctx.response_msg(std::move(result.response));
            runEffects(result.effects);
        });
}

void CoordinatorHost::postTransferObservation(TransferObservationReport req) {
    executor_.post([this, req = std::move(req)]() {
        auto result = state_machine_.handleTransferObservation(req);
        runEffects(result.effects);
    });
}

void CoordinatorHost::runEffects(
    const std::vector<CoordinatorEffect>& effects) {
    for (const auto& effect : effects) {
        std::visit(
            overloaded{
                [this](const PushEffect<RankStateUpdatePush>& e) {
                    if (e.target.kind == EffectTarget::Kind::BroadcastOnline) {
                        for (int i = 0; i < max_world_size_; ++i) {
                            if (state_machine_.getRankState(i) !=
                                RankState::OFFLINE) {
                                pushToAgent(i, e.push);
                            }
                        }
                    } else {
                        pushToAgent(e.target.rank, e.push);
                    }
                },
                [this](const ViewUpdateEffect& e) { pushViewUpdate(e); },
                [this](const ReplyViewUpdateEffect& e) {
                    auto it = pending_rpcs_.find(e.propose_id);
                    if (it != pending_rpcs_.end()) {
                        it->second.response_msg(e.response);
                        pending_rpcs_.erase(it);
                    }
                },
                [this](const PushEffect<PeerJoinedPush>& e) {
                    for (int i = 0; i < max_world_size_; ++i) {
                        if (i != e.push.rank && state_machine_.getRankState(
                                                    i) != RankState::OFFLINE) {
                            pushToAgent(i, e.push);
                        }
                    }
                },
            },
            effect);
    }
}

void CoordinatorHost::pushToAgent(GlobalRank rank,
                                  const RankStateUpdatePush& msg) {
    const auto& addr = state_machine_.getAgentAddr(rank);
    if (addr.empty()) return;
    rpc_client_->send<&AgentRpcService::onRankStateUpdate>(addr, msg);
}

void CoordinatorHost::pushToAgent(GlobalRank rank, const PeerJoinedPush& msg) {
    const auto& addr = state_machine_.getAgentAddr(rank);
    if (addr.empty()) return;
    rpc_client_->send<&AgentRpcService::onPeerJoined>(addr, msg);
}

void CoordinatorHost::pushViewUpdate(const ViewUpdateEffect& effect) {
    ViewUpdatePush push{effect.view.group_id, effect.descriptor, effect.view};
    auto group_id = effect.view.group_id;
    auto ack_route = effect.ack_route;  // capture for ACK routing

    for (int i = 0; i < max_world_size_; ++i) {
        const auto& addr = state_machine_.getAgentAddr(i);
        if (state_machine_.getRankState(i) == RankState::OFFLINE ||
            addr.empty())
            continue;

        // If required_acks is non-empty, only send to required ranks.
        if (!effect.required_acks.empty()) {
            bool required = std::find(effect.required_acks.begin(),
                                      effect.required_acks.end(),
                                      i) != effect.required_acks.end();
            if (!required) continue;
        }

        // Use callAsync (not fire-and-forget send) to capture ACKs from
        // agents.  Bootstrap ACKs always drive the bootstrap 2PC handler;
        // proposal ACKs additionally drive the proposal 2PC handler.
        rpc_client_->callAsync<&AgentRpcService::onViewUpdate>(
            addr, push,
            [this, group_id, ack_route, rank = i](ViewUpdateAck ack) {
                if (!ack.applied) return;
                // 1. Drive Bootstrap 2PC (always safe; no-op if not in
                // BootstrapSyncing).
                postBootstrapAck(group_id, rank, ack.epoch);
                // 2. Drive Proposal 2PC (only for proposal-originated pushes).
                std::visit(overloaded{[](const BootstrapAckRoute&) {},
                                      [&](const ProposalAckRoute& r) {
                                          postProposalAck(r.propose_id, rank,
                                                          ack.epoch,
                                                          ack.applied);
                                      }},
                           ack_route);
            });
    }
}

}  // namespace mooncake
