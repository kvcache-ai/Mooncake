#include "control_plane/coordinator_host.h"

#include <chrono>
#include <glog/logging.h>

#include "control_plane/rpc.h"
#include "control_plane/rpc_runtime.h"
#include "pg_utils.h"

namespace mooncake {

void CoordinatorRpcServiceImpl::registerAgent(
    coro_rpc::context<RegisterAgentResponse> ctx, RegisterAgentRequest req) {
    host_.postRegisterAgent(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::heartbeat(
    coro_rpc::context<HeartbeatResponse> ctx, HeartbeatRequest req) {
    host_.postHeartbeat(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::registerGroup(
    coro_rpc::context<RegisterGroupResponse> ctx, RegisterGroupRequest req) {
    host_.postRegisterGroup(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::unregisterGroup(UnregisterGroupRequest req) {
    host_.postUnregisterGroup(std::move(req));
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

void CoordinatorRpcServiceImpl::reportLinkEvent(LinkEventReport req) {
    host_.postLinkEventReport(std::move(req));
}

void CoordinatorRpcServiceImpl::syncAfterFailure(
    coro_rpc::context<SyncAfterFailureResponse> ctx,
    SyncAfterFailureRequest req) {
    host_.postSyncAfterFailure(std::move(ctx), std::move(req));
}

CoordinatorHost::CoordinatorHost(c10::intrusive_ptr<c10d::Store> store,
                                 const std::string& host_ip, int max_world_size,
                                 int64_t fault_reconciliation_window_us)
    : state_machine_(max_world_size,
                     std::chrono::microseconds(fault_reconciliation_window_us)),
      executor_("CoordinatorHost"),
      store_(std::move(store)),
      host_ip_(host_ip),
      max_world_size_(max_world_size),
      rpc_client_(std::make_unique<RpcClient>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::microseconds(fault_reconciliation_window_us) * 2))) {
}

CoordinatorHost::~CoordinatorHost() { shutdown(); }

void CoordinatorHost::start() {
    rpc_server_ = std::make_unique<RpcServer>(/*port=*/0, /*thread_num=*/2);
    rpc_impl_ = std::make_unique<CoordinatorRpcServiceImpl>(*this);
    rpc_server_->registerHandler<&CoordinatorRpcService::registerAgent,
                                 &CoordinatorRpcService::heartbeat,
                                 &CoordinatorRpcService::registerGroup,
                                 &CoordinatorRpcService::unregisterGroup,
                                 &CoordinatorRpcService::proposeViewUpdate,
                                 &CoordinatorRpcService::publishEndpoint,
                                 &CoordinatorRpcService::reportLinkEvent,
                                 &CoordinatorRpcService::syncAfterFailure>(
        rpc_impl_.get());

    bool server_started = rpc_server_->start();
    if (!server_started) {
        LOG(FATAL) << "CoordinatorHost: failed to start RPC server";
    }

    std::string addr = rpc_server_->getListenAddr(host_ip_);
    store_->set("coordinator_addr", addr);

    executor_.setTickCallback([this]() {
        auto result = state_machine_.tick();
        runEffects(result.effects);
    });

    executor_.start();
}

void CoordinatorHost::shutdown() {
    if (rpc_server_) rpc_server_->shutdown();
    executor_.shutdown();

    for (auto& [propose_id, ctx] : pending_rpcs_) {
        ctx.response_msg(ProposeViewUpdateResponse{
            ViewUpdateStatus::Rejected, 0, {}, "coordinator shutting down"});
    }
    pending_rpcs_.clear();

    for (auto& [sync_id, ctx] : pending_sync_ctxs_) {
        SyncAfterFailureResponse response;
        response.status = SyncAfterFailureStatus::Rejected;
        response.reject_reason = "coordinator shutting down";
        ctx.response_msg(std::move(response));
    }
    pending_sync_ctxs_.clear();

    if (rpc_client_) rpc_client_->shutdown();
}

void CoordinatorHost::postRegisterAgent(
    coro_rpc::context<RegisterAgentResponse> ctx, RegisterAgentRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto r = state_machine_.handleRegisterAgent(req);
            runEffects(r.effects);
            ctx.response_msg(std::move(r.response));
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

void CoordinatorHost::postRegisterGroup(
    coro_rpc::context<RegisterGroupResponse> ctx, RegisterGroupRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handleRegisterGroup(req);
            ctx.response_msg(std::move(result.response));
            runEffects(result.effects);
        });
}

void CoordinatorHost::postUnregisterGroup(UnregisterGroupRequest req) {
    executor_.post([this, req = std::move(req)]() {
        auto result = state_machine_.handleUnregisterGroup(req);
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

void CoordinatorHost::postPublishEndpoint(
    coro_rpc::context<PublishEndpointResponse> ctx,
    PublishEndpointRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handlePublishEndpoint(req);
            ctx.response_msg(std::move(result.response));
            runEffects(result.effects);
        });
}

void CoordinatorHost::postLinkEventReport(LinkEventReport req) {
    executor_.post([this, req = std::move(req)]() {
        auto result = state_machine_.handleLinkEventReport(req);
        runEffects(result.effects);
    });
}

void CoordinatorHost::postSyncAfterFailure(
    coro_rpc::context<SyncAfterFailureResponse> ctx,
    SyncAfterFailureRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            uint64_t sync_id = next_sync_id_++;
            pending_sync_ctxs_.emplace(sync_id, std::move(ctx));
            auto result = state_machine_.handleSyncAfterFailure(sync_id, req);
            runEffects(result.effects);
        });
}

void CoordinatorHost::postViewUpdateAck(GroupId group_id, GlobalRank rank,
                                        uint64_t epoch, bool applied) {
    executor_.post([this, group_id, rank, epoch, applied]() {
        auto result =
            state_machine_.handleViewUpdateAck(group_id, rank, epoch, applied);
        runEffects(result.effects);
    });
}

void CoordinatorHost::runEffects(
    const std::vector<CoordinatorEffect>& effects) {
    for (const auto& effect : effects) {
        std::visit(
            overloaded{
                [this](const BroadcastRankState& e) {
                    for (int i = 0; i < max_world_size_; ++i) {
                        if (state_machine_.getRankState(i) !=
                            RankState::Offline) {
                            pushToAgent<&AgentRpcService::onRankStateUpdate>(
                                i, e.push);
                        }
                    }
                },
                [this](const PushViewUpdate& e) { pushViewUpdate(e); },
                [this](const ReplyProposal& e) {
                    auto it = pending_rpcs_.find(e.propose_id);
                    if (it != pending_rpcs_.end()) {
                        it->second.response_msg(e.response);
                        pending_rpcs_.erase(it);
                    }
                },
                [this](const ReplySync& e) {
                    auto it = pending_sync_ctxs_.find(e.sync_id);
                    if (it != pending_sync_ctxs_.end()) {
                        it->second.response_msg(e.response);
                        pending_sync_ctxs_.erase(it);
                    }
                },
                [this](const BroadcastPeerJoined& e) {
                    for (int i = 0; i < max_world_size_; ++i) {
                        if (i != e.push.rank && state_machine_.getRankState(
                                                    i) != RankState::Offline) {
                            pushToAgent<&AgentRpcService::onPeerJoined>(i,
                                                                        e.push);
                        }
                    }
                },
                [this](const AckLinkEventReport& e) {
                    if (state_machine_.getRankState(e.ack.rank) !=
                        RankState::Offline) {
                        pushToAgent<&AgentRpcService::onLinkEventReportAck>(
                            e.ack.rank, e.ack);
                    }
                },
            },
            effect);
    }
}

void CoordinatorHost::pushViewUpdate(const PushViewUpdate& effect) {
    ViewUpdatePush push{effect.view.group_id, effect.view};
    auto group_id = effect.view.group_id;

    for (int32_t i = 0; i < max_world_size_; ++i) {
        const auto& member = effect.view.members[i];
        if (member.status == GroupMemberState::None ||
            member.status == GroupMemberState::Left) {
            continue;
        }

        const auto& addr = state_machine_.getAgentAddr(i);
        if (state_machine_.getRankState(i) == RankState::Offline ||
            addr.empty())
            continue;

        rpc_client_->callAsync<&AgentRpcService::onViewUpdate>(
            addr, push, [this, group_id, rank = i](ViewUpdateAck ack) {
                postViewUpdateAck(group_id, rank, ack.epoch, ack.applied);
            });
    }
}

}  // namespace mooncake
