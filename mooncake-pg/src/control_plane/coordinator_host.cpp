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

void CoordinatorRpcServiceImpl::unregisterAgent(
    coro_rpc::context<UnregisterAgentResponse> ctx,
    UnregisterAgentRequest req) {
    host_.postUnregisterAgent(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::registerGroup(
    coro_rpc::context<RegisterGroupResponse> ctx, RegisterGroupRequest req) {
    host_.postRegisterGroup(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::unregisterGroup(
    coro_rpc::context<UnregisterGroupResponse> ctx,
    UnregisterGroupRequest req) {
    host_.postUnregisterGroup(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::confirmReadyForActivation(
    coro_rpc::context<ConfirmReadyForActivationResponse> ctx,
    ConfirmReadyForActivationRequest req) {
    host_.postConfirmReadyForActivation(std::move(ctx), std::move(req));
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

void CoordinatorRpcServiceImpl::reportLinkEvent(
    coro_rpc::context<LinkEventReportAck> ctx, LinkEventReport req) {
    host_.postLinkEventReport(std::move(ctx), std::move(req));
}

void CoordinatorRpcServiceImpl::syncAfterFailure(
    coro_rpc::context<SyncAfterFailureResponse> ctx,
    SyncAfterFailureRequest req) {
    host_.postSyncAfterFailure(std::move(ctx), std::move(req));
}

CoordinatorHost::CoordinatorHost(const std::string& host_ip, int max_world_size,
                                 int64_t fault_reconciliation_window_us)
    : state_machine_(max_world_size,
                     std::chrono::microseconds(fault_reconciliation_window_us)),
      executor_("CoordinatorHost"),
      host_ip_(host_ip),
      max_world_size_(max_world_size),
      rpc_client_(std::make_unique<RpcClient>()) {}

CoordinatorHost::~CoordinatorHost() { shutdown(); }

PGResult<void> CoordinatorHost::setFaultReconciliationWindow(
    int64_t timeout_us) {
    return executor_.postAndWait([this, timeout_us] {
        state_machine_.setFaultReconciliationWindow(
            std::chrono::microseconds(timeout_us));
    });
}

PGResult<void> CoordinatorHost::start() {
    PG_VALIDATE_STATE(!shutdown_requested_.load(std::memory_order_acquire),
                      "CoordinatorHost cannot start after shutdown");

    rpc_server_ = std::make_unique<RpcServer>(/*port=*/0, /*thread_num=*/2);
    rpc_impl_ = std::make_unique<CoordinatorRpcServiceImpl>(*this);
    rpc_server_
        ->registerHandler<&CoordinatorRpcService::registerAgent,
                          &CoordinatorRpcService::heartbeat,
                          &CoordinatorRpcService::unregisterAgent,
                          &CoordinatorRpcService::registerGroup,
                          &CoordinatorRpcService::unregisterGroup,
                          &CoordinatorRpcService::confirmReadyForActivation,
                          &CoordinatorRpcService::proposeViewUpdate,
                          &CoordinatorRpcService::publishEndpoint,
                          &CoordinatorRpcService::reportLinkEvent,
                          &CoordinatorRpcService::syncAfterFailure>(
            rpc_impl_.get());

    bool server_started = rpc_server_->start();
    if (!server_started) {
        rpc_impl_.reset();
        rpc_server_.reset();
        return makePGError(PGErrorCode::SystemError,
                           "CoordinatorHost failed to start RPC server");
    }

    listen_addr_ = rpc_server_->getListenAddr(host_ip_);

    executor_.setTickCallback([this]() {
        auto result = state_machine_.tick();
        runEffects(result.effects);
    });

    executor_.start();
    return {};
}

void CoordinatorHost::shutdown() {
    if (shutdown_requested_.exchange(true, std::memory_order_acq_rel)) return;

    if (rpc_server_) {
        auto shutdown_confirmation = shutdown_confirmation_.get_future();
        auto post_result = executor_.postAndWait([this]() {
            auto result = state_machine_.requestShutdown();
            runEffects(result.effects);
        });

        if (!post_result.has_value()) {
            LOG(WARNING) << "[COORD] failed to request shutdown: "
                         << post_result.error().message;
        } else if (shutdown_confirmation.wait_for(kShutdownDrainTimeout) !=
                   std::future_status::ready) {
            LOG(WARNING) << "[COORD] shutdown drain timed out";
        }
        rpc_server_->shutdown();
    }

    // Keep the executor alive while outbound callbacks finish; callbacks may
    // still post their final state-machine work during client draining.
    if (rpc_client_) rpc_client_->shutdown();
    executor_.shutdown();
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
            runEffects(result.effects);
            ctx.response_msg(std::move(result.response));
        });
}

void CoordinatorHost::postUnregisterAgent(
    coro_rpc::context<UnregisterAgentResponse> ctx,
    UnregisterAgentRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handleUnregisterAgent(req);
            runEffects(result.effects);
            ctx.response_msg(std::move(result.response));
        });
}

void CoordinatorHost::postRegisterGroup(
    coro_rpc::context<RegisterGroupResponse> ctx, RegisterGroupRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handleRegisterGroup(req);
            runEffects(result.effects);
            ctx.response_msg(std::move(result.response));
        });
}

void CoordinatorHost::postUnregisterGroup(
    coro_rpc::context<UnregisterGroupResponse> ctx,
    UnregisterGroupRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handleUnregisterGroup(req);
            runEffects(result.effects);
            ctx.response_msg(std::move(result.response));
        });
}

void CoordinatorHost::postConfirmReadyForActivation(
    coro_rpc::context<ConfirmReadyForActivationResponse> ctx,
    ConfirmReadyForActivationRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handleConfirmReadyForActivation(req);
            runEffects(result.effects);
            ctx.response_msg(std::move(result.response));
        });
}

void CoordinatorHost::postProposeViewUpdate(
    coro_rpc::context<ProposeViewUpdateResponse> ctx,
    ProposeViewUpdateRequest req) {
    executor_.post([this, ctx = std::move(ctx),
                    req = std::move(req)]() mutable {
        uint64_t propose_id = next_propose_id_++;
        pending_proposal_resps_.emplace(propose_id, std::move(ctx));
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
            runEffects(result.effects);
            ctx.response_msg(std::move(result.response));
        });
}

void CoordinatorHost::postLinkEventReport(
    coro_rpc::context<LinkEventReportAck> ctx, LinkEventReport req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            auto result = state_machine_.handleLinkEventReport(req);
            runEffects(result.effects);
            ctx.response_msg(std::move(result.response));
        });
}

void CoordinatorHost::postSyncAfterFailure(
    coro_rpc::context<SyncAfterFailureResponse> ctx,
    SyncAfterFailureRequest req) {
    executor_.post(
        [this, ctx = std::move(ctx), req = std::move(req)]() mutable {
            uint64_t sync_id = next_sync_id_++;
            pending_sync_resps_.emplace(sync_id, std::move(ctx));
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
                    auto it = pending_proposal_resps_.find(e.propose_id);
                    if (it != pending_proposal_resps_.end()) {
                        it->second.response_msg(e.response);
                        pending_proposal_resps_.erase(it);
                    }
                },
                [this](const ReplySync& e) {
                    auto it = pending_sync_resps_.find(e.sync_id);
                    if (it != pending_sync_resps_.end()) {
                        it->second.response_msg(e.response);
                        pending_sync_resps_.erase(it);
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
                [this](const ShutdownCoordinatorHost&) {
                    shutdown_confirmation_.set_value();
                },
            },
            effect);
    }
}

void CoordinatorHost::pushViewUpdate(const PushViewUpdate& effect) {
    ViewUpdatePush push{effect.view};
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
            addr, push,
            [this, group_id, rank = i](PGResult<ViewUpdateAck> result) {
                if (!result.has_value()) return;
                auto ack = std::move(result).value();
                postViewUpdateAck(group_id, rank, ack.epoch, ack.applied);
            });
    }
}

}  // namespace mooncake
