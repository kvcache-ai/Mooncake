#include "control_plane/agent_host.h"

#include <chrono>
#include <exception>
#include <stdexcept>
#include <unistd.h>

#include <glog/logging.h>

#include "mooncake_backend.h"
#include "control_plane/link_manager.h"
#include "control_plane/rpc_runtime.h"

namespace mooncake {

namespace {

// Generate a process-unique starting point for agent_session_epoch so that
// replacement processes cannot collide with the old rank's session epoch.
uint64_t generateInitialAgentSessionEpoch() {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    uint64_t pid = static_cast<uint64_t>(getpid());
    uint64_t base = (pid << 32) ^ static_cast<uint64_t>(now);
    return base == 0 ? 1 : base;
}

}  // namespace

void AgentRpcServiceImpl::onPeerJoined(PeerJoinedPush push) {
    host_.postPeerJoined(std::move(push));
}

void AgentRpcServiceImpl::onRankStateUpdate(RankStatePush push) {
    host_.postRankStateUpdate(std::move(push));
}

void AgentRpcServiceImpl::onLinkEventReportAck(LinkEventReportAck ack) {
    host_.postLinkEventReportAck(std::move(ack));
}

void AgentRpcServiceImpl::onViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                                       ViewUpdatePush push) {
    host_.postViewUpdate(std::move(ctx), std::move(push));
}

AgentHost::AgentHost(c10::intrusive_ptr<c10d::Store> store,
                     const std::string& host_ip, GlobalRank rank,
                     int max_world_size, LinkManager& link_manager)
    : agent_(rank, max_world_size),
      executor_("AgentHost"),
      link_manager_(link_manager),
      store_(std::move(store)),
      host_ip_(host_ip),
      rank_(rank),
      max_world_size_(max_world_size),
      agent_session_epoch_(generateInitialAgentSessionEpoch()),
      rpc_client_(std::make_unique<RpcClient>()) {}

AgentHost::~AgentHost() { shutdown(); }

void AgentHost::start() {
    link_manager_.setEventCallback(
        [this](TELinkUpEvent event) { postLinkUpEvent(std::move(event)); });

    rpc_server_ = std::make_unique<RpcServer>(/*port=*/0, /*thread_num=*/2);
    rpc_impl_ = std::make_unique<AgentRpcServiceImpl>(*this);
    rpc_server_->registerHandler<
        &AgentRpcService::onPeerJoined, &AgentRpcService::onRankStateUpdate,
        &AgentRpcService::onLinkEventReportAck, &AgentRpcService::onViewUpdate>(
        rpc_impl_.get());
    bool server_started = rpc_server_->start();
    if (!server_started) {
        LOG(FATAL) << "AgentHost: failed to start RPC server rank=" << rank_;
    }

    BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
        AgentHost::kCoordinatorAddrPollInterval));

    bool found = waiter.wait_for(AgentHost::kCoordinatorAddrTimeout, [this]() {
        try {
            store_->wait({"coordinator_addr"});
            coordinator_addr_ = store_->get_to_str("coordinator_addr");
            return !coordinator_addr_.empty();
        } catch (const std::exception& e) {
            LOG(WARNING) << "AgentHost: store access failed rank=" << rank_
                         << ": " << e.what();
        }
        return false;
    });

    if (!found) {
        LOG(FATAL) << "AgentHost: timed out after "
                   << std::chrono::duration_cast<std::chrono::seconds>(
                          AgentHost::kCoordinatorAddrTimeout)
                          .count()
                   << "s waiting for coordinator_addr in Store";
    }

    executor_.setTickCallback([this]() { tick(); });
    executor_.start();

    executor_.post([this]() { startAgentRegistration(); });
}

void AgentHost::shutdown() {
    if (rpc_server_) rpc_server_->shutdown();
    executor_.shutdown();
    if (rpc_client_) {
        rpc_client_->shutdown();
        rpc_client_.reset();
    }
    link_manager_.setEventCallback(nullptr);
}

bool AgentHost::waitUntilRegistered(std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    executor_.post([this, promise]() {
        if (agent_registration_done_) {
            promise->set_value();
        } else {
            agent_registration_promises_.push_back(promise);
        }
    });

    if (future.wait_for(timeout) != std::future_status::ready) {
        // Timeout: remove the dangling promise on the executor thread.
        executor_.post([this, promise]() {
            std::erase(agent_registration_promises_, promise);
        });
        return false;
    }
    return true;
}

// Block until the group reaches Ready status (all active ranks ACKed the
// bootstrap ViewUpdate).  Throws on timeout.
//
// Note: if a peer dies during the BootstrapSyncing phase, the Coordinator
// will not transition the group to Ready, and this call will hang until
// the timeout expires.  The caller should handle this as a bootstrap failure.
GroupView AgentHost::waitUntilGroupReady(GroupId group_id,
                                         std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<GroupView>>();
    auto future = promise->get_future();

    executor_.post([this, group_id, promise]() {
        auto view = agent_.getGroupView(group_id);
        if (view.status == GroupStatus::Ready) {
            promise->set_value(view);
        } else {
            group_ready_promises_[group_id].push_back(promise);
        }
    });

    if (future.wait_for(timeout) != std::future_status::ready) {
        // Clean up the dangling promise before throwing.
        executor_.post([this, group_id, promise]() {
            auto it = group_ready_promises_.find(group_id);
            if (it != group_ready_promises_.end()) {
                auto& vec = it->second;
                vec.erase(std::remove(vec.begin(), vec.end(), promise),
                          vec.end());
                if (vec.empty()) group_ready_promises_.erase(it);
            }
        });
        throw std::runtime_error("waitUntilGroupReady timed out for group " +
                                 group_id);
    }
    return future.get();
}

void AgentHost::waitUntilRankActive(GroupId group_id, GlobalRank rank,
                                    std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    executor_.post([this, group_id, rank, promise]() {
        auto view = agent_.getGroupView(group_id);
        if (view.members[rank].isActive()) {
            promise->set_value();
        } else {
            rank_active_promises_[group_id][rank].push_back(promise);
        }
    });

    if (future.wait_for(timeout) != std::future_status::ready) {
        executor_.post([this, group_id, rank, promise]() {
            auto it = rank_active_promises_.find(group_id);
            if (it != rank_active_promises_.end()) {
                auto rit = it->second.find(rank);
                if (rit != it->second.end()) {
                    auto& vec = rit->second;
                    vec.erase(std::remove(vec.begin(), vec.end(), promise),
                              vec.end());
                    if (vec.empty()) it->second.erase(rit);
                }
                if (it->second.empty()) rank_active_promises_.erase(it);
            }
        });
        throw std::runtime_error("waitUntilRankActive timed out for rank " +
                                 std::to_string(rank) + " in group " +
                                 group_id);
    }
}

void AgentHost::registerGroup(const GroupView& group, bool auto_deactivate,
                              MooncakeBackend* backend) {
    executor_.postAndWait([this, group = group, auto_deactivate,
                           backend]() mutable {
        auto group_id = group.group_id;
        backends_.insert_or_assign(group_id, backend);
        agent_.registerGroup(group, auto_deactivate);

        RegisterGroupRequest req;
        req.rank = rank_;
        req.agent_session_epoch = agent_.getAgentSessionEpoch();
        req.group = std::move(group);
        req.group.auto_deactivate = auto_deactivate;

        auto resp = rpc_client_->call<&CoordinatorRpcService::registerGroup>(
            coordinator_addr_, std::move(req));

        if (!resp.success) {
            LOG(ERROR) << "AgentHost: registerGroup failed for group "
                       << group_id << ": " << resp.reject_reason;
            auto it = group_ready_promises_.find(group_id);
            if (it != group_ready_promises_.end()) {
                for (auto& p : it->second) {
                    p->set_exception(std::make_exception_ptr(std::runtime_error(
                        "registerGroup rejected: " + resp.reject_reason)));
                }
                group_ready_promises_.erase(it);
            }
        }
    });
}

void AgentHost::unregisterGroup(GroupId group_id) {
    executor_.postAndWait([this, group_id]() {
        agent_.unregisterGroup(group_id);
        backends_.erase(group_id);

        UnregisterGroupRequest req;
        req.group_id = group_id;
        req.rank = rank_;
        req.agent_session_epoch = agent_.getAgentSessionEpoch();
        rpc_client_->send<&CoordinatorRpcService::unregisterGroup>(
            coordinator_addr_, req);
    });
}

void AgentHost::sendPublishEndpointRpc(GroupEndpointPublication endpoint) {
    PublishEndpointRequest req;
    req.rank = rank_;
    req.agent_session_epoch = agent_.getAgentSessionEpoch();
    req.endpoints.push_back(std::move(endpoint));
    rpc_client_->call<&CoordinatorRpcService::publishEndpoint>(
        coordinator_addr_, std::move(req));
}

void AgentHost::publishLocalEndpoint(GroupEndpointPublication endpoint) {
    executor_.postAndWait([this, endpoint = std::move(endpoint)]() mutable {
        sendPublishEndpointRpc(std::move(endpoint));
    });
}

ProposeViewUpdateResponse AgentHost::proposeViewUpdateInternal(
    GroupId group_id, const std::vector<GlobalRank>& ranks,
    bool is_activation) {
    ProposeViewUpdateRequest req;
    req.group_id = group_id;
    req.source_rank = rank_;
    req.agent_session_epoch = agent_.getAgentSessionEpoch();
    req.requested_ranks = ranks;
    req.is_activation = is_activation;
    return rpc_client_->call<&CoordinatorRpcService::proposeViewUpdate>(
        coordinator_addr_, req);
}

ProposeViewUpdateResponse AgentHost::proposeActivate(
    GroupId group_id, const std::vector<GlobalRank>& ranks) {
    return proposeViewUpdateInternal(group_id, ranks, /*is_activation=*/true);
}

ProposeViewUpdateResponse AgentHost::proposeDeactivate(
    GroupId group_id, const std::vector<GlobalRank>& ranks) {
    return proposeViewUpdateInternal(group_id, ranks, /*is_activation=*/false);
}

void AgentHost::pushLinkEvent(const LinkEvent& event) {
    executor_.post(
        [this, event]() { runEffects(agent_.pushLinkEvent(event)); });
}

SyncAfterFailureResponse AgentHost::syncAfterFailure(GroupId group_id) {
    SyncAfterFailureRequest req;
    req.group_id = group_id;

    executor_.postAndWait([this, &req]() {
        req.reporter_rank = rank_;
        req.agent_session_epoch = agent_.getAgentSessionEpoch();
        req.link_event_report = agent_.getLinkEventReport();
        req.current_epoch = agent_.getGroupView(req.group_id).epoch;
    });

    // Synchronous RPC should be issued outside the executor.
    // Blocking the serialized executor would stall all local state-machine
    // tasks.
    auto response = rpc_client_->call<&CoordinatorRpcService::syncAfterFailure>(
        coordinator_addr_, req);

    executor_.postAndWait([this, group_id,
                           request_session = req.agent_session_epoch,
                           &response]() {
        if (request_session != agent_.getAgentSessionEpoch()) {
            response.status = SyncAfterFailureStatus::Rejected;
            response.reject_reason = "agent session changed while syncing";
            return;
        }

        if (response.status == SyncAfterFailureStatus::Rejected) return;

        auto [effects, applied] =
            agent_.applyGroupView(group_id, response.view);
        runEffects(std::move(effects));
        if (!applied) {
            response.status = SyncAfterFailureStatus::Rejected;
            response.reject_reason = "failed to apply group view";
        }
    });
    return response;
}

void AgentHost::postPeerJoined(PeerJoinedPush push) {
    executor_.post([this, push = std::move(push)]() {
        runEffects(agent_.handlePeerJoined(push));
    });
}

void AgentHost::postRankStateUpdate(RankStatePush push) {
    executor_.post([this, push = std::move(push)]() {
        runEffects(agent_.handleRankStateUpdate(push));
    });
}

void AgentHost::postLinkEventReportAck(LinkEventReportAck ack) {
    executor_.post([this, ack = std::move(ack)]() {
        agent_.handleLinkEventReportAck(ack);
    });
}

void AgentHost::postViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                               ViewUpdatePush push) {
    auto group_id = push.group_id;
    auto epoch = push.view.epoch;

    executor_.post([this, ctx = std::move(ctx), push = std::move(push),
                    group_id, epoch]() mutable {
        auto [effects, applied] = agent_.handleViewUpdate(push);
        runEffects(std::move(effects));
        ctx.response_msg(
            ViewUpdateAck{.rank = rank_,
                          .group_id = group_id,
                          .epoch = epoch,
                          .applied = applied,
                          .error_msg = applied ? "" : "group not found"});
    });
}

void AgentHost::postLinkUpEvent(TELinkUpEvent event) {
    executor_.post([this, event = std::move(event)]() {
        runEffects(agent_.handleLinkUp(event.peer));
    });
}

void AgentHost::startAgentRegistration() {
    // Avoid duplicate registration RPCs.  This also covers the case where a
    // heartbeat response callback asks for re-registration while another
    // registration is already in flight.
    if (agent_.getCoordinatorConnection() ==
        AgentStateMachine::CoordinatorConnection::AgentRegistering) {
        return;
    }
    agent_.setCoordinatorConnection(
        AgentStateMachine::CoordinatorConnection::AgentRegistering);

    RegisterAgentRequest req;
    req.rank = rank_;
    req.agent_addr = rpc_server_->getListenAddr(host_ip_);
    req.te_server_name = link_manager_.localServerName();
    req.warmup_recv_addr = link_manager_.getWarmupRecvAddr();
    req.agent_session_epoch = ++agent_session_epoch_;
    runEffects(agent_.reset(agent_session_epoch_));

    rpc_client_->callAsync<&CoordinatorRpcService::registerAgent>(
        coordinator_addr_, std::move(req), [this](RegisterAgentResponse resp) {
            executor_.post([this, resp = std::move(resp)]() mutable {
                auto effects = agent_.applyRegisterAgentResponse(resp);
                runEffects(effects);

                if (resp.success) {
                    if (!agent_registration_done_) {
                        agent_registration_done_ = true;
                        for (auto& p : agent_registration_promises_) {
                            p->set_value();
                        }
                        agent_registration_promises_.clear();
                    }

                    // Re-publish all local backends' endpoints after
                    // (re-)reg. (Old session endpoints were cleared by
                    // Coordinator.)
                    forEachBackend([&](auto backend) {
                        sendPublishEndpointRpc(
                            backend->buildEndpointMetadata());
                    });
                } else {
                    auto now = std::chrono::steady_clock::now();
                    if (last_agent_register_error_log_time_
                                .time_since_epoch() ==
                            std::chrono::steady_clock::duration{} ||
                        now - last_agent_register_error_log_time_ >=
                            kAgentRegisterErrorLogInterval) {
                        std::string suppressed_msg;
                        if (agent_register_error_log_suppressed_ > 0) {
                            suppressed_msg =
                                " (suppressed " +
                                std::to_string(
                                    agent_register_error_log_suppressed_) +
                                " identical log" +
                                (agent_register_error_log_suppressed_ > 1
                                     ? "s"
                                     : "") +
                                " since last print)";
                        }
                        LOG(ERROR) << "AgentHost: registerAgent failed: "
                                   << resp.reject_reason
                                   << " (will retry after heartbeat "
                                      "interval; if this "
                                      "persists, the Coordinator may be "
                                      "rejecting a "
                                      "replacement rank before the old one "
                                      "times out)"
                                   << suppressed_msg;
                        last_agent_register_error_log_time_ = now;
                        agent_register_error_log_suppressed_ = 0;
                    } else {
                        ++agent_register_error_log_suppressed_;
                    }
                }
            });
        });
}

void AgentHost::tick() {
    if (!rpc_client_) return;

    if (agent_.getCoordinatorConnection() ==
        AgentStateMachine::CoordinatorConnection::Disconnected) {
        if (rpc_client_->tryReconnect(coordinator_addr_)) {
            startAgentRegistration();
        }
        return;
    }

    if (agent_.getCoordinatorConnection() ==
        AgentStateMachine::CoordinatorConnection::AgentRegistering) {
        return;
    }

    sendLinkEventReport();

    auto req = agent_.buildHeartbeat();
    req.agent_session_epoch = agent_.getAgentSessionEpoch();
    auto request_session = req.agent_session_epoch;

    rpc_client_->callAsync<&CoordinatorRpcService::heartbeat>(
        coordinator_addr_, std::move(req),
        [this, request_session](HeartbeatResponse resp) {
            executor_.post([this, request_session, resp]() {
                if (request_session != agent_.getAgentSessionEpoch()) return;
                if (resp.require_reregister) {
                    startAgentRegistration();
                }
            });
        });
}

void AgentHost::sendLinkEventReport() {
    auto report = agent_.getLinkEventReport();
    if (!report.has_value() || !rpc_client_ || coordinator_addr_.empty()) {
        return;
    }
    rpc_client_->send<&CoordinatorRpcService::reportLinkEvent>(
        coordinator_addr_, std::move(*report));
}

void AgentHost::runEffects(const AgentApplyResult& effects) {
    for (const auto& effect : effects) {
        std::visit(
            overloaded{
                [this](const EnablePeerProbe& e) {
                    link_manager_.enablePeerProbe(e.rank, e.te_server_name,
                                                  e.warmup_recv_addr);
                },
                [this](const DisconnectLink& e) {
                    link_manager_.disconnect(e.peer);
                },
                [this](const RequestLinkHealthCheck& e) {
                    link_manager_.requestHealthCheck(e.peer);
                },
                [this](const StopReconnect& e) {
                    link_manager_.stopReconnect(e.peer);
                },
                [this](const RefreshPeerLink& e) {
                    link_manager_.refreshPeerSegment(e.peer);
                },
                [this](const ResetPeerState& e) {
                    for (auto& [group_id, backend] : backends_) {
                        auto view = agent_.getGroupView(group_id);
                        for (int lr = 0;
                             lr < static_cast<int>(view.rank_order.size());
                             ++lr) {
                            if (view.rank_order[lr] == e.peer) {
                                backend->onPeerLinkReset(lr);
                                break;
                            }
                        }
                    }
                },
                [this](const NotifyLinkRefreshed& e) {
                    for (auto& [group_id, backend] : backends_) {
                        auto view = agent_.getGroupView(group_id);
                        for (int lr = 0;
                             lr < static_cast<int>(view.rank_order.size());
                             ++lr) {
                            if (view.rank_order[lr] == e.peer) {
                                backend->refreshSegmentID(lr);
                                break;
                            }
                        }
                    }
                },
                [this](const DisconnectAllLinks&) {
                    for (int i = 0; i < max_world_size_; ++i) {
                        if (i != rank_) {
                            link_manager_.disconnect(i);
                        }
                    }
                },
                [this](const ClearAllPeerMetadata&) {
                    for (int i = 0; i < max_world_size_; ++i) {
                        if (i != rank_) {
                            link_manager_.publishLinkDown(i);
                        }
                    }
                },
                [this](const ApplyViewToBackend& e) {
                    withBackend(e.group_id, [&](auto backend) {
                        backend->applyViewUpdate(e.view, e.rank_states,
                                                 e.activatable);
                    });
                },
                [this](const NotifyGroupReady& e) {
                    auto it = group_ready_promises_.find(e.group_id);
                    if (it == group_ready_promises_.end()) return;
                    auto view = agent_.getGroupView(e.group_id);
                    for (auto& p : it->second) p->set_value(view);
                    group_ready_promises_.erase(it);
                },
                [this](const NotifyRanksActivated& e) {
                    auto it = rank_active_promises_.find(e.group_id);
                    if (it == rank_active_promises_.end()) return;
                    for (GlobalRank gr : e.ranks) {
                        auto rit = it->second.find(gr);
                        if (rit != it->second.end()) {
                            for (auto& p : rit->second) p->set_value();
                            it->second.erase(rit);
                        }
                    }
                    if (it->second.empty()) rank_active_promises_.erase(it);
                },
            },
            effect);
    }
}

}  // namespace mooncake
