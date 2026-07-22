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

// Generate a process-unique key for one logical registration.
uint64_t generateInitialAgentSessionId() {
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
                     int max_world_size, LinkManager& link_manager,
                     int64_t fault_reconciliation_window_us)
    : agent_(rank, max_world_size),
      executor_("AgentHost"),
      link_manager_(link_manager),
      store_(std::move(store)),
      host_ip_(host_ip),
      rank_(rank),
      max_world_size_(max_world_size),
      agent_session_id_(generateInitialAgentSessionId()),
      rpc_client_(std::make_unique<RpcClient>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::microseconds(fault_reconciliation_window_us) * 2))) {
}

AgentHost::~AgentHost() { shutdown(); }

void AgentHost::start() {
    link_manager_.setEventCallback([this](TELinkUpEvent event) {
        if (event.peer < 0 || event.peer >= max_world_size_) return;
        LinkEvent link_event;
        link_event.events.assign(max_world_size_, LinkEvent::EventType::None);
        link_event.target_rank_epochs.assign(max_world_size_, 0);
        link_event.events[event.peer] = LinkEvent::EventType::Success;
        link_event.target_rank_epochs[event.peer] = event.target_rank_epoch;
        pushLinkEvent(link_event);
    });

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
        req.agent_session_id = agent_.getAgentSessionId();
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
            return;
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
        req.agent_session_id = agent_.getAgentSessionId();
        rpc_client_->send<&CoordinatorRpcService::unregisterGroup>(
            coordinator_addr_, req);
    });
}

void AgentHost::confirmReadyForActivation(GroupId group_id) {
    ConfirmReadyForActivationRequest req;
    req.group_id = std::move(group_id);
    req.rank = rank_;
    req.agent_session_id = agent_.getAgentSessionId();
    auto resp =
        rpc_client_->call<&CoordinatorRpcService::confirmReadyForActivation>(
            coordinator_addr_, std::move(req));
    if (!resp.success) {
        throw std::runtime_error("confirmReadyForActivation rejected: " +
                                 resp.reject_reason);
    }
}

void AgentHost::sendPublishEndpointRpc(GroupEndpointPublication endpoint) {
    PublishEndpointRequest req;
    req.rank = rank_;
    req.agent_session_id = agent_.getAgentSessionId();
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
    req.agent_session_id = agent_.getAgentSessionId();
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
        req.agent_session_id = agent_.getAgentSessionId();
        req.link_event_report = agent_.getLinkEventReport();
        req.current_epoch = agent_.getGroupView(req.group_id).epoch;
    });

    // Synchronous RPC should be issued outside the executor.
    // Blocking the serialized executor would stall all local state-machine
    // tasks.
    auto response = rpc_client_->call<&CoordinatorRpcService::syncAfterFailure>(
        coordinator_addr_, req);

    executor_.postAndWait(
        [this, group_id, request_session = req.agent_session_id, &response]() {
            if (request_session != agent_.getAgentSessionId()) {
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

void AgentHost::startAgentRegistration(bool start_new_session) {
    // Avoid duplicate registration RPCs.  This also covers the case where a
    // heartbeat response callback asks for re-registration while another
    // registration is already in flight.
    if (agent_.getCoordinatorConnection() ==
        AgentStateMachine::CoordinatorConnection::AgentRegistering) {
        return;
    }
    if (start_new_session) {
        ++agent_session_id_;
        agent_session_initialized_ = false;
    }
    if (!agent_session_initialized_) {
        runEffects(agent_.reset(agent_session_id_));
        agent_session_initialized_ = true;
    }

    agent_.setCoordinatorConnection(
        AgentStateMachine::CoordinatorConnection::AgentRegistering);

    RegisterAgentRequest req;
    req.rank = rank_;
    req.agent_addr = rpc_server_->getListenAddr(host_ip_);
    req.te_server_name = link_manager_.localServerName();
    req.warmup_recv_addr = link_manager_.getWarmupRecvAddr();
    req.agent_session_id = agent_session_id_;
    const uint64_t request_session_id = req.agent_session_id;

    rpc_client_->callAsync<&CoordinatorRpcService::registerAgent>(
        coordinator_addr_, std::move(req),
        [this, request_session_id](RegisterAgentResponse resp) {
            executor_.post(
                [this, request_session_id, resp = std::move(resp)]() mutable {
                    if (request_session_id != agent_.getAgentSessionId())
                        return;

                    auto effects = agent_.applyRegisterAgentResponse(resp);
                    runEffects(effects);

                    if (agent_.getCoordinatorConnection() ==
                        AgentStateMachine::CoordinatorConnection::Connected) {
                        next_heartbeat_at_ = std::chrono::steady_clock::now();
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

                        if (resp.require_new_session) {
                            // The accepted session has gone Offline.
                            startAgentRegistration(/*start_new_session=*/true);
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

    auto now = std::chrono::steady_clock::now();
    if (now < next_heartbeat_at_) return;
    next_heartbeat_at_ = now + kHeartbeatInterval;

    // Link reports are delivery-ACKed and idempotent by report_id. Retry the
    // latest unacknowledged snapshot with the heartbeat cadence so a dropped
    // fire-and-forget report (or ACK) cannot permanently suppress the only
    // LinkUp observation for an epoch.
    if (auto report = agent_.getLinkEventReport()) {
        rpc_client_->send<&CoordinatorRpcService::reportLinkEvent>(
            coordinator_addr_, std::move(*report));
    }

    auto req = agent_.buildHeartbeat();
    req.agent_session_id = agent_.getAgentSessionId();
    auto request_session = req.agent_session_id;

    rpc_client_->callAsync<&CoordinatorRpcService::heartbeat>(
        coordinator_addr_, std::move(req),
        [this, request_session](HeartbeatResponse resp) {
            executor_.post([this, request_session, resp]() {
                if (request_session != agent_.getAgentSessionId()) return;
                if (resp.require_new_session) {
                    // The current session is no longer valid.
                    startAgentRegistration(/*start_new_session=*/true);
                }
            });
        });
}

void AgentHost::runEffects(const AgentApplyResult& effects) {
    for (const auto& effect : effects) {
        std::visit(
            overloaded{
                [this](const EnablePeerProbe& e) {
                    link_manager_.enablePeerProbe(e.rank, e.rank_epoch,
                                                  e.te_server_name,
                                                  e.warmup_recv_addr);
                },
                [this](const DisconnectLink& e) {
                    link_manager_.disconnect(e.peer);
                },
                [this](const RequestLinkHealthCheck& e) {
                    link_manager_.requestHealthCheck(e.peer);
                },
                [this](const SendLinkEventReport& e) {
                    if (!rpc_client_ || coordinator_addr_.empty()) return;
                    rpc_client_->send<&CoordinatorRpcService::reportLinkEvent>(
                        coordinator_addr_, e.report);
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
                                                 e.rank_epochs, e.activatable);
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
