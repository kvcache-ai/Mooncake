#include "control_plane/agent_host.h"

#include <algorithm>
#include <chrono>
#include <unistd.h>

#include <glog/logging.h>

#include "mooncake_communicator.h"
#include "control_plane/link_manager.h"
#include "control_plane/rpc_runtime.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

// Generate a process-unique key for one logical registration.
uint64_t generateInitialAgentSessionId() {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    uint64_t pid = static_cast<uint64_t>(getpid());
    uint64_t base = (pid << 32) ^ static_cast<uint64_t>(now);
    return base == 0 ? 1 : base;
}

template <auto Func, typename Request>
PGResult<void> callAndCheck(RpcClient& client, const std::string& addr,
                            Request request) {
    PG_TRY(auto response, client.call<Func>(addr, std::move(request)));
    if (!response.success) {
        return makePGError(PGErrorCode::InvalidState,
                           std::string(coro_rpc::get_func_name<Func>()) +
                               " rejected: " + response.reject_reason);
    }
    return {};
}

}  // namespace

void AgentRpcServiceImpl::onPeerJoined(PeerJoinedPush push) {
    host_.postPeerJoined(std::move(push));
}

void AgentRpcServiceImpl::onRankStateUpdate(RankStatePush push) {
    host_.postRankStateUpdate(std::move(push));
}

void AgentRpcServiceImpl::onViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                                       ViewUpdatePush push) {
    host_.postViewUpdate(std::move(ctx), std::move(push));
}

AgentHost::AgentHost(std::string coordinator_addr, const std::string& host_ip,
                     GlobalRank rank, int max_world_size,
                     LinkManager& link_manager,
                     int64_t fault_reconciliation_window_us)
    : agent_(rank, max_world_size),
      executor_("AgentHost"),
      link_manager_(link_manager),
      host_ip_(host_ip),
      rank_(rank),
      max_world_size_(max_world_size),
      coordinator_addr_(std::move(coordinator_addr)),
      fault_reconciliation_window_us_(fault_reconciliation_window_us),
      agent_session_id_(generateInitialAgentSessionId()),
      rpc_client_(std::make_unique<RpcClient>()) {}

AgentHost::~AgentHost() { shutdown(); }

void AgentHost::setFaultReconciliationWindow(int64_t timeout_us) {
    fault_reconciliation_window_us_.store(timeout_us,
                                          std::memory_order_relaxed);
}

PGResult<void> AgentHost::start() {
    PG_VALIDATE_ARG(!coordinator_addr_.empty(),
                    "AgentHost coordinator address must not be empty");

    PG_VALIDATE_STATE(!shutdown_requested_.load(std::memory_order_acquire),
                      "AgentHost cannot start after shutdown");

    link_manager_.setEventCallback([this](TELinkUpEvent event) {
        if (shutdown_requested_.load(std::memory_order_acquire)) return;
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
    rpc_server_->registerHandler<&AgentRpcService::onPeerJoined,
                                 &AgentRpcService::onRankStateUpdate,
                                 &AgentRpcService::onViewUpdate>(
        rpc_impl_.get());
    bool server_started = rpc_server_->start();
    if (!server_started) {
        link_manager_.setEventCallback(nullptr);
        rpc_impl_.reset();
        rpc_server_.reset();
        return makePGError(PGErrorCode::SystemError,
                           "AgentHost failed to start RPC server for rank " +
                               std::to_string(rank_));
    }

    executor_.setTickCallback([this]() { tick(); });
    executor_.start();

    return executor_.post([this]() { startAgentRegistration(); });
}

void AgentHost::shutdown() {
    if (shutdown_requested_.exchange(true, std::memory_order_acq_rel)) return;

    link_manager_.setEventCallback(nullptr);
    if (rpc_server_) rpc_server_->shutdown();
    // Finish operations that callers already submitted, including explicit
    // unregisterGroup calls, before releasing process-level rank ownership.
    executor_.shutdown();
    link_manager_.stop();
    unregisterAgent();
    if (rpc_client_) {
        rpc_client_->shutdown();
        rpc_client_.reset();
    }
}

void AgentHost::unregisterAgent() {
    if (!rpc_client_ || coordinator_addr_.empty()) return;

    const auto agent_session_id = agent_.getAgentSessionId();
    if (agent_session_id == 0) return;

    UnregisterAgentRequest req;
    req.rank = rank_;
    req.agent_session_id = agent_session_id;
    auto result = rpc_client_->call<&CoordinatorRpcService::unregisterAgent>(
        coordinator_addr_, std::move(req));
    if (!result.has_value()) {
        LOG(WARNING) << "AgentHost: unregisterAgent RPC failed, rank=" << rank_
                     << ": " << result.error().message;
        return;
    }
    const auto& response = result.value();
    if (!response.success) {
        LOG(WARNING) << "AgentHost: unregisterAgent rejected, rank=" << rank_
                     << ": " << response.reject_reason;
    }
}

PGResult<void> AgentHost::waitUntilRegistered(
    std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    PG_TRY(executor_.post([this, promise]() {
        if (agent_registration_done_) {
            promise->set_value();
        } else {
            agent_registration_promises_.push_back(promise);
        }
    }));

    if (future.wait_for(timeout) != std::future_status::ready) {
        // Timeout: remove the dangling promise on the executor thread.
        executor_.post([this, promise]() {
            std::erase(agent_registration_promises_, promise);
        });
        return makePGError(PGErrorCode::Timeout,
                           "timed out waiting for agent registration");
    }
    return {};
}

// Block until the group reaches Ready status (all active ranks ACKed the
// bootstrap ViewUpdate). Returns a timeout error if the deadline expires.
//
// Note: if a peer dies during the BootstrapSyncing phase, the Coordinator
// will not transition the group to Ready, and this call will hang until
// the timeout expires.  The caller should handle this as a bootstrap failure.
PGResult<GroupView> AgentHost::waitUntilGroupReady(
    GroupId group_id, std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<GroupView>>();
    auto future = promise->get_future();

    PG_TRY(executor_.post([this, group_id, promise]() {
        auto view = agent_.getGroupView(group_id);
        if (view.status == GroupStatus::Ready) {
            promise->set_value(view);
        } else {
            group_ready_promises_[group_id].push_back(promise);
        }
    }));

    if (future.wait_for(timeout) != std::future_status::ready) {
        // Clean up the dangling promise before returning the timeout.
        executor_.post([this, group_id, promise]() {
            auto it = group_ready_promises_.find(group_id);
            if (it != group_ready_promises_.end()) {
                auto& vec = it->second;
                vec.erase(std::remove(vec.begin(), vec.end(), promise),
                          vec.end());
                if (vec.empty()) group_ready_promises_.erase(it);
            }
        });
        return makePGError(
            PGErrorCode::Timeout,
            "waitUntilGroupReady timed out for group " + group_id);
    }
    return future.get();
}

PGResult<void> AgentHost::waitUntilRankActive(
    GroupId group_id, GlobalRank rank, std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    PG_TRY(executor_.post([this, group_id, rank, promise]() {
        auto view = agent_.getGroupView(group_id);
        if (view.members[rank].isActive()) {
            promise->set_value();
        } else {
            rank_active_promises_[group_id][rank].push_back(promise);
        }
    }));

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
        return makePGError(PGErrorCode::Timeout,
                           "waitUntilRankActive timed out for rank " +
                               std::to_string(rank) + " in group " + group_id);
    }
    return {};
}

PGResult<GroupId> AgentHost::registerGroup(
    GroupBootstrapId group_bootstrap_id, int32_t max_group_size,
    std::vector<GlobalRank> rank_order,
    GroupBootstrapIdResolvePolicy resolve_policy, bool auto_deactivate,
    MooncakeCommunicator* communicator) {
    return executor_.postAndWait(
        [this, group_bootstrap_id = std::move(group_bootstrap_id),
         max_group_size, rank_order = std::move(rank_order), resolve_policy,
         auto_deactivate, communicator]() mutable -> PGResult<GroupId> {
            RegisterGroupRequest req;
            req.rank = rank_;
            req.agent_session_id = agent_.getAgentSessionId();
            req.group_bootstrap_id = std::move(group_bootstrap_id);
            req.max_group_size = max_group_size;
            req.rank_order = std::move(rank_order);
            req.resolve_policy = resolve_policy;
            req.auto_deactivate = auto_deactivate;

            PG_TRY(auto resp,
                   rpc_client_->call<&CoordinatorRpcService::registerGroup>(
                       coordinator_addr_, std::move(req)));

            if (!resp.success) {
                // A rejected group must not affect the process-scoped Agent.
                // Return an empty id so this communicator can remain
                // group-scoped and execute local-only collectives.
                LOG(WARNING)
                    << "AgentHost: registerGroup rejected for rank=" << rank_
                    << ": " << resp.reject_reason
                    << "; leaving this group out of Agent state and falling "
                       "back to local-only execution";
                return GroupId{};
            }

            const auto& group_id = resp.view.group_id;
            communicators_.insert_or_assign(group_id, communicator);
            runEffects(agent_.registerGroup(resp.view));
            return group_id;
        });
}

void AgentHost::detachCommunicator(GroupId group_id) {
    executor_.postAndWait(
        [this, group_id]() { communicators_.erase(group_id); });
}

PGResult<void> AgentHost::unregisterGroup(GroupId group_id) {
    return executor_.postAndWait([this, group_id]() -> PGResult<void> {
        agent_.unregisterGroup(group_id);

        UnregisterGroupRequest req;
        req.group_id = group_id;
        req.rank = rank_;
        req.agent_session_id = agent_.getAgentSessionId();
        return callAndCheck<&CoordinatorRpcService::unregisterGroup>(
            *rpc_client_, coordinator_addr_, std::move(req));
    });
}

PGResult<void> AgentHost::confirmReadyForActivation(GroupId group_id) {
    ConfirmReadyForActivationRequest req;
    req.group_id = std::move(group_id);
    req.rank = rank_;
    req.agent_session_id = agent_.getAgentSessionId();
    return callAndCheck<&CoordinatorRpcService::confirmReadyForActivation>(
        *rpc_client_, coordinator_addr_, std::move(req));
}

PGResult<void> AgentHost::sendPublishEndpointRpc(
    GroupEndpointPublication endpoint) {
    PublishEndpointRequest req;
    req.rank = rank_;
    req.agent_session_id = agent_.getAgentSessionId();
    req.endpoints.push_back(std::move(endpoint));
    return callAndCheck<&CoordinatorRpcService::publishEndpoint>(
        *rpc_client_, coordinator_addr_, std::move(req));
}

PGResult<void> AgentHost::publishLocalEndpoint(
    GroupEndpointPublication endpoint) {
    return executor_.postAndWait(
        [this, endpoint = std::move(endpoint)]() mutable {
            return sendPublishEndpointRpc(std::move(endpoint));
        });
}

void AgentHost::sendLinkEventReport(LinkEventReport report) {
    if (!rpc_client_ || coordinator_addr_.empty()) return;

    const auto request_session = report.agent_session_id;
    rpc_client_->callAsync<&CoordinatorRpcService::reportLinkEvent>(
        coordinator_addr_, std::move(report),
        [this, request_session](PGResult<LinkEventReportAck> result) {
            if (!result.has_value()) return;
            auto ack = std::move(result).value();
            executor_.post([this, request_session, ack = std::move(ack)]() {
                if (shutdown_requested_.load(std::memory_order_acquire)) return;
                if (request_session != agent_.getAgentSessionId()) return;
                agent_.handleLinkEventReportAck(ack);
            });
        });
}

PGResult<ProposeViewUpdateResponse> AgentHost::proposeViewUpdateInternal(
    GroupId group_id, const std::vector<InGroupRank>& ranks,
    bool is_activation) {
    ProposeViewUpdateRequest req;
    req.group_id = group_id;
    req.source_rank = rank_;
    req.agent_session_id = agent_.getAgentSessionId();
    req.requested_ranks = ranks;
    req.is_activation = is_activation;

    const auto coordinator_timeout =
        kProposalAdmissionTimeout + kViewUpdateAckTimeout;
    const auto rpc_timeout =
        std::max(RpcClient::kDefaultRequestTimeout,
                 std::chrono::duration_cast<std::chrono::milliseconds>(
                     2 * coordinator_timeout));
    return rpc_client_->call<&CoordinatorRpcService::proposeViewUpdate>(
        coordinator_addr_, std::move(req), rpc_timeout);
}

PGResult<ProposeViewUpdateResponse> AgentHost::proposeActivate(
    GroupId group_id, const std::vector<InGroupRank>& ranks) {
    return proposeViewUpdateInternal(group_id, ranks, /*is_activation=*/true);
}

PGResult<ProposeViewUpdateResponse> AgentHost::proposeDeactivate(
    GroupId group_id, const std::vector<InGroupRank>& ranks) {
    return proposeViewUpdateInternal(group_id, ranks, /*is_activation=*/false);
}

void AgentHost::pushLinkEvent(const LinkEvent& event) {
    executor_.post(
        [this, event]() { runEffects(agent_.pushLinkEvent(event)); });
}

PGResult<SyncAfterFailureResponse> AgentHost::syncAfterFailure(
    GroupId group_id) {
    SyncAfterFailureRequest req;
    req.group_id = group_id;

    PG_TRY(executor_.postAndWait([this, &req]() {
        req.reporter_rank = rank_;
        req.agent_session_id = agent_.getAgentSessionId();
        req.link_event_report = agent_.getLinkEventReport();
        req.current_epoch = agent_.getGroupView(req.group_id).epoch;
    }));

    // Synchronous RPC should be issued outside the executor.
    // Blocking the serialized executor would stall all local state-machine
    // tasks.
    const auto reconciliation_window = std::chrono::microseconds(
        fault_reconciliation_window_us_.load(std::memory_order_relaxed));
    const auto reconciliation_timeout =
        std::chrono::ceil<std::chrono::milliseconds>(reconciliation_window);
    const auto rpc_timeout =
        std::max(RpcClient::kDefaultRequestTimeout, 2 * reconciliation_timeout);
    PG_TRY(auto response,
           rpc_client_->call<&CoordinatorRpcService::syncAfterFailure>(
               coordinator_addr_, req, rpc_timeout));

    PG_TRY(executor_.postAndWait([this, request_session = req.agent_session_id,
                                  &response]() -> PGResult<void> {
        PG_VALIDATE_STATE(request_session == agent_.getAgentSessionId(),
                          "agent session changed while syncing");

        if (response.link_event_report_ack.has_value()) {
            agent_.handleLinkEventReportAck(*response.link_event_report_ack);
        }

        if (response.status != SyncAfterFailureStatus::Rejected) {
            PG_TRY(auto effects, agent_.applyGroupView(response.view));
            runEffects(effects);
        }
        return {};
    }));
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

void AgentHost::postViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                               ViewUpdatePush push) {
    auto group_id = push.view.group_id;
    auto epoch = push.view.epoch;

    executor_.post([this, ctx = std::move(ctx), push = std::move(push),
                    group_id, epoch]() mutable {
        auto apply_result = agent_.handleViewUpdate(push);
        ViewUpdateAck ack{.rank = rank_,
                          .group_id = group_id,
                          .epoch = epoch,
                          .applied = false,
                          .error_msg = ""};
        if (apply_result.has_value()) {
            runEffects(std::move(apply_result).value());
            ack.applied = true;
        } else {
            ack.error_msg = std::move(apply_result).error().message;
        }
        ctx.response_msg(std::move(ack));
    });
}

void AgentHost::startAgentRegistration(bool start_new_session) {
    if (shutdown_requested_.load(std::memory_order_acquire)) return;

    // Avoid duplicate registration RPCs.  This also covers the case where a
    // heartbeat response callback asks for re-registration while another
    // registration is already in flight.
    if (agent_.getCoordinatorConnection() ==
        AgentStateMachine::CoordinatorConnection::AgentRegistering) {
        return;
    }
    if (start_new_session) {
        link_manager_.stop();
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
        [this, request_session_id](PGResult<RegisterAgentResponse> result) {
            executor_.post([this, request_session_id,
                            result = std::move(result)]() mutable {
                if (shutdown_requested_.load(std::memory_order_acquire)) return;
                if (request_session_id != agent_.getAgentSessionId()) return;

                if (!result.has_value()) {
                    agent_.setCoordinatorConnection(
                        AgentStateMachine::CoordinatorConnection::Disconnected);
                    if (shouldLogAgentRegistrationError()) {
                        LOG(ERROR) << "AgentHost: registerAgent RPC failed: "
                                   << result.error().message << "; will retry";
                    }
                    return;
                }

                auto resp = std::move(result).value();
                if (!resp.success) {
                    agent_.setCoordinatorConnection(
                        AgentStateMachine::CoordinatorConnection::Disconnected);
                    if (shouldLogAgentRegistrationError()) {
                        LOG(ERROR) << "AgentHost: registerAgent rejected: "
                                   << resp.reject_reason << "; will retry";
                    }
                    if (resp.require_new_session) {
                        startAgentRegistration(/*start_new_session=*/true);
                    }
                    return;
                }

                auto effects = agent_.applyRegisterAgentResponse(resp);
                runEffects(effects);
                if (agent_.getCoordinatorConnection() !=
                    AgentStateMachine::CoordinatorConnection::Connected)
                    return;

                link_manager_.start(agent_.getRankEpoch());

                if (!agent_registration_done_) {
                    agent_registration_done_ = true;
                    for (auto& p : agent_registration_promises_) {
                        p->set_value();
                    }
                    agent_registration_promises_.clear();
                }

                // Re-publish all local communicators' endpoints after (re-)reg.
                // Old session endpoints were cleared by Coordinator.
                forEachCommunicator([&](auto communicator) {
                    auto result = sendPublishEndpointRpc(
                        communicator->buildEndpointMetadata());
                    if (!result.has_value()) {
                        LOG(ERROR) << "AgentHost: failed to re-publish "
                                      "communicator endpoint: "
                                   << result.error().message;
                    }
                });
            });
        });
}

bool AgentHost::shouldLogAgentRegistrationError() {
    const auto now = std::chrono::steady_clock::now();
    if (last_agent_register_error_log_time_.time_since_epoch() !=
            std::chrono::steady_clock::duration{} &&
        now - last_agent_register_error_log_time_ <
            kAgentRegisterErrorLogInterval) {
        return false;
    }
    last_agent_register_error_log_time_ = now;
    return true;
}

void AgentHost::tick() {
    if (shutdown_requested_.load(std::memory_order_acquire)) return;
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

    // Link reports are idempotent by report_id. Retry the latest unacknowledged
    // snapshot with the heartbeat cadence when the request or its response is
    // lost.
    if (auto report = agent_.getLinkEventReport()) {
        sendLinkEventReport(std::move(*report));
    }

    auto req = agent_.buildHeartbeat();
    req.agent_session_id = agent_.getAgentSessionId();
    auto request_session = req.agent_session_id;

    rpc_client_->callAsync<&CoordinatorRpcService::heartbeat>(
        coordinator_addr_, std::move(req),
        [this, request_session](PGResult<HeartbeatResponse> result) {
            if (!result.has_value()) return;
            auto resp = std::move(result).value();
            executor_.post([this, request_session, resp]() {
                if (shutdown_requested_.load(std::memory_order_acquire)) return;
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
                    sendLinkEventReport(e.report);
                },
                [this](const StopReconnect& e) {
                    link_manager_.stopReconnect(e.peer);
                },
                [this](const RefreshPeerLink& e) {
                    link_manager_.refreshPeerSegment(e.peer);
                },
                [this](const ResetPeerState& e) {
                    for (auto& [group_id, communicator] : communicators_) {
                        auto view = agent_.getGroupView(group_id);
                        for (int lr = 0;
                             lr < static_cast<int>(view.rank_order.size());
                             ++lr) {
                            if (view.rank_order[lr] == e.peer) {
                                communicator->onPeerLinkReset(lr);
                                break;
                            }
                        }
                    }
                },
                [this](const NotifyLinkRefreshed& e) {
                    for (auto& [group_id, communicator] : communicators_) {
                        auto view = agent_.getGroupView(group_id);
                        for (int lr = 0;
                             lr < static_cast<int>(view.rank_order.size());
                             ++lr) {
                            if (view.rank_order[lr] == e.peer) {
                                communicator->refreshSegmentID(lr);
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
                [this](const ApplyViewToCommunicator& e) {
                    withCommunicator(e.view.group_id, [&](auto communicator) {
                        communicator->applyViewUpdate(e.view, e.rank_states,
                                                      e.rank_epochs,
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
