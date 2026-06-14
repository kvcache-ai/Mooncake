#include "control_plane/agent_host.h"

#include <exception>
#include <stdexcept>

#include <glog/logging.h>

#include "mooncake_backend.h"
#include "control_plane/link_manager.h"
#include "control_plane/rpc_runtime.h"

namespace mooncake {

// AgentRpcServiceImpl

void AgentRpcServiceImpl::onPeerJoined(PeerJoinedPush push) {
    host_.postPeerJoined(std::move(push));
}

void AgentRpcServiceImpl::onRankStateUpdate(RankStateUpdatePush push) {
    host_.postRankStateUpdate(std::move(push));
}

void AgentRpcServiceImpl::onViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                                       ViewUpdatePush push) {
    host_.postViewUpdate(std::move(ctx), std::move(push));
}

// AgentHost

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
      rpc_client_(std::make_unique<RpcClient>()) {}

AgentHost::~AgentHost() { shutdown(); }

void AgentHost::start() {
    // Register LinkManager event callback.
    link_manager_.setEventCallback(
        [this](TELinkEvent event) { postTELinkEvent(std::move(event)); });

    // Start Agent RPC server.
    rpc_server_ = std::make_unique<RpcServer>(/*port=*/0, /*thread_num=*/2);
    rpc_impl_ = std::make_unique<AgentRpcServiceImpl>(*this);
    rpc_server_->registerHandler<&AgentRpcService::onPeerJoined,
                                 &AgentRpcService::onRankStateUpdate,
                                 &AgentRpcService::onViewUpdate>(
        rpc_impl_.get());
    if (!rpc_server_->start()) {
        LOG(ERROR) << "AgentHost: failed to start RPC server";
    }

    // Read Coordinator address from Store with backoff poll.
    // We cannot guarantee the Coordinator is initialised before this
    // Agent  - non-rank-0 processes may reach this point before rank 0
    // has written the key.  check() is non-blocking; the predicate
    // catches exceptions so a transient Store failure retries instead
    // of crashing the process.
    BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
        AgentHost::kCoordinatorAddrPollInterval));

    bool found = waiter.wait_for(AgentHost::kCoordinatorAddrTimeout, [this]() {
        try {
            if (store_->check({"coordinator_addr"})) {
                coordinator_addr_ = store_->get_to_str("coordinator_addr");
                return !coordinator_addr_.empty();
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "AgentHost: store access failed: " << e.what();
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

    // Set up periodic tick.
    executor_.setTickCallback([this]() { tick(); });

    executor_.start();
    LOG(INFO) << "AgentHost::start() executor started, rank=" << rank_;

    // Initial registration.
    executor_.post([this]() {
        LOG(INFO) << "AgentHost: executor running startRegisterRpc, rank="
                  << rank_;
        startRegisterRpc();
    });
}

void AgentHost::shutdown() {
    // Stop RPC server first  - no new pushes accepted, in-flight handlers
    // are guaranteed to have finished posting to the executor.
    if (rpc_server_) rpc_server_->shutdown();
    // Drain the executor next so that pending tasks / tick callbacks finish
    // before we tear down the RpcClient they may be using.
    executor_.shutdown();
    // Mark RpcClient as shutting down so any in-flight async coroutines on the
    // global I/O executor drop silently instead of logging errors or invoking
    // callbacks that may touch this dying AgentHost.
    if (rpc_client_) rpc_client_->shutdown();
    // Drop the shared-state reference.  At this point the executor is stopped
    // and in-flight coroutines have been told to drop, so no code path will
    // dereference rpc_client_.
    if (rpc_client_) rpc_client_.reset();
    link_manager_.setEventCallback(nullptr);
}

// Agent interface: Bootstrap

bool AgentHost::waitUntilRegistered(std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    executor_.post([this, promise]() {
        if (registration_done_) {
            promise->set_value();
        } else {
            registration_promises_.push_back(promise);
        }
    });

    if (future.wait_for(timeout) != std::future_status::ready) {
        // Timeout: remove the dangling promise on the executor thread.
        executor_.post(
            [this, promise]() { std::erase(registration_promises_, promise); });
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
                                 std::to_string(group_id));
    }
    return future.get();
}

// Agent interface: Group management

void AgentHost::doRegisterGroup(GroupDeclaration declaration,
                                MooncakeBackend* backend) {
    auto group_id = declaration.descriptor.group_id;
    backends_[group_id] = backend;
    agent_.registerGroup(declaration);

    DeclareGroupRequest req;
    req.rank = rank_;
    req.agent_session_epoch = agent_.getAgentSessionEpoch();
    req.group = std::move(declaration);

    auto resp = rpc_client_->call<&CoordinatorRpcService::declareGroup>(
        coordinator_addr_, std::move(req));

    if (resp.success) {
        // applyGroupView preserves the descriptor that was registered locally
        // and only updates the view returned by the Coordinator.
        runEffects(agent_.applyGroupView(group_id, resp.current_view));
    } else {
        LOG(ERROR) << "AgentHost: declareGroup failed for group " << group_id
                   << ": " << resp.reject_reason;
        auto it = group_ready_promises_.find(group_id);
        if (it != group_ready_promises_.end()) {
            for (auto& p : it->second) {
                p->set_exception(std::make_exception_ptr(std::runtime_error(
                    "declareGroup rejected: " + resp.reject_reason)));
            }
            group_ready_promises_.erase(it);
        }
    }
}

void AgentHost::registerGroup(GroupDeclaration declaration,
                              MooncakeBackend* backend) {
    executor_.postAndWait(
        [this, declaration = std::move(declaration), backend]() mutable {
            doRegisterGroup(std::move(declaration), backend);
        });
}

void AgentHost::unregisterGroup(GroupId group_id) {
    executor_.post([this, group_id]() {
        agent_.unregisterGroup(group_id);
        backends_.erase(group_id);
    });
}

GroupView AgentHost::getGroupView(GroupId group_id) {
    auto promise = std::make_shared<std::promise<GroupView>>();
    auto future = promise->get_future();
    executor_.post([this, group_id, promise]() {
        promise->set_value(agent_.getGroupView(group_id));
    });
    return future.get();
}

void AgentHost::doPublishLocalEndpoint(GroupEndpointPublication endpoint) {
    PublishEndpointRequest req;
    req.rank = rank_;
    req.agent_session_epoch = agent_.getAgentSessionEpoch();
    req.endpoints.push_back(std::move(endpoint));
    LOG(INFO) << "AgentHost: publishEndpoint rank=" << rank_
              << " epoch=" << req.agent_session_epoch << " to "
              << coordinator_addr_;
    rpc_client_->call<&CoordinatorRpcService::publishEndpoint>(
        coordinator_addr_, std::move(req));
}

void AgentHost::publishLocalEndpoint(GroupEndpointPublication endpoint) {
    LOG(INFO) << "AgentHost: publishLocalEndpoint group_id="
              << endpoint.group_id << " rank=" << rank_;
    executor_.postAndWait([this, endpoint = std::move(endpoint)]() mutable {
        doPublishLocalEndpoint(std::move(endpoint));
    });
}

// Agent interface: Membership proposals (synchronous)

ProposeViewUpdateResponse AgentHost::proposeActivate(
    GroupId group_id, const std::vector<GlobalRank>& ranks) {
    ProposeViewUpdateRequest req;
    req.group_id = group_id;
    req.source_rank = rank_;
    req.agent_session_epoch = agent_.getAgentSessionEpoch();
    req.requested_ranks = ranks;
    req.is_activate = true;
    return rpc_client_->call<&CoordinatorRpcService::proposeViewUpdate>(
        coordinator_addr_, req);
}

ProposeViewUpdateResponse AgentHost::proposeDeactivate(
    GroupId group_id, const std::vector<GlobalRank>& ranks) {
    ProposeViewUpdateRequest req;
    req.group_id = group_id;
    req.source_rank = rank_;
    req.agent_session_epoch = agent_.getAgentSessionEpoch();
    req.requested_ranks = ranks;
    req.is_activate = false;
    return rpc_client_->call<&CoordinatorRpcService::proposeViewUpdate>(
        coordinator_addr_, req);
}

// Agent interface: Transfer observation (thread-safe)

void AgentHost::pushTransferObservation(GroupId group_id,
                                        std::vector<uint8_t> attempted_ranks,
                                        std::vector<uint8_t> failed_ranks,
                                        std::vector<uint8_t> succeeded_ranks) {
    observation_queue_.enqueue(TransferObservationEvent{
        group_id, std::move(attempted_ranks), std::move(failed_ranks),
        std::move(succeeded_ranks)});
}

// RPC push callbacks

void AgentHost::postPeerJoined(PeerJoinedPush push) {
    executor_.post([this, push = std::move(push)]() {
        runEffects(agent_.handlePeerJoined(push));
    });
}

void AgentHost::postRankStateUpdate(RankStateUpdatePush push) {
    executor_.post([this, push = std::move(push)]() {
        runEffects(agent_.handleRankStateUpdate(push));
    });
}

void AgentHost::postViewUpdate(coro_rpc::context<ViewUpdateAck> ctx,
                               ViewUpdatePush push) {
    auto group_id = push.group_id;
    auto epoch = push.view.epoch;
    executor_.post([this, ctx = std::move(ctx), push = std::move(push),
                    group_id, epoch]() mutable {
        runEffects(agent_.handleViewUpdate(push));

        // Wake up waitUntilGroupReady callers when group reaches Ready status.
        if (push.view.status == GroupStatus::Ready) {
            auto it = group_ready_promises_.find(group_id);
            if (it != group_ready_promises_.end()) {
                for (auto& p : it->second) {
                    p->set_value(push.view);
                }
                group_ready_promises_.erase(it);
            }
        }

        ctx.response_msg(ViewUpdateAck{.rank = rank_,
                                       .group_id = group_id,
                                       .epoch = epoch,
                                       .applied = true,
                                       .error_msg = ""});
    });
}

// LinkManager event

void AgentHost::postTELinkEvent(TELinkEvent event) {
    executor_.post([this, event = std::move(event)]() {
        if (event.kind == TELinkEvent::Kind::LinkUp) {
            if (event.target_id.has_value()) {
                // LinkManager already updated the read model (publishLinkUp)
                // before emitting this event  - just update the state machine.
                runEffects(agent_.handleLinkStateChanged(event.peer, true));
            } else {
                LOG(WARNING) << "AgentHost: LinkUp event for peer "
                             << event.peer << " without target_id; ignoring.";
            }
        } else {
            // LinkDown: LinkManager already called publishLinkDown in
            // tearDownPeerLink.  P2P reset is modeled as a
            // ResetPeerP2PState effect from handleLinkStateChanged.
            runEffects(agent_.handleLinkStateChanged(event.peer, false));
        }
    });
}

// Internal: startRegisterRpc

void AgentHost::startRegisterRpc() {
    // Avoid duplicate registration RPCs.  This also covers the case where a
    // heartbeat response callback asks for re-registration while another
    // registration is already in flight.
    if (agent_.getCoordinatorConnection() ==
        AgentStateMachine::CoordinatorConnection::Registering) {
        return;
    }
    agent_.setCoordinatorRegistering();

    RegisterRequest req;
    req.rank = rank_;
    req.agent_addr = rpc_server_->getListenAddr(host_ip_);
    req.te_server_name = link_manager_.localServerName();
    req.warmup_recv_addr = link_manager_.getWarmupRecvAddr();
    req.agent_session_epoch = ++agent_session_epoch_;
    agent_.setAgentSessionEpoch(agent_session_epoch_);

    LOG(INFO) << "AgentHost: sending registerAgent to " << coordinator_addr_
              << " rank=" << rank_ << " epoch=" << agent_session_epoch_
              << " agent_addr=" << req.agent_addr;

    rpc_client_->callAsync<&CoordinatorRpcService::registerAgent>(
        coordinator_addr_, std::move(req), [this](RegisterResponse resp) {
            LOG(INFO) << "AgentHost: registerAgent callback fired, rank="
                      << rank_ << " success=" << resp.success;
            executor_.post([this, resp = std::move(resp)]() mutable {
                LOG(INFO) << "AgentHost: processing registerResponse, rank="
                          << rank_ << " success=" << resp.success;
                auto effects = agent_.applyRegisterResponse(resp);
                runEffects(effects);

                if (resp.success) {
                    if (!registration_done_) {
                        registration_done_ = true;
                        for (auto& p : registration_promises_) {
                            p->set_value();
                        }
                        registration_promises_.clear();
                    }

                    // Re-publish all local backends' endpoints after (re-)reg.
                    // (Old session endpoints were cleared by Coordinator.)
                    for (auto& [group_id, backend] : backends_) {
                        doPublishLocalEndpoint(
                            backend->buildEndpointMetadata());
                    }
                } else {
                    LOG(ERROR)
                        << "AgentHost: registerAgent failed: " << resp.error_msg
                        << " (will retry after heartbeat interval; if this "
                           "persists, the Coordinator may be rejecting a "
                           "replacement rank before the old one times out)";
                }
            });
        });
}

// Internal: tick

void AgentHost::tick() {
    if (!rpc_client_) return;

    // Consume transfer observation queue.
    TransferObservationEvent event;
    while (observation_queue_.try_dequeue(event)) {
        auto effects = agent_.processTransferObservation(event);
        for (auto& effect : effects) {
            if (auto* e = std::get_if<SendTransferObservation>(&effect)) {
                e->request.agent_session_epoch = agent_.getAgentSessionEpoch();
            }
        }
        runEffects(effects);
    }

    // Check if reconnect is needed.
    if (agent_.getCoordinatorConnection() ==
        AgentStateMachine::CoordinatorConnection::Disconnected) {
        if (rpc_client_->tryReconnect(coordinator_addr_)) {
            agent_.setCoordinatorRegistering();
            startRegisterRpc();
        }
        return;
    }

    // Do not send heartbeats while a registration is in flight; wait for the
    // registerAgent response first.
    if (agent_.getCoordinatorConnection() ==
        AgentStateMachine::CoordinatorConnection::Registering) {
        return;
    }

    // Send heartbeat.
    auto req = agent_.buildHeartbeat();
    req.agent_session_epoch = agent_.getAgentSessionEpoch();

    rpc_client_->callAsync<&CoordinatorRpcService::heartbeat>(
        coordinator_addr_, std::move(req), [this](HeartbeatResponse resp) {
            executor_.post([this, resp]() {
                if (resp.require_reregister) {
                    runEffects(agent_.prepareCleanSlateRegister());
                    startRegisterRpc();
                }
            });
        });

    // Check connection to Coordinator.
    if (!rpc_client_->isConnected(coordinator_addr_)) {
        runEffects(agent_.markOffline());
    }
}

// Internal: runEffects

void AgentHost::runEffects(const AgentApplyResult& effects) {
    for (const auto& effect : effects) {
        std::visit(
            overloaded{
                [this](const SendTransferObservation& e) {
                    rpc_client_->send<
                        &CoordinatorRpcService::reportTransferObservation>(
                        coordinator_addr_, e.request);
                },
                [this](const EnablePeerProbe& e) {
                    link_manager_.enablePeerProbe(e.rank, e.te_server_name,
                                                  e.warmup_recv_addr);
                },
                [this](const DisconnectLink& e) {
                    link_manager_.disconnect(e.peer);
                },
                [this](const StopReconnect& e) {
                    link_manager_.stopReconnect(e.peer);
                },
                [this](const ClearPeerMetadata& e) {
                    link_manager_.publishLinkDown(e.peer);
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
                [this](const PublishRankStateSnapshot& e) {
                    link_manager_.setRankStates(e.states);
                },
                [this](const ApplyViewToBackend& e) {
                    auto it = backends_.find(e.group_id);
                    if (it != backends_.end()) {
                        it->second->applyViewChange(e.descriptor, e.view);
                    }
                },
                [this](const MarkBackendViewStale& e) {
                    auto it = backends_.find(e.group_id);
                    if (it != backends_.end()) {
                        it->second->markViewStale();
                    }
                },
                [this](const NotifyTEUnreachable& e) {
                    forEachBackend([&](MooncakeBackend* backend) {
                        backend->onPeerLinkReset(e.peer);
                    });
                },
            },
            effect);
    }
}

}  // namespace mooncake
