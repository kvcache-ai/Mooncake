// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport/tcp_transport/tcp_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>
#include <asio/ip/v6_only.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

#include "cuda_alike.h"

namespace mooncake {
using tcpsocket = asio::ip::tcp::socket;

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
namespace {
using LaneConnectHandlerHook = void (*)() noexcept;
using LaneConnectFailureInjectionHook = bool (*)(size_t) noexcept;
using LaneRetryHandlerHook = void (*)() noexcept;
using LaneAdmissionHandlerHook = void (*)() noexcept;
using LaneObserverHook = void (*)(int, size_t, uint64_t, size_t, bool) noexcept;
using LaneFailureReasonHook = void (*)(int) noexcept;
using SessionProgressHook = int (*)(int, bool) noexcept;

std::mutex lane_test_hook_mutex;
LaneConnectHandlerHook lane_connect_handler_hook = nullptr;
LaneConnectFailureInjectionHook lane_connect_failure_injection_hook = nullptr;
LaneRetryHandlerHook lane_retry_handler_hook = nullptr;
LaneAdmissionHandlerHook lane_admission_handler_hook = nullptr;
LaneObserverHook lane_observer_hook = nullptr;
LaneFailureReasonHook lane_failure_reason_hook = nullptr;
SessionProgressHook session_progress_hook = nullptr;

enum LaneTestEvent {
    kLaneQueueAdmitted = 1,
    kLaneQueueRejected = 2,
    kLaneConnecting = 3,
    kLaneBusy = 4,
    kLaneTerminal = 5,
    kLaneShutdownClean = 6,
    kLaneLateHandler = 7,
    kLaneRetryArmed = 8,
    kLaneRetryFired = 9,
    kLaneRetryLate = 10,
    kLaneCooldownStarted = 11,
    kLaneAdmissionPending = 12,
    kLaneAdmissionPromoted = 13,
    kLaneAdmissionTimerArmed = 14,
    kLaneAdmissionTimerFired = 15,
    kLaneAdmissionTimerLate = 16,
    kLaneAdmissionHardRejected = 17,
};

enum SessionProgressTestEvent {
    kSessionReadBodySuccess = 1,
    kSessionWriteAckSuccess = 2,
    kSessionTimeoutCommitted = 3,
    kSessionTimeoutStale = 4,
    kSessionTerminal = 5,
};

enum SessionProgressTestAction {
    kSessionProgressNoAction = 0,
    kSessionCommitTimeoutBeforeProgress = 1,
    kSessionReplayPreviousTimeoutAfterProgress = 2,
};

void invokeLaneConnectHandlerHook() noexcept {
    LaneConnectHandlerHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_connect_handler_hook;
    }
    if (hook) hook();
}

bool invokeLaneConnectFailureInjectionHook(size_t lane_id) noexcept {
    LaneConnectFailureInjectionHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_connect_failure_injection_hook;
    }
    return hook && hook(lane_id);
}

void invokeLaneRetryHandlerHook() noexcept {
    LaneRetryHandlerHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_retry_handler_hook;
    }
    if (hook) hook();
}

void invokeLaneAdmissionHandlerHook() noexcept {
    LaneAdmissionHandlerHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_admission_handler_hook;
    }
    if (hook) hook();
}

void invokeLaneObserverHook(int event, size_t queue_depth,
                            uint64_t queued_bytes, size_t active_sockets,
                            bool lane_has_current) noexcept {
    LaneObserverHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_observer_hook;
    }
    if (hook)
        hook(event, queue_depth, queued_bytes, active_sockets,
             lane_has_current);
}

void invokeLaneFailureReasonHook(int reason) noexcept {
    LaneFailureReasonHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_failure_reason_hook;
    }
    if (hook) hook(reason);
}

int invokeSessionProgressHook(int event, bool detail) noexcept {
    SessionProgressHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = session_progress_hook;
    }
    return hook ? hook(event, detail) : kSessionProgressNoAction;
}
}  // namespace

void tcpTransportSetLaneConnectHandlerHookForTest(
    LaneConnectHandlerHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_connect_handler_hook = hook;
}

void tcpTransportSetLaneConnectFailureInjectionHookForTest(
    LaneConnectFailureInjectionHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_connect_failure_injection_hook = hook;
}

void tcpTransportSetLaneObserverHookForTest(LaneObserverHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_observer_hook = hook;
}

void tcpTransportSetLaneRetryHandlerHookForTest(
    LaneRetryHandlerHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_retry_handler_hook = hook;
}

void tcpTransportSetLaneAdmissionHandlerHookForTest(
    LaneAdmissionHandlerHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_admission_handler_hook = hook;
}

void tcpTransportSetLaneFailureReasonHookForTest(
    LaneFailureReasonHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_failure_reason_hook = hook;
}

void tcpTransportSetSessionProgressHookForTest(
    SessionProgressHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    session_progress_hook = hook;
}

bool tcpTransportLaneTypesAreMoveOnlyForTest() noexcept {
    return std::is_move_constructible<TcpTransport::TcpWorkItem>::value &&
           !std::is_copy_constructible<TcpTransport::TcpWorkItem>::value &&
           !std::is_copy_assignable<TcpTransport::TcpWorkItem>::value &&
           std::is_move_constructible<TcpTransport::TerminalAction>::value &&
           !std::is_copy_constructible<TcpTransport::TerminalAction>::value &&
           !std::is_copy_assignable<TcpTransport::TerminalAction>::value;
}
#endif

#include "tcp_transport_session_impl.h"

namespace {
constexpr size_t kMaxTcpLanesPerPeer = 16;

size_t parseBoundedTcpSetting(const char* name, const char* value,
                              size_t default_value, size_t minimum,
                              size_t maximum) {
    if (!value) return default_value;

    const std::string text(value);
    size_t parsed = 0;
    bool valid = !text.empty();
    for (char c : text) {
        if (c < '0' || c > '9') {
            valid = false;
            break;
        }
        const size_t digit = static_cast<size_t>(c - '0');
        if (parsed > (maximum - digit) / size_t(10)) {
            valid = false;
            break;
        }
        parsed = parsed * 10 + digit;
    }
    if (valid && parsed >= minimum && parsed <= maximum) return parsed;

    LOG(WARNING) << "Invalid " << name << " value: " << text
                 << ", using default " << default_value;
    return default_value;
}

bool validateTcpAddress(const std::shared_ptr<TransferMetadata>& metadata,
                        uint64_t addr, uint64_t size) {
    if (size == 0 || addr + size < addr) return false;

    auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!desc) return false;
    for (const auto& buffer : desc->buffers) {
        if (buffer.addr + buffer.length < buffer.addr) continue;
        if (buffer.addr <= addr && addr + size <= buffer.addr + buffer.length)
            return true;
    }
    return false;
}
}  // namespace

TcpTransport::TcpTransport()
    : context_(nullptr),
      running_(false),
      lane_state_(std::make_shared<ConnectionLaneState>()) {
    if (getenv("MC_TCP_ENABLE_CONNECTION_POOL") != nullptr) {
        std::string val(getenv("MC_TCP_ENABLE_CONNECTION_POOL"));
        std::transform(val.begin(), val.end(), val.begin(),
                       [](unsigned char c) -> char { return std::tolower(c); });
        if (val == "0" || val == "false" || val == "no") {
            enable_connection_pool_ = false;
        } else {
            enable_connection_pool_ = true;
        }
    }

    constexpr size_t kDefaultLanesPerPeer = 4;
    constexpr size_t kDefaultQueuedTransfersPerPeer = 1024;
    constexpr size_t kMaxQueuedTransfersPerPeer = 65535;
    constexpr size_t kDefaultPendingAdmissionsPerPeer = 1024;
    constexpr size_t kMaxPendingAdmissionsPerPeer = 65535;
    constexpr size_t kDefaultAdmissionTimeoutMs = 1000;
    constexpr size_t kMaxAdmissionTimeoutMs = 600000;

    const char* lanes_env = getenv("MC_TCP_LANES_PER_PEER");
    if (lanes_env) {
        lane_state_->lanes_per_peer = parseBoundedTcpSetting(
            "MC_TCP_LANES_PER_PEER", lanes_env, kDefaultLanesPerPeer, 1,
            kMaxTcpLanesPerPeer);
    } else {
        const char* deprecated_env = getenv("MC_TCP_MAX_CONNECTIONS_PER_PEER");
        if (deprecated_env) {
            LOG(WARNING) << "MC_TCP_MAX_CONNECTIONS_PER_PEER is deprecated; "
                            "use MC_TCP_LANES_PER_PEER";
            lane_state_->lanes_per_peer = parseBoundedTcpSetting(
                "MC_TCP_MAX_CONNECTIONS_PER_PEER", deprecated_env,
                kDefaultLanesPerPeer, 1, kMaxTcpLanesPerPeer);
        }
    }

    lane_state_->max_queued_transfers_per_peer = parseBoundedTcpSetting(
        "MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER",
        getenv("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER"),
        kDefaultQueuedTransfersPerPeer, 1, kMaxQueuedTransfersPerPeer);
    lane_state_->max_pending_admissions_per_peer = parseBoundedTcpSetting(
        "MC_TCP_MAX_PENDING_ADMISSIONS_PER_PEER",
        getenv("MC_TCP_MAX_PENDING_ADMISSIONS_PER_PEER"),
        kDefaultPendingAdmissionsPerPeer, 1, kMaxPendingAdmissionsPerPeer);
    lane_state_->admission_timeout =
        std::chrono::milliseconds(parseBoundedTcpSetting(
            "MC_TCP_ADMISSION_TIMEOUT_MS",
            getenv("MC_TCP_ADMISSION_TIMEOUT_MS"), kDefaultAdmissionTimeoutMs,
            1, kMaxAdmissionTimeoutMs));
}

TcpTransport::~TcpTransport() {
    shutdownConnectionLanes();

    if (context_) {
        delete context_;
        context_ = nullptr;
    }

    metadata_->removeSegmentDesc(local_server_name_);
}

int TcpTransport::startHandshakeDaemon() {
    return metadata_->startHandshakeDaemon(nullptr,
                                           metadata_->localRpcMeta().rpc_port,
                                           metadata_->localRpcMeta().sockfd);
}

int TcpTransport::install(std::string& local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;
    int sockfd = -1;
    int tcp_port = findAvailableTcpPort(sockfd);
    if (tcp_port == 0) {
        LOG(ERROR) << "TcpTransport: unable to find available tcp port for "
                      "data transmission";
        return -1;
    }

    int ret = allocateLocalSegmentID(tcp_port);
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot allocate local segment";
        return -1;
    }

    ret = startHandshakeDaemon();
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot start handshake daemon";
        return -1;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return -1;
    }

    close(sockfd);  // the above function has opened a socket
    LOG(INFO) << "TcpTransport: listen on port " << tcp_port;
    auto metadata = metadata_;
    context_ = new TcpContext(tcp_port, [metadata = std::move(metadata)](
                                            uint64_t addr, uint64_t size) {
        return validateTcpAddress(metadata, addr, size);
    });
    lane_runtime_ =
        std::make_shared<ConnectionLaneRuntime>(context_->io_context);
    lane_state_->runtime = lane_runtime_;
    running_ = true;
    thread_ = std::thread(&TcpTransport::worker, this);
    return 0;
}

int TcpTransport::allocateLocalSegmentID(int tcp_data_port) {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc) desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (!desc->protocol.empty()) desc->protocol += ",";
    desc->protocol += "tcp";
#else
    desc->protocol = "tcp";
#endif
    desc->tcp_data_port = tcp_data_port;
    // Advertise acknowledged framing (#2086); initiators fall back to v1
    // against descriptors that do not carry the field.
    desc->tcp_proto_version = 2;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int TcpTransport::registerLocalMemory(void* addr, size_t length,
                                      const std::string& location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    buffer_desc.name = local_server_name_;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = length;
#ifdef ENABLE_MULTI_PROTOCOL
    buffer_desc.protocol = "tcp";
#endif
    return metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
}

int TcpTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int TcpTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int TcpTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    for (auto& addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

Status TcpTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "TcpTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status TcpTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "TcpTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "TcpTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto& request : entries) {
        TransferTask& task = batch_desc.task_list[task_id];
        ++task_id;
        startTransfer(prepareTransfer(&task, request));
    }

    return Status::OK();
}

Status TcpTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    for (size_t i = 0; i < task_list.size();) {
        auto* task = task_list[i];
        assert(task && task->request);
        const auto group_id = task->request->task_group_id;
        if (group_id == TransferRequest::kNoTaskGroup) {
            startTransfer(prepareTransfer(task, *task->request));
            ++i;
            continue;
        }

        std::vector<Slice*> slices;
        do {
            task = task_list[i];
            assert(task && task->request);
            slices.push_back(prepareTransfer(task, *task->request));
            ++i;
        } while (i < task_list.size() && task_list[i]->request &&
                 task_list[i]->request->task_group_id == group_id &&
                 task_list[i - 1]->request + task_list[i - 1]->request_count ==
                     task_list[i]->request);
        startTransferSequence(std::move(slices));
    }
    return Status::OK();
}

Status TcpTransport::submitTransferTaskGroup(
    const std::vector<TransferTask*>& task_list) {
    std::vector<Slice*> slices;
    slices.reserve(task_list.size());
    for (auto* task : task_list) {
        assert(task && task->request);
        slices.push_back(prepareTransfer(task, *task->request));
    }
    startTransferSequence(std::move(slices));
    return Status::OK();
}

Transport::Slice* TcpTransport::prepareTransfer(
    TransferTask* task, const TransferRequest& request) {
    task->total_bytes = request.length;
    Slice* slice = getSliceCache().allocate();
    slice->source_addr = static_cast<char*>(request.source);
    slice->length = request.length;
    slice->opcode = request.opcode;
    slice->tcp.dest_addr = request.target_offset;
    slice->task = task;
    slice->target_id = request.target_id;
    slice->status = Slice::PENDING;
    slice->ts = 0;
    task->slice_list.push_back(slice);
    __sync_fetch_and_add(&task->slice_count, 1);
    return slice;
}

void TcpTransport::startTransferSequence(std::vector<Slice*> slices) {
    struct Sequence {
        std::mutex mutex;
        std::vector<Slice*> slices;
        size_t next = 0;
        bool advancing = false;
        bool resume_requested = false;
    };

    auto sequence = std::make_shared<Sequence>();
    sequence->slices = std::move(slices);

    auto advance = std::make_shared<std::function<void()>>();
    std::weak_ptr<std::function<void()>> weak_advance = advance;

    *advance = [this, sequence, weak_advance]() {
        auto advance = weak_advance.lock();
        if (!advance) return;

        {
            std::lock_guard<std::mutex> lock(sequence->mutex);
            if (sequence->next == sequence->slices.size()) return;
            if (sequence->advancing) {
                sequence->resume_requested = true;
                return;
            }
            sequence->advancing = true;
        }

        while (true) {
            Slice* slice = nullptr;
            bool has_more = false;
            {
                std::lock_guard<std::mutex> lock(sequence->mutex);
                if (sequence->next == sequence->slices.size()) {
                    sequence->advancing = false;
                    return;
                }
                slice = sequence->slices[sequence->next++];
                has_more = sequence->next < sequence->slices.size();
                sequence->resume_requested = false;
            }

            std::function<void()> continuation;
            if (has_more) {
                continuation = [advance]() { (*advance)(); };
            }

            // startTransfer may fail synchronously (including during shutdown).
            // The trampoline above turns a synchronous continuation into
            // another loop iteration rather than recursive calls. For an
            // asynchronous completion, this invocation returns and the
            // continuation becomes the next runner.
            startTransfer(slice, std::move(continuation), true);

            {
                std::lock_guard<std::mutex> lock(sequence->mutex);
                if (!sequence->resume_requested) {
                    sequence->advancing = false;
                    return;
                }
            }
        }
    };

    (*advance)();
}

void TcpTransport::worker() {
    while (running_) {
        try {
            context_->doAccept();
            context_->io_context.run();
        } catch (std::exception& e) {
            LOG(ERROR) << "TcpTransport::worker encountered an exception "
                          "during doAccept/run: "
                       << e.what();
            context_->io_context.restart();
        }
    }
}

std::shared_ptr<asio::ip::tcp::socket> TcpTransport::getConnection(
    const std::string& host, uint16_t port) {
    // The reusable path is owned by fixed connection lanes. This helper is
    // only for the connection-pool-disabled one-shot path.
    try {
        asio::ip::tcp::resolver resolver(context_->io_context);
        auto endpoint_iterator = resolver.resolve(host, std::to_string(port));
        auto socket_ptr =
            std::make_shared<asio::ip::tcp::socket>(context_->io_context);
        asio::connect(*socket_ptr, endpoint_iterator);
        socket_ptr->set_option(asio::ip::tcp::no_delay(true));
        return socket_ptr;
    } catch (std::exception& e) {
        LOG(ERROR)
            << "TcpTransport::getConnection failed to create connection to "
            << host << ":" << port << ". Error: " << e.what();
        return nullptr;
    }
}

#include "tcp_transport_lane_impl.h"

void TcpTransport::startTransfer(Slice* slice,
                                 std::function<void()> continuation,
                                 bool reuse_connection) {
    auto finish = [slice, &continuation](TransferStatusEnum status) mutable {
        if (status == TransferStatusEnum::COMPLETED)
            slice->markSuccess();
        else
            slice->markFailed();
        if (continuation) {
            auto next = std::move(continuation);
            next();
        }
    };

    auto desc = metadata_->getSegmentDescByID(slice->target_id);
    if (!desc) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get segment "
                      "description for target_id: "
                   << slice->target_id;
        finish(TransferStatusEnum::FAILED);
        return;
    }

    TransferMetadata::RpcMetaDesc meta_entry;
    if (metadata_->getRpcMetaEntry(desc->name, meta_entry)) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get RPC meta "
                      "entry for segment name: "
                   << desc->name;
        finish(TransferStatusEnum::FAILED);
        return;
    }

    // Zero-length requests are complete by definition. v1 reported them
    // COMPLETED while the server silently rejected size==0 in address
    // validation; preserve that outcome without a round trip.
    if (slice->length == 0) {
        finish(TransferStatusEnum::COMPLETED);
        return;
    }

    const ConnectionKey key{meta_entry.ip_or_host_name,
                            static_cast<uint16_t>(desc->tcp_data_port)};
    const bool use_v2 = desc->tcp_proto_version >= 2 && !forceLegacyTcpProto();
    TcpWorkItem work(slice, use_v2, std::move(continuation));

    // Scatter task groups request reuse even when the general pool setting is
    // disabled. Fixed lanes provide the same serial socket reuse without
    // reviving the old unbounded dynamic pool.
    if (enable_connection_pool_ || reuse_connection) {
        enqueuePooledTransfer(key, std::move(work));
        return;
    }

    // Preserve the connection-pool-disabled synchronous one-shot path.
    auto socket = getConnection(key.host, key.port);
    if (!socket) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get connection to "
                   << key.host << ":" << key.port;
        completeTerminalAction(
            TerminalAction(std::move(work), TransferStatusEnum::FAILED, false));
        return;
    }
    startTransferWithSocket(std::move(work), std::move(socket));
}

void TcpTransport::startTransferWithSocket(
    TcpWorkItem work, std::shared_ptr<asio::ip::tcp::socket> socket) noexcept {
    const Slice* slice = work.slice;
    std::shared_ptr<std::optional<TcpWorkItem>> terminal_work;
    try {
        terminal_work =
            std::make_shared<std::optional<TcpWorkItem>>(std::move(work));
        auto session = std::make_shared<ClientSession>(
            socket, terminal_work->value().use_v2,
            [terminal_work, socket](TransferStatusEnum status, bool) noexcept {
                closeSocketNoThrow(socket);
                if (!terminal_work->has_value()) return;
                auto completed = std::move(terminal_work->value());
                terminal_work->reset();
                completeTerminalAction(
                    TerminalAction(std::move(completed), status, false));
            });
        session->initiate(terminal_work->value().slice->source_addr,
                          terminal_work->value().slice->tcp.dest_addr,
                          terminal_work->value().slice->length,
                          terminal_work->value().slice->opcode);
    } catch (const std::exception& e) {
        LOG(ERROR) << "TcpTransport::startTransfer encountered an exception. "
                      "Slice details - source_addr: "
                   << slice->source_addr << ", length: " << slice->length
                   << ", opcode: " << (int)slice->opcode
                   << ", target_id: " << slice->target_id
                   << ". Exception: " << e.what();
        closeSocketNoThrow(socket);
        if (terminal_work && terminal_work->has_value()) {
            auto failed = std::move(terminal_work->value());
            terminal_work->reset();
            failWorkItem(std::move(failed), WorkFailureReason::SESSION_FAILED,
                         lane_state_->failure_counters);
        } else if (work.slice) {
            failWorkItem(std::move(work), WorkFailureReason::SESSION_FAILED,
                         lane_state_->failure_counters);
        }
    } catch (...) {
        LOG(ERROR) << "TcpTransport::startTransfer encountered an unknown "
                      "exception. Slice details - source_addr: "
                   << slice->source_addr << ", length: " << slice->length
                   << ", opcode: " << (int)slice->opcode
                   << ", target_id: " << slice->target_id;
        closeSocketNoThrow(socket);
        if (terminal_work && terminal_work->has_value()) {
            auto failed = std::move(terminal_work->value());
            terminal_work->reset();
            failWorkItem(std::move(failed), WorkFailureReason::SESSION_FAILED,
                         lane_state_->failure_counters);
        } else if (work.slice) {
            failWorkItem(std::move(work), WorkFailureReason::SESSION_FAILED,
                         lane_state_->failure_counters);
        }
    }
}

}  // namespace mooncake
