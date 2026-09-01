// Copyright 2026 KVCache.AI
#include "tent/transport/hp_tcp/hp_tcp_transport.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <random>
#include <sstream>
#include <utility>

#include <glog/logging.h>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"

namespace mooncake::tent {
namespace {

constexpr uint64_t kIoProgressStepBytes = 1ULL << 20;

std::string HostFromRpc(const std::string& address) {
    if (address.empty()) return {};
    if (address.front() == '[') {
        const auto close = address.find(']');
        return close == std::string::npos ? std::string{}
                                          : address.substr(1, close - 1);
    }
    const auto colon = address.rfind(':');
    if (colon == std::string::npos) return address;
    // An unbracketed address with multiple colons is IPv6 and cannot be
    // safely split as host:port. RPC addresses produced by TENT are bracketed,
    // so reject rather than guessing.
    if (address.find(':') != colon) return {};
    return address.substr(0, colon);
}

bool HasTransport(const BufferDesc& buffer, TransportType type) {
    return std::find(buffer.transports.begin(), buffer.transports.end(),
                     type) != buffer.transports.end();
}

bool RemotePermissionAllows(const HighPerformanceTcpBufferAttr& attr,
                            Request::OpCode opcode) {
    if (opcode == Request::READ) {
        return attr.permission == "global_read_only" ||
               attr.permission == "global_read_write";
    }
    return attr.permission == "global_read_write";
}

Status FirstError(Status first, const Status& next) {
    if (first.ok() && !next.ok()) return next;
    return first;
}

Status NeedsRefresh(const std::string& message) {
    return Status::NeedsRefreshCache(message + LOC_MARK);
}

Status CheckRuntimeHealth(
    const HighPerformanceTcpWorkers* workers,
    const HighPerformanceTcpAdmissionController* admission) {
    if (workers != nullptr && workers->hasFailedWorker()) {
        return Status::InternalError(
            "HP TCP worker runtime has failed" LOC_MARK);
    }
    if (admission != nullptr && admission->failed()) {
        return Status::InternalError(
            "HP TCP admission accounting has failed" LOC_MARK);
    }
    return Status::OK();
}

Status RemoteWireStatus(HighPerformanceTcpStatus status) {
    switch (status) {
        case HighPerformanceTcpStatus::kStaleRegistration:
            return Status::NeedsRefreshCache(
                "remote HP TCP registration is stale" LOC_MARK);
        case HighPerformanceTcpStatus::kPermissionDenied:
            return Status::AddressNotRegistered(
                "remote HP TCP permission denied" LOC_MARK);
        case HighPerformanceTcpStatus::kRangeRejected:
            return Status::AddressNotRegistered(
                "remote HP TCP range rejected" LOC_MARK);
        case HighPerformanceTcpStatus::kShuttingDown:
            return Status::TooManyRequests(
                "remote HP TCP transport is shutting down" LOC_MARK);
        case HighPerformanceTcpStatus::kOk:
            return Status::OK();
        default:
            return Status::InvalidEntry(
                "remote HP TCP protocol status " +
                std::to_string(static_cast<uint16_t>(status)) + LOC_MARK);
    }
}

}  // namespace

struct HighPerformanceTcpTransport::TaskPlan {
    std::shared_ptr<HighPerformanceTcpTaskState> task;
    HighPerformanceTcpWorkers::Command command;
};

HighPerformanceTcpTransport::HighPerformanceTcpTransport()
    : HighPerformanceTcpTransport(HighPerformanceTcpParams{}) {}

HighPerformanceTcpTransport::HighPerformanceTcpTransport(
    HighPerformanceTcpParams params)
    : params_(std::move(params)) {
    caps.dram_to_dram = true;
}

HighPerformanceTcpTransport::~HighPerformanceTcpTransport() {
    const Status status = uninstall();
    if (!status.ok()) {
        LOG(ERROR) << "HP TCP destructor uninstall failed: "
                   << status.ToString();
    }
}

Status HighPerformanceTcpTransport::validateParams() const {
    if (params_.worker_count == 0 || params_.connections_per_peer == 0 ||
        params_.max_outstanding_tasks == 0 ||
        params_.max_outstanding_bytes == 0 || params_.max_transfer_bytes == 0 ||
        params_.connect_timeout_ms == 0 || params_.progress_timeout_ms == 0) {
        return Status::InvalidArgument(
            "invalid high-performance TCP limits" LOC_MARK);
    }
    return Status::OK();
}

std::string HighPerformanceTcpTransport::makeIncarnation() const {
    std::random_device device;
    std::mt19937_64 random(device());
    std::ostringstream out;
    for (int i = 0; i < 2; ++i) {
        out << std::hex << std::setw(16) << std::setfill('0') << random();
    }
    return out.str();
}

Status HighPerformanceTcpTransport::rollbackPublishedEndpoint(
    const std::optional<std::string>& previous_attr) {
    if (!metadata_) return Status::OK();
    Status first = metadata_->segmentManager().updateLocal(
        [&](SegmentDesc& desc) -> Status {
            if (desc.type != SegmentType::Memory) {
                return Status::InvalidMetadataType(
                    "local segment is not memory while rolling back HP "
                    "TCP" LOC_MARK);
            }
            auto& attrs =
                std::get<MemorySegmentDesc>(desc.detail).transport_attrs;
            if (previous_attr.has_value()) {
                attrs[static_cast<int>(TransportType::HP_TCP)] = *previous_attr;
            } else {
                attrs.erase(static_cast<int>(TransportType::HP_TCP));
            }
            return Status::OK();
        });
    if (first.ok()) {
        first = metadata_->segmentManager().synchronizeLocal();
    }
    return first;
}

Status HighPerformanceTcpTransport::install(
    std::string&, std::shared_ptr<ControlService> metadata,
    std::shared_ptr<Topology>, std::shared_ptr<Config>) {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    if (installed_.load(std::memory_order_acquire) || workers_ || server_ ||
        client_ || admission_) {
        return Status::InvalidArgument(
            "HP TCP transport is already installed or was not fully torn "
            "down" LOC_MARK);
    }
    CHECK_STATUS(validateParams());
    CHECK_STATUS(registry_.reopen());
    if (!metadata) {
        return Status::InvalidArgument("HP TCP metadata is null" LOC_MARK);
    }

    metadata_ = std::move(metadata);
    stopping_.store(false, std::memory_order_release);

    std::optional<std::string> previous_attr;
    {
        const SegmentDescRef local = metadata_->segmentManager().getLocal();
        if (!local || local->type != SegmentType::Memory) {
            metadata_.reset();
            return Status::InvalidMetadataType(
                "HP TCP requires a local memory segment" LOC_MARK);
        }
        const auto& attrs = local->getMemory().transport_attrs;
        const auto it = attrs.find(static_cast<int>(TransportType::HP_TCP));
        if (it != attrs.end()) previous_attr = it->second;
    }

    admission_ = std::make_unique<HighPerformanceTcpAdmissionController>(
        params_.max_outstanding_tasks, params_.max_outstanding_bytes);
    workers_ = std::make_unique<HighPerformanceTcpWorkers>(
        HighPerformanceTcpWorkers::Config{params_.worker_count});

    Status status = workers_->start();
    if (!status.ok()) {
        admission_.reset();
        workers_.reset();
        metadata_.reset();
        return status;
    }

    client_ = std::make_unique<HighPerformanceTcpClient>(
        HighPerformanceTcpClient::Config{
            params_.max_transfer_bytes,
            static_cast<size_t>(std::min<uint64_t>(kIoProgressStepBytes,
                                                   params_.max_transfer_bytes)),
            params_.connect_timeout_ms, params_.progress_timeout_ms,
            params_.connections_per_peer},
        workers_.get());

    const uint64_t max_connections_u64 = std::max<uint64_t>(
        params_.connections_per_peer, params_.max_outstanding_tasks);
    const size_t max_connections = static_cast<size_t>(std::min<uint64_t>(
        max_connections_u64, std::numeric_limits<size_t>::max()));
    server_ = std::make_unique<HighPerformanceTcpServer>(
        HighPerformanceTcpServer::Config{
            params_.bind_address, params_.port, params_.max_transfer_bytes,
            static_cast<size_t>(std::min<uint64_t>(kIoProgressStepBytes,
                                                   params_.max_transfer_bytes)),
            params_.progress_timeout_ms, max_connections},
        &registry_, workers_.get());

    uint16_t bound_port = 0;
    status = server_->start(&bound_port);
    if (!status.ok()) {
        (void)server_->stop();
        (void)client_->cancelAll(CANCELED);
        (void)workers_->stop();
        server_.reset();
        client_.reset();
        workers_.reset();
        admission_.reset();
        metadata_.reset();
        return status;
    }

    const SegmentDescRef local = metadata_->segmentManager().getLocal();
    const std::string host = params_.advertise_address.empty()
                                 ? HostFromRpc(local->rpc_server_addr)
                                 : params_.advertise_address;
    if (host.empty()) {
        status = Status::InvalidArgument(
            "unable to derive HP TCP advertise address" LOC_MARK);
    } else {
        const std::string incarnation = makeIncarnation();
        std::string encoded;
        status = EncodeHighPerformanceTcpEndpointAttr(
            {incarnation, host, bound_port, params_.max_transfer_bytes},
            &encoded);
        if (status.ok()) {
            status = metadata_->segmentManager().updateLocal(
                [&](SegmentDesc& desc) -> Status {
                    if (desc.type != SegmentType::Memory) {
                        return Status::InvalidMetadataType(
                            "HP TCP local segment changed type" LOC_MARK);
                    }
                    std::get<MemorySegmentDesc>(desc.detail)
                        .transport_attrs[static_cast<int>(
                            TransportType::HP_TCP)] = encoded;
                    return Status::OK();
                });
        }
        if (status.ok()) {
            status = metadata_->segmentManager().synchronizeLocal();
        }
    }

    if (!status.ok()) {
        const Status rollback = rollbackPublishedEndpoint(previous_attr);
        if (!rollback.ok()) {
            LOG(ERROR) << "HP TCP install metadata rollback failed: "
                       << rollback.ToString();
        }
        (void)server_->stopAccepting();
        (void)client_->cancelAll(CANCELED);
        (void)server_->cancelAll();
        (void)server_->stop();
        (void)workers_->stop();
        server_.reset();
        client_.reset();
        workers_.reset();
        admission_.reset();
        metadata_.reset();
        return status;
    }

    metadata_->setNotifyCallback([this](const Notification& notification) {
        RWSpinlock::WriteGuard guard(notify_lock_);
        notifications_.push_back(notification);
        return 0;
    });
    installed_.store(true, std::memory_order_release);
    return Status::OK();
}

Status HighPerformanceTcpTransport::stopRuntime() {
    // Lifecycle invariant: this is the sole normal teardown order. Client and
    // server async callbacks hold raw parent pointers, so they must quiesce
    // before their owners are destroyed and before worker contexts disappear.
    Status first = Status::OK();
    if (admission_) admission_->close();
    if (server_) first = FirstError(std::move(first), server_->stopAccepting());
    registry_.close();

    if (workers_) first = FirstError(std::move(first), workers_->barrier());
    if (client_)
        first = FirstError(std::move(first), client_->cancelAll(CANCELED));
    if (server_) first = FirstError(std::move(first), server_->cancelAll());

    if (admission_) {
        if (workers_ && workers_->hasFailedWorker() &&
            (admission_->outstandingTasks() != 0 ||
             admission_->outstandingBytes() != 0)) {
            first = FirstError(
                std::move(first),
                Status::InternalError("HP TCP admission cannot drain after "
                                      "worker failure" LOC_MARK));
        } else {
            first = FirstError(std::move(first), admission_->waitForZero());
        }
    }
    if (server_) first = FirstError(std::move(first), server_->stop());
    if (first.ok()) {
        DCHECK(!admission_ || (admission_->outstandingTasks() == 0 &&
                               admission_->outstandingBytes() == 0));
        DCHECK(!client_ || client_->activeOperations() == 0);
        DCHECK(!server_ || server_->activeSessionsForTest() == 0);
    }
    if (workers_) first = FirstError(std::move(first), workers_->stop());
    return first;
}

Status HighPerformanceTcpTransport::quiesce() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    if (!workers_ && !server_ && !client_ && !admission_) return Status::OK();
    if (stopping_.exchange(true, std::memory_order_acq_rel)) {
        // A previous quiesce under the same lifecycle lock completed all
        // cancellation/join work before returning.
        return Status::OK();
    }
    return stopRuntime();
}

Status HighPerformanceTcpTransport::uninstall() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    if (!installed_.load(std::memory_order_acquire) && !workers_ && !server_ &&
        !client_ && !admission_) {
        return Status::OK();
    }

    Status first = Status::OK();
    if (!stopping_.exchange(true, std::memory_order_acq_rel)) {
        first = stopRuntime();
    }

    if (metadata_) {
        metadata_->setNotifyCallback(nullptr);
        Status removed = metadata_->segmentManager().updateLocal(
            [](SegmentDesc& desc) -> Status {
                if (desc.type != SegmentType::Memory) {
                    return Status::InvalidMetadataType(
                        "HP TCP local segment is not memory during "
                        "uninstall" LOC_MARK);
                }
                std::get<MemorySegmentDesc>(desc.detail)
                    .transport_attrs.erase(
                        static_cast<int>(TransportType::HP_TCP));
                return Status::OK();
            });
        first = FirstError(std::move(first), removed);
        if (removed.ok()) {
            first = FirstError(std::move(first),
                               metadata_->segmentManager().synchronizeLocal());
        }
    }

    server_.reset();
    client_.reset();
    workers_.reset();
    admission_.reset();
    metadata_.reset();
    installed_.store(false, std::memory_order_release);
    return first;
}

Status HighPerformanceTcpTransport::allocateSubBatch(SubBatchRef& batch,
                                                     size_t max_size) {
    if (batch != nullptr) {
        return Status::InvalidArgument(
            "HP TCP SubBatch output must be null" LOC_MARK);
    }
    auto* result = Slab<HighPerformanceTcpSubBatch>::Get().allocate();
    if (result == nullptr) {
        return Status::InternalError(
            "unable to allocate HP TCP SubBatch" LOC_MARK);
    }
    result->max_size = max_size;
    result->tasks.clear();
    try {
        result->tasks.reserve(max_size);
    } catch (...) {
        Slab<HighPerformanceTcpSubBatch>::Get().deallocate(result);
        return Status::InternalError(
            "unable to reserve HP TCP SubBatch storage" LOC_MARK);
    }
    batch = result;
    return Status::OK();
}

Status HighPerformanceTcpTransport::freeSubBatch(SubBatchRef& batch) {
    auto* hp_batch = dynamic_cast<HighPerformanceTcpSubBatch*>(batch);
    if (hp_batch == nullptr) {
        return Status::InvalidArgument("invalid HP TCP SubBatch" LOC_MARK);
    }
    for (const auto& task : hp_batch->tasks) {
        const TransferStatusEnum state = task->snapshot().s;
        if (state == INITIAL || state == PENDING) {
            return Status::InvalidArgument(
                "cannot free an HP TCP SubBatch with pending tasks" LOC_MARK);
        }
    }
    hp_batch->tasks.clear();
    hp_batch->max_size = 0;
    Slab<HighPerformanceTcpSubBatch>::Get().deallocate(hp_batch);
    batch = nullptr;
    return Status::OK();
}

Status HighPerformanceTcpTransport::planTask(const Request& request,
                                             HighPerformanceTcpSubBatch* batch,
                                             TaskPlan* plan) {
    if (batch == nullptr || plan == nullptr) {
        return Status::InvalidArgument(
            "invalid HP TCP task plan output" LOC_MARK);
    }
    if (request.source == nullptr || request.length == 0 ||
        request.length > params_.max_transfer_bytes ||
        (request.opcode != Request::READ && request.opcode != Request::WRITE)) {
        return Status::InvalidArgument("invalid HP TCP request" LOC_MARK);
    }

    HighPerformanceTcpBufferRegistry::Lease local_lease;
    const uint64_t local_addr =
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(request.source));
    CHECK_STATUS(
        registry_.acquireLocalLease(local_addr, request.length, &local_lease));

    HighPerformanceTcpEndpointAttr endpoint_attr;
    HighPerformanceTcpBufferAttr buffer_attr;
    SegmentDescRef pin;
    Status resolved = metadata_->segmentManager().withCachedSegment(
        request.target_id, pin, [&](SegmentDesc* segment) -> Status {
            if (segment == nullptr || segment->type != SegmentType::Memory) {
                return NeedsRefresh("HP TCP target is not a memory segment");
            }
            BufferDesc* buffer =
                segment->findBuffer(request.target_offset, request.length);
            if (buffer == nullptr ||
                !HasTransport(*buffer, TransportType::HP_TCP)) {
                return NeedsRefresh("HP TCP target buffer is not advertised");
            }
            const auto endpoint_it = segment->getMemory().transport_attrs.find(
                static_cast<int>(TransportType::HP_TCP));
            if (endpoint_it == segment->getMemory().transport_attrs.end()) {
                return NeedsRefresh("HP TCP endpoint metadata is missing");
            }
            Status decoded = DecodeHighPerformanceTcpEndpointAttr(
                endpoint_it->second, &endpoint_attr);
            if (!decoded.ok()) {
                return NeedsRefresh("HP TCP endpoint metadata is incompatible");
            }
            const auto registration_it =
                buffer->transport_attrs.find(TransportType::HP_TCP);
            if (registration_it == buffer->transport_attrs.end()) {
                return NeedsRefresh("HP TCP buffer registration is missing");
            }
            decoded = DecodeHighPerformanceTcpBufferAttr(
                registration_it->second, &buffer_attr);
            if (!decoded.ok()) {
                return NeedsRefresh("HP TCP buffer metadata is incompatible");
            }
            return Status::OK();
        });
    if (!resolved.ok()) return resolved;

    if (request.length > endpoint_attr.max_transfer_bytes) {
        return Status::InvalidArgument(
            "HP TCP request exceeds remote endpoint capability" LOC_MARK);
    }
    if (!RemotePermissionAllows(buffer_attr, request.opcode)) {
        return Status::AddressNotRegistered(
            "HP TCP remote permission does not allow requested "
            "operation" LOC_MARK);
    }

    uint64_t request_id =
        next_request_id_.fetch_add(1, std::memory_order_relaxed);
    if (request_id == 0) {
        return Status::InternalError(
            "HP TCP request id space exhausted" LOC_MARK);
    }
    const uint32_t lane_id = static_cast<uint32_t>(
        request_id % static_cast<uint64_t>(params_.connections_per_peer));
    const size_t owner_worker =
        workers_->affinityOwner(request.target_id, lane_id);

    auto task = std::make_shared<HighPerformanceTcpTaskState>(
        request.length, batch->progress_batch_id, batch->notify_progress,
        std::move(local_lease));
    task->setDispatchIdentity(owner_worker, request_id);

    HighPerformanceTcpClient::Operation operation;
    operation.peer_id = request.target_id;
    operation.incarnation = endpoint_attr.incarnation;
    operation.host = endpoint_attr.host;
    operation.port = endpoint_attr.port;
    operation.lane_id = lane_id;
    operation.registration_id = buffer_attr.registration_id;
    operation.remote_addr = request.target_offset;
    operation.local_addr = request.source;
    operation.length = request.length;
    operation.opcode = request.opcode == Request::READ
                           ? HighPerformanceTcpOpcode::kRead
                           : HighPerformanceTcpOpcode::kWrite;
    operation.request_id = request_id;
    operation.complete =
        [task](TransferStatusEnum terminal, size_t bytes,
               std::optional<HighPerformanceTcpStatus> remote_status) {
            (void)task->completeOnce(terminal, bytes, remote_status);
        };

    HighPerformanceTcpWorkers::Command command;
    command.worker_id = owner_worker;
    command.run = [this, task,
                   operation = std::move(operation)](size_t worker_id) mutable {
        if (task->cancelRequested() ||
            stopping_.load(std::memory_order_acquire)) {
            (void)task->completeOnce(CANCELED, 0);
            return;
        }
        client_->enqueueOnOwner(worker_id, std::move(operation));
    };
    command.cancel = [task] { (void)task->completeOnce(CANCELED, 0); };

    plan->task = std::move(task);
    plan->command = std::move(command);
    return Status::OK();
}

Status HighPerformanceTcpTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& requests) {
    auto* hp_batch = dynamic_cast<HighPerformanceTcpSubBatch*>(batch);
    if (hp_batch == nullptr) {
        return Status::InvalidArgument("invalid HP TCP SubBatch" LOC_MARK);
    }
    if (!installed_.load(std::memory_order_acquire) ||
        stopping_.load(std::memory_order_acquire) || workers_ == nullptr ||
        client_ == nullptr || admission_ == nullptr) {
        return Status::InvalidArgument(
            "HP TCP transport is unavailable" LOC_MARK);
    }
    CHECK_STATUS(CheckRuntimeHealth(workers_.get(), admission_.get()));
    if (requests.empty()) return Status::OK();
    if (requests.size() > hp_batch->max_size - hp_batch->tasks.size()) {
        return Status::TooManyRequests(
            "HP TCP SubBatch capacity exceeded" LOC_MARK);
    }

    uint64_t total_bytes = 0;
    for (const Request& request : requests) {
        if (request.length >
                std::numeric_limits<uint64_t>::max() - total_bytes ||
            request.length == 0) {
            return Status::InvalidArgument(
                "HP TCP batch byte count overflow" LOC_MARK);
        }
        total_bytes += request.length;
    }

    std::vector<TaskPlan> plans;
    std::vector<HighPerformanceTcpWorkers::Command> commands;
    try {
        plans.resize(requests.size());
        commands.reserve(requests.size());
    } catch (...) {
        return Status::InternalError(
            "unable to allocate HP TCP task planning storage" LOC_MARK);
    }

    for (size_t i = 0; i < requests.size(); ++i) {
        CHECK_STATUS(planTask(requests[i], hp_batch, &plans[i]));
    }
    for (auto& plan : plans) {
        commands.push_back(std::move(plan.command));
    }

    // SubBatch capacity was reserved at allocateSubBatch(). The callback below
    // therefore performs only noexcept shared_ptr moves and reservation flag
    // stores while dispatch ownership is being committed.
    const size_t old_size = hp_batch->tasks.size();
    Status committed = workers_->tryCommitBatch(
        commands, admission_.get(), requests.size(), total_bytes, [&] {
            for (auto& plan : plans) {
                plan.task->activateReservation(admission_.get());
                hp_batch->tasks.push_back(std::move(plan.task));
            }
        });
    if (!committed.ok()) {
        // The worker transaction promises not to invoke on_commit on failure.
        // Keep a defensive assertion in debug logs without mutating the batch.
        if (hp_batch->tasks.size() != old_size) {
            LOG(FATAL) << "HP TCP atomic admission violated SubBatch rollback";
        }
        return committed;
    }
    return Status::OK();
}

Status HighPerformanceTcpTransport::getTransferStatus(SubBatchRef batch,
                                                      int task_id,
                                                      TransferStatus& status) {
    auto* hp_batch = dynamic_cast<HighPerformanceTcpSubBatch*>(batch);
    if (hp_batch == nullptr || task_id < 0 ||
        static_cast<size_t>(task_id) >= hp_batch->tasks.size()) {
        return Status::InvalidArgument("invalid HP TCP task id" LOC_MARK);
    }
    const auto& task = hp_batch->tasks[static_cast<size_t>(task_id)];
    status = task->snapshot();
    if (status.s == PENDING) {
        return CheckRuntimeHealth(workers_.get(), admission_.get());
    }
    const auto remote_status = task->remoteStatus();
    if (remote_status.has_value()) return RemoteWireStatus(*remote_status);
    return Status::OK();
}

Status HighPerformanceTcpTransport::retryTransferTask(SubBatchRef batch,
                                                      int task_id,
                                                      const Request& request) {
    auto* hp_batch = dynamic_cast<HighPerformanceTcpSubBatch*>(batch);
    if (hp_batch == nullptr || task_id < 0 ||
        static_cast<size_t>(task_id) >= hp_batch->tasks.size()) {
        return Status::InvalidArgument("invalid HP TCP retry task id" LOC_MARK);
    }
    CHECK_STATUS(CheckRuntimeHealth(workers_.get(), admission_.get()));
    if (hp_batch->tasks[static_cast<size_t>(task_id)]->snapshot().s != FAILED) {
        return Status::InvalidArgument(
            "HP TCP retry requires a failed attempt" LOC_MARK);
    }

    TaskPlan plan;
    CHECK_STATUS(planTask(request, hp_batch, &plan));
    std::vector<HighPerformanceTcpWorkers::Command> commands;
    try {
        commands.push_back(std::move(plan.command));
    } catch (...) {
        return Status::InternalError(
            "unable to allocate HP TCP retry command" LOC_MARK);
    }

    Status committed = workers_->tryCommitBatch(
        commands, admission_.get(), 1, request.length, [&] {
            plan.task->activateReservation(admission_.get());
            hp_batch->tasks[static_cast<size_t>(task_id)] =
                std::move(plan.task);
        });
    return committed;
}

Status HighPerformanceTcpTransport::cancelTransferTask(SubBatchRef batch,
                                                       int task_id) {
    auto* hp_batch = dynamic_cast<HighPerformanceTcpSubBatch*>(batch);
    if (hp_batch == nullptr || task_id < 0 ||
        static_cast<size_t>(task_id) >= hp_batch->tasks.size()) {
        return Status::InvalidArgument("invalid HP TCP task id" LOC_MARK);
    }
    const auto& task = hp_batch->tasks[static_cast<size_t>(task_id)];
    const TransferStatusEnum state = task->snapshot().s;
    if (state != INITIAL && state != PENDING) return Status::OK();

    task->requestCancel();
    if (client_ == nullptr || workers_ == nullptr) return Status::OK();
    const Status canceled =
        client_->cancelRequest(task->ownerWorker(), task->requestId());
    // A request still in the worker dispatch queue has no client lane yet;
    // its command observes cancelRequested() and settles it. Treat inability
    // to find/post a lane cancellation during shutdown as best effort.
    if (canceled.IsInternalError() &&
        stopping_.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    return canceled;
}

Status HighPerformanceTcpTransport::addMemoryBuffer(
    BufferDesc& desc, const MemoryOptions& options) {
    if (stopping_.load(std::memory_order_acquire)) {
        return Status::TooManyRequests(
            "HP TCP transport is shutting down" LOC_MARK);
    }
    const LocationParser location(desc.location);
    if ((location.type() != "cpu" && location.type() != kWildcardLocation) ||
        Platform::getLoader().getMemoryType(
            reinterpret_cast<void*>(desc.addr)) != MTYPE_CPU) {
        return Status::InvalidArgument(
            "HP TCP v1 supports CPU DRAM only" LOC_MARK);
    }

    uint64_t registration_id = 0;
    CHECK_STATUS(
        registry_.add(desc.addr, desc.length, options.perm, &registration_id));
    if (options.perm == kLocalReadWrite) return Status::OK();

    std::string encoded;
    Status status = EncodeHighPerformanceTcpBufferAttr(
        {registration_id, HighPerformanceTcpPermissionName(options.perm)},
        &encoded);
    if (!status.ok()) {
        (void)registry_.remove(desc.addr, desc.length);
        return status;
    }
    desc.transport_attrs[TransportType::HP_TCP] = std::move(encoded);
    if (!HasTransport(desc, TransportType::HP_TCP)) {
        desc.transports.push_back(TransportType::HP_TCP);
    }
    return Status::OK();
}

Status HighPerformanceTcpTransport::addMemoryBuffer(
    std::vector<BufferDesc>& desc_list, const MemoryOptions& options) {
    std::vector<size_t> created;
    try {
        created.reserve(desc_list.size());
    } catch (...) {
        return Status::InternalError(
            "unable to allocate HP TCP registration rollback state" LOC_MARK);
    }

    for (size_t i = 0; i < desc_list.size(); ++i) {
        const bool tracked_before =
            registry_.tracks(desc_list[i].addr, desc_list[i].length);
        Status status = addMemoryBuffer(desc_list[i], options);
        if (status.ok()) {
            if (!tracked_before) created.push_back(i);
            continue;
        }
        for (auto it = created.rbegin(); it != created.rend(); ++it) {
            const Status rollback = removeMemoryBuffer(desc_list[*it]);
            if (!rollback.ok()) {
                LOG(ERROR) << "HP TCP registration rollback failed: "
                           << rollback.ToString();
            }
        }
        return status;
    }
    return Status::OK();
}

Status HighPerformanceTcpTransport::removeMemoryBuffer(BufferDesc& desc) {
    if (!registry_.tracks(desc.addr, desc.length)) return Status::OK();
    Status status = registry_.remove(desc.addr, desc.length);
    if (!status.ok()) return status;
    desc.transport_attrs.erase(TransportType::HP_TCP);
    desc.transports.erase(
        std::remove(desc.transports.begin(), desc.transports.end(),
                    TransportType::HP_TCP),
        desc.transports.end());
    return Status::OK();
}

Status HighPerformanceTcpTransport::sendNotification(
    SegmentID target_id, const Notification& notification) {
    if (!metadata_) {
        return Status::InternalError("HP TCP metadata is unavailable" LOC_MARK);
    }
    return metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc* segment) {
            return ControlClient::notify(segment->rpc_server_addr,
                                         notification);
        });
}

Status HighPerformanceTcpTransport::receiveNotification(
    std::vector<Notification>& notifications) {
    RWSpinlock::WriteGuard guard(notify_lock_);
    notifications.clear();
    notifications.swap(notifications_);
    return Status::OK();
}

}  // namespace mooncake::tent
