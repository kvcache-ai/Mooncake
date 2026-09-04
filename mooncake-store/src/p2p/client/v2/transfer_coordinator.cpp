#include "p2p/client/v2/transfer_coordinator.h"

#include <algorithm>
#include <thread>
#include <unordered_map>
#include <utility>

#include <async_simple/Promise.h>
#include <async_simple/Try.h>
#include <glog/logging.h>
#include <ylt/coro_io/coro_io.hpp>

#include "transport/transport.h"

namespace mooncake::v2 {
namespace {

constexpr int64_t kWaitTimeoutSeconds = 10;
constexpr int64_t kDrainTimeoutSeconds = 10;
constexpr auto kPollInterval = std::chrono::microseconds(100);

async_simple::Future<tl::expected<void, ErrorCode>> MakeReadyFuture(
    tl::expected<void, ErrorCode> value) {
    async_simple::Promise<tl::expected<void, ErrorCode>> promise;
    auto future = promise.getFuture();
    promise.setValue(std::move(value));
    return future;
}

/**
 * @brief Start `lazy` on `executor` and complete `promise` from it, running
 *        `on_complete` exactly once whichever way it ends.
 *
 * The in-flight bookkeeping depends on that "exactly once", including when
 * start() itself throws, which is why the completion hook is not simply put at
 * the end of the coroutine.
 */
async_simple::Future<tl::expected<void, ErrorCode>> LaunchOnExecutor(
    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> lazy,
    async_simple::Executor* executor, std::function<void()> on_complete) {
    auto promise = std::make_shared<
        async_simple::Promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->getFuture();
    auto complete =
        std::make_shared<std::function<void()>>(std::move(on_complete));
    auto finish = [promise, complete](
                      async_simple::Try<tl::expected<void, ErrorCode>>&& t) {
        try {
            promise->setValue(t.value());
        } catch (...) {
            try {
                promise->setException(std::current_exception());
            } catch (...) {
            }
        }
        (*complete)();
    };
    try {
        std::move(lazy).via(executor).start(std::move(finish));
    } catch (...) {
        try {
            promise->setException(std::current_exception());
        } catch (...) {
        }
        (*complete)();
    }
    return future;
}

}  // namespace

TransferCoordinator::TransferCoordinator(
    std::shared_ptr<TransferEngine> transfer_engine,
    std::shared_ptr<coro_io::io_context_pool> wait_pool)
    : transfer_engine_(std::move(transfer_engine)),
      wait_pool_(std::move(wait_pool)) {}

TransferCoordinator::~TransferCoordinator() { Stop(); }

void TransferCoordinator::Stop() {
    std::unique_lock<std::mutex> lock(wait_mu_);
    stopped_.store(true, std::memory_order_release);
    // In-flight waits must finish before the pool they run on can be stopped,
    // or a promise is destroyed without ever being completed and its awaiter
    // hangs forever.
    wait_cv_.wait(lock, [this] { return wait_inflight_ == 0; });
}

void TransferCoordinator::ReleaseWaitInflight() {
    std::lock_guard<std::mutex> lock(wait_mu_);
    --wait_inflight_;
    if (wait_inflight_ == 0) wait_cv_.notify_all();
}

tl::expected<void, ErrorCode> TransferCoordinator::ValidateRemoteBuffers(
    const std::vector<RemoteBufferDesc>& buffers) {
    if (buffers.empty()) {
        LOG(ERROR) << "No peer buffers supplied";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (const auto& buffer : buffers) {
        if (buffer.segment_endpoint.empty()) {
            LOG(ERROR) << "Peer buffer has an empty segment endpoint";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.addr == 0) {
            LOG(ERROR) << "Peer buffer has a null address";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.size == 0) {
            LOG(ERROR) << "Peer buffer has a zero size";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    return {};
}

tl::expected<Transport::BatchID, ErrorCode> TransferCoordinator::SubmitRequests(
    const std::string& endpoint,
    const std::vector<Transport::TransferRequest>& requests) {
    Transport::BatchID batch_id =
        transfer_engine_->allocateBatchID(requests.size());
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate a batch id for " << endpoint;
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    Status status = transfer_engine_->submitTransfer(batch_id, requests);
    if (!status.ok()) {
        LOG(ERROR) << "submitTransfer failed for " << endpoint << ": "
                   << status.message();
        // Nothing reached the hardware, so the tasks stay WAITING forever and
        // the drain-then-free dance cannot help; free directly.
        auto freed = transfer_engine_->freeBatchID(batch_id);
        if (!freed.ok()) {
            LOG(WARNING) << "freeBatchID after a failed submit also failed, "
                            "the descriptor may leak: "
                         << freed.message();
        }
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    return batch_id;
}

tl::expected<std::vector<TransferCoordinator::Batch>, ErrorCode>
TransferCoordinator::Submit(void* local_base, size_t total_size,
                            const std::vector<RemoteBufferDesc>& peers,
                            Transport::TransferRequest::OpCode opcode) {
    if (local_base == nullptr || total_size == 0) {
        LOG(ERROR) << "Transfer needs a non-empty local buffer";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // One batch per peer segment: batches are per-endpoint in the engine, and
    // grouping here keeps the failure blast radius to a single peer.
    std::unordered_map<std::string, std::vector<size_t>> by_segment;
    for (size_t i = 0; i < peers.size(); ++i) {
        by_segment[peers[i].segment_endpoint].push_back(i);
    }
    std::vector<size_t> offsets(peers.size());
    size_t running = 0;
    for (size_t i = 0; i < peers.size(); ++i) {
        offsets[i] = running;
        running += peers[i].size;
    }

    std::vector<Batch> submitted;
    for (const auto& [endpoint, indices] : by_segment) {
        Transport::SegmentHandle segment =
            transfer_engine_->openSegment(endpoint);
        if (segment == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment '" << endpoint << "'";
            for (const auto& [id, tasks, ep] : submitted) {
                CancelBatch(id, tasks);
            }
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        std::vector<Transport::TransferRequest> requests;
        requests.reserve(indices.size());
        for (size_t index : indices) {
            const auto& peer = peers[index];
            const size_t offset = offsets[index];
            if (offset >= total_size) continue;
            const size_t length = std::min(peer.size, total_size - offset);
            if (length == 0) continue;

            Transport::TransferRequest request;
            request.opcode = opcode;
            request.source = static_cast<char*>(local_base) + offset;
            request.target_id = segment;
            request.target_offset = peer.addr;
            request.length = length;
            requests.push_back(request);
        }
        if (requests.empty()) continue;

        auto batch = SubmitRequests(endpoint, requests);
        if (!batch) {
            for (const auto& [id, tasks, ep] : submitted) {
                CancelBatch(id, tasks);
            }
            return tl::make_unexpected(batch.error());
        }
        submitted.emplace_back(batch.value(), requests.size(), endpoint);
    }

    if (submitted.empty()) {
        LOG(ERROR) << "No transfer batch could be built from the peer buffers";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    return submitted;
}

TransferCoordinator::PollResult TransferCoordinator::PollOnce(
    Transport::BatchID batch_id, size_t num_tasks, const std::string& endpoint,
    std::chrono::steady_clock::time_point start) {
    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
    if (elapsed >= kWaitTimeoutSeconds) {
        LOG(ERROR) << "Transfer batch " << batch_id << " to '" << endpoint
                   << "' timed out after " << elapsed << "s";
        return PollResult::kFailed;
    }

    bool all_completed = true;
    for (size_t i = 0; i < num_tasks; ++i) {
        TransferStatus status;
        Status s = transfer_engine_->getTransferStatus(batch_id, i, status);
        if (!s.ok()) {
            LOG(ERROR) << "getTransferStatus failed for task " << i
                       << " of batch " << batch_id << " to '" << endpoint
                       << "': " << s.message();
            return PollResult::kFailed;
        }
        if (status.s == TransferStatusEnum::COMPLETED) continue;
        if (status.s == TransferStatusEnum::FAILED ||
            status.s == TransferStatusEnum::CANCELED ||
            status.s == TransferStatusEnum::INVALID ||
            status.s == TransferStatusEnum::TIMEOUT) {
            LOG(ERROR) << "Transfer task " << i << " of batch " << batch_id
                       << " to '" << endpoint << "' ended in status "
                       << static_cast<int>(status.s);
            return PollResult::kFailed;
        }
        all_completed = false;
    }
    return all_completed ? PollResult::kCompleted : PollResult::kPending;
}

bool TransferCoordinator::IsDrained(Transport::BatchID batch_id,
                                    size_t num_tasks) {
    for (size_t i = 0; i < num_tasks; ++i) {
        TransferStatus status;
        Status s = transfer_engine_->getTransferStatus(batch_id, i, status);
        if (!s.ok() || (status.s != TransferStatusEnum::COMPLETED &&
                        status.s != TransferStatusEnum::FAILED)) {
            return false;
        }
    }
    return true;
}

void TransferCoordinator::CancelBatch(Transport::BatchID batch_id,
                                      size_t num_tasks) {
    // There is no cancel primitive, and freeBatchID only releases the
    // descriptor once every task is finished. So "cancel" is: poll until the
    // batch is terminal, then free. The timeout bounds shutdown at the cost of
    // knowingly leaking one descriptor.
    const auto start = std::chrono::steady_clock::now();
    while (!IsDrained(batch_id, num_tasks)) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
        if (elapsed >= kDrainTimeoutSeconds) {
            LOG(WARNING) << "Giving up draining batch " << batch_id << " after "
                         << elapsed << "s; its descriptor may leak";
            return;
        }
        std::this_thread::sleep_for(kPollInterval);
    }
    auto freed = transfer_engine_->freeBatchID(batch_id);
    if (!freed.ok()) {
        LOG(WARNING) << "freeBatchID failed for a drained batch: "
                     << freed.message();
    }
}

async_simple::coro::Lazy<void> TransferCoordinator::CancelBatchCoro(
    Transport::BatchID batch_id, size_t num_tasks) {
    const auto start = std::chrono::steady_clock::now();
    while (!IsDrained(batch_id, num_tasks)) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
        if (elapsed >= kDrainTimeoutSeconds) {
            LOG(WARNING) << "Giving up draining batch " << batch_id << " after "
                         << elapsed << "s; its descriptor may leak";
            co_return;
        }
        (void)co_await coro_io::sleep_for(kPollInterval);
    }
    auto freed = transfer_engine_->freeBatchID(batch_id);
    if (!freed.ok()) {
        LOG(WARNING) << "freeBatchID failed for a drained batch: "
                     << freed.message();
    }
    co_return;
}

async_simple::coro::Lazy<void> TransferCoordinator::CancelRemainingCoro(
    const std::vector<Batch>& batches, size_t from_index) {
    for (size_t i = from_index; i < batches.size(); ++i) {
        co_await CancelBatchCoro(std::get<0>(batches[i]),
                                 std::get<1>(batches[i]));
    }
    co_return;
}

tl::expected<void, ErrorCode> TransferCoordinator::WaitBatch(
    Transport::BatchID batch_id, size_t num_tasks,
    const std::string& endpoint) {
    const auto start = std::chrono::steady_clock::now();
    for (;;) {
        const PollResult poll = PollOnce(batch_id, num_tasks, endpoint, start);
        if (poll == PollResult::kFailed) {
            CancelBatch(batch_id, num_tasks);
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }
        if (poll == PollResult::kCompleted) {
            transfer_engine_->freeBatchID(batch_id);
            return {};
        }
        std::this_thread::sleep_for(kPollInterval);
    }
}

tl::expected<void, ErrorCode> TransferCoordinator::WaitAll(
    const std::vector<Batch>& batches) {
    for (size_t i = 0; i < batches.size(); ++i) {
        auto waited =
            WaitBatch(std::get<0>(batches[i]), std::get<1>(batches[i]),
                      std::get<2>(batches[i]));
        if (!waited) {
            // Partial success is not success: everything still outstanding is
            // cancelled before the caller is told the transfer failed.
            for (size_t j = i + 1; j < batches.size(); ++j) {
                CancelBatch(std::get<0>(batches[j]), std::get<1>(batches[j]));
            }
            return waited;
        }
    }
    return {};
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
TransferCoordinator::WaitBatchCoro(Transport::BatchID batch_id,
                                   size_t num_tasks, std::string endpoint) {
    const auto start = std::chrono::steady_clock::now();
    for (;;) {
        if (stopped_.load(std::memory_order_acquire)) {
            LOG(WARNING) << "Transfer wait for batch " << batch_id
                         << " cancelled by shutdown";
            co_await CancelBatchCoro(batch_id, num_tasks);
            co_return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
        }
        const PollResult poll = PollOnce(batch_id, num_tasks, endpoint, start);
        if (poll == PollResult::kFailed) {
            co_await CancelBatchCoro(batch_id, num_tasks);
            co_return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }
        if (poll == PollResult::kCompleted) {
            transfer_engine_->freeBatchID(batch_id);
            co_return tl::expected<void, ErrorCode>{};
        }
        (void)co_await coro_io::sleep_for(kPollInterval);
    }
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
TransferCoordinator::WaitAllCoro(std::vector<Batch> batches) {
    for (size_t i = 0; i < batches.size(); ++i) {
        auto waited = co_await WaitBatchCoro(std::get<0>(batches[i]),
                                             std::get<1>(batches[i]),
                                             std::get<2>(batches[i]));
        if (!waited) {
            co_await CancelRemainingCoro(batches, i + 1);
            co_return waited;
        }
    }
    co_return tl::expected<void, ErrorCode>{};
}

tl::expected<void, ErrorCode> TransferCoordinator::Transfer(
    void* local_base, size_t total_size,
    const std::vector<RemoteBufferDesc>& peers,
    Transport::TransferRequest::OpCode opcode) {
    auto validated = ValidateRemoteBuffers(peers);
    if (!validated) return validated;

    size_t peer_total = 0;
    for (const auto& peer : peers) peer_total += peer.size;
    if (peer_total != total_size) {
        LOG(ERROR) << "Peer buffers cover " << peer_total << " bytes but the "
                   << "local buffer is " << total_size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!transfer_engine_->getMetadata()) {
        LOG(ERROR) << "TransferEngine is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // The synchronous path has to be accounted for too. It used to submit and
    // poll without ever touching wait_inflight_ or stopped_, so Stop() returned
    // while a poll loop was still running and ~TransferCoordinator could then
    // destroy the engine underneath it. That path is no longer only a fallback:
    // the TransferEngine copier calls Transfer directly on every local copy.
    {
        std::lock_guard<std::mutex> lock(wait_mu_);
        if (stopped_.load(std::memory_order_acquire)) {
            return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
        }
        ++wait_inflight_;
    }
    // RAII rather than a release on each return: Submit and WaitAll between
    // them have several early exits, and one missed release wedges Stop()
    // forever on its condition variable.
    struct InflightGuard {
        TransferCoordinator* owner;
        ~InflightGuard() { owner->ReleaseWaitInflight(); }
    } guard{this};

    auto batches = Submit(local_base, total_size, peers, opcode);
    if (!batches) return tl::make_unexpected(batches.error());
    return WaitAll(batches.value());
}

async_simple::Future<tl::expected<void, ErrorCode>>
TransferCoordinator::TransferAsync(void* local_base, size_t total_size,
                                   const std::vector<RemoteBufferDesc>& peers,
                                   Transport::TransferRequest::OpCode opcode) {
    if (wait_pool_ == nullptr) {
        return MakeReadyFuture(Transfer(local_base, total_size, peers, opcode));
    }

    auto validated = ValidateRemoteBuffers(peers);
    if (!validated) return MakeReadyFuture(validated);

    size_t peer_total = 0;
    for (const auto& peer : peers) peer_total += peer.size;
    if (peer_total != total_size) {
        LOG(ERROR) << "Peer buffers cover " << peer_total << " bytes but the "
                   << "local buffer is " << total_size;
        return MakeReadyFuture(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (!transfer_engine_->getMetadata()) {
        LOG(ERROR) << "TransferEngine is not initialized";
        return MakeReadyFuture(tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
    }

    async_simple::Executor* executor = nullptr;
    {
        std::lock_guard<std::mutex> lock(wait_mu_);
        if (stopped_.load(std::memory_order_acquire)) {
            return MakeReadyFuture(
                tl::make_unexpected(ErrorCode::SHUTTING_DOWN));
        }
        // Counted before the launch so Stop() cannot decide the coordinator is
        // idle while this wait is still being set up.
        ++wait_inflight_;
        executor = wait_pool_->get_executor();
    }

    auto batches = Submit(local_base, total_size, peers, opcode);
    if (!batches) {
        ReleaseWaitInflight();
        return MakeReadyFuture(tl::make_unexpected(batches.error()));
    }
    return LaunchOnExecutor(WaitAllCoro(std::move(batches.value())), executor,
                            [this] { ReleaseWaitInflight(); });
}

}  // namespace mooncake::v2
