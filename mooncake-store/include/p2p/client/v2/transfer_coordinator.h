#pragma once

// TransferCoordinator: node-to-node movement over the TransferEngine.
//
// It owns the batch lifecycle -- open segment, submit, poll to a terminal
// state, free -- and the two rules that make partial failure safe:
//
//   1. A transfer succeeds only if every batch succeeded. The first failure
//      cancels the batches that have not been waited on yet, so no request
//      keeps running against a caller that has already been told it failed.
//   2. A batch id is only freed once every task in it has reached a terminal
//      state. The TransferEngine has no cancel primitive and freeBatchID is a
//      no-op until then, so "cancel" means drain-then-free, with a timeout
//      after which the descriptor is knowingly leaked rather than blocking
//      shutdown forever.
//
// Waiting is offloaded to a dedicated pool when one is configured, so a poll
// loop never occupies an RPC thread.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include <async_simple/Executor.h>
#include <async_simple/Future.h>
#include <async_simple/coro/Lazy.h>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/client/client_rpc_types.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @class TransferCoordinator
 */
class TransferCoordinator {
   public:
    TransferCoordinator(std::shared_ptr<TransferEngine> transfer_engine,
                        std::shared_ptr<coro_io::io_context_pool> wait_pool);
    ~TransferCoordinator();

    TransferCoordinator(const TransferCoordinator&) = delete;
    TransferCoordinator& operator=(const TransferCoordinator&) = delete;

    /** Endpoint, address and size must all be present and non-zero. */
    static tl::expected<void, ErrorCode> ValidateRemoteBuffers(
        const std::vector<RemoteBufferDesc>& buffers);

    /**
     * @brief Submit and wait on the calling thread.
     * @param opcode WRITE: local -> peers; READ: peers -> local.
     */
    tl::expected<void, ErrorCode> Transfer(
        void* local_base, size_t total_size,
        const std::vector<RemoteBufferDesc>& peers,
        Transport::TransferRequest::OpCode opcode);

    /**
     * @brief Submit here, wait on the wait pool when one is configured.
     *
     * The returned Future always completes, including after Stop(): a caller
     * blocked on it must never be left hanging by a shutdown.
     */
    async_simple::Future<tl::expected<void, ErrorCode>> TransferAsync(
        void* local_base, size_t total_size,
        const std::vector<RemoteBufferDesc>& peers,
        Transport::TransferRequest::OpCode opcode);

    /** Reject new transfers and wait for the in-flight ones to finish. */
    void Stop();

   private:
    // (batch id, task count, segment endpoint)
    using Batch = std::tuple<Transport::BatchID, size_t, std::string>;

    tl::expected<std::vector<Batch>, ErrorCode> Submit(
        void* local_base, size_t total_size,
        const std::vector<RemoteBufferDesc>& peers,
        Transport::TransferRequest::OpCode opcode);

    tl::expected<Transport::BatchID, ErrorCode> SubmitRequests(
        const std::string& endpoint,
        const std::vector<Transport::TransferRequest>& requests);

    enum class PollResult { kPending, kCompleted, kFailed };

    PollResult PollOnce(Transport::BatchID batch_id, size_t num_tasks,
                        const std::string& endpoint,
                        std::chrono::steady_clock::time_point start);

    tl::expected<void, ErrorCode> WaitBatch(Transport::BatchID batch_id,
                                            size_t num_tasks,
                                            const std::string& endpoint);
    tl::expected<void, ErrorCode> WaitAll(const std::vector<Batch>& batches);

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> WaitBatchCoro(
        Transport::BatchID batch_id, size_t num_tasks, std::string endpoint);
    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> WaitAllCoro(
        std::vector<Batch> batches);

    bool IsDrained(Transport::BatchID batch_id, size_t num_tasks);
    void CancelBatch(Transport::BatchID batch_id, size_t num_tasks);
    async_simple::coro::Lazy<void> CancelBatchCoro(Transport::BatchID batch_id,
                                                   size_t num_tasks);
    async_simple::coro::Lazy<void> CancelRemainingCoro(
        const std::vector<Batch>& batches, size_t from_index);

    void ReleaseWaitInflight();

    std::shared_ptr<TransferEngine> transfer_engine_;
    std::shared_ptr<coro_io::io_context_pool> wait_pool_;

    std::mutex wait_mu_;
    std::condition_variable wait_cv_;
    std::atomic<bool> stopped_{false};
    size_t wait_inflight_ = 0;  // guarded by wait_mu_
};

}  // namespace mooncake::v2
