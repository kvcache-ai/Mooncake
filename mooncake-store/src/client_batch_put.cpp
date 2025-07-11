#include "client_batch_put.h"

#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <optional>
#include <ranges>

#include "client.h"
#include "transfer_engine.h"
#include "transfer_task.h"
#include "types.h"

namespace mooncake {

// Create PutOp objects from keys and slices for batch processing
std::vector<PutOp> Client::makeOps(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    std::vector<PutOp> ops;
    ops.reserve(keys.size());
    // Pair each key with its corresponding slices
    for (size_t i = 0; i < keys.size(); ++i) {
        ops.emplace_back(keys[i], batched_slices[i]);
    }
    return ops;
}

// Stage 1: Initialize put operations and get replica assignments from master
void Client::stageStart(std::vector<PutOp>& ops,
                        const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;
    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());

    // Extract keys and slice sizes from operations for master request
    for (const auto& op : ops) {
        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }

        keys.emplace_back(op.key);
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    // Request replica assignments from master server
    auto start_responses =
        master_client_.BatchPutStart(keys, slice_lengths, config);

    // Validate response size matches request size
    if (start_responses.size() != ops.size()) {
        LOG(ERROR) << "BatchPutStart response size mismatch: expected "
                   << ops.size() << ", got " << start_responses.size();
        for (auto& op : ops) {
            op.result = tl::unexpected(ErrorCode::RPC_FAIL);
        }
        return;
    }

    // Process responses and assign replicas to operations
    for (size_t i = 0; i < ops.size(); ++i) {
        if (!start_responses[i]) {
            ops[i].result = tl::unexpected(start_responses[i].error());
        } else {
            ops[i].replicas = std::move(start_responses[i].value());
        }
    }
}

// Stage 2: Transfer data to replicas using the transfer engine
void Client::stageTransfer(std::vector<PutOp>& ops) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    // Filter operations that put start successfully
    std::vector<PutOp*> success_ops;
    success_ops.reserve(ops.size());
    for (auto& op : ops) {
        if (op.result.has_value()) {
            success_ops.emplace_back(&op);
        }
    }

    // Submit transfer tasks for each replica of successful operations
    for (auto& op : success_ops) {
        op->replica_futures.reserve(op->replicas.size());
        for (const auto& replica : op->replicas) {
            auto future = transfer_submitter_->submit(replica, op->slices,
                                                      TransferRequest::WRITE);
            op->replica_futures.emplace_back(
                future.has_value() ? tl::expected<TransferFuture, ErrorCode>(
                                         std::move(*future))
                                   : tl::unexpected(ErrorCode::TRANSFER_FAIL));
        }
    }

    // Wait for all transfer tasks to complete and collect results
    for (auto& op : success_ops) {
        for (auto& future : op->replica_futures) {
            if (!future.has_value()) {
                continue;
            }
            ErrorCode result = future->get();
            if (result != ErrorCode::OK) {
                future = tl::unexpected(result);
            }
        }
    }
}

// Stage 3: Finalize put operations by reporting results to master
void Client::stageEnd(std::vector<PutOp>& ops) {
    std::vector<std::string> keys;
    std::vector<std::vector<PutResult>> put_results;

    keys.reserve(ops.size());
    put_results.reserve(ops.size());

    // Collect transfer results for each operation
    for (const auto& op : ops) {
        keys.emplace_back(op.key);

        std::vector<PutResult> op_results;
        if (op.result.has_value()) {
            // Operation was successful so far, check individual replica results
            assert(op.replicas.size() == op.replica_futures.size());
            op_results.reserve(op.replica_futures.size());

            for (const auto& future : op.replica_futures) {
                op_results.emplace_back(future.has_value() ? PutResult::SUCCESS
                                                           : PutResult::FAILED);
            }
        } else {
            // Operation failed, mark all replicas as failed
            op_results.resize(op.replicas.size(), PutResult::FAILED);
        }
        put_results.emplace_back(std::move(op_results));
    }

    // Report results to master server for finalization
    if (!keys.empty()) {
        auto end_results = master_client_.BatchPutEnd(keys, put_results);

        // Update operation results based on server response
        for (size_t i = 0; i < ops.size() && i < end_results.size(); ++i) {
            if (!end_results[i].has_value()) {
                ops[i].result = tl::unexpected(end_results[i].error());
            }
        }
    }
}

// Collect final results from all operations, treating OBJECT_ALREADY_EXISTS as
// success
std::vector<tl::expected<void, ErrorCode>> Client::collect(
    const std::vector<PutOp>& ops) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(ops.size());
    for (const auto& op : ops) {
        // Treat OBJECT_ALREADY_EXISTS as a successful operation
        if (!op.result &&
            op.result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
            results.emplace_back();
        } else {
            results.emplace_back(op.result);
        }
    }
    return results;
}

// Main entry point for batch put operations - orchestrates the three-stage
// process
std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    // Execute the three-stage batch put process
    auto ops = makeOps(keys, batched_slices);  // Create operation objects
    stageStart(ops, config);  // Stage 1: Get replica assignments
    stageTransfer(ops);       // Stage 2: Transfer data to replicas
    stageEnd(ops);            // Stage 3: Finalize with master
    return collect(ops);      // Collect and return final results
}

}  // namespace mooncake
