#pragma once

#include <glog/logging.h>

#include <span>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "transfer_task.h"
#include "types.h"

namespace mooncake {

class Client;

// Calculate total size of all slices in the collection
[[nodiscard]] size_t CalculateSliceSize(const std::vector<Slice>& slices);
[[nodiscard]] size_t CalculateSliceSize(std::span<const Slice> slices);

// Represents a single put operation in a batch
struct PutOp {
    explicit PutOp(std::string key, std::vector<Slice> slices)
        : key(std::move(key)), slices(std::move(slices)) {}

    std::string key;            // Object key to store
    std::vector<Slice> slices;  // Data slices to be stored
    std::vector<Replica::Descriptor>
        replicas;  // Replica locations (filled by stage-1, put start)
    std::vector<tl::expected<TransferFuture, ErrorCode>>
        replica_futures;                   // Transfer futures for each replica
    tl::expected<void, ErrorCode> result;  // Final operation result
};

}  // namespace mooncake
