#pragma once

// Strict envelope and recognized-field decoding for the independent vLLM and
// Mooncake map protocols. Event-local failures are materialized in the batch
// so valid siblings can still be dispatched in source order.

#include <cstddef>
#include <string>

#include "conductor/zmq/event_type.h"

namespace conductor {
namespace zmq {

template <typename Batch>
struct BatchDecodeResult {
    bool ok = false;
    Batch batch;
    std::string error;
};

using VllmEventBatchResult = BatchDecodeResult<VllmEventBatch>;
using MooncakeEventBatchResult = BatchDecodeResult<MooncakeEventBatch>;

VllmEventBatchResult DecodeVllmEventBatch(const char* data, size_t len);
MooncakeEventBatchResult DecodeMooncakeEventBatch(const char* data, size_t len);

}  // namespace zmq
}  // namespace conductor
