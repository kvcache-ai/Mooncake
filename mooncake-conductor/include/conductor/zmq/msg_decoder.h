#pragma once

// Lenient msgpack decoding for the two publisher schemas:
//  - vLLM topic:  3-element batch [timestamp, events, dp_rank]
//  - mooncake:    2-element batch [timestamp, events]
//
// Field values are read through msgpack::object type checks (never rigid
// MSGPACK_DEFINE structs) because the Python publisher picks integer
// widths dynamically and emits nil-able fields.

#include <cstddef>
#include <string>

#include "conductor/zmq/event_type.h"

namespace conductor {
namespace zmq {

struct EventBatchResult {
    bool ok = false;
    EventBatch batch;
    std::string error;  // set when !ok; human-readable error string
};

EventBatchResult DecodeVllmEventBatch(const char* data, size_t len);
EventBatchResult DecodeMooncakeEventBatch(const char* data, size_t len);

}  // namespace zmq
}  // namespace conductor
