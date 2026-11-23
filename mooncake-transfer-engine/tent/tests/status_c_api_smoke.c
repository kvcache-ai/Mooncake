// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "tent/transfer_engine.h"

/* Compile the consumer calls as C, and link the installed shared-library ABI.
 */
int status_c_open(tent_engine_t engine, tent_segment_id_t* segment,
                  const char* name) {
    return tent_open_segment(engine, segment, name);
}
int status_c_info(tent_engine_t engine, tent_segment_id_t segment,
                  tent_segment_info_t* info) {
    return tent_get_segment_info(engine, segment, info);
}
int status_c_submit(tent_engine_t engine, tent_batch_id_t batch,
                    tent_request_t* requests, size_t count, int notify) {
    if (notify)
        return tent_submit_notif(engine, batch, requests, count, "test",
                                 "test");
    return tent_submit(engine, batch, requests, count);
}
