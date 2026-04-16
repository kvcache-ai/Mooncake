#include "transfer_engine.h"
#include "transfer_engine_c.h"

extern "C" int mooncake_classic_republish_local_metadata(
    transfer_engine_t engine) {
    if (engine == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return -1;
    }

    auto local_desc = metadata->getSegmentDescByID(mooncake::LOCAL_SEGMENT_ID);
    if (!local_desc) {
        return -1;
    }

    auto rpc_desc = metadata->localRpcMeta();
    int rc = metadata->addRpcMetaEntry(local_desc->name, rpc_desc);
    if (rc != 0) {
        return rc;
    }
    return metadata->updateLocalSegmentDesc();
}

extern "C" int mooncake_classic_get_batch_transfer_status(
    transfer_engine_t engine, batch_id_t batch_id,
    struct transfer_status *status) {
    if (engine == nullptr || status == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    mooncake::TransferStatus native_status;
    auto result =
        native->getBatchTransferStatus((mooncake::BatchID)batch_id, native_status);
    if (!result.ok()) {
        return static_cast<int>(result.code());
    }

    status->status = static_cast<int>(native_status.s);
    status->transferred_bytes = native_status.transferred_bytes;
    return 0;
}

extern "C" int mooncake_classic_get_segment_first_buffer(
    transfer_engine_t engine, segment_handle_t segment_id, uint64_t *addr_out,
    uint64_t *length_out) {
    if (engine == nullptr || addr_out == nullptr || length_out == nullptr) {
        return -1;
    }

    auto *native = reinterpret_cast<mooncake::TransferEngine *>(engine);
    auto metadata = native->getMetadata();
    if (!metadata) {
        return -1;
    }

    auto desc = metadata->getSegmentDescByID(segment_id);
    if (!desc || desc->buffers.empty()) {
        return -1;
    }

    *addr_out = desc->buffers[0].addr;
    *length_out = desc->buffers[0].length;
    return 0;
}
