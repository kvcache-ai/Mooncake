// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_file_storage_owner_reader.h"

#include "file_storage.h"
#include <glog/logging.h>

namespace mooncake::ssd_offload {
SsdOwnerCallbacks::Reader MakeFileStorageOwnerReader(
    std::shared_ptr<FileStorage> storage) {
    return [storage = std::move(storage)](
               const SsdReadRequestV1& request,
               std::shared_ptr<SourceDrainedWriteTarget> target,
               std::chrono::steady_clock::time_point deadline)
               -> std::optional<SsdReadResultV1> {
        if (!storage) return {};
        auto read =
            storage->ReadOwnerStreaming(request, std::move(target), deadline);
        // ReadOwnerStreaming error returns are complete preflight failures.
        // Post-submission or direct_io failures return a shaped result with
        // explicit drain status.
        if (!read) {
            VLOG(1) << "SSD owner streaming read preflight rejected request="
                    << request.attempt.request_id
                    << " error=" << toString(read.error());
            return {};
        }
        return std::move(*read);
    };
}
}  // namespace mooncake::ssd_offload
