// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_owner_read_worker.h"

namespace mooncake {
class FileStorage;
namespace ssd_offload {
// Owns FileStorage through queued/running worker calls, including source and
// arena drain. Stop the control server, then drain its endpoint before release.
SsdOwnerReadWorker::Reader MakeFileStorageOwnerReader(
    std::shared_ptr<FileStorage>);
}  // namespace ssd_offload
}  // namespace mooncake
