// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <memory>

#include "ssd_owner_callbacks.h"

namespace mooncake {
class FileStorage;
namespace ssd_offload {
// Owns FileStorage through queued/running owner calls, including source and
// arena drain. Stop the control server, then drain its endpoint before release.
SsdOwnerCallbacks::Reader MakeFileStorageOwnerReader(
    std::shared_ptr<FileStorage> storage);
}  // namespace ssd_offload
}  // namespace mooncake
