// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef MOONCAKE_PXN_MEMORY_MANAGER_H_
#define MOONCAKE_PXN_MEMORY_MANAGER_H_

#include <glog/logging.h>

#include <memory>
#include <string>
#include "common.h"

namespace mooncake {
namespace pxn {

int PxnEnsureGpuMemory(void *start, bool &isGpuMem, uint32_t &gpuidx);
}
}  // namespace mooncake

#endif
