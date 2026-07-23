// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include "gds/gds_device_ops.h"
#include <glog/logging.h>

// Each vendor .cpp provides its own factory function when compiled
// with the corresponding SDK flag. All declared here; exactly one
// (fallback) is always available.
namespace mooncake {
extern std::unique_ptr<GdsDeviceOps> CreateNvidiaGdsDeviceOps();
extern std::unique_ptr<GdsDeviceOps> CreateHygonGdsDeviceOps();
extern std::unique_ptr<GdsDeviceOps> CreateAscendGdsDeviceOps();
extern std::unique_ptr<GdsDeviceOps> CreateMooreThreadsGdsDeviceOps();
extern std::unique_ptr<GdsDeviceOps> CreateFallbackGdsDeviceOps();
}  // namespace mooncake

std::unique_ptr<mooncake::GdsDeviceOps> mooncake::CreateGdsDeviceOps() {
#ifdef USE_GDS_NVIDIA
    auto ops = CreateNvidiaGdsDeviceOps();
    if (ops->ProbeDeviceNode()) {
        return ops;
    }
#endif

#ifdef USE_GDS_HYGON
    auto ops = CreateHygonGdsDeviceOps();
    if (ops->ProbeDeviceNode()) {
        return ops;
    }
#endif

#ifdef USE_GDS_ASCEND
    auto ops = CreateAscendGdsDeviceOps();
    if (ops->ProbeDeviceNode()) {
        return ops;
    }
#endif

#ifdef USE_GDS_MOORE_THREADS
    auto ops = CreateMooreThreadsGdsDeviceOps();
    if (ops->ProbeDeviceNode()) {
        return ops;
    }
#endif

    LOG(INFO) << "GDS: no hardware found, using fallback (GDS disabled)";
    return CreateFallbackGdsDeviceOps();
}
