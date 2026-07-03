// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include "gds/gds_device_ops.h"

#ifdef USE_GDS_ASCEND

namespace mooncake {
namespace {

class AscendGdsDeviceOps final : public GdsDeviceOps {
   public:
    bool ProbeDeviceNode() override { return false; }
    GdsDeviceError DriverOpen() override { return GdsDeviceError{-1}; }
    GdsDeviceError FileHandleRegister(GdsDeviceFileHandle*, int) override {
        return GdsDeviceError{-1};
    }
    void FileHandleDeregister(GdsDeviceFileHandle) override {}
    GdsDeviceError BufRegister(void*, size_t) override {
        return GdsDeviceError{-1};
    }
    void BufDeregister(void*) override {}
    ssize_t Write(GdsDeviceFileHandle, void*, size_t, off_t) override {
        return -1;
    }
    ssize_t Read(GdsDeviceFileHandle, void*, size_t, off_t) override {
        return -1;
    }
    void* Malloc(size_t) override { return nullptr; }
    void Free(void*) override {}
    void Memset(void*, int, size_t) override {}
    void DeviceSynchronize() override {}
    int GetDevice() override { return -1; }
    void CopyDeviceToDevice(void*, const void*, size_t) override {}
};

}  // namespace

std::unique_ptr<GdsDeviceOps> CreateAscendGdsDeviceOps() {
    return std::make_unique<AscendGdsDeviceOps>();
}

}  // namespace mooncake
#endif  // USE_GDS_ASCEND
