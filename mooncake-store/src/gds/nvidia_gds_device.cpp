// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include "gds/gds_device_ops.h"

#ifdef USE_GDS_NVIDIA

#include <cufile.h>
#include <cuda_runtime.h>
#include <unistd.h>
#include <sys/stat.h>

namespace mooncake {
namespace {

class NvidiaGdsDeviceOps final : public GdsDeviceOps {
   public:
    bool ProbeDeviceNode() override {
        struct stat st;
        return (::stat("/dev/nvidia-fs", &st) == 0 && S_ISCHR(st.st_mode)) ||
               (::stat("/dev/nvidia-fs0", &st) == 0 && S_ISCHR(st.st_mode));
    }

    GdsDeviceError DriverOpen() override {
        CUfileError_t r = cuFileDriverOpen();
        return GdsDeviceError{static_cast<int>(r.err)};
    }

    GdsDeviceError FileHandleRegister(GdsDeviceFileHandle* out,
                                      int fd) override {
        CUfileDescr_t desc{};
        desc.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
        desc.handle.fd = fd;
        CUfileError_t r =
            cuFileHandleRegister(reinterpret_cast<CUfileHandle_t*>(out), &desc);
        return GdsDeviceError{static_cast<int>(r.err)};
    }

    void FileHandleDeregister(GdsDeviceFileHandle handle) override {
        if (handle) cuFileHandleDeregister(static_cast<CUfileHandle_t>(handle));
    }

    GdsDeviceError BufRegister(void* ptr, size_t size) override {
        CUfileError_t r = cuFileBufRegister(ptr, size, 0);
        return GdsDeviceError{static_cast<int>(r.err)};
    }

    void BufDeregister(void* ptr) override {
        if (ptr) cuFileBufDeregister(ptr);
    }

    ssize_t Write(GdsDeviceFileHandle fh, void* buf, size_t size,
                  off_t file_offset) override {
        return cuFileWrite(static_cast<CUfileHandle_t>(fh), buf, size,
                           file_offset, 0);
    }

    ssize_t Read(GdsDeviceFileHandle fh, void* buf, size_t size,
                 off_t file_offset) override {
        return cuFileRead(static_cast<CUfileHandle_t>(fh), buf, size,
                          file_offset, 0);
    }

    void* Malloc(size_t size) override {
        void* ptr = nullptr;
        return (cudaMalloc(&ptr, size) == cudaSuccess) ? ptr : nullptr;
    }

    void Free(void* ptr) override {
        if (ptr) cudaFree(ptr);
    }

    void Memset(void* ptr, int value, size_t size) override {
        cudaMemset(ptr, value, size);
    }

    void SetDevice(int device_id) override { cudaSetDevice(device_id); }
    void DeviceSynchronize() override { cudaDeviceSynchronize(); }

    int GetDevice() override {
        int dev = -1;
        cudaGetDevice(&dev);
        return dev;
    }

    void CopyDeviceToDevice(void* dst, const void* src, size_t size) override {
        cudaMemcpy(dst, src, size, cudaMemcpyDeviceToDevice);
    }
};

}  // namespace

std::unique_ptr<GdsDeviceOps> CreateNvidiaGdsDeviceOps() {
    return std::make_unique<NvidiaGdsDeviceOps>();
}

}  // namespace mooncake
#endif  // USE_GDS_NVIDIA
