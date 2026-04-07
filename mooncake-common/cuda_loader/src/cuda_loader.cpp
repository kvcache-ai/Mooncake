// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cuda_loader/cuda_loader.h"
#include "cudart_symbols.h"
#include "cuda_driver_symbols.h"
#ifdef USE_GDS
#include "cufile_symbols.h"
#endif

#include <dlfcn.h>
#include <glog/logging.h>

namespace cuda_loader {

// Try dlopen with a list of candidate library names, return first success.
template <size_t N>
static void* TryDlopen(const char* const (&candidates)[N]) {
    for (auto name : candidates) {
        void* h = dlopen(name, RTLD_NOW | RTLD_LOCAL);
        if (h) {
            LOG(INFO) << "cuda_loader: dlopening " << name;
            return h;
        }
    }
    return nullptr;
}

template <typename Fn>
static bool LoadSymbol(void* handle, const char* const name, Fn& out) {
    void* sym = dlsym(handle, name);
    if (!sym) {
        LOG(WARNING) << "cuda_loader: missing symbol: " << name
                     << ", error: " << dlerror();
        return false;
    }
    out = reinterpret_cast<Fn>(sym);
    return true;
}

class CudaLoader {
   public:
    static CudaLoader& Instance() {
        static CudaLoader instance;
        return instance;
    }

    bool cudart_ok() const { return cudart_ok_; }
    bool driver_ok() const { return driver_ok_; }
    bool cufile_ok() const {
#ifdef USE_GDS
        return cufile_ok_;
#else
        return false;
#endif
    }

    CudaRtSymbols& rt() { return rt_; }
    CudaDriverSymbols& drv() { return drv_; }
#ifdef USE_GDS
    CuFileSymbols& cf() { return cf_; }
#endif

   private:
    CudaLoader() {
        loadCudaRt();
        loadCudaDriver();
#ifdef USE_GDS
        loadCuFile();
#endif
    }

    ~CudaLoader() {
#ifdef USE_GDS
        if (cufile_handle_) {
            dlclose(cufile_handle_);
            cufile_handle_ = nullptr;
        }
#endif
        if (driver_handle_) {
            dlclose(driver_handle_);
            driver_handle_ = nullptr;
        }
        if (cudart_handle_) {
            dlclose(cudart_handle_);
            cudart_handle_ = nullptr;
        }
    }

    CudaLoader(const CudaLoader&) = delete;
    CudaLoader& operator=(const CudaLoader&) = delete;

    void loadCudaRt() {
        static const char* const kCudaRtNames[] = {
            "libcudart.so",
            "libcudart.so.13",
            "libcudart.so.12",
            "libcudart.so.11",
        };
        cudart_handle_ = TryDlopen(kCudaRtNames);
        if (!cudart_handle_) {
            LOG(INFO) << "cuda_loader: libcudart.so not available: "
                      << dlerror();
            return;
        }

        bool ok = true;
        // Memory management
        ok &= LoadSymbol(cudart_handle_, "cudaMalloc", rt_.p_cudaMalloc);
        ok &= LoadSymbol(cudart_handle_, "cudaFree", rt_.p_cudaFree);
        ok &= LoadSymbol(cudart_handle_, "cudaMemcpy", rt_.p_cudaMemcpy);
        ok &= LoadSymbol(cudart_handle_, "cudaMemcpyAsync",
                         rt_.p_cudaMemcpyAsync);
        ok &= LoadSymbol(cudart_handle_, "cudaMemset", rt_.p_cudaMemset);
        ok &= LoadSymbol(cudart_handle_, "cudaMemsetAsync",
                         rt_.p_cudaMemsetAsync);
        ok &=
            LoadSymbol(cudart_handle_, "cudaMallocHost", rt_.p_cudaMallocHost);
        ok &= LoadSymbol(cudart_handle_, "cudaHostAlloc", rt_.p_cudaHostAlloc);
        ok &= LoadSymbol(cudart_handle_, "cudaFreeHost", rt_.p_cudaFreeHost);
        ok &= LoadSymbol(cudart_handle_, "cudaHostRegister",
                         rt_.p_cudaHostRegister);
        ok &= LoadSymbol(cudart_handle_, "cudaHostUnregister",
                         rt_.p_cudaHostUnregister);
        ok &= LoadSymbol(cudart_handle_, "cudaHostGetDevicePointer",
                         rt_.p_cudaHostGetDevicePointer);

        // Device management
        ok &= LoadSymbol(cudart_handle_, "cudaSetDevice", rt_.p_cudaSetDevice);
        ok &= LoadSymbol(cudart_handle_, "cudaGetDevice", rt_.p_cudaGetDevice);
        ok &= LoadSymbol(cudart_handle_, "cudaGetDeviceCount",
                         rt_.p_cudaGetDeviceCount);
        ok &= LoadSymbol(cudart_handle_, "cudaDeviceGetAttribute",
                         rt_.p_cudaDeviceGetAttribute);
        ok &= LoadSymbol(cudart_handle_, "cudaDeviceGetPCIBusId",
                         rt_.p_cudaDeviceGetPCIBusId);
        ok &= LoadSymbol(cudart_handle_, "cudaDeviceSynchronize",
                         rt_.p_cudaDeviceSynchronize);
        ok &= LoadSymbol(cudart_handle_, "cudaDeviceCanAccessPeer",
                         rt_.p_cudaDeviceCanAccessPeer);
        ok &= LoadSymbol(cudart_handle_, "cudaDeviceEnablePeerAccess",
                         rt_.p_cudaDeviceEnablePeerAccess);

        // Pointer attributes
        ok &= LoadSymbol(cudart_handle_, "cudaPointerGetAttributes",
                         rt_.p_cudaPointerGetAttributes);

        // Stream management
        ok &= LoadSymbol(cudart_handle_, "cudaStreamCreate",
                         rt_.p_cudaStreamCreate);
        ok &= LoadSymbol(cudart_handle_, "cudaStreamCreateWithFlags",
                         rt_.p_cudaStreamCreateWithFlags);
        ok &= LoadSymbol(cudart_handle_, "cudaStreamDestroy",
                         rt_.p_cudaStreamDestroy);
        ok &= LoadSymbol(cudart_handle_, "cudaStreamSynchronize",
                         rt_.p_cudaStreamSynchronize);
        ok &= LoadSymbol(cudart_handle_, "cudaStreamQuery",
                         rt_.p_cudaStreamQuery);
        ok &= LoadSymbol(cudart_handle_, "cudaLaunchHostFunc",
                         rt_.p_cudaLaunchHostFunc);

        // Event management
        ok &= LoadSymbol(cudart_handle_, "cudaEventCreateWithFlags",
                         rt_.p_cudaEventCreateWithFlags);
        ok &= LoadSymbol(cudart_handle_, "cudaEventDestroy",
                         rt_.p_cudaEventDestroy);
        ok &= LoadSymbol(cudart_handle_, "cudaEventRecord",
                         rt_.p_cudaEventRecord);
        ok &=
            LoadSymbol(cudart_handle_, "cudaEventQuery", rt_.p_cudaEventQuery);

        // IPC
        ok &= LoadSymbol(cudart_handle_, "cudaIpcGetMemHandle",
                         rt_.p_cudaIpcGetMemHandle);
        ok &= LoadSymbol(cudart_handle_, "cudaIpcOpenMemHandle",
                         rt_.p_cudaIpcOpenMemHandle);
        ok &= LoadSymbol(cudart_handle_, "cudaIpcCloseMemHandle",
                         rt_.p_cudaIpcCloseMemHandle);

        // Error handling
        ok &= LoadSymbol(cudart_handle_, "cudaGetLastError",
                         rt_.p_cudaGetLastError);
        ok &= LoadSymbol(cudart_handle_, "cudaGetErrorString",
                         rt_.p_cudaGetErrorString);

        if (!ok) {
            LOG(WARNING) << "cuda_loader: libcudart.so loaded but missing "
                         << "required symbols.";
            dlclose(cudart_handle_);
            cudart_handle_ = nullptr;
            rt_ = {};
            return;
        }

        cudart_ok_ = true;
        LOG(INFO) << "cuda_loader: libcudart.so loaded successfully";
    }

    void loadCudaDriver() {
        static const char* const kDriverNames[] = {
            "libcuda.so.1",
        };
        driver_handle_ = TryDlopen(kDriverNames);
        if (!driver_handle_) {
            LOG(INFO) << "cuda_loader: libcuda.so.1 not available: "
                      << dlerror();
            return;
        }

        // cuInit is required before any other driver API call.
        // Load and call it first.
        CUresult (*p_cuInit)(unsigned int) = nullptr;
        if (!LoadSymbol(driver_handle_, "cuInit", p_cuInit)) {
            LOG(WARNING) << "cuda_loader: libcuda.so.1 missing cuInit";
            dlclose(driver_handle_);
            driver_handle_ = nullptr;
            return;
        }
        CUresult init_err = p_cuInit(0);
        if (init_err != CUDA_SUCCESS) {
            LOG(WARNING) << "cuda_loader: cuInit(0) failed with error "
                         << init_err;
            dlclose(driver_handle_);
            driver_handle_ = nullptr;
            return;
        }

        bool ok = true;
        // Device management
        ok &= LoadSymbol(driver_handle_, "cuDeviceGet", drv_.p_cuDeviceGet);
        ok &= LoadSymbol(driver_handle_, "cuDeviceGetAttribute",
                         drv_.p_cuDeviceGetAttribute);

        // Memory allocation (VMM)
        ok &= LoadSymbol(driver_handle_, "cuMemCreate", drv_.p_cuMemCreate);
        ok &= LoadSymbol(driver_handle_, "cuMemRelease", drv_.p_cuMemRelease);
        ok &= LoadSymbol(driver_handle_, "cuMemRetainAllocationHandle",
                         drv_.p_cuMemRetainAllocationHandle);
        ok &= LoadSymbol(driver_handle_, "cuMemGetAllocationGranularity",
                         drv_.p_cuMemGetAllocationGranularity);

        // Virtual address management
        ok &= LoadSymbol(driver_handle_, "cuMemAddressReserve",
                         drv_.p_cuMemAddressReserve);
        ok &= LoadSymbol(driver_handle_, "cuMemAddressFree",
                         drv_.p_cuMemAddressFree);

        // Mapping
        ok &= LoadSymbol(driver_handle_, "cuMemMap", drv_.p_cuMemMap);
        ok &= LoadSymbol(driver_handle_, "cuMemUnmap", drv_.p_cuMemUnmap);
        ok &=
            LoadSymbol(driver_handle_, "cuMemSetAccess", drv_.p_cuMemSetAccess);

        // IPC / sharing
        ok &= LoadSymbol(driver_handle_, "cuMemExportToShareableHandle",
                         drv_.p_cuMemExportToShareableHandle);
        ok &= LoadSymbol(driver_handle_, "cuMemImportFromShareableHandle",
                         drv_.p_cuMemImportFromShareableHandle);

        // Address range queries
        // NOTE: cuda.h defines `#define cuMemGetAddressRange
        // cuMemGetAddressRange_v2`. The v2 symbol uses 64-bit
        // CUdeviceptr/size_t parameters, while the unversioned v1 symbol uses
        // 32-bit unsigned int. We must dlsym the v2 name explicitly since
        // string literals are not affected by macros.
        ok &= LoadSymbol(driver_handle_, "cuMemGetAddressRange_v2",
                         drv_.p_cuMemGetAddressRange);
        ok &= LoadSymbol(driver_handle_, "cuMemGetHandleForAddressRange",
                         drv_.p_cuMemGetHandleForAddressRange);

        // Pointer attributes
        ok &= LoadSymbol(driver_handle_, "cuPointerGetAttribute",
                         drv_.p_cuPointerGetAttribute);

        // Error handling
        ok &= LoadSymbol(driver_handle_, "cuGetErrorString",
                         drv_.p_cuGetErrorString);

        if (!ok) {
            LOG(WARNING) << "cuda_loader: libcuda.so.1 loaded but missing "
                         << "required symbols.";
            dlclose(driver_handle_);
            driver_handle_ = nullptr;
            drv_ = {};
            return;
        }

        driver_ok_ = true;
        LOG(INFO) << "cuda_loader: libcuda.so.1 loaded successfully";
    }

#ifdef USE_GDS
    void loadCuFile() {
        static const char* const kCuFileNames[] = {
            "libcufile.so",
            "libcufile.so.0",
        };
        cufile_handle_ = TryDlopen(kCuFileNames);
        if (!cufile_handle_) {
            LOG(INFO) << "cuda_loader: libcufile.so not available: "
                      << dlerror();
            return;
        }

        bool ok = true;
        ok &= LoadSymbol(cufile_handle_, "cuFileDriverOpen",
                         cf_.p_cuFileDriverOpen);
        ok &= LoadSymbol(cufile_handle_, "cuFileHandleRegister",
                         cf_.p_cuFileHandleRegister);
        ok &= LoadSymbol(cufile_handle_, "cuFileHandleDeregister",
                         cf_.p_cuFileHandleDeregister);
        ok &= LoadSymbol(cufile_handle_, "cuFileBufRegister",
                         cf_.p_cuFileBufRegister);
        ok &= LoadSymbol(cufile_handle_, "cuFileBufDeregister",
                         cf_.p_cuFileBufDeregister);
        ok &= LoadSymbol(cufile_handle_, "cuFileBatchIOSetUp",
                         cf_.p_cuFileBatchIOSetUp);
        ok &= LoadSymbol(cufile_handle_, "cuFileBatchIODestroy",
                         cf_.p_cuFileBatchIODestroy);
        ok &= LoadSymbol(cufile_handle_, "cuFileBatchIOSubmit",
                         cf_.p_cuFileBatchIOSubmit);
        ok &= LoadSymbol(cufile_handle_, "cuFileBatchIOGetStatus",
                         cf_.p_cuFileBatchIOGetStatus);

        if (!ok) {
            LOG(WARNING) << "cuda_loader: libcufile.so loaded but missing "
                         << "required symbols.";
            dlclose(cufile_handle_);
            cufile_handle_ = nullptr;
            cf_ = {};
            return;
        }

        cufile_ok_ = true;
        LOG(INFO) << "cuda_loader: libcufile.so loaded successfully";
    }
#endif  // USE_GDS

   private:
    void* cudart_handle_ = nullptr;
    void* driver_handle_ = nullptr;
    CudaRtSymbols rt_{};
    CudaDriverSymbols drv_{};
    bool cudart_ok_ = false;
    bool driver_ok_ = false;
#ifdef USE_GDS
    void* cufile_handle_ = nullptr;
    CuFileSymbols cf_{};
    bool cufile_ok_ = false;
#endif
};

// Accessor used by wrapper functions
CudaLoader& GetLoader() { return CudaLoader::Instance(); }

CudaRtSymbols& GetCudaRtSymbols() { return GetLoader().rt(); }
CudaDriverSymbols& GetCudaDriverSymbols() { return GetLoader().drv(); }
#ifdef USE_GDS
CuFileSymbols& GetCuFileSymbols() { return GetLoader().cf(); }
#endif

}  // namespace cuda_loader

// C API implementation
extern "C" {

int cuda_loader_cudart_available(void) {
    return cuda_loader::GetLoader().cudart_ok() ? 1 : 0;
}

int cuda_loader_driver_available(void) {
    return cuda_loader::GetLoader().driver_ok() ? 1 : 0;
}

int cuda_loader_cufile_available(void) {
    return cuda_loader::GetLoader().cufile_ok() ? 1 : 0;
}

int cuda_loader_is_available(void) { return cuda_loader_cudart_available(); }

}  // extern "C"
