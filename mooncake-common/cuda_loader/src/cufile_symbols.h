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

#ifndef CUDA_LOADER_CUFILE_SYMBOLS_H
#define CUDA_LOADER_CUFILE_SYMBOLS_H

#include <cufile.h>

namespace cuda_loader {

struct CuFileSymbols {
    CUfileError_t (*p_cuFileDriverOpen)(void) = nullptr;
    CUfileError_t (*p_cuFileHandleRegister)(CUfileHandle_t*,
                                            CUfileDescr_t*) = nullptr;
    void (*p_cuFileHandleDeregister)(CUfileHandle_t) = nullptr;
    CUfileError_t (*p_cuFileBufRegister)(const void*, size_t, int) = nullptr;
    CUfileError_t (*p_cuFileBufDeregister)(const void*) = nullptr;
    CUfileError_t (*p_cuFileBatchIOSetUp)(CUfileBatchHandle_t*,
                                          unsigned) = nullptr;
    void (*p_cuFileBatchIODestroy)(CUfileBatchHandle_t) = nullptr;
    CUfileError_t (*p_cuFileBatchIOSubmit)(CUfileBatchHandle_t, unsigned,
                                           CUfileIOParams_t*,
                                           unsigned int) = nullptr;
    CUfileError_t (*p_cuFileBatchIOGetStatus)(CUfileBatchHandle_t, unsigned,
                                              unsigned*, CUfileIOEvents_t*,
                                              struct timespec*) = nullptr;
};

}  // namespace cuda_loader

#endif  // CUDA_LOADER_CUFILE_SYMBOLS_H
