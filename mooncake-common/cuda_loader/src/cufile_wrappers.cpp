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

#include <cufile.h>

#include "internal.h"

using cuda_loader::GetCuFileSymbols;

static CUfileError_t cufile_not_available() {
    CUfileError_t err;
    err.err = CU_FILE_DRIVER_NOT_INITIALIZED;
    err.cu_err = CUDA_SUCCESS;
    return err;
}

extern "C" {

CUfileError_t cuFileDriverOpen(void) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileDriverOpen) return s.p_cuFileDriverOpen();
    return cufile_not_available();
}

CUfileError_t cuFileHandleRegister(CUfileHandle_t* fh, CUfileDescr_t* descr) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileHandleRegister) return s.p_cuFileHandleRegister(fh, descr);
    return cufile_not_available();
}

void cuFileHandleDeregister(CUfileHandle_t fh) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileHandleDeregister) s.p_cuFileHandleDeregister(fh);
}

CUfileError_t cuFileBufRegister(const void* devPtr_base, size_t size,
                                int flags) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileBufRegister)
        return s.p_cuFileBufRegister(devPtr_base, size, flags);
    return cufile_not_available();
}

CUfileError_t cuFileBufDeregister(const void* devPtr_base) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileBufDeregister) return s.p_cuFileBufDeregister(devPtr_base);
    return cufile_not_available();
}

CUfileError_t cuFileBatchIOSetUp(CUfileBatchHandle_t* batch_idp, unsigned nr) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileBatchIOSetUp) return s.p_cuFileBatchIOSetUp(batch_idp, nr);
    return cufile_not_available();
}

void cuFileBatchIODestroy(CUfileBatchHandle_t batch_idp) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileBatchIODestroy) s.p_cuFileBatchIODestroy(batch_idp);
}

CUfileError_t cuFileBatchIOSubmit(CUfileBatchHandle_t batch_idp, unsigned nr,
                                  CUfileIOParams_t* io_params,
                                  unsigned int flags) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileBatchIOSubmit)
        return s.p_cuFileBatchIOSubmit(batch_idp, nr, io_params, flags);
    return cufile_not_available();
}

CUfileError_t cuFileBatchIOGetStatus(CUfileBatchHandle_t batch_idp,
                                     unsigned min_nr, unsigned* nr,
                                     CUfileIOEvents_t* io_events,
                                     struct timespec* timeout) {
    auto& s = GetCuFileSymbols();
    if (s.p_cuFileBatchIOGetStatus)
        return s.p_cuFileBatchIOGetStatus(batch_idp, min_nr, nr, io_events,
                                          timeout);
    return cufile_not_available();
}

}  // extern "C"
