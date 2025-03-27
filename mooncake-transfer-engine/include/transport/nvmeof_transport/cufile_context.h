// Copyright 2024 KVCache.AI
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

#ifndef CUFILE_CONTEXT_H_
#define CUFILE_CONTEXT_H_

#include <cufile.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>
#include <system_error>

static inline const char *GetCuErrorString(CUresult curesult) {
    const char *descp = "";
    if (cuGetErrorName(curesult, &descp) != CUDA_SUCCESS)
        descp = "unknown cuda error";
    return descp;
}

static std::string cuFileGetErrorString(int status) {
    status = std::abs(status);
    return IS_CUFILE_ERR(status) ? std::string(CUFILE_ERRSTR(status))
                                 : std::string(std::strerror(status));
}

static std::string cuFileGetErrorString(CUfileError_t status) {
    std::string errStr = cuFileGetErrorString(static_cast<int>(status.err));
    if (IS_CUDA_ERR(status))
        errStr.append(".").append(GetCuErrorString(status.cu_err));
    return errStr;
}

#define CUFILE_CHECK(e)                                                       \
    do {                                                                      \
        if (e.err != CU_FILE_SUCCESS) {                                       \
            throw std::runtime_error("Error Code: " + std::to_string(e.err) + \
                                     " " + cuFileGetErrorString(e) + " @ " +  \
                                     __FILE__ + ":" +                         \
                                     std::to_string(__LINE__));               \
            assert(false);                                                    \
        }                                                                     \
    } while (0)

class CuFileContext {
    CUfileHandle_t handle = NULL;
    CUfileDescr_t desc;

   public:
    CUfileHandle_t getHandle() const { return handle; }

    /// Create a GDS segment from file name. Return NULL on error.
    explicit CuFileContext(const char *filename) {
        int fd = open(filename, O_RDWR | O_DIRECT);
        memset(&desc, 0, sizeof(desc));
        desc.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
        desc.handle.fd = fd;
        CUFILE_CHECK(cuFileHandleRegister(&handle, &desc));
    }

    CuFileContext(const CuFileContext &) = delete;
    CuFileContext &operator=(const CuFileContext &) = delete;

    ~CuFileContext() {
        if (handle) {
            cuFileHandleDeregister(handle);
        }
        if (desc.handle.fd) {
            close(desc.handle.fd);
        }
    }
};

#endif