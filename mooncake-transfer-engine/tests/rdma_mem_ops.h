#pragma once

#include <glog/logging.h>
#include <numa.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "cuda_alike.h"

#if defined(USE_MLU)
#include <cnrt.h>
#endif

namespace mooncake {
namespace rdma_test {

struct MemOps {
    virtual ~MemOps() = default;
    virtual std::string name() const = 0;
    virtual void setDev(int dev_id) = 0;
    virtual void *alloc(size_t size, int sock_id) = 0;
    virtual void freeBuf(void *addr, size_t size) = 0;
    virtual void copyIn(void *dst, const void *src, size_t size) = 0;
    virtual void copyOut(void *dst, const void *src, size_t size) = 0;
    virtual std::string loc(int dev_id) const = 0;
};

class CpuMemOps final : public MemOps {
   public:
    std::string name() const override { return "cpu"; }

    void setDev(int dev_id) override { (void)dev_id; }

    void *alloc(size_t size, int sock_id) override {
        return numa_alloc_onnode(size, sock_id);
    }

    void freeBuf(void *addr, size_t size) override { numa_free(addr, size); }

    void copyIn(void *dst, const void *src, size_t size) override {
        std::memcpy(dst, src, size);
    }

    void copyOut(void *dst, const void *src, size_t size) override {
        std::memcpy(dst, src, size);
    }

    std::string loc(int dev_id) const override {
        (void)dev_id;
        return "cpu:0";
    }
};

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
inline void checkGpu(int ret, const char *msg) {
    if (ret != cudaSuccess) {
        LOG(ERROR) << msg << " (code=" << ret << ", msg="
                   << cudaGetErrorString(ret) << ")";
        std::exit(EXIT_FAILURE);
    }
}

class GpuMemOps final : public MemOps {
   public:
    std::string name() const override { return "gpu"; }

    void setDev(int dev_id) override {
        checkGpu(cudaSetDevice(dev_id), "Failed to set GPU device");
    }

    void *alloc(size_t size, int sock_id) override {
        (void)sock_id;
        void *ptr = nullptr;
        checkGpu(cudaMalloc(&ptr, size), "Failed to allocate GPU memory");
        return ptr;
    }

    void freeBuf(void *addr, size_t size) override {
        (void)size;
        checkGpu(cudaFree(addr), "Failed to free GPU memory");
    }

    void copyIn(void *dst, const void *src, size_t size) override {
        checkGpu(cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice),
                 "Failed to copy host data to GPU");
    }

    void copyOut(void *dst, const void *src, size_t size) override {
        checkGpu(cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost),
                 "Failed to copy GPU data to host");
    }

    std::string loc(int dev_id) const override {
        return GPU_PREFIX + std::to_string(dev_id);
    }
};
#endif

#if defined(USE_MLU)
inline void checkMlu(cnrtRet_t ret, const char *msg) {
    if (ret != cnrtSuccess) {
        LOG(ERROR) << msg << ", cnrt error=" << ret;
        std::exit(EXIT_FAILURE);
    }
}

class MluMemOps final : public MemOps {
   public:
    std::string name() const override { return "mlu"; }

    void setDev(int dev_id) override {
        checkMlu(cnrtSetDevice(dev_id), "Failed to set MLU device");
    }

    void *alloc(size_t size, int sock_id) override {
        (void)sock_id;
        void *ptr = nullptr;
        checkMlu(cnrtMalloc(&ptr, size), "Failed to allocate MLU memory");
        return ptr;
    }

    void freeBuf(void *addr, size_t size) override {
        (void)size;
        checkMlu(cnrtFree(addr), "Failed to free MLU memory");
    }

    void copyIn(void *dst, const void *src, size_t size) override {
        checkMlu(cnrtMemcpy(dst, const_cast<void *>(src), size,
                            cnrtMemcpyHostToDev),
                 "Failed to copy host data to MLU");
    }

    void copyOut(void *dst, const void *src, size_t size) override {
        checkMlu(cnrtMemcpy(dst, const_cast<void *>(src), size,
                            cnrtMemcpyDevToHost),
                 "Failed to copy MLU data to host");
    }

    std::string loc(int dev_id) const override {
        return "mlu:" + std::to_string(dev_id);
    }
};
#endif

inline std::shared_ptr<MemOps> mkMem(const std::string &backend) {
    if (backend == "cpu") {
        return std::make_shared<CpuMemOps>();
    }
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    if (backend == "gpu") {
        return std::make_shared<GpuMemOps>();
    }
#endif
#if defined(USE_MLU)
    if (backend == "mlu") {
        return std::make_shared<MluMemOps>();
    }
#endif
    return nullptr;
}

}  // namespace rdma_test
}  // namespace mooncake
