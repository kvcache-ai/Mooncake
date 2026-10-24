// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "transport/rdma_transport/pxn/pxn_memcpy_backend.h"

#include <glog/logging.h>

#include <chrono>
#include <type_traits>
#include <utility>

namespace mooncake {
namespace pxn {

// ============================================================
// The cudaMemcpyBatchAsync API was introduced in CUDA 12.8 with an
// 8-argument signature:
//     (dsts, srcs, sizes, count, attrs*, attrsIdxs*, numAttrs, stream)
// A later CUDA release adds an extra `size_t *failIdx` parameter
// (9 args total):
//     (dsts, srcs, sizes, count, attrs*, attrsIdxs*, numAttrs,
//      failIdx*, stream)
//
// Different CUDA 12.8+ / 13.x toolkits (and hotfix updates) ship
// different signatures without the CUDART_VERSION macro necessarily
// changing, so we cannot rely on CUDART_VERSION to pick the right
// overload. We use two helper functions plus expression SFINAE so the
// compiler simply lets the current toolkit's overload set do argument
// deduction, and the branch that does not fit is discarded.
// ============================================================
#if CUDART_VERSION >= 12080
namespace detail {

// 9-arg call. Enabled only when the current toolkit's
// cudaMemcpyBatchAsync accepts a `size_t* failIdx` in that position.
template <typename Stream>
inline auto PxnInvokeMemcpyBatchAsync(
    void* const* dsts, void** srcs, const size_t* sizes, size_t count,
    cudaMemcpyAttributes* attrs, size_t* attrs_idxs, size_t num_attrs,
    size_t* fail_idx, Stream stream, int /*tag*/ = 0)
    -> decltype(cudaMemcpyBatchAsync(dsts, srcs, sizes, count, attrs,
                                     attrs_idxs, num_attrs, fail_idx,
                                     stream)) {
    return cudaMemcpyBatchAsync(dsts, srcs, sizes, count, attrs, attrs_idxs,
                                num_attrs, fail_idx, stream);
}

// 8-arg call (variadic fallback so the 9-arg overload wins whenever it
// exists). Silently ignores fail_idx.
template <typename Stream>
inline cudaError_t PxnInvokeMemcpyBatchAsync(
    void* const* dsts, void** srcs, const size_t* sizes, size_t count,
    cudaMemcpyAttributes* attrs, size_t* attrs_idxs, size_t num_attrs,
    size_t* /*fail_idx*/, Stream stream, ...) {
    return cudaMemcpyBatchAsync(dsts, srcs, sizes, count, attrs, attrs_idxs,
                                num_attrs, stream);
}

}  // namespace detail

// CUDA 12.8+
class PxnBatchAsyncBackend : public PxnMemcpyBackend {
   public:
    cudaError_t PxnSubmitBatch(
        const std::vector<void*>& dsts,
        const std::vector<const void*>& srcs,
        const std::vector<size_t>& sizes,
        cudaStream_t stream,
        cudaEvent_t event) override {
        if (dsts.empty()) return cudaSuccess;

        cudaMemcpyAttributes attr = {};
        attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;

        size_t attrs_idxs = 0;
        size_t fail_idx = 0;
        cudaError_t err = detail::PxnInvokeMemcpyBatchAsync(
            dsts.data(),
            const_cast<void**>(srcs.data()),
            sizes.data(),
            dsts.size(),
            &attr,
            &attrs_idxs,
            /*numAttrs=*/1,
            &fail_idx,
            stream);
        if (err != cudaSuccess) {
            LOG(ERROR) << "PXN: cudaMemcpyBatchAsync failed"
                       << " (failIdx=" << fail_idx << "): "
                       << cudaGetErrorString(err);
            return err;
        }
        return cudaEventRecord(event, stream);
    }

    cudaError_t PxnSubmitBatchMultiEvent(
        const std::vector<void*>& dsts,
        const std::vector<const void*>& srcs,
        const std::vector<size_t>& sizes,
        cudaStream_t stream,
        const std::vector<cudaEvent_t>& events) override {
        if (dsts.empty()) return cudaSuccess;

        cudaMemcpyAttributes attr = {};
        attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;

        size_t attrs_idxs = 0;
        size_t fail_idx = 0;
        cudaError_t err = detail::PxnInvokeMemcpyBatchAsync(
            dsts.data(),
            const_cast<void**>(srcs.data()),
            sizes.data(),
            dsts.size(),
            &attr,
            &attrs_idxs,
            1,
            &fail_idx,
            stream);

        if (err != cudaSuccess) {
            LOG(ERROR) << "PXN: cudaMemcpyBatchAsync failed"
                       << " (failIdx=" << fail_idx << "): "
                       << cudaGetErrorString(err);
            return err;
        }
        for (auto ev : events) {
            cudaError_t e = cudaEventRecord(ev, stream);
            if (e != cudaSuccess) return e;
        }

        return cudaSuccess;
    }

    const char* PxnBackendName() const override {
        return "cudaMemcpyBatchAsync";
    }
};
#endif  // CUDART_VERSION >= 12080

// ============================================================
// Serves two roles:
//   - CUDA < 12.8 build: the only backend (BatchAsync API is missing at
//     compile time, so PxnBatchAsyncBackend is not compiled).
//   - CUDA >= 12.8 build: runtime fallback for the case where
//     cudaRuntimeGetVersion() reports a version older than the headers
//     were built against (e.g. mixed toolkit / driver deployment).
// Therefore this class is NOT guarded by CUDART_VERSION.
//
// src / dst devices are derived from the pointer attributes at every
// submit call (via cudaPointerGetAttributes), matching the "no
// constructor arguments" contract of PxnBatchAsyncBackend so callers
// can treat the two backends symmetrically.
// ============================================================
class PxnPeerAsyncBackend : public PxnMemcpyBackend {
   public:
    PxnPeerAsyncBackend() = default;

    cudaError_t PxnSubmitBatch(
        const std::vector<void*>& dsts,
        const std::vector<const void*>& srcs,
        const std::vector<size_t>& sizes,
        cudaStream_t stream,
        cudaEvent_t event) override {
        if (dsts.empty()) return cudaSuccess;
        for (size_t entry_idx = 0; entry_idx < dsts.size(); ++entry_idx) {
            int src_device = -1;
            int dst_device = -1;
            cudaPointerAttributes src_attr = {};
            cudaPointerAttributes dst_attr = {};
            if (cudaPointerGetAttributes(&src_attr, srcs[entry_idx]) !=
                    cudaSuccess ||
                cudaPointerGetAttributes(&dst_attr, dsts[entry_idx]) !=
                    cudaSuccess) {
                cudaGetLastError();
                LOG(ERROR) << "PXN: cudaPointerGetAttributes failed at index "
                           << entry_idx;
                return cudaErrorInvalidValue;
            }
            src_device = src_attr.device;
            dst_device = dst_attr.device;
            cudaError_t err = cudaMemcpyPeerAsync(
                dsts[entry_idx], dst_device,
                srcs[entry_idx], src_device,
                sizes[entry_idx], stream);
            if (err != cudaSuccess) {
                LOG(ERROR) << "PXN: cudaMemcpyPeerAsync failed at index "
                           << entry_idx << ": " << cudaGetErrorString(err);
                return err;
            }
        }
        return cudaEventRecord(event, stream);
    }

    cudaError_t PxnSubmitBatchMultiEvent(
        const std::vector<void*>& dsts,
        const std::vector<const void*>& srcs,
        const std::vector<size_t>& sizes,
        cudaStream_t stream,
        const std::vector<cudaEvent_t>& events) override {
        if (dsts.empty()) return cudaSuccess;
        for (size_t entry_idx = 0; entry_idx < dsts.size(); ++entry_idx) {
            cudaPointerAttributes src_attr = {};
            cudaPointerAttributes dst_attr = {};
            if (cudaPointerGetAttributes(&src_attr, srcs[entry_idx]) !=
                    cudaSuccess ||
                cudaPointerGetAttributes(&dst_attr, dsts[entry_idx]) !=
                    cudaSuccess) {
                cudaGetLastError();
                return cudaErrorInvalidValue;
            }
            cudaError_t err = cudaMemcpyPeerAsync(
                dsts[entry_idx], dst_attr.device,
                srcs[entry_idx], src_attr.device,
                sizes[entry_idx], stream);
            if (err != cudaSuccess) return err;
        }
        for (auto ev : events) {
            cudaError_t e = cudaEventRecord(ev, stream);
            if (e != cudaSuccess) return e;
        }
        return cudaSuccess;
    }

    const char* PxnBackendName() const override {
        return "cudaMemcpyPeerAsync (fallback)";
    }
};

// choose pxn memcpy backend
std::unique_ptr<PxnMemcpyBackend> PxnCreateMemcpyBackend() {
#if CUDART_VERSION >= 12080
    int runtime_version = 0;
    if (cudaRuntimeGetVersion(&runtime_version) == cudaSuccess &&
        runtime_version >= 12080) {
        static bool logged = false;
        if (!logged) {
            LOG(INFO) << "PXN: using cudaMemcpyBatchAsync (CUDA "
                      << runtime_version << ")";
            logged = true;
        }
        return std::unique_ptr<PxnMemcpyBackend>(new PxnBatchAsyncBackend());
    }
#endif
    static bool logged = false;
    if (!logged) {
        LOG(WARNING)
            << "PXN: using cudaMemcpyPeerAsync loop fallback; "
            << "upgrade to CUDA 12.8+ for optimal PXN performance";
        logged = true;
    }
    return std::unique_ptr<PxnMemcpyBackend>(new PxnPeerAsyncBackend());
}

}  // namespace pxn
}  // namespace mooncake
