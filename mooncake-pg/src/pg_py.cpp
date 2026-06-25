#include <mooncake_backend.h>
#include <mooncake_worker.cuh>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/python.h>
#include <torch/torch.h>
#include <cstring>
#ifndef MOONCAKE_EP_USE_MUSA
#include <ATen/cuda/CUDAContext.h>
#include <c10/cuda/CUDAGuard.h>
#include <cuda_runtime_api.h>
#endif

namespace py = pybind11;

namespace mooncake {

c10::intrusive_ptr<c10d::ProcessGroup> createMooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    return c10::make_intrusive<MooncakeBackend>(std::move(distBackendOpts),
                                                std::move(backendOptions));
}

c10::intrusive_ptr<c10d::ProcessGroup> createMooncakeCpuBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    return c10::make_intrusive<MooncakeBackend>(
        std::move(distBackendOpts), std::move(backendOptions), true);
}

__attribute__((constructor)) static void MooncakeBackendConstructor() {
    py::object module = py::module::import("torch.distributed");
    py::object register_backend =
        module.attr("Backend").attr("register_backend");
    py::dict kwargsCpu;
    kwargsCpu["devices"] = py::make_tuple("cpu");
    register_backend("mooncake-cpu", py::cpp_function(createMooncakeCpuBackend),
                     /* extended_api */ true, **kwargsCpu);
#ifndef MOONCAKE_EP_USE_MUSA
    py::dict kwargsCuda;
    kwargsCuda["devices"] = py::make_tuple("cuda");
    register_backend("mooncake", py::cpp_function(createMooncakeBackend),
                     /* extended_api */ true, **kwargsCuda);
#else
    py::dict kwargsMusa;
    kwargsMusa["devices"] = py::make_tuple("musa");
    register_backend("mooncake", py::cpp_function(createMooncakeBackend),
                     /* extended_api */ true, **kwargsMusa);
#endif
}

std::string getPreferredHca(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                            std::string location) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getPreferredHca(location);
}

at::Tensor getActiveRanks(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getActiveRanksTensor();
}

int getNumSyncedRanks(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getNumSyncedRanks();
}

void extendGroupSizeTo(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                       int size) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->extendGroupSizeTo(size);
}

std::vector<bool> getPeerState(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                               const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getPeerState(ranks);
}

void recoverRanks(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                  const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->recoverRanks(ranks);
}

void joinGroup(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->joinGroup();
}

/// Python-facing wrapper that extracts the raw TransferEngine* from a
/// mooncake.engine.TransferEngine Python object and passes it to
/// MooncakeBackend::setExternalEngine().  The caller must ensure the
/// TransferEnginePy object outlives all MooncakeBackend instances.
void setTransferEnginePy(pybind11::object engine_obj) {
    if (engine_obj.is_none()) {
        MooncakeBackend::setExternalEngine(nullptr);
        return;
    }
    auto get_engine_ptr = engine_obj.attr("get_engine_ptr");
    uintptr_t ptr = get_engine_ptr().cast<uintptr_t>();
    auto* engine = reinterpret_cast<TransferEngine*>(ptr);
    MooncakeBackend::setExternalEngine(engine);
}

#ifndef MOONCAKE_EP_USE_MUSA
py::bytes ipcExportTensor(at::Tensor tensor) {
    TORCH_CHECK(tensor.is_cuda(), "ipc_export_tensor expects a CUDA tensor");
    TORCH_CHECK(tensor.is_contiguous(),
                "ipc_export_tensor expects a contiguous tensor");
    cudaIpcMemHandle_t handle;
    auto err = cudaIpcGetMemHandle(&handle, tensor.data_ptr());
    TORCH_CHECK(err == cudaSuccess, "cudaIpcGetMemHandle failed: ",
                cudaGetErrorString(err));
    return py::bytes(reinterpret_cast<const char*>(&handle), sizeof(handle));
}

at::Tensor ipcAllocLike(int64_t numel, at::Tensor like) {
    TORCH_CHECK(like.is_cuda(), "ipc_alloc_like expects a CUDA tensor");
    TORCH_CHECK(numel >= 0, "ipc_alloc_like expects non-negative numel");
    at::cuda::CUDAGuard deviceGuard(like.device());
    void* ptr = nullptr;
    const size_t bytes = static_cast<size_t>(numel) * like.element_size();
    auto err = cudaMalloc(&ptr, bytes == 0 ? 1 : bytes);
    TORCH_CHECK(err == cudaSuccess, "cudaMalloc failed: ",
                cudaGetErrorString(err));
    return at::from_blob(
        ptr, {numel}, [](void* p) {
            if (p) cudaFree(p);
        }, like.options());
}

at::Tensor ipcAllocInt32(int64_t numel, at::Tensor like) {
    TORCH_CHECK(like.is_cuda(), "ipc_alloc_int32 expects a CUDA tensor");
    TORCH_CHECK(numel >= 0, "ipc_alloc_int32 expects non-negative numel");
    at::cuda::CUDAGuard deviceGuard(like.device());
    void* ptr = nullptr;
    const size_t bytes = static_cast<size_t>(numel) * sizeof(int32_t);
    auto err = cudaMalloc(&ptr, bytes == 0 ? sizeof(int32_t) : bytes);
    TORCH_CHECK(err == cudaSuccess, "cudaMalloc int32 failed: ",
                cudaGetErrorString(err));
    err = cudaMemset(ptr, 0, bytes == 0 ? sizeof(int32_t) : bytes);
    TORCH_CHECK(err == cudaSuccess, "cudaMemset int32 failed: ",
                cudaGetErrorString(err));
    auto options = at::TensorOptions().dtype(at::kInt).device(like.device());
    return at::from_blob(
        ptr, {numel}, [](void* p) {
            if (p) cudaFree(p);
        }, options);
}

void ipcEnablePeerAccess(int numDevices) {
    int current = -1;
    auto err = cudaGetDevice(&current);
    TORCH_CHECK(err == cudaSuccess, "cudaGetDevice failed: ",
                cudaGetErrorString(err));
    for (int dev = 0; dev < numDevices; ++dev) {
        if (dev == current) continue;
        int canAccess = 0;
        err = cudaDeviceCanAccessPeer(&canAccess, current, dev);
        TORCH_CHECK(err == cudaSuccess, "cudaDeviceCanAccessPeer failed: ",
                    cudaGetErrorString(err));
        if (!canAccess) continue;
        err = cudaDeviceEnablePeerAccess(dev, 0);
        if (err == cudaErrorPeerAccessAlreadyEnabled) {
            cudaGetLastError();
            continue;
        }
        TORCH_CHECK(err == cudaSuccess, "cudaDeviceEnablePeerAccess failed: ",
                    cudaGetErrorString(err));
    }
}

std::vector<uintptr_t> ipcOpenOutputHandles(const std::vector<py::bytes>& handles,
                                            at::Tensor localOutput,
                                            int rank) {
    TORCH_CHECK(localOutput.is_cuda(),
                "ipc_open_output_handles expects a CUDA tensor");
    TORCH_CHECK(localOutput.is_contiguous(),
                "ipc_open_output_handles expects a contiguous tensor");
    TORCH_CHECK(rank >= 0 && static_cast<size_t>(rank) < handles.size(),
                "rank out of range for IPC handle list");

    std::vector<uintptr_t> ptrs(handles.size(), 0);
    for (size_t i = 0; i < handles.size(); ++i) {
        if (static_cast<int>(i) == rank) {
            ptrs[i] = reinterpret_cast<uintptr_t>(localOutput.data_ptr());
            continue;
        }
        std::string bytes = handles[i];
        TORCH_CHECK(bytes.size() == sizeof(cudaIpcMemHandle_t),
                    "Invalid CUDA IPC handle size: ", bytes.size());
        cudaIpcMemHandle_t handle;
        std::memcpy(&handle, bytes.data(), sizeof(handle));
        void* ptr = nullptr;
        auto err = cudaIpcOpenMemHandle(
            &ptr, handle, cudaIpcMemLazyEnablePeerAccess);
        TORCH_CHECK(err == cudaSuccess, "cudaIpcOpenMemHandle failed for rank ",
                    i, ": ", cudaGetErrorString(err));
        ptrs[i] = reinterpret_cast<uintptr_t>(ptr);
    }
    return ptrs;
}

void ipcCloseHandles(const std::vector<uintptr_t>& ptrs, int rank) {
    for (size_t i = 0; i < ptrs.size(); ++i) {
        if (static_cast<int>(i) == rank || ptrs[i] == 0) continue;
        auto err = cudaIpcCloseMemHandle(reinterpret_cast<void*>(ptrs[i]));
        TORCH_CHECK(err == cudaSuccess, "cudaIpcCloseMemHandle failed for rank ",
                    i, ": ", cudaGetErrorString(err));
    }
}

void ipcAllgatherMemcpy(at::Tensor input, const std::vector<uintptr_t>& outPtrs,
                        int rank, size_t tensorBytes) {
    TORCH_CHECK(input.is_cuda(), "ipc_allgather_memcpy expects CUDA input");
    TORCH_CHECK(input.is_contiguous(),
                "ipc_allgather_memcpy expects contiguous input");
    TORCH_CHECK(rank >= 0, "rank must be non-negative");
    auto stream = at::cuda::getCurrentCUDAStream(input.device().index());
    int srcDevice = input.get_device();
    for (size_t peer = 0; peer < outPtrs.size(); ++peer) {
        TORCH_CHECK(outPtrs[peer] != 0, "Missing output pointer for peer ", peer);
        void* dst = reinterpret_cast<char*>(outPtrs[peer]) +
                    static_cast<size_t>(rank) * tensorBytes;
        cudaError_t err;
        if (static_cast<int>(peer) == rank) {
            err = cudaMemcpyAsync(dst, input.data_ptr(), tensorBytes,
                                  cudaMemcpyDeviceToDevice, stream.stream());
        } else {
            err = cudaMemcpyPeerAsync(dst, static_cast<int>(peer),
                                      input.data_ptr(), srcDevice, tensorBytes,
                                      stream.stream());
        }
        TORCH_CHECK(err == cudaSuccess, "cudaMemcpyAsync IPC allgather failed: ",
                    cudaGetErrorString(err));
    }
}

at::Tensor ipcPackOutputPtrs(const std::vector<uintptr_t>& outPtrs,
                             at::Tensor like) {
    TORCH_CHECK(like.is_cuda(), "ipc_pack_output_ptrs expects CUDA tensor");
    TORCH_CHECK(!outPtrs.empty(), "ipc_pack_output_ptrs expects non-empty ptrs");
    auto options = at::TensorOptions().dtype(at::kLong).device(like.device());
    at::Tensor ptrTensor = at::empty({static_cast<int64_t>(outPtrs.size())}, options);
    auto err = cudaMemcpy(ptrTensor.data_ptr<int64_t>(), outPtrs.data(),
                          outPtrs.size() * sizeof(uintptr_t),
                          cudaMemcpyHostToDevice);
    TORCH_CHECK(err == cudaSuccess, "cudaMemcpy output ptr table failed: ",
                cudaGetErrorString(err));
    return ptrTensor;
}

void ipcAllgatherStore(at::Tensor input, at::Tensor outPtrTensor, int rank,
                       size_t tensorBytes) {
    TORCH_CHECK(input.is_cuda(), "ipc_allgather_store expects CUDA input");
    TORCH_CHECK(input.is_contiguous(),
                "ipc_allgather_store expects contiguous input");
    TORCH_CHECK(outPtrTensor.is_cuda(),
                "ipc_allgather_store expects CUDA pointer tensor");
    TORCH_CHECK(outPtrTensor.is_contiguous(),
                "ipc_allgather_store expects contiguous pointer tensor");
    TORCH_CHECK(outPtrTensor.scalar_type() == at::kLong,
                "ipc_allgather_store expects int64 pointer tensor");
    TORCH_CHECK(rank >= 0, "rank must be non-negative");
    const int numRanks = static_cast<int>(outPtrTensor.numel());
    TORCH_CHECK(rank < numRanks, "rank out of range for pointer tensor");
    TORCH_CHECK(tensorBytes <=
                    static_cast<size_t>(input.numel()) * input.element_size(),
                "tensorBytes exceeds input storage span");

    auto stream = at::cuda::getCurrentCUDAStream(input.device().index());
    launchIpcAllgatherStoreKernel(
        input.data_ptr(), reinterpret_cast<const uintptr_t*>(outPtrTensor.data_ptr<int64_t>()),
        rank, numRanks, tensorBytes, stream.stream());
    auto err = cudaGetLastError();
    TORCH_CHECK(err == cudaSuccess, "ipc allgather store kernel launch failed: ",
                cudaGetErrorString(err));
}

void ipcAllgatherStoreSignal(at::Tensor input, at::Tensor outPtrTensor,
                             at::Tensor signalPtrTensor, int rank,
                             uint32_t sequence, size_t tensorBytes) {
    TORCH_CHECK(input.is_cuda(),
                "ipc_allgather_store_signal expects CUDA input");
    TORCH_CHECK(input.is_contiguous(),
                "ipc_allgather_store_signal expects contiguous input");
    TORCH_CHECK(outPtrTensor.is_cuda() && outPtrTensor.is_contiguous() &&
                    outPtrTensor.scalar_type() == at::kLong,
                "ipc_allgather_store_signal expects CUDA int64 output ptrs");
    TORCH_CHECK(signalPtrTensor.is_cuda() && signalPtrTensor.is_contiguous() &&
                    signalPtrTensor.scalar_type() == at::kLong,
                "ipc_allgather_store_signal expects CUDA int64 signal ptrs");
    const int numRanks = static_cast<int>(outPtrTensor.numel());
    TORCH_CHECK(signalPtrTensor.numel() == numRanks,
                "output and signal pointer tables must have same length");
    TORCH_CHECK(rank >= 0 && rank < numRanks,
                "rank out of range for pointer tables");
    TORCH_CHECK(sequence > 0, "sequence must be positive");
    TORCH_CHECK(tensorBytes <=
                    static_cast<size_t>(input.numel()) * input.element_size(),
                "tensorBytes exceeds input storage span");

    auto stream = at::cuda::getCurrentCUDAStream(input.device().index());
    launchIpcAllgatherStoreSignalKernel(
        input.data_ptr(),
        reinterpret_cast<const uintptr_t*>(outPtrTensor.data_ptr<int64_t>()),
        reinterpret_cast<const uintptr_t*>(signalPtrTensor.data_ptr<int64_t>()),
        rank, numRanks, sequence, tensorBytes, stream.stream());
    auto err = cudaGetLastError();
    TORCH_CHECK(err == cudaSuccess,
                "ipc allgather store+signal kernel launch failed: ",
                cudaGetErrorString(err));
}
#endif

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("createMooncakeBackend", &createMooncakeBackend);
    m.def("createMooncakeCpuBackend", &createMooncakeCpuBackend);
    m.def("set_host_ip", &MooncakeBackend::setHostIp);
    m.def("set_device_filter", &MooncakeBackend::setDeviceFilter);
    m.def("set_transfer_engine", &setTransferEnginePy, py::arg("engine"),
          "Set an external TransferEngine to be used by MooncakeBackend. "
          "Must be called before init_process_group(). The engine must already "
          "be initialized. Pass None to reset to default behavior. "
          "The caller must ensure the TransferEngine object outlives all "
          "MooncakeBackend instances.");
    m.def("get_preferred_hca", &getPreferredHca);
    m.def("get_active_ranks", &getActiveRanks);
    m.def("get_num_synced_ranks", &getNumSyncedRanks);
    m.def("extend_group_size_to", &extendGroupSizeTo);
    m.def("get_peer_state", &getPeerState);
    m.def("recover_ranks", &recoverRanks);
    m.def("join_group", &joinGroup);

#ifndef MOONCAKE_EP_USE_MUSA
    m.def("ipc_alloc_like", &ipcAllocLike, py::arg("numel"), py::arg("like"));
    m.def("ipc_alloc_int32", &ipcAllocInt32, py::arg("numel"),
          py::arg("like"));
    m.def("ipc_export_tensor", &ipcExportTensor, py::arg("tensor"));
    m.def("ipc_enable_peer_access", &ipcEnablePeerAccess,
          py::arg("num_devices"));
    m.def("ipc_open_output_handles", &ipcOpenOutputHandles,
          py::arg("handles"), py::arg("local_output"), py::arg("rank"));
    m.def("ipc_close_handles", &ipcCloseHandles, py::arg("ptrs"),
          py::arg("rank"));
    m.def("ipc_allgather_memcpy", &ipcAllgatherMemcpy, py::arg("input"),
          py::arg("out_ptrs"), py::arg("rank"), py::arg("tensor_bytes"));
    m.def("ipc_pack_output_ptrs", &ipcPackOutputPtrs, py::arg("out_ptrs"),
          py::arg("like"));
    m.def("ipc_allgather_store", &ipcAllgatherStore, py::arg("input"),
          py::arg("out_ptr_tensor"), py::arg("rank"),
          py::arg("tensor_bytes"));
    m.def("ipc_allgather_store_signal", &ipcAllgatherStoreSignal,
          py::arg("input"), py::arg("out_ptr_tensor"),
          py::arg("signal_ptr_tensor"), py::arg("rank"), py::arg("sequence"),
          py::arg("tensor_bytes"));
#endif

    py::class_<MooncakeBackend::MooncakeBackendOptions,
               c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>>(
        m, "MooncakeBackendOptions")
        .def(py::init<at::Tensor>(), py::arg("active_ranks"))
        .def(py::init<at::Tensor, bool>(), py::arg("active_ranks"),
             py::arg("is_extension"))
        .def(py::init<at::Tensor, bool, int>(), py::arg("active_ranks"),
             py::arg("is_extension"), py::arg("max_world_size"));
}

}  // namespace mooncake
