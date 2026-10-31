// Host adaptation only. READ, issue scheduling, QPs and completion use the
// unmodified DeepEP Engram kernels from MOONCAKE_DEEPEP_ROOT.
// Host launch adaptation derived from DeepEP, Copyright (c) 2025 DeepSeek.
// See DEEP_EP_LICENSE (MIT) distributed alongside this source.
#include <torch/extension.h>
#include <csrc/kernels/comm/api.hpp>
#include <csrc/kernels/engram/engram.hpp>
#include <deep_ep/layout/engram/workspace.cuh>

namespace py = pybind11;

class SharedStorage {
    CUdeviceptr address_ = 0;
    CUmemGenericAllocationHandle handle_ = {};
    int fd_ = -1;
    bool mapped_ = false;

    void release() noexcept {
        using namespace deep_ep;
        if (mapped_) lazy_cuMemUnmap(address_, size);
        if (address_) lazy_cuMemAddressFree(address_, size);
        if (handle_) lazy_cuMemRelease(handle_);
        if (fd_ >= 0) ::close(fd_);
        address_ = 0;
        handle_ = {};
        fd_ = -1;
        mapped_ = false;
    }

   public:
    int64_t size;
    SharedStorage(int64_t bytes, int import_fd) {
        using namespace deep_ep;
        TORCH_CHECK(bytes > 0);
        size = (bytes + kNumAllocationAlignmentBytes - 1) /
               kNumAllocationAlignmentBytes * kNumAllocationAlignmentBytes;
        try {
            symmetric::DeviceContext device;
            if (import_fd < 0) {
                auto prop = device.cpu_alloc_prop(true);
                symmetric::cumem_create_with_fallback(&handle_, size, &prop);
                CUDA_DRIVER_CHECK(lazy_cuMemExportToShareableHandle(
                    &fd_, handle_, CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                    0));
            } else {
                CUDA_DRIVER_CHECK(lazy_cuMemImportFromShareableHandle(
                    &handle_,
                    reinterpret_cast<void*>(static_cast<uintptr_t>(import_fd)),
                    CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR));
                fd_ = ::dup(import_fd);
                TORCH_CHECK(fd_ >= 0,
                            "Cannot duplicate Engram CPU allocation FD");
            }
            CUDA_DRIVER_CHECK(lazy_cuMemAddressReserve(
                &address_, size, kNumAllocationAlignmentBytes, 0, 0));
            CUDA_DRIVER_CHECK(lazy_cuMemMap(address_, size, 0, handle_, 0));
            mapped_ = true;
            symmetric::set_access(address_, size, device.device_idx,
                                  device.numa_idx);
        } catch (...) {
            release();
            throw;
        }
    }
    void* data() const { return reinterpret_cast<void*>(address_); }
    int fd() const { return fd_; }
    torch::Tensor cpu_view() const {
        return torch::from_blob(data(), {size},
                                torch::TensorOptions().dtype(torch::kByte));
    }
    ~SharedStorage() { release(); }
};

class EngramReader {
    std::shared_ptr<deep_ep::comm::Context> ctx_;
    std::shared_ptr<SharedStorage> storage_;
    int rows_, width_, capacity_, depth_, rank_, device_;

   public:
    EngramReader(int64_t comm, int rank, int world,
                 std::shared_ptr<SharedStorage> storage, int rows, int width,
                 int capacity, int qps, int depth, int timeout)
        : storage_(std::move(storage)),
          rows_(rows),
          width_(width),
          capacity_(capacity),
          depth_(depth),
          rank_(rank) {
        TORCH_CHECK(rows > 0 && width > 0 && width % 8 == 0 && capacity > 0);
        TORCH_CHECK(storage_ && storage_->size >= int64_t(rows) * width);
        TORCH_CHECK(depth > 256 && world * qps <= 2048);
        CUDA_RUNTIME_CHECK(cudaGetDevice(&device_));
        const auto align = [](int64_t n) {
            constexpr int64_t a = deep_ep::kNumAllocationAlignmentBytes;
            return (n + a - 1) / a * a;
        };
        setenv("NCCL_ELASTIC_BUFFER_REGISTER", "1", 0);
        ctx_ = std::make_shared<deep_ep::comm::Context>(
            comm, deep_ep::symmetric::shared_comm_t{}, world, rank,
            align(sizeof(deep_ep::layout::EngramWorkspace)),
            align(int64_t(capacity) * width), 0, true, false, std::nullopt, qps,
            depth, timeout, timeout);
        try {
            TORCH_CHECK(
                ctx_->gin_min_stride == 1,
                "Engram requires one caller per node in each rail group");
            ctx_->set_barrier_signals(
                &static_cast<deep_ep::layout::EngramWorkspace*>(ctx_->workspace)
                     ->barrier);
            void* mapped = storage_->data();
            ctx_->local_rdma_storage_ptr = mapped;
            ctx_->num_rdma_storage_bytes = storage_->size;
            ncclWindow_t storage_window = nullptr;
            NCCL_CHECK(ncclCommWindowRegister(ctx_->comm, mapped,
                                              storage_->size, &storage_window,
                                              NCCL_WIN_GIN_ONLY));
            ctx_->rdma_storage_windows.push_back(storage_window);
            CUDA_RUNTIME_CHECK(cudaMalloc(
                reinterpret_cast<void**>(&ctx_->rdma_storage_windows_gpu),
                sizeof(ncclWindow_t)));
            CUDA_RUNTIME_CHECK(cudaMemcpy(ctx_->rdma_storage_windows_gpu,
                                          ctx_->rdma_storage_windows.data(),
                                          sizeof(ncclWindow_t),
                                          cudaMemcpyHostToDevice));
        } catch (...) {
            try {
                close();
            } catch (...) {
            }
            throw;
        }
    }

    uintptr_t local_address() const {
        TORCH_CHECK(ctx_, "Engram reader is closed");
        return reinterpret_cast<uintptr_t>(ctx_->local_rdma_storage_ptr);
    }

    torch::Tensor fetch(torch::Tensor indices, int qps) {
        TORCH_CHECK(ctx_, "Engram reader is closed");
        TORCH_CHECK(indices.is_cuda() && indices.get_device() == device_ &&
                    indices.is_contiguous() &&
                    indices.scalar_type() == torch::kInt &&
                    indices.dim() == 2 && indices.numel() <= capacity_);
        TORCH_CHECK(qps > 0 && qps <= ctx_->num_allocated_qps);
        int tokens = indices.size(0), entries = indices.size(1);
        TORCH_CHECK(tokens > 0 && entries > 0);
        auto requests =
            torch::empty({ctx_->num_ranks * qps, int(sizeof(ncclGinRequest_t))},
                         indices.options().dtype(torch::kByte));
        auto stream = at::cuda::getCurrentCUDAStream();
        deep_ep::engram::launch_engram_fetch(
            ctx_->dev_comm, ctx_->window, ctx_->rdma_storage_windows_gpu,
            ctx_->buffer, ctx_->workspace, indices.data_ptr<int>(),
            reinterpret_cast<ncclGinRequest_t*>(requests.data_ptr()), nullptr,
            nullptr, 0, 0, 0, ctx_->num_nvl_ranks, {rows_}, width_ / 2, 2, 0,
            entries, tokens, capacity_ / entries, ctx_->num_ranks, 1, qps,
            depth_ - 256, ctx_->num_gpu_timeout_cycles, stream);
        deep_ep::engram::launch_engram_fetch_wait(
            reinterpret_cast<ncclGinRequest_t*>(requests.data_ptr()),
            ctx_->dev_comm, ctx_->window, ctx_->num_ranks,
            std::min(qps, (tokens + 31) / 32), stream);
        return torch::from_blob(ctx_->buffer, {indices.numel(), width_},
                                indices.options().dtype(torch::kByte));
    }

    void close() {
        if (!ctx_) return;
        CUDA_RUNTIME_CHECK(cudaSetDevice(device_));
        CUDA_RUNTIME_CHECK(cudaDeviceSynchronize());
        ctx_->finalize();
        ctx_.reset();
        storage_.reset();
    }
    ~EngramReader() {
        try {
            close();
        } catch (...) {
        }
    }
};

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("init_jit", &deep_ep::init_jit);
    m.def("unique_id", &deep_ep::comm::get_local_unique_id);
    m.def("connect", [](const py::bytearray& value, int world, int rank) {
        // Conversion needs the GIL; NCCL bootstrap must release it so this
        // process's local FD server can serve the other GPU callers.
        const auto bytes = value.cast<std::string>();
        TORCH_CHECK(bytes.size() == sizeof(ncclUniqueId));
        ncclUniqueId id;
        std::memcpy(&id, bytes.data(), sizeof(id));
        py::gil_scoped_release release;
        ncclComm_t comm;
        NCCL_CHECK(ncclCommInitRank(&comm, world, id, rank));
        return reinterpret_cast<int64_t>(comm);
    });
    m.def("disconnect", &deep_ep::comm::destroy_nccl_comm,
          py::call_guard<py::gil_scoped_release>());
    py::class_<SharedStorage, std::shared_ptr<SharedStorage>>(m,
                                                              "SharedStorage")
        .def(py::init<int64_t, int>())
        .def("cpu_view", &SharedStorage::cpu_view)
        .def("fd", &SharedStorage::fd)
        .def_readonly("size", &SharedStorage::size);
    py::class_<EngramReader>(m, "Reader")
        .def(py::init<int64_t, int, int, std::shared_ptr<SharedStorage>, int,
                      int, int, int, int, int>(),
             py::call_guard<py::gil_scoped_release>())
        .def("local_address", &EngramReader::local_address)
        .def("fetch", &EngramReader::fetch)
        .def("close", &EngramReader::close);
}
