// Private CUDA implementation; the public API is mooncake.engram.EngramStore.
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "engram_local_cuda.h"

namespace py = nanobind;
namespace {
void check(cudaError_t rc) {
    if (rc != cudaSuccess) throw std::runtime_error(cudaGetErrorString(rc));
}

struct DeviceGuard {
    int previous;
    explicit DeviceGuard(int device) {
        check(cudaGetDevice(&previous));
        check(cudaSetDevice(device));
    }
    ~DeviceGuard() { cudaSetDevice(previous); }
};

void require_warmup(uintptr_t stream) {
    cudaStreamCaptureStatus status;
    check(
        cudaStreamIsCapturing(reinterpret_cast<cudaStream_t>(stream), &status));
    if (status != cudaStreamCaptureStatusNone)
        throw std::runtime_error(
            "Warm up this Engram table combination before capture");
}

struct LocalTable {
    using ByteTable = py::ndarray<py::numpy, const uint8_t, py::c_contig,
                                  py::device::cpu, py::ndim<2>>;
    ByteTable owner;
    int64_t count;
    size_t width;
    void* host;
    explicit LocalTable(py::handle array) {
        auto ndarray = py::module_::import_("numpy").attr("ndarray");
        if (!py::isinstance(array, ndarray) ||
            !py::try_cast(array, owner, false)) {
            throw std::invalid_argument("Expected contiguous uint8 table");
        }
        count = owner.shape(0);
        width = owner.shape(1);
        host = const_cast<uint8_t*>(owner.data());
        if (owner.nbytes())
            check(cudaHostRegister(
                host, owner.nbytes(),
                cudaHostRegisterMapped | cudaHostRegisterPortable));
    }
    ~LocalTable() {
        if (owner.nbytes()) cudaHostUnregister(host);
    }
};

// Validate the complete span, including tensor storage offsets and outer
// strides.
struct View {
    uintptr_t address;
    size_t span;
    std::vector<size_t> shape, strides;
    View(py::object tensor, const std::string& dtype, size_t element_size) {
        if (!py::cast<bool>(tensor.attr("is_cuda")) ||
            py::cast<std::string>(py::str(tensor.attr("dtype"))) != dtype)
            throw std::invalid_argument("Unexpected tensor device or dtype");
        shape = py::cast<std::vector<size_t>>(tensor.attr("shape"));
        strides = py::cast<std::vector<size_t>>(tensor.attr("stride")());
        address = py::cast<uintptr_t>(tensor.attr("data_ptr")());
        span = element_size;
        for (size_t i = 0; i < shape.size(); ++i) {
            if (shape[i] && strides[i] > (SIZE_MAX - span) / element_size /
                                             std::max<size_t>(1, shape[i] - 1))
                throw std::invalid_argument("Tensor span overflows");
            if (shape[i]) span += (shape[i] - 1) * strides[i] * element_size;
        }
        if (std::find(shape.begin(), shape.end(), 0) != shape.end()) span = 0;
        auto storage = tensor.attr("untyped_storage")();
        const auto base = py::cast<uintptr_t>(storage.attr("data_ptr")());
        const auto size = py::cast<size_t>(storage.attr("nbytes")());
        if (span && (address < base || address - base > size ||
                     span > size - (address - base)))
            throw std::invalid_argument("Tensor exceeds backing allocation");
    }
};

class Lookup {
    std::vector<std::shared_ptr<LocalTable>> owners_;
    EngramLocalTable* device_tables_ = nullptr;
    int device_;
    size_t width_;

   public:
    Lookup(std::vector<std::shared_ptr<LocalTable>> owners,
           const std::vector<int64_t>& offsets)
        : owners_(std::move(owners)) {
        if (owners_.empty() || owners_.size() > 65535 ||
            offsets.size() != owners_.size())
            throw std::invalid_argument("Invalid table count");
        check(cudaGetDevice(&device_));
        width_ = owners_.front()->width;
        std::vector<EngramLocalTable> tables;
        for (size_t i = 0; i < owners_.size(); ++i) {
            const auto& owner = *owners_[i];
            if (owner.width != width_ || offsets[i] < 0)
                throw std::invalid_argument("Invalid table width or offset");
            void* pointer = nullptr;
            if (owner.count)
                check(cudaHostGetDevicePointer(&pointer, owner.host, 0));
            tables.push_back({static_cast<const uint8_t*>(pointer), owner.count,
                              offsets[i]});
        }
        check(cudaMalloc(reinterpret_cast<void**>(&device_tables_),
                         tables.size() * sizeof(EngramLocalTable)));
        const auto rc = cudaMemcpy(device_tables_, tables.data(),
                                   tables.size() * sizeof(EngramLocalTable),
                                   cudaMemcpyHostToDevice);
        if (rc != cudaSuccess) {
            cudaFree(device_tables_);
            device_tables_ = nullptr;
            check(rc);
        }
    }
    ~Lookup() {
        int previous = 0;
        cudaGetDevice(&previous);
        cudaSetDevice(device_);
        cudaDeviceSynchronize();
        cudaFree(device_tables_);
        cudaSetDevice(previous);
    }
    void synchronize() {
        DeviceGuard guard(device_);
        py::gil_scoped_release release;
        check(cudaDeviceSynchronize());
    }
    void run(py::object ids, py::object output, uintptr_t stream) {
        int device;
        check(cudaGetDevice(&device));
        if (device != device_ ||
            py::cast<int>(ids.attr("device").attr("index")) != device_ ||
            !ids.attr("device").equal(output.attr("device")))
            throw std::invalid_argument(
                "IDs, output and current CUDA device must match");
        const auto dtype = py::cast<std::string>(py::str(ids.attr("dtype")));
        if (dtype != "torch.int32" && dtype != "torch.int64")
            throw std::invalid_argument("IDs must be int32 or int64");
        View input(ids, dtype, dtype == "torch.int64" ? 8 : 4);
        View out(output, "torch.uint8", 1);
        if (input.shape.size() != 2 || input.shape[1] != owners_.size() ||
            out.shape !=
                std::vector<size_t>{input.shape[0], owners_.size(), width_} ||
            out.strides[2] != 1)
            throw std::invalid_argument(
                "Expected IDs [T,K], byte output [T,K,R]");
        std::vector<std::pair<size_t, size_t>> outer{
            {out.strides[0], out.shape[0]}, {out.strides[1], out.shape[1]}};
        std::sort(outer.begin(), outer.end());
        size_t span = width_;
        for (const auto& [stride, count] : outer) {
            if (count <= 1) continue;
            if (stride < span)
                throw std::invalid_argument("Output rows overlap");
            span += stride * (count - 1);
        }
        if (input.span && out.span && input.address < out.address + out.span &&
            out.address < input.address + input.span)
            throw std::invalid_argument("IDs and output overlap");
        check(engram_local_gather(
            device_tables_, reinterpret_cast<const void*>(input.address),
            dtype == "torch.int64", reinterpret_cast<uint8_t*>(out.address),
            input.shape[0], input.shape[1], width_, input.strides[0],
            input.strides[1], out.strides[0], out.strides[1],
            reinterpret_cast<cudaStream_t>(stream)));
    }
};
}  // namespace

NB_MODULE(_engram_cuda, m) {
    m.def("require_warmup", &require_warmup);
    py::class_<LocalTable>(m, "LocalTable")
        .def(py::init<py::handle>(), py::arg("array").noconvert());
    py::class_<Lookup>(m, "Lookup")
        .def(py::init<std::vector<std::shared_ptr<LocalTable>>,
                      const std::vector<int64_t>&>())
        .def("run", &Lookup::run)
        .def("synchronize", &Lookup::synchronize);
}
