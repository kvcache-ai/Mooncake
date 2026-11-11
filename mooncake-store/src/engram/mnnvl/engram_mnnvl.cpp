#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <cuda_runtime.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>

#include "engram_local_cuda.h"
#include "engram_mapped_table.h"
#include "tent/transfer_engine.h"
#include "tent/common/config.h"

namespace py = pybind11;
using namespace mooncake::tent;
namespace {
void check(const Status& status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}
void check(cudaError_t status) {
    if (status != cudaSuccess)
        throw std::runtime_error(cudaGetErrorString(status));
}
struct DeviceGuard {
    int previous;
    explicit DeviceGuard(int device) {
        check(cudaGetDevice(&previous));
        check(cudaSetDevice(device));
    }
    ~DeviceGuard() { cudaSetDevice(previous); }
};
struct DeviceBuffer {
    void* pointer = nullptr;
    explicit DeviceBuffer(size_t size) { check(cudaMalloc(&pointer, size)); }
    ~DeviceBuffer() { cudaFree(pointer); }
};

class Session {
    struct Allocation {
        void* address;
        size_t length;
        int numa;
        bool registered = false;
    };
    int device_;
    uint64_t next_id_ = 1;
    std::unique_ptr<TransferEngine> engine_;
    std::map<uint64_t, Allocation> allocations_;
    std::map<uint64_t, std::shared_ptr<MappedMemory>> mappings_;
    std::map<std::string, SegmentID> segments_;

    void require_open() const {
        if (!engine_) throw std::runtime_error("EGM session is closed");
    }

   public:
    Session(int device, const std::string& master, const std::string& segment,
            const std::string& hostname)
        : device_(device) {
        if (!std::getenv("MC_ENABLE_MNNVL"))
            throw std::runtime_error(
                "MC_ENABLE_MNNVL=1 must be set before startup");
        DeviceGuard guard(device_);
        auto conf = std::make_shared<Config>();
        conf->set("metadata_type", "http");
        conf->set("metadata_servers", master);
        conf->set("local_segment_name", segment);
        conf->set("rpc_server_hostname", hostname);
        for (const char* name : {"tcp", "rdma", "shm", "nvlink", "io_uring",
                                 "gds", "fabric", "mpcomm"})
            conf->set(std::string("transports/") + name + "/enable", false);
        conf->set("transports/mnnvl/enable", true);
        conf->set("transports/mnnvl/egm", true);
        conf->set("transports/mnnvl/egm_strict", true);
        conf->set("transports/nvlink/handle_type", "fabric");
        engine_ = std::make_unique<TransferEngine>(conf);
        std::vector<EgmNumaInfo> nodes;
        check(engine_->getMnnvlEgmInfo(nodes));
        if (engine_->getSegmentName() != segment)
            throw std::runtime_error(
                "Environment config overrode Engram segment/session identity");
    }

    py::dict info() {
        require_open();
        DeviceGuard guard(device_);
        std::vector<EgmNumaInfo> nodes;
        check(engine_->getMnnvlEgmInfo(nodes));
        cudaDeviceProp properties;
        check(cudaGetDeviceProperties(&properties, device_));
        const auto& uuid = properties.uuid;
        static constexpr char hex[] = "0123456789abcdef";
        std::string identity;
        for (unsigned char byte : uuid.bytes) {
            identity += hex[byte >> 4];
            identity += hex[byte & 15];
        }
        py::dict result, numa;
        for (auto node : nodes)
            numa[py::int_(node.numa_node)] = node.granularity;
        result["device"] = device_;
        result["gpu_uuid"] = identity;
        result["numa"] = numa;
        std::map<int, size_t> used;
        for (const auto& [id, allocation] : allocations_)
            used[allocation.numa] += allocation.length;
        result["used_bytes"] = used;
        result["segment"] = engine_->getSegmentName();
        return result;
    }

    uint64_t allocate(size_t bytes, int numa, size_t budget) {
        require_open();
        DeviceGuard guard(device_);
        size_t used = 0;
        for (const auto& [id, allocation] : allocations_) {
            if (allocation.numa != numa) continue;
            used += allocation.length;
        }
        // Subtraction wraps when used > budget; both checks must avoid
        // wrapping.
        if (used > budget || bytes > budget - used)
            throw std::invalid_argument(
                "Multiple Stores in this session exceed total NUMA budget");
        std::vector<EgmNumaInfo> nodes;
        check(engine_->getMnnvlEgmInfo(nodes));
        auto it = std::find_if(
            nodes.begin(), nodes.end(),
            [numa](const auto& node) { return node.numa_node == numa; });
        if (it == nodes.end() || !bytes || bytes % it->granularity)
            throw std::invalid_argument(
                "EGM arena must be aligned on the current GPU's host NUMA");
        MemoryOptions options;
        options.type = MNNVL;
        options.location = "cpu:" + std::to_string(numa);
        void* address = nullptr;
        check(engine_->allocateLocalMemory(&address, bytes, options));
        auto id = next_id_++;
        allocations_.emplace(id, Allocation{address, bytes, numa});
        return id;
    }

    void write(uint64_t id, size_t offset, py::array rows) {
        require_open();
        auto& allocation = allocations_.at(id);
        if (allocation.registered ||
            !rows.dtype().is(py::dtype::of<uint8_t>()) ||
            !(rows.flags() & py::array::c_style) ||
            offset > allocation.length ||
            static_cast<size_t>(rows.nbytes()) > allocation.length - offset)
            throw std::invalid_argument(
                "Invalid or already published EGM write");
        const auto* source = static_cast<const char*>(rows.data());
        auto* target = static_cast<char*>(allocation.address) + offset;
        const size_t nbytes = static_cast<size_t>(rows.nbytes());
        py::gil_scoped_release release;
        // A HOST_NUMA allocation already grants CPU access; do not build
        // another full table copy.
        constexpr size_t chunk = 64ULL << 20;
        for (size_t done = 0; done < nbytes; done += chunk)
            std::memcpy(target + done, source + done,
                        std::min(chunk, nbytes - done));
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    py::dict publish(uint64_t id, int numa) {
        require_open();
        DeviceGuard guard(device_);
        auto& allocation = allocations_.at(id);
        if (allocation.registered)
            throw std::runtime_error("arena is already published");
        check(cudaDeviceSynchronize());
        MemoryOptions options;
        options.type = MNNVL;
        options.location = "cpu:" + std::to_string(numa);
        options.perm = kGlobalReadOnly;
        check(engine_->registerLocalMemory(allocation.address,
                                           allocation.length, options));
        allocation.registered = true;
        py::dict result;
        result["address"] = reinterpret_cast<uint64_t>(allocation.address);
        result["length"] = allocation.length;
        result["segment"] = engine_->getSegmentName();
        return result;
    }

    uint64_t acquire(const std::string& segment, uint64_t address,
                     size_t length) {
        require_open();
        DeviceGuard guard(device_);
        auto it = segments_.find(segment);
        if (it == segments_.end()) {
            SegmentID handle;
            check(engine_->openSegment(handle, segment));
            it = segments_.emplace(segment, handle).first;
        }
        std::shared_ptr<MappedMemory> mapping;
        check(engine_->acquireMnnvlMemory(it->second, address, length, device_,
                                          mapping));
        auto id = next_id_++;
        mappings_.emplace(id, std::move(mapping));
        return id;
    }

    py::capsule table(uint64_t id, size_t offset, int64_t count, size_t width) {
        require_open();
        if (count < 0 || !width ||
            static_cast<uint64_t>(count) > SIZE_MAX / width)
            throw std::invalid_argument("Invalid table layout");
        std::shared_ptr<MappedMemory> mapping;
        const uint8_t* address = nullptr;
        if (count) {
            mapping = mappings_.at(id);
            if (!mapping->address() || offset > mapping->length() ||
                static_cast<size_t>(count) * width > mapping->length() - offset)
                throw std::invalid_argument("table exceeds the arena bounds");
            address =
                reinterpret_cast<const uint8_t*>(mapping->address()) + offset;
        }
        auto* view =
            new EngramMappedTable{address, count, width, device_, mapping};
        return py::capsule(view, kEngramMappedTableCapsule, [](void* pointer) {
            delete static_cast<EngramMappedTable*>(pointer);
        });
    }

    py::bytes sample(uint64_t id, size_t offset, size_t length) {
        require_open();
        DeviceGuard guard(device_);
        const auto& mapping = mappings_.at(id);
        if (!length || length > 64 || offset > mapping->length() ||
            length > mapping->length() - offset)
            throw std::invalid_argument(
                "readback sample must be within the arena and at most 64 "
                "bytes");
        DeviceBuffer table_buffer(sizeof(EngramLocalTable));
        DeviceBuffer id_buffer(sizeof(int64_t));
        DeviceBuffer output(length);
        EngramLocalTable table{
            reinterpret_cast<const uint8_t*>(mapping->address()) + offset, 1,
            0};
        int64_t row = 0;
        check(cudaMemcpy(table_buffer.pointer, &table, sizeof(table),
                         cudaMemcpyHostToDevice));
        check(cudaMemcpy(id_buffer.pointer, &row, sizeof(row),
                         cudaMemcpyHostToDevice));
        check(engram_local_gather(
            static_cast<EngramLocalTable*>(table_buffer.pointer),
            id_buffer.pointer, true, static_cast<uint8_t*>(output.pointer), 1,
            1, length, 1, 1, length, length, nullptr));
        std::string bytes(length, '\0');
        check(cudaMemcpy(bytes.data(), output.pointer, length,
                         cudaMemcpyDeviceToHost));
        return py::bytes(bytes);
    }

    void synchronize() {
        DeviceGuard guard(device_);
        py::gil_scoped_release release;
        check(cudaDeviceSynchronize());
    }
    void release_mapping(uint64_t id) {
        DeviceGuard guard(device_);
        auto it = mappings_.find(id);
        if (it == mappings_.end()) return;
        // A table may still own the lease; its destructor performs cleanup
        // when the last reference goes away.
        if (it->second.use_count() == 1) check(it->second->release());
        mappings_.erase(it);
    }
    void free(uint64_t id) {
        require_open();
        DeviceGuard guard(device_);
        auto it = allocations_.find(id);
        if (it == allocations_.end()) return;
        if (it->second.registered) {
            check(engine_->unregisterLocalMemory(it->second.address,
                                                 it->second.length));
            it->second.registered = false;
        }
        check(engine_->freeLocalMemory(it->second.address));
        allocations_.erase(it);
    }
    void close() {
        if (!engine_) return;
        if (!allocations_.empty() || !mappings_.empty())
            throw std::runtime_error(
                "close tables/mappings and cooperatively release the arena "
                "first");
        for (auto it = segments_.begin(); it != segments_.end();) {
            check(engine_->closeSegment(it->second));
            it = segments_.erase(it);
        }
        engine_.reset();
    }
};
}  // namespace

PYBIND11_MODULE(_engram_mnnvl, m) {
    m.def("current_device", [] {
        int device;
        check(cudaGetDevice(&device));
        return device;
    });
    py::class_<Session, std::shared_ptr<Session>>(m, "Session")
        .def(py::init<int, const std::string&, const std::string&,
                      const std::string&>())
        .def("info", &Session::info)
        .def("allocate", &Session::allocate)
        .def("write", &Session::write)
        .def("publish", &Session::publish)
        .def("acquire", &Session::acquire)
        .def("table", &Session::table)
        .def("sample", &Session::sample)
        .def("synchronize", &Session::synchronize)
        .def("release_mapping", &Session::release_mapping)
        .def("free", &Session::free)
        .def("close", &Session::close);
}
