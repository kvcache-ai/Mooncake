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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "tent/transfer_engine.h"

namespace py = pybind11;
using namespace mooncake::tent;

// =============================================================================
// Custom Exception Hierarchy
// =============================================================================

class TentException : public std::exception {
   public:
    explicit TentException(const std::string& msg) : msg_(msg) {}
    const char* what() const noexcept override { return msg_.c_str(); }

   protected:
    std::string msg_;
};

#define DEFINE_TENT_EXCEPTION(cls)                                          \
    class cls##Error : public TentException {                               \
       public:                                                              \
        explicit cls##Error(const std::string& msg) : TentException(msg) {} \
    }

DEFINE_TENT_EXCEPTION(InvalidArgument);
DEFINE_TENT_EXCEPTION(AddressNotRegistered);
DEFINE_TENT_EXCEPTION(DeviceNotFound);
DEFINE_TENT_EXCEPTION(InvalidEntry);
DEFINE_TENT_EXCEPTION(Rdma);
DEFINE_TENT_EXCEPTION(Cuda);
DEFINE_TENT_EXCEPTION(Metadata);
DEFINE_TENT_EXCEPTION(RpcService);
DEFINE_TENT_EXCEPTION(Internal);
DEFINE_TENT_EXCEPTION(NotImplemented);

#undef DEFINE_TENT_EXCEPTION

// =============================================================================
// Error Handling - Map Status::Code to Exception Types
// =============================================================================

static void ThrowStatus(const Status& s, const char* where) {
    if (s.ok()) return;

    std::string full_msg = std::string(where) + ": " + s.ToString();

    switch (s.code()) {
        case Status::Code::kInvalidArgument:
            throw InvalidArgumentError(full_msg);
        case Status::Code::kAddressNotRegistered:
            throw AddressNotRegisteredError(full_msg);
        case Status::Code::kDeviceNotFound:
            throw DeviceNotFoundError(full_msg);
        case Status::Code::kInvalidEntry:
            throw InvalidEntryError(full_msg);
        case Status::Code::kRdmaError:
            throw RdmaError(full_msg);
        case Status::Code::kCudaError:
            throw CudaError(full_msg);
        case Status::Code::kMetadataError:
            throw MetadataError(full_msg);
        case Status::Code::kRpcServiceError:
            throw RpcServiceError(full_msg);
        case Status::Code::kNotImplemented:
            throw NotImplementedError(full_msg);
        default:
            throw InternalError(full_msg);
    }
}

// =============================================================================
// Pointer Conversion Utilities
// =============================================================================

static inline void* U64ToPtr(uint64_t a) {
    return reinterpret_cast<void*>(static_cast<std::uintptr_t>(a));
}

static inline uint64_t PtrToU64(void* p) {
    return static_cast<uint64_t>(reinterpret_cast<std::uintptr_t>(p));
}

static inline std::vector<void*> U64VectorToPtrVector(
    const std::vector<uint64_t>& addr_list) {
    std::vector<void*> ptrs;
    ptrs.reserve(addr_list.size());
    for (auto a : addr_list) ptrs.push_back(U64ToPtr(a));
    return ptrs;
}

// =============================================================================
// RAII Wrappers
// =============================================================================

class MemoryGuard {
   public:
    MemoryGuard(TransferEngine* engine, void* ptr, size_t size)
        : engine_(engine), ptr_(ptr), size_(size), released_(false) {}

    ~MemoryGuard() {
        if (!released_ && ptr_ != nullptr) {
            engine_->freeLocalMemory(ptr_);
        }
    }

    // Prevent copying
    MemoryGuard(const MemoryGuard&) = delete;
    MemoryGuard& operator=(const MemoryGuard&) = delete;

    // Allow moving
    MemoryGuard(MemoryGuard&& other) noexcept
        : engine_(other.engine_),
          ptr_(other.ptr_),
          size_(other.size_),
          released_(other.released_) {
        other.released_ = true;
        other.ptr_ = nullptr;
    }

    MemoryGuard& operator=(MemoryGuard&& other) noexcept {
        if (this != &other) {
            if (!released_ && ptr_ != nullptr) {
                engine_->freeLocalMemory(ptr_);
            }
            engine_ = other.engine_;
            ptr_ = other.ptr_;
            size_ = other.size_;
            released_ = other.released_;
            other.released_ = true;
            other.ptr_ = nullptr;
        }
        return *this;
    }

    void* get() const { return ptr_; }
    uint64_t address() const { return PtrToU64(ptr_); }
    size_t size() const { return size_; }

    void release() {
        if (!released_ && ptr_ != nullptr) {
            engine_->freeLocalMemory(ptr_);
            released_ = true;
            ptr_ = nullptr;
        }
    }

   private:
    TransferEngine* engine_;
    void* ptr_;
    size_t size_;
    bool released_;
};

class BatchGuard {
   public:
    BatchGuard(TransferEngine* engine, uint64_t batch_id)
        : engine_(engine), batch_id_(batch_id), released_(false) {}

    ~BatchGuard() {
        if (!released_) {
            engine_->freeBatch(static_cast<BatchID>(batch_id_));
        }
    }

    // Prevent copying
    BatchGuard(const BatchGuard&) = delete;
    BatchGuard& operator=(const BatchGuard&) = delete;

    // Allow moving
    BatchGuard(BatchGuard&& other) noexcept
        : engine_(other.engine_),
          batch_id_(other.batch_id_),
          released_(other.released_) {
        other.released_ = true;
    }

    BatchGuard& operator=(BatchGuard&& other) noexcept {
        if (this != &other) {
            if (!released_) {
                engine_->freeBatch(static_cast<BatchID>(batch_id_));
            }
            engine_ = other.engine_;
            batch_id_ = other.batch_id_;
            released_ = other.released_;
            other.released_ = true;
        }
        return *this;
    }

    uint64_t id() const { return batch_id_; }

    void release() {
        if (!released_) {
            engine_->freeBatch(static_cast<BatchID>(batch_id_));
            released_ = true;
        }
    }

   private:
    TransferEngine* engine_;
    uint64_t batch_id_;
    bool released_;
};

// =============================================================================
// Python Module Definition
// =============================================================================

PYBIND11_MODULE(tent, m) {
    m.doc() = "TENT Python bindings for Mooncake Transfer Engine";

    // -------------------------------------------------------------------------
    // Register Exception Classes
    // -------------------------------------------------------------------------
    static py::exception<TentException> exc_tent(m, "TentException");
    py::register_exception<InvalidArgumentError>(m, "InvalidArgumentError",
                                                 exc_tent);
    py::register_exception<AddressNotRegisteredError>(
        m, "AddressNotRegisteredError", exc_tent);
    py::register_exception<DeviceNotFoundError>(m, "DeviceNotFoundError",
                                                exc_tent);
    py::register_exception<InvalidEntryError>(m, "InvalidEntryError", exc_tent);
    py::register_exception<RdmaError>(m, "RdmaError", exc_tent);
    py::register_exception<CudaError>(m, "CudaError", exc_tent);
    py::register_exception<MetadataError>(m, "MetadataError", exc_tent);
    py::register_exception<RpcServiceError>(m, "RpcServiceError", exc_tent);
    py::register_exception<InternalError>(m, "InternalError", exc_tent);
    py::register_exception<NotImplementedError>(m, "NotImplementedError",
                                                exc_tent);

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------
    m.attr("LOCAL_SEGMENT_ID") = py::int_(LOCAL_SEGMENT_ID);
    m.attr("kWildcardLocation") = py::str(kWildcardLocation);

    // -------------------------------------------------------------------------
    // Enums
    // -------------------------------------------------------------------------
    py::enum_<Request::OpCode>(m, "OpCode")
        .value("READ", Request::OpCode::READ)
        .value("WRITE", Request::OpCode::WRITE)
        .export_values();

    py::enum_<TransferStatusEnum>(m, "TransferStatusEnum")
        .value("INITIAL", TransferStatusEnum::INITIAL)
        .value("PENDING", TransferStatusEnum::PENDING)
        .value("INVALID", TransferStatusEnum::INVALID)
        .value("CANCELED", TransferStatusEnum::CANCELED)
        .value("COMPLETED", TransferStatusEnum::COMPLETED)
        .value("TIMEOUT", TransferStatusEnum::TIMEOUT)
        .value("FAILED", TransferStatusEnum::FAILED)
        .export_values();

    py::enum_<Permission>(m, "Permission")
        .value("LocalReadWrite", Permission::kLocalReadWrite)
        .value("GlobalReadOnly", Permission::kGlobalReadOnly)
        .value("GlobalReadWrite", Permission::kGlobalReadWrite)
        .export_values();

    py::enum_<TransportType>(m, "TransportType")
        .value("RDMA", TransportType::RDMA)
        .value("MNNVL", TransportType::MNNVL)
        .value("SHM", TransportType::SHM)
        .value("NVLINK", TransportType::NVLINK)
        .value("GDS", TransportType::GDS)
        .value("IOURING", TransportType::IOURING)
        .value("TCP", TransportType::TCP)
        .value("AscendDirect", TransportType::AscendDirect)
        .value("UNSPEC", TransportType::UNSPEC)
        .export_values();

    py::enum_<SegmentInfo::Type>(m, "SegmentInfoType")
        .value("Memory", SegmentInfo::Type::Memory)
        .value("File", SegmentInfo::Type::File)
        .export_values();

    // -------------------------------------------------------------------------
    // Data Structures
    // -------------------------------------------------------------------------
    py::class_<Notification>(m, "Notification")
        .def(py::init<>())
        .def(py::init<const std::string&, const std::string&>(),
             py::arg("name"), py::arg("msg"))
        .def_readwrite("name", &Notification::name)
        .def_readwrite("msg", &Notification::msg);

    py::class_<Request>(m, "Request")
        .def(py::init<>())
        .def(py::init([](Request::OpCode opcode, uint64_t source,
                         uint64_t target_id, uint64_t target_offset,
                         size_t length) {
                 Request r;
                 r.opcode = opcode;
                 r.source = U64ToPtr(source);
                 r.target_id = target_id;
                 r.target_offset = target_offset;
                 r.length = length;
                 return r;
             }),
             py::arg("opcode"), py::arg("source"), py::arg("target_id"),
             py::arg("target_offset"), py::arg("length"))
        .def_property(
            "opcode", [](const Request& r) { return r.opcode; },
            [](Request& r, Request::OpCode op) { r.opcode = op; })
        .def_property(
            "source", [](const Request& r) { return PtrToU64(r.source); },
            [](Request& r, uint64_t addr) { r.source = U64ToPtr(addr); })
        .def_readwrite("target_id", &Request::target_id)
        .def_readwrite("target_offset", &Request::target_offset)
        .def_readwrite("length", &Request::length);

    py::class_<TransferStatus>(m, "TransferStatus")
        .def(py::init<>())
        .def_readwrite("state", &TransferStatus::s)
        .def_readwrite("bytes", &TransferStatus::transferred_bytes);

    py::class_<SegmentInfo::Buffer>(m, "SegmentInfoBuffer")
        .def(py::init<>())
        .def_readwrite("base", &SegmentInfo::Buffer::base)
        .def_readwrite("length", &SegmentInfo::Buffer::length)
        .def_readwrite("location", &SegmentInfo::Buffer::location);

    py::class_<SegmentInfo>(m, "SegmentInfo")
        .def(py::init<>())
        .def_readwrite("type", &SegmentInfo::type)
        .def_readwrite("buffers", &SegmentInfo::buffers);

    py::class_<MemoryOptions>(m, "MemoryOptions")
        .def(py::init<>())
        .def_readwrite("location", &MemoryOptions::location)
        .def_readwrite("perm", &MemoryOptions::perm)
        .def_readwrite("type", &MemoryOptions::type)
        .def_readwrite("shm_path", &MemoryOptions::shm_path)
        .def_readwrite("shm_offset", &MemoryOptions::shm_offset)
        .def_readwrite("internal", &MemoryOptions::internal);

    // -------------------------------------------------------------------------
    // RAII Wrapper Classes
    // -------------------------------------------------------------------------
    py::class_<MemoryGuard>(m, "MemoryGuard")
        .def("address", &MemoryGuard::address,
             "Get the memory address as uint64_t")
        .def("size", &MemoryGuard::size, "Get the memory size")
        .def("__enter__", &MemoryGuard::address,
             "Enter context manager, returns address")
        .def("__exit__", [](MemoryGuard& self, py::args) {
            py::gil_scoped_release release;
            self.release();
            return py::none();
        });

    py::class_<BatchGuard>(m, "BatchGuard")
        .def("id", &BatchGuard::id, "Get the batch ID")
        .def("release", &BatchGuard::release,
             "Explicitly free the batch before destructor")
        .def("__enter__", &BatchGuard::id,
             "Enter context manager, returns batch ID")
        .def("__exit__", [](BatchGuard& self, py::args) {
            py::gil_scoped_release release;
            self.release();
            return py::none();
        });

    // -------------------------------------------------------------------------
    // TransferEngine Class
    // -------------------------------------------------------------------------
    py::class_<TransferEngine>(m, "TransferEngine")
        // ctors
        .def(py::init<>())
        .def(py::init<const std::string>(), py::arg("config_path"))

        .def("available", &TransferEngine::available)

        .def("get_segment_name", &TransferEngine::getSegmentName)
        .def("get_rpc_server_address", &TransferEngine::getRpcServerAddress)
        .def("get_rpc_server_port", &TransferEngine::getRpcServerPort)

        // ---------------------------------------------------------------------
        // export/import: out param -> return
        // ---------------------------------------------------------------------
        .def("export_local_segment",
             [](TransferEngine& self) -> std::string {
                 py::gil_scoped_release release;
                 std::string shared_handle;
                 auto s = self.exportLocalSegment(shared_handle);
                 ThrowStatus(s, "export_local_segment");
                 return shared_handle;
             })

        .def(
            "import_remote_segment",
            [](TransferEngine& self,
               const std::string& shared_handle) -> uint64_t {
                py::gil_scoped_release release;
                SegmentID handle = 0;
                auto s = self.importRemoteSegment(handle, shared_handle);
                ThrowStatus(s, "import_remote_segment");
                return (uint64_t)handle;
            },
            py::arg("shared_handle"))

        // ---------------------------------------------------------------------
        // open/close/get_segment_info
        // ---------------------------------------------------------------------
        .def(
            "open_segment",
            [](TransferEngine& self,
               const std::string& segment_name) -> uint64_t {
                py::gil_scoped_release release;
                SegmentID handle = 0;
                auto s = self.openSegment(handle, segment_name);
                ThrowStatus(s, "open_segment");
                return (uint64_t)handle;
            },
            py::arg("segment_name"))

        .def(
            "close_segment",
            [](TransferEngine& self, uint64_t handle) {
                py::gil_scoped_release release;
                auto s = self.closeSegment((SegmentID)handle);
                ThrowStatus(s, "close_segment");
            },
            py::arg("handle"))

        .def(
            "get_segment_info",
            [](TransferEngine& self, uint64_t handle) -> SegmentInfo {
                py::gil_scoped_release release;
                SegmentInfo info;
                auto s = self.getSegmentInfo((SegmentID)handle, info);
                ThrowStatus(s, "get_segment_info");
                return info;
            },
            py::arg("handle"))

        // ---------------------------------------------------------------------
        // allocate/free memory (basic)
        // ---------------------------------------------------------------------
        .def(
            "allocate_local_memory",
            [](TransferEngine& self, size_t size,
               const Location& location) -> uint64_t {
                py::gil_scoped_release release;
                void* addr = nullptr;
                auto s = self.allocateLocalMemory(&addr, size, location);
                ThrowStatus(s, "allocate_local_memory");
                return PtrToU64(addr);
            },
            py::arg("size"), py::arg("location") = kWildcardLocation)

        // allocate/free memory (advanced)
        .def(
            "allocate_local_memory_ex",
            [](TransferEngine& self, size_t size,
               MemoryOptions& options) -> uint64_t {
                py::gil_scoped_release release;
                void* addr = nullptr;
                auto s = self.allocateLocalMemory(&addr, size, options);
                ThrowStatus(s, "allocate_local_memory_ex");
                return PtrToU64(addr);
            },
            py::arg("size"), py::arg("options"))

        .def(
            "free_local_memory",
            [](TransferEngine& self, uint64_t addr) {
                py::gil_scoped_release release;
                auto s = self.freeLocalMemory(U64ToPtr(addr));
                ThrowStatus(s, "free_local_memory");
            },
            py::arg("addr"))

        // ---------------------------------------------------------------------
        // RAII Memory Allocation (Context Manager)
        // ---------------------------------------------------------------------
        .def(
            "allocate_memory_guard",
            [](TransferEngine& self, size_t size,
               const Location& location) -> std::unique_ptr<MemoryGuard> {
                py::gil_scoped_release release;
                void* addr = nullptr;
                auto s = self.allocateLocalMemory(&addr, size, location);
                ThrowStatus(s, "allocate_memory_guard");
                return std::make_unique<MemoryGuard>(&self, addr, size);
            },
            py::arg("size"), py::arg("location") = kWildcardLocation)

        .def(
            "allocate_memory_guard_ex",
            [](TransferEngine& self, size_t size,
               MemoryOptions& options) -> std::unique_ptr<MemoryGuard> {
                py::gil_scoped_release release;
                void* addr = nullptr;
                auto s = self.allocateLocalMemory(&addr, size, options);
                ThrowStatus(s, "allocate_memory_guard_ex");
                return std::make_unique<MemoryGuard>(&self, addr, size);
            },
            py::arg("size"), py::arg("options"))

        // ---------------------------------------------------------------------
        // register/unregister single
        // ---------------------------------------------------------------------
        .def(
            "register_local_memory",
            [](TransferEngine& self, uint64_t addr, size_t size,
               Permission permission) {
                py::gil_scoped_release release;
                auto s =
                    self.registerLocalMemory(U64ToPtr(addr), size, permission);
                ThrowStatus(s, "register_local_memory");
            },
            py::arg("addr"), py::arg("size"),
            py::arg("permission") = Permission::kGlobalReadWrite)

        .def(
            "unregister_local_memory",
            [](TransferEngine& self, uint64_t addr, size_t size) {
                py::gil_scoped_release release;
                auto s = self.unregisterLocalMemory(U64ToPtr(addr), size);
                ThrowStatus(s, "unregister_local_memory");
            },
            py::arg("addr"), py::arg("size") = 0)

        // ---------------------------------------------------------------------
        // register/unregister batch (with helper function)
        // ---------------------------------------------------------------------
        .def(
            "register_local_memory_batch",
            [](TransferEngine& self, const std::vector<uint64_t>& addr_list,
               const std::vector<size_t>& size_list, Permission permission) {
                py::gil_scoped_release release;
                auto ptrs = U64VectorToPtrVector(addr_list);
                auto s = self.registerLocalMemory(ptrs, size_list, permission);
                ThrowStatus(s, "register_local_memory_batch");
            },
            py::arg("addr_list"), py::arg("size_list"),
            py::arg("permission") = Permission::kGlobalReadWrite)

        .def(
            "unregister_local_memory_batch",
            [](TransferEngine& self, const std::vector<uint64_t>& addr_list,
               const std::vector<size_t>& size_list) {
                py::gil_scoped_release release;
                auto ptrs = U64VectorToPtrVector(addr_list);
                auto s = self.unregisterLocalMemory(ptrs, size_list);
                ThrowStatus(s, "unregister_local_memory_batch");
            },
            py::arg("addr_list"), py::arg("size_list") = std::vector<size_t>{})

        // ---------------------------------------------------------------------
        // register memory (advanced options)
        // ---------------------------------------------------------------------
        .def(
            "register_local_memory_ex",
            [](TransferEngine& self, uint64_t addr, size_t size,
               MemoryOptions& options) {
                py::gil_scoped_release release;
                auto s =
                    self.registerLocalMemory(U64ToPtr(addr), size, options);
                ThrowStatus(s, "register_local_memory_ex");
            },
            py::arg("addr"), py::arg("size"), py::arg("options"))

        .def(
            "register_local_memory_batch_ex",
            [](TransferEngine& self, const std::vector<uint64_t>& addr_list,
               const std::vector<size_t>& size_list, MemoryOptions& options) {
                py::gil_scoped_release release;
                auto ptrs = U64VectorToPtrVector(addr_list);
                auto s = self.registerLocalMemory(ptrs, size_list, options);
                ThrowStatus(s, "register_local_memory_batch_ex");
            },
            py::arg("addr_list"), py::arg("size_list"), py::arg("options"))

        // ---------------------------------------------------------------------
        // batch operations
        // ---------------------------------------------------------------------
        .def("allocate_transfer_batch", &TransferEngine::allocateBatch,
             py::arg("batch_size"))

        .def(
            "free_transfer_batch",
            [](TransferEngine& self, uint64_t batch_id) {
                py::gil_scoped_release release;
                auto s = self.freeBatch((BatchID)batch_id);
                ThrowStatus(s, "free_transfer_batch");
            },
            py::arg("batch_id"))

        // RAII Batch Allocation
        .def(
            "allocate_batch_guard",
            [](TransferEngine& self,
               size_t batch_size) -> std::unique_ptr<BatchGuard> {
                py::gil_scoped_release release;
                auto batch_id = self.allocateBatch(batch_size);
                if (batch_id == 0) {
                    throw InternalError(
                        "allocate_batch_guard: failed to allocate batch");
                }
                return std::make_unique<BatchGuard>(&self, batch_id);
            },
            py::arg("batch_size"))

        // ---------------------------------------------------------------------
        // submitTransfer overloads
        // ---------------------------------------------------------------------
        .def(
            "submit_transfer",
            [](TransferEngine& self, uint64_t batch_id,
               const std::vector<Request>& request_list) {
                py::gil_scoped_release release;
                auto s = self.submitTransfer((BatchID)batch_id, request_list);
                ThrowStatus(s, "submit_transfer");
            },
            py::arg("batch_id"), py::arg("request_list"))

        .def(
            "submit_transfer",
            [](TransferEngine& self, uint64_t batch_id,
               const std::vector<Request>& request_list,
               const Notification& notifi) {
                py::gil_scoped_release release;
                auto s = self.submitTransfer((BatchID)batch_id, request_list,
                                             notifi);
                ThrowStatus(s, "submit_transfer");
            },
            py::arg("batch_id"), py::arg("request_list"), py::arg("notifi"))

        // C-style sugar
        .def(
            "submit_transfer_notif",
            [](TransferEngine& self, uint64_t batch_id,
               const std::vector<Request>& request_list,
               const std::string& name, const std::string& message) {
                py::gil_scoped_release release;
                Notification n{name, message};
                auto s =
                    self.submitTransfer((BatchID)batch_id, request_list, n);
                ThrowStatus(s, "submit_transfer_notif");
            },
            py::arg("batch_id"), py::arg("request_list"), py::arg("name"),
            py::arg("message"))

        // ---------------------------------------------------------------------
        // notification send/receive
        // ---------------------------------------------------------------------
        .def(
            "send_notifi",
            [](TransferEngine& self, uint64_t target_id,
               const Notification& notifi) {
                py::gil_scoped_release release;
                auto s = self.sendNotification((SegmentID)target_id, notifi);
                ThrowStatus(s, "send_notifi");
            },
            py::arg("target_id"), py::arg("notifi"))

        .def("recv_notifi",
             [](TransferEngine& self) -> std::vector<Notification> {
                 py::gil_scoped_release release;
                 std::vector<Notification> notifi_list;
                 auto s = self.receiveNotification(notifi_list);
                 ThrowStatus(s, "recv_notifi");
                 return notifi_list;
             })

        // ---------------------------------------------------------------------
        // status queries
        // ---------------------------------------------------------------------
        .def(
            "get_transfer_status",
            [](TransferEngine& self, uint64_t batch_id,
               size_t task_id) -> TransferStatus {
                py::gil_scoped_release release;
                TransferStatus st;
                auto s = self.getTransferStatus((BatchID)batch_id, task_id, st);
                ThrowStatus(s, "get_transfer_status");
                return st;
            },
            py::arg("batch_id"), py::arg("task_id"))

        .def(
            "get_transfer_status_list",
            [](TransferEngine& self,
               uint64_t batch_id) -> std::vector<TransferStatus> {
                py::gil_scoped_release release;
                std::vector<TransferStatus> status_list;
                auto s = self.getTransferStatus((BatchID)batch_id, status_list);
                ThrowStatus(s, "get_transfer_status_list");
                return status_list;
            },
            py::arg("batch_id"))

        .def(
            "get_transfer_status_overall",
            [](TransferEngine& self, uint64_t batch_id) -> TransferStatus {
                py::gil_scoped_release release;
                TransferStatus overall;
                auto s = self.getTransferStatus((BatchID)batch_id, overall);
                ThrowStatus(s, "get_transfer_status_overall");
                return overall;
            },
            py::arg("batch_id"));
}
