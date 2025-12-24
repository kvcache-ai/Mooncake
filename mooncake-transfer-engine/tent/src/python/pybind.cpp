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
#include <stdexcept>
#include <string>
#include <vector>

#include "tent/transfer_engine.h"

namespace py = pybind11;
using namespace mooncake::tent;

static inline void ThrowIfNotOk(const Status& s, const char* where) {
    if (s.ok()) return;
    throw std::runtime_error(std::string(where) + ": " + s.ToString());
}

static inline void ThrowIfNotOkReacquireGil(const Status& s,
                                            const char* where) {
    if (s.ok()) return;
    py::gil_scoped_acquire acquire;
    throw std::runtime_error(std::string(where) + ": " + s.ToString());
}

static inline void* U64ToPtr(uint64_t a) {
    return reinterpret_cast<void*>(static_cast<std::uintptr_t>(a));
}
static inline uint64_t PtrToU64(void* p) {
    return static_cast<uint64_t>(reinterpret_cast<std::uintptr_t>(p));
}

PYBIND11_MODULE(tent, m) {
    m.doc() = "TENT Python bindings (direct translation of C++ TransferEngine)";

    // ---------------------------------------------------------------------
    // constants
    // ---------------------------------------------------------------------
    m.attr("LOCAL_SEGMENT_ID") = py::int_(LOCAL_SEGMENT_ID);
    m.attr("kWildcardLocation") = py::str(kWildcardLocation);

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

    auto recv_notifi = [](TransferEngine& self) -> std::vector<Notification> {
        py::gil_scoped_release release;
        std::vector<Notification> notifi_list;
        auto s = self.receiveNotification(notifi_list);
        ThrowIfNotOkReacquireGil(s, "recv_notifi");
        return notifi_list;
    };

    py::class_<TransferEngine>(m, "TransferEngine")
        // ctors
        .def(py::init<>())
        .def(py::init<const std::string>(), py::arg("config_path"))

        .def("available", &TransferEngine::available)

        .def("get_segment_name", &TransferEngine::getSegmentName)
        .def("get_rpc_server_address", &TransferEngine::getRpcServerAddress)
        .def("get_rpc_server_port", &TransferEngine::getRpcServerPort)

        // export/import: out param -> return
        .def("export_local_segment",
             [](TransferEngine& self) -> std::string {
                 py::gil_scoped_release release;
                 std::string shared_handle;
                 auto s = self.exportLocalSegment(shared_handle);
                 ThrowIfNotOkReacquireGil(s, "export_local_segment");
                 return shared_handle;
             })

        .def(
            "import_remote_segment",
            [](TransferEngine& self,
               const std::string& shared_handle) -> uint64_t {
                py::gil_scoped_release release;
                SegmentID handle = 0;
                auto s = self.importRemoteSegment(handle, shared_handle);
                ThrowIfNotOkReacquireGil(s, "import_remote_segment");
                return (uint64_t)handle;
            },
            py::arg("shared_handle"))

        // open/close/get_segment_info
        .def(
            "open_segment",
            [](TransferEngine& self,
               const std::string& segment_name) -> uint64_t {
                py::gil_scoped_release release;
                SegmentID handle = 0;
                auto s = self.openSegment(handle, segment_name);
                ThrowIfNotOkReacquireGil(s, "open_segment");
                return (uint64_t)handle;
            },
            py::arg("segment_name"))

        .def(
            "close_segment",
            [](TransferEngine& self, uint64_t handle) {
                py::gil_scoped_release release;
                auto s = self.closeSegment((SegmentID)handle);
                ThrowIfNotOkReacquireGil(s, "close_segment");
            },
            py::arg("handle"))

        .def(
            "get_segment_info",
            [](TransferEngine& self, uint64_t handle) -> SegmentInfo {
                py::gil_scoped_release release;
                SegmentInfo info;
                auto s = self.getSegmentInfo((SegmentID)handle, info);
                ThrowIfNotOkReacquireGil(s, "get_segment_info");
                return info;
            },
            py::arg("handle"))

        // allocate/free memory (basic)
        .def(
            "allocate_local_memory",
            [](TransferEngine& self, size_t size,
               const Location& location) -> uint64_t {
                py::gil_scoped_release release;
                void* addr = nullptr;
                auto s = self.allocateLocalMemory(&addr, size, location);
                ThrowIfNotOkReacquireGil(s, "allocate_local_memory");
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
                ThrowIfNotOkReacquireGil(s, "allocate_local_memory_ex");
                return PtrToU64(addr);
            },
            py::arg("size"), py::arg("options"))

        .def(
            "free_local_memory",
            [](TransferEngine& self, uint64_t addr) {
                py::gil_scoped_release release;
                auto s = self.freeLocalMemory(U64ToPtr(addr));
                ThrowIfNotOkReacquireGil(s, "free_local_memory");
            },
            py::arg("addr"))

        // register/unregister single
        .def(
            "register_local_memory",
            [](TransferEngine& self, uint64_t addr, size_t size,
               Permission permission) {
                py::gil_scoped_release release;
                auto s =
                    self.registerLocalMemory(U64ToPtr(addr), size, permission);
                ThrowIfNotOkReacquireGil(s, "register_local_memory");
            },
            py::arg("addr"), py::arg("size"),
            py::arg("permission") = Permission::kGlobalReadWrite)

        .def(
            "unregister_local_memory",
            [](TransferEngine& self, uint64_t addr, size_t size) {
                py::gil_scoped_release release;
                auto s = self.unregisterLocalMemory(U64ToPtr(addr), size);
                ThrowIfNotOkReacquireGil(s, "unregister_local_memory");
            },
            py::arg("addr"), py::arg("size") = 0)

        // register/unregister batch
        .def(
            "register_local_memory_batch",
            [](TransferEngine& self, const std::vector<uint64_t>& addr_list,
               const std::vector<size_t>& size_list, Permission permission) {
                py::gil_scoped_release release;
                std::vector<void*> ptrs;
                ptrs.reserve(addr_list.size());
                for (auto a : addr_list) ptrs.push_back(U64ToPtr(a));
                auto s = self.registerLocalMemory(ptrs, size_list, permission);
                ThrowIfNotOkReacquireGil(s, "register_local_memory_batch");
            },
            py::arg("addr_list"), py::arg("size_list"),
            py::arg("permission") = Permission::kGlobalReadWrite)

        .def(
            "unregister_local_memory_batch",
            [](TransferEngine& self, const std::vector<uint64_t>& addr_list,
               const std::vector<size_t>& size_list) {
                py::gil_scoped_release release;
                std::vector<void*> ptrs;
                ptrs.reserve(addr_list.size());
                for (auto a : addr_list) ptrs.push_back(U64ToPtr(a));
                auto s = self.unregisterLocalMemory(ptrs, size_list);
                ThrowIfNotOkReacquireGil(s, "unregister_local_memory_batch");
            },
            py::arg("addr_list"), py::arg("size_list") = std::vector<size_t>{})

        // register memory (advanced options)
        .def(
            "register_local_memory_ex",
            [](TransferEngine& self, uint64_t addr, size_t size,
               MemoryOptions& options) {
                py::gil_scoped_release release;
                auto s =
                    self.registerLocalMemory(U64ToPtr(addr), size, options);
                ThrowIfNotOkReacquireGil(s, "register_local_memory_ex");
            },
            py::arg("addr"), py::arg("size"), py::arg("options"))

        .def(
            "register_local_memory_batch_ex",
            [](TransferEngine& self, const std::vector<uint64_t>& addr_list,
               const std::vector<size_t>& size_list, MemoryOptions& options) {
                py::gil_scoped_release release;
                std::vector<void*> ptrs;
                ptrs.reserve(addr_list.size());
                for (auto a : addr_list) ptrs.push_back(U64ToPtr(a));
                auto s = self.registerLocalMemory(ptrs, size_list, options);
                ThrowIfNotOkReacquireGil(s, "register_local_memory_batch_ex");
            },
            py::arg("addr_list"), py::arg("size_list"), py::arg("options"))

        // batch
        .def("allocate_transfer_batch", &TransferEngine::allocateBatch,
             py::arg("batch_size"))

        .def(
            "free_transfer_batch",
            [](TransferEngine& self, uint64_t batch_id) {
                py::gil_scoped_release release;
                auto s = self.freeBatch((BatchID)batch_id);
                ThrowIfNotOkReacquireGil(s, "free_transfer_batch");
            },
            py::arg("batch_id"))

        // submitTransfer overloads
        .def(
            "submit_transfer",
            [](TransferEngine& self, uint64_t batch_id,
               const std::vector<Request>& request_list) {
                py::gil_scoped_release release;
                auto s = self.submitTransfer((BatchID)batch_id, request_list);
                ThrowIfNotOkReacquireGil(s, "submit_transfer");
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
                ThrowIfNotOkReacquireGil(s, "submit_transfer");
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
                ThrowIfNotOkReacquireGil(s, "submit_transfer_notif");
            },
            py::arg("batch_id"), py::arg("request_list"), py::arg("name"),
            py::arg("message"))

        // notification send/receive
        .def(
            "send_notifi",
            [](TransferEngine& self, uint64_t target_id,
               const Notification& notifi) {
                py::gil_scoped_release release;
                auto s = self.sendNotification((SegmentID)target_id, notifi);
                ThrowIfNotOkReacquireGil(s, "send_notifi");
            },
            py::arg("target_id"), py::arg("notifi"))

        .def("recv_notifi", recv_notifi)

        // status queries
        .def(
            "get_transfer_status",
            [](TransferEngine& self, uint64_t batch_id,
               size_t task_id) -> TransferStatus {
                py::gil_scoped_release release;
                TransferStatus st;
                auto s = self.getTransferStatus((BatchID)batch_id, task_id, st);
                ThrowIfNotOkReacquireGil(s, "get_transfer_status");
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
                ThrowIfNotOkReacquireGil(s, "get_transfer_status_list");
                return status_list;
            },
            py::arg("batch_id"))

        .def(
            "get_transfer_status_overall",
            [](TransferEngine& self, uint64_t batch_id) -> TransferStatus {
                py::gil_scoped_release release;
                TransferStatus overall;
                auto s = self.getTransferStatus((BatchID)batch_id, overall);
                ThrowIfNotOkReacquireGil(s, "get_transfer_status_overall");
                return overall;
            },
            py::arg("batch_id"));
}
