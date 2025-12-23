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

#include "tent/runtime/transfer_engine_impl.h"

#include <cassert>
#include <numeric>
#include <fstream>
#include <pybind11/stl.h>
#include <glog/logging.h>

using namespace mooncake::tent;

class TransferEnginePy {
   public:
    using SegmentID = uint64_t;

   public:
    TransferEnginePy() = default;

    ~TransferEnginePy() = default;

    int start() {
        if (impl_) {
            LOG(WARNING) << "TransferEngine already started";
            return -1;
        }
        impl_ = std::make_shared<TransferEngineImpl>();
        return 0;
    }

    int startWithConfig(const char* config_path) {
        if (impl_) {
            LOG(WARNING) << "TransferEngine already started";
            return -1;
        }
        std::shared_ptr<Config> config = std::make_shared<Config>();
        Status s = config->load(config_path);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to load config file: " << s.ToString();
            return -2;
        }
        impl_ = std::make_shared<TransferEngineImpl>(config);
        return 0;
    }

    int stop() {
        if (!impl_) {
            LOG(WARNING) << "TransferEngine not started";
            return -1;
        }
        impl_.reset();
        return 0;
    }

    bool started() const { return impl_ != nullptr && impl_->available(); }

    const std::string getSegmentName() const {
        if (!impl_) return "";
        return impl_->getSegmentName();
    }

    const std::string getRpcServerAddress() const {
        if (!impl_) return "";
        return impl_->getRpcServerAddress();
    }

    int getRpcServerPort() const {
        if (!impl_) return -1;
        return impl_->getRpcServerPort();
    }

    int64_t openSegment(const std::string& segment_name) {
        if (!impl_) return -1;
        SegmentID handle;
        auto status = impl_->openSegment(handle, segment_name);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to open segment: " << status.ToString();
            return -2;
        }
        return handle;
    }

    int closeSegment(int64_t handle) {
        if (!impl_) return -1;
        auto status = impl_->closeSegment((SegmentID)handle);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to close segment: " << status.ToString();
            return -2;
        }
        return 0;
    }

    SegmentInfo getSegmentInfo(int64_t handle) {
        SegmentInfo info;
        if (!impl_) return info;
        auto status = impl_->getSegmentInfo((SegmentID)handle, info);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to get segment info: " << status.ToString();
            return info;
        }
        return info;
    }

    uint64_t allocateLocalMemory(size_t size, std::string location = "*") {
        if (!impl_) return -1;
        void* addr = nullptr;
        auto status = impl_->allocateLocalMemory(&addr, size, location);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to allocate local memory: "
                       << status.ToString();
            return 0;
        }
        return (uint64_t)(addr);
    }

    int freeLocalMemory(uint64_t addr) {
        if (!impl_) return -1;
        auto status = impl_->freeLocalMemory((void*)addr);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to free local memory: " << status.ToString();
            return -2;
        }
        return 0;
    }

    int registerLocalMemory(uint64_t addr, size_t size) {
        if (!impl_) return -1;
        auto status = impl_->registerLocalMemory((void*)addr, size);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to register local memory: "
                       << status.ToString();
            return -2;
        }
        return 0;
    }

    int unregisterLocalMemory(uint64_t addr, size_t size = 0) {
        if (!impl_) return -1;
        auto status = impl_->unregisterLocalMemory((void*)addr, size);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to unregister local memory: "
                       << status.ToString();
            return -2;
        }
        return 0;
    }

    int registerLocalMemory(std::vector<uint64_t> addr_list,
                            std::vector<size_t> size_list) {
        if (!impl_) return -1;
        std::vector<void*> void_ptr_list;
        for (auto addr : addr_list) {
            void_ptr_list.push_back((void*)addr);
        }
        auto status = impl_->registerLocalMemory(void_ptr_list, size_list);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to register local memory: "
                       << status.ToString();
            return -2;
        }
        return 0;
    }

    int unregisterLocalMemory(std::vector<uint64_t> addr_list,
                              std::vector<size_t> size_list = {}) {
        if (!impl_) return -1;
        std::vector<void*> void_ptr_list;
        for (auto addr : addr_list) {
            void_ptr_list.push_back((void*)addr);
        }
        auto status = impl_->unregisterLocalMemory(void_ptr_list, size_list);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to unregister local memory: "
                       << status.ToString();
            return -2;
        }
        return 0;
    }

    int64_t allocateBatch(size_t batch_size) {
        if (!impl_) return -1;
        BatchID batch_id = impl_->allocateBatch(batch_size);
        return (int64_t)batch_id;
    }

    int freeBatch(int64_t batch_id) {
        if (!impl_) return -1;
        auto status = impl_->freeBatch((BatchID)batch_id);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to free batch: " << status.ToString();
            return -2;
        }
        return 0;
    }

    int submitTransfer(int64_t batch_id,
                       const std::vector<Request>& request_list) {
        if (!impl_) return -1;
        auto status = impl_->submitTransfer((BatchID)batch_id, request_list);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to submit transfer tasks: "
                       << status.ToString();
            return -2;
        }
        return 0;
    }

    TransferStatus getTransferStatus(int64_t batch_id, size_t task_id) {
        TransferStatus status;
        status.s = TransferStatusEnum::INVALID;
        status.transferred_bytes = 0;
        if (!impl_) return status;
        auto s = impl_->getTransferStatus((BatchID)batch_id, task_id, status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status: " << s.ToString();
            return status;
        }
        return status;
    }

    TransferStatus getTransferOverallStatus(int64_t batch_id) {
        TransferStatus status;
        status.s = TransferStatusEnum::INVALID;
        status.transferred_bytes = 0;
        if (!impl_) return status;
        auto s = impl_->getTransferStatus((BatchID)batch_id, status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status: " << s.ToString();
            return status;
        }
        return status;
    }

   private:
    std::shared_ptr<TransferEngineImpl> impl_;
};

namespace py = pybind11;

PYBIND11_MODULE(tent, m) {
    py::enum_<Request::OpCode>(m, "RequestOpCode")
        .value("READ", Request::OpCode::READ)
        .value("WRITE", Request::OpCode::WRITE)
        .export_values();

    py::class_<Request>(m, "Request")
        .def(py::init<>())
        .def_property(
            "opcode", [](const Request& r) { return r.opcode; },
            [](Request& r, Request::OpCode op) { r.opcode = op; })
        .def_property(
            "source",
            [](const Request& r) -> std::uintptr_t {
                return reinterpret_cast<std::uintptr_t>(r.source);
            },
            [](Request& r, std::uintptr_t addr) {
                r.source = reinterpret_cast<void*>(addr);
            })
        .def_readwrite("target_id", &Request::target_id)
        .def_readwrite("target_offset", &Request::target_offset)
        .def_readwrite("length", &Request::length);

    py::enum_<TransferStatusEnum>(m, "TransferStatusEnum")
        .value("INITIAL", TransferStatusEnum::INITIAL)
        .value("PENDING", TransferStatusEnum::PENDING)
        .value("INVALID", TransferStatusEnum::INVALID)
        .value("CANCELED", TransferStatusEnum::CANCELED)
        .value("COMPLETED", TransferStatusEnum::COMPLETED)
        .value("TIMEOUT", TransferStatusEnum::TIMEOUT)
        .value("FAILED", TransferStatusEnum::FAILED)
        .export_values();

    py::class_<TransferStatus>(m, "TransferStatus")
        .def(py::init<>())
        .def_readwrite("s", &TransferStatus::s)
        .def_readwrite("transferred_bytes", &TransferStatus::transferred_bytes);

    py::enum_<SegmentInfo::Type>(m, "SegmentInfoType")
        .value("Memory", SegmentInfo::Type::Memory)
        .value("File", SegmentInfo::Type::File)
        .export_values();

    py::class_<SegmentInfo::Buffer>(m, "SegmentInfoBuffer")
        .def_property_readonly(
            "base", [](const SegmentInfo::Buffer& b) { return b.base; })
        .def_property_readonly(
            "length", [](const SegmentInfo::Buffer& b) { return b.length; })
        .def_property_readonly(
            "location",
            [](const SegmentInfo::Buffer& b) -> const Location& {
                return b.location;
            },
            py::return_value_policy::reference_internal);

    py::class_<SegmentInfo>(m, "SegmentInfo")
        .def(py::init<>())
        .def_property_readonly("type",
                               [](const SegmentInfo& s) { return s.type; })
        .def_property_readonly(
            "buffers",
            [](const SegmentInfo& s)
                -> const std::vector<SegmentInfo::Buffer>& {
                return s.buffers;
            },
            py::return_value_policy::reference_internal);

    auto adaptor_cls =
        py::class_<TransferEnginePy>(m, "TransferEngine")
            .def(py::init<>())
            .def("start", &TransferEnginePy::start)
            .def("start_with_config", &TransferEnginePy::startWithConfig,
                 py::arg("config_path"))
            .def("stop", &TransferEnginePy::stop)
            .def("started", &TransferEnginePy::started)
            .def("get_segment_name", &TransferEnginePy::getSegmentName)
            .def("get_rpc_server_address",
                 &TransferEnginePy::getRpcServerAddress)
            .def("get_rpc_server_port", &TransferEnginePy::getRpcServerPort)
            .def("open_segment", &TransferEnginePy::openSegment,
                 py::arg("segment_name"))
            .def("close_segment", &TransferEnginePy::closeSegment,
                 py::arg("handle"))
            .def("get_segment_info", &TransferEnginePy::getSegmentInfo,
                 py::arg("handle"))
            .def("allocate_local_memory",
                 &TransferEnginePy::allocateLocalMemory, py::arg("size"),
                 py::arg("location") = "*")
            .def("free_local_memory", &TransferEnginePy::freeLocalMemory,
                 py::arg("addr"))
            .def("register_local_memory",
                 py::overload_cast<uint64_t, size_t>(
                     &TransferEnginePy::registerLocalMemory),
                 py::arg("addr"), py::arg("size"))
            .def("unregister_local_memory",
                 py::overload_cast<uint64_t, size_t>(
                     &TransferEnginePy::unregisterLocalMemory),
                 py::arg("addr"), py::arg("size") = 0)
            .def("register_local_memory_batch",
                 py::overload_cast<std::vector<uint64_t>, std::vector<size_t>>(
                     &TransferEnginePy::registerLocalMemory),
                 py::arg("addr_list"), py::arg("size_list"))
            .def("unregister_local_memory_batch",
                 py::overload_cast<std::vector<uint64_t>, std::vector<size_t>>(
                     &TransferEnginePy::unregisterLocalMemory),
                 py::arg("addr_list"),
                 py::arg("size_list") = std::vector<size_t>{})
            .def("allocate_transfer", &TransferEnginePy::allocateBatch,
                 py::arg("batch_size"))
            .def("free_transfer", &TransferEnginePy::freeBatch,
                 py::arg("batch_id"))
            .def("submit_transfer", &TransferEnginePy::submitTransfer,
                 py::arg("batch_id"), py::arg("request_list"))
            .def("get_transfer_status", &TransferEnginePy::getTransferStatus,
                 py::arg("batch_id"), py::arg("task_id"))
            .def("get_transfer_status_overall",
                 &TransferEnginePy::getTransferOverallStatus,
                 py::arg("batch_id"));
}
