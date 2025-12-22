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
    enum class TransferOpcode { READ = 0, WRITE = 1 };
    struct TransferNotify {
        std::string name;
        std::string msg;
    };

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

    int getSegmentInfo(int64_t handle, SegmentInfo& info) {
        if (!impl_) return -1;
        auto status = impl_->getSegmentInfo((SegmentID)handle, info);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to get segment info: " << status.ToString();
            return -2;
        }
        return 0;
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

    int getTransferStatus(int64_t batch_id, size_t task_id,
                          TransferStatus& status) {
        if (!impl_) return -1;
        auto s = impl_->getTransferStatus((BatchID)batch_id, task_id, status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status: " << s.ToString();
            return -2;
        }
        return 0;
    }

    int getTransferStatus(int64_t batch_id, TransferStatus& overall_status) {
        if (!impl_) return -1;
        auto s = impl_->getTransferStatus((BatchID)batch_id, overall_status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status: " << s.ToString();
            return -2;
        }
        return 0;
    }

   private:
    std::shared_ptr<TransferEngineImpl> impl_;
};

namespace py = pybind11;

PYBIND11_MODULE(tent, m) {
    py::enum_<TransferEnginePy::TransferOpcode> transfer_opcode(
        m, "TransferOpcode", py::arithmetic());

    transfer_opcode.value("Read", TransferEnginePy::TransferOpcode::READ)
        .value("Write", TransferEnginePy::TransferOpcode::WRITE)
        .export_values();

    py::class_<TransferEnginePy::TransferNotify>(m, "TransferNotify")
        .def(py::init<>())
        .def(py::init<const std::string&, const std::string&>(),
             py::arg("name"), py::arg("msg"))
        .def_readwrite("name", &TransferEnginePy::TransferNotify::name)
        .def_readwrite("msg", &TransferEnginePy::TransferNotify::msg);

    auto adaptor_cls =
        py::class_<TransferEnginePy>(m, "TransferEngine")
            .def(py::init<>())
            .def("start", &TransferEnginePy::start)
            .def("start_with_config", &TransferEnginePy::startWithConfig)
            .def("stop", &TransferEnginePy::stop)
            .def("started", &TransferEnginePy::started);

    adaptor_cls.attr("TransferOpcode") = transfer_opcode;
    py::class_<TransferEngine, std::shared_ptr<TransferEngine>>(
        m, "InnerTransferEngine");
}
