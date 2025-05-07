// Copyright 2024 KVCache.AI
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

#include "transfer_engine_py.h"

#include <cassert>
#include <fstream>

TransferEnginePy::TransferEnginePy() {}

TransferEnginePy::~TransferEnginePy() {
    for (auto &handle : handle_map_) engine_->closeSegment(handle.second);
    handle_map_.clear();
    engine_.reset();
    for (auto &buffer : buffer_list_) free(buffer);
    buffer_list_.clear();
    for (auto &buffer : large_buffer_list_) free(buffer);
    large_buffer_list_.clear();
}

std::vector<std::string> buildDeviceFilter(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }
    return tokens;
}

std::pair<std::string, std::string> parseConnectionString(
    const std::string &conn_string) {
    std::pair<std::string, std::string> result;
    std::string proto = "etcd";
    std::string domain;
    std::size_t pos = conn_string.find("://");

    if (pos != std::string::npos) {
        proto = conn_string.substr(0, pos);
        domain = conn_string.substr(pos + 3);
    } else if (conn_string == P2PHANDSHAKE) {
        proto = "";
        domain = P2PHANDSHAKE;
    } else {
        domain = conn_string;
    }

    result.first = proto;
    result.second = domain;
    return result;
}

std::string buildConnString(const std::string &metadata_type,
                            const std::string &metadata_server) {
    if (metadata_server == P2PHANDSHAKE) {
        return P2PHANDSHAKE;
    }

    std::string conn_string = metadata_server;
    if (conn_string.find("://") == std::string::npos)
        conn_string = metadata_type + "://" + metadata_server;
    return conn_string;
}

int TransferEnginePy::initialize(const char *local_hostname,
                                 const char *metadata_server,
                                 const char *protocol,
                                 const char *device_name) {
    auto conn_string = parseConnectionString(metadata_server);
    return initializeExt(local_hostname, conn_string.second.c_str(), protocol,
                         device_name, conn_string.first.c_str());
}

int TransferEnginePy::initializeExt(const char *local_hostname,
                                    const char *metadata_server,
                                    const char *protocol,
                                    const char *device_name,
                                    const char *metadata_type) {
    (void)(protocol);
    std::string conn_string = buildConnString(metadata_type, metadata_server);

    auto device_name_safe = device_name ? std::string(device_name) : "";
    auto device_filter = buildDeviceFilter(device_name_safe);
    engine_ = std::make_unique<TransferEngine>(true, device_filter);
    if (getenv("MC_LEGACY_RPC_PORT_BINDING")) {
        auto hostname_port = parseHostNameWithPort(local_hostname);
        int ret =
            engine_->init(conn_string, local_hostname,
                          hostname_port.first.c_str(), hostname_port.second);
        if (ret) return -1;
    } else {
        // the last two params are unused
        int ret = engine_->init(conn_string, local_hostname, "", 0);
        if (ret) return -1;
    }

    free_list_.resize(kSlabSizeKBTabLen);
    doBuddyAllocate(kMaxClassId);
    return 0;
}

int TransferEnginePy::getRpcPort() { return engine_->getRpcPort(); }

char *TransferEnginePy::allocateRawBuffer(size_t capacity) {
    auto buffer = malloc(capacity);
    if (!buffer) return nullptr;
    int ret = engine_->registerLocalMemory(buffer, capacity, "cpu:0");
    if (ret) {
        free(buffer);
        return nullptr;
    }
    return (char *)buffer;
}

int TransferEnginePy::findClassId(size_t size) {
    if (size > 1024ull * kSlabSizeKB[kMaxClassId]) return -1;
    for (int i = kMaxClassId - 2; i >= 0; --i)
        if (size > 1024ull * kSlabSizeKB[i]) return i + 1;
    return 0;
}

int TransferEnginePy::doBuddyAllocate(int class_id) {
    if (class_id == kMaxClassId) {
        auto buffer = allocateRawBuffer(kDefaultBufferCapacity);
        buffer_list_.push_back(buffer);
        for (size_t offset = 0; offset < kDefaultBufferCapacity;
             offset += 1024ull * kSlabSizeKB[kMaxClassId])
            free_list_[kMaxClassId].push(buffer + offset);
        return 0;
    }
    if (free_list_[class_id + 1].empty()) {
        int ret = doBuddyAllocate(class_id + 1);
        if (ret) return ret;
    }
    assert(!free_list_[class_id + 1].empty());
    char *buffer = free_list_[class_id + 1].top();
    free_list_[class_id + 1].pop();
    free_list_[class_id].push(buffer);
    free_list_[class_id].push(buffer + kSlabSizeKB[class_id] * 1024);
    return 0;
}

uintptr_t TransferEnginePy::allocateManagedBuffer(size_t length) {
    std::lock_guard<std::mutex> guard(mutex_);
    int class_id = findClassId(length);
    if (class_id < 0) {
        char *buffer = allocateRawBuffer(length);
        if (buffer) large_buffer_list_.insert(buffer);
        return (uintptr_t)buffer;
    }
    if (free_list_[class_id].empty())
        if (doBuddyAllocate(class_id)) return 0;
    assert(!free_list_[class_id].empty());
    char *buffer = free_list_[class_id].top();
    free_list_[class_id].pop();
    return (uintptr_t)buffer;
}

int TransferEnginePy::freeManagedBuffer(uintptr_t buffer_addr, size_t length) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto buffer = (char *)buffer_addr;
    int class_id = findClassId(length);
    if (class_id < 0) {
        large_buffer_list_.erase(buffer);
        engine_->unregisterLocalMemory(buffer);
        free(buffer);
        return 0;
    }
    free_list_[class_id].push(buffer);
    return 0;
}

int TransferEnginePy::transferSyncWrite(const char *target_hostname,
                                        uintptr_t buffer,
                                        uintptr_t peer_buffer_address,
                                        size_t length) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    if (handle_map_.count(target_hostname)) {
        handle = handle_map_[target_hostname];
    } else {
        handle = engine_->openSegment(target_hostname);
        if (handle == (Transport::SegmentHandle)-1) return -1;
        handle_map_[target_hostname] = handle;
    }

    auto batch_id = engine_->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = length;
    entry.source = (void *)buffer;
    entry.target_id = handle;
    entry.target_offset = peer_buffer_address;

    Status s = engine_->submitTransfer(batch_id, {entry});
    if (!s.ok()) return -1;

    TransferStatus status;
    while (true) {
        Status s = engine_->getTransferStatus(batch_id, 0, status);
        LOG_ASSERT(s.ok());
        if (status.s == TransferStatusEnum::COMPLETED) {
            engine_->freeBatchID(batch_id);
            return 0;
        } else if (status.s == TransferStatusEnum::FAILED) {
            engine_->freeBatchID(batch_id);
            return -1;
        }
    }
}

int TransferEnginePy::transferSyncRead(const char *target_hostname,
                                       uintptr_t buffer,
                                       uintptr_t peer_buffer_address,
                                       size_t length) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    if (handle_map_.count(target_hostname)) {
        handle = handle_map_[target_hostname];
    } else {
        handle = engine_->openSegment(target_hostname);
        if (handle == (Transport::SegmentHandle)-1) return -1;
        handle_map_[target_hostname] = handle;
    }

    auto batch_id = engine_->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::READ;
    entry.length = length;
    entry.source = (void *)buffer;
    entry.target_id = handle;
    entry.target_offset = peer_buffer_address;

    Status s = engine_->submitTransfer(batch_id, {entry});
    if (!s.ok()) return -1;

    TransferStatus status;
    while (true) {
        Status s = engine_->getTransferStatus(batch_id, 0, status);
        LOG_ASSERT(s.ok());
        if (status.s == TransferStatusEnum::COMPLETED) {
            engine_->freeBatchID(batch_id);
            return 0;
        } else if (status.s == TransferStatusEnum::FAILED) {
            engine_->freeBatchID(batch_id);
            return -1;
        }
    }
}

int TransferEnginePy::transferSync(const char *target_hostname,
                                   uintptr_t buffer,
                                   uintptr_t peer_buffer_address, size_t length,
                                   TransferOpcode opcode) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    if (handle_map_.count(target_hostname)) {
        handle = handle_map_[target_hostname];
    } else {
        handle = engine_->openSegment(target_hostname);
        if (handle == (Transport::SegmentHandle)-1) return -1;
        handle_map_[target_hostname] = handle;
    }

    // TODO this is just a workaround
    // When transfer engine submits one task, it will be dispatch to a worker 
    // associated with one local RNIC. If the local RNIC fails to connect to any
    // remote RNIC, it will eventually fail. This allows selecting multiple local 
    // RNIC in one transferSync call. Will be fixed in the next revision.
    const int max_retry = 3;
    for (int retry = 0; retry < max_retry; ++retry) {
        auto batch_id = engine_->allocateBatchID(1);
        TransferRequest entry;
        if (opcode == TransferOpcode::WRITE) {
            entry.opcode = TransferRequest::WRITE;
        } else {
            entry.opcode = TransferRequest::READ;
        }
        entry.length = length;
        entry.source = (void *)buffer;
        entry.target_id = handle;
        entry.target_offset = peer_buffer_address;

        Status s = engine_->submitTransfer(batch_id, {entry});
        if (!s.ok()) return -1;

        TransferStatus status;
        bool completed = false;
        while (!completed) {
            Status s = engine_->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine_->freeBatchID(batch_id);
                return 0;
            } else if (status.s == TransferStatusEnum::FAILED) {
                engine_->freeBatchID(batch_id);
                completed = true;
            }
        }
    }
    return -1;
}

int TransferEnginePy::transferSubmitWrite(const char *target_hostname,
                                          uintptr_t buffer,
                                          uintptr_t peer_buffer_address,
                                          size_t length) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    if (handle_map_.count(target_hostname)) {
        handle = handle_map_[target_hostname];
    } else {
        handle = engine_->openSegment(target_hostname);
        if (handle == (Transport::SegmentHandle)-1) return -1;
        handle_map_[target_hostname] = handle;
    }

    auto batch_id = engine_->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = length;
    entry.source = (void *)buffer;
    entry.target_id = handle;
    entry.target_offset = peer_buffer_address;

    Status s = engine_->submitTransfer(batch_id, {entry});
    if (!s.ok()) return -1;

    return batch_id;
}

int TransferEnginePy::transferCheckStatus(int batch_id) {
    pybind11::gil_scoped_release release;
    TransferStatus status;
    Status s = engine_->getTransferStatus(batch_id, 0, status);
    LOG_ASSERT(s.ok());
    if (status.s == TransferStatusEnum::COMPLETED) {
        engine_->freeBatchID(batch_id);
        return 1;
    } else if (status.s == TransferStatusEnum::FAILED) {
        engine_->freeBatchID(batch_id);
        return -1;
    } else {
        return 0;
    }
}

int TransferEnginePy::registerMemory(uintptr_t buffer_addr, size_t capacity) {
    char *buffer = reinterpret_cast<char *>(buffer_addr);
    return engine_->registerLocalMemory(buffer, capacity);
}

int TransferEnginePy::unregisterMemory(uintptr_t buffer_addr) {
    char *buffer = reinterpret_cast<char *>(buffer_addr);
    return engine_->unregisterLocalMemory(buffer);
}

uintptr_t TransferEnginePy::getFirstBufferAddress(
    const std::string &segment_name) {
    Transport::SegmentHandle segment_id =
        engine_->openSegment(segment_name.c_str());
    auto segment_desc = engine_->getMetadata()->getSegmentDescByID(segment_id);
    return segment_desc->buffers[0].addr;
}

namespace py = pybind11;

PYBIND11_MODULE(engine, m) {
    py::enum_<TransferEnginePy::TransferOpcode> transfer_opcode(
        m, "TransferOpcode", py::arithmetic());
    transfer_opcode.value("Read", TransferEnginePy::TransferOpcode::READ)
        .value("Write", TransferEnginePy::TransferOpcode::WRITE)
        .export_values();

    auto adaptor_cls =
        py::class_<TransferEnginePy>(m, "TransferEngine")
            .def(py::init<>())
            .def("initialize", &TransferEnginePy::initialize)
            .def("initialize_ext", &TransferEnginePy::initializeExt)
            .def("get_rpc_port", &TransferEnginePy::getRpcPort)
            .def("allocate_managed_buffer",
                 &TransferEnginePy::allocateManagedBuffer)
            .def("free_managed_buffer", &TransferEnginePy::freeManagedBuffer)
            .def("transfer_sync_write", &TransferEnginePy::transferSyncWrite)
            .def("transfer_sync_read", &TransferEnginePy::transferSyncRead)
            .def("transfer_sync", &TransferEnginePy::transferSync)
            .def("transfer_submit_write", &TransferEnginePy::transferSubmitWrite)
            .def("transfer_check_status", &TransferEnginePy::transferCheckStatus)
            .def("write_bytes_to_buffer", &TransferEnginePy::writeBytesToBuffer)
            .def("read_bytes_from_buffer",
                 &TransferEnginePy::readBytesFromBuffer)
            .def("register_memory", &TransferEnginePy::registerMemory)
            .def("unregister_memory", &TransferEnginePy::unregisterMemory)
            .def("get_first_buffer_address",
                 &TransferEnginePy::getFirstBufferAddress);

    adaptor_cls.attr("TransferOpcode") = transfer_opcode;
}
