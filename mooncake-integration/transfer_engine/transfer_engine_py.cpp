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
#include <numeric>
#include <fstream>

#include <pybind11/stl.h>

auto torch = py::module_::import("torch");

#ifdef USE_MNNVL
#include "transport/nvlink_transport/nvlink_transport.h"
static void *allocateMemory(size_t size) {
    return mooncake::NvlinkTransport::allocatePinnedLocalMemory(size);
}
static void freeMemory(void *ptr) {
    mooncake::NvlinkTransport::freePinnedLocalMemory(ptr);
}
#else
static void *allocateMemory(size_t size) { return malloc(size); }
static void freeMemory(void *ptr) { free(ptr); }
#endif

TransferEnginePy::TransferEnginePy() {
    const int64_t kNanosPerSecond = 1000 * 1000 * 1000;
    if (getenv("MC_TRANSFER_TIMEOUT")) {
        int timeout_sec = std::max(5, atoi(getenv("MC_TRANSFER_TIMEOUT")));
        transfer_timeout_nsec_ = timeout_sec * kNanosPerSecond;
    } else {
        transfer_timeout_nsec_ = 30 * kNanosPerSecond;
    }
}

TransferEnginePy::~TransferEnginePy() {
    for (auto &handle : handle_map_) engine_->closeSegment(handle.second);
    handle_map_.clear();
    engine_.reset();
    for (auto &buffer : buffer_list_) freeMemory(buffer);
    buffer_list_.clear();
    for (auto &buffer : large_buffer_list_) freeMemory(buffer);
    large_buffer_list_.clear();
}

template <typename T>
py::array TransferEnginePy::create_typed_array(char *exported_data, size_t offset, size_t total_length) {
    py::capsule free_when_done(
        exported_data, [](void *p) { delete[] static_cast<char *>(p); });
    return py::array_t<T>({static_cast<ssize_t>(total_length / sizeof(T))},
                          (T *)(exported_data + offset), free_when_done);
}

using ArrayCreatorFunc = std::function<py::array(char *, size_t, size_t)>;

static const std::array<ArrayCreatorFunc, 11> array_creators = {{
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<float>(data, offset, total_length); 
    },     // FLOAT32 = 0
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<double>(data, offset, total_length); 
    },    // FLOAT64 = 1
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<int8_t>(data, offset, total_length); 
    },    // INT8 = 2
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<uint8_t>(data, offset, total_length); 
    },   // UINT8 = 3
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<int16_t>(data, offset, total_length); 
    },   // INT16 = 4
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<uint16_t>(data, offset, total_length); 
    },  // UINT16 = 5
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<int32_t>(data, offset, total_length); 
    },   // INT32 = 6
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<uint32_t>(data, offset, total_length); 
    },  // UINT32 = 7
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<int64_t>(data, offset, total_length); 
    },   // INT64 = 8
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<uint64_t>(data, offset, total_length); 
    },  // UINT64 = 9
    [](char* data, size_t offset, size_t total_length) { 
        return TransferEnginePy{}.create_typed_array<bool>(data, offset, total_length); 
    }       // BOOL = 10
}};

TensorDtype TransferEnginePy::get_tensor_dtype(py::object dtype_obj) {
    if (dtype_obj.is_none()) {
        return TensorDtype::UNKNOWN;
    }

    if (dtype_obj.equal(torch.attr("float32"))) return TensorDtype::FLOAT32;
    if (dtype_obj.equal(torch.attr("float64"))) return TensorDtype::FLOAT64;
    if (dtype_obj.equal(torch.attr("int8"))) return TensorDtype::INT8;
    if (dtype_obj.equal(torch.attr("uint8"))) return TensorDtype::UINT8;
    if (dtype_obj.equal(torch.attr("int16"))) return TensorDtype::INT16;
    if (dtype_obj.equal(torch.attr("uint16"))) return TensorDtype::UINT16;
    if (dtype_obj.equal(torch.attr("int32"))) return TensorDtype::INT32;
    if (dtype_obj.equal(torch.attr("uint32"))) return TensorDtype::UINT32;
    if (dtype_obj.equal(torch.attr("int64"))) return TensorDtype::INT64;
    if (dtype_obj.equal(torch.attr("uint64"))) return TensorDtype::UINT64;
    if (dtype_obj.equal(torch.attr("bool"))) return TensorDtype::BOOL;

    return TensorDtype::UNKNOWN;
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
#ifndef USE_ASCEND
    doBuddyAllocate(kMaxClassId);
#endif
    return 0;
}

int TransferEnginePy::getRpcPort() { return engine_->getRpcPort(); }

char *TransferEnginePy::allocateRawBuffer(size_t capacity) {
    auto buffer = allocateMemory(capacity);
    if (!buffer) return nullptr;
    int ret = engine_->registerLocalMemory(buffer, capacity, kWildcardLocation);
    if (ret) {
        freeMemory(buffer);
        return nullptr;
    }
    return (char *)buffer;
}

int TransferEnginePy::findClassId(size_t size) {
    if (size > 1024ull * kSlabSizeKB[kMaxClassId]) return -1;
    for (int i = kMaxClassId - 1; i >= 0; --i)
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
        freeMemory(buffer);
        return 0;
    }
    free_list_[class_id].push(buffer);
    return 0;
}

int TransferEnginePy::transferSyncWrite(const char *target_hostname,
                                        uintptr_t buffer,
                                        uintptr_t peer_buffer_address,
                                        size_t length) {
    return transferSync(target_hostname, buffer, peer_buffer_address, length,
                        TransferOpcode::WRITE);
}

int TransferEnginePy::transferSyncRead(const char *target_hostname,
                                       uintptr_t buffer,
                                       uintptr_t peer_buffer_address,
                                       size_t length) {
    return transferSync(target_hostname, buffer, peer_buffer_address, length,
                        TransferOpcode::READ);
}

int TransferEnginePy::batchTransferSyncWrite(const char *target_hostname,
                                             std::vector<uintptr_t> buffers,
                                             std::vector<uintptr_t> peer_buffer_addresses,
                                             std::vector<size_t> lengths) {
    return batchTransferSync(target_hostname, buffers, peer_buffer_addresses, lengths,
                             TransferOpcode::WRITE);
}

int TransferEnginePy::batchTransferSyncRead(const char *target_hostname,
                                            std::vector<uintptr_t> buffers,
                                            std::vector<uintptr_t> peer_buffer_addresses,
                                            std::vector<size_t> lengths) {
    return batchTransferSync(target_hostname, buffers, peer_buffer_addresses, lengths,
                             TransferOpcode::READ);
}

batch_id_t TransferEnginePy::batchTransferAsyncWrite(const char *target_hostname,
                                              const std::vector<uintptr_t> &buffers,
                                              const std::vector<uintptr_t> &peer_buffer_addresses,
                                              const std::vector<size_t> &lengths) {
    return batchTransferAsync(target_hostname, buffers, peer_buffer_addresses, lengths,
                             TransferOpcode::WRITE);
}

batch_id_t TransferEnginePy::batchTransferAsyncRead(const char *target_hostname,
                                             const std::vector<uintptr_t> &buffers,
                                             const std::vector<uintptr_t> &peer_buffer_addresses,
                                             const std::vector<size_t> &lengths) {
    return batchTransferAsync(target_hostname, buffers, peer_buffer_addresses, lengths,
                             TransferOpcode::READ);
}

int TransferEnginePy::transferSync(const char *target_hostname,
                                   uintptr_t buffer,
                                   uintptr_t peer_buffer_address, size_t length,
                                   TransferOpcode opcode) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            handle = engine_->openSegment(target_hostname);
            if (handle == (Transport::SegmentHandle)-1) return -1;
            handle_map_[target_hostname] = handle;
        }
    }

    // TODO this is just a workaround
    // When transfer engine submits one task, it will be dispatch to a worker
    // associated with one local RNIC. If the local RNIC fails to connect to any
    // remote RNIC, it will eventually fail. This allows selecting multiple
    // local RNIC in one transferSync call. Will be fixed in the next revision.
    const int max_retry =
        engine_->numContexts() + 1;  // Iter all possible local contexts
    auto start_ts = getCurrentTimeInNano();
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
        entry.advise_retry_cnt = retry;

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
            } else if (status.s == TransferStatusEnum::TIMEOUT) {
                LOG(INFO) << "Sync data transfer timeout";
                completed = true;
            }
            auto current_ts = getCurrentTimeInNano();
            const int64_t timeout =
                transfer_timeout_nsec_ + length;  // 1GiB per second
            if (current_ts - start_ts > timeout) {
                LOG(INFO) << "Sync data transfer timeout after "
                          << current_ts - start_ts << "ns, local buffer "
                          << (void *)buffer << " remote buffer "
                          << (void *)peer_buffer_address << " length "
                          << length;
                return -1;
            }
        }
    }
    return -1;
}

int TransferEnginePy::batchTransferSync(const char *target_hostname,
                                   std::vector<uintptr_t> buffers,
                                   std::vector<uintptr_t> peer_buffer_addresses,
                                   std::vector<size_t> lengths,
                                   TransferOpcode opcode) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            handle = engine_->openSegment(target_hostname);
            if (handle == (Transport::SegmentHandle)-1) return -1;
            handle_map_[target_hostname] = handle;
        }
    }

    if (buffers.size() != peer_buffer_addresses.size() || buffers.size() != lengths.size()) {
        LOG(ERROR) << "buffers, peer_buffer_addresses and lengths have different size";
        return -1;
    }

    const int max_retry = engine_->numContexts() + 1;
    auto start_ts = getCurrentTimeInNano();
    auto total_length = std::accumulate(lengths.begin(), lengths.end(), 0ull);
    auto batch_size = buffers.size();
    std::vector<TransferRequest> entries;
    for (size_t i = 0; i < batch_size; ++i) {
        TransferRequest entry;
        if (opcode == TransferOpcode::WRITE) {
            entry.opcode = TransferRequest::WRITE;
        } else {
            entry.opcode = TransferRequest::READ;
        }
        entry.length = lengths[i];
        entry.source = (void *)buffers[i];
        entry.target_id = handle;
        entry.target_offset = peer_buffer_addresses[i];
        entry.advise_retry_cnt = 0;
        entries.push_back(entry);
    }

    for (int retry = 0; retry < max_retry; ++retry) {
        auto batch_id = engine_->allocateBatchID(batch_size);
        Status s = engine_->submitTransfer(batch_id, entries);
        if (!s.ok()) {
            engine_->freeBatchID(batch_id);
            return -1;
        }

        TransferStatus status;
        bool completed = false;
        bool already_freed = false;
        while (!completed) {
            Status s = engine_->getBatchTransferStatus(batch_id, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine_->freeBatchID(batch_id);
                return 0;
            } else if (status.s == TransferStatusEnum::FAILED) {
                engine_->freeBatchID(batch_id);
                already_freed = true;
                completed = true;
            } else if (status.s == TransferStatusEnum::TIMEOUT) {
                LOG(INFO) << "Sync data transfer timeout";
                completed = true;
            }
            auto current_ts = getCurrentTimeInNano();
            const int64_t timeout = transfer_timeout_nsec_ + total_length; // 1GiB per second
            if (current_ts - start_ts > timeout) {
                LOG(INFO) << "Sync batch data transfer timeout after " 
                          << current_ts - start_ts << "ns";
                // TODO: as @doujiang24 mentioned, early free(while there are still waiting tasks)
                // the batch_id may fail and cause memory leak(a known issue).
                if (!already_freed) {
                    engine_->freeBatchID(batch_id);
                }
                return -1;
            }
        }
    }
    return -1;
}

batch_id_t TransferEnginePy::batchTransferAsync(const char *target_hostname,
                                                const std::vector<uintptr_t>& buffers,
                                                const std::vector<uintptr_t>& peer_buffer_addresses,
                                                const std::vector<size_t>& lengths,
                                                TransferOpcode opcode) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            handle = engine_->openSegment(target_hostname);
            if (handle == (Transport::SegmentHandle)-1) return -1;
            handle_map_[target_hostname] = handle;
        }
    }

    if (buffers.size() != peer_buffer_addresses.size() || buffers.size() != lengths.size()) {
        LOG(ERROR) << "buffers, peer_buffer_addresses and lengths have different size";
        return 0;
    }

    const int max_retry = engine_->numContexts() + 1;
    auto batch_size = buffers.size();
    std::vector<TransferRequest> entries;
    batch_id_t batch_id = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        TransferRequest entry;
        if (opcode == TransferOpcode::WRITE) {
            entry.opcode = TransferRequest::WRITE;
        } else {
            entry.opcode = TransferRequest::READ;
        }
        entry.length = lengths[i];
        entry.source = (void *)buffers[i];
        entry.target_id = handle;
        entry.target_offset = peer_buffer_addresses[i];
        entry.advise_retry_cnt = 0;
        entries.push_back(entry);
    }

    for (int retry = 0; retry < max_retry; ++retry) {
        batch_id = engine_->allocateBatchID(batch_size);
        auto batch_desc = reinterpret_cast<BatchDesc *>(batch_id);

        auto start_ts = getCurrentTimeInNano();
        batch_desc->start_timestamp = start_ts;

        Status s = engine_->submitTransfer(batch_id, entries);
        if (!s.ok()) {
            engine_->freeBatchID(batch_id);
            return 0;
        } else {
            break;
        }
    }

    return batch_id;
}

int TransferEnginePy::transferTensorSyncWrite(const char* target_hostname,
                                             pybind11::object tensor,
                                             uintptr_t peer_buffer_address) {
    try {
        // Check whether ot is pytorch tensor
        if (!(tensor.attr("__class__")
                  .attr("__name__")
                  .cast<std::string>()
                  .find("Tensor") != std::string::npos)) {
            LOG(ERROR) << "Input is not a PyTorch tensor";
            return -1;
        }

        // Get tensor metadata
        uintptr_t data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
        size_t numel = tensor.attr("numel")().cast<size_t>();
        size_t element_size = tensor.attr("element_size")().cast<size_t>();
        size_t tensor_size = numel * element_size;

        pybind11::object shape_obj = tensor.attr("shape");
        pybind11::object dtype_obj = tensor.attr("dtype");

        // Build tensor metadata
        TensorDtype dtype_enum = get_tensor_dtype(dtype_obj);
        if (dtype_enum == TensorDtype::UNKNOWN) {
            LOG(ERROR) << "Unsupported tensor dtype!";
            return -1;
        }

        // Currently support tensors with no more than 4 dimetions
        pybind11::tuple shape_tuple = pybind11::cast<pybind11::tuple>(shape_obj);
        int32_t ndim = static_cast<int32_t>(shape_tuple.size());
        if (ndim > 4) {
            LOG(ERROR) << "Tensor has more than 4 dimensions: " << ndim;
            return -1;
        }

        TensorMetadata metadata;
        metadata.dtype = static_cast<int32_t>(dtype_enum);
        metadata.ndim = ndim;

        for (int i = 0; i < 4; i++) {
            if (i < ndim) {
                metadata.shape[i] = shape_tuple[i].cast<int32_t>();
            } else {
                metadata.shape[i] = -1;
            }
        }

        // Register memory
        int ret = registerMemory(data_ptr, tensor_size);
        if (ret != 0) {
            LOG(ERROR) << "Failed to register tensor memory";
            return -1;
        }

        // Allocate temporary buffer to store metadata
        uintptr_t metadata_buffer = allocateManagedBuffer(sizeof(TensorMetadata));
        if (metadata_buffer == 0) {
            unregisterMemory(data_ptr);
            LOG(ERROR) << "Failed to allocate metadata buffer";
            return -1;
        }

        memcpy(reinterpret_cast<void*>(metadata_buffer), &metadata, sizeof(TensorMetadata));

        // Batch transfer for tensor
        std::vector<uintptr_t> local_buffers = {metadata_buffer, data_ptr};
        std::vector<uintptr_t> peer_addresses = {
            peer_buffer_address, 
            peer_buffer_address + sizeof(TensorMetadata)
        };
        std::vector<size_t> lengths = {sizeof(TensorMetadata), tensor_size};

        ret = batchTransferSync(target_hostname, local_buffers, peer_addresses, lengths,
                               TransferOpcode::WRITE);

        // Clear and release
        freeManagedBuffer(metadata_buffer, sizeof(TensorMetadata));
        unregisterMemory(data_ptr);

        return ret;

    } catch (const pybind11::error_already_set &e) {
        LOG(ERROR) << "Failed to access tensor data: " << e.what();
        return -1;
    }
}

pybind11::object TransferEnginePy::transferTensorSyncRead(const char* target_hostname,
                                                         uintptr_t peer_buffer_address,
                                                         size_t total_size) {
    try {
        // Allocate buffer for metadata and tensor
        uintptr_t metadata_buffer = allocateManagedBuffer(sizeof(TensorMetadata));
        if (metadata_buffer == 0) {
            LOG(ERROR) << "Failed to allocate metadata buffer";
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        size_t tensor_size = total_size - sizeof(TensorMetadata);
        uintptr_t tensor_buffer = allocateManagedBuffer(tensor_size);
        if (tensor_buffer == 0) {
            freeManagedBuffer(metadata_buffer, sizeof(TensorMetadata));
            LOG(ERROR) << "Failed to allocate tensor buffer";
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        // Batch transfer to get meatadata and tensor respectively
        std::vector<uintptr_t> local_buffers = {metadata_buffer, tensor_buffer};
        std::vector<uintptr_t> peer_addresses = {
            peer_buffer_address,
            peer_buffer_address + sizeof(TensorMetadata)
        };
        std::vector<size_t> lengths = {sizeof(TensorMetadata), tensor_size};

        int ret = batchTransferSync(target_hostname, local_buffers, peer_addresses, lengths,
                                   TransferOpcode::READ);
        if (ret != 0) {
            freeManagedBuffer(metadata_buffer, sizeof(TensorMetadata));
            freeManagedBuffer(tensor_buffer, tensor_size);
            LOG(ERROR) << "Failed to transfer tensor data";
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        // Parse the metadata
        TensorMetadata metadata;
        memcpy(&metadata, reinterpret_cast<void*>(metadata_buffer), sizeof(TensorMetadata));

        if (metadata.ndim < 0 || metadata.ndim > 4) {
            freeManagedBuffer(metadata_buffer, sizeof(TensorMetadata));
            freeManagedBuffer(tensor_buffer, tensor_size);
            LOG(ERROR) << "Invalid tensor metadata: ndim=" << metadata.ndim;
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        TensorDtype dtype_enum = static_cast<TensorDtype>(metadata.dtype);
        if (dtype_enum == TensorDtype::UNKNOWN) {
            freeManagedBuffer(metadata_buffer, sizeof(TensorMetadata));
            freeManagedBuffer(tensor_buffer, tensor_size);
            LOG(ERROR) << "Unknown tensor dtype!";
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        // Copy data from buffer to contiguous memory
        char* exported_data = new char[total_size];
        memcpy(exported_data, reinterpret_cast<void*>(metadata_buffer), sizeof(TensorMetadata));
        memcpy(exported_data + sizeof(TensorMetadata), reinterpret_cast<void*>(tensor_buffer), tensor_size);

        // Release buffer space
        freeManagedBuffer(metadata_buffer, sizeof(TensorMetadata));
        freeManagedBuffer(tensor_buffer, tensor_size);

        // Set up numpy array
        pybind11::object np_array;
        int dtype_index = static_cast<int>(dtype_enum);
        if (dtype_index >= 0 && dtype_index < static_cast<int>(array_creators.size())) {
            np_array = array_creators[dtype_index](exported_data, sizeof(TensorMetadata), tensor_size);
        } else {
            delete[] exported_data;
            LOG(ERROR) << "Unsupported dtype enum: " << dtype_index;
            py::gil_scoped_acquire acquire_gil;
            return pybind11::none();
        }

        // Reshape tensor data
        if (metadata.ndim > 0) {
            std::vector<int> shape_vec;
            for (int i = 0; i < metadata.ndim; i++) {
                shape_vec.push_back(metadata.shape[i]);
            }
            py::tuple shape_tuple = py::cast(shape_vec);
            np_array = np_array.attr("reshape")(shape_tuple);
        }

        py::gil_scoped_acquire acquire_gil;
        pybind11::object tensor = torch.attr("from_numpy")(np_array);
        return tensor;

    } catch (const pybind11::error_already_set &e) {
        LOG(ERROR) << "Failed to get tensor data: " << e.what();
        py::gil_scoped_acquire acquire_gil;
        return pybind11::none();
    }
}

int TransferEnginePy::getBatchTransferStatus(const std::vector<batch_id_t>& batch_ids) {
    pybind11::gil_scoped_release release;
    TransferStatus status;
    std::unordered_map<batch_id_t, int64_t> timeout_table{};
    for (auto &batch_id : batch_ids) {
        int64_t total_length = 0;
        auto batch_desc = reinterpret_cast<BatchDesc *>(batch_id);
        const size_t task_count = batch_desc->task_list.size();

        for (size_t task_id = 0; task_id < task_count; task_id++) {
            auto &task = batch_desc->task_list[task_id];
            for (auto &slice : task.slice_list) {
                total_length += slice->length;
            }
        }

        timeout_table[batch_id] = total_length + transfer_timeout_nsec_;
    }

    bool failed_or_timeout = false;
    std::unordered_set<batch_id_t> remove_ids {};
    while (!timeout_table.empty() && !failed_or_timeout) {
        for (auto &entry : timeout_table) {
            auto batch_desc = reinterpret_cast<BatchDesc *>(entry.first);
            Status s = engine_->getBatchTransferStatus(entry.first, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine_->freeBatchID(entry.first);
                LOG(INFO) << "Batch Transfer completed!";
                remove_ids.insert(entry.first);
            } else if (status.s == TransferStatusEnum::FAILED) {
                failed_or_timeout = true;
            } else if (status.s == TransferStatusEnum::TIMEOUT) {
                LOG(INFO) << "Sync data transfer timeout";
            }
            auto current_ts = getCurrentTimeInNano();
            if (current_ts - batch_desc->start_timestamp > entry.second) {
                LOG(INFO) << "Sync batch data transfer timeout after " 
                            << current_ts - batch_desc->start_timestamp << "ns";
                failed_or_timeout = true;
            }
        }

        for (auto &remove_id : remove_ids) {
            timeout_table.erase(remove_id);
        }

        remove_ids.clear();
    }

    if (failed_or_timeout) {
        for (auto &entry : timeout_table) {
            engine_->freeBatchID(entry.first);
        }
    }

    return failed_or_timeout ? -1 : 0;
}

batch_id_t TransferEnginePy::transferSubmitWrite(const char *target_hostname,
                                                 uintptr_t buffer,
                                                 uintptr_t peer_buffer_address,
                                                 size_t length) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            handle = engine_->openSegment(target_hostname);
            if (handle == (Transport::SegmentHandle)-1) return -1;
            handle_map_[target_hostname] = handle;
        }
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

int TransferEnginePy::transferCheckStatus(batch_id_t batch_id) {
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
    } else if (status.s == TransferStatusEnum::TIMEOUT) {
        return -2;
    } else {
        return 0;
    }
}

int TransferEnginePy::batchRegisterMemory(std::vector<uintptr_t> buffer_addresses,
                                          std::vector<size_t> capacities) {
    pybind11::gil_scoped_release release;
    auto batch_size = buffer_addresses.size();
    std::vector<BufferEntry> buffers;
    for (size_t i = 0; i < batch_size; i ++ ) {
        buffers.push_back(BufferEntry{(void *)buffer_addresses[i], capacities[i]});
    }
    return engine_->registerLocalMemoryBatch(buffers, kWildcardLocation);
}

int TransferEnginePy::batchUnregisterMemory(std::vector<uintptr_t> buffer_addresses) {
    pybind11::gil_scoped_release release;
    auto batch_size = buffer_addresses.size();
    std::vector<void *> buffers;
    for (size_t i = 0; i < batch_size; i ++ ) {
        buffers.push_back(reinterpret_cast<char *>(buffer_addresses[i]));
    }
    return engine_->unregisterLocalMemoryBatch(buffers);
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
    py::enum_<TensorDtype>(m, "TensorDtype", py::arithmetic())
        .value("FLOAT32", TensorDtype::FLOAT32)
        .value("FLOAT64", TensorDtype::FLOAT64)
        .value("INT8", TensorDtype::INT8)
        .value("UINT8", TensorDtype::UINT8)
        .value("INT16", TensorDtype::INT16)
        .value("UINT16", TensorDtype::UINT16)
        .value("INT32", TensorDtype::INT32)
        .value("UINT32", TensorDtype::UINT32)
        .value("INT64", TensorDtype::INT64)
        .value("UINT64", TensorDtype::UINT64)
        .value("BOOL", TensorDtype::BOOL)
        .value("UNKNOWN", TensorDtype::UNKNOWN)
        .export_values();
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
            .def("batch_transfer_sync_write", &TransferEnginePy::batchTransferSyncWrite)
            .def("batch_transfer_sync_read", &TransferEnginePy::batchTransferSyncRead)
            .def("batch_transfer_async_write", &TransferEnginePy::batchTransferAsyncWrite)
            .def("batch_transfer_async_read", &TransferEnginePy::batchTransferAsyncRead)
            .def("transfer_sync", &TransferEnginePy::transferSync)
            .def("batch_transfer_sync", &TransferEnginePy::batchTransferSync)
            .def("batch_transfer_async", &TransferEnginePy::batchTransferAsync)
            .def("transfer_tensor_sync_write", &TransferEnginePy::transferTensorSyncWrite)
            .def("transfer_tensor_sync_read", &TransferEnginePy::transferTensorSyncRead)
            .def("get_batch_transfer_status", &TransferEnginePy::getBatchTransferStatus)
            .def("transfer_submit_write",
                 &TransferEnginePy::transferSubmitWrite)
            .def("transfer_check_status",
                 &TransferEnginePy::transferCheckStatus)
            .def("write_bytes_to_buffer", &TransferEnginePy::writeBytesToBuffer)
            .def("read_bytes_from_buffer",
                 &TransferEnginePy::readBytesFromBuffer)
            .def("register_memory", &TransferEnginePy::registerMemory)
            .def("unregister_memory", &TransferEnginePy::unregisterMemory)
            .def("batch_register_memory", &TransferEnginePy::batchRegisterMemory)
            .def("batch_unregister_memory", &TransferEnginePy::batchUnregisterMemory)
            .def("get_first_buffer_address",
                 &TransferEnginePy::getFirstBufferAddress);

    adaptor_cls.attr("TransferOpcode") = transfer_opcode;
}
