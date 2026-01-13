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
#include "transport/rpc_communicator/rpc_interface.h"

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
#if !defined(USE_ASCEND) && !defined(USE_ASCEND_DIRECT) && \
    !defined(USE_ASCEND_HETEROGENEOUS)
    bool pass_alloc = false;
    const char *pass_alloc_env = std::getenv("PASS_ALLOC");
    if (pass_alloc_env) {
        try {
            if (std::stoi(pass_alloc_env) != 0) {
                pass_alloc = true;
            }
        } catch (const std::exception &) {
            LOG(WARNING) << "Ignore value from environment variable "
                            "PASS_ALLOC";
        }
    }
    if (!pass_alloc) {
        doBuddyAllocate(kMaxClassId);
    }
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

int TransferEnginePy::batchTransferSyncWrite(
    const char *target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths) {
    return batchTransferSync(target_hostname, buffers, peer_buffer_addresses,
                             lengths, TransferOpcode::WRITE);
}

int TransferEnginePy::batchTransferSyncRead(
    const char *target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths) {
    return batchTransferSync(target_hostname, buffers, peer_buffer_addresses,
                             lengths, TransferOpcode::READ);
}

batch_id_t TransferEnginePy::batchTransferAsyncWrite(
    const char *target_hostname, const std::vector<uintptr_t> &buffers,
    const std::vector<uintptr_t> &peer_buffer_addresses,
    const std::vector<size_t> &lengths) {
    return batchTransferAsync(target_hostname, buffers, peer_buffer_addresses,
                              lengths, TransferOpcode::WRITE);
}

batch_id_t TransferEnginePy::batchTransferAsyncRead(
    const char *target_hostname, const std::vector<uintptr_t> &buffers,
    const std::vector<uintptr_t> &peer_buffer_addresses,
    const std::vector<size_t> &lengths) {
    return batchTransferAsync(target_hostname, buffers, peer_buffer_addresses,
                              lengths, TransferOpcode::READ);
}

int TransferEnginePy::transferSync(const char *target_hostname,
                                   uintptr_t buffer,
                                   uintptr_t peer_buffer_address, size_t length,
                                   TransferOpcode opcode,
                                   TransferNotify *notify) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            LOG(INFO)
                << "transferSync, cache not found, openSegment with target "
                << target_hostname;
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

        Status s =
            notify
                ? engine_->submitTransferWithNotify(
                      batch_id, {entry},
                      TransferMetadata::NotifyDesc{notify->name, notify->msg})
                : engine_->submitTransfer(batch_id, {entry});
        if (!s.ok()) {
            Status segment_status = engine_->CheckSegmentStatus(handle);
            if (!segment_status.ok()) {
                LOG(WARNING)
                    << "submitTransfer failed with target " << target_hostname
                    << ", CheckSegmentStatus not ok, ready to closeSegment";
                std::lock_guard<std::mutex> guard(mutex_);
                engine_->closeSegment(handle);
                engine_->getMetadata()->removeSegmentDesc(target_hostname);
                handle_map_.erase(target_hostname);
            }
            return -1;
        }

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

int TransferEnginePy::batchTransferSync(
    const char *target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths,
    TransferOpcode opcode, TransferNotify *notify) {
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

    if (buffers.size() != peer_buffer_addresses.size() ||
        buffers.size() != lengths.size()) {
        LOG(ERROR)
            << "buffers, peer_buffer_addresses and lengths have different size";
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
        Status s =
            notify
                ? engine_->submitTransferWithNotify(
                      batch_id, entries,
                      TransferMetadata::NotifyDesc{notify->name, notify->msg})
                : engine_->submitTransfer(batch_id, entries);
        if (!s.ok()) {
            engine_->freeBatchID(batch_id);
            Status segment_status = engine_->CheckSegmentStatus(handle);
            if (!segment_status.ok()) {
                LOG(WARNING)
                    << "submitTransfer failed with target " << target_hostname
                    << ", CheckSegmentStatus not ok, ready to closeSegment";
                std::lock_guard<std::mutex> guard(mutex_);
                engine_->closeSegment(handle);
                engine_->getMetadata()->removeSegmentDesc(target_hostname);
                handle_map_.erase(target_hostname);
            }
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
            const int64_t timeout =
                transfer_timeout_nsec_ + total_length;  // 1GiB per second
            if (current_ts - start_ts > timeout) {
                LOG(INFO) << "Sync batch data transfer timeout after "
                          << current_ts - start_ts << "ns";
                // TODO: as @doujiang24 mentioned, early free(while there are
                // still waiting tasks) the batch_id may fail and cause memory
                // leak(a known issue).
                if (!already_freed) {
                    engine_->freeBatchID(batch_id);
                }
                return -1;
            }
        }
    }
    return -1;
}

batch_id_t TransferEnginePy::batchTransferAsync(
    const char *target_hostname, const std::vector<uintptr_t> &buffers,
    const std::vector<uintptr_t> &peer_buffer_addresses,
    const std::vector<size_t> &lengths, TransferOpcode opcode) {
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

    if (buffers.size() != peer_buffer_addresses.size() ||
        buffers.size() != lengths.size()) {
        LOG(ERROR)
            << "buffers, peer_buffer_addresses and lengths have different size";
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

int TransferEnginePy::getBatchTransferStatus(
    const std::vector<batch_id_t> &batch_ids) {
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
    std::unordered_set<batch_id_t> remove_ids{};
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

int TransferEnginePy::batchRegisterMemory(
    std::vector<uintptr_t> buffer_addresses, std::vector<size_t> capacities) {
    pybind11::gil_scoped_release release;
    auto batch_size = buffer_addresses.size();
    std::vector<BufferEntry> buffers;
    for (size_t i = 0; i < batch_size; i++) {
        buffers.push_back(
            BufferEntry{(void *)buffer_addresses[i], capacities[i]});
    }
    return engine_->registerLocalMemoryBatch(buffers, kWildcardLocation);
}

int TransferEnginePy::batchUnregisterMemory(
    std::vector<uintptr_t> buffer_addresses) {
    pybind11::gil_scoped_release release;
    auto batch_size = buffer_addresses.size();
    std::vector<void *> buffers;
    for (size_t i = 0; i < batch_size; i++) {
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

std::string TransferEnginePy::getLocalTopology(const char *device_name) {
    pybind11::gil_scoped_release release;
    auto device_name_safe = device_name ? std::string(device_name) : "";
    auto device_filter = buildDeviceFilter(device_name_safe);
    std::shared_ptr<TransferEngine> tmp_engine =
        std::make_shared<TransferEngine>(true, device_filter);

    std::string metadata_conn_string{"P2PHANDSHAKE"}, local_server_name{};
    tmp_engine->init(metadata_conn_string, local_server_name);

    return tmp_engine->getLocalTopology()->toString();
}

std::vector<TransferEnginePy::TransferNotify> TransferEnginePy::getNotifies() {
    std::vector<TransferMetadata::NotifyDesc> notifies;
    std::vector<TransferNotify> result;

    int ret = engine_->getNotifies(notifies);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get notifies: " << ret;
        return result;
    }

    for (const auto &notify : notifies) {
        result.emplace_back(
            TransferEnginePy::TransferNotify{notify.name, notify.notify_msg});
    }

    return result;
}

namespace py = pybind11;

// Implementation of coro_rpc_interface binding function
void bind_coro_rpc_interface(py::module_ &m) {
    // Note: RpcInterface, ReceivedData and ReceivedTensor are already
    // registered by bind_rpc_interface() so we don't register them again here
    // to avoid duplicate type registration errors. The factory functions are
    // also registered by bind_rpc_interface(), so we don't need to register
    // them again.

    // Add CoroRPCInterface as an alias to RpcInterface
    m.attr("CoroRPCInterface") = m.attr("RpcInterface");
}

PYBIND11_MODULE(engine, m) {
    py::enum_<TransferEnginePy::TransferOpcode> transfer_opcode(
        m, "TransferOpcode", py::arithmetic());
    transfer_opcode.value("Read", TransferEnginePy::TransferOpcode::READ)
        .value("Write", TransferEnginePy::TransferOpcode::WRITE)
        .export_values();

    py::class_<TransferEnginePy::TransferNotify>(m, "TransferNotify")
        .def(py::init<>())
        .def(py::init<const std::string &, const std::string &>(),
             py::arg("name"), py::arg("msg"))
        .def_readwrite("name", &TransferEnginePy::TransferNotify::name)
        .def_readwrite("msg", &TransferEnginePy::TransferNotify::msg);

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
            .def("batch_transfer_sync_write",
                 &TransferEnginePy::batchTransferSyncWrite)
            .def("batch_transfer_sync_read",
                 &TransferEnginePy::batchTransferSyncRead)
            .def("batch_transfer_async_write",
                 &TransferEnginePy::batchTransferAsyncWrite)
            .def("batch_transfer_async_read",
                 &TransferEnginePy::batchTransferAsyncRead)
            .def("transfer_sync", &TransferEnginePy::transferSync,
                 py::arg("target_hostname"), py::arg("buffer"),
                 py::arg("peer_buffer_address"), py::arg("length"),
                 py::arg("opcode"), py::arg("notify") = nullptr)
            .def("batch_transfer_sync", &TransferEnginePy::batchTransferSync)
            .def("batch_transfer_async", &TransferEnginePy::batchTransferAsync)
            .def("get_batch_transfer_status",
                 &TransferEnginePy::getBatchTransferStatus)
            .def("transfer_submit_write",
                 &TransferEnginePy::transferSubmitWrite)
            .def("transfer_check_status",
                 &TransferEnginePy::transferCheckStatus)
            .def("write_bytes_to_buffer", &TransferEnginePy::writeBytesToBuffer)
            .def("read_bytes_from_buffer",
                 &TransferEnginePy::readBytesFromBuffer)
            .def("register_memory", &TransferEnginePy::registerMemory)
            .def("unregister_memory", &TransferEnginePy::unregisterMemory)
            .def("batch_register_memory",
                 &TransferEnginePy::batchRegisterMemory)
            .def("batch_unregister_memory",
                 &TransferEnginePy::batchUnregisterMemory)
            .def("get_local_topology", &TransferEnginePy::getLocalTopology,
                 py::arg("device_name") = nullptr)
            .def("get_first_buffer_address",
                 &TransferEnginePy::getFirstBufferAddress)
            .def("get_notifies", &TransferEnginePy::getNotifies)
            .def("get_engine", &TransferEnginePy::getEngine);

    adaptor_cls.attr("TransferOpcode") = transfer_opcode;

    py::class_<TransferEngine, std::shared_ptr<TransferEngine>>(
        m, "InnerTransferEngine");

    // Bind RpcInterface (this also registers ReceivedData, ReceivedTensor, and
    // factory functions)
    mooncake::bind_rpc_interface(m);

    // Add CoroRPCInterface as an alias to RpcInterface if needed
    bind_coro_rpc_interface(m);
}
