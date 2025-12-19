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

#include "tent/transfer_engine.h"
#include "tent/common/config.h"

#include <cassert>
#include <numeric>
#include <fstream>

#include <pybind11/stl.h>
#include "transport/rpc_communicator/rpc_interface.h"

#ifdef USE_MNNVL
#include "transport/nvlink_transport/nvlink_transport.h"
static void* allocateMemory(size_t size) {
    return mooncake::NvlinkTransport::allocatePinnedLocalMemory(size);
}
static void freeMemory(void* ptr) {
    mooncake::NvlinkTransport::freePinnedLocalMemory(ptr);
}
#else
static void* allocateMemory(size_t size) { return malloc(size); }
static void freeMemory(void* ptr) { free(ptr); }
#endif

using SegmentHandle = Transport::SegmentHandle;
std::vector<std::string> buildDeviceFilter(const std::string& device_names);

struct TEBackend {
    virtual ~TEBackend() = default;
    virtual int getRpcPort() = 0;

    virtual int openSegment(SegmentHandle& h, const char* target_hostname) = 0;
    virtual void closeSegment(SegmentHandle h) = 0;

    virtual int registerLocalMemory(void* ptr, size_t cap) = 0;
    virtual int unregisterLocalMemory(void* ptr) = 0;
    virtual int registerLocalMemoryBatch(const std::vector<uintptr_t>& addrs,
                                         const std::vector<size_t>& caps) = 0;
    virtual int unregisterLocalMemoryBatch(
        const std::vector<uintptr_t>& addrs) = 0;

    // High-level helper functions for batch data transfer
    virtual int transferBatchSync(SegmentHandle h,
                                  const std::vector<uintptr_t>& buffers,
                                  const std::vector<uintptr_t>& peer_addrs,
                                  const std::vector<size_t>& lengths,
                                  TransferEnginePy::TransferOpcode opcode,
                                  TransferEnginePy::TransferNotify* notify,
                                  uint64_t timeout_nsec,
                                  const char* target_hostname) = 0;

    virtual batch_id_t transferBatchAsync(
        SegmentHandle h, const std::vector<uintptr_t>& buffers,
        const std::vector<uintptr_t>& peer_addrs,
        const std::vector<size_t>& lengths,
        TransferEnginePy::TransferOpcode opcode,
        TransferEnginePy::TransferNotify* notify) = 0;

    virtual int transferCompletion(const std::vector<batch_id_t>& batch_ids,
                                   uint64_t timeout_nsec) = 0;

    // Direct transfer verbs
    virtual batch_id_t allocateBatchID(size_t batch_size) = 0;
    virtual int freeBatchID(batch_id_t batch_id) = 0;
    virtual int submitTransfer(batch_id_t batch_id, SegmentHandle h,
                               const std::vector<uintptr_t>& buffers,
                               const std::vector<uintptr_t>& peer_addrs,
                               const std::vector<size_t>& lengths,
                               TransferEnginePy::TransferOpcode opcode,
                               TransferEnginePy::TransferNotify* notify) = 0;
    virtual int getTransferStatus(batch_id_t batch_id) = 0;

    virtual uintptr_t getFirstBufferAddress(
        const std::string& segment_name) = 0;
    virtual std::string getLocalTopology(const char* device_name) = 0;
    virtual std::vector<TransferEnginePy::TransferNotify> getNotifies() = 0;
    virtual std::shared_ptr<TransferEngine> getEngine() const {
        return nullptr;
    }
};

struct TEBackendClassic final : TEBackend {
    explicit TEBackendClassic(std::shared_ptr<TransferEngine> engine)
        : engine_(std::move(engine)) {}

    int getRpcPort() override { return engine_->getRpcPort(); }

    int openSegment(SegmentHandle& h, const char* target) override {
        h = engine_->openSegment(target);
        return (h == (SegmentHandle)-1) ? -1 : 0;
    }

    void closeSegment(SegmentHandle h) override { engine_->closeSegment(h); }

    int registerLocalMemory(void* ptr, size_t cap) override {
        return engine_->registerLocalMemory(ptr, cap, kWildcardLocation);
    }

    int unregisterLocalMemory(void* ptr) override {
        return engine_->unregisterLocalMemory(ptr);
    }

    int registerLocalMemoryBatch(const std::vector<uintptr_t>& addrs,
                                 const std::vector<size_t>& caps) override {
        std::vector<BufferEntry> buffers;
        buffers.reserve(addrs.size());
        for (size_t i = 0; i < addrs.size(); ++i)
            buffers.push_back(BufferEntry{(void*)addrs[i], caps[i]});
        return engine_->registerLocalMemoryBatch(buffers, kWildcardLocation);
    }

    int unregisterLocalMemoryBatch(
        const std::vector<uintptr_t>& addrs) override {
        std::vector<void*> buffers;
        buffers.reserve(addrs.size());
        for (auto a : addrs) buffers.push_back((void*)a);
        return engine_->unregisterLocalMemoryBatch(buffers);
    }

    int transferBatchSync(SegmentHandle h,
                          const std::vector<uintptr_t>& buffers,
                          const std::vector<uintptr_t>& peer_addrs,
                          const std::vector<size_t>& lengths,
                          TransferEnginePy::TransferOpcode opcode,
                          TransferEnginePy::TransferNotify* notify,
                          uint64_t timeout_nsec,
                          const char* target_hostname) override {
        const int max_retry = engine_->numContexts() + 1;
        for (int retry = 0; retry < max_retry; ++retry) {
            batch_id_t batch_id = transferBatchAsync(h, buffers, peer_addrs,
                                                     lengths, opcode, notify);
            if (batch_id == 0) {
                auto segment_status = engine_->CheckSegmentStatus(h);
                if (!segment_status.ok()) {
                    LOG(WARNING) << "submitTransfer failed with target "
                                 << target_hostname
                                 << ", CheckSegmentStatus not ok, closeSegment";
                    engine_->closeSegment(h);
                    engine_->getMetadata()->removeSegmentDesc(target_hostname);
                }
                return -1;
            }
            int rc = transferCompletion(std::vector<batch_id_t>{batch_id},
                                        timeout_nsec);
            if (rc == 0) return 0;
        }
        return -1;
    }

    batch_id_t transferBatchAsync(
        SegmentHandle h, const std::vector<uintptr_t>& buffers,
        const std::vector<uintptr_t>& peer_addrs,
        const std::vector<size_t>& lengths,
        TransferEnginePy::TransferOpcode opcode,
        TransferEnginePy::TransferNotify* notify) override {
        if (buffers.size() != peer_addrs.size() ||
            buffers.size() != lengths.size()) {
            LOG(ERROR) << "buffers, peer_addrs and lengths have different size";
            return 0;
        }
        auto batch_id = allocateBatchID(buffers.size());
        int ret = submitTransfer(batch_id, h, buffers, peer_addrs, lengths,
                                 opcode, notify);
        if (ret != 0) {
            freeBatchID(batch_id);
            return 0;
        }
        return batch_id;
    }

    int transferCompletion(const std::vector<batch_id_t>& batch_ids,
                           uint64_t timeout_nsec) override {
        TransferStatus status;
        std::unordered_map<batch_id_t, int64_t> timeout_table;

        for (auto& batch_id : batch_ids) {
            int64_t total_length = 0;
            auto batch_desc =
                reinterpret_cast<TransferEnginePy::BatchDesc*>(batch_id);
            for (auto& task : batch_desc->task_list) {
                for (auto& slice : task.slice_list)
                    total_length += slice->length;
            }
            timeout_table[batch_id] = total_length + timeout_nsec;
        }

        bool failed_or_timeout = false;
        std::unordered_set<batch_id_t> remove_ids;

        while (!timeout_table.empty() && !failed_or_timeout) {
            for (auto& kv : timeout_table) {
                auto batch_id = kv.first;
                auto batch_desc =
                    reinterpret_cast<TransferEnginePy::BatchDesc*>(batch_id);

                auto s = engine_->getBatchTransferStatus(batch_id, status);
                LOG_ASSERT(s.ok());
                if (status.s == TransferStatusEnum::COMPLETED) {
                    engine_->freeBatchID(batch_id);
                    remove_ids.insert(batch_id);
                } else if (status.s == TransferStatusEnum::FAILED) {
                    failed_or_timeout = true;
                } else if (status.s == TransferStatusEnum::TIMEOUT) {
                    LOG(INFO) << "Sync data transfer timeout";
                }

                auto current_ts = getCurrentTimeInNano();
                if (current_ts - batch_desc->start_timestamp > kv.second) {
                    LOG(INFO)
                        << "Sync batch data transfer timeout after "
                        << current_ts - batch_desc->start_timestamp << "ns";
                    failed_or_timeout = true;
                }
            }

            for (auto& id : remove_ids) timeout_table.erase(id);
            remove_ids.clear();
        }

        if (failed_or_timeout) {
            for (auto& kv : timeout_table) engine_->freeBatchID(kv.first);
        }
        return failed_or_timeout ? -1 : 0;
    }

    batch_id_t allocateBatchID(size_t batch_size) override {
        return engine_->allocateBatchID(batch_size);
    }

    int freeBatchID(batch_id_t batch_id) override {
        auto status = engine_->freeBatchID(batch_id);
        return status.ok() ? 0 : -1;
    }

    int submitTransfer(batch_id_t batch_id, SegmentHandle h,
                       const std::vector<uintptr_t>& buffers,
                       const std::vector<uintptr_t>& peer_addrs,
                       const std::vector<size_t>& lengths,
                       TransferEnginePy::TransferOpcode opcode,
                       TransferEnginePy::TransferNotify* notify) override {
        if (buffers.size() != peer_addrs.size() ||
            buffers.size() != lengths.size()) {
            LOG(ERROR) << "buffers, peer_addrs and lengths have different size";
            return -1;
        }

        std::vector<TransferRequest> entries;
        entries.reserve(buffers.size());
        for (size_t i = 0; i < buffers.size(); ++i) {
            TransferRequest e;
            e.opcode = (opcode == TransferEnginePy::TransferOpcode::WRITE)
                           ? TransferRequest::WRITE
                           : TransferRequest::READ;
            e.length = lengths[i];
            e.source = (void*)buffers[i];
            e.target_id = h;
            e.target_offset = peer_addrs[i];
            e.advise_retry_cnt = 0;
            entries.push_back(e);
        }

        auto s =
            notify
                ? engine_->submitTransferWithNotify(
                      batch_id, entries,
                      TransferMetadata::NotifyDesc{notify->name, notify->msg})
                : engine_->submitTransfer(batch_id, entries);
        return s.ok() ? 0 : -1;
    }

    int getTransferStatus(batch_id_t batch_id) override {
        TransferStatus status;
        auto s = engine_->getBatchTransferStatus(batch_id, status);
        LOG_ASSERT(s.ok());
        if (status.s == TransferStatusEnum::COMPLETED) {
            engine_->freeBatchID(batch_id);
            return 1;
        } else if (status.s == TransferStatusEnum::FAILED) {
            engine_->freeBatchID(batch_id);
            return -1;
        } else if (status.s == TransferStatusEnum::TIMEOUT) {
            return -2;
        }
        return 0;
    }

    uintptr_t getFirstBufferAddress(const std::string& segment_name) override {
        auto segment_id = engine_->openSegment(segment_name.c_str());
        auto segment_desc =
            engine_->getMetadata()->getSegmentDescByID(segment_id);
        return segment_desc->buffers[0].addr;
    }

    std::string getLocalTopology(const char* device_name) override {
        auto device_name_safe = device_name ? std::string(device_name) : "";
        auto device_filter = buildDeviceFilter(device_name_safe);
        std::shared_ptr<TransferEngine> tmp =
            std::make_shared<TransferEngine>(true, device_filter);
        tmp->init("P2PHANDSHAKE", "");
        return tmp->getLocalTopology()->toString();
    }

    std::vector<TransferEnginePy::TransferNotify> getNotifies() override {
        std::vector<TransferMetadata::NotifyDesc> notifies;
        std::vector<TransferEnginePy::TransferNotify> result;
        int ret = engine_->getNotifies(notifies);
        if (ret != 0) return result;
        for (auto& n : notifies) result.push_back({n.name, n.notify_msg});
        return result;
    }

    std::shared_ptr<TransferEngine> getEngine() const override {
        return engine_;
    }

    std::shared_ptr<TransferEngine> engine_;
};

struct TEBackendNext final : TEBackend {
    explicit TEBackendNext(
        std::shared_ptr<mooncake::tent::TransferEngine> engine)
        : engine_(std::move(engine)) {}

    int getRpcPort() override { return engine_->getRpcServerPort(); }

    int openSegment(SegmentHandle& h, const char* target) override {
        auto st = engine_->openSegment(h, target);
        return st.ok() ? 0 : -1;
    }

    void closeSegment(SegmentHandle h) override { engine_->closeSegment(h); }

    int registerLocalMemory(void* ptr, size_t cap) override {
        auto st = engine_->registerLocalMemory(ptr, cap);
        return st.ok() ? 0 : -1;
    }

    int unregisterLocalMemory(void* ptr) override {
        auto st = engine_->unregisterLocalMemory(ptr);
        return st.ok() ? 0 : -1;
    }

    int registerLocalMemoryBatch(const std::vector<uintptr_t>& addrs,
                                 const std::vector<size_t>& caps) override {
        std::vector<void*> buffers;
        buffers.reserve(addrs.size());
        for (auto a : addrs) buffers.push_back((void*)a);
        auto st = engine_->registerLocalMemory(buffers, caps);
        return st.ok() ? 0 : -1;
    }

    int unregisterLocalMemoryBatch(
        const std::vector<uintptr_t>& addrs) override {
        std::vector<void*> buffers;
        buffers.reserve(addrs.size());
        for (auto a : addrs) buffers.push_back((void*)a);
        auto st = engine_->unregisterLocalMemory(buffers);
        return st.ok() ? 0 : -1;
    }

    int transferBatchSync(SegmentHandle h,
                          const std::vector<uintptr_t>& buffers,
                          const std::vector<uintptr_t>& peer_addrs,
                          const std::vector<size_t>& lengths,
                          TransferEnginePy::TransferOpcode opcode,
                          TransferEnginePy::TransferNotify* /*notify*/,
                          uint64_t timeout_nsec,
                          const char* target_hostname) override {
        auto batch_id = transferBatchAsync(h, buffers, peer_addrs, lengths,
                                           opcode, nullptr);
        if (batch_id == 0) {
            LOG(WARNING) << "submitTransfer failed with target "
                         << target_hostname
                         << ", CheckSegmentStatus not ok, closeSegment";
            return -1;
        }
        return transferCompletion(std::vector<batch_id_t>{batch_id},
                                  timeout_nsec);
    }

    batch_id_t transferBatchAsync(
        SegmentHandle h, const std::vector<uintptr_t>& buffers,
        const std::vector<uintptr_t>& peer_addrs,
        const std::vector<size_t>& lengths,
        TransferEnginePy::TransferOpcode opcode,
        TransferEnginePy::TransferNotify* notify) override {
        if (buffers.size() != peer_addrs.size() ||
            buffers.size() != lengths.size()) {
            LOG(ERROR) << "buffers, peer_addrs and lengths have different size";
            return 0;
        }
        auto batch_id = allocateBatchID(buffers.size());
        int ret = submitTransfer(batch_id, h, buffers, peer_addrs, lengths,
                                 opcode, notify);
        if (ret != 0) {
            freeBatchID(batch_id);
            return 0;
        }
        return batch_id;
    }

    int transferCompletion(const std::vector<batch_id_t>& batch_ids,
                           uint64_t timeout_nsec) override {
        uint64_t start_ts = getCurrentTimeInNano();
        for (auto id : batch_ids) {
            bool completed = false;
            while (!completed) {
                uint64_t current_ts = getCurrentTimeInNano();
                if (current_ts - start_ts > timeout_nsec) {
                    LOG(INFO) << "Sync batch data transfer timeout after "
                              << current_ts - start_ts << "ns";
                    engine_->freeBatch(id);
                    return -1;
                }
                mooncake::tent::TransferStatus status;
                auto ret = engine_->getTransferStatus(id, status);
                if (!ret.ok()) {
                    engine_->freeBatch(id);
                    return -1;
                }
                engine_->freeBatch(id);
                if (status.s == mooncake::tent::COMPLETED)
                    completed = true;
                else if (status.s == mooncake::tent::PENDING)
                    continue;
                else {
                    LOG(INFO) << "Data transfer failed with status "
                              << static_cast<int>(status.s);
                    return -1;
                }
            }
        }
        return 0;
    }

    batch_id_t allocateBatchID(size_t batch_size) override {
        return engine_->allocateBatch(batch_size);
    }

    int freeBatchID(batch_id_t batch_id) override {
        auto status = engine_->freeBatch(batch_id);
        return status.ok() ? 0 : -1;
    }

    int submitTransfer(batch_id_t batch_id, SegmentHandle h,
                       const std::vector<uintptr_t>& buffers,
                       const std::vector<uintptr_t>& peer_addrs,
                       const std::vector<size_t>& lengths,
                       TransferEnginePy::TransferOpcode opcode,
                       TransferEnginePy::TransferNotify* /*notify*/) override {
        if (buffers.size() != peer_addrs.size() ||
            buffers.size() != lengths.size()) {
            LOG(ERROR) << "buffers, peer_addrs and lengths have different size";
            return -1;
        }

        std::vector<mooncake::tent::Request> entries;
        entries.reserve(buffers.size());
        for (size_t i = 0; i < buffers.size(); ++i) {
            mooncake::tent::Request e;
            e.opcode = (opcode == TransferEnginePy::TransferOpcode::WRITE)
                           ? mooncake::tent::Request::WRITE
                           : mooncake::tent::Request::READ;
            e.length = lengths[i];
            e.source = (void*)buffers[i];
            e.target_id = h;
            e.target_offset = peer_addrs[i];
            entries.push_back(e);
        }

        auto ret = engine_->submitTransfer(batch_id, entries);
        return ret.ok() ? 0 : -1;
    }

    int getTransferStatus(batch_id_t batch_id) override {
        mooncake::tent::TransferStatus status;
        auto ret = engine_->getTransferStatus(batch_id, 0, status);
        if (!ret.ok()) return -1;
        if (status.s == mooncake::tent::COMPLETED) {
            engine_->freeBatch(batch_id);
            return 1;
        } else if (status.s == mooncake::tent::FAILED) {
            engine_->freeBatch(batch_id);
            return -1;
        } else if (status.s == mooncake::tent::TIMEOUT) {
            return -2;
        }
        return 0;
    }

    uintptr_t getFirstBufferAddress(const std::string& segment_name) override {
        SegmentHandle h = 0;
        mooncake::tent::SegmentInfo info;
        auto st = engine_->openSegment(h, segment_name);
        if (!st.ok()) return 0;
        st = engine_->getSegmentInfo(h, info);
        if (!st.ok() || info.buffers.empty()) return 0;
        return info.buffers[0].base;
    }

    std::string getLocalTopology(const char* /*device_name*/) override {
        return "{unsupported}";
    }

    std::vector<TransferEnginePy::TransferNotify> getNotifies() override {
        std::vector<TransferEnginePy::TransferNotify> result;
        std::vector<mooncake::tent::Notification> notifies;
        auto st = engine_->receiveNotification(notifies);
        if (!st.ok()) return result;
        for (auto& n : notifies) result.push_back({n, n});
        return result;
    }

    std::shared_ptr<mooncake::tent::TransferEngine> engine_;
};

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
    for (auto& handle : handle_map_) engine_->closeSegment(handle.second);
    handle_map_.clear();
    engine_.reset();
    for (auto& buffer : buffer_list_) freeMemory(buffer);
    buffer_list_.clear();
    for (auto& buffer : large_buffer_list_) freeMemory(buffer);
    large_buffer_list_.clear();
}

std::vector<std::string> buildDeviceFilter(const std::string& device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }
    return tokens;
}

std::pair<std::string, std::string> parseConnectionString(
    const std::string& conn_string) {
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

std::string buildConnString(const std::string& metadata_type,
                            const std::string& metadata_server) {
    if (metadata_server == P2PHANDSHAKE) {
        return P2PHANDSHAKE;
    }

    std::string conn_string = metadata_server;
    if (conn_string.find("://") == std::string::npos)
        conn_string = metadata_type + "://" + metadata_server;
    return conn_string;
}

int TransferEnginePy::initialize(const char* local_hostname,
                                 const char* metadata_server,
                                 const char* protocol,
                                 const char* device_name) {
    auto conn_string = parseConnectionString(metadata_server);
    return initializeExt(local_hostname, conn_string.second.c_str(), protocol,
                         device_name, conn_string.first.c_str());
}

int TransferEnginePy::initializeExt(const char* local_hostname,
                                    const char* metadata_server,
                                    const char* protocol,
                                    const char* device_name,
                                    const char* metadata_type) {
    (void)(protocol);
    bool use_tent = getenv("MC_USE_TENT") != nullptr;
    std::string conn_string = buildConnString(metadata_type, metadata_server);
    auto device_name_safe = device_name ? std::string(device_name) : "";
    auto device_filter = buildDeviceFilter(device_name_safe);
    if (use_tent) {
        auto config = std::make_shared<mooncake::tent::Config>();
        if (strcmp(metadata_server, P2PHANDSHAKE) == 0) {
            config->set("local_segment_name", local_hostname);
            config->set("metadata_type", "p2p");
        } else {
            if (metadata_type && strlen(metadata_type) > 0)
                config->set("metadata_type", metadata_type);
            if (metadata_server && strlen(metadata_server) > 0)
                config->set("metadata_servers", metadata_server);
            if (local_hostname && strlen(local_hostname) > 0)
                config->set("local_segment_name", local_hostname);
        }
        auto te_engine =
            std::make_shared<mooncake::tent::TransferEngine>(config);
        engine_ = std::make_shared<TEBackendNext>(te_engine);
        if (getenv("MC_LEGACY_RPC_PORT_BINDING")) {
            LOG(WARNING)
                << "MC_LEGACY_RPC_PORT_BINDING is ignored in TENT mode";
        }
    } else {
        auto te_engine = std::make_shared<TransferEngine>(true, device_filter);
        engine_ = std::make_shared<TEBackendClassic>(te_engine);
        if (getenv("MC_LEGACY_RPC_PORT_BINDING")) {
            auto hostname_port = parseHostNameWithPort(local_hostname);
            int ret = te_engine->init(conn_string, local_hostname,
                                      hostname_port.first.c_str(),
                                      hostname_port.second);
            if (ret) return -1;
        } else {
            // the last two params are unused
            int ret = te_engine->init(conn_string, local_hostname, "", 0);
            if (ret) return -1;
        }
    }

    free_list_.resize(kSlabSizeKBTabLen);
#if !defined(USE_ASCEND) && !defined(USE_ASCEND_DIRECT) && \
    !defined(USE_ASCEND_HETEROGENEOUS)
    bool pass_alloc = false;
    const char* pass_alloc_env = std::getenv("PASS_ALLOC");
    if (pass_alloc_env) {
        try {
            if (std::stoi(pass_alloc_env) != 0) {
                pass_alloc = true;
            }
        } catch (const std::exception&) {
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

char* TransferEnginePy::allocateRawBuffer(size_t capacity) {
    auto buffer = allocateMemory(capacity);
    if (!buffer) return nullptr;
    int ret = 0;
    ret = engine_->registerLocalMemory(buffer, capacity);
    if (ret) {
        freeMemory(buffer);
        return nullptr;
    }
    return (char*)buffer;
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
    char* buffer = free_list_[class_id + 1].top();
    free_list_[class_id + 1].pop();
    free_list_[class_id].push(buffer);
    free_list_[class_id].push(buffer + kSlabSizeKB[class_id] * 1024);
    return 0;
}

uintptr_t TransferEnginePy::allocateManagedBuffer(size_t length) {
    std::lock_guard<std::mutex> guard(mutex_);
    int class_id = findClassId(length);
    if (class_id < 0) {
        char* buffer = allocateRawBuffer(length);
        if (buffer) large_buffer_list_.insert(buffer);
        return (uintptr_t)buffer;
    }
    if (free_list_[class_id].empty())
        if (doBuddyAllocate(class_id)) return 0;
    assert(!free_list_[class_id].empty());
    char* buffer = free_list_[class_id].top();
    free_list_[class_id].pop();
    return (uintptr_t)buffer;
}

int TransferEnginePy::freeManagedBuffer(uintptr_t buffer_addr, size_t length) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto buffer = (char*)buffer_addr;
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

int TransferEnginePy::transferSyncWrite(const char* target_hostname,
                                        uintptr_t buffer,
                                        uintptr_t peer_buffer_address,
                                        size_t length) {
    return transferSync(target_hostname, buffer, peer_buffer_address, length,
                        TransferOpcode::WRITE);
}

int TransferEnginePy::transferSyncRead(const char* target_hostname,
                                       uintptr_t buffer,
                                       uintptr_t peer_buffer_address,
                                       size_t length) {
    return transferSync(target_hostname, buffer, peer_buffer_address, length,
                        TransferOpcode::READ);
}

int TransferEnginePy::batchTransferSyncWrite(
    const char* target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths) {
    return batchTransferSync(target_hostname, buffers, peer_buffer_addresses,
                             lengths, TransferOpcode::WRITE);
}

int TransferEnginePy::batchTransferSyncRead(
    const char* target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths) {
    return batchTransferSync(target_hostname, buffers, peer_buffer_addresses,
                             lengths, TransferOpcode::READ);
}

batch_id_t TransferEnginePy::batchTransferAsyncWrite(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths) {
    return batchTransferAsync(target_hostname, buffers, peer_buffer_addresses,
                              lengths, TransferOpcode::WRITE);
}

batch_id_t TransferEnginePy::batchTransferAsyncRead(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths) {
    return batchTransferAsync(target_hostname, buffers, peer_buffer_addresses,
                              lengths, TransferOpcode::READ);
}

#define GET_HANDLE(handle, target_hostname)                          \
    do {                                                             \
        std::lock_guard<std::mutex> guard(mutex_);                   \
        if (handle_map_.count(target_hostname)) {                    \
            handle = handle_map_[target_hostname];                   \
        } else {                                                     \
            int ret = engine_->openSegment(handle, target_hostname); \
            if (ret) return ret;                                     \
            handle_map_[target_hostname] = handle;                   \
        }                                                            \
    } while (0)

int TransferEnginePy::transferSync(const char* target_hostname,
                                   uintptr_t buffer,
                                   uintptr_t peer_buffer_address, size_t length,
                                   TransferOpcode opcode,
                                   TransferNotify* notify) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    GET_HANDLE(handle, target_hostname);
    return engine_->transferBatchSync(handle, {buffer}, {peer_buffer_address},
                                      {length}, opcode, notify,
                                      transfer_timeout_nsec_, target_hostname);
}

int TransferEnginePy::batchTransferSync(
    const char* target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths,
    TransferOpcode opcode, TransferNotify* notify) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    GET_HANDLE(handle, target_hostname);
    return engine_->transferBatchSync(handle, buffers, peer_buffer_addresses,
                                      lengths, opcode, notify,
                                      transfer_timeout_nsec_, target_hostname);
}

batch_id_t TransferEnginePy::batchTransferAsync(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths, TransferOpcode opcode) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    GET_HANDLE(handle, target_hostname);
    return engine_->transferBatchAsync(handle, buffers, peer_buffer_addresses,
                                       lengths, opcode, nullptr);
}

int TransferEnginePy::getBatchTransferStatus(
    const std::vector<batch_id_t>& batch_ids) {
    pybind11::gil_scoped_release release;
    return engine_->transferCompletion(batch_ids, transfer_timeout_nsec_);
}

batch_id_t TransferEnginePy::transferSubmitWrite(const char* target_hostname,
                                                 uintptr_t buffer,
                                                 uintptr_t peer_buffer_address,
                                                 size_t length) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    GET_HANDLE(handle, target_hostname);
    return engine_->transferBatchAsync(handle, {buffer}, {peer_buffer_address},
                                       {length}, TransferOpcode::WRITE,
                                       nullptr);
}

int TransferEnginePy::transferCheckStatus(batch_id_t batch_id) {
    pybind11::gil_scoped_release release;
    return engine_->getTransferStatus(batch_id);
}

int TransferEnginePy::batchRegisterMemory(
    std::vector<uintptr_t> buffer_addresses, std::vector<size_t> capacities) {
    pybind11::gil_scoped_release release;
    return engine_->registerLocalMemoryBatch(buffer_addresses, capacities);
}

int TransferEnginePy::batchUnregisterMemory(
    std::vector<uintptr_t> buffer_addresses) {
    pybind11::gil_scoped_release release;
    return engine_->unregisterLocalMemoryBatch(buffer_addresses);
}

int TransferEnginePy::registerMemory(uintptr_t buffer_addr, size_t capacity) {
    char* buffer = reinterpret_cast<char*>(buffer_addr);
    return engine_->registerLocalMemory(buffer, capacity);
}

int TransferEnginePy::unregisterMemory(uintptr_t buffer_addr) {
    char* buffer = reinterpret_cast<char*>(buffer_addr);
    return engine_->unregisterLocalMemory(buffer);
}

uintptr_t TransferEnginePy::getFirstBufferAddress(
    const std::string& segment_name) {
    return engine_->getFirstBufferAddress(segment_name);
}

std::shared_ptr<TransferEngine> TransferEnginePy::getEngine() const {
    return engine_->getEngine();
}

std::string TransferEnginePy::getLocalTopology(const char* device_name) {
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
    return engine_->getNotifies();
}

namespace py = pybind11;

PYBIND11_MODULE(engine, m) {
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

    // Bind RpcInterface
    mooncake::bind_rpc_interface(m);
}
