// Copyright 2026 KVCache.AI
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

#include "transport/nccl_transport/nccl_transport.h"

#include <cuda_runtime.h>
#include <glog/logging.h>
#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif
#include <nccl.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.h"
#include "error.h"
#include "transfer_metadata.h"

#if NCCL_VERSION_CODE < 23004
#error "Mooncake NCCL host transport requires NCCL 2.30.4 or newer"
#endif

namespace mooncake {
namespace {

constexpr char kHandshakeProtocol[] = "nccl";
constexpr int kSessionTimeoutSeconds = 60;

struct BufferInfo {
    uint64_t addr = 0;
    uint64_t length = 0;
    int device_id = -1;
};

bool containsRange(const BufferInfo& buffer, uint64_t addr, size_t length) {
    if (length == 0 || addr < buffer.addr || length > buffer.length) {
        return false;
    }
    return addr - buffer.addr <= buffer.length - length;
}

std::string ncclError(ncclResult_t result, const char* operation) {
    std::ostringstream out;
    out << operation << " failed: " << ncclGetErrorString(result);
    return out.str();
}

std::string cudaError(cudaError_t result, const char* operation) {
    std::ostringstream out;
    out << operation << " failed: " << cudaGetErrorString(result);
    return out.str();
}

std::string encodeJson(const Json::Value& value) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, value);
}

bool decodeJson(const std::string& encoded, Json::Value* value,
                std::string* error) {
    if (!value) return false;
    Json::CharReaderBuilder builder;
    builder["allowComments"] = false;
    builder["allowTrailingCommas"] = false;
    builder["failIfExtra"] = true;
    builder["rejectDupKeys"] = true;
    builder["strictRoot"] = true;
    std::istringstream input(encoded);
    try {
        if (!Json::parseFromStream(builder, input, value, error)) return false;
    } catch (const Json::Exception& exception) {
        if (error) *error = exception.what();
        return false;
    }
    if (!value->isObject()) {
        if (error) *error = "NCCL handshake payload is not an object";
        return false;
    }
    return true;
}

bool hasStringField(const Json::Value& value, const char* name) {
    return value.isObject() && value.isMember(name) && value[name].isString();
}

bool hasIntField(const Json::Value& value, const char* name) {
    return value.isObject() && value.isMember(name) && value[name].isInt();
}

bool hasArrayField(const Json::Value& value, const char* name) {
    return value.isObject() && value.isMember(name) && value[name].isArray();
}

Json::Value encodeBuffers(const std::vector<BufferInfo>& buffers) {
    Json::Value result(Json::arrayValue);
    for (const auto& buffer : buffers) {
        Json::Value item;
        item["addr"] = static_cast<Json::UInt64>(buffer.addr);
        item["length"] = static_cast<Json::UInt64>(buffer.length);
        item["device_id"] = buffer.device_id;
        result.append(std::move(item));
    }
    return result;
}

bool decodeBuffers(const Json::Value& value, int expected_device,
                   std::vector<BufferInfo>* buffers, std::string* error) {
    if (!buffers || expected_device < 0 || !value.isArray()) {
        if (error) *error = "NCCL buffer catalog is not an array";
        return false;
    }

    std::vector<BufferInfo> decoded;
    decoded.reserve(value.size());
    for (const auto& item : value) {
        if (!item.isObject() || !item.isMember("addr") ||
            !item["addr"].isUInt64() || !item.isMember("length") ||
            !item["length"].isUInt64() || !item.isMember("device_id") ||
            !item["device_id"].isInt()) {
            if (error) *error = "Malformed NCCL buffer catalog entry";
            return false;
        }
        BufferInfo buffer;
        buffer.addr = item["addr"].asUInt64();
        buffer.length = item["length"].asUInt64();
        buffer.device_id = item["device_id"].asInt();
        if (!buffer.addr || !buffer.length ||
            buffer.length >
                std::numeric_limits<uint64_t>::max() - buffer.addr ||
            buffer.device_id != expected_device) {
            if (error) *error = "Invalid NCCL buffer catalog entry";
            return false;
        }
        decoded.push_back(buffer);
    }
    if (decoded.empty()) {
        if (error) *error = "NCCL buffer catalog is empty";
        return false;
    }
    // Preserve the sender's registration order: NCCL window registration is
    // collective, so both ranks must pair corresponding buffers by the same
    // index. Use a separate address-ordered copy only to validate overlap.
    auto by_address = decoded;
    std::sort(by_address.begin(), by_address.end(),
              [](const BufferInfo& lhs, const BufferInfo& rhs) {
                  return lhs.addr < rhs.addr;
              });
    for (size_t i = 1; i < by_address.size(); ++i) {
        const auto& previous = by_address[i - 1];
        if (previous.addr + previous.length > by_address[i].addr) {
            if (error) *error = "NCCL buffer catalog contains overlap";
            return false;
        }
    }
    *buffers = std::move(decoded);
    return true;
}

std::string encodeUniqueId(const ncclUniqueId& id) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(&id);
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (size_t i = 0; i < sizeof(id); ++i) {
        out << std::setw(2) << static_cast<unsigned int>(bytes[i]);
    }
    return out.str();
}

int decodeHexNibble(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

bool decodeUniqueId(const std::string& encoded, ncclUniqueId* id) {
    if (!id || encoded.size() != 2 * sizeof(*id)) return false;
    auto* bytes = reinterpret_cast<unsigned char*>(id);
    for (size_t i = 0; i < sizeof(*id); ++i) {
        const int high = decodeHexNibble(encoded[2 * i]);
        const int low = decodeHexNibble(encoded[2 * i + 1]);
        if (high < 0 || low < 0) return false;
        bytes[i] = static_cast<unsigned char>((high << 4) | low);
    }
    return true;
}

int getPointerDevice(const void* ptr) {
    cudaPointerAttributes attributes{};
    cudaError_t result = cudaPointerGetAttributes(&attributes, ptr);
    if (result != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    if (attributes.type != cudaMemoryTypeDevice &&
        attributes.type != cudaMemoryTypeManaged) {
        return -1;
    }
    return attributes.device;
}

std::string makeSessionKey(const std::string& local_name, int local_device,
                           const std::string& peer_name, int peer_device) {
    std::ostringstream out;
    if (local_name < peer_name) {
        out << local_name << '#' << local_device << '|' << peer_name << '#'
            << peer_device;
    } else {
        out << peer_name << '#' << peer_device << '|' << local_name << '#'
            << local_device;
    }
    return out.str();
}

class NcclSession {
   public:
    NcclSession(std::string key, std::string peer_name, int local_device,
                int peer_device, int rank, ncclUniqueId unique_id,
                std::array<std::vector<BufferInfo>, 2> rank_buffers)
        : key_(std::move(key)),
          peer_name_(std::move(peer_name)),
          local_device_(local_device),
          peer_device_(peer_device),
          rank_(rank),
          unique_id_(unique_id),
          unique_id_string_(encodeUniqueId(unique_id)),
          rank_buffers_(std::move(rank_buffers)) {}

    ~NcclSession() {
        if (init_thread_.joinable()) init_thread_.join();
        cleanup();
    }

    const std::string& uniqueIdString() const { return unique_id_string_; }
    int rank() const { return rank_; }

    void start() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (started_) return;
        started_ = true;
        init_thread_ = std::thread([this] { initialize(); });
    }

    bool waitReady(std::string* error) {
        std::unique_lock<std::mutex> lock(state_mutex_);
        const bool finished = state_cv_.wait_for(
            lock, std::chrono::seconds(kSessionTimeoutSeconds),
            [this] { return ready_ || failed_; });
        if (!finished) {
            if (error) *error = "Timed out initializing NCCL RMA session";
            return false;
        }
        if (failed_) {
            if (error) *error = error_;
            return false;
        }
        return true;
    }

    int enqueuePut(const void* source, int owner_rank, uint64_t dest_addr,
                   size_t length, cudaEvent_t event, std::string* error) {
        if (!waitReady(error)) return -1;
        const Window* window = findWindow(owner_rank, dest_addr, length);
        if (!window) {
            if (error) *error = "Destination is outside an NCCL window";
            return -1;
        }

        std::lock_guard<std::mutex> lock(submit_mutex_);
        int saved_device = -1;
        cudaGetDevice(&saved_device);
        cudaError_t cuda_result = cudaSetDevice(local_device_);
        if (cuda_result != cudaSuccess) {
            if (error) *error = cudaError(cuda_result, "cudaSetDevice");
            return -1;
        }

        const auto& destination = window->buffers[owner_rank];
        ncclResult_t result = ncclPutSignal(
            source, length, ncclUint8, owner_rank, window->handle,
            dest_addr - destination.addr, 0, 0, 0, comm_, stream_);
        cudaError_t sync_result = cudaSuccess;
        if (result == ncclSuccess && event) {
            cuda_result = cudaEventRecord(event, stream_);
            if (cuda_result != cudaSuccess) {
                // The put is already enqueued. Do not let the caller reuse or
                // free its source until the stream has drained.
                sync_result = cudaStreamSynchronize(stream_);
            }
        }
        if (saved_device >= 0) cudaSetDevice(saved_device);

        if (result != ncclSuccess) {
            if (error) *error = ncclError(result, "ncclPutSignal");
            return -1;
        }
        if (cuda_result != cudaSuccess) {
            std::string failure = cudaError(cuda_result, "cudaEventRecord");
            if (sync_result != cudaSuccess) {
                failure += "; fallback " +
                           cudaError(sync_result, "cudaStreamSynchronize");
                setFailure(failure);
            }
            if (error) *error = std::move(failure);
            return -1;
        }
        return 0;
    }

   private:
    struct Window {
        std::array<BufferInfo, 2> buffers;
        void* local_ptr = nullptr;
        ncclWindow_t handle = nullptr;
    };

    void setFailure(const std::string& error) {
        LOG(ERROR) << "[Host NCCL] session " << key_ << ": " << error;
        std::lock_guard<std::mutex> lock(state_mutex_);
        error_ = error;
        failed_ = true;
        state_cv_.notify_all();
    }

    void initialize() {
        if (rank_buffers_[0].empty() ||
            rank_buffers_[0].size() != rank_buffers_[1].size()) {
            setFailure(
                "NCCL peers must register the same number of CUDA buffers");
            return;
        }
        for (size_t i = 0; i < rank_buffers_[0].size(); ++i) {
            if (rank_buffers_[0][i].length != rank_buffers_[1][i].length) {
                setFailure(
                    "Corresponding NCCL buffers must have identical lengths");
                return;
            }
        }

        int saved_device = -1;
        cudaGetDevice(&saved_device);
        cudaError_t cuda_result = cudaSetDevice(local_device_);
        if (cuda_result != cudaSuccess) {
            setFailure(cudaError(cuda_result, "cudaSetDevice"));
            return;
        }

        ncclConfig_t config = NCCL_CONFIG_INITIALIZER;
        config.numRmaCtx = 1;
        ncclResult_t result =
            ncclCommInitRankConfig(&comm_, 2, unique_id_, rank_, &config);
        if (result != ncclSuccess) {
            setFailure(ncclError(result, "ncclCommInitRankConfig"));
            if (saved_device >= 0) cudaSetDevice(saved_device);
            return;
        }

        cuda_result =
            cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking);
        if (cuda_result != cudaSuccess) {
            setFailure(cudaError(cuda_result, "cudaStreamCreate"));
            if (saved_device >= 0) cudaSetDevice(saved_device);
            return;
        }

        for (size_t i = 0; i < rank_buffers_[0].size(); ++i) {
            Window window;
            window.buffers[0] = rank_buffers_[0][i];
            window.buffers[1] = rank_buffers_[1][i];
            const auto& local_buffer = window.buffers[rank_];
            window.local_ptr = reinterpret_cast<void*>(local_buffer.addr);

            result = ncclCommWindowRegister(comm_, window.local_ptr,
                                            local_buffer.length, &window.handle,
                                            NCCL_WIN_COLL_SYMMETRIC);
            if (result != ncclSuccess || !window.handle) {
                if (result == ncclSuccess) result = ncclInternalError;
                setFailure(ncclError(result, "ncclCommWindowRegister"));
                if (saved_device >= 0) cudaSetDevice(saved_device);
                return;
            }
            windows_.push_back(window);
        }

        // NCCL initializes the host-RMA copy-engine resources collectively on
        // the first host RMA submission. Warm them here while both session
        // ranks are already participating, so later TE puts are one-sided.
        result = ncclSignal(1 - rank_, 0, 0, 0, comm_, stream_);
        if (result != ncclSuccess) {
            setFailure(ncclError(result, "ncclSignal (RMA warm-up)"));
            if (saved_device >= 0) cudaSetDevice(saved_device);
            return;
        }
        cuda_result = cudaStreamSynchronize(stream_);
        if (cuda_result != cudaSuccess) {
            setFailure(
                cudaError(cuda_result, "cudaStreamSynchronize (RMA warm-up)"));
            if (saved_device >= 0) cudaSetDevice(saved_device);
            return;
        }

        if (saved_device >= 0) cudaSetDevice(saved_device);
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            ready_ = true;
        }
        state_cv_.notify_all();
        LOG(INFO) << "[Host NCCL] session ready peer=" << peer_name_
                  << " rank=" << rank_ << " local_device=" << local_device_
                  << " peer_device=" << peer_device_
                  << " windows=" << windows_.size();
    }

    const Window* findWindow(int owner_rank, uint64_t addr,
                             size_t length) const {
        for (const auto& window : windows_) {
            const auto& buffer = window.buffers[owner_rank];
            if (containsRange(buffer, addr, length)) {
                return &window;
            }
        }
        return nullptr;
    }

    void cleanup() {
        if (!comm_) return;
        int saved_device = -1;
        cudaGetDevice(&saved_device);
        cudaSetDevice(local_device_);
        if (stream_) cudaStreamSynchronize(stream_);
        for (auto it = windows_.rbegin(); it != windows_.rend(); ++it) {
            if (it->handle) ncclCommWindowDeregister(comm_, it->handle);
        }
        windows_.clear();
        if (stream_) {
            cudaStreamDestroy(stream_);
            stream_ = nullptr;
        }
        ncclCommDestroy(comm_);
        comm_ = nullptr;
        if (saved_device >= 0) cudaSetDevice(saved_device);
    }

    std::string key_;
    std::string peer_name_;
    int local_device_;
    int peer_device_;
    int rank_;
    ncclUniqueId unique_id_{};
    std::string unique_id_string_;
    std::array<std::vector<BufferInfo>, 2> rank_buffers_;

    std::thread init_thread_;
    std::mutex state_mutex_;
    std::condition_variable state_cv_;
    bool started_ = false;
    bool ready_ = false;
    bool failed_ = false;
    std::string error_;

    ncclComm_t comm_ = nullptr;
    cudaStream_t stream_ = nullptr;
    std::vector<Window> windows_;
    std::mutex submit_mutex_;
};

}  // namespace

class NcclHostTransport::Impl {
   public:
    explicit Impl(NcclHostTransport* owner) : owner_(owner) {}

    ~Impl() {
        std::unordered_map<std::string, std::shared_ptr<NcclSession>> sessions;
        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            sessions.swap(sessions_);
            bootstrap_ids_.clear();
        }
        sessions.clear();
    }

    int install(const std::string& local_server_name,
                std::shared_ptr<TransferMetadata> metadata) {
        local_server_name_ = local_server_name;
        metadata_ = std::move(metadata);

        auto segment = std::make_shared<SegmentDesc>();
        segment->name = local_server_name_;
        segment->protocol = kHandshakeProtocol;
        int result = metadata_->addLocalSegment(
            LOCAL_SEGMENT_ID, local_server_name_, std::move(segment));
        if (result != 0) return result;

        result = metadata_->startHandshakeDaemon(
            [this](const HandShakeDesc& peer, HandShakeDesc& local) {
                return onHandshake(peer, local);
            },
            metadata_->localRpcMeta().rpc_port,
            metadata_->localRpcMeta().sockfd);
        if (result != 0) return result;
        return metadata_->updateLocalSegmentDesc();
    }

    int registerMemory(void* addr, size_t length, const std::string& location,
                       bool remote_accessible, bool update_metadata) {
        std::lock_guard<std::mutex> lock(buffers_mutex_);
        return registerMemoryLocked(addr, length, location, remote_accessible,
                                    update_metadata);
    }

    int unregisterMemory(void* addr, bool update_metadata) {
        std::lock_guard<std::mutex> lock(buffers_mutex_);
        return unregisterMemoryLocked(addr, update_metadata);
    }

    int registerMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                            const std::string& location) {
        std::lock_guard<std::mutex> lock(buffers_mutex_);
        std::vector<void*> registered;
        registered.reserve(buffer_list.size());
        for (const auto& buffer : buffer_list) {
            int result = registerMemoryLocked(buffer.addr, buffer.length,
                                              location, true, false);
            if (result != 0) {
                for (auto it = registered.rbegin(); it != registered.rend();
                     ++it) {
                    unregisterMemoryLocked(*it, false);
                }
                return result;
            }
            registered.push_back(buffer.addr);
        }
        int result = metadata_->updateLocalSegmentDesc();
        if (result != 0) {
            for (auto it = registered.rbegin(); it != registered.rend(); ++it) {
                int rollback_result = unregisterMemoryLocked(*it, false);
                if (rollback_result != 0) {
                    LOG(ERROR) << "[Host NCCL] failed to roll back buffer "
                               << *it << " after metadata publication failure: "
                               << rollback_result;
                }
            }
        }
        return result;
    }

    int unregisterMemoryBatch(const std::vector<void*>& addr_list) {
        std::lock_guard<std::mutex> lock(buffers_mutex_);
        if (buffers_frozen_) {
            LOG(ERROR) << "[Host NCCL] cannot unregister memory after the "
                          "session catalog has been frozen";
            return ERR_INVALID_ARGUMENT;
        }
        std::vector<BufferDesc> metadata_buffers;
        metadata_buffers.reserve(addr_list.size());
        for (size_t index = 0; index < addr_list.size(); ++index) {
            void* addr = addr_list[index];
            if (std::find(addr_list.begin(), addr_list.begin() + index, addr) !=
                addr_list.begin() + index) {
                return ERR_INVALID_ARGUMENT;
            }
            auto it = std::find_if(local_buffers_.begin(), local_buffers_.end(),
                                   [addr](const BufferInfo& buffer) {
                                       return buffer.addr ==
                                              reinterpret_cast<uint64_t>(addr);
                                   });
            if (it == local_buffers_.end()) return ERR_ADDRESS_NOT_REGISTERED;
            BufferDesc metadata_buffer;
            if (!findMetadataBuffer(addr, &metadata_buffer)) {
                return ERR_ADDRESS_NOT_REGISTERED;
            }
            metadata_buffers.push_back(std::move(metadata_buffer));
        }
        size_t removed = 0;
        for (; removed < addr_list.size(); ++removed) {
            void* addr = addr_list[removed];
            int result = metadata_->removeLocalMemoryBuffer(addr, false);
            if (result != 0) {
                restoreMetadataBuffers(metadata_buffers, removed);
                return result;
            }
        }
        int result = metadata_->updateLocalSegmentDesc();
        if (result != 0) {
            restoreMetadataBuffers(metadata_buffers, metadata_buffers.size());
            return result;
        }
        for (void* addr : addr_list) {
            local_buffers_.erase(
                std::remove_if(local_buffers_.begin(), local_buffers_.end(),
                               [addr](const BufferInfo& buffer) {
                                   return buffer.addr ==
                                          reinterpret_cast<uint64_t>(addr);
                               }),
                local_buffers_.end());
        }
        return 0;
    }

    Status submitTasks(const std::vector<TransferTask*>& task_list) {
        Status overall = Status::OK();
        for (TransferTask* task : task_list) {
            if (!task || !task->request) {
                overall =
                    Status::InvalidArgument("Missing NCCL transfer request");
                continue;
            }
            const TransferRequest& request = *task->request;
            task->total_bytes = request.length;

            Slice* slice = owner_->getSliceCache().allocate();
            slice->source_addr = request.source;
            slice->length = request.length;
            slice->opcode = request.opcode;
            slice->target_id = request.target_id;
            slice->task = task;
            slice->status = Slice::PENDING;
            slice->ts = getCurrentTimeInNano();
            slice->nccl.event = nullptr;
            slice->nccl.device_id = -1;
            task->slice_list.push_back(slice);
            __sync_fetch_and_add(&task->slice_count, 1);

            std::string error;
            if (request.opcode != TransferRequest::WRITE) {
                error =
                    "NCCL host transport supports WRITE only; NCCL "
                    "exposes no public host-side Get operation";
                slice->markFailed();
                if (overall.ok()) {
                    overall = Status::NotSupportedTransport(error);
                }
                LOG(ERROR) << "[Host NCCL] submit failed: " << error;
                continue;
            }

            auto target = metadata_->getSegmentDescByID(request.target_id);
            if (!target) {
                error = "Target segment metadata is unavailable";
            } else if (request.target_id == LOCAL_SEGMENT_ID) {
                error = "NCCL host transport requires a remote target";
            }

            BufferInfo remote_buffer;
            if (error.empty() &&
                !findRemoteBuffer(*target, request.target_offset,
                                  request.length, &remote_buffer)) {
                error = "Target address is not in a registered NCCL buffer";
            }

            int local_device = -1;
            if (error.empty()) {
                local_device = getPointerDevice(request.source);
                if (local_device < 0) {
                    error = "NCCL local buffer must be CUDA device memory";
                } else if (!freezeAndValidateLocalBuffer(
                               reinterpret_cast<uint64_t>(request.source),
                               request.length, local_device)) {
                    error = "NCCL local buffer is not registered";
                }
            }

            std::shared_ptr<NcclSession> session;
            if (error.empty()) {
                session = getOrCreateSession(target->name, local_device,
                                             remote_buffer.device_id, &error);
            }

            if (error.empty()) {
                int saved_device = -1;
                cudaGetDevice(&saved_device);
                cudaSetDevice(local_device);
                cudaEvent_t event = nullptr;
                cudaError_t cuda_result =
                    cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
                if (saved_device >= 0) cudaSetDevice(saved_device);
                if (cuda_result != cudaSuccess) {
                    error = cudaError(cuda_result, "cudaEventCreate");
                } else {
                    slice->nccl.event = event;
                    slice->nccl.device_id = local_device;
                    const int peer_rank = session->rank() == 0 ? 1 : 0;
                    if (session->enqueuePut(
                            request.source, peer_rank, request.target_offset,
                            request.length, event, &error) == 0) {
                        slice->status = Slice::POSTED;
                    }
                }
            }

            if (!error.empty()) {
                if (slice->nccl.event) {
                    int saved_device = -1;
                    cudaGetDevice(&saved_device);
                    cudaSetDevice(slice->nccl.device_id);
                    cudaEventDestroy(
                        reinterpret_cast<cudaEvent_t>(slice->nccl.event));
                    if (saved_device >= 0) cudaSetDevice(saved_device);
                    slice->nccl.event = nullptr;
                }
                LOG(ERROR) << "[Host NCCL] submit failed: " << error;
                slice->markFailed();
                if (overall.ok()) overall = Status::Context(error);
            }
        }
        return overall;
    }

    Status poll(BatchID batch_id, size_t task_id, TransferStatus& status) {
        auto& batch = Transport::toBatchDesc(batch_id);
        if (task_id >= batch.task_list.size()) {
            return Status::InvalidArgument("NCCL task ID out of range");
        }
        auto& task = batch.task_list[task_id];
        for (Slice* slice : task.slice_list) {
            if (!slice || slice->status != Slice::POSTED) continue;
            int saved_device = -1;
            cudaGetDevice(&saved_device);
            cudaSetDevice(slice->nccl.device_id);
            cudaEvent_t event =
                reinterpret_cast<cudaEvent_t>(slice->nccl.event);
            cudaError_t result = cudaEventQuery(event);
            if (result == cudaSuccess) {
                cudaEventDestroy(event);
                slice->nccl.event = nullptr;
                slice->markSuccess();
            } else if (result != cudaErrorNotReady) {
                cudaGetLastError();
                cudaEventDestroy(event);
                slice->nccl.event = nullptr;
                slice->markFailed();
            }
            if (saved_device >= 0) cudaSetDevice(saved_device);
        }

        status.transferred_bytes = task.transferred_bytes;
        if (task.success_slice_count + task.failed_slice_count ==
            task.slice_count) {
            status.s = task.failed_slice_count ? TransferStatusEnum::FAILED
                                               : TransferStatusEnum::COMPLETED;
            task.is_finished = true;
        } else {
            status.s = TransferStatusEnum::WAITING;
        }
        return Status::OK();
    }

   private:
    bool findMetadataBuffer(void* addr, BufferDesc* result) const {
        if (!result) return false;
        auto segment = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!segment) return false;
        auto it = std::find_if(
            segment->buffers.begin(), segment->buffers.end(),
            [addr](const BufferDesc& buffer) {
                return buffer.addr == reinterpret_cast<uint64_t>(addr);
            });
        if (it == segment->buffers.end()) return false;
        *result = *it;
        return true;
    }

    void restoreMetadataBuffers(const std::vector<BufferDesc>& buffers,
                                size_t count) {
        for (size_t index = 0; index < count; ++index) {
            int result = metadata_->addLocalMemoryBuffer(buffers[index], false);
            if (result != 0) {
                LOG(ERROR) << "[Host NCCL] failed to restore metadata buffer "
                           << reinterpret_cast<void*>(buffers[index].addr)
                           << ": " << result;
            }
        }
    }

    // buffers_mutex_ must be held across each complete catalog mutation so the
    // first session cannot snapshot a partially committed registration.
    int registerMemoryLocked(void* addr, size_t length,
                             const std::string& location,
                             bool remote_accessible, bool update_metadata) {
        (void)remote_accessible;
        if (!addr || length == 0) return ERR_INVALID_ARGUMENT;
        int device_id = getPointerDevice(addr);
        if (device_id < 0) {
            LOG(ERROR) << "[Host NCCL] only CUDA device or managed memory "
                          "can be registered";
            return ERR_INVALID_ARGUMENT;
        }

        const uint64_t address = reinterpret_cast<uint64_t>(addr);
        if (length > std::numeric_limits<uint64_t>::max() - address) {
            LOG(ERROR) << "[Host NCCL] memory registration range overflows";
            return ERR_INVALID_ARGUMENT;
        }

        BufferInfo info{address, length, device_id};
        if (buffers_frozen_) {
            LOG(ERROR) << "[Host NCCL] register all buffers before the first "
                          "transfer freezes the session catalog";
            return ERR_INVALID_ARGUMENT;
        }
        for (const auto& buffer : local_buffers_) {
            const uint64_t lhs_end = info.addr + info.length;
            const uint64_t rhs_end = buffer.addr + buffer.length;
            if (info.addr < rhs_end && buffer.addr < lhs_end) {
                LOG(ERROR) << "[Host NCCL] overlapping memory registration";
                return ERR_ADDRESS_OVERLAPPED;
            }
        }
        local_buffers_.push_back(info);

        BufferDesc desc;
        desc.name = location;
        desc.addr = info.addr;
        desc.length = info.length;
        desc.device_id = info.device_id;
        int result = metadata_->addLocalMemoryBuffer(desc, update_metadata);
        if (result != 0) {
            int rollback_result =
                metadata_->removeLocalMemoryBuffer(addr, false);
            if (rollback_result != 0 &&
                rollback_result != ERR_ADDRESS_NOT_REGISTERED) {
                LOG(ERROR) << "[Host NCCL] failed to roll back metadata for "
                           << addr << ": " << rollback_result;
            }
            local_buffers_.erase(
                std::remove_if(local_buffers_.begin(), local_buffers_.end(),
                               [addr](const BufferInfo& buffer) {
                                   return buffer.addr ==
                                          reinterpret_cast<uint64_t>(addr);
                               }),
                local_buffers_.end());
        }
        return result;
    }

    int unregisterMemoryLocked(void* addr, bool update_metadata) {
        if (buffers_frozen_) {
            LOG(ERROR) << "[Host NCCL] cannot unregister memory after the "
                          "session catalog has been frozen";
            return ERR_INVALID_ARGUMENT;
        }

        auto it = std::find_if(local_buffers_.begin(), local_buffers_.end(),
                               [addr](const BufferInfo& buffer) {
                                   return buffer.addr ==
                                          reinterpret_cast<uint64_t>(addr);
                               });
        if (it == local_buffers_.end()) return ERR_ADDRESS_NOT_REGISTERED;

        BufferDesc metadata_buffer;
        if (!findMetadataBuffer(addr, &metadata_buffer)) {
            return ERR_ADDRESS_NOT_REGISTERED;
        }
        int result = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
        if (result != 0) {
            if (update_metadata) {
                restoreMetadataBuffers({metadata_buffer}, 1);
            }
            return result;
        }
        local_buffers_.erase(it);
        return 0;
    }

    bool freezeBuffersForDevice(int device_id,
                                std::vector<BufferInfo>* result) {
        if (!result) return false;
        std::lock_guard<std::mutex> lock(buffers_mutex_);
        std::vector<BufferInfo> snapshot;
        for (const auto& buffer : local_buffers_) {
            if (buffer.device_id == device_id) snapshot.push_back(buffer);
        }
        if (snapshot.empty()) return false;
        // Registration and unregistration take the same lock and check this
        // flag. The first session therefore snapshots a complete, immutable
        // catalog instead of racing a buffer mutation between two locks.
        buffers_frozen_ = true;
        // Keep registration order so collective window index i represents the
        // same logical buffer on both ranks even when their virtual addresses
        // differ.
        *result = std::move(snapshot);
        return true;
    }

    bool freezeAndValidateLocalBuffer(uint64_t addr, size_t length,
                                      int device_id) {
        std::lock_guard<std::mutex> lock(buffers_mutex_);
        for (const auto& buffer : local_buffers_) {
            if (buffer.device_id == device_id &&
                containsRange(buffer, addr, length)) {
                // Once validation succeeds, unregister must not be able to
                // remove the source before the session snapshots its windows.
                buffers_frozen_ = true;
                return true;
            }
        }
        return false;
    }

    bool findRemoteBuffer(const SegmentDesc& segment, uint64_t addr,
                          size_t length, BufferInfo* result) const {
        for (const auto& buffer : segment.buffers) {
            BufferInfo info{buffer.addr, buffer.length, buffer.device_id};
            if (info.device_id >= 0 && containsRange(info, addr, length)) {
                if (result) *result = info;
                return true;
            }
        }
        return false;
    }

    bool selectBootstrapId(const std::string& key,
                           const std::string& proposed_id, bool may_create,
                           ncclUniqueId* unique_id, std::string* encoded_id,
                           std::string* error) {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        auto it = bootstrap_ids_.find(key);
        if (it != bootstrap_ids_.end()) {
            if (!proposed_id.empty() && proposed_id != it->second) {
                if (error) *error = "Concurrent NCCL bootstrap ID mismatch";
                return false;
            }
            *encoded_id = it->second;
            if (!decodeUniqueId(*encoded_id, unique_id)) {
                if (error) *error = "Stored NCCL bootstrap ID is invalid";
                return false;
            }
            return true;
        }

        if (!proposed_id.empty()) {
            *encoded_id = proposed_id;
            if (!decodeUniqueId(*encoded_id, unique_id)) {
                if (error) *error = "Invalid NCCL unique ID in bootstrap";
                return false;
            }
        } else {
            if (!may_create) {
                if (error) *error = "NCCL rank 1 cannot create a unique ID";
                return false;
            }
            ncclResult_t result = ncclGetUniqueId(unique_id);
            if (result != ncclSuccess) {
                if (error) *error = ncclError(result, "ncclGetUniqueId");
                return false;
            }
            *encoded_id = encodeUniqueId(*unique_id);
        }
        bootstrap_ids_.emplace(key, *encoded_id);
        return true;
    }

    std::shared_ptr<NcclSession> getOrCreateSession(
        const std::string& peer_name, int local_device, int peer_device,
        std::string* error) {
        const std::string key = makeSessionKey(local_server_name_, local_device,
                                               peer_name, peer_device);
        std::shared_ptr<NcclSession> existing;
        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            auto it = sessions_.find(key);
            if (it != sessions_.end()) {
                existing = it->second;
            }
        }
        if (existing) {
            if (!existing->waitReady(error)) return nullptr;
            return existing;
        }

        std::vector<BufferInfo> local_buffers;
        if (!freezeBuffersForDevice(local_device, &local_buffers)) {
            if (error) *error = "No NCCL buffers registered on local device";
            return nullptr;
        }
        const int local_rank = local_server_name_ < peer_name ? 0 : 1;
        ncclUniqueId unique_id{};
        std::string unique_id_string;
        if (local_rank == 0 && !selectBootstrapId(key, "", true, &unique_id,
                                                  &unique_id_string, error)) {
            return nullptr;
        }

        Json::Value request;
        request["op"] = "bootstrap";
        request["peer_name"] = local_server_name_;
        request["local_device"] = local_device;
        request["remote_device"] = peer_device;
        request["unique_id"] = unique_id_string;
        request["buffers"] = encodeBuffers(local_buffers);

        HandShakeDesc local_desc;
        local_desc.payload = encodeJson(request);
        HandShakeDesc peer_desc;
        int result = metadata_->sendHandshake(peer_name, local_desc, peer_desc);
        if (result != 0) {
            if (error) *error = "NCCL bootstrap handshake failed";
            return nullptr;
        }

        Json::Value response;
        std::string parse_error;
        if (!decodeJson(peer_desc.payload, &response, &parse_error)) {
            if (error)
                *error = "Invalid NCCL bootstrap response: " + parse_error;
            return nullptr;
        }
        if (!hasStringField(response, "unique_id") ||
            !hasArrayField(response, "buffers")) {
            if (error) *error = "Malformed NCCL bootstrap response";
            return nullptr;
        }
        const std::string response_id = response["unique_id"].asString();
        if (!selectBootstrapId(key, response_id, local_rank == 0, &unique_id,
                               &unique_id_string, error)) {
            return nullptr;
        }

        std::vector<BufferInfo> peer_buffers;
        if (!decodeBuffers(response["buffers"], peer_device, &peer_buffers,
                           error)) {
            return nullptr;
        }
        std::array<std::vector<BufferInfo>, 2> rank_buffers;
        rank_buffers[local_rank] = local_buffers;
        rank_buffers[1 - local_rank] = peer_buffers;

        std::shared_ptr<NcclSession> session;
        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            auto it = sessions_.find(key);
            if (it != sessions_.end()) {
                session = it->second;
                if (session->uniqueIdString() != unique_id_string) {
                    if (error) *error = "Concurrent NCCL bootstrap conflict";
                    return nullptr;
                }
            } else {
                session = std::make_shared<NcclSession>(
                    key, peer_name, local_device, peer_device, local_rank,
                    unique_id, std::move(rank_buffers));
                sessions_.emplace(key, session);
                session->start();
            }
        }
        if (!session->waitReady(error)) return nullptr;
        return session;
    }

    int onHandshake(const HandShakeDesc& peer_desc, HandShakeDesc& local_desc) {
        Json::Value request;
        std::string error;
        if (!decodeJson(peer_desc.payload, &request, &error)) {
            local_desc.reply_msg = "Invalid NCCL handshake payload: " + error;
            return 0;
        }
        if (!hasStringField(request, "op")) {
            local_desc.reply_msg =
                "Missing or invalid 'op' field in NCCL handshake request";
            return 0;
        }

        const std::string op = request["op"].asString();
        Json::Value response;
        try {
            if (op == "bootstrap") {
                if (handleBootstrap(request, &response, &error) != 0) {
                    local_desc.reply_msg = error;
                }
            } else {
                local_desc.reply_msg = "Unknown NCCL handshake operation";
            }
        } catch (const Json::Exception& exception) {
            local_desc.reply_msg = "Malformed NCCL handshake request: " +
                                   std::string(exception.what());
        }
        local_desc.payload = encodeJson(response);
        return 0;
    }

    int handleBootstrap(const Json::Value& request, Json::Value* response,
                        std::string* error) {
        if (!response || !hasStringField(request, "peer_name") ||
            !hasIntField(request, "local_device") ||
            !hasIntField(request, "remote_device") ||
            !hasStringField(request, "unique_id") ||
            !hasArrayField(request, "buffers")) {
            if (error) *error = "Malformed NCCL bootstrap request";
            return -1;
        }
        const std::string peer_name = request["peer_name"].asString();
        const int peer_device = request["local_device"].asInt();
        const int local_device = request["remote_device"].asInt();
        if (peer_name.empty() || local_device < 0 || peer_device < 0) {
            if (error) *error = "Invalid NCCL bootstrap endpoint";
            return -1;
        }

        std::vector<BufferInfo> peer_buffers;
        if (!decodeBuffers(request["buffers"], peer_device, &peer_buffers,
                           error)) {
            return -1;
        }
        const int local_rank = local_server_name_ < peer_name ? 0 : 1;

        const std::string key = makeSessionKey(local_server_name_, local_device,
                                               peer_name, peer_device);
        ncclUniqueId unique_id{};
        std::string unique_id_string;
        const std::string proposed_id = request["unique_id"].asString();
        ncclUniqueId proposed_unique_id{};
        if ((!proposed_id.empty() &&
             !decodeUniqueId(proposed_id, &proposed_unique_id)) ||
            (proposed_id.empty() && local_rank != 0)) {
            if (error) *error = "Invalid NCCL unique ID in bootstrap";
            return -1;
        }

        // Do not let malformed endpoint data permanently freeze registration.
        // Once the request is valid, take the immutable catalog snapshot used
        // to create the collective windows.
        std::vector<BufferInfo> local_buffers;
        if (!freezeBuffersForDevice(local_device, &local_buffers)) {
            if (error) *error = "No NCCL buffers registered on local device";
            return -1;
        }

        if (!selectBootstrapId(key, proposed_id, local_rank == 0, &unique_id,
                               &unique_id_string, error)) {
            return -1;
        }

        std::array<std::vector<BufferInfo>, 2> rank_buffers;
        rank_buffers[local_rank] = local_buffers;
        rank_buffers[1 - local_rank] = peer_buffers;

        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            auto it = sessions_.find(key);
            if (it != sessions_.end()) {
                if (it->second->uniqueIdString() != unique_id_string) {
                    if (error) *error = "Concurrent NCCL bootstrap conflict";
                    return -1;
                }
            } else {
                auto session = std::make_shared<NcclSession>(
                    key, peer_name, local_device, peer_device, local_rank,
                    unique_id, std::move(rank_buffers));
                sessions_.emplace(key, session);
                session->start();
            }
        }

        (*response)["unique_id"] = unique_id_string;
        (*response)["buffers"] = encodeBuffers(local_buffers);
        return 0;
    }

    NcclHostTransport* owner_;
    std::string local_server_name_;
    std::shared_ptr<TransferMetadata> metadata_;

    mutable std::mutex buffers_mutex_;
    std::vector<BufferInfo> local_buffers_;
    bool buffers_frozen_ = false;
    std::mutex sessions_mutex_;
    std::unordered_map<std::string, std::shared_ptr<NcclSession>> sessions_;
    std::unordered_map<std::string, std::string> bootstrap_ids_;
};

NcclHostTransport::NcclHostTransport() : impl_(std::make_unique<Impl>(this)) {}

NcclHostTransport::~NcclHostTransport() = default;

int NcclHostTransport::install(std::string& local_server_name,
                               std::shared_ptr<TransferMetadata> metadata,
                               std::shared_ptr<Topology> topology) {
    (void)topology;
    local_server_name_ = local_server_name;
    metadata_ = metadata;
    return impl_->install(local_server_name, std::move(metadata));
}

Status NcclHostTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch = toBatchDesc(batch_id);
    if (batch.task_list.size() + entries.size() > batch.batch_size) {
        return Status::TooManyRequests("NCCL batch capacity exceeded");
    }
    const size_t first = batch.task_list.size();
    batch.task_list.resize(first + entries.size());
    std::vector<TransferTask*> tasks;
    tasks.reserve(entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        auto& task = batch.task_list[first + i];
        task.batch_id = batch_id;
        task.transport_ = this;
        task.request = &entries[i];
        tasks.push_back(&task);
    }
    return impl_->submitTasks(tasks);
}

Status NcclHostTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    return impl_->submitTasks(task_list);
}

Status NcclHostTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                            TransferStatus& status) {
    return impl_->poll(batch_id, task_id, status);
}

int NcclHostTransport::registerLocalMemory(void* addr, size_t length,
                                           const std::string& location,
                                           bool remote_accessible,
                                           bool update_metadata) {
    return impl_->registerMemory(addr, length, location, remote_accessible,
                                 update_metadata);
}

int NcclHostTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return impl_->unregisterMemory(addr, update_metadata);
}

int NcclHostTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    return impl_->registerMemoryBatch(buffer_list, location);
}

int NcclHostTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    return impl_->unregisterMemoryBatch(addr_list);
}

}  // namespace mooncake
