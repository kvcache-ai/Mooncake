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

#include "transport/nvlink_transport/nvlink_transport.h"

#include <bits/stdint-uintn.h>
#include "cuda_alike.h"
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <sys/un.h>
#include <sys/stat.h>

#include "common.h"
#include "config.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

static bool checkCudaErrorReturn(cudaError_t result, const char *message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        return false;
    }
    return true;
}

namespace mooncake {
static int getNumDevices() {
    static int cached_num_devices = -1;
    if (cached_num_devices == -1) {
        if (!checkCudaErrorReturn(
                cudaGetDeviceCount(&cached_num_devices),
                "NvlinkTransport: cudaGetDeviceCount failed")) {
            return 0;
        }
    }
    return cached_num_devices;
}

std::string NvlinkTransport::getSocketPath() const {
    return "/tmp/nvlink_export_" + std::to_string(getpid());
}

void NvlinkTransport::startExportServer() {
    if (!server_running_) return;
}

void NvlinkTransport::cleanupExportServer() {
    std::string path = getSocketPath();
    unlink(path.c_str());
}

void NvlinkTransport::exportServerLoop() {
    std::string path = getSocketPath();
    unlink(path.c_str());

    int sock = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (sock < 0) {
        LOG(ERROR) << "Failed to create socket: " << strerror(errno);
        return;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "Failed to bind socket: " << strerror(errno);
        close(sock);
        return;
    }

    chmod(path.c_str(), 0777);  // 允许其他用户连接

    LOG(INFO) << "NVLink FD Export Server listening on " << path;

    char buf[256];

    while (server_running_) {
        // char request_buf[sizeof(uint64_t)];
        // struct msghdr msg = {};
        // struct iovec iov = { request_buf, sizeof(request_buf) };
        // char ctrl_buf[CMSG_SPACE(sizeof(int))];
        struct sockaddr_un client_addr;
        socklen_t addr_len = sizeof(client_addr);
        // 初始化 client_addr
        memset(&client_addr, 0, sizeof(client_addr));

        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                             (struct sockaddr*)&client_addr, &addr_len);
        if (n <= 0) continue;

        std::string request(buf, n);
        LOG(INFO) << "Received request: " << request;

        // 检查是否以 "REQ:" 开头
        if (request.size() < 4 || memcmp(request.data(), "REQ:", 4) != 0) {
            LOG(WARNING) << "Invalid request prefix: " << request;
            continue;
        }

        size_t first_colon = request.find(':', 3);
        size_t second_colon = request.find(':', first_colon + 1);
        if (first_colon == std::string::npos || second_colon == std::string::npos) {
            LOG(WARNING) << "Malformed request: missing colons";
            continue;
        }

        std::string addr_str = request.substr(4, first_colon - 4);
        std::string client_socket_path = request.substr(second_colon + 1);

        uint64_t requested_addr = 0;
        try {
            requested_addr = std::stoull(addr_str);
        } catch (...) {
            LOG(ERROR) << "Failed to parse address: " << addr_str;
            continue;
        }

        LOG(INFO) << "Request for addr: " << (void*)requested_addr
                << " from client socket: " << client_socket_path;


        // 查找 buffer
        std::lock_guard<std::mutex> lock(exported_mutex_);
        auto it = exported_buffers_.find((void*)requested_addr);
        if (it == exported_buffers_.end()) {
            LOG(WARNING) << "Requested address not found: " << (void*)requested_addr;
            continue;
        }

        auto& buf = it->second;
        LOG(INFO) << "Found buffer, handle: " << buf.alloc_handle;

        // 导出 fd
        int exported_fd;
        CUresult result = cuMemExportToShareableHandle(
            &exported_fd, buf.alloc_handle,
            CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR, 0);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR) << "cuMemExportToShareableHandle failed: " << result;
            continue;
        }
        LOG(INFO) << "Exported fd: " << exported_fd;

        // --- 通过 client_socket_path 回复 fd ---
        int reply_sock = socket(AF_UNIX, SOCK_DGRAM, 0);
        struct sockaddr_un dest_addr;
        memset(&dest_addr, 0, sizeof(dest_addr));
        dest_addr.sun_family = AF_UNIX;
        strncpy(dest_addr.sun_path, client_socket_path.c_str(), sizeof(dest_addr.sun_path) - 1);

        // 构造 sendmsg
        union {
            struct cmsghdr cm;
            char control[CMSG_SPACE(sizeof(int))];
        } cmsgbuf;
        memset(&cmsgbuf, 0, sizeof(cmsgbuf));

        char dummy = 'x';
        struct iovec iov_send = {&dummy, 1};
        struct msghdr send_msg = {
            .msg_name = &dest_addr,
            .msg_namelen = sizeof(dest_addr),
            .msg_iov = &iov_send,
            .msg_iovlen = 1,
            .msg_control = cmsgbuf.control,
            .msg_controllen = sizeof(cmsgbuf.control)
        };

        struct cmsghdr *cmsg = CMSG_FIRSTHDR(&send_msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(sizeof(int));
        memcpy(CMSG_DATA(cmsg), &exported_fd, sizeof(exported_fd));

        ssize_t sent = sendmsg(reply_sock, &send_msg, 0);
        if (sent == -1) {
            LOG(ERROR) << "sendmsg failed: " << strerror(errno);
        } else {
            LOG(INFO) << "Successfully sent fd " << exported_fd << " to client";
        }

        close(reply_sock);
        close(exported_fd);  // fd 已发送，关闭
    }

    close(sock);
}

static bool supportFabricMem() {
    if (getenv("MC_USE_NVLINK_IPC")) return false;

    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDeviceCount failed: "
                   << cudaGetErrorString(err);
        return false;
    }
    if (num_devices == 0) {
        LOG(ERROR) << "NvlinkTransport: no device found";
        return false;
    }

#ifdef USE_CUDA
    for (int device_id = 0; device_id < num_devices; ++device_id) {
        int device_support_fabric_mem = 0;
        cuDeviceGetAttribute(&device_support_fabric_mem,
                             CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                             device_id);
        if (!device_support_fabric_mem) {
            return false;
        }
    }
#endif
    return true;
}

static bool enableP2PAccess(int src_device_id, int dst_device_id) {
    int canAccessPeer = 0;
    if (!checkCudaErrorReturn(cudaDeviceCanAccessPeer(
                                  &canAccessPeer, src_device_id, dst_device_id),
                              "NvlinkTransport: failed to query peer access")) {
        return false;
    }

    if (!canAccessPeer) {
        LOG(ERROR) << "NvlinkTransport: device " << src_device_id
                   << " cannot p2p access device " << dst_device_id;
        return false;
    }

    // enable src->dst p2p access
    if (!checkCudaErrorReturn(cudaSetDevice(src_device_id),
                              "NvlinkTransport: failed to set device")) {
        return false;
    }
    cudaError_t result = cudaDeviceEnablePeerAccess(dst_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR)
            << "NvlinkTransport: failed to enable p2p access (Error code: "
            << result << " - " << cudaGetErrorString(result) << ")"
            << std::endl;

        return false;
    }

    // enable dst->src p2p access
    if (!checkCudaErrorReturn(cudaSetDevice(dst_device_id),
                              "NvlinkTransport: failed to set device")) {
        return false;
    }
    result = cudaDeviceEnablePeerAccess(src_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR)
            << "NvlinkTransport: failed to enable p2p access (Error code: "
            << result << " - " << cudaGetErrorString(result) << ")"
            << std::endl;

        return false;
    }

    return true;
}

NvlinkTransport::NvlinkTransport() : use_fabric_mem_(supportFabricMem()) {
        server_running_ = true;
        export_server_thread_ = std::thread(&NvlinkTransport::exportServerLoop, this);
        LOG(INFO) << "NvlinkTransport: FD export server started at " << getSocketPath();
}
//     int num_devices = getNumDevices();
//     if (globalConfig().trace) {
//         LOG(INFO) << "NvlinkTransport: use_fabric_mem_:" << use_fabric_mem_
//                   << ", num_devices: " << num_devices;
//     }

//     for (int src_device_id = 0; src_device_id < num_devices; ++src_device_id)
//     {
//         for (int dst_device_id = src_device_id + 1; dst_device_id <
//         num_devices;
//              ++dst_device_id) {
//             if (enableP2PAccess(src_device_id, dst_device_id)) {
//                 if (globalConfig().trace) {
//                     LOG(INFO)
//                         << "NvlinkTransport: enabled p2p access between
//                         device "
//                         << src_device_id << " and " << dst_device_id;
//                 }
//             } else {
//                 LOG(ERROR) << "NvlinkTransport: failed to enable p2p access "
//                               "between device "
//                            << src_device_id << " and " << dst_device_id;
//             }
//         }
//     }
// }

NvlinkTransport::~NvlinkTransport() {
    if (use_fabric_mem_) {
        for (auto &entry : remap_entries_) {
            freePinnedLocalMemory(entry.second.shm_addr);
        }
    } else {
        for (auto &entry : remap_entries_) {
            cudaIpcCloseMemHandle(entry.second.shm_addr);
        }
    }
    remap_entries_.clear();
    if (server_running_) {
        server_running_ = false;
        if (export_server_thread_.joinable()) {
            export_server_thread_.join();
        }
        cleanupExportServer();
    }
}

int NvlinkTransport::install(std::string &local_server_name,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "nvlink";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status NvlinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "NvlinkTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "NvlinkTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        uint64_t dest_addr = request.target_offset;
        LOG(INFO) << "submitTransfer Request addr " << (void *)dest_addr << " to "
               << (void *)(dest_addr + request.length);
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        __sync_fetch_and_add(&task.slice_count, 1);
        cudaError_t err;
        if (slice->opcode == TransferRequest::READ)
            err = cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                             slice->length, cudaMemcpyDefault);
        else
            err = cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                             slice->length, cudaMemcpyDefault);
        if (err != cudaSuccess)
            slice->markFailed();
        else
            slice->markSuccess();
    }

    return Status::OK();
}

Status NvlinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "NvlinkTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status NvlinkTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        uint64_t dest_addr = request.target_offset;
        // LOG(INFO) << "Inside submitTransferTask";
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        cudaError_t err;
        if (slice->opcode == TransferRequest::READ)
            err = cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                             slice->length, cudaMemcpyDefault);
        else
            err = cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                             slice->length, cudaMemcpyDefault);
        if (err != cudaSuccess)
            slice->markFailed();
        else
            slice->markSuccess();
    }
    return Status::OK();
}

int hexCharToValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    throw std::invalid_argument("Invalid hexadecimal character");
}

std::string serializeBinaryData(const void *data, size_t length) {
    if (!data) {
        throw std::invalid_argument("Data pointer cannot be null");
    }

    std::string hexString;
    hexString.reserve(length * 2);

    const unsigned char *byteData = static_cast<const unsigned char *>(data);
    for (size_t i = 0; i < length; ++i) {
        hexString.push_back("0123456789ABCDEF"[(byteData[i] >> 4) & 0x0F]);
        hexString.push_back("0123456789ABCDEF"[byteData[i] & 0x0F]);
    }

    return hexString;
}

void deserializeBinaryData(const std::string &hexString,
                           std::vector<unsigned char> &buffer) {
    if (hexString.length() % 2 != 0) {
        throw std::invalid_argument("Input string length must be even");
    }

    buffer.clear();
    buffer.reserve(hexString.length() / 2);

    for (size_t i = 0; i < hexString.length(); i += 2) {
        int high = hexCharToValue(hexString[i]);
        int low = hexCharToValue(hexString[i + 1]);
        buffer.push_back(static_cast<unsigned char>((high << 4) | low));
    }
}

int NvlinkTransport::registerLocalMemory(void *addr, size_t length,
                                         const std::string &location,
                                         bool remote_accessible,
                                         bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);
    LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }
    if (!use_fabric_mem_) {
        cudaPointerAttributes attr;
        cudaError_t err = cudaPointerGetAttributes(&attr, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaPointerGetAttributes failed";
            return -1;
        }

        if (attr.type != cudaMemoryTypeDevice) {
            LOG(ERROR) << "Unsupported memory type, " << addr << " "
                       << attr.type;
            return -1;
        }

        cudaIpcMemHandle_t handle;
        err = cudaIpcGetMemHandle(&handle, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaIpcGetMemHandle failed";
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name =
            serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
        return metadata_->addLocalMemoryBuffer(desc, true);
    } else {
        CUmemGenericAllocationHandle handle;
        auto result = cuMemRetainAllocationHandle(&handle, addr);
        if (result != CUDA_SUCCESS) {
            LOG(WARNING) << "Memory region " << addr
                         << " is not allocated by cuMemCreate, "
                         << "but it can be used as local buffer";
            return 0;
        }

        // Find whole physical page for memory registration
        void *real_addr;
        size_t real_size;
        result = cuMemGetAddressRange((CUdeviceptr *)&real_addr, &real_size,
                                      (CUdeviceptr)addr);
        if (result != CUDA_SUCCESS) {
            LOG(WARNING) << "NvlinkTransport: cuMemGetAddressRange failed: "
                         << result;
            const uint64_t granularity = 2 * 1024 * 1024;
            real_addr = addr;
            real_size = (length + granularity - 1) & ~(granularity - 1);
        }

        std::vector<uint8_t> shm_data;
        uint32_t handle_type = 0;

        CUmemFabricHandle fabric_handle;
        LOG(INFO) << "The Generic Handle: " << handle;
        result = cuMemExportToShareableHandle(&fabric_handle, handle,
                                              CU_MEM_HANDLE_TYPE_FABRIC, 0);

        if (result == CUDA_SUCCESS){
            // Blackwell GB200
            memcpy(shm_data.data(), &fabric_handle, sizeof(fabric_handle));
            handle_type = 1;  // 标记为 FABRIC
            LOG(INFO) << "Using CU_MEM_HANDLE_TYPE_FABRIC for GB200";
        }
        else {
            // Hopper or Ampere, Fallback to POSIX
            // Fallback to POSIX FD (supported on Hopper+, CUDA 11.4+)
            int fd = -1;
            {
                std::lock_guard<std::mutex> lock(exported_mutex_);
                ExportedBuffer buf;
                buf.base_addr = real_addr;
                buf.size = real_size;
                buf.alloc_handle = handle;
                exported_buffers_[real_addr] = buf;
            }

            BufferDesc desc;
            desc.addr = (uint64_t)real_addr;  // (uint64_t)addr;
            desc.length = real_size;          // length;
            desc.name = location;

            desc.metadata["handle_type"] = "2";  // POSIX_FD
            desc.metadata["export_pid"] = std::to_string(getpid());
            desc.metadata["socket_path"] = getSocketPath();  // 如 /tmp/nvlink_export_12345OG(INFO) << "Directly send fd";
            LOG(INFO) << "Metadata - handle_type: " << desc.metadata["handle_type"];
            LOG(INFO) << "Metadata - export_pid: " << desc.metadata["export_pid"];
            LOG(INFO) << "Metadata - socket_path: " << desc.metadata["socket_path"];

            desc.shm_name = serializeBinaryData(&fd, sizeof(int));
            return metadata_->addLocalMemoryBuffer(desc, true);
        }
        LOG(INFO) << "still use shm_data";
        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)real_addr;  // (uint64_t)addr;
        desc.length = real_size;          // length;
        desc.name = location;

        desc.metadata["handle_type"] = std::to_string(handle_type);

        desc.shm_name = serializeBinaryData(shm_data.data(), shm_data.size());
        return metadata_->addLocalMemoryBuffer(desc, true);
    }
}

int NvlinkTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int NvlinkTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                 uint64_t length,
                                                 uint64_t target_id) {
    // LOG(INFO) << "relocateShareMemoryaddress";
    auto desc = metadata_->getSegmentDescByID(target_id);
    int index = 0;
    // LOG(INFO) << "The dest_addr is " << (void *)dest_addr << " length : " << length;
    for (auto &entry : desc->buffers) {
        // bool res = (!entry.shm_name.empty() && entry.addr <= dest_addr &&
        //     dest_addr + length <= entry.addr + entry.length);
        // LOG(INFO) << "Condition value is " << res;
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            remap_lock_.lockShared();
            if (remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                auto shm_addr =
                    remap_entries_[std::make_pair(target_id, entry.addr)]
                        .shm_addr;
                remap_lock_.unlockShared();
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();
            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            if (!remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);
                LOG(INFO) << "sizeof(CUmemFabricHandle) " << sizeof(CUmemFabricHandle);
                LOG(INFO) << "use_fabric_mem_ " << use_fabric_mem_;
                LOG(INFO) << "size of output buffer " << output_buffer.size();
                if (output_buffer.size() == sizeof(cudaIpcMemHandle_t) &&
                    !use_fabric_mem_) {
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    void *shm_addr = nullptr;
                    cudaError_t err = cudaIpcOpenMemHandle(
                        &shm_addr, handle, cudaIpcMemLazyEnablePeerAccess);
                    if (err != cudaSuccess) {
                        LOG(ERROR)
                            << "NvlinkTransport: cudaIpcOpenMemHandle failed: "
                            << cudaGetErrorString(err);
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = length;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else if (output_buffer.size() == sizeof(CUmemFabricHandle) &&
                           use_fabric_mem_) {
                    CUmemFabricHandle export_handle;
                    memcpy(&export_handle, output_buffer.data(),
                           sizeof(export_handle));
                    void *shm_addr = nullptr;
                    CUmemGenericAllocationHandle handle;
                    auto result = cuMemImportFromShareableHandle(
                        &handle, &export_handle, CU_MEM_HANDLE_TYPE_FABRIC);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "NvlinkTransport: "
                                      "cuMemImportFromShareableHandle failed: "
                                   << result;
                        return -1;
                    }
                    result = cuMemAddressReserve((CUdeviceptr *)&shm_addr,
                                                 entry.length, 0, 0, 0);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemAddressReserve failed: "
                            << result;
                        return -1;
                    }
                    result = cuMemMap((CUdeviceptr)shm_addr, entry.length, 0,
                                      handle, 0);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemMap failed: " << result;
                        return -1;
                    }

                    int device_count;
                    cudaGetDeviceCount(&device_count);
                    CUmemAccessDesc accessDesc[device_count];
                    for (int device_id = 0; device_id < device_count;
                         ++device_id) {
                        accessDesc[device_id].location.type =
                            CU_MEM_LOCATION_TYPE_DEVICE;
                        accessDesc[device_id].location.id = device_id;
                        accessDesc[device_id].flags =
                            CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                    }
                    result = cuMemSetAccess((CUdeviceptr)shm_addr, entry.length,
                                            accessDesc, device_count);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "NvlinkTransport: cuMemSetAccess failed: "
                                   << result;
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = length;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else if (output_buffer.size() == sizeof(int) && use_fabric_mem_){
                    LOG(INFO) << "Inside POSIX part";
                    // 解析 metadata
                    auto type_it = entry.metadata.find("handle_type");
                    auto pid_it = entry.metadata.find("export_pid");
                    auto sock_it = entry.metadata.find("socket_path");


                    if (type_it == entry.metadata.end() || pid_it == entry.metadata.end()) {
                        LOG(ERROR) << "Missing metadata for POSIX_FD import";
                        return -1;
                    }

                    if (std::stoi(type_it->second) != 2) return -1;

                    pid_t export_pid = std::stoi(pid_it->second);
                    std::string socket_path = sock_it->second;
                    LOG(INFO) << "Receiver - type_it: " << type_it->second;
                    LOG(INFO) << "Receiver - pid_it: "  << pid_it->second;
                    LOG(INFO) << "Receiver - sock_it: " << sock_it->second;

                    // --- 创建自己的 socket ---
                    int client_sock = socket(AF_UNIX, SOCK_DGRAM, 0);
                    if (client_sock < 0) {
                        LOG(ERROR) << "Failed to create client socket: " << strerror(errno);
                        return false;
                    }

                    std::string client_socket_path = "/tmp/nvlink_client_" + std::to_string(getpid());
                    unlink(client_socket_path.c_str());
                    struct sockaddr_un client_addr;
                    memset(&client_addr, 0, sizeof(client_addr));
                    client_addr.sun_family = AF_UNIX;
                    strncpy(client_addr.sun_path, client_socket_path.c_str(), sizeof(client_addr.sun_path) - 1);
                    if (bind(client_sock, (struct sockaddr*)&client_addr, sizeof(client_addr)) < 0) {
                        LOG(ERROR) << "Failed to bind client socket: " << strerror(errno);
                        close(client_sock);
                        return false;
                    }
                    chmod(client_socket_path.c_str(), 0777);

                    LOG(INFO) << "Client socket created at: " << client_socket_path;

                    //连接server
                    int server_sock = socket(AF_UNIX, SOCK_DGRAM, 0);
                    struct sockaddr_un server_addr;
                    memset(&server_addr, 0, sizeof(server_addr));
                    server_addr.sun_family = AF_UNIX;
                    strncpy(server_addr.sun_path, socket_path.c_str(), sizeof(server_addr.sun_path) - 1);

                    // --- 发送请求：地址 + 我的 socket 路径 ---
                    std::string request = "REQ:" + std::to_string(entry.addr) + ":" + client_socket_path;
                    if (sendto(server_sock, request.c_str(), request.size(), 0,
                            (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
                        LOG(ERROR) << "Failed to send request: " << strerror(errno);
                        close(client_sock);
                        close(server_sock);
                        return false;
                    }
                    LOG(INFO) << "Sent request to server: " << request;

                    char dummy;
                    struct iovec iov = {&dummy, 1};
                    char ctrl_buf[CMSG_SPACE(sizeof(int))];
                    struct msghdr msg = {};
                    struct sockaddr_un sender_addr;
                    socklen_t sender_len = sizeof(sender_addr);

                    msg.msg_name = &sender_addr;
                    msg.msg_namelen = sender_len;
                    msg.msg_iov = &iov;
                    msg.msg_iovlen = 1;
                    msg.msg_control = ctrl_buf;
                    msg.msg_controllen = sizeof(ctrl_buf);

                    ssize_t n = recvmsg(client_sock, &msg, 0);
                    if (n <= 0) {
                        LOG(ERROR) << "recvmsg failed: " << strerror(errno);
                        close(client_sock);
                        close(server_sock);
                        return false;
                    }
                    // LOG(INFO) << "Received fd from server";

                    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
                    if (!cmsg || cmsg->cmsg_type != SCM_RIGHTS) {
                        LOG(ERROR) << "No SCM_RIGHTS received";
                        close(client_sock);
                        close(server_sock);
                        return false;
                    }
                    int  received_fd;
                    memmove(&received_fd, CMSG_DATA(cmsg), sizeof(received_fd));
                    LOG(INFO) << "Received fd from server: " << received_fd;

                    // --- 导入 CUDA handle ---
                    CUmemGenericAllocationHandle imported_fd;
                    CUresult result = cuMemImportFromShareableHandle(
                        &imported_fd, (void *)received_fd,
                        CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "cuMemImportFromShareableHandle failed: " << result;
                        close(imported_fd);
                        close(client_sock);
                        close(server_sock);
                        return false;
                    }

                    // 地址保留
                    void *mapped_addr = nullptr;
                    result = cuMemAddressReserve((CUdeviceptr*)&mapped_addr, entry.length, 0, 0, 0);
                    if (result != CUDA_SUCCESS){
                        cuMemRelease(imported_fd);
                        return -1;
                    };

                    result = cuMemMap((CUdeviceptr)mapped_addr, entry.length, 0, imported_fd, 0);
                    if (result != CUDA_SUCCESS) {
                        cuMemAddressFree((CUdeviceptr)mapped_addr, entry.length);
                        cuMemRelease(imported_fd);
                        return -1;
                    }

                    // 设置访问权限
                    int device_count;
                    cudaGetDeviceCount(&device_count);
                    std::vector<CUmemAccessDesc> access_descs(device_count);
                    for (int i = 0; i < device_count; ++i) {
                        access_descs[i].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
                        access_descs[i].location.id = i;
                        access_descs[i].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                    }
                    result = cuMemSetAccess((CUdeviceptr)mapped_addr, entry.length,
                                            access_descs.data(), device_count);
                    if (result != CUDA_SUCCESS) {
                        cuMemUnmap((CUdeviceptr)mapped_addr, entry.length);
                        cuMemAddressFree((CUdeviceptr)mapped_addr, entry.length);
                        cuMemRelease(imported_fd);
                        return -1;
                    }

                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = mapped_addr;
                    shm_entry.length = length;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;

                    dest_addr = dest_addr - entry.addr + (uint64_t)mapped_addr;
                    return 0;

                }
                else {
                    LOG(ERROR) << "Mismatched NVLink data transfer method";
                    return -1;
                }
            }
            auto shm_addr =
                remap_entries_[std::make_pair(target_id, entry.addr)].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
        index++;
    }
    LOG(ERROR) << "Requested address " << (void *)dest_addr << " to "
               << (void *)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int NvlinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int NvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

void *NvlinkTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        void *ptr = nullptr;
        LOG(INFO) << "Not support FabricMem";
        cudaMalloc(&ptr, size);
        return ptr;
    }
    LOG(INFO) << "Inside NvlinkTransport allocate Pinned Local Memory";
    size_t granularity = 0;
    CUdevice currentDev;
    CUmemAllocationProp prop = {};
    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    int cudaDev;
    int flag = 0;
    cudaError_t err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDevice failed: "
                   << cudaGetErrorString(err);
        return nullptr;
    }
    CUresult result = cuDeviceGet(&currentDev, cudaDev);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuDeviceGet failed: " << result;
        return nullptr;
    }
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    // prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    prop.location.id = currentDev;
    result = cuDeviceGetAttribute(
        &flag, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
        currentDev);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuDeviceGetAttribute failed: "
                   << result;
        return nullptr;
    }
    // if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;
    result = cuMemGetAllocationGranularity(&granularity, &prop,
                                           CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemGetAllocationGranularity failed: "
                   << result;
        return nullptr;
    }
    // fix size
    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;
    result = cuMemCreate(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemCreate failed: " << result;
        return nullptr;
    }
    result = cuMemAddressReserve((CUdeviceptr *)&ptr, size, granularity, 0, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemAddressReserve failed: " << result;
        cuMemRelease(handle);
        return nullptr;
    }
    result = cuMemMap((CUdeviceptr)ptr, size, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemMap failed: " << result;
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    int device_count;
    cudaGetDeviceCount(&device_count);
    CUmemAccessDesc accessDesc[device_count];
    for (int idx = 0; idx < device_count; ++idx) {
        accessDesc[idx].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        accessDesc[idx].location.id = idx;
        accessDesc[idx].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    }
    result = cuMemSetAccess((CUdeviceptr)ptr, size, accessDesc, device_count);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemSetAccess failed: " << result;
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    return ptr;
}

void NvlinkTransport::freePinnedLocalMemory(void *ptr) {
    if (!supportFabricMem()) {
        cudaFree(ptr);
        return;
    }
    CUmemGenericAllocationHandle handle;
    size_t size = 0;
    auto result = cuMemRetainAllocationHandle(&handle, ptr);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemRetainAllocationHandle failed: "
                   << result;
        return;
    }
    result = cuMemGetAddressRange(NULL, &size, (CUdeviceptr)ptr);
    if (result == CUDA_SUCCESS) {
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
    }
    cuMemRelease(handle);
}
}  // namespace mooncake
