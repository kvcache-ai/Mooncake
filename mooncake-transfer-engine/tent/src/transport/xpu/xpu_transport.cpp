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

#include "tent/transport/xpu/xpu_transport.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <sstream>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {

namespace {

// Identity of this process's pid namespace ("pid:[<inode>]"): a pid published
// by a peer is only meaningful to pidfd_open when both sides share it.
std::string pidNamespace() {
    char buf[64];
    ssize_t n = readlink("/proc/self/ns/pid", buf, sizeof(buf) - 1);
    if (n <= 0) return "";
    buf[n] = '\0';
    return buf;
}

// Exporter attribute: "<pid>-<base>-<size>-<handle hex>-<pid namespace>".
// The handle is the allocation's Level Zero IPC handle as produced in the
// exporter; it names a descriptor of that process, hence the pid.
struct IpcAttr {
    pid_t pid = 0;
    uint64_t base = 0;
    uint64_t size = 0;
    XpuPlatform::IpcHandle handle{};
    std::string pid_namespace;
};

std::string formatIpcAttr(const IpcAttr &attr) {
    static const char kHex[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(attr.handle.size() * 2);
    for (uint8_t byte : attr.handle) {
        hex.push_back(kHex[byte >> 4]);
        hex.push_back(kHex[byte & 0xf]);
    }
    std::ostringstream ss;
    ss << attr.pid << '-' << attr.base << '-' << attr.size << '-' << hex << '-'
       << attr.pid_namespace;
    return ss.str();
}

bool parseIpcAttr(const std::string &text, IpcAttr &attr) {
    std::vector<std::string> parts;
    std::stringstream ss(text);
    std::string token;
    while (std::getline(ss, token, '-')) parts.push_back(token);
    if (parts.size() != 5) return false;
    try {
        attr.pid = static_cast<pid_t>(std::stol(parts[0]));
        attr.base = std::stoull(parts[1]);
        attr.size = std::stoull(parts[2]);
    } catch (const std::exception &) {
        return false;
    }
    const std::string &hex = parts[3];
    if (hex.size() != attr.handle.size() * 2) return false;
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < attr.handle.size(); ++i) {
        const int hi = nibble(hex[2 * i]), lo = nibble(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return false;
        attr.handle[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    attr.pid_namespace = parts[4];
    return attr.pid > 0 && attr.size > 0;
}

}  // namespace

XpuTransport::XpuTransport()
    : installed_(false), ipc_enabled_(false), platform_(nullptr) {}

XpuTransport::~XpuTransport() { uninstall(); }

Status XpuTransport::install(std::string &local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "XPU transport has been installed" LOC_MARK);
    }
    // Bind to the concrete XpuPlatform (as NVLinkTransport binds to
    // CudaPlatform); every device operation is driven through it.
    platform_ = dynamic_cast<XpuPlatform *>(&Platform::getLoader());
    if (!platform_) {
        return Status::InvalidArgument(
            "XPU transport requires the XpuPlatform to be active" LOC_MARK);
    }
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    ipc_enabled_ = !(conf && conf->get("transports/xpu/disable_ipc", false));
    if (metadata_) {
        auto local = metadata_->segmentManager().getLocal();
        if (local) machine_id_ = local->machine_id;
    }
    pid_namespace_ = pidNamespace();
    installed_ = true;
    // Executes same-host copies only: the device<->host staging hop, the
    // intra-node device<->device copy (same XPU, or PCIe P2P between two XPUs
    // with peer access; the platform bounces through host memory otherwise),
    // and the same copy against a peer process's buffer once it has been
    // mapped in with Level Zero IPC (prepareRemoteBuffer). Direct cross-node
    // VRAM access is RdmaTransport's job (dma-buf registration).
    caps.gpu_to_dram = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_gpu = true;
    return Status::OK();
}

Status XpuTransport::uninstall() {
    if (installed_) {
        {
            std::lock_guard<std::mutex> lock(imports_lock_);
            for (auto &segment : imports_) {
                for (auto &entry : segment.second) {
                    auto status = platform_->closeImport(entry.ptr);
                    if (!status.ok())
                        LOG(WARNING) << "XpuTransport: closing imported "
                                        "buffer failed: "
                                     << status.ToString();
                }
            }
            imports_.clear();
            failed_imports_.clear();
        }
        metadata_.reset();
        platform_ = nullptr;
        installed_ = false;
    }
    return Status::OK();
}

Status XpuTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto xpu_batch = Slab<XpuSubBatch>::Get().allocate();
    if (!xpu_batch)
        return Status::InternalError(
            "Unable to allocate XPU sub-batch" LOC_MARK);
    batch = xpu_batch;
    xpu_batch->task_list.reserve(max_size);
    xpu_batch->max_size = max_size;
    return Status::OK();
}

Status XpuTransport::freeSubBatch(SubBatchRef &batch) {
    auto xpu_batch = dynamic_cast<XpuSubBatch *>(batch);
    if (!xpu_batch)
        return Status::InvalidArgument("Invalid XPU sub-batch" LOC_MARK);
    Slab<XpuSubBatch>::Get().deallocate(xpu_batch);
    batch = nullptr;
    return Status::OK();
}

Status XpuTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto xpu_batch = dynamic_cast<XpuSubBatch *>(batch);
    if (!xpu_batch)
        return Status::InvalidArgument("Invalid XPU sub-batch" LOC_MARK);
    if (request_list.size() + xpu_batch->task_list.size() > xpu_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    // Resolve every target before any copy starts, so a request whose peer
    // buffer is not mapped fails synchronously with no half-appended tasks
    // left behind; the engine then fails the task over to another transport.
    std::vector<uint64_t> targets;
    targets.reserve(request_list.size());
    for (auto &request : request_list) {
        uint64_t target = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID &&
            !relocate(request.target_id, target, request.length)) {
            return Status::InvalidArgument(
                "XPU transport: target buffer of remote segment is not mapped "
                "into this process" LOC_MARK);
        }
        targets.push_back(target);
    }
    for (size_t i = 0; i < request_list.size(); ++i) {
        // emplace_back default-constructs in place: XpuTask holds std::atomic
        // members and is therefore non-movable, and the reserved capacity in
        // allocateSubBatch guarantees no reallocation.
        auto &task = xpu_batch->task_list.emplace_back();
        task.request = request_list[i];
        task.target_addr = targets[i];
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;
        startTransfer(&task, xpu_batch);
    }
    return Status::OK();
}

void XpuTransport::startTransfer(XpuTask *task, XpuSubBatch *batch) {
    // Every copy is executed by this process: `target_addr` is either a
    // local address (LOCAL_SEGMENT_ID: a staging hop or an intra-node
    // device<->device copy) or the local mapping of a peer process's XPU
    // buffer (submitTransferTasks relocated it).
    void *target = reinterpret_cast<void *>(task->target_addr);

    // At least one side must be XPU VRAM. In a staging hop exactly one side
    // is: the local stage copies VRAM<->host staging buffer, and a delegated
    // remote stage copies the peer's host staging buffer<->its VRAM (so
    // `target` is the device side there). In a local device<->device request
    // both sides are, as they are when `target` is an imported peer buffer.
    // Classify through the bound XpuPlatform, which resolves both base and
    // interior USM pointers (own, foreign and imported) via its backend
    // registry, so a device pointer handed to us as `base + chunk_offset` is
    // still recognised. If neither side is device memory the request is
    // malformed; fail loudly rather than let a plain host memcpy corrupt the
    // transfer.
    const bool source_is_device =
        platform_->getMemoryType(task->request.source) == MTYPE_XPU;
    const bool target_is_device = platform_->getMemoryType(target) == MTYPE_XPU;
    if (!source_is_device && !target_is_device) {
        LOG(ERROR) << "XpuTransport: neither source=" << task->request.source
                   << " nor target=" << target
                   << " is XPU device memory. This indicates a routing bug or "
                      "an unclassified external USM pointer.";
        task->status_word = TransferStatusEnum::FAILED;
        task->transferred_bytes = 0;
        batch->notifyProgress();
        return;
    }

    // Which side is the device depends on the stage (see above), so the
    // direction is expressed as dataflow: READ pulls `target` into `source`,
    // WRITE pushes `source` into `target`. platform_->copy() derives
    // H2D/D2H/D2D from the pointer classification.
    Status status;
    if (task->request.opcode == Request::READ)
        // target -> source
        status =
            platform_->copy(task->request.source, target, task->request.length);
    else
        // source -> target
        status =
            platform_->copy(target, task->request.source, task->request.length);

    if (status.ok()) {
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    } else {
        LOG(WARNING) << "XpuTransport: local copy failed: "
                     << status.ToString();
        task->status_word = TransferStatusEnum::FAILED;
    }
    batch->notifyProgress();
}

Status XpuTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto xpu_batch = dynamic_cast<XpuSubBatch *>(batch);
    if (!xpu_batch)
        return Status::InvalidArgument("Invalid XPU sub-batch" LOC_MARK);
    if (task_id < 0 || task_id >= (int)xpu_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = xpu_batch->task_list[task_id];
    status =
        TransferStatus{task.status_word.load(), task.transferred_bytes.load()};
    return Status::OK();
}

Status XpuTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    LocationParser location(desc.location);
    // Tag both XPU device buffers and host staging buffers so the staging
    // policy can route the local VRAM<->host hop through this transport.
    // Routing keys on the target (host) buffer's transports, so the host buffer
    // must carry TransportType::XPU as well (mirrors NVLinkTransport).
    if (location.type() != "xpu" && location.type() != "cpu" &&
        location.type() != kWildcardLocation) {
        return Status::OK();  // Not our buffer; leave it untagged.
    }
    // Idempotent: a buffer may be re-registered, so avoid duplicate XPU tags.
    if (std::find(desc.transports.begin(), desc.transports.end(),
                  TransportType::XPU) == desc.transports.end()) {
        desc.transports.push_back(TransportType::XPU);
    }
    // Publish the Level Zero IPC handle of an XPU buffer so a peer process on
    // this host can map it (prepareRemoteBuffer). The handle stays valid until
    // the allocation is freed. desc.addr/length are left as registered -- the
    // attribute carries the allocation's own base and size -- so RDMA
    // registration of the same buffer is unaffected. A buffer that cannot be
    // exported (non-Level-Zero device, range spanning two allocations) simply
    // carries no attribute and is reached through RDMA or staging like before.
    if (ipc_enabled_ && location.type() == "xpu") {
        XpuPlatform::IpcExport exported;
        auto status = platform_->exportIpc(reinterpret_cast<void *>(desc.addr),
                                           desc.length, exported);
        if (status.ok()) {
            IpcAttr attr;
            attr.pid = getpid();
            attr.handle = exported.handle;
            attr.base = exported.base;
            attr.size = exported.size;
            attr.pid_namespace = pid_namespace_;
            desc.transport_attrs[TransportType::XPU] = formatIpcAttr(attr);
        } else {
            LOG(INFO) << "XpuTransport: buffer " << (void *)desc.addr << " ("
                      << desc.length
                      << " bytes) is not shareable with peer processes: "
                      << status.ToString();
            desc.transport_attrs.erase(TransportType::XPU);
        }
    }
    return Status::OK();
}

Status XpuTransport::removeMemoryBuffer(BufferDesc &desc) {
    // Keep buffer metadata consistent with addMemoryBuffer by dropping the tag.
    desc.transports.erase(
        std::remove(desc.transports.begin(), desc.transports.end(),
                    TransportType::XPU),
        desc.transports.end());
    desc.transport_attrs.erase(TransportType::XPU);
    return Status::OK();
}

bool XpuTransport::prepareRemoteBuffer(SegmentID target_id,
                                       const SegmentDesc &segment,
                                       const BufferDesc &entry,
                                       void *local_source) {
    if (!installed_ || !ipc_enabled_ || target_id == LOCAL_SEGMENT_ID)
        return false;
    auto it = entry.transport_attrs.find(TransportType::XPU);
    if (it == entry.transport_attrs.end() || it->second.empty()) return false;
    // machine_id embeds the boot id, so equal values mean this very host.
    if (machine_id_.empty() || segment.machine_id != machine_id_) return false;
    const std::string &attr_text = it->second;

    std::lock_guard<std::mutex> lock(imports_lock_);
    auto &entries = imports_[target_id];
    for (auto &imported : entries) {
        if (imported.attr == attr_text) return true;
    }
    if (failed_imports_.count(attr_text)) return false;

    IpcAttr attr;
    if (!parseIpcAttr(attr_text, attr) ||
        attr.pid_namespace != pid_namespace_ || attr.base > entry.addr ||
        entry.addr + entry.length > attr.base + attr.size) {
        LOG_EVERY_N(WARNING, 1000)
            << "XpuTransport: peer buffer " << (void *)entry.addr
            << " in segment " << segment.name
            << " carries an XPU IPC attribute this process cannot use ("
            << attr_text << "); using another route";
        failed_imports_.insert(attr_text);
        return false;
    }
    // The peer may have freed and re-registered the same range: drop the
    // mapping of the previous allocation before importing the new one.
    for (size_t i = 0; i < entries.size(); ++i) {
        if (entries[i].base != attr.base) continue;
        auto status = platform_->closeImport(entries[i].ptr);
        if (!status.ok())
            LOG(WARNING) << "XpuTransport: closing stale imported buffer "
                            "failed: "
                         << status.ToString();
        entries.erase(entries.begin() + i);
        break;
    }

    // Open on the device that will drive the copy: the one holding the local
    // source, or device 0 for a host source. The local device reaches the
    // peer's VRAM over PCIe P2P (or Xe Link) through this mapping.
    int device_index = platform_->deviceIndex(local_source);
    if (device_index < 0) device_index = 0;
    void *ptr = nullptr;
    errno = 0;
    auto status = platform_->importIpc(attr.handle, attr.pid, attr.size,
                                       device_index, &ptr);
    if (!status.ok()) {
        const int err = errno;
        LOG_EVERY_N(WARNING, 1000)
            << "XpuTransport: cannot map peer buffer " << (void *)entry.addr
            << " of process " << attr.pid << " in segment " << segment.name
            << " on xpu:" << device_index << ": " << status.ToString()
            << (err == EPERM
                    ? " (fetching the exporter's dma-buf needs ptrace access "
                      "to it: same user with kernel.yama.ptrace_scope <= 1, "
                      "or CAP_SYS_PTRACE)"
                    : "")
            << "; using another route";
        failed_imports_.insert(attr_text);
        return false;
    }
    LOG(INFO) << "XpuTransport: mapped peer buffer " << (void *)attr.base
              << " (" << attr.size << " bytes) of segment " << segment.name
              << " at " << ptr << " on xpu:" << device_index;
    entries.push_back(ImportEntry{attr_text, attr.base, attr.size, ptr});
    return true;
}

bool XpuTransport::relocate(SegmentID target_id, uint64_t &addr,
                            uint64_t length) {
    std::lock_guard<std::mutex> lock(imports_lock_);
    auto it = imports_.find(target_id);
    if (it == imports_.end()) return false;
    for (auto &entry : it->second) {
        if (entry.base <= addr && addr + length <= entry.base + entry.size) {
            addr = addr - entry.base + reinterpret_cast<uint64_t>(entry.ptr);
            return true;
        }
    }
    return false;
}

}  // namespace tent
}  // namespace mooncake
