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

#include "xpu_rdma.h"

#include <dirent.h>
#include <limits.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <random>
#include <utility>
#include <vector>

#include "xpu_ep_common.h"

namespace mooncake {
namespace xpu_ep {

namespace {

constexpr int kAccess =
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

std::string errnoText(int err) {
    return std::string(std::strerror(err)) + " (errno " + std::to_string(err) +
           ")";
}

// A GID as an address: dotted IPv4 for IPv4-mapped GIDs (RoCE v2 over
// IPv4), else eight hex groups.
std::string gidText(const uint8_t* g) {
    static const uint8_t kV4Mapped[12] = {0, 0, 0, 0, 0,    0,
                                          0, 0, 0, 0, 0xff, 0xff};
    char buf[16];
    if (std::memcmp(g, kV4Mapped, sizeof(kV4Mapped)) == 0) {
        std::snprintf(buf, sizeof(buf), "%u.%u.%u.%u", g[12], g[13], g[14],
                      g[15]);
        return buf;
    }
    std::string s;
    for (int i = 0; i < 16; i += 2) {
        std::snprintf(buf, sizeof(buf), "%s%02x%02x", i ? ":" : "", g[i],
                      g[i + 1]);
        s += buf;
    }
    return s;
}

// Tries to move a queue pair to RTR. On RoCE that resolves the peer's MAC,
// and the kernel gives up after about a second without a neighbour reply.
constexpr int kRtrAttempts = 5;

std::string realPath(const std::string& path) {
    char buf[PATH_MAX];
    return realpath(path.c_str(), buf) ? std::string(buf) : std::string();
}

// Number of leading path components two absolute paths share.
int commonDepth(const std::string& a, const std::string& b) {
    int depth = 0;
    size_t i = 1;  // both start with '/'
    while (i < a.size() && i < b.size()) {
        size_t ea = a.find('/', i), eb = b.find('/', i);
        if (ea == std::string::npos) ea = a.size();
        if (eb == std::string::npos) eb = b.size();
        if (a.compare(i, ea - i, b, i, eb - i) != 0) break;
        ++depth;
        i = ea + 1;
    }
    return depth;
}

using NicPaths = std::vector<std::pair<std::string, std::string>>;

// The names of the NICs nearest the device at `pci_path`, in `nics` order.
std::vector<std::string> nearestNics(const std::string& pci_path,
                                     const NicPaths& nics) {
    std::vector<std::string> best;
    int best_depth = -1;
    for (const auto& [name, path] : nics) {
        const int depth = commonDepth(pci_path, path);
        if (depth > best_depth) {
            best_depth = depth;
            best.clear();
        }
        if (depth == best_depth) best.push_back(name);
    }
    return best;
}

std::string readSysfs(const std::string& path) {
    std::ifstream in(path);
    std::string s;
    in >> s;
    return s;
}

// The sysfs paths of all Intel display controllers, in PCI order, whether or
// not this process can see them.
std::vector<std::string> intelGpuPaths() {
    std::vector<std::string> out;
    if (DIR* dir = opendir("/sys/bus/pci/devices")) {
        while (dirent* e = readdir(dir)) {
            if (e->d_name[0] == '.') continue;
            const std::string dev =
                std::string("/sys/bus/pci/devices/") + e->d_name;
            if (readSysfs(dev + "/vendor") != "0x8086" ||
                readSysfs(dev + "/class").compare(0, 4, "0x03") != 0)
                continue;
            const std::string path = realPath(dev);
            if (!path.empty()) out.push_back(path);
        }
        closedir(dir);
    }
    std::sort(out.begin(), out.end());
    return out;
}

bool isIpv4Mapped(const ibv_gid& gid) {
    static const uint8_t kPrefix[12] = {0, 0, 0, 0, 0,    0,
                                        0, 0, 0, 0, 0xff, 0xff};
    return std::memcmp(gid.raw, kPrefix, sizeof(kPrefix)) == 0;
}

}  // namespace

std::string nearestRdmaDevice(const sycl::device& device) {
    if (const char* env = std::getenv("MC_EP_XPU_NIC"); env && *env) return env;

    auto ze_dev =
        sycl::get_native<sycl::backend::ext_oneapi_level_zero>(device);
    ze_pci_ext_properties_t pci{};
    pci.stype = ZE_STRUCTURE_TYPE_PCI_EXT_PROPERTIES;
    XPU_EP_CHECK(zeDevicePciGetPropertiesExt(ze_dev, &pci) == ZE_RESULT_SUCCESS,
                 "zeDevicePciGetPropertiesExt failed");
    char bdf[32];
    std::snprintf(bdf, sizeof(bdf), "%04x:%02x:%02x.%x", pci.address.domain,
                  pci.address.bus, pci.address.device, pci.address.function);
    const std::string gpu_path =
        realPath(std::string("/sys/bus/pci/devices/") + bdf);
    XPU_EP_CHECK(!gpu_path.empty(),
                 std::string("no sysfs entry for the GPU at ") + bdf);

    std::vector<std::string> names;
    if (DIR* dir = opendir("/sys/class/infiniband")) {
        while (dirent* e = readdir(dir))
            if (e->d_name[0] != '.') names.emplace_back(e->d_name);
        closedir(dir);
    }
    std::sort(names.begin(), names.end());
    NicPaths nics;
    for (const auto& name : names) {
        std::string nic_path =
            realPath("/sys/class/infiniband/" + name + "/device");
        if (!nic_path.empty()) nics.emplace_back(name, std::move(nic_path));
    }
    const std::vector<std::string> best = nearestNics(gpu_path, nics);
    XPU_EP_CHECK(!best.empty(), "no RDMA device found");

    // GPUs equally near the same NICs take them in turn, in PCI order, so
    // two GPUs beside a dual-port card get a port each instead of sharing one.
    size_t index = 0;
    for (const auto& other : intelGpuPaths()) {
        if (other == gpu_path) break;
        if (nearestNics(other, nics) == best) ++index;
    }
    return best[index % best.size()];
}

void exportDmabuf(ze_context_handle_t ctx, const void* addr, int* fd,
                  uint64_t* offset) {
    void* base = nullptr;
    size_t size = 0;
    XPU_EP_CHECK(
        zeMemGetAddressRange(ctx, addr, &base, &size) == ZE_RESULT_SUCCESS &&
            base,
        "zeMemGetAddressRange failed");
    ze_external_memory_export_fd_t export_fd{
        ZE_STRUCTURE_TYPE_EXTERNAL_MEMORY_EXPORT_FD, nullptr,
        ZE_EXTERNAL_MEMORY_TYPE_FLAG_DMA_BUF, -1};
    ze_memory_allocation_properties_t props{};
    props.stype = ZE_STRUCTURE_TYPE_MEMORY_ALLOCATION_PROPERTIES;
    props.pNext = &export_fd;
    ze_device_handle_t dev = nullptr;
    XPU_EP_CHECK(
        zeMemGetAllocProperties(ctx, addr, &props, &dev) == ZE_RESULT_SUCCESS &&
            props.type == ZE_MEMORY_TYPE_DEVICE && export_fd.fd >= 0,
        "the driver did not export the allocation as a dma-buf");
    // dma_buf_llseek reports the size and leaves the (shared) fd's position.
    const off_t dmabuf_size = lseek(export_fd.fd, 0, SEEK_END);
    XPU_EP_CHECK(dmabuf_size >= static_cast<off_t>(size),
                 "the dma-buf is smaller than the allocation");
    *fd = export_fd.fd;
    *offset =
        reinterpret_cast<uintptr_t>(addr) - reinterpret_cast<uintptr_t>(base);
}

// ---------------------------------------------------------------------------
// RdmaDevice
// ---------------------------------------------------------------------------

RdmaDevice::RdmaDevice(const std::string& name, uint8_t port)
    : name_(name), port_(port) {
    int num = 0;
    ibv_device** list = ibv_get_device_list(&num);
    XPU_EP_CHECK(list, "ibv_get_device_list: " + errnoText(errno));
    for (int i = 0; i < num && !ctx_; ++i)
        if (name == ibv_get_device_name(list[i]))
            ctx_ = ibv_open_device(list[i]);
    ibv_free_device_list(list);
    XPU_EP_CHECK(ctx_, "cannot open RDMA device " + name);

    XPU_EP_CHECK(ibv_query_port(ctx_, port_, &port_attr_) == 0,
                 "ibv_query_port failed on " + name);
    XPU_EP_CHECK(port_attr_.state == IBV_PORT_ACTIVE,
                 name + " port " + std::to_string(port_) + " is not active");
    pd_ = ibv_alloc_pd(ctx_);
    XPU_EP_CHECK(pd_, "ibv_alloc_pd failed on " + name);

    if (port_attr_.link_layer != IBV_LINK_LAYER_ETHERNET) {
        gid_index_ = 0;
    } else if (const char* env = std::getenv("MC_EP_XPU_GID_INDEX");
               env && *env) {
        gid_index_ = std::atoi(env);
    } else {
        int first_v2 = -1;
        for (int i = 0; i < port_attr_.gid_tbl_len; ++i) {
            ibv_gid_entry entry{};
            if (ibv_query_gid_ex(ctx_, port_, i, &entry, 0) != 0) continue;
            if (entry.gid_type != IBV_GID_TYPE_ROCE_V2) continue;
            if (first_v2 < 0) first_v2 = i;
            if (isIpv4Mapped(entry.gid)) {
                gid_index_ = i;
                break;
            }
        }
        if (gid_index_ < 0) gid_index_ = first_v2;
        XPU_EP_CHECK(gid_index_ >= 0, "no RoCE v2 GID on " + name);
    }
    XPU_EP_CHECK(ibv_query_gid(ctx_, port_, gid_index_, &gid_) == 0,
                 "ibv_query_gid failed on " + name);
}

RdmaDevice::~RdmaDevice() {
    for (ibv_mr* mr : mrs_) ibv_dereg_mr(mr);
    if (pd_) ibv_dealloc_pd(pd_);
    if (ctx_) ibv_close_device(ctx_);
}

RdmaRegion RdmaDevice::registerDmabuf(void* addr, size_t bytes, int fd,
                                      uint64_t offset) {
    ibv_mr* mr = ibv_reg_dmabuf_mr(
        pd_, offset, bytes, reinterpret_cast<uint64_t>(addr), fd, kAccess);
    XPU_EP_CHECK(mr, "ibv_reg_dmabuf_mr on " + name_ + ": " + errnoText(errno));
    mrs_.push_back(mr);
    return {reinterpret_cast<uint64_t>(addr), mr->lkey, mr->rkey};
}

RdmaRegion RdmaDevice::registerHost(void* addr, size_t bytes) {
    ibv_mr* mr = ibv_reg_mr(pd_, addr, bytes, kAccess);
    XPU_EP_CHECK(mr, "ibv_reg_mr on " + name_ + ": " + errnoText(errno));
    mrs_.push_back(mr);
    return {reinterpret_cast<uint64_t>(addr), mr->lkey, mr->rkey};
}

// ---------------------------------------------------------------------------
// RdmaQueuePair
// ---------------------------------------------------------------------------

RdmaQueuePair::RdmaQueuePair(RdmaDevice& device, int max_send_wr)
    : device_(device), max_send_wr_(max_send_wr) {
    cq_ = ibv_create_cq(device_.ctx_, max_send_wr, nullptr, nullptr, 0);
    XPU_EP_CHECK(cq_,
                 "ibv_create_cq on " + device_.name_ + ": " + errnoText(errno));

    ibv_qp_init_attr init{};
    init.send_cq = cq_;
    init.recv_cq = cq_;
    init.qp_type = IBV_QPT_RC;
    init.cap.max_send_wr = max_send_wr;
    init.cap.max_recv_wr = 1;
    init.cap.max_send_sge = 1;
    init.cap.max_recv_sge = 1;
    init.cap.max_inline_data = sizeof(uint32_t);
    qp_ = ibv_create_qp(device_.pd_, &init);
    XPU_EP_CHECK(qp_,
                 "ibv_create_qp on " + device_.name_ + ": " + errnoText(errno));

    psn_ = std::random_device{}() & 0xffffff;

    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = device_.port_;
    attr.qp_access_flags = kAccess;
    const int ret = ibv_modify_qp(
        qp_, &attr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    XPU_EP_CHECK(ret == 0, "queue pair to INIT: " + errnoText(ret));
}

RdmaQueuePair::~RdmaQueuePair() {
    if (qp_) ibv_destroy_qp(qp_);
    if (cq_) ibv_destroy_cq(cq_);
}

RdmaQpInfo RdmaQueuePair::localInfo() const {
    RdmaQpInfo info;
    info.qpn = qp_->qp_num;
    info.psn = psn_;
    info.lid = device_.port_attr_.lid;
    info.mtu = static_cast<uint8_t>(device_.port_attr_.active_mtu);
    std::memcpy(info.gid, device_.gid_.raw, sizeof(info.gid));
    return info;
}

void RdmaQueuePair::connect(const RdmaQpInfo& remote) {
    ibv_device_attr dev_attr{};
    XPU_EP_CHECK(ibv_query_device(device_.ctx_, &dev_attr) == 0,
                 "ibv_query_device failed on " + device_.name_);
    const int rd_atomic = std::min(16, dev_attr.max_qp_rd_atom);

    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = static_cast<ibv_mtu>(
        std::min<int>(device_.port_attr_.active_mtu, remote.mtu));
    attr.dest_qp_num = remote.qpn;
    attr.rq_psn = remote.psn;
    attr.max_dest_rd_atomic = rd_atomic;
    attr.min_rnr_timer = 12;
    attr.ah_attr.port_num = device_.port_;
    if (device_.port_attr_.link_layer == IBV_LINK_LAYER_ETHERNET) {
        attr.ah_attr.is_global = 1;
        std::memcpy(attr.ah_attr.grh.dgid.raw, remote.gid, sizeof(remote.gid));
        attr.ah_attr.grh.sgid_index = device_.gid_index_;
        attr.ah_attr.grh.hop_limit = 64;
    } else {
        attr.ah_attr.dlid = remote.lid;
    }
    // A slow neighbour reply times out (ETIMEDOUT); the next attempt may
    // find the entry the first one requested.
    int ret = 0;
    for (int attempt = 0; attempt < kRtrAttempts; ++attempt) {
        ret =
            ibv_modify_qp(qp_, &attr,
                          IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                              IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                              IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
        if (ret != ETIMEDOUT) break;
    }
    XPU_EP_CHECK(ret == 0, "queue pair to RTR from " + device_.name_ +
                               " to GID " + gidText(remote.gid) + ": " +
                               errnoText(ret));

    attr = {};
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = psn_;
    attr.max_rd_atomic = rd_atomic;
    ret = ibv_modify_qp(qp_, &attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    XPU_EP_CHECK(ret == 0, "queue pair to RTS: " + errnoText(ret));
}

void RdmaQueuePair::post(ibv_send_wr& wr) {
    // An unsignaled request holds its send-queue slot until a later signaled
    // one completes and is polled.
    XPU_EP_CHECK(outstanding_ < max_send_wr_,
                 "send queue full: signal and poll more often");
    ibv_send_wr* bad = nullptr;
    const int ret = ibv_post_send(qp_, &wr, &bad);
    XPU_EP_CHECK(ret == 0,
                 "ibv_post_send on " + device_.name_ + ": " + errnoText(ret));
    ++outstanding_;
    ++since_signaled_;
    if (wr.send_flags & IBV_SEND_SIGNALED) {
        signaled_batches_.push_back(since_signaled_);
        since_signaled_ = 0;
    }
}

void RdmaQueuePair::write(uint64_t laddr, uint32_t lkey, uint64_t raddr,
                          uint32_t rkey, uint32_t bytes, bool signaled) {
    ibv_sge sge{laddr, bytes, lkey};
    ibv_send_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey = rkey;
    post(wr);
}

void RdmaQueuePair::writeValue(uint32_t value, uint64_t raddr, uint32_t rkey,
                               bool signaled) {
    // Inline data is copied into the work request by ibv_post_send, so the
    // stack variable may go away right after.
    ibv_sge sge{reinterpret_cast<uint64_t>(&value), sizeof(value), 0};
    ibv_send_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_INLINE | (signaled ? IBV_SEND_SIGNALED : 0);
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey = rkey;
    post(wr);
}

void RdmaQueuePair::poll(int count, int timeout_ms) {
    XPU_EP_CHECK(count <= static_cast<int>(signaled_batches_.size()),
                 "polling for more completions than signaled requests");
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    ibv_wc wcs[16];
    while (count > 0) {
        const int n = ibv_poll_cq(cq_, std::min(count, 16), wcs);
        XPU_EP_CHECK(n >= 0, "ibv_poll_cq failed on " + device_.name_);
        for (int i = 0; i < n; ++i) {
            XPU_EP_CHECK(wcs[i].status == IBV_WC_SUCCESS,
                         "RDMA completion on " + device_.name_ + ": " +
                             ibv_wc_status_str(wcs[i].status));
            outstanding_ -= signaled_batches_.front();
            signaled_batches_.pop_front();
        }
        count -= n;
        if (count > 0 && std::chrono::steady_clock::now() > deadline)
            throw std::runtime_error("Mooncake EP (XPU): RDMA completion on " +
                                     device_.name_ + " timed out");
    }
}

}  // namespace xpu_ep
}  // namespace mooncake
