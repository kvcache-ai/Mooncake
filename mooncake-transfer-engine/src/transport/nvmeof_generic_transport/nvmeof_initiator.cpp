// Copyright 2025 Alibaba Cloud and its affiliates
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

#include "transport/nvmeof_generic_transport/nvmeof_initiator.h"

#include <fcntl.h>
#include <unordered_set>

namespace mooncake {
static constexpr auto kMaxRescanDuration = std::chrono::seconds(15);

static nvme_ctrl_t nvme_find_ctrl(nvme_root_t root, nvme_host_t host,
                                  const std::string &trtype,
                                  const std::string &traddr,
                                  const std::string &trsvcid,
                                  const std::string &subnqn) {
    nvme_subsystem_t subsys;
    nvme_ctrl_t ctrl;

    // Scan the topology first.
    nvme_scan_topology(root, NULL, NULL);

    nvme_for_each_subsystem(host, subsys) {
        nvme_subsystem_for_each_ctrl(subsys, ctrl) {
            if (strcasecmp(nvme_ctrl_get_transport(ctrl), trtype.c_str())) {
                continue;
            }

            if (strcmp(nvme_ctrl_get_traddr(ctrl), traddr.c_str())) {
                continue;
            }

            if (strcmp(nvme_ctrl_get_trsvcid(ctrl), trsvcid.c_str())) {
                continue;
            }

            if (strcmp(nvme_ctrl_get_subsysnqn(ctrl), subnqn.c_str())) {
                continue;
            }

            return ctrl;
        }
    }

    return nullptr;
}

static int nvme_get_active_ns_list(nvme_ctrl_t ctrl,
                                   std::unordered_set<uint32_t> &ns_list) {
    struct nvme_ns_list ns_list_ = {0};

    int fd = nvme_ctrl_get_fd(ctrl);
    if (fd < 0) {
        LOG(ERROR) << "Invalid fd " << fd << " of controller "
                   << nvme_ctrl_get_subsysnqn(ctrl);
        return -EINVAL;
    }

    int rc = nvme_identify_active_ns_list(fd, 0, &ns_list_);
    if (rc != 0) {
        LOG(ERROR) << "Failed to identify active ns list of controller "
                   << nvme_ctrl_get_subsysnqn(ctrl) << ", rc=" << rc;
        return -EIO;
    }

    for (size_t i = 0; i < NVME_ID_NS_LIST_MAX; i++) {
        if (ns_list_.ns[i] > 0) {
            ns_list.insert(ns_list_.ns[i]);
        }
    }

    return 0;
}

std::shared_ptr<NVMeoFInitiator> NVMeoFInitiator::create(bool direct_io) {
    auto initiator =
        std::shared_ptr<NVMeoFInitiator>(new NVMeoFInitiator(direct_io));
    int rc = initiator->setup();
    if (rc != 0) {
        LOG(ERROR) << "Failed to create nvmeof initiator, rc=" << rc;
        return nullptr;
    }

    return initiator;
}

NVMeoFInitiator::NVMeoFInitiator(bool direct_io)
    : direct_io(direct_io), root(nullptr), host(nullptr) {}

NVMeoFInitiator::~NVMeoFInitiator() {
    if (root != nullptr) {
        nvme_free_tree(root);
    }
}

int NVMeoFInitiator::setup() {
    nvmf_default_config(&cfg);

    // Disconnect the controller immediately on error.
    cfg.ctrl_loss_tmo = 0;

    root = nvme_scan(NULL);
    if (root == NULL) {
        LOG(ERROR) << "Failed to create NVMe root";
        return -ENOMEM;
    }

    host = nvme_default_host(root);
    if (host == NULL) {
        LOG(ERROR) << "Failed to create default NVMe host";
        return -ENOMEM;
    }

    return 0;
}

std::shared_ptr<NVMeoFController> NVMeoFInitiator::attachController(
    const std::string &trtype, const std::string &adrfam,
    const std::string &traddr, const std::string &trsvcid,
    const std::string &subnqn) {
    auto ctrlr = std::shared_ptr<NVMeoFController>(new NVMeoFController(
        shared_from_this(), trtype, adrfam, traddr, trsvcid, subnqn));
    int rc = ctrlr->connect();
    if (rc != 0) {
        LOG(ERROR) << "Failed to connect controller " << subnqn
                   << ", rc=" << rc;
        return nullptr;
    }

    return ctrlr;
}

void NVMeoFInitiator::detachController(
    std::shared_ptr<NVMeoFController> ctrlr) {
    ctrlr->disconnect();
}

NVMeoFController::NVMeoFController(std::shared_ptr<NVMeoFInitiator> initiator,
                                   const std::string &trtype,
                                   const std::string &adrfam,
                                   const std::string &traddr,
                                   const std::string &trsvcid,
                                   const std::string &subnqn)
    : initiator(initiator),
      trtype(trtype),
      adrfam(adrfam),
      traddr(traddr),
      trsvcid(trsvcid),
      subnqn(subnqn),
      ctrl(nullptr),
      should_disconnect_ctrl(false) {}

NVMeoFController::~NVMeoFController() {
    if (ctrl != nullptr) {
        if (should_disconnect_ctrl) {
            nvme_disconnect_ctrl(ctrl);
        }
        nvme_free_ctrl(ctrl);
    }
}

int NVMeoFController::connect() {
    ctrl = nvme_find_ctrl(initiator->root, initiator->host, trtype, traddr,
                          trsvcid, subnqn);
    if (ctrl != nullptr) {
        // The controller has been connected.
        rescan();
        return 0;
    }

    ctrl = nvme_create_ctrl(initiator->root, subnqn.c_str(), trtype.c_str(),
                            traddr.c_str(), NULL, NULL, trsvcid.c_str());
    if (ctrl == NULL) {
        LOG(ERROR) << "Failed to create nvme controller " << subnqn;
        return -ENOMEM;
    }

    int rc = nvmf_add_ctrl(initiator->host, ctrl, &initiator->cfg);
    if (rc != 0) {
        LOG(ERROR) << "Failed to connect to controller, " << subnqn
                   << " rc=" << rc;
        return rc;
    }

    // We connected the controller, so we are responsible for disconnecting it.
    should_disconnect_ctrl = true;

    // Trigger rescan to open namespaces.
    rescan();

    return 0;
}

void NVMeoFController::rescan() {
    if (ctrl == nullptr) {
        // Do not scan disconnected controller.
        return;
    }

    RWSpinlock::WriteGuard guard(ns_lock);
    const auto rescan_timeout =
        std::chrono::steady_clock::now() + kMaxRescanDuration;

    while (true) {
        // Retrieve active namespace list via NVMe Identify command.
        std::unordered_set<uint32_t> active_ns;
        int rc = nvme_get_active_ns_list(ctrl, active_ns);
        if (rc != 0) {
            LOG(ERROR) << "Failed to get active ns list of controller "
                       << nvme_ctrl_get_name(ctrl) << ", rc=" << rc;
            break;
        }

        // Remove invalid namespaces.
        auto it = namespaces.begin();
        while (it != namespaces.end()) {
            if (!active_ns.contains(it->first)) {
                it = namespaces.erase(it);
            } else {
                it++;
            }
        }

        // Scan controller sysfs directory to get attached namespaces.
        struct dirent **ns_dirents = NULL;
        int num_ns_dirents = nvme_scan_ctrl_namespaces(ctrl, &ns_dirents);
        if (num_ns_dirents < 0) {
            LOG(ERROR) << "Failed to scan namespaces of controller "
                       << nvme_ctrl_get_name(ctrl) << ", errno=" << errno;
            break;
        }

        // Open namespace block devices.
        for (int i = 0; i < num_ns_dirents; i++) {
            char ns_dev[256];
            rc = snprintf(ns_dev, sizeof(ns_dev), "/dev/%s",
                          ns_dirents[i]->d_name);
            if (rc <= 0) {
                LOG(ERROR) << "Invalid namespace device name "
                           << ns_dirents[i]->d_name;
                continue;
            }

            int flags = O_RDWR;
            if (initiator->direct_io) flags |= O_DIRECT;

            int fd = open(ns_dev, flags);
            if (fd < 0) {
                LOG(ERROR) << "Failed to open nvme namespace " << ns_dev
                           << ", errno=" << errno;
                continue;
            }

            uint32_t nsid;
            rc = nvme_get_nsid(fd, &nsid);
            if (rc != 0) {
                LOG(ERROR) << "Failed to get nsid of namespace "
                           << ns_dirents[i]->d_name << ", errno=" << errno;
                close(fd);
                continue;
            }

            if (namespaces.contains(nsid) && namespaces[nsid].fd >= 0) {
                // The namespace has been open.
                close(fd);
                continue;
            }

            LOG(INFO) << "Added namespace " << nsid << " of controller "
                      << nvme_ctrl_get_name(ctrl);
            namespaces[nsid] = {nsid, fd};
        }

        // Free dirents.
        for (int i = 0; i < num_ns_dirents; i++) {
            free(ns_dirents[i]);
        }
        free(ns_dirents);

        // Check if all active namespaces are open.
        if (namespaces.size() == active_ns.size()) {
            break;
        }

        if (std::chrono::steady_clock::now() >= rescan_timeout) {
            LOG(ERROR) << "Timedout to wait for namespaces of " << subnqn
                       << " to be attached, expected " << active_ns.size()
                       << ", attached " << namespaces.size();
            break;
        }

        // Wait a moment for namespaces to be attached.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

int NVMeoFController::disconnect() {
    {
        RWSpinlock::WriteGuard guard(ns_lock);
        namespaces.clear();
    }

    if (ctrl != nullptr) {
        if (should_disconnect_ctrl) {
            should_disconnect_ctrl = false;
            nvme_disconnect_ctrl(ctrl);
        }
        nvme_free_ctrl(ctrl);
        ctrl = nullptr;
    }

    return 0;
}

std::unique_ptr<NVMeoFQueue> NVMeoFController::createQueue(size_t queueDepth) {
    auto queue = std::unique_ptr<NVMeoFQueue>(
        new NVMeoFQueue(shared_from_this(), queueDepth));
    int rc = queue->setup();
    if (rc != 0) {
        LOG(ERROR) << "Failed to create queue, rc=" << rc;
        return nullptr;
    }

    return queue;
}

int NVMeoFController::getNsFd(NamespaceID nsid) {
    RWSpinlock::ReadGuard guard(ns_lock);
    auto it = namespaces.find(nsid);
    if (it == namespaces.end()) {
        return -1;
    }
    return it->second.fd;
}

NVMeoFQueue::NVMeoFQueue(std::shared_ptr<NVMeoFController> ctrlr,
                         size_t queueDepth)
    : ctrlr(ctrlr), depth(queueDepth), io_ctx(nullptr), events(depth) {}

NVMeoFQueue::~NVMeoFQueue() {
    if (io_ctx != nullptr) {
        io_destroy(this->io_ctx);
    }
}

int NVMeoFQueue::setup() {
    int rc = io_setup(this->depth, &this->io_ctx);
    if (rc != 0) {
        LOG(ERROR) << "Failed to setup aio context, rc=" << rc;
        return rc;
    }
    return 0;
}

int NVMeoFQueue::submitRequest(Slice *slice) {
    int fd = ctrlr->getNsFd(slice->file_id);
    if (fd < 0) {
        LOG(ERROR) << "No namespace " << slice->file_id
                   << " in nvme controller";
        return -ENOENT;
    }

    struct iocb *iocb = &slice->nvmeof_generic.iocb;
    if (slice->opcode == Transport::TransferRequest::READ) {
        io_prep_pread(iocb, fd, slice->source_addr, slice->length,
                      slice->nvmeof_generic.offset);
    } else {
        io_prep_pwrite(iocb, fd, slice->source_addr, slice->length,
                       slice->nvmeof_generic.offset);
    }
    iocb->data = slice;

    int rc = io_submit(this->io_ctx, 1, &iocb);
    return rc > 0 ? 0 : rc;
}

void NVMeoFQueue::reapCompletions() {
    struct timespec timeout = {
        .tv_sec = 0,
        .tv_nsec = 0,
    };
    Slice *slice = nullptr;

    int rc = io_getevents(this->io_ctx, 0, this->depth, this->events.data(),
                          &timeout);
    if (rc < 0) {
        LOG(ERROR) << "Failed to poll aio events, rc = " << rc;
        return;
    }

    for (int i = 0; i < rc; i++) {
        slice = (Slice *)(events[i].data);
        if (events[i].res == slice->length) {
            slice->markSuccess();
        } else {
            slice->markFailed();
        }
    }
}
};  // namespace mooncake