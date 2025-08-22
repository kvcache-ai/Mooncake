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

namespace mooncake {
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

nvme_ctrl_t NVMeoFController::findCtrl() {
    nvme_subsystem_t subsys;
    nvme_ctrl_t ctrl;

    // Scan the topology first.
    nvme_scan_topology(initiator->root, NULL, NULL);

    nvme_for_each_subsystem(initiator->host, subsys) {
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

int NVMeoFController::connect() {
    ctrl = findCtrl();
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

    // Wait a moment to ensure all namespaces are attached.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Trigger rescan to open namespaces.
    rescan();

    return 0;
}

void NVMeoFController::rescan() {
    if (ctrl == nullptr) {
        // Do not scan disconnected controller.
        return;
    }

    // Rescan the topology.
    nvme_scan_topology(initiator->root, NULL, NULL);

    RWSpinlock::WriteGuard guard(ns_lock);
    nvme_ns_t ns;
    char ns_dev[64];

    nvme_ctrl_for_each_ns(ctrl, ns) {
        auto nsid = static_cast<NamespaceID>(nvme_ns_get_nsid(ns));
        auto it = namespaces.find(nsid);
        if (it != namespaces.end() && it->second.fd >= 0) {
            // Namespace has been open.
            continue;
        }

        const char *name = nvme_ns_get_name(ns);
        int rc = snprintf(ns_dev, sizeof(ns_dev), "/dev/%s", name);
        if (rc <= 0) {
            LOG(ERROR) << "Invalid namespace device name " << name;
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

        LOG(INFO) << "Added namespace " << nsid << " to controller "
                  << nvme_ctrl_get_name(ctrl);
        namespaces[nsid] = {nsid, fd};
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
                      slice->nvmeof.offset);
    } else {
        io_prep_pwrite(iocb, fd, slice->source_addr, slice->length,
                       slice->nvmeof.offset);
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