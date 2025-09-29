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

#ifndef NVMEOF_GENERIC_INITIATOR_H_
#define NVMEOF_GENERIC_INITIATOR_H_

#include <memory>

#include <libaio.h>
#include <libnvme.h>

#include "transport/transport.h"

namespace mooncake {

using Slice = Transport::Slice;
using NamespaceID = Transport::FileBufferID;

class NVMeoFQueue;
class NVMeoFController;

class NVMeoFInitiator : public std::enable_shared_from_this<NVMeoFInitiator> {
    friend class NVMeoFController;

   public:
    static std::shared_ptr<NVMeoFInitiator> create(bool direct_io = false);

    ~NVMeoFInitiator();

    std::shared_ptr<NVMeoFController> attachController(
        const std::string &trtype, const std::string &adrfam,
        const std::string &traddr, const std::string &trsvcid,
        const std::string &subnqn);

    void detachController(std::shared_ptr<NVMeoFController> ctrlr);

   private:
    NVMeoFInitiator(bool direct_io);

    int setup();

    const bool direct_io;
    struct nvme_fabrics_config cfg;
    nvme_root_t root;
    nvme_host_t host;
};

class NVMeoFController : public std::enable_shared_from_this<NVMeoFController> {
    friend class NVMeoFInitiator;

   public:
    ~NVMeoFController();

    void rescan();

    std::unique_ptr<NVMeoFQueue> createQueue(size_t queueDepth);

    int getNsFd(NamespaceID nsid);

   private:
    struct NVMeoFNamespace {
        NamespaceID nsid;
        int fd;

        ~NVMeoFNamespace() { close(fd); }
    };

    NVMeoFController(std::shared_ptr<NVMeoFInitiator> initiator,
                     const std::string &trtype, const std::string &adrfam,
                     const std::string &traddr, const std::string &trsvcid,
                     const std::string &subnqn);

    int connect();

    int disconnect();

    const std::shared_ptr<NVMeoFInitiator> initiator;
    const std::string trtype;
    const std::string adrfam;
    const std::string traddr;
    const std::string trsvcid;
    const std::string subnqn;

    nvme_ctrl_t ctrl;
    bool should_disconnect_ctrl;

    RWSpinlock ns_lock;
    std::unordered_map<NamespaceID, NVMeoFNamespace> namespaces;
};

class NVMeoFQueue {
    friend class NVMeoFController;

   public:
    ~NVMeoFQueue();

    int submitRequest(Slice *slice);

    void reapCompletions();

    std::shared_ptr<NVMeoFController> getCtrlr() { return this->ctrlr; }

   private:
    NVMeoFQueue(std::shared_ptr<NVMeoFController> ctrlr, size_t queueDepth);

    int setup();

    std::shared_ptr<NVMeoFController> ctrlr;
    size_t depth;
    io_context_t io_ctx;
    std::vector<struct io_event> events;
};

}  // namespace mooncake
#endif