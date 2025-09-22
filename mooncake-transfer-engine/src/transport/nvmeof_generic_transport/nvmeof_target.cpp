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

#include "transport/nvmeof_generic_transport/nvmeof_target.h"

#include <glog/logging.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <filesystem>

namespace mooncake {
namespace nvmeof_target {
static const std::filesystem::path kNVMeTConfigPath =
    "/sys/kernel/config/nvmet";

static inline std::filesystem::path makePortPath(uint32_t portid) {
    return kNVMeTConfigPath / "ports" / std::to_string(portid);
}

static inline std::filesystem::path makeSubsysPath(const std::string &subnqn) {
    return kNVMeTConfigPath / "subsystems" / subnqn;
}

static inline std::filesystem::path makeSubsysLinkPath(
    const std::string &subnqn, uint32_t portid) {
    return makePortPath(portid) / "subsystems" / subnqn;
}

static inline std::filesystem::path makeNamespacePath(const std::string &subnqn,
                                                      NamespaceID nsid) {
    return makeSubsysPath(subnqn) / "namespaces" / std::to_string(nsid);
}

static inline bool pathExists(const std::filesystem::path &path) {
    return access(path.c_str(), F_OK) == 0;
}

static inline int mkDir(const std::filesystem::path &path) {
    int rc = mkdir(path.c_str(), 0644);
    if (rc != 0) {
        LOG(ERROR) << "Failed to make directory " << path
                   << ", errno=" << errno;
        return -errno;
    }
    return 0;
}

static inline int rmDir(const std::filesystem::path &path) {
    int rc = rmdir(path.c_str());
    if (rc != 0) {
        LOG(ERROR) << "Failed to remove directory " << path
                   << ", errno=" << errno;
        return -errno;
    }
    return 0;
}

static inline int setAttr(const std::filesystem::path &path,
                          const std::string &value) {
    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open file " << path << ", errno=" << errno;
        return -errno;
    }

    int rc = write(fd, value.c_str(), value.size());
    if (rc < 0) {
        LOG(ERROR) << "Failed to write \"" << value << "\" to file " << path
                   << ", errno=" << errno;
        rc = -errno;
    }

    close(fd);

    return rc >= 0 ? 0 : rc;
}

static inline int symLink(const std::filesystem::path &dest,
                          const std::filesystem::path &name) {
    int rc = symlink(dest.c_str(), name.c_str());
    if (rc != 0) {
        LOG(ERROR) << "Failed to create symlink to " << dest << " at " << name
                   << ", errno=" << errno;
        return -errno;
    }
    return 0;
}

static inline int unLink(const std::filesystem::path &name) {
    int rc = unlink(name.c_str());
    if (rc != 0) {
        LOG(ERROR) << "Failed to unlink " << name << ", errno=" << errno;
        return -errno;
    }
    return 0;
}

std::atomic<uint32_t> NVMeoFListener::next_id = 1;

NVMeoFNamespace::NVMeoFNamespace(const std::string &subnqn, NamespaceID nsid,
                                 const std::string &file)
    : subnqn(subnqn), nsid(nsid), file(file) {}

NVMeoFNamespace::~NVMeoFNamespace() {
    auto path = makeNamespacePath(subnqn, nsid);
    if (pathExists(path)) {
        if (pathExists(path / "enable")) {
            setAttr(path / "enable", "0");
        }
        rmDir(path);
    }
}

int NVMeoFNamespace::setup() {
    auto path = makeNamespacePath(subnqn, nsid);

    int rc = mkDir(path);
    if (rc != 0) {
        LOG(ERROR) << "Failed to create ns " << std::to_string(nsid)
                   << " of subsys " << subnqn;
        return rc;
    }

    rc = setAttr(path / "device_path", file);
    if (rc != 0) {
        LOG(ERROR) << "Failed to set device_path for ns "
                   << std::to_string(nsid) << " of subsys " << subnqn;
        return rc;
    }

    rc = setAttr(path / "enable", "1");
    if (rc != 0) {
        LOG(ERROR) << "Failed to enable ns " << std::to_string(nsid)
                   << " of subsys " << subnqn;
        return rc;
    }

    return 0;
}

NVMeoFSubsystem::NVMeoFSubsystem(const std::string &subnqn) : subnqn(subnqn) {}

NVMeoFSubsystem::~NVMeoFSubsystem() {
    // Remove namespaces before removing the subsystem.
    namespaces.clear();

    auto path = makeSubsysPath(subnqn);
    if (pathExists(path)) {
        rmDir(path);
    }
}

int NVMeoFSubsystem::setup() {
    auto path = makeSubsysPath(subnqn);

    int rc = mkDir(path);
    if (rc != 0) {
        LOG(ERROR) << "Failed to create subsystem " << subnqn;
        return rc;
    }

    rc = setAttr(path / "attr_allow_any_host", "1");
    if (rc != 0) {
        LOG(ERROR) << "Failed to set allow_any_host for subsystem " << subnqn;
        return rc;
    }

    return 0;
}

int NVMeoFSubsystem::addNamespace(NamespaceID nsid, const std::string &file) {
    for (auto &it : namespaces) {
        if (it.first == nsid || it.second->file == file) {
            LOG(ERROR) << "Duplicated namespace " << nsid << ", file=" << file;
            return -EEXIST;
        }
    }

    auto ns = std::make_unique<NVMeoFNamespace>(subnqn, nsid, file);
    int rc = ns->setup();
    if (rc != 0) {
        LOG(ERROR) << "Failed to add namespace " << nsid << " to subsystem "
                   << subnqn << ", rc=" << rc;
        return rc;
    }

    namespaces[nsid] = std::move(ns);
    return 0;
}

int NVMeoFSubsystem::removeNamespace(NamespaceID nsid) {
    namespaces.erase(nsid);
    return 0;
}

NVMeoFListener::NVMeoFListener(const std::string &trtype,
                               const std::string &adrfam,
                               const std::string &traddr,
                               const std::string &trsvcid)
    : trtype(trtype),
      adrfam(adrfam),
      traddr(traddr),
      trsvcid(trsvcid),
      id(next_id++) {}

NVMeoFListener::~NVMeoFListener() {
    for (auto &subsys : subsystems) {
        auto path = makeSubsysLinkPath(subsys->subnqn, id);
        if (pathExists(path)) {
            unLink(path);
        }
    }
    subsystems.clear();

    auto path = makePortPath(id);
    if (pathExists(path)) {
        rmDir(path);
    }
}

int NVMeoFListener::setup() {
    auto path = makePortPath(id);

    int rc = mkDir(path);
    if (rc != 0) {
        LOG(ERROR) << "Failed to create port " << std::to_string(id);
        return rc;
    }

    rc = setAttr(path / "addr_trtype", trtype);
    if (rc != 0) {
        LOG(ERROR) << "Failed to set trtype " << trtype << " for port "
                   << std::to_string(id);
        return rc;
    }

    rc = setAttr(path / "addr_adrfam", adrfam);
    if (rc != 0) {
        LOG(ERROR) << "Failed to set adrfam " << adrfam << " for port "
                   << std::to_string(id);
        return rc;
    }

    rc = setAttr(path / "addr_traddr", traddr);
    if (rc != 0) {
        LOG(ERROR) << "Failed to set traddr " << traddr << " for port "
                   << std::to_string(id);
        return rc;
    }

    rc = setAttr(path / "addr_trsvcid", trsvcid);
    if (rc != 0) {
        LOG(ERROR) << "Failed to set trsvcid " << trsvcid << " for port "
                   << std::to_string(id);
        return rc;
    }

    return 0;
}

int NVMeoFListener::addSubsystem(std::shared_ptr<NVMeoFSubsystem> subsys) {
    auto dest = makeSubsysPath(subsys->subnqn);
    auto name = makeSubsysLinkPath(subsys->subnqn, id);
    return symLink(dest, name);
}

int NVMeoFListener::removeSubsystem(std::shared_ptr<NVMeoFSubsystem> subsys) {
    auto name = makeSubsysLinkPath(subsys->subnqn, id);
    return unLink(name);
}
}  // namespace nvmeof_target

NVMeoFTarget::NVMeoFTarget(const std::string &hostname)
    : hostname(hostname), listener(nullptr), subsystem(nullptr) {}

NVMeoFTarget::~NVMeoFTarget() {
    if (listener != nullptr && subsystem != nullptr) {
        listener->removeSubsystem(subsystem);
    }
}

int NVMeoFTarget::setup(const std::string &trtype, const std::string &adrfam,
                        const std::string &traddr, const std::string &trsvcid) {
    listener = std::make_unique<nvmeof_target::NVMeoFListener>(trtype, adrfam,
                                                               traddr, trsvcid);
    int rc = listener->setup();
    if (rc != 0) {
        LOG(ERROR) << "Failed to setup nvmeof target listener, trtype="
                   << trtype << " adrfam=" << adrfam << " traddr=" << traddr
                   << " trsvcid=" << trsvcid << ", rc=" << rc;
        return rc;
    }

    auto subnqn = "nqn.2016-06.io.mc:" + hostname;
    subsystem = std::make_shared<nvmeof_target::NVMeoFSubsystem>(subnqn);
    rc = subsystem->setup();
    if (rc != 0) {
        LOG(ERROR) << "Failed to setup nvmeof subsystem, subnqn=" << subnqn
                   << ", rc=" << rc;
        return rc;
    }

    rc = listener->addSubsystem(subsystem);
    if (rc != 0) {
        LOG(ERROR) << "Failed to add subsystem " << subsystem->subnqn
                   << " to listener, rc=" << rc;
        return rc;
    }

    return 0;
}

int NVMeoFTarget::addFile(FileBufferID file_id, const std::string &file) {
    std::lock_guard<std::mutex> guard(this->mutex);
    return subsystem->addNamespace(file_id, file);
}

int NVMeoFTarget::removeFile(FileBufferID file_id) {
    std::lock_guard<std::mutex> guard(this->mutex);
    return subsystem->removeNamespace(file_id);
}
}  // namespace mooncake