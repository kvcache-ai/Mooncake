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

#ifndef NVMEOF_GENERIC_TARGET_H_
#define NVMEOF_GENERIC_TARGET_H_

#include <memory>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <mutex>

namespace mooncake {
using FileBufferID = uint32_t;
using NamespaceID = FileBufferID;

namespace nvmeof_target {
class NVMeoFNamespace {
   public:
    const std::string subnqn;
    const NamespaceID nsid;
    const std::string file;

    NVMeoFNamespace(const std::string &subnqn, NamespaceID nsid,
                    const std::string &file);
    ~NVMeoFNamespace();

    int setup();
};

class NVMeoFSubsystem {
   public:
    const std::string subnqn;

    NVMeoFSubsystem(const std::string &subnqn);
    ~NVMeoFSubsystem();

    int setup();

    int addNamespace(NamespaceID nsid, const std::string &file);

    int removeNamespace(NamespaceID nsid);

   private:
    std::unordered_map<NamespaceID, std::unique_ptr<NVMeoFNamespace>>
        namespaces;
};

class NVMeoFListener {
   public:
    const std::string trtype;
    const std::string adrfam;
    const std::string traddr;
    const std::string trsvcid;

    NVMeoFListener(const std::string &trtype, const std::string &adrfam,
                   const std::string &traddr, const std::string &trsvcid);
    ~NVMeoFListener();

    int setup();

    int addSubsystem(std::shared_ptr<NVMeoFSubsystem> subsys);

    int removeSubsystem(std::shared_ptr<NVMeoFSubsystem> subsys);

   private:
    static std::atomic<uint32_t> next_id;

    const unsigned int id;
    std::vector<std::shared_ptr<NVMeoFSubsystem>> subsystems;
};
}  // namespace nvmeof_target

class NVMeoFTarget {
   public:
    NVMeoFTarget(const std::string &hostname);
    ~NVMeoFTarget();

    int setup(const std::string &trtype, const std::string &adrfam,
              const std::string &traddr, const std::string &trsvcid);

    int addFile(FileBufferID file_id, const std::string &file);

    int removeFile(FileBufferID file_id);

    const std::string &getSubNQN() { return subsystem->subnqn; }

   private:
    const std::string hostname;

    std::mutex mutex;
    std::unique_ptr<nvmeof_target::NVMeoFListener> listener;
    std::shared_ptr<nvmeof_target::NVMeoFSubsystem> subsystem;
};

}  // namespace mooncake

#endif