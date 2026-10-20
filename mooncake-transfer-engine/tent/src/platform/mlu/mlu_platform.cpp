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

#include "tent/platform/mlu.h"

#include <cnrt.h>
#include <cn_api.h>

#include <charconv>
#include <fstream>
#include <limits>
#include <string_view>

namespace mooncake::tent {
namespace {
Status checked(cnrtRet_t result) {
    if (result == cnrtSuccess) return Status::OK();
    return Status::InternalError(std::string("CNRT: ") +
                                 cnrtGetErrorStr(result));
}

Status checkedDriver(CNresult result) {
    if (result == CN_SUCCESS) return Status::OK();
    const char* text = nullptr;
    cnGetErrorString(result, &text);
    return Status::InternalError(std::string("CNDRV: ") +
                                 (text ? text : std::to_string(result)));
}

// Driver contexts are thread-local. Copies on engine workers must use the
// allocation's context, including memory allocated outside of this platform.
class ContextGuard {
   public:
    ContextGuard() : status_(checkedDriver(cnCtxGetCurrent(&previous_))) {}
    ~ContextGuard() {
        if (status_.ok()) {
            auto result = checkedDriver(cnCtxSetCurrent(previous_));
            if (!result.ok()) LOG(ERROR) << result.ToString();
        }
    }
    Status status() const { return status_; }
    Status use(void* ptr) {
        CHECK_STATUS(status_);
        CNcontext context = nullptr;
        CHECK_STATUS(
            checkedDriver(cnGetMemAttribute(&context, CN_MEM_ATTRIBUTE_CONTEXT,
                                            reinterpret_cast<CNaddr>(ptr))));
        return checkedDriver(cnCtxSetCurrent(context));
    }

   private:
    CNcontext previous_ = nullptr;
    Status status_;
};

Status attributes(void* ptr, cnrtPointerAttributes_t& attr) {
    if (!ptr) return Status::InvalidArgument("Null memory address");
    // Initialize the worker's runtime context through CNRT before using
    // driver queries, which can fail without a current context.
    return checked(cnrtPointerGetAttributes(&attr, ptr));
}

bool hostMemory(const cnrtPointerAttributes_t& attr) {
    return attr.type == cnrtMemTypeHost || attr.type == cnrtMemTypeUnregistered;
}

Status deviceRange(void* ptr, size_t length, bool require_base = false) {
    CNaddr base = 0;
    cn_uint64_t size = 0;
    auto address = reinterpret_cast<CNaddr>(ptr);
    CHECK_STATUS(checkedDriver(cnMemGetAddressRange(&base, &size, address)));
    if (address < base || address - base > size ||
        length > size - (address - base) || (require_base && address != base))
        return Status::InvalidArgument(
            "MLU memory range exceeds its allocation");
    return Status::OK();
}
}  // namespace

Status MluPlatform::probe(std::vector<Topology::NicEntry>& nics,
                          std::vector<Topology::MemEntry>& mems) {
    CHECK_STATUS(CpuPlatform::probe(nics, mems));
    unsigned int count = 0;
    CHECK_STATUS(checked(cnrtGetDeviceCount(&count)));
    for (unsigned int device = 0; device < count; ++device) {
        Topology::MemEntry entry;
        entry.name = "mlu:" + std::to_string(device);
        entry.type = Topology::MEM_MLU;
        entry.numa_node = -1;
        char pci[32] = {};
        CHECK_STATUS(checked(cnrtDeviceGetPCIBusId(pci, sizeof(pci), device)));
        entry.pci_bus_id = pci;
        std::ifstream numa_file(std::string("/sys/bus/pci/devices/") + pci +
                                "/numa_node");
        numa_file >> entry.numa_node;
        for (size_t nic = 0; nic < nics.size(); ++nic) {
            auto rank =
                entry.numa_node >= 0 && entry.numa_node == nics[nic].numa_node
                    ? 0
                    : 2;
            entry.device_list[rank].push_back(nic);
        }
        mems.push_back(std::move(entry));
    }
    return Status::OK();
}

Status MluPlatform::allocate(void** ptr, size_t size, MemoryOptions& options) {
    if (!ptr || !size) return Status::InvalidArgument("Empty allocation");
    *ptr = nullptr;
    if (options.location == "mlu" || options.location.rfind("mlu:", 0) == 0) {
        int device = 0;
        if (options.location != "mlu") {
            std::string_view ordinal(options.location.data() + 4,
                                     options.location.size() - 4);
            auto [end, error] = std::from_chars(
                ordinal.data(), ordinal.data() + ordinal.size(), device);
            if (error != std::errc() ||
                end != ordinal.data() + ordinal.size() || device < 0)
                return Status::InvalidArgument("Invalid MLU location: " +
                                               options.location);
        }
        unsigned int count = 0;
        CHECK_STATUS(checked(cnrtGetDeviceCount(&count)));
        if (static_cast<unsigned int>(device) >= count)
            return Status::InvalidArgument("MLU device ordinal out of range");
        ContextGuard guard;
        CHECK_STATUS(guard.status());
        CHECK_STATUS(checked(cnrtSetDevice(device)));
        return checked(cnrtMalloc(ptr, size));
    }
    if (options.location != kWildcardLocation && options.location != "cpu" &&
        LocationParser(options.location).type() != "cpu")
        return Status::InvalidArgument("Unsupported memory location: " +
                                       options.location);
    return CpuPlatform::allocate(ptr, size, options);
}

Status MluPlatform::free(void* ptr, size_t size) {
    cnrtPointerAttributes_t attr{};
    CHECK_STATUS(attributes(ptr, attr));
    if (attr.type == cnrtMemTypeDevice) {
        ContextGuard guard;
        CHECK_STATUS(guard.use(ptr));
        CHECK_STATUS(deviceRange(ptr, size, true));
        return checked(cnrtFree(ptr));
    }
    if (!hostMemory(attr))
        return Status::InvalidArgument("Unknown memory type");
    return CpuPlatform::free(ptr, size);
}

Status MluPlatform::copy(void* dst, void* src, size_t length) {
    if (!length) return Status::OK();
    cnrtPointerAttributes_t source{}, target{};
    CHECK_STATUS(attributes(src, source));
    CHECK_STATUS(attributes(dst, target));
    bool source_device = source.type == cnrtMemTypeDevice;
    bool target_device = target.type == cnrtMemTypeDevice;
    if (source_device && target_device)
        return Status::NotImplemented(
            "MLU device-to-device copies require host staging");
    if ((!source_device && !hostMemory(source)) ||
        (!target_device && !hostMemory(target)))
        return Status::InvalidArgument("Unknown memory type");
    if (!source_device && !target_device)
        return CpuPlatform::copy(dst, src, length);
    void* device = source_device ? src : dst;
    ContextGuard guard;
    CHECK_STATUS(guard.use(device));
    CHECK_STATUS(deviceRange(device, length));
    // These driver calls are synchronous: completion can be published only
    // after the device/host copy has finished.
    return checkedDriver(
        source_device
            ? cnMemcpyDtoH(dst, reinterpret_cast<CNaddr>(src), length)
            : cnMemcpyHtoD(reinterpret_cast<CNaddr>(dst), src, length));
}

MemoryType MluPlatform::getMemoryType(void* addr) {
    cnrtPointerAttributes_t attr{};
    if (!attributes(addr, attr).ok()) return MTYPE_UNKNOWN;
    if (attr.type == cnrtMemTypeDevice) return MTYPE_MLU;
    return hostMemory(attr) ? MTYPE_CPU : MTYPE_UNKNOWN;
}

const std::vector<RangeLocation> MluPlatform::getLocation(void* start,
                                                          size_t len,
                                                          bool skip_prefault) {
    if (!len || len > std::numeric_limits<uintptr_t>::max() -
                          reinterpret_cast<uintptr_t>(start))
        return {};
    cnrtPointerAttributes_t attr{};
    if (!attributes(start, attr).ok()) return {};
    if (attr.type == cnrtMemTypeDevice) {
        ContextGuard guard;
        if (!guard.use(start).ok() || !deviceRange(start, len).ok()) return {};
        return {{reinterpret_cast<uint64_t>(start), len,
                 "mlu:" + std::to_string(attr.device)}};
    }
    if (!hostMemory(attr)) return {};
    return CpuPlatform::getLocation(start, len, skip_prefault);
}

Status MluPlatform::synchronizeDevices(const Topology* topology) {
    ContextGuard guard;
    CHECK_STATUS(guard.status());
    for (int device : topologyDeviceIndices(topology, Topology::MEM_MLU)) {
        CHECK_STATUS(checked(cnrtSetDevice(device)));
        CHECK_STATUS(checked(cnrtSyncDevice()));
    }
    return Status::OK();
}
}  // namespace mooncake::tent
