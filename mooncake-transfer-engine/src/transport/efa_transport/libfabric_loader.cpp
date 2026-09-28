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

#include "transport/efa_transport/efa_fabric.h"

#include <dlfcn.h>
#include <glog/logging.h>

#include <cstdlib>
#include <mutex>
#include <vector>

namespace {

using GetinfoFn = int (*)(uint32_t, const char*, const char*, uint64_t,
                          const struct fi_info*, struct fi_info**);
using FreeinfoFn = void (*)(struct fi_info*);
using DupinfoFn = struct fi_info* (*)(const struct fi_info*);
using FabricFn = int (*)(struct fi_fabric_attr*, struct fid_fabric**, void*);
using StrerrorFn = const char* (*)(int);
using VersionFn = uint32_t (*)();

struct Libfabric {
    GetinfoFn getinfo = nullptr;
    FreeinfoFn freeinfo = nullptr;
    DupinfoFn dupinfo = nullptr;
    FabricFn fabric = nullptr;
    StrerrorFn strerror = nullptr;
    VersionFn version = nullptr;
};

Libfabric g_fabric;
std::once_flag g_load_once;
bool g_loaded = false;
std::string g_load_error;

std::string versionString(uint32_t v) {
    return std::to_string(FI_MAJOR(v)) + "." + std::to_string(FI_MINOR(v));
}

// MC_LIBFABRIC_PATH overrides the search. Otherwise use the regular loader
// search (LD_LIBRARY_PATH, ld.so.cache), then the aws-efa-installer location.
std::vector<std::string> candidatePaths() {
    if (const char* path = std::getenv("MC_LIBFABRIC_PATH")) {
        return {path};
    }
    return {"libfabric.so.1", "/opt/amazon/efa/lib/libfabric.so.1"};
}

template <typename Fn>
bool bind(void* handle, const char* name, Fn* out, std::string* error) {
    *out = reinterpret_cast<Fn>(dlsym(handle, name));
    if (*out == nullptr) {
        *error = std::string("missing symbol ") + name;
        return false;
    }
    return true;
}

void load() {
    std::string tried;
    void* handle = nullptr;
    std::string path;
    for (const auto& candidate : candidatePaths()) {
        handle = dlopen(candidate.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle) {
            path = candidate;
            break;
        }
        tried += "\n  " + candidate + ": " + dlerror();
    }
    if (!handle) {
        g_load_error =
            "cannot load libfabric. Install the AWS EFA userspace and put its "
            "lib directory (e.g. /opt/amazon/efa/lib) on LD_LIBRARY_PATH, or "
            "set MC_LIBFABRIC_PATH. Tried:" +
            tried;
        return;
    }

    Libfabric f;
    std::string error;
    if (!bind(handle, "fi_getinfo", &f.getinfo, &error) ||
        !bind(handle, "fi_freeinfo", &f.freeinfo, &error) ||
        !bind(handle, "fi_dupinfo", &f.dupinfo, &error) ||
        !bind(handle, "fi_fabric", &f.fabric, &error) ||
        !bind(handle, "fi_strerror", &f.strerror, &error) ||
        !bind(handle, "fi_version", &f.version, &error)) {
        g_load_error = path + ": " + error;
        dlclose(handle);
        return;
    }

    uint32_t version = f.version();
    if (FI_VERSION_LT(version, mooncake::kEfaRequiredFabricVersion)) {
        g_load_error =
            path + " is libfabric " + versionString(version) +
            ", but the EFA transport needs " +
            versionString(mooncake::kEfaRequiredFabricVersion) +
            " or newer. Put the AWS EFA libfabric (e.g. /opt/amazon/efa/lib) "
            "first on LD_LIBRARY_PATH, or set MC_LIBFABRIC_PATH.";
        dlclose(handle);
        return;
    }

    // Keep the handle open for the lifetime of the process.
    g_fabric = f;
    g_loaded = true;
    LOG(INFO) << "EFA transport: loaded libfabric " << versionString(version)
              << " from " << path;
}

}  // namespace

namespace mooncake {

bool loadLibfabric(std::string* error) {
    std::call_once(g_load_once, load);
    if (!g_loaded && error) *error = g_load_error;
    return g_loaded;
}

}  // namespace mooncake

// Forwarders for the renamed entry points declared by <rdma/*.h> through
// efa_fabric.h. The headers wrap their declarations in extern "C".
extern "C" {

int mc_fi_getinfo(uint32_t version, const char* node, const char* service,
                  uint64_t flags, const struct fi_info* hints,
                  struct fi_info** info) {
    return g_fabric.getinfo(version, node, service, flags, hints, info);
}

void mc_fi_freeinfo(struct fi_info* info) { g_fabric.freeinfo(info); }

struct fi_info* mc_fi_dupinfo(const struct fi_info* info) {
    return g_fabric.dupinfo(info);
}

int mc_fi_fabric(struct fi_fabric_attr* attr, struct fid_fabric** fabric,
                 void* context) {
    return g_fabric.fabric(attr, fabric, context);
}

const char* mc_fi_strerror(int errnum) {
    // Error paths may format errors before libfabric was loaded.
    return g_fabric.strerror ? g_fabric.strerror(errnum)
                             : "libfabric not loaded";
}

uint32_t mc_fi_version(void) {
    return g_fabric.version ? g_fabric.version() : 0;
}

}  // extern "C"
