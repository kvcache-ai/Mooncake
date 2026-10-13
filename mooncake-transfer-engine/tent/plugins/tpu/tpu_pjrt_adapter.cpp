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

// Mooncake's built-in TPU adapter. Implements TENT's adapter ABI
// (tpu_pjrt_abi.h) on top of the token registry, delegating the actual device
// copies to a TPU buffer provider (tpu_buffer_provider.h), e.g. tpu-sync.

// The library is built with hidden visibility; only the C entry points
// declared in these headers are exported.
#pragma GCC visibility push(default)
#include "tent/platform/tpu_pjrt_abi.h"
#include "tpu_pjrt_adapter.h"
#pragma GCC visibility pop

#include <dlfcn.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "tent/platform/tpu_buffer_provider.h"
#include "tpu_token_registry.h"

namespace mooncake {
namespace tent {
namespace {

// The bound provider. Written once under g_provider_mutex and never unbound:
// handles may outlive any attempt to unload it, so the library is never
// dlclose()d either.
std::mutex g_provider_mutex;
std::string g_provider_path;
mc_tpu_buffer_provider_t g_provider_table{};
const mc_tpu_buffer_provider_t *g_provider = nullptr;

const mc_tpu_buffer_provider_t *boundProvider() {
    std::lock_guard<std::mutex> lock(g_provider_mutex);
    return g_provider;
}

bool tableIsComplete(const mc_tpu_buffer_provider_t &t) {
    return t.size && t.device_index && t.numa_node && t.copy_to_host &&
           t.copy_from_host && t.release;
}

// A registry region backed by one provider handle. Owns the handle.
class ProviderRegion : public DeviceRegion {
   public:
    ProviderRegion(const mc_tpu_buffer_provider_t *provider,
                   mc_tpu_buffer_t buffer)
        : provider_(provider),
          buffer_(buffer),
          size_(provider->size(buffer)),
          device_(provider->device_index(buffer)),
          numa_(provider->numa_node(buffer)) {}

    ~ProviderRegion() override { provider_->release(buffer_); }

    size_t size() const override { return size_; }
    int deviceIndex() const override { return device_; }
    int numaNode() const override { return numa_; }

    bool copyToHost(void *host_dst, size_t offset, size_t len) override {
        if (provider_->copy_to_host(buffer_, offset, host_dst, len) == 0) {
            return true;
        }
        tpuAdapterLog(std::string(provider_->name) + ": D2H of " +
                      std::to_string(len) + " bytes at offset " +
                      std::to_string(offset) + " failed");
        return false;
    }

    bool copyFromHost(const void *host_src, size_t offset,
                      size_t len) override {
        if (provider_->copy_from_host(buffer_, offset, host_src, len) == 0) {
            return true;
        }
        tpuAdapterLog(std::string(provider_->name) + ": H2D of " +
                      std::to_string(len) + " bytes at offset " +
                      std::to_string(offset) + " failed");
        return false;
    }

   private:
    const mc_tpu_buffer_provider_t *provider_;
    mc_tpu_buffer_t buffer_;
    size_t size_;
    int device_;
    int numa_;
};

}  // namespace
}  // namespace tent
}  // namespace mooncake

using mooncake::tent::tpuAdapterLog;
using mooncake::tent::TpuTokenRegistry;

extern "C" {

// --- Control API (tpu_pjrt_adapter.h) --------------------------------------

int mc_tpu_adapter_load_provider(const char *path) {
    using namespace mooncake::tent;
    if (!path || path[0] == '\0') return 1;
    std::lock_guard<std::mutex> lock(g_provider_mutex);
    if (g_provider) {
        if (g_provider_path == path) return 0;
        tpuAdapterLog("a TPU buffer provider is already bound ('" +
                      g_provider_path + "'); refusing '" + path + "'");
        return 1;
    }

    void *handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        tpuAdapterLog(std::string("cannot load TPU buffer provider: ") +
                      dlerror());
        return 1;
    }
    auto get = reinterpret_cast<mc_tpu_buffer_provider_get_fn>(
        dlsym(handle, MC_TPU_BUFFER_PROVIDER_ENTRY));
    mc_tpu_buffer_provider_t table{};
    if (!get || get(MC_TPU_BUFFER_PROVIDER_ABI_VERSION, &table) != 0 ||
        table.abi_version != MC_TPU_BUFFER_PROVIDER_ABI_VERSION ||
        !tableIsComplete(table)) {
        tpuAdapterLog(std::string("'") + path +
                      "' is not a compatible TPU buffer provider (needs " +
                      MC_TPU_BUFFER_PROVIDER_ENTRY + " serving ABI version " +
                      std::to_string(MC_TPU_BUFFER_PROVIDER_ABI_VERSION) +
                      " with every function set)");
        dlclose(handle);
        return 1;
    }
    if (!table.name) table.name = "unnamed provider";
    g_provider_table = table;
    g_provider = &g_provider_table;
    g_provider_path = path;
    return 0;
}

int mc_tpu_adapter_register(uint64_t buffer, uint64_t *token_out,
                            uint64_t *size_out, int *device_out) {
    using namespace mooncake::tent;
    const mc_tpu_buffer_provider_t *provider = boundProvider();
    if (!provider) {
        tpuAdapterLog("no TPU buffer provider is bound");
        return 1;
    }
    if (buffer == 0) return 1;
    // Adopt the handle first so it is released on every failure path below.
    auto region = std::make_unique<ProviderRegion>(provider, buffer);
    const size_t size = region->size();
    const int device = region->deviceIndex();
    if (!token_out || !size_out || !device_out) return 1;
    const uintptr_t token = TpuTokenRegistry::instance().add(std::move(region));
    if (token == 0) return 1;
    *token_out = token;
    *size_out = size;
    *device_out = device;
    return 0;
}

int mc_tpu_adapter_unregister(uint64_t token) {
    return TpuTokenRegistry::instance().remove(static_cast<uintptr_t>(token))
               ? 0
               : 1;
}

int mc_tpu_adapter_release_buffer(uint64_t buffer) {
    const mc_tpu_buffer_provider_t *provider = mooncake::tent::boundProvider();
    if (!provider || buffer == 0) return 1;
    provider->release(buffer);
    return 0;
}

// --- TENT adapter ABI (tpu_pjrt_abi.h) -------------------------------------

// TENT calls this once, while constructing its TPU platform, and then reads
// the device topology. Devices are learned from registered buffers, so
// buffers must be registered before the TransferEngine is created. Failing
// here makes TENT disable TPU support loudly instead of running with an empty
// TPU topology.
int mc_tpu_pjrt_init(void) {
    if (TpuTokenRegistry::instance().deviceCount() == 0) {
        tpuAdapterLog(
            "no TPU buffers are registered. Register them (e.g. with "
            "mooncake.tpu.prepare) before creating the TransferEngine.");
        return 1;
    }
    return 0;
}

int mc_tpu_pjrt_is_device_ptr(const void *addr) {
    return TpuTokenRegistry::instance().contains(addr) ? 1 : 0;
}

int mc_tpu_pjrt_device_index(const void *addr) {
    return TpuTokenRegistry::instance().deviceIndex(addr);
}

int mc_tpu_pjrt_copy_d2h(void *host_dst, const void *device_src, size_t len) {
    return TpuTokenRegistry::instance().copyD2H(host_dst, device_src, len);
}

int mc_tpu_pjrt_copy_h2d(void *device_dst, const void *host_src, size_t len) {
    return TpuTokenRegistry::instance().copyH2D(device_dst, host_src, len);
}

int mc_tpu_pjrt_device_count(void) {
    return TpuTokenRegistry::instance().deviceCount();
}

int mc_tpu_pjrt_device_numa(int index) {
    return TpuTokenRegistry::instance().deviceNuma(index);
}

}  // extern "C"
