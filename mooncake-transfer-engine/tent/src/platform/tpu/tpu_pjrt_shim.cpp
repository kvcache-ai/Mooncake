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

#include "tent/platform/tpu_pjrt_shim.h"

#include <dlfcn.h>
#include <glog/logging.h>

#include <cstdlib>

namespace mooncake {
namespace tent {

namespace {
constexpr const char kDefaultAdapterLib[] = "libmooncake_tpu_pjrt.so";

const char *adapterLibraryPath() {
    const char *env = std::getenv("MC_TPU_PJRT_LIB");
    if (env && env[0] != '\0') return env;
    return kDefaultAdapterLib;
}
}  // namespace

TpuPjrtShim &TpuPjrtShim::instance() {
    static TpuPjrtShim g_instance;
    return g_instance;
}

TpuPjrtShim::TpuPjrtShim() { load(); }

TpuPjrtShim::~TpuPjrtShim() {
    if (handle_) dlclose(handle_);
}

void TpuPjrtShim::load() {
    const char *lib = adapterLibraryPath();
    handle_ = dlopen(lib, RTLD_NOW | RTLD_LOCAL);
    if (!handle_) {
        LOG(WARNING) << "TpuPjrtShim: unable to load TPU PJRT adapter '" << lib
                     << "': " << dlerror()
                     << ". TPU transfers will be unavailable. Set "
                        "MC_TPU_PJRT_LIB to override the adapter path.";
        available_ = false;
        return;
    }

    // Resolve every entrypoint; treat any missing symbol as a fatal load error
    // so we never partially bind an incompatible adapter.
    auto resolve = [&](const char *name) -> void * {
        void *sym = dlsym(handle_, name);
        if (!sym)
            LOG(WARNING) << "TpuPjrtShim: adapter '" << lib
                         << "' is missing symbol '" << name << "'";
        return sym;
    };

    fn_init_ = reinterpret_cast<int (*)()>(resolve("mc_tpu_pjrt_init"));
    fn_is_device_ptr_ = reinterpret_cast<int (*)(const void *)>(
        resolve("mc_tpu_pjrt_is_device_ptr"));
    fn_device_index_ = reinterpret_cast<int (*)(const void *)>(
        resolve("mc_tpu_pjrt_device_index"));
    fn_copy_d2h_ = reinterpret_cast<int (*)(void *, const void *, size_t)>(
        resolve("mc_tpu_pjrt_copy_d2h"));
    fn_copy_h2d_ = reinterpret_cast<int (*)(void *, const void *, size_t)>(
        resolve("mc_tpu_pjrt_copy_h2d"));
    fn_device_count_ =
        reinterpret_cast<int (*)()>(resolve("mc_tpu_pjrt_device_count"));
    fn_device_numa_ =
        reinterpret_cast<int (*)(int)>(resolve("mc_tpu_pjrt_device_numa"));

    if (!fn_init_ || !fn_is_device_ptr_ || !fn_device_index_ || !fn_copy_d2h_ ||
        !fn_copy_h2d_ || !fn_device_count_ || !fn_device_numa_) {
        LOG(ERROR) << "TpuPjrtShim: adapter '" << lib
                   << "' does not satisfy the required ABI; disabling TPU.";
        dlclose(handle_);
        handle_ = nullptr;
        available_ = false;
        return;
    }

    if (fn_init_() != 0) {
        LOG(ERROR) << "TpuPjrtShim: mc_tpu_pjrt_init() failed; disabling TPU.";
        dlclose(handle_);
        handle_ = nullptr;
        available_ = false;
        return;
    }

    available_ = true;
    LOG(INFO) << "TpuPjrtShim: TPU PJRT adapter '" << lib << "' loaded ("
              << fn_device_count_() << " device(s)).";
}

bool TpuPjrtShim::isDevicePtr(const void *addr) const {
    if (!available_ || !addr) return false;
    return fn_is_device_ptr_(addr) != 0;
}

int TpuPjrtShim::deviceIndex(const void *addr) const {
    if (!available_ || !addr) return -1;
    return fn_device_index_(addr);
}

Status TpuPjrtShim::copyD2H(void *host_dst, const void *device_src,
                            size_t length) const {
    if (!available_)
        return Status::NotImplemented("TPU PJRT adapter unavailable" LOC_MARK);
    if (fn_copy_d2h_(host_dst, device_src, length) != 0)
        return Status::InternalError("TPU device->host copy failed" LOC_MARK);
    return Status::OK();
}

Status TpuPjrtShim::copyH2D(void *device_dst, const void *host_src,
                            size_t length) const {
    if (!available_)
        return Status::NotImplemented("TPU PJRT adapter unavailable" LOC_MARK);
    if (fn_copy_h2d_(device_dst, host_src, length) != 0)
        return Status::InternalError("TPU host->device copy failed" LOC_MARK);
    return Status::OK();
}

int TpuPjrtShim::deviceCount() const {
    if (!available_) return 0;
    return fn_device_count_();
}

int TpuPjrtShim::deviceNumaNode(int index) const {
    if (!available_) return -1;
    return fn_device_numa_(index);
}

}  // namespace tent
}  // namespace mooncake
