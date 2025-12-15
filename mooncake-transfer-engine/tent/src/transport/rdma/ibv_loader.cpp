// Copyright 2025 KVCache.AI
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

#include "tent/transport/rdma/ibv_loader.h"

#include <dlfcn.h>
#include <glog/logging.h>

namespace mooncake {
namespace tent {

template <typename Fn>
bool LoadSymbol(void* handle, const char* name, Fn& out) {
    void* sym = dlsym(handle, name);
    if (!sym) {
        LOG(WARNING) << "libibverbs missing required symbol: " << name
                     << ", error: " << dlerror();
        return false;
    }
    out = reinterpret_cast<Fn>(sym);
    return true;
}

IbvLoader& IbvLoader::Instance() {
    static IbvLoader instance;
    return instance;
}

IbvLoader::IbvLoader() {
    handle_ = dlopen("libibverbs.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!handle_) {
        LOG(INFO) << "IbvLoader: libibverbs.so.1 not available: " << dlerror();
        return;
    }

    bool ok = true;
    ok &= LoadSymbol(handle_, "ibv_get_device_list",
                     symbols_.ibv_get_device_list);
    ok &= LoadSymbol(handle_, "ibv_free_device_list",
                     symbols_.ibv_free_device_list);
    ok &= LoadSymbol(handle_, "ibv_open_device", symbols_.ibv_open_device);
    ok &= LoadSymbol(handle_, "ibv_close_device", symbols_.ibv_close_device);
    ok &= LoadSymbol(handle_, "ibv_query_device", symbols_.ibv_query_device);
    ok &= LoadSymbol(handle_, "ibv_query_gid", symbols_.ibv_query_gid);
    ok &=
        LoadSymbol(handle_, "ibv_query_port", symbols_.ibv_query_port_default);
    ok &= LoadSymbol(handle_, "ibv_get_device_name",
                     symbols_.ibv_get_device_name);

    ok &= LoadSymbol(handle_, "ibv_alloc_pd", symbols_.ibv_alloc_pd);
    ok &= LoadSymbol(handle_, "ibv_dealloc_pd", symbols_.ibv_dealloc_pd);

    ok &= LoadSymbol(handle_, "ibv_create_comp_channel",
                     symbols_.ibv_create_comp_channel);
    ok &= LoadSymbol(handle_, "ibv_destroy_comp_channel",
                     symbols_.ibv_destroy_comp_channel);

    ok &= LoadSymbol(handle_, "ibv_create_cq", symbols_.ibv_create_cq);
    ok &= LoadSymbol(handle_, "ibv_destroy_cq", symbols_.ibv_destroy_cq);

    ok &= LoadSymbol(handle_, "ibv_create_qp", symbols_.ibv_create_qp);
    ok &= LoadSymbol(handle_, "ibv_destroy_qp", symbols_.ibv_destroy_qp);
    ok &= LoadSymbol(handle_, "ibv_modify_qp", symbols_.ibv_modify_qp);

    ok &= LoadSymbol(handle_, "ibv_get_cq_event", symbols_.ibv_get_cq_event);
    ok &= LoadSymbol(handle_, "ibv_ack_cq_events", symbols_.ibv_ack_cq_events);

    ok &= LoadSymbol(handle_, "ibv_reg_mr", symbols_.ibv_reg_mr_default);
    ok &= LoadSymbol(handle_, "ibv_reg_mr_iova2", symbols_.ibv_reg_mr_iova2);
    ok &= LoadSymbol(handle_, "ibv_dereg_mr", symbols_.ibv_dereg_mr);

    ok &= LoadSymbol(handle_, "ibv_fork_init", symbols_.ibv_fork_init);

    if (!ok) {
        LOG(WARNING) << "IbvLoader: missing required verbs symbols, "
                     << "RDMA will be disabled.";
        dlclose(handle_);
        handle_ = nullptr;
        return;
    }

    int num_devices = 0;
    ibv_device** dev_list = symbols_.ibv_get_device_list(&num_devices);
    if (!dev_list || num_devices <= 0) {
        if (dev_list) {
            symbols_.ibv_free_device_list(dev_list);
        }
        LOG(INFO) << "IbvLoader: no RDMA devices found, "
                     "RDMA will be unavailable.";
        dlclose(handle_);
        handle_ = nullptr;
        return;
    }

    symbols_.ibv_free_device_list(dev_list);
    available_ = true;
    LOG(INFO) << "IbvLoader: libibverbs loaded successfully, devices="
              << num_devices;
}

IbvLoader::~IbvLoader() {
    if (handle_) {
        dlclose(handle_);
        handle_ = nullptr;
    }
}
}  // namespace tent
}  // namespace mooncake