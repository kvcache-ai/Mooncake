#include "tent/runtime/platform.h"
#include "tent/transport/rdma/ibv_loader.h"

#include <algorithm>
#include <cstdint>
#include <limits>

extern "C" int mooncake_tent_platform_free_memory(void* addr, size_t size) {
    if (addr == nullptr) {
        return -1;
    }
    auto status = mooncake::tent::Platform::getLoader().free(addr, size);
    return status.ok() ? 0 : -1;
}

extern "C" uint64_t mooncake_tent_probe_rdma_max_mr_size() {
    auto& loader = mooncake::tent::IbvLoader::Instance();
    if (!loader.available()) {
        return 0;
    }

    const auto& verbs = loader.sym();
    int device_count = 0;
    ibv_device** devices = verbs.ibv_get_device_list(&device_count);
    if (devices == nullptr || device_count <= 0) {
        if (devices != nullptr) {
            verbs.ibv_free_device_list(devices);
        }
        return 0;
    }

    uint64_t limit = std::numeric_limits<uint64_t>::max();
    bool found = false;
    for (int index = 0; index < device_count; ++index) {
        ibv_context* context = verbs.ibv_open_device(devices[index]);
        if (context == nullptr) {
            continue;
        }
        ibv_device_attr attr = {};
        if (verbs.ibv_query_device(context, &attr) == 0 && attr.max_mr_size > 0) {
            limit = std::min(limit, static_cast<uint64_t>(attr.max_mr_size));
            found = true;
        }
        verbs.ibv_close_device(context);
    }
    verbs.ibv_free_device_list(devices);
    return found ? limit : 0;
}
