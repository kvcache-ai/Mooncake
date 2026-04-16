#include "tent/runtime/platform.h"

extern "C" int mooncake_tent_platform_free_memory(void* addr, size_t size) {
    if (addr == nullptr) {
        return -1;
    }
    auto status = mooncake::tent::Platform::getLoader().free(addr, size);
    return status.ok() ? 0 : -1;
}
