#include "memory_alloc.h"

#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif

void *hugepage_memory_alloc(size_t size) {
#ifndef USE_NOF
    return nullptr;
#else
    return mooncake::SpdkWrapper::GetInstance().Alloc(size, 0x1000, -1);
#endif
}

void hugepage_memory_free(void *ptr) {
#ifdef USE_NOF
    mooncake::SpdkWrapper::GetInstance().Free(ptr);
#endif
}
