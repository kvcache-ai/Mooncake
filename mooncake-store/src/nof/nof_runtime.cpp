#include "nof/nof_runtime.h"

#ifdef USE_NOF
#include "nof/spdk_initiator.h"
#endif

namespace mooncake {

NofRuntime CreateNofRuntime() {
#ifdef USE_NOF
    return NofRuntime{std::make_shared<SpdkInitiator>(),
                      std::make_shared<SpdkDmaAllocator>()};
#else
    return NofRuntime{nullptr, std::make_shared<SystemDmaAllocator>()};
#endif
}

std::shared_ptr<DmaBufferAllocator> CreateDefaultDmaAllocator() {
#ifdef USE_NOF
    return std::make_shared<SpdkDmaAllocator>();
#else
    return nullptr;  // 保持 hugepage_memory_alloc 的历史行为
#endif
}

}  // namespace mooncake
