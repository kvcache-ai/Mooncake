#include "nof/nof_runtime.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>

#ifdef USE_NOF
#include <glog/logging.h>
#endif
#ifdef USE_NOF_SPDK
#include "nof/spdk_initiator.h"
#endif
#ifdef USE_NOF_LINUX
#include "nof/linux_initiator.h"
#endif

namespace mooncake {
namespace {
#ifdef USE_NOF
// MC_NOF_BACKEND selects both the initiator and its matching allocator
// to satisfy the backend's buffer alignment and registration requirements.
enum class NofBackend { kSpdk, kLinux, kNone };

NofBackend ResolveNofBackend() {
#ifdef USE_NOF_SPDK
    constexpr auto default_backend = NofBackend::kSpdk;
    constexpr const char* default_name = "spdk";
#else
    constexpr auto default_backend = NofBackend::kLinux;
    constexpr const char* default_name = "linux";
#endif
    const char* raw = std::getenv("MC_NOF_BACKEND");
    if (!raw || *raw == '\0') {
        return default_backend;
    }
    std::string v(raw);
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (v == "none") {
        LOG(INFO) << "MC_NOF_BACKEND=none: NoF disabled at runtime";
        return NofBackend::kNone;
    }
    if (v == "spdk") return NofBackend::kSpdk;
    if (v == "linux") return NofBackend::kLinux;
    LOG(WARNING) << "Unrecognized MC_NOF_BACKEND=\"" << raw
                 << "\"; falling back to \"" << default_name << "\"";
    return default_backend;
}
#endif

#ifdef USE_NOF_LINUX
// Use at least 4 KiB alignment for 512/4096-byte logical blocks,
// including older kernels that require block-aligned buffers.
class LinuxBufferAllocator final : public DmaBufferAllocator {
   public:
    void* Alloc(size_t size, size_t align, int) override {
        void* buffer = nullptr;
        return posix_memalign(&buffer, std::max<size_t>(4096, align), size) == 0
                   ? buffer
                   : nullptr;
    }
    void Free(void* buffer) override { free(buffer); }
};
#endif
}  // namespace

NofRuntime CreateNofRuntime() {
#ifdef USE_NOF
    const auto backend = ResolveNofBackend();
#ifdef USE_NOF_SPDK
    if (backend == NofBackend::kSpdk)
        return {std::make_shared<SpdkInitiator>(),
                std::make_shared<SpdkDmaAllocator>()};
#endif
#ifdef USE_NOF_LINUX
    if (backend == NofBackend::kLinux)
        return {std::make_shared<LinuxNvmeInitiator>(),
                std::make_shared<LinuxBufferAllocator>()};
#endif
    if (backend != NofBackend::kNone) {
        LOG(ERROR) << "Selected NoF backend is not built; NoF is unavailable";
    }
#endif
    return {nullptr, std::make_shared<SystemDmaAllocator>()};
}

std::shared_ptr<DmaBufferAllocator> CreateDefaultDmaAllocator() {
#ifdef USE_NOF
    const auto backend = ResolveNofBackend();
#ifdef USE_NOF_SPDK
    if (backend == NofBackend::kSpdk)
        return std::make_shared<SpdkDmaAllocator>();
#endif
#ifdef USE_NOF_LINUX
    if (backend == NofBackend::kLinux)
        return std::make_shared<LinuxBufferAllocator>();
#endif
#endif
    return nullptr;
}
}  // namespace mooncake
