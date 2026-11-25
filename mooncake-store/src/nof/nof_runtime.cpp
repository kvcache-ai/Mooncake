#include "nof/nof_runtime.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>

#ifdef USE_NOF
#include <glog/logging.h>

#include "nof/spdk_initiator.h"
#endif

namespace mooncake {

#ifdef USE_NOF
namespace {

// Backend selector (MC_NOF_BACKEND, case-insensitive). One knob selects a
// *paired* Initiator+Allocator — a mismatched pair would silently violate
// the SPDK initiator's alignment/registration assumptions.
//   spdk (default; unset/empty) -> SpdkInitiator + SpdkDmaAllocator
//   none                        -> runtime-disable NoF: {nullptr, System}
// Unrecognized values fall back to spdk with a warning: a typo that silently
// disables NoF turns into write failures, which is worse.
enum class NofBackend { kSpdk, kNone };

NofBackend ResolveNofBackend() {
    const char* raw = std::getenv("MC_NOF_BACKEND");
    if (!raw || *raw == '\0') {
        return NofBackend::kSpdk;  // unset/empty -> default
    }
    std::string v(raw);
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (v == "none") {
        // INFO, not WARNING: an explicit operator choice, but logged so a
        // forgotten inherited env var does not silently disable NoF.
        LOG(INFO) << "MC_NOF_BACKEND=none: NoF disabled at runtime";
        return NofBackend::kNone;
    }
    if (v != "spdk") {
        LOG(WARNING) << "Unrecognized MC_NOF_BACKEND=\"" << raw
                     << "\"; falling back to \"spdk\"";
    }
    return NofBackend::kSpdk;
}

}  // namespace
#endif  // USE_NOF

NofRuntime CreateNofRuntime() {
#ifdef USE_NOF
    if (ResolveNofBackend() == NofBackend::kNone) {
        // Runtime-disable: nullptr initiator is the uniform "NoF unavailable"
        // signal. dma_allocator stays non-null (SystemDmaAllocator) to honor
        // the NofRuntime invariant; callers gate on initiator, so the system
        // allocator never actually reaches ClientBufferAllocator.
        return NofRuntime{nullptr, std::make_shared<SystemDmaAllocator>()};
    }
    return NofRuntime{std::make_shared<SpdkInitiator>(),
                      std::make_shared<SpdkDmaAllocator>()};
#else
    // Non-USE_NOF builds ignore MC_NOF_BACKEND (a backend that isn't compiled
    // in cannot be selected).
    return NofRuntime{nullptr, std::make_shared<SystemDmaAllocator>()};
#endif
}

std::shared_ptr<DmaBufferAllocator> CreateDefaultDmaAllocator() {
#ifdef USE_NOF
    if (ResolveNofBackend() == NofBackend::kNone) {
        // Honor "none": return nullptr, matching the historical
        // "NoF unavailable" signal.
        return nullptr;
    }
    return std::make_shared<SpdkDmaAllocator>();
#else
    return nullptr;  // preserve hugepage_memory_alloc's historical behavior
#endif
}

}  // namespace mooncake
