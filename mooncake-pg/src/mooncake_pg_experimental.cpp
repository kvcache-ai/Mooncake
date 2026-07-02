#include "mooncake_pg_experimental.h"

#include <algorithm>
#include <cstdlib>

#include "mooncake_worker.cuh"

namespace mooncake {

namespace {

bool envEnabled(const char* name) {
    const char* value = std::getenv(name);
    return value && value[0] != '\0' && value[0] != '0';
}

}  // namespace

bool useDeviceApiCollectivesPoc() {
    return envEnabled("MOONCAKE_PG_DEVICE_API_COLLECTIVES");
}

bool deviceCollectiveRuntimeDebugEnabled() {
    return envEnabled("MOONCAKE_PG_DEVICE_RUNTIME_DEBUG");
}

uint32_t directP2pDeviceSequenceSlotCapacity() {
    const char* value =
        std::getenv("MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS");
    if (!value || value[0] == '\0') return 1u << 20;
    char* end = nullptr;
    unsigned long parsed = std::strtoul(value, &end, 10);
    if (end == value || parsed < 128) return 128;
    if (parsed > 1u << 20) return 1u << 20;
    return static_cast<uint32_t>(parsed);
}

size_t directP2pEffectiveBufferSize() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_BUFFER_MB");
    if (!value || value[0] == '\0') return kBufferSize;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) return kBufferSize;
    const size_t mb = static_cast<size_t>(parsed);
    const size_t max_mb = kBufferSize >> 20;
    const size_t clamped_mb = std::min(std::max<size_t>(mb, 16), max_mb);
    return clamped_mb << 20;
}

size_t directP2pSlotStride(size_t effectiveBufferSize, size_t signalBytes,
                           int slots, int activeSize) {
    if (slots <= 0 || activeSize <= 0 || effectiveBufferSize <= signalBytes) {
        return 0;
    }
    return (((effectiveBufferSize - signalBytes) / static_cast<size_t>(slots) /
             static_cast<size_t>(activeSize)) &
            ~static_cast<size_t>(7));
}

size_t directP2pSlottedControlBytes(int slots, bool deviceApiCollective) {
    const size_t signalBytes =
        static_cast<size_t>(slots) * kMaxNumRanks * sizeof(uint32_t);
    return signalBytes * (deviceApiCollective ? 3 : 2);
}

}  // namespace mooncake
