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

bool envEnabledDefaultOn(const char* name, bool default_on) {
    const char* value = std::getenv(name);
    if (!value || value[0] == '\0') return default_on;
    return value[0] != '0';
}

}  // namespace

bool useDeviceApiCollectivesPoc() {
    return envEnabled("MOONCAKE_PG_DEVICE_API_COLLECTIVES");
}

bool deviceCollectiveRuntimeDebugEnabled() {
    return envEnabled("MOONCAKE_PG_DEVICE_RUNTIME_DEBUG");
}

bool useDeviceApiHierarchicalAllReducePoc() {
    if (!useDeviceApiCollectivesPoc()) return false;
    return envEnabledDefaultOn("MOONCAKE_PG_DEVICE_API_HIER_AR", true);
}

bool useDirectP2pAllgatherPoc() {
    return envEnabledDefaultOn("MOONCAKE_PG_DIRECT_P2P_AG",
                               useDeviceApiCollectivesPoc());
}

bool useDirectP2pReduceScatterPoc() {
    return envEnabledDefaultOn("MOONCAKE_PG_DIRECT_P2P_RS",
                               useDeviceApiCollectivesPoc());
}

bool useDirectP2pAllReducePoc() {
    return envEnabledDefaultOn("MOONCAKE_PG_DIRECT_P2P_AR",
                               useDeviceApiCollectivesPoc());
}

bool useDirectP2pAllReduceRingPoc() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_AR_RING");
}

bool useDirectP2pAllReduceFusedRsAgPoc() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_AR_FUSED_RSAG");
}

bool useDirectP2pAllReduceAllgatherSkipSelfPoc() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_AR_AG_SKIP_SELF");
}

bool useDirectP2pAllReduceDirectOutputAllgatherPoc() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_AR_DIRECT_OUTPUT_AG");
}

bool useDirectP2pOutputAllgatherPoc() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_OUTPUT_AG");
}

bool useDirectP2pChunked() {
    return envEnabledDefaultOn("MOONCAKE_PG_DIRECT_P2P_CHUNKED",
                               useDeviceApiCollectivesPoc());
}

bool useDirectP2pDeviceSequence() {
    return envEnabledDefaultOn("MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE",
                               useDeviceApiCollectivesPoc());
}

bool useFusedDirectP2pDeviceSequenceReserve() {
    return envEnabled("MOONCAKE_PG_FUSED_DEVICE_SEQUENCE");
}

bool useDirectP2pCurrentStream() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_CURRENT_STREAM");
}

bool useDirectP2pNoEventWork() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_NO_EVENT");
}

bool captureTraceEnabled() { return envEnabled("MOONCAKE_PG_CAPTURE_TRACE"); }

bool replayTraceEnabled() { return envEnabled("MOONCAKE_PG_REPLAY_TRACE"); }

bool replayTraceSequenceEnabled() {
    return envEnabled("MOONCAKE_PG_REPLAY_TRACE_SEQUENCE");
}

bool directP2pAllgatherDebug() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_AG_DEBUG");
}

bool directP2pCountEnabled() {
    return envEnabled("MOONCAKE_PG_DIRECT_P2P_COUNT");
}

bool directP2pAllReduceStageProfileEnabled() {
    return envEnabled("MOONCAKE_PG_AR_STAGE_PROFILE");
}

bool useDeviceRingAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    if (!value || value[0] == '\0') return useDeviceApiCollectivesPoc();
    return value[0] != '0';
}

int deviceRingSignalMode() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    if (value && value[0] == '2') return 1;
    if (value && value[0] == '4') return 2;
    return 0;
}

bool useDeviceFifoRingAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    return value && value[0] == '3';
}

bool useDeviceStoreSignalAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    return value && value[0] == '5';
}

bool useDeviceStoreSignalSlottedAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    if (!value || value[0] == '\0') return useDeviceApiCollectivesPoc();
    return value[0] == '6';
}

int deviceStoreSignalSlots() {
    const char* value = std::getenv("MOONCAKE_PG_STORE_SIGNAL_SLOTS");
    if (!value || value[0] == '\0') {
        return useDeviceApiCollectivesPoc() ? 64 : 8;
    }
    char* end = nullptr;
    long parsed = std::strtol(value, &end, 10);
    if (end == value || parsed < 2) return 2;
    if (parsed > 64) return 64;
    return static_cast<int>(parsed);
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

uint64_t directP2pAllReduceStageProfileLimit() {
    const char* value = std::getenv("MOONCAKE_PG_AR_STAGE_PROFILE_LIMIT");
    if (!value || value[0] == '\0') return 8;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) return 8;
    return parsed;
}

size_t directP2pAllReduceMaxBytes() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_AR_MAX_BYTES");
    if (!value || value[0] == '\0') return 256 * 1024;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) return 0;
    return static_cast<size_t>(parsed);
}

size_t directP2pEffectiveBufferSize() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_BUFFER_MB");
    if (!value || value[0] == '\0') return kDefaultBufferSize;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) return kDefaultBufferSize;
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

bool disableDirectP2pDuringCudaGraphCapture() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_GRAPH_CAPTURE");
    if (useDirectP2pDeviceSequence()) return false;
    return !value || value[0] == '\0' || value[0] == '0';
}

}  // namespace mooncake
