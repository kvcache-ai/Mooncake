#ifndef MOONCAKE_PG_EXPERIMENTAL_H
#define MOONCAKE_PG_EXPERIMENTAL_H

#include <cstddef>
#include <cstdint>

namespace mooncake {

bool useDeviceApiCollectivesPoc();
bool deviceCollectiveRuntimeDebugEnabled();
bool useDeviceApiHierarchicalAllReducePoc();

bool useDirectP2pAllgatherPoc();
bool useDirectP2pReduceScatterPoc();
bool useDirectP2pAllReducePoc();
bool useDirectP2pAllReduceRingPoc();
bool useDirectP2pAllReduceFusedRsAgPoc();
bool useDirectP2pAllReduceAllgatherSkipSelfPoc();
bool useDirectP2pAllReduceDirectOutputAllgatherPoc();
bool useDirectP2pOutputAllgatherPoc();
bool useDirectP2pChunked();
bool useDirectP2pDeviceSequence();
bool useFusedDirectP2pDeviceSequenceReserve();
bool useDirectP2pCurrentStream();
bool useDirectP2pNoEventWork();

bool captureTraceEnabled();
bool replayTraceEnabled();
bool replayTraceSequenceEnabled();
bool directP2pAllgatherDebug();
bool directP2pCountEnabled();
bool directP2pAllReduceStageProfileEnabled();
bool disableDirectP2pDuringCudaGraphCapture();

int deviceRingSignalMode();
int deviceStoreSignalSlots();

uint32_t directP2pDeviceSequenceSlotCapacity();
uint64_t directP2pAllReduceStageProfileLimit();

size_t directP2pAllReduceMaxBytes();
size_t directP2pEffectiveBufferSize();
size_t directP2pSlotStride(size_t effectiveBufferSize, size_t signalBytes,
                           int slots, int activeSize);
size_t directP2pSlottedControlBytes(int slots, bool deviceApiCollective);

bool useDeviceRingAllgatherPoc();
bool useDeviceFifoRingAllgatherPoc();
bool useDeviceStoreSignalAllgatherPoc();
bool useDeviceStoreSignalSlottedAllgatherPoc();

}  // namespace mooncake

#endif  // MOONCAKE_PG_EXPERIMENTAL_H
