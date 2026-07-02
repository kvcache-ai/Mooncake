#ifndef MOONCAKE_PG_EXPERIMENTAL_H
#define MOONCAKE_PG_EXPERIMENTAL_H

#include <cstddef>
#include <cstdint>

namespace mooncake {

bool useDeviceApiCollectivesPoc();
bool deviceCollectiveRuntimeDebugEnabled();

uint32_t directP2pDeviceSequenceSlotCapacity();
size_t directP2pEffectiveBufferSize();
size_t directP2pSlotStride(size_t effectiveBufferSize, size_t signalBytes,
                           int slots, int activeSize);
size_t directP2pSlottedControlBytes(int slots, bool deviceApiCollective);

}  // namespace mooncake

#endif  // MOONCAKE_PG_EXPERIMENTAL_H
