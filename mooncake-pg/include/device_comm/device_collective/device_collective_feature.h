#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_FEATURE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_FEATURE_H

// Collective v2 is temporarily CUDA-only. Support for other GPU platforms will
// be added as the runtime matures. Until then, keep this compatibility
// workaround centralized in this feature gate.
#ifndef MOONCAKE_PG_HAS_COLLECTIVE_V2
#if defined(USE_CUDA)
#define MOONCAKE_PG_HAS_COLLECTIVE_V2 1
#else
#define MOONCAKE_PG_HAS_COLLECTIVE_V2 0
#endif
#endif

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_FEATURE_H
