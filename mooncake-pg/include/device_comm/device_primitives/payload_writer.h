#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_PAYLOAD_WRITER_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_PAYLOAD_WRITER_H

#include "common_types.h"
#include "error_types.h"

namespace mooncake {

class DeviceTransferService;

// Host-side resource query for the device PayloadWriter. The selected route
// remains a DTS implementation detail; callers only learn whether they must
// ask DTS to prepare local staging before publishing a Plan.
PGResult<bool> payloadWriterRequiresStaging(
    DeviceTransferService& transfer_service, GlobalRank peer);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_PAYLOAD_WRITER_H
