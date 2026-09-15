#include "device_comm/device_primitives/payload_writer.h"

#include <string>

#include "device_comm/device_transfer/transfer_service.h"
#include "pg_utils.h"

namespace mooncake {

PGResult<bool> payloadWriterRequiresStaging(
    DeviceTransferService& transfer_service, GlobalRank peer) {
    PG_TRY(auto route_kind, transfer_service.routeKind(peer));
    switch (route_kind) {
        case DeviceRouteKind::P2p:
            return false;
        case DeviceRouteKind::HostProxy:
            return true;
        case DeviceRouteKind::Unreachable:
            return makePGError(PGErrorCode::InvalidState,
                               "PayloadWriter peer " + std::to_string(peer) +
                                   " has no device transfer route");
    }
    return makePGError(PGErrorCode::NotSupported,
                       "PayloadWriter selected an unsupported transfer route");
}

}  // namespace mooncake
