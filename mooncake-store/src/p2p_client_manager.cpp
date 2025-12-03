#include "p2p_client_manager.h"
#include "master_metric_manager.h"
#include <glog/logging.h>

namespace mooncake {

P2PClientManager::P2PClientManager(const int64_t client_live_ttl_sec) :
    ClientManager(client_live_ttl_sec) {}

ErrorCode P2PClientManager::UnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    // TODO
    return ErrorCode::OK;
}

void P2PClientManager::ClientMonitorFunc() {
    while (client_monitor_running_) {
        // TODO
    }
}

}  // namespace mooncake
