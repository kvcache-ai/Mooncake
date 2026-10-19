#include "spdk_controller_config.h"

#include <glog/logging.h>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

SpdkControllerConfig SpdkControllerConfig::FromEnvironment() {
    SpdkControllerConfig config;
    using Variables = SpdkControllerEnvironmentVariables;
    config.num_io_queues = Environ::Read(Variables::MC_NVME_NUM_IO_QUEUES);
    config.io_queue_size = Environ::Read(Variables::MC_NVME_IO_QUEUE_SIZE);
    config.io_queue_requests =
        Environ::Read(Variables::MC_NVME_IO_QUEUE_REQUESTS);
    config.transport_ack_timeout =
        Environ::Read(Variables::MC_NVME_TRANSPORT_ACK_TIMEOUT);
    config.admin_queue_size =
        Environ::Read(Variables::MC_NVME_ADMIN_QUEUE_SIZE);
    config.fabrics_connect_timeout_us =
        Environ::Read(Variables::MC_NVME_FABRICS_CONNECT_TIMEOUT_US);
    config.header_digest = Environ::Read(Variables::MC_NVME_HEADER_DIGEST);
    config.data_digest = Environ::Read(Variables::MC_NVME_DATA_DIGEST);

    config.keep_alive_timeout_ms =
        Environ::Read(Variables::MC_NVME_KEEP_ALIVE_TIMEOUT_MS);
    // SPDK 23.01 computes the keep-alive interval as kato_ms * 1000 / 2
    // in uint32_t arithmetic. Above ~71 minutes, the multiplication
    // overflows before division. Cap KATO at one hour to avoid this.
    if (config.keep_alive_timeout_ms.value_or(0) > 3600000) {
        LOG(WARNING) << "MC_NVME_KEEP_ALIVE_TIMEOUT_MS must be between 0 and "
                        "3600000; using the default";
        config.keep_alive_timeout_ms.reset();
    }
    return config;
}

}  // namespace mooncake
