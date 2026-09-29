#include "spdk_controller_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

SpdkControllerConfig SpdkControllerConfig::FromEnvironment(const Environ& env) {
    SpdkControllerConfig config;
    using Variables = CommonEnvironmentVariables::SpdkController;
    config.num_io_queues = env.GetTyped(Variables::MC_NVME_NUM_IO_QUEUES);
    config.io_queue_size = env.GetTyped(Variables::MC_NVME_IO_QUEUE_SIZE);
    config.io_queue_requests =
        env.GetTyped(Variables::MC_NVME_IO_QUEUE_REQUESTS);
    config.transport_ack_timeout =
        env.GetTyped(Variables::MC_NVME_TRANSPORT_ACK_TIMEOUT);
    config.admin_queue_size = env.GetTyped(Variables::MC_NVME_ADMIN_QUEUE_SIZE);
    config.fabrics_connect_timeout_us =
        env.GetTyped(Variables::MC_NVME_FABRICS_CONNECT_TIMEOUT_US);
    config.header_digest = env.GetTyped(Variables::MC_NVME_HEADER_DIGEST);
    config.data_digest = env.GetTyped(Variables::MC_NVME_DATA_DIGEST);
    return config;
}

}  // namespace mooncake
