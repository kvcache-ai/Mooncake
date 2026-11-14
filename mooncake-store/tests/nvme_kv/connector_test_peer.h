#pragma once

#include <memory>
#include <utility>

#include "nvme_kv/connector.h"

namespace mooncake::test {

class NvmeKvConnectorTestPeer {
   public:
    static void SetExecutor(NvmeKvConnector& connector,
                            std::unique_ptr<NvmeKvCommandExecutor> executor) {
        connector.executor_ = std::move(executor);
    }
};

}  // namespace mooncake::test
