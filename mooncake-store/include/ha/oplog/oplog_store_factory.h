#pragma once

#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "ha/oplog/oplog_store.h"

namespace mooncake {
namespace ha {

struct OpLogStoreFactoryOptions {
    bool enable_latest_seq_batch_update{false};
    bool enable_batch_write{false};
};

tl::expected<std::shared_ptr<OpLogStore>, ErrorCode> CreateOpLogStore(
    const HABackendSpec& spec, const OpLogStoreFactoryOptions& options = {});

bool SupportsOpLogFollowing(HABackendType backend_type);

}  // namespace ha
}  // namespace mooncake
