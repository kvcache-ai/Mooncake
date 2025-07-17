// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport/cxl_transport/cxl_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
CxlTransport::CxlTransport() {
	// TODO
}

CxlTransport::~CxlTransport() {
}

CxlTransport::BatchID CxlTransport::allocateBatchID(size_t batch_size) {
	auto batch_id = Transport::allocateBatchID(batch_size);
	return batch_id;
}

Status CxlTransport::getTransferStatus(BatchID batch_id, size_t task_id, TransferStatus &status) {
	return 0;
}

Status CxlTransport::submitTransfer(BatchID batch_id, const std::vector<TransferRequest> &entries) {
	return 0;
}

int CxlTransport::freeBatchID(BatchID batch_id) {
	return 0;
}

int CxlTransport::install(std::string &local_server_name, std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
	return 0;
}

int CxlTransport::registerLocalMemory(void *addr, size_t length, const string &location, bool remote_accessible,
                                      bool update_metadata) {
	return 0;
}

int CxlTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
	return 0;
}
} // namespace mooncake
