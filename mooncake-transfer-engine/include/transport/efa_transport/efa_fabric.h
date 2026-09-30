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

#ifndef EFA_FABRIC_H
#define EFA_FABRIC_H

// Include this instead of the <rdma/fi_*.h> headers in the EFA transport.
//
// libfabric is loaded at runtime (see libfabric_loader.cpp) so that Mooncake
// wheels built with USE_EFA still import on hosts without libfabric. Most of
// the libfabric API is static inline and dispatches through provider ops
// tables; only the entry points below are real exported symbols. They are
// renamed before the headers are parsed, so every declaration and every
// inline caller (e.g. fi_allocinfo -> fi_dupinfo) binds to the mc_fi_*
// forwarders instead of the libfabric symbols. The prefix also keeps them
// from interposing on other libfabric users in the process (NIXL,
// aws-ofi-nccl).
#if defined(FABRIC_H)
#error "Include efa_fabric.h before any <rdma/fabric.h> in EFA sources"
#endif

#define fi_getinfo mc_fi_getinfo
#define fi_freeinfo mc_fi_freeinfo
#define fi_dupinfo mc_fi_dupinfo
#define fi_fabric mc_fi_fabric
#define fi_strerror mc_fi_strerror
#define fi_version mc_fi_version

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>

#include <string>

namespace mooncake {

// Minimum libfabric API the EFA transport requests from fi_getinfo().
constexpr uint32_t kEfaRequiredFabricVersion = FI_VERSION(1, 18);

// Loads libfabric on first call; later calls return the cached result.
// Returns false and sets `error` if libfabric cannot be loaded or is older
// than kEfaRequiredFabricVersion. The mc_fi_* entry points must not be used
// before this has returned true.
bool loadLibfabric(std::string* error);

}  // namespace mooncake

#endif  // EFA_FABRIC_H
