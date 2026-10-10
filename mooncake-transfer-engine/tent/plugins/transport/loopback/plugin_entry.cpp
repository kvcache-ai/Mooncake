// Copyright 2025 KVCache.AI
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

#ifndef TENT_TRANSPORT_PLUGIN_VERSION
#define TENT_TRANSPORT_PLUGIN_VERSION "1"
#endif

#include "loopback_transport.h"
#include "tent/transport_plugin.hpp"

TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(loopback,
                                       mooncake::tent::LoopbackTransport,
                                       mooncake::tent::UNSPEC,
                                       TENT_XPORT_FLAG_SAME_MACHINE_ONLY |
                                           TENT_XPORT_FLAG_NOTIFICATION |
                                           TENT_XPORT_FLAG_CANCELLATION,
                                       TENT_XPORT_CAP_DRAM_TO_DRAM)
