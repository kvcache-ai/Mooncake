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

// The built-in TCP transport as a plugin (TENT_XPORT_TCP=plugin or both). It
// claims the "tcp" slot and, like the built-in, is enabled by default; a core
// that already has TCP rejects it as a conflict.

#ifndef TENT_TRANSPORT_PLUGIN_VERSION
#define TENT_TRANSPORT_PLUGIN_VERSION "1"
#endif

#include "tent/transport/tcp/tcp_transport.h"
#include "tent/transport_plugin.hpp"

// TcpTransport reports notification support and, on a non-CPU platform, the
// GPU capabilities as well.
TENT_DEFINE_TRANSPORT_PLUGIN(
    tcp, mooncake::tent::TcpTransport, mooncake::tent::TCP,
    TENT_XPORT_FLAG_ENABLED_BY_DEFAULT | TENT_XPORT_FLAG_NOTIFICATION,
    TENT_XPORT_CAP_DRAM_TO_DRAM | TENT_XPORT_CAP_DRAM_TO_GPU |
        TENT_XPORT_CAP_GPU_TO_DRAM | TENT_XPORT_CAP_GPU_TO_GPU)
