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

#pragma once

#include <cstddef>

namespace mooncake {

// These functions are no-ops for non-Ascend memory and when the required
// runtime symbols are unavailable. Registrations of the same range are
// reference-counted because RdmaTransport registers it once per NIC.
int ascendUbRegister(void *addr, size_t length);
int ascendUbUnregister(void *addr, size_t length);

}  // namespace mooncake
