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

#ifndef ERROR_H
#define ERROR_H

#define ERR_INVALID_ARGUMENT (-1)
#define ERR_TOO_MANY_REQUESTS (-2)
#define ERR_ADDRESS_NOT_REGISTERED (-3)
#define ERR_BATCH_BUSY (-4)
#define ERR_DEVICE_NOT_FOUND (-6)
#define ERR_ADDRESS_OVERLAPPED (-7)

#define ERR_DNS (-101)
#define ERR_SOCKET (-102)
#define ERR_MALFORMED_JSON (-103)
#define ERR_REJECT_HANDSHAKE (-104)

#define ERR_METADATA (-200)
#define ERR_ENDPOINT (-201)
#define ERR_CONTEXT (-202)
#define ERR_ENDPOINT_NOT_FOUND (-203)
#define ERR_ENDPOINT_ID_MISMATCH (-204)

#define ERR_NUMA (-300)
#define ERR_CLOCK (-301)
#define ERR_MEMORY (-302)
#define ERR_NOT_IMPLEMENTED (-303)

#endif  // ERROR_H
