// Copyright 2026 KVCache.AI
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

// Public device-side umbrella for GPU-initiated MTLink P2P on MUSA.
// MUSA kernels should include this header instead of individual
// platform/network/IR headers.

#include <tent/runtime/device_resources.h>
#include <tent/device/ir/device_ops.cuh>
#include <tent/device/platform/musa/musa_ops.cuh>
#include <tent/device/mtlink.cuh>
