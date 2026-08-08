// Copyright 2026 Mooncake Authors
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

namespace mooncake {

// Installs best-effort stderr stack traces for fatal crash signals. The
// installation is process-global and idempotent. It deliberately leaves
// SIGINT and SIGTERM unchanged so host applications retain shutdown ownership.
// After reporting, the original fatal signal is re-raised by glog's handler,
// preserving normal signal termination and operating-system core-dump policy.
void InstallCrashHandler();

}  // namespace mooncake
