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

#include "crash_handler.h"

#include <mutex>
#include <signal.h>

#include <glog/logging.h>

namespace mooncake {

void InstallCrashHandler() {
    static std::once_flag install_once;
    std::call_once(install_once, []() {
        // glog versions supported by Mooncake also register SIGTERM as a
        // failure signal. Preserve the host's current disposition across the
        // installation: SIGTERM is a shutdown request, not a crash.
        struct sigaction previous_sigterm{};
        if (sigaction(SIGTERM, nullptr, &previous_sigterm) != 0) return;

        google::InstallFailureSignalHandler();
        sigaction(SIGTERM, &previous_sigterm, nullptr);
    });
}

}  // namespace mooncake
