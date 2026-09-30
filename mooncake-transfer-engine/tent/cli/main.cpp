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

#include <iostream>

#include "CLI11.hpp"
#include "tent/diagnostics/diagnostic.h"

namespace diagnostics = mooncake::tent::diagnostics;

int main(int argc, char** argv) {
    CLI::App app{"Tent CLI", "tent"};
    app.failure_message(CLI::FailureMessage::help);
    auto* diagnostics_command =
        app.add_subcommand("diagnostics", "Run offline diagnostics");
    auto* version_command = diagnostics_command->add_subcommand(
        "version", "Show build and feature information");
    bool json = false;
    version_command->add_flag("--json", json, "Emit JSON");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        app.exit(error);
        return error.get_exit_code() == 0 ? 0 : 2;
    }
    if (!*diagnostics_command) {
        std::cerr << app.help();
        return 2;
    }
    if (!*version_command) {
        std::cerr << diagnostics_command->help();
        return 2;
    }

    const auto snapshot = diagnostics::makeVersionSnapshot();
    if (json) {
        std::cout << diagnostics::renderJson(snapshot) << '\n';
    } else {
        std::cout << diagnostics::renderText(snapshot);
    }
    return diagnostics::diagnosticExitCode(snapshot);
}
