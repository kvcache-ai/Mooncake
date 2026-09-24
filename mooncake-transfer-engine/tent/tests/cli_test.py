# Copyright 2026 KVCache.AI
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import sys

cases = [
    (["--help"], 0, "diagnostics", ""),
    (["diagnostics", "--help"], 0, "version", ""),
    (["diagnostics", "version"], 0, "build.info.available", ""),
    (["diagnostics", "version", "--json"], 0, '"command": "version"', ""),
    ([], 2, "", "tent [OPTIONS]"),
    (["unknown"], 2, "", "tent [OPTIONS]"),
    (["version"], 2, "", "tent [OPTIONS]"),
    (["diagnostics"], 2, "", "diagnostics [OPTIONS]"),
    (["diagnostics", "unknown"], 2, "", "tent diagnostics [OPTIONS]"),
    (["diagnostics", "version", "--bad"], 2, "", "tent diagnostics version"),
    (["diagnostics", "version", "--json", "extra"], 2, "", "tent diagnostics version"),
]

for args, code, stdout, stderr in cases:
    result = subprocess.run(
        [sys.argv[1], *args], capture_output=True, text=True, timeout=10, check=False
    )
    assert result.returncode == code, (args, result)
    assert stdout in result.stdout if stdout else not result.stdout, (args, result)
    assert stderr in result.stderr if stderr else not result.stderr, (args, result)
