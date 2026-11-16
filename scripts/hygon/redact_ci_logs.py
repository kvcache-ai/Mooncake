#!/usr/bin/env python3
# Copyright 2026 Hygon Information Technology Co., Ltd.
#
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

"""Create redacted log copies for artifact upload; never modify raw logs."""

import ipaddress
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit


def redact_ipv6(match):
    try:
        ipaddress.IPv6Address(match.group())
    except ValueError:
        return match.group()  # Preserve colon-separated timestamps and error text.
    return "[REDACTED_IP]"


def main():
    source, destination = map(Path, sys.argv[1:])
    replacements = {}
    for name in (
        "DTK_PKG_URL",
        "PIP_INDEX_URL",
        "TARGET_HOST",
        "INITIATOR_HOST",
        "TARGET_FILTER",
        "INITIATOR_FILTER",
    ):
        value = os.environ.get(name, "")
        if not value:
            raise SystemExit("ERROR: missing redaction configuration: " + name)
        marker = "[REDACTED_" + name + "]"
        replacements[value] = marker
        if name.endswith("URL"):
            url = urlsplit(value)
            for component in (url.netloc, url.hostname):
                if component:
                    replacements[component] = marker
        elif name.endswith("HOST"):
            try:
                ipaddress.ip_address(value)
            except ValueError:
                replacements[value.split(".")[0]] = marker

    # Match original values in one pass so replacement markers stay intact.
    pattern = re.compile(
        "|".join(
            re.escape(value) for value in sorted(replacements, key=len, reverse=True)
        )
    )
    destination.mkdir(parents=True, exist_ok=True)
    for log in sorted(source.glob("*.log")):
        text = log.read_text(encoding="utf-8", errors="surrogateescape")
        text = pattern.sub(lambda match: replacements[match.group()], text)
        # RDMA logs may encode addresses as 16 colon-separated hex bytes.
        text = re.sub(
            r"(?i)(?<![\w:])(?:[0-9a-f]{2}:){15}[0-9a-f]{2}(?![\w:])",
            "[REDACTED_GID]",
            text,
        )
        text = re.sub(
            r"(?i)(?<![\w:])(?:[0-9a-f]{0,4}:){2,}[0-9a-f]{0,4}(?![\w:])",
            redact_ipv6,
            text,
        )
        text = re.sub(
            r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])", "[REDACTED_IP]", text
        )
        text = re.sub(r"\bmlx5_\d+\b", "[REDACTED_RDMA_DEVICE]", text)
        (destination / log.name).write_text(
            text, encoding="utf-8", errors="surrogateescape"
        )


if __name__ == "__main__":
    main()
