# SPDX-License-Identifier: Apache-2.0
"""Launch SGLang after registering the Mooncake model loader patch."""

from __future__ import annotations


def main() -> None:
    from mooncake.model_loader.sglang_adapter import register

    register()

    # Delegate to SGLang's normal server entrypoint.
    from sglang.srt.entrypoints.http_server import launch_server
    from sglang.srt.server_args import prepare_server_args

    server_args = prepare_server_args()
    launch_server(server_args)


if __name__ == "__main__":
    main()
