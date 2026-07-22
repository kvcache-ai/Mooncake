# SPDX-License-Identifier: Apache-2.0
"""CLI: save a loaded SGLang Engine model into Mooncake Store.

Mirrors ``python -m sglang.save_model_remote_loader`` but targets Mooncake's
first-party ``RemoteWeightIO`` when an Engine ``save_remote_model`` API is
unavailable.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
from typing import Optional

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SaveRemoteArgs:
    url: str
    draft_url: Optional[str] = None

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--url",
            type=str,
            required=True,
            help=(
                "Remote Mooncake URL, e.g. mooncake:///<model_name>. "
                "Legacy redis:// / s3:// URLs are only supported when calling "
                "into an installed SGLang Engine.save_remote_model()."
            ),
        )
        parser.add_argument(
            "--draft-url",
            type=str,
            default=None,
            help="Optional remote URL for a speculative draft model.",
        )

    @classmethod
    def from_cli_args(cls, args: argparse.Namespace) -> "SaveRemoteArgs":
        return cls(url=args.url, draft_url=args.draft_url)


def run_save_remote(server_args, save_args: SaveRemoteArgs) -> None:
    """Initialize SGLang Engine and save weights to remote storage."""
    from sglang.srt.entrypoints.engine import Engine

    print(
        f"Initializing SGLang Engine for model: {server_args.model_path}\n"
        f"Target remote URL: {save_args.url}\n"
    )
    server_args.log_level = "info"

    with Engine(server_args=server_args) as engine:
        print("Engine initialized successfully. Starting remote model save...\n")
        rpc_kwargs = {"url": save_args.url}
        if save_args.draft_url is not None:
            rpc_kwargs["draft_url"] = save_args.draft_url

        if hasattr(engine, "save_remote_model"):
            engine.save_remote_model(**rpc_kwargs)
        else:
            raise RuntimeError(
                "Installed SGLang Engine does not expose save_remote_model(). "
                "Upgrade SGLang, or seed Mooncake Store via "
                "mooncake.model_loader.seed_model_from_files instead."
            )

        print(
            "\nModel saved to remote storage successfully.\n"
            f"  Model URL : {save_args.url}\n"
        )


def main(argv: Optional[list[str]] = None) -> None:
    from sglang.srt.server_args import ServerArgs

    parser = argparse.ArgumentParser(
        description=(
            "Initialize an SGLang Engine and save model weights to "
            "Mooncake Store (mooncake:///<name>)."
        )
    )
    ServerArgs.add_cli_args(parser)
    SaveRemoteArgs.add_cli_args(parser)
    args = parser.parse_args(argv)
    server_args = ServerArgs.from_cli_args(args)
    save_args = SaveRemoteArgs.from_cli_args(args)
    run_save_remote(server_args, save_args)


if __name__ == "__main__":
    main()
