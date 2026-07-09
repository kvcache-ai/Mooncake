from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Optional

from .model import DEFAULT_FILE_CHUNK_SIZE, ModelFileCacheClient


DEFAULT_CONFIG_PATH = "~/.config/mooncake/weight_store.json"

# The CLI is always a pure Store client: it never contributes a storage
# segment (global_segment_size = 0) and only needs a small local RDMA buffer.
_CLIENT_GLOBAL_SEGMENT_SIZE = 0
_CLIENT_LOCAL_BUFFER_SIZE = 512 * 1024 * 1024


def build_parser(argv: Optional[List[str]] = None) -> argparse.ArgumentParser:
    if argv is None:
        argv = sys.argv[1:]
    argv = _normalize_global_args(argv)
    defaults = _load_config_defaults(argv)
    parser = argparse.ArgumentParser(description="Manage Mooncake model weight cache.")
    _add_store_args(parser, defaults)

    subparsers = parser.add_subparsers(dest="command", required=True)
    config_parser = subparsers.add_parser("config")
    config_subparsers = config_parser.add_subparsers(
        dest="config_command", required=True
    )
    config_subparsers.add_parser("init").set_defaults(handler=_cmd_config_init)

    model_parser = subparsers.add_parser("model")
    model_subparsers = model_parser.add_subparsers(dest="model_command", required=True)

    import_parser = model_subparsers.add_parser("import")
    import_parser.add_argument("--checkpoint-id", required=True)
    import_parser.add_argument("--model-id", required=True)
    import_parser.add_argument("--revision", required=True)
    import_parser.add_argument("--source", required=True)
    import_parser.add_argument(
        "--replica-num", type=int, default=defaults["replica_num"]
    )
    import_parser.add_argument("--file-chunk-size", default=defaults["file_chunk_size"])
    import_parser.set_defaults(handler=_cmd_model_import)

    model_subparsers.add_parser("list").set_defaults(handler=_cmd_model_list)

    handlers = {
        "inspect": _cmd_model_inspect,
        "verify": _cmd_model_verify,
        "delete": _cmd_model_delete,
    }
    for command, handler in handlers.items():
        command_parser = model_subparsers.add_parser(command)
        command_parser.add_argument("checkpoint_id")
        command_parser.set_defaults(handler=handler)

    materialize_parser = model_subparsers.add_parser("materialize-file")
    materialize_parser.add_argument("checkpoint_id")
    materialize_parser.add_argument("--path", required=True)
    materialize_parser.add_argument("--output", required=True)
    materialize_parser.set_defaults(handler=_cmd_model_materialize)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    argv = _normalize_global_args(argv)
    args = build_parser(argv).parse_args(argv)
    return args.handler(args)


def _cmd_config_init(args) -> int:
    config_path = Path(args.config).expanduser()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    payload = _store_config_from_args(args)
    config_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(_to_json({"config": str(config_path), "written": True}))
    return 0


def _cmd_model_import(args) -> int:
    manifest = _create_model_client(args).import_model(
        checkpoint_id=args.checkpoint_id,
        model_id=args.model_id,
        revision=args.revision,
        source_uri=args.source,
    )
    print(_to_json(manifest))
    return 0


def _cmd_model_list(args) -> int:
    print(_to_json(_create_model_client(args).list_models()))
    return 0


def _cmd_model_inspect(args) -> int:
    print(_to_json(_create_model_client(args).inspect_model(args.checkpoint_id)))
    return 0


def _cmd_model_verify(args) -> int:
    print(_to_json(_create_model_client(args).verify_model(args.checkpoint_id)))
    return 0


def _cmd_model_delete(args) -> int:
    _create_model_client(args).delete_model(args.checkpoint_id)
    print(_to_json({"checkpoint_id": args.checkpoint_id, "deleted": True}))
    return 0


def _cmd_model_materialize(args) -> int:
    _create_model_client(args).materialize_file(
        args.checkpoint_id, args.path, args.output
    )
    print(
        _to_json(
            {
                "checkpoint_id": args.checkpoint_id,
                "path": args.path,
                "output": args.output,
            }
        )
    )
    return 0


def _to_json(value) -> str:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    return json.dumps(value, ensure_ascii=False, indent=2)


def _create_model_client(args) -> ModelFileCacheClient:
    # replica_num / file_chunk_size only apply to import; other commands do not
    # define them and fall back to the client defaults.
    kwargs = {}
    if getattr(args, "replica_num", None) is not None:
        kwargs["replica_num"] = args.replica_num
    if getattr(args, "file_chunk_size", None) is not None:
        kwargs["file_chunk_size"] = _parse_size(args.file_chunk_size)
    return ModelFileCacheClient(_connect_store(args), progress=True, **kwargs)


def _add_store_args(parser: argparse.ArgumentParser, defaults: dict) -> None:
    parser.add_argument("--config", default=defaults["config"])
    parser.add_argument("--local-hostname", default=defaults["local_hostname"])
    parser.add_argument("--metadata-server", default=defaults["metadata_server"])
    parser.add_argument("--protocol", default=defaults["protocol"])
    parser.add_argument("--rdma-devices", default=defaults["rdma_devices"])
    parser.add_argument("--master-server-addr", default=defaults["master_server_addr"])


def _connect_store(args):
    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    global_segment_size = _CLIENT_GLOBAL_SEGMENT_SIZE
    local_buffer_size = _CLIENT_LOCAL_BUFFER_SIZE
    config = {
        "local_hostname": args.local_hostname,
        "metadata_server": args.metadata_server,
        "global_segment_size": global_segment_size,
        "local_buffer_size": local_buffer_size,
        "protocol": args.protocol,
        "rdma_devices": args.rdma_devices,
        "master_server_addr": args.master_server_addr,
    }

    try:
        result = store.setup(
            args.local_hostname,
            args.metadata_server,
            global_segment_size,
            local_buffer_size,
            args.protocol,
            args.rdma_devices,
            args.master_server_addr,
        )
    except TypeError:
        result = store.setup(config)

    if result != 0:
        raise RuntimeError(f"failed to setup MooncakeDistributedStore: {result}")
    return store


def _parse_size(value) -> int:
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    units = {
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
    }
    upper = text.upper()
    for suffix, multiplier in units.items():
        if upper.endswith(suffix):
            number = upper[: -len(suffix)].strip()
            return int(float(number) * multiplier)
    raise ValueError(f"invalid size: {value!r}")


def _load_config_defaults(argv: Optional[List[str]]) -> dict:
    defaults = {
        "config": os.environ.get("MOONCAKE_WEIGHT_STORE_CONFIG", DEFAULT_CONFIG_PATH),
        "local_hostname": "localhost",
        "metadata_server": "http://127.0.0.1:8080/metadata",
        "protocol": "tcp",
        "rdma_devices": "",
        "master_server_addr": "127.0.0.1:50051",
        "replica_num": 1,
        "file_chunk_size": DEFAULT_FILE_CHUNK_SIZE,
    }
    config_path = _extract_config_path(argv, defaults["config"])
    defaults["config"] = config_path
    path = Path(config_path).expanduser()
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        for key in defaults:
            if key in loaded and key != "config":
                defaults[key] = loaded[key]
    return defaults


def _extract_config_path(argv: Optional[List[str]], default: str) -> str:
    if argv is None:
        return default
    for index, item in enumerate(argv):
        if item == "--config" and index + 1 < len(argv):
            return argv[index + 1]
        if item.startswith("--config="):
            return item.split("=", 1)[1]
    return default


def _normalize_global_args(argv: List[str]) -> List[str]:
    value_flags = {
        "--config",
        "--local-hostname",
        "--metadata-server",
        "--protocol",
        "--rdma-devices",
        "--master-server-addr",
    }
    moved: List[str] = []
    remaining: List[str] = []
    index = 0
    while index < len(argv):
        item = argv[index]
        flag = item.split("=", 1)[0]
        if flag in value_flags:
            moved.append(item)
            if "=" not in item:
                if index + 1 >= len(argv):
                    remaining.append(item)
                else:
                    moved.append(argv[index + 1])
                    index += 1
        else:
            remaining.append(item)
        index += 1
    return moved + remaining


def _store_config_from_args(args) -> dict:
    return {
        "local_hostname": args.local_hostname,
        "metadata_server": args.metadata_server,
        "protocol": args.protocol,
        "rdma_devices": args.rdma_devices,
        "master_server_addr": args.master_server_addr,
    }


if __name__ == "__main__":
    raise SystemExit(main())
