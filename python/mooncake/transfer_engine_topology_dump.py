import argparse
import json
import os
import sys
import tempfile


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Dump device topology for classic Transfer Engine or TENT. "
            "By default clears custom topology overrides so the output "
            "reflects auto-discovery. Use --use-custom-topo or "
            "--custom-topo-json to load a custom NIC priority matrix instead. "
            "TENT backend prints native {nics,mems} JSON with rank0/1/2."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--device-name",
        type=str,
        default="",
        help=(
            "Filter topology by IB device name(s), comma-separated. "
            "Under TENT this maps to MC_TE_FILTERS."
        ),
    )
    parser.add_argument(
        "--backend",
        choices=("auto", "te", "tent"),
        default="auto",
        help=(
            "Topology backend: te=classic Transfer Engine, tent=Mooncake TENT, "
            "auto=follow MC_USE_TENT/MC_USE_TEV1"
        ),
    )
    parser.add_argument(
        "--use-custom-topo",
        action="store_true",
        help=(
            "Keep existing MC_CUSTOM_TOPO_JSON / TENT topology overrides "
            "(compare discover vs custom by running with and without this flag)"
        ),
    )
    parser.add_argument(
        "--custom-topo-json",
        type=str,
        default=None,
        help=(
            "Path to custom topology JSON; sets MC_CUSTOM_TOPO_JSON "
            "(also honored by TENT as topology/custom_json_path)"
        ),
    )
    return parser.parse_args()


def resolve_backend(backend: str) -> str:
    if backend == "auto":
        if os.environ.get("MC_USE_TENT") or os.environ.get("MC_USE_TEV1"):
            return "tent"
        return "te"
    return backend


def apply_backend(backend: str) -> None:
    if backend == "tent":
        os.environ.setdefault("MC_USE_TENT", "1")
    else:
        os.environ.pop("MC_USE_TENT", None)
        os.environ.pop("MC_USE_TEV1", None)


def _load_tent_conf_object(conf: str):
    if not conf:
        return None, False
    if os.path.isfile(conf):
        with open(conf, "r", encoding="utf-8") as f:
            return json.load(f), True
    try:
        return json.loads(conf), False
    except json.JSONDecodeError:
        return None, False


def sanitize_tent_conf_for_discover():
    """
    Remove topology/priority_matrix and topology/custom_json_path from
    MC_TENT_CONF so discover mode is not overridden by an existing conf.
    Returns a temp file path that the caller should delete, or None.
    """
    conf = os.environ.get("MC_TENT_CONF", "")
    data, _ = _load_tent_conf_object(conf)
    if not isinstance(data, dict):
        return None
    topo = data.get("topology")
    if not isinstance(topo, dict):
        return None
    if "priority_matrix" not in topo and "custom_json_path" not in topo:
        return None

    sanitized = dict(data)
    new_topo = dict(topo)
    new_topo.pop("priority_matrix", None)
    new_topo.pop("custom_json_path", None)
    sanitized["topology"] = new_topo

    fd, path = tempfile.mkstemp(prefix="tent-topo-dump-", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(sanitized, f)
    os.environ["MC_TENT_CONF"] = path
    return path


def dump_te_topology(device_name: str) -> str:
    from mooncake.engine import TransferEngine

    engine = TransferEngine()
    return engine.get_local_topology(device_name=device_name or None)


def dump_tent_topology(device_name: str) -> str:
    """
    Prefer the native tent Python module (same engine path as tebench).
    Fall back to mooncake.engine under MC_USE_TENT, which returns native
    {"nics","mems"} via get_local_topology / getLocalTopologyString.
    """
    if device_name:
        os.environ["MC_TE_FILTERS"] = device_name

    try:
        import tent

        engine = tent.TransferEngine()
        if not engine.available():
            raise RuntimeError("tent.TransferEngine is not available")
        return engine.get_local_topology()
    except ImportError:
        return dump_te_topology(device_name)


def main():
    args = parse_args()
    os.environ["MC_LOG_LEVEL"] = "ERROR"

    backend = resolve_backend(args.backend)
    apply_backend(backend)

    temp_conf = None
    if args.custom_topo_json is not None:
        os.environ["MC_CUSTOM_TOPO_JSON"] = args.custom_topo_json
        mode = f"custom ({args.custom_topo_json})"
    elif args.use_custom_topo:
        custom_path = os.environ.get("MC_CUSTOM_TOPO_JSON", "")
        has_inline = False
        if backend == "tent":
            data, _ = _load_tent_conf_object(os.environ.get("MC_TENT_CONF", ""))
            topo = data.get("topology") if isinstance(data, dict) else None
            has_inline = isinstance(topo, dict) and (
                "priority_matrix" in topo or "custom_json_path" in topo
            )
        if not custom_path and not has_inline:
            print(
                "error: --use-custom-topo requires MC_CUSTOM_TOPO_JSON "
                "or TENT topology/priority_matrix|custom_json_path in "
                "MC_TENT_CONF",
                file=sys.stderr,
            )
            return 1
        mode = (
            f"custom ({custom_path})"
            if custom_path
            else "custom (MC_TENT_CONF topology)"
        )
    else:
        # Force discover path. Empty string still makes getenv non-null in C++,
        # which falls back to auto-detect when the path cannot be loaded.
        os.environ["MC_CUSTOM_TOPO_JSON"] = ""
        if backend == "tent":
            temp_conf = sanitize_tent_conf_for_discover()
        mode = "discover"

    try:
        if backend == "tent":
            topo = dump_tent_topology(args.device_name)
        else:
            topo = dump_te_topology(args.device_name)
        print(f"Local topology [{backend}/{mode}]: ", end="")
        print(topo)
        return 0
    except Exception as exc:
        print(f"error: failed to dump topology: {exc}", file=sys.stderr)
        return 1
    finally:
        if temp_conf and os.path.exists(temp_conf):
            try:
                os.unlink(temp_conf)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
