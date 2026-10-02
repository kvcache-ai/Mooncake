# Environment probing for manifest.json: hardware, driver, framework versions, model.

import json
import os
import platform
import subprocess
import sys
import time


def _run(command, timeout=30):
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def probe_gpus():
    """Probe GPU model and count through nvidia-smi and torch."""
    info = {
        "nvidia_smi_query": None,
        "gpu_names": [],
        "gpu_count": 0,
        "per_gpu_memory_total_mib": [],
        "per_gpu_memory_used_mib": [],
        "driver_version": None,
        "cuda_version_reported_by_driver": None,
    }
    query = _run(
        [
            "nvidia-smi",
            "--query-gpu=index,name,memory.total,memory.used,driver_version",
            "--format=csv,noheader",
        ]
    )
    if query:
        info["nvidia_smi_query"] = query
        for line in query.splitlines():
            parts = [part.strip() for part in line.split(",")]
            if len(parts) < 5:
                continue
            info["gpu_names"].append(parts[1])
            info["per_gpu_memory_total_mib"].append(parts[2])
            info["per_gpu_memory_used_mib"].append(parts[3])
            info["driver_version"] = parts[4]
        info["gpu_count"] = len(info["gpu_names"])
    header = _run(["nvidia-smi"])
    if header:
        for line in header.splitlines():
            if "CUDA Version" in line:
                marker = line.split("CUDA Version:")[-1].strip()
                info["cuda_version_reported_by_driver"] = marker.split()[0]
                break
    return info


def visible_gpu_count():
    """Number of GPUs visible to this process, honouring CUDA_VISIBLE_DEVICES."""
    import torch

    if not torch.cuda.is_available():
        return 0
    return torch.cuda.device_count()


def probe_torch():
    info = {
        "torch_version": None,
        "cuda_runtime": None,
        "device_count": None,
        "device_names": [],
        "attention_backends": [],
    }
    try:
        import torch
    except ImportError:
        return info
    info["torch_version"] = torch.__version__
    info["cuda_runtime"] = torch.version.cuda
    if torch.cuda.is_available():
        info["device_count"] = torch.cuda.device_count()
        info["device_names"] = [
            torch.cuda.get_device_name(index)
            for index in range(torch.cuda.device_count())
        ]
    return info


def probe_package(name):
    try:
        module = __import__(name)
    except ImportError:
        return None
    version = getattr(module, "__version__", None)
    if version is None:
        try:
            import importlib.metadata

            version = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            version = "unknown"
    return {"version": str(version), "path": getattr(module, "__file__", None)}


def probe_git(repo_dir):
    info = {
        "commit": _run(["git", "-C", repo_dir, "rev-parse", "HEAD"]),
        "subject": _run(["git", "-C", repo_dir, "log", "-1", "--pretty=%s"]),
        "branch": _run(["git", "-C", repo_dir, "rev-parse", "--abbrev-ref", "HEAD"]),
        "dirty": bool(_run(["git", "-C", repo_dir, "status", "--porcelain"])),
        "source": "detected",
    }
    return info


def probe_model_revision(model_path):
    info = {"path": model_path, "revision": None, "config_sha": None}
    if not model_path or not os.path.isdir(model_path):
        return info
    for name in (".mv", ".msc", "revision.txt"):
        candidate = os.path.join(model_path, name)
        if os.path.isfile(candidate):
            with open(candidate, "r", encoding="utf-8", errors="replace") as handle:
                info["revision"] = handle.read().strip()[:200]
            break
    config_path = os.path.join(model_path, "config.json")
    if os.path.isfile(config_path):
        import hashlib

        with open(config_path, "rb") as handle:
            info["config_sha"] = hashlib.sha256(handle.read()).hexdigest()[:16]
    return info


def build_manifest(
    repo_dir,
    model_path,
    model_kv_config,
    plan,
    command_line,
    extra=None,
    git_override=None,
):
    """Collect the full environment description for one run.

    git_override is used when the directory under test has no complete .git, so
    the caller supplies the commit information directly.
    """
    git_info = probe_git(repo_dir)
    if git_info.get("commit") is None and git_override:
        git_info = {
            "commit": git_override.get("commit"),
            "subject": git_override.get("subject"),
            "branch": git_override.get("branch"),
            "dirty": git_override.get("dirty"),
            "source": "explicit_from_driver",
            "repo_dir": repo_dir,
        }

    manifest = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "epoch_seconds": time.time(),
        "command": command_line,
        "repo": git_info,
        "hostname": platform.node(),
        "python_version": sys.version.split()[0],
        "gpu": probe_gpus(),
        "torch": probe_torch(),
        "flashinfer": probe_package("flashinfer"),
        "sglang": probe_package("sglang"),
        "model": probe_model_revision(model_path),
        "workload": plan.as_dict() if plan is not None else None,
    }

    if model_kv_config is not None:
        aggregate, per_rank = model_kv_config.kv_bytes_per_token(plan.tp_size)
        manifest["model"]["kv_config"] = model_kv_config.as_dict()
        manifest["model"]["dtype"] = model_kv_config.torch_dtype
        manifest["model"]["tp_size"] = plan.tp_size
        manifest["model"]["kv_bytes_per_token_aggregate"] = aggregate
        manifest["model"]["kv_bytes_per_token_per_rank"] = per_rank

    if extra:
        manifest.update(extra)
    return manifest


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=False, default=str)
        handle.write("\n")
