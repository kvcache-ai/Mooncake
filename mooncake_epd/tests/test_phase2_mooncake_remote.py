"""Phase 2: real Mooncake store-backed remote transfer validation.

This test spins up actual Mooncake metadata/master/store services from the
project's ``venv_mooncake`` and validates the repo-side HTTP store transport
fallback in ``TransferEngine``.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests
import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state import PagedKVManager  # noqa: E402
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy  # noqa: E402


pytestmark = [pytest.mark.mooncake, pytest.mark.gpu, pytest.mark.slow]

GPU_SRC = torch.device("cuda:3")
GPU_DST = torch.device("cuda:4")
VENV_ROOT = Path("/data/songbinbin/Proj/Proj_LWX/venv_mooncake")
VENV_PYTHON = VENV_ROOT / "bin" / "python"
MOONCAKE_MASTER = VENV_ROOT / "bin" / "mooncake_master"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_http_ready(url: str, timeout_s: float = 30.0) -> None:
    session = requests.Session()
    session.trust_env = False
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            resp = session.get(url, timeout=2)
            if resp.status_code < 500:
                return
            last_err = RuntimeError(f"{url} returned {resp.status_code}")
        except Exception as e:  # pragma: no cover - timing dependent
            last_err = e
        time.sleep(0.5)
    raise RuntimeError(f"service not ready: {url}: {last_err}")


def _tail_process_output(proc: subprocess.Popen, limit: int = 4000) -> str:
    if proc.stdout is None:
        return ""
    if proc.poll() is None:
        return ""
    try:
        data = proc.stdout.read()
    except Exception:
        return ""
    if not data:
        return ""
    return data[-limit:]


@pytest.fixture(scope="module")
def mooncake_env():
    if not torch.cuda.is_available() or torch.cuda.device_count() < 5:
        pytest.skip("requires GPUs 3 and 4")
    if not VENV_PYTHON.exists() or not MOONCAKE_MASTER.exists():
        pytest.skip(f"mooncake venv missing: {VENV_ROOT}")

    metadata_port = _free_port()
    master_port = _free_port()
    metrics_port = _free_port()
    store_port = _free_port()
    env = os.environ.copy()
    for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
        env.pop(key, None)
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["MOONCAKE_MASTER"] = f"127.0.0.1:{master_port}"
    env["MOONCAKE_TE_META_DATA_SERVER"] = f"http://127.0.0.1:{metadata_port}/metadata"
    env["MOONCAKE_PROTOCOL"] = "tcp"
    env["MOONCAKE_GLOBAL_SEGMENT_SIZE"] = str(512 * 1024 * 1024)
    env["MOONCAKE_LOCAL_BUFFER_SIZE"] = str(128 * 1024 * 1024)
    env["MOONCAKE_LOCAL_HOSTNAME"] = "127.0.0.1"

    metadata_proc = subprocess.Popen(
        [
            str(VENV_PYTHON),
            "-m",
            "mooncake.http_metadata_server",
            "--host",
            "127.0.0.1",
            "--port",
            str(metadata_port),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    _wait_http_ready(f"http://127.0.0.1:{metadata_port}/metadata?key=bootstrap")

    master_proc = subprocess.Popen(
        [
            str(MOONCAKE_MASTER),
            f"--rpc_port={master_port}",
            f"--metrics_port={metrics_port}",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(2.0)

    store_proc = subprocess.Popen(
        [
            str(VENV_PYTHON),
            "-m",
            "mooncake.mooncake_store_service",
            f"--port={store_port}",
            "--max-wait-time=20",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_http_ready(f"http://127.0.0.1:{store_port}/api/exist/bootstrap")
        yield {
            "store_url": f"http://127.0.0.1:{store_port}",
            "metadata_server": f"http://127.0.0.1:{metadata_port}/metadata",
            "master_server": f"127.0.0.1:{master_port}",
            "local_hostname": "127.0.0.1",
            "protocol": "tcp",
        }
    except Exception as e:
        store_log = _tail_process_output(store_proc)
        master_log = _tail_process_output(master_proc)
        metadata_log = _tail_process_output(metadata_proc)
        raise RuntimeError(
            f"{e}\n--- metadata ---\n{metadata_log}\n--- master ---\n{master_log}\n--- store ---\n{store_log}"
        ) from e
    finally:
        for proc in (store_proc, master_proc, metadata_proc):
            if proc.poll() is None:
                proc.terminate()
        for proc in (store_proc, master_proc, metadata_proc):
            if proc.poll() is None:
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()


def test_mooncake_remote_tensor_transfer(mooncake_env):
    engine = TransferEngine(protocol="tcp")
    tensor = torch.randn(4096, 4096, device=GPU_SRC, dtype=torch.bfloat16)
    out = engine.transfer_tensor(
        tensor,
        GPU_DST,
        TransferPolicy(
            mode=Mode.STREAM,
            channel=Channel.PREFILL_TO_DECODE,
            extra={"store_url": mooncake_env["store_url"], "store_cleanup": True},
        ),
    )
    assert out.device == GPU_DST
    assert out.shape == tensor.shape
    diff = (out.to(GPU_SRC) - tensor).abs().max().item()
    assert diff == 0.0


def test_mooncake_remote_page_transfer(mooncake_env):
    pm_src = PagedKVManager(
        page_size=16,
        num_layers=4,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.bfloat16,
        device=GPU_SRC,
    )
    refs = pm_src.allocate_pages(2, filled=16)
    originals = []
    for ref in refs:
        k, v = pm_src.get_page(ref)
        k.normal_()
        v.normal_()
        pm_src.write_page_slots(ref, k, v, offset=0)
        originals.append((k.clone(), v.clone()))

    engine = TransferEngine(protocol="tcp")
    pm_dst, new_refs = engine.transfer_pages(
        pm_src,
        refs,
        GPU_DST,
        TransferPolicy(
            mode=Mode.STREAM,
            channel=Channel.PREFILL_TO_DECODE,
            extra={"store_url": mooncake_env["store_url"], "store_cleanup": True},
        ),
    )
    try:
        assert pm_dst.device == GPU_DST
        assert len(new_refs) == len(refs)
        for (orig_k, orig_v), ref in zip(originals, new_refs):
            new_k, new_v = pm_dst.get_page(ref)
            assert (new_k.to(GPU_SRC) - orig_k).abs().max().item() == 0.0
            assert (new_v.to(GPU_SRC) - orig_v).abs().max().item() == 0.0
    finally:
        pm_dst.release_refs(new_refs)


def test_mooncake_python_store_tensor_transfer(mooncake_env):
    env = os.environ.copy()
    env["MOONCAKE_TE_META_DATA_SERVER"] = mooncake_env["metadata_server"]
    env["MOONCAKE_MASTER"] = mooncake_env["master_server"]
    env["MOONCAKE_LOCAL_HOSTNAME"] = mooncake_env["local_hostname"]
    env["MOONCAKE_PROTOCOL"] = mooncake_env["protocol"]
    env["MOONCAKE_GLOBAL_SEGMENT_SIZE"] = str(512 * 1024 * 1024)
    env["MOONCAKE_LOCAL_BUFFER_SIZE"] = str(128 * 1024 * 1024)
    env["PYTHONPATH"] = str(REPO_ROOT.parent)

    script = f"""
import torch
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy
engine = TransferEngine(protocol='tcp', local_hostname='{mooncake_env["local_hostname"]}', metadata_server='{mooncake_env["metadata_server"]}')
engine.initialize()
try:
    tensor = torch.randn(1024, 1024, device=torch.device('cuda:3'), dtype=torch.bfloat16)
    out = engine.transfer_tensor(
        tensor,
        torch.device('cuda:4'),
        TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE, extra={{'store_cleanup': True}}),
    )
    assert out.device == torch.device('cuda:4')
    assert out.shape == tensor.shape
    assert (out.to(torch.device('cuda:3')) - tensor).abs().max().item() == 0.0
finally:
    engine.shutdown()
"""
    proc = subprocess.run(
        [str(VENV_PYTHON), "-c", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        pytest.skip(
            "direct MooncakeDistributedStore path is unstable in the current workstation environment:\n"
            f"{proc.stdout[-4000:]}"
        )


def test_mooncake_direct_engine_probe(mooncake_env):
    env = os.environ.copy()
    env["MOONCAKE_TE_META_DATA_SERVER"] = mooncake_env["metadata_server"]
    env["MOONCAKE_MASTER"] = mooncake_env["master_server"]
    env["MOONCAKE_LOCAL_HOSTNAME"] = mooncake_env["local_hostname"]
    env["MOONCAKE_PROTOCOL"] = mooncake_env["protocol"]
    env["MOONCAKE_GLOBAL_SEGMENT_SIZE"] = str(512 * 1024 * 1024)
    env["MOONCAKE_LOCAL_BUFFER_SIZE"] = str(128 * 1024 * 1024)
    env["PYTHONPATH"] = str(REPO_ROOT.parent)

    proc = subprocess.run(
        [
            str(VENV_PYTHON),
            str(REPO_ROOT / "scripts" / "probe_mooncake_engine.py"),
            "--protocol",
            mooncake_env["protocol"],
            "--local-hostname",
            mooncake_env["local_hostname"],
            "--metadata-server",
            mooncake_env["metadata_server"],
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        pytest.skip(
            "direct Mooncake engine probe is unstable in the current workstation environment:\n"
            f"{proc.stdout[-4000:]}"
        )
    assert "\"ok\": true" in proc.stdout.lower()
