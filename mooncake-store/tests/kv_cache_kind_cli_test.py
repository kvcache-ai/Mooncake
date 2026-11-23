"""Check the effective KV cache policy selected by the master executable."""

from pathlib import Path
import subprocess
import sys
import tempfile
import time


def check_policy(binary, args, expected, directory):
    log = directory / "master.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            [
                binary,
                "--rpc_address=127.0.0.1",
                "--port=0",
                "--metrics_port=0",
                "--rpc_thread_num=1",
                "--logtostderr",
                *args,
            ],
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                data = log.read_text()
                if ", kv_cache_kind_evict=" in data or process.poll() is not None:
                    break
                time.sleep(0.05)
            assert f", kv_cache_kind_evict={expected}," in log.read_text(), (
                args,
                log.read_text(),
            )
        finally:
            process.kill()
            process.wait()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as path:
        directory = Path(path)
        config = directory / "master.yaml"
        config.write_text("kv_cache_kind_evict: 'SWA=DEFER'\n")
        file_arg = f"--config_path={config}"
        flag = "--kv_cache_kind_evict=MAMBA=PROTECT"
        for args, expected in [
            ([flag], "MAMBA=PROTECT"),
            ([file_arg], "SWA=DEFER"),
            ([file_arg, flag], "MAMBA=PROTECT"),
            ([file_arg, "--kv_cache_kind_evict="], ""),
        ]:
            check_policy(sys.argv[1], args, expected, directory)
