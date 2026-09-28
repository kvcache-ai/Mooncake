import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run_rpc_smoke.sh"


class RpcSmokeTest(unittest.TestCase):
    def test_transfer_result_and_server_cleanup(self):
        # The bandwidth client runs continuously. A timeout is only acceptable
        # after it has reported an actual transfer; other failures must propagate.
        for client_status, bandwidth, expected_status in [
            (0, True, 0),
            (124, True, 0),
            (2, True, 2),
            (124, False, 1),
            (0, False, 1),
        ]:
            with self.subTest(client_status=client_status, bandwidth=bandwidth):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    ready = root / "ready"
                    os.mkfifo(ready)
                    commands = {
                        "python": """#!/usr/bin/python3
import os
import signal
from pathlib import Path
root = Path(os.environ['RUNNER_TEMP'])
(root / 'server.pid').write_text(str(os.getpid()))
with (root / 'ready').open('w') as pipe:
    pipe.write('listening\\n')
signal.pause()
""",
                        # Coordinate readiness through a pipe rather than timing
                        # the background process or binding a real network port.
                        "ss": '#!/bin/sh\ncat "$RUNNER_TEMP/ready"\n',
                        "timeout": """#!/bin/sh
if [ "$REPORT_BANDWIDTH" = true ]; then
    echo 'bandwidth: 1 GB/s'
fi
exit "$CLIENT_STATUS"
""",
                    }
                    for name, content in commands.items():
                        command = root / name
                        command.write_text(content)
                        command.chmod(0o755)

                    result = subprocess.run(
                        ["bash", str(SCRIPT)],
                        env={
                            **os.environ,
                            "PATH": f"{root}:{os.environ['PATH']}",
                            "RUNNER_TEMP": directory,
                            "CLIENT_STATUS": str(client_status),
                            "REPORT_BANDWIDTH": str(bandwidth).lower(),
                        },
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    pid = int((root / "server.pid").read_text())
                    try:
                        with self.assertRaises(ProcessLookupError):
                            os.kill(pid, 0)
                    finally:
                        # Do not leave a server behind if cleanup regresses.
                        try:
                            os.kill(pid, 9)
                        except ProcessLookupError:
                            pass
                    self.assertEqual(
                        result.returncode,
                        expected_status,
                        result.stdout + result.stderr,
                    )


if __name__ == "__main__":
    unittest.main()
