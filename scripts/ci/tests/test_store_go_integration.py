import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run_store_go_integration.sh"


class StoreGoIntegrationScriptTest(unittest.TestCase):
    def test_master_and_go_client_use_dedicated_metadata_port(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "build/mooncake-store/src/mooncake_master"
            binary.parent.mkdir(parents=True)
            binary.write_text(
                "#!/usr/bin/env bash\n"
                'printf "%s\\n" "$@" > "$CI_CAPTURE/master-args"\n'
                'exec cat "$CI_CAPTURE/hold"\n'
            )
            binary.chmod(0o755)
            os.mkfifo(root / "hold")
            (root / "build/CMakeCache.txt").touch()
            (root / "mooncake-store/go").mkdir(parents=True)

            tools = root / "bin"
            tools.mkdir()
            ss = tools / "ss"
            ss.write_text(
                "#!/usr/bin/env bash\n"
                'printf "%s\\n" "$*" >> "$CI_CAPTURE/ports-checked"\n'
                'if [ -f "$CI_CAPTURE/master-args" ]; then echo LISTEN; fi\n'
            )
            ss.chmod(0o755)
            go = tools / "go"
            go.write_text(
                "#!/usr/bin/env bash\n"
                'printf "%s\\n" "$MC_METADATA_SERVER" > "$CI_CAPTURE/go-metadata"\n'
            )
            go.chmod(0o755)

            result = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=SCRIPT.parents[2],
                env={
                    **os.environ,
                    "PATH": f"{tools}:{os.environ['PATH']}",
                    "CI_CAPTURE": str(root),
                    "GITHUB_WORKSPACE": str(root),
                    "RUNNER_TEMP": str(root),
                    "MOONCAKE_STORE_CLUSTER_ID": "go_test_cluster",
                    "MOONCAKE_STORE_GO_SANITIZED": "0",
                },
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(
                "--http_metadata_server_port=18080",
                (root / "master-args").read_text().splitlines(),
            )
            self.assertIn(":18080", (root / "ports-checked").read_text())
            self.assertEqual(
                (root / "go-metadata").read_text().strip(),
                "http://127.0.0.1:18080/metadata",
            )


if __name__ == "__main__":
    unittest.main()
