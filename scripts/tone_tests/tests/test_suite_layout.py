import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parents[2]


class TestSuiteLayout(unittest.TestCase):
    def shell(self, command):
        return subprocess.check_output(["bash", "-eu", "-c", command], text=True)

    def test_platform_lifecycle_has_no_accelerator_dispatch(self):
        for suite in ("tone_tests", "rocm_tests"):
            text = (SCRIPTS / suite / "scripts/common.sh").read_text()
            self.assertNotIn('"${CI_ACCELERATOR:-cuda}"', text)

    def test_shared_helpers_are_loaded_explicitly(self):
        for suite in ("tone_tests", "rocm_tests"):
            common = SCRIPTS / suite / "scripts/common.sh"
            self.shell(
                f'CONTAINER_NAME=test; E2E_DIR="{SCRIPTS}/e2e"; source "{common}"; '
                "declare -f launch_sglang_server >/dev/null; "
                "declare -f docker_launch >/dev/null"
            )
            self.assertFalse(any(p.is_symlink() for p in (SCRIPTS / suite).rglob("*")))

    @unittest.skipUnless(shutil.which("rsync"), "rsync is required")
    def test_remote_setup_uses_explicit_shared_directory(self):
        for suite in ("tone_tests", "rocm_tests"):
            with tempfile.TemporaryDirectory() as destination:
                root = Path(destination)
                platform = root / "platform"
                (platform / "scripts").mkdir(parents=True)
                shutil.copy(SCRIPTS / suite / "scripts/common.sh", platform / "scripts")
                self.shell(
                    f"""CONTAINER_NAME=test
                    SUITE_DIR="{platform}"
                    RUN_DIR="$SUITE_DIR/run"
                    E2E_DIR="{SCRIPTS}/e2e"
                    REMOTE_WORK_ROOT="{root}/worker"
                    REMOTE_TEST_DIR=$REMOTE_WORK_ROOT
                    REMOTE_IP=192.0.2.2
                    REMOTE_SSH_TARGET=worker
                    SSH_CMD=mock_ssh
                    RSYNC_RSH="ssh -F /test/config"
                    MOONCAKE_RENDER_DEVICES="/dev/dri/renderD129 /dev/dri/renderD137"
                    source "$SUITE_DIR/scripts/common.sh"
                    source "$E2E_DIR/scripts/controller.sh"
                    get_whl() {{ :; }}
                    mock_ssh() {{ shift; bash -eu -c "$1"; }}
                    rsync() {{
                        [ "$1" = -av ]
                        local src="${{@: -2:1}}" dst="${{@: -1}}"
                        command rsync -a "$src" "${{dst#worker:}}"
                    }}
                    prepare_double_env image SGLANG
                    source "$REMOTE_TEST_DIR/run/.shrc"
                    [ "$E2E_DIR" = "$REMOTE_TEST_DIR/e2e" ]
                    [ "$BASE_DIR" = "$REMOTE_TEST_DIR" ]
                    [ "$RSYNC_RSH" = "ssh -F /test/config" ]
                    [ "$MOONCAKE_RENDER_DEVICES" = "/dev/dri/renderD129 /dev/dri/renderD137" ]
                    source "$BASE_DIR/scripts/common.sh"
                    [ "$(get_test_type test_1p1d_erdma.sh)" = double ]
                    test -f "$E2E_DIR/assets/test_cat.jpg"
                    test -f "$E2E_DIR/python/toy_proxy_server.py"
                    """
                )
                self.assertFalse(any(p.is_symlink() for p in root.rglob("*")))

    def test_case_functions_do_not_leak(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "first.sh").write_text(
                "run_test() { return 0; }\nparse() { return 0; }\nLEAK=yes\n"
            )
            (root / "second.sh").write_text('run_test() { [ "${LEAK:-}" = "" ]; }\n')
            self.shell(
                f'source "{SCRIPTS}/e2e/scripts/controller.sh"; '
                f'cd "{root}"; BASE_DIR="{root}"; '
                "setup_log_directory() { :; }; "
                "execute_test first.sh; execute_test second.sh; "
                "! declare -f parse"
            )


if __name__ == "__main__":
    unittest.main()
