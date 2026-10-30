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

    def test_store_rs_cases_are_an_explicit_tone_lane(self):
        runner = SCRIPTS / "tone_tests" / "scripts" / "run_test.sh"
        regular = self.shell(f'"{runner}" list-tests')
        store_rs = self.shell(f'"{runner}" list-tests STORE_RS')

        self.assertEqual(
            store_rs.splitlines(),
            [
                "test_hicache_storage_mooncake_store_rs.sh",
                "test_vllm_mooncake_store_rs.sh",
            ],
        )
        self.assertNotIn("mooncake_store_rs", regular)

    def test_store_rs_controller_uses_the_matching_framework(self):
        controller = SCRIPTS / "e2e" / "scripts" / "controller.sh"
        output = self.shell(
            f"""All_TEST_SCRIPTS_SGLANG=(regular-sglang.sh)
            All_TEST_SCRIPTS_VLLM=(regular-vllm.sh)
            All_TEST_SCRIPTS_STORE_RS_SGLANG=(store-rs-sglang.sh)
            All_TEST_SCRIPTS_STORE_RS_VLLM=(store-rs-vllm.sh)
            source "{controller}"
            [ "$(get_framework_from_test_name store-rs-sglang.sh)" = SGLANG ]
            [ "$(get_framework_from_test_name store-rs-vllm.sh)" = VLLM ]
            run_single_test() {{ printf '%s\n' "$1"; }}
            run_store_rs_tests
            """
        )
        self.assertEqual(
            output.splitlines(), ["store-rs-sglang.sh", "store-rs-vllm.sh"]
        )

    def test_store_rs_tone_job_redirects_the_existing_run_all_command(self):
        runner = SCRIPTS / "tone_tests" / "scripts" / "run_test.sh"
        output = self.shell(
            f'''RUN_STORE_RS_TESTS=true LOCAL_IP=192.0.2.1 REMOTE_IP=192.0.2.2
            source "{runner}"
            main() {{ printf '%s\\n' "$@"; }}
            set -- run-all SGLANG
            if [ "$RUN_STORE_RS_TESTS" = true ] && [ "$1" = run-all ]; then
                main run-store-rs
            else
                main "$@"
            fi'''
        )
        self.assertEqual(output.splitlines(), ["run-store-rs"])

    def test_store_rs_e2e_job_selects_the_exact_combined_wheel(self):
        repository_root = SCRIPTS.parent
        nightly_workflow = (
            repository_root / ".github/workflows/nightly.yml"
        ).read_text()
        integration_workflow = (
            repository_root / ".github/workflows/integration-test.yml"
        ).read_text()
        wheel_workflow = (
            repository_root / ".github/workflows/_build-wheel.yaml"
        ).read_text()

        self.assertIn("build-wheel-store-rs-tone:", nightly_workflow)
        self.assertIn("artifact-prefix: tone-store-rs-wheel", nightly_workflow)
        self.assertIn(
            "artifact_name: tone-store-rs-wheel-py312", nightly_workflow
        )
        self.assertIn("run-store-rs: true", nightly_workflow)
        self.assertIn("tone-store-rs-integration-tent:", nightly_workflow)
        self.assertIn("store-rs-transport-backend: classic-te", nightly_workflow)
        self.assertIn("store-rs-transport-backend: tent", nightly_workflow)
        self.assertNotIn("artifact-prefix: nightly-store-rs", nightly_workflow)
        self.assertIn("RUN_STORE_RS_TESTS=true", integration_workflow)
        self.assertIn(
            "MC_STORE_RS_TRANSPORT_BACKEND=${store_rs_backend}",
            integration_workflow,
        )
        self.assertIn("use-store-rs:", wheel_workflow)
        self.assertIn("default: false", wheel_workflow)


if __name__ == "__main__":
    unittest.main()
