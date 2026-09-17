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

    def test_local_links_have_shared_mount(self):
        for suite in ("tone_tests", "rocm_tests"):
            common = SCRIPTS / suite / "scripts/common.sh"
            output = self.shell(
                f'CONTAINER_NAME=test; source "{common}"; '
                'printf "%s\\n" "${SHARED_MOUNT_ARGS[@]}"'
            )
            self.assertIn(f"{SCRIPTS}/e2e:/e2e:ro", output)

    @unittest.skipUnless(shutil.which("rsync"), "rsync is required")
    def test_remote_copy_is_self_contained(self):
        for suite in ("tone_tests", "rocm_tests"):
            with tempfile.TemporaryDirectory() as destination:
                subprocess.run(
                    [
                        "rsync",
                        "-aL",
                        "--exclude=run",
                        f"{SCRIPTS / suite}/",
                        destination,
                    ],
                    check=True,
                )
                root = Path(destination)
                self.assertFalse(any(p.is_symlink() for p in root.rglob("*")))
                self.assertTrue((root / "assets/test_cat.jpg").is_file())
                self.shell(
                    f'CONTAINER_NAME=test; source "{root}/scripts/common.sh"; '
                    '[ "${#SHARED_MOUNT_ARGS[@]}" = 0 ]'
                )

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
