import pathlib
import subprocess
import tempfile
import unittest

import yaml


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


class CodeFormatBaseTests(unittest.TestCase):
    def run_format_step(self, event, *, base_ref="", before=""):
        workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yml").read_text())
        step = next(
            step
            for step in workflow["jobs"]["clang-format"]["steps"]
            if step["name"] == "Check code format"
        )
        script = step["run"]
        context = {
            "github.event_name": event,
            "github.base_ref": base_ref,
            "github.event.before": before,
            "github.event.repository.default_branch": "main",
        }
        for expression, value in context.items():
            script = script.replace("${{ " + expression + " }}", value)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            formatter = root / "scripts/code_format.sh"
            formatter.parent.mkdir()
            # Capture the arguments passed by the real workflow shell step.
            formatter.write_text('#!/bin/bash\nprintf "%s\\n" "$@"\n')
            formatter.chmod(0o755)
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", script],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=10,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(
            result.stdout.splitlines()[1:4],
            ["--check", "--changed-lines", "--base"],
        )
        return result.stdout.splitlines()[4]

    def test_manual_run_compares_against_default_branch(self):
        self.assertEqual(self.run_format_step("workflow_dispatch"), "origin/main")

    def test_pr_compares_against_target_branch(self):
        self.assertEqual(
            self.run_format_step("pull_request", base_ref="release/v1"),
            "origin/release/v1",
        )

    def test_new_branch_push_compares_against_default_branch(self):
        self.assertEqual(self.run_format_step("push", before="0" * 40), "origin/main")

    def test_existing_branch_push_compares_against_previous_commit(self):
        previous = "a" * 40
        self.assertEqual(self.run_format_step("push", before=previous), previous)


if __name__ == "__main__":
    unittest.main()
