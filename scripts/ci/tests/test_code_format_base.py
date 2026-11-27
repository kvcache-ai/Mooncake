import os
import pathlib
import subprocess
import tempfile
import unittest

import yaml


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


class CodeFormatBaseTests(unittest.TestCase):
    def run_format_step(self, event, *, base_ref="", before=""):
        workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yml").read_text())
        steps = workflow["jobs"]["fast-checks"]["steps"]
        resolve_step = next(
            step for step in steps if step.get("name") == "Resolve base ref"
        )
        format_step = next(
            step for step in steps if step.get("name") == "Check code format"
        )
        context = {
            "github.event_name": event,
            "github.base_ref": base_ref,
            "github.event.before": before,
            "github.event.repository.default_branch": "main",
        }

        def render(text):
            for expression, value in context.items():
                text = text.replace("${{ " + expression + " }}", value)
            return text

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            github_env = root / "github_env"
            github_env.touch()

            resolve_env = {
                **os.environ,
                "GITHUB_ENV": str(github_env),
                **{
                    key: render(str(value))
                    for key, value in resolve_step.get("env", {}).items()
                },
            }
            resolve_result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", render(resolve_step["run"])],
                cwd=root,
                env=resolve_env,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(
                resolve_result.returncode,
                0,
                resolve_result.stdout + resolve_result.stderr,
            )

            exported_env = dict(os.environ)
            for line in github_env.read_text().splitlines():
                key, value = line.split("=", 1)
                exported_env[key] = value

            formatter = root / "scripts/code_format.sh"
            formatter.parent.mkdir()
            # Capture the arguments passed by the real workflow shell step.
            formatter.write_text('#!/bin/bash\nprintf "%s\\n" "$@"\n')
            formatter.chmod(0o755)
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", render(format_step["run"])],
                cwd=root,
                env=exported_env,
                capture_output=True,
                text=True,
                timeout=10,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        lines = result.stdout.splitlines()
        self.assertEqual(lines[:3], ["--check", "--changed-lines", "--base"])
        return lines[3]

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
