"""Exercise NPU release versioning without building or publishing wheels."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


REPOSITORY = Path(__file__).resolve().parents[3]


def step_named(job, name):
    return next(step for step in job["steps"] if step.get("name") == name)


class NpuReleaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = REPOSITORY / ".github/workflows/release-npu.yaml"
        cls.workflow = yaml.safe_load(path.read_text())
        cls.jobs = cls.workflow["jobs"]

    def prepare_version(self, tag, directory):
        output = directory / "outputs"
        step = step_named(self.jobs["version-stamp"], "Normalize release tag")
        result = subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", step["run"]],
            cwd=REPOSITORY,
            env={**os.environ, "RELEASE_TAG": tag, "GITHUB_OUTPUT": str(output)},
            capture_output=True,
            text=True,
            timeout=10,
        )
        values = (
            dict(line.split("=", 1) for line in output.read_text().splitlines())
            if output.exists()
            else {}
        )
        return result, values

    def test_classifies_tags_using_repository_release_policy(self):
        cases = (
            ("v0.3.14", "0.3.14", "false"),
            ("v0.3.13.post1", "0.3.13.post1", "false"),
            ("v0.3.14rc1", "0.3.14rc1", "false"),
            ("v0.3.14-rc1", "0.3.14rc1", "true"),
            ("v0.3.14-alpha1", "0.3.14a1", "true"),
            ("v0.3.14-beta1", "0.3.14b1", "true"),
            ("v0.3.14-pre1", "0.3.14rc1", "true"),
        )
        for tag, version, prerelease in cases:
            with self.subTest(tag=tag), tempfile.TemporaryDirectory() as temporary:
                result, values = self.prepare_version(tag, Path(temporary))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(values["release_tag"], tag)
                self.assertEqual(values["package_version"], version)
                self.assertEqual(values["is_prerelease"], prerelease)

    def test_invalid_tags_fail_before_exporting_outputs(self):
        for tag in (
            "0.3.14-rc1",
            "vnot-a-version",
            "v0.3.14+local",
            "v0.3.14-rc1+local",
            "v0.3.13-1",
            "v0.3.13-post1",
            "v1!0.3.14",
            "v0.3.14\ninjected=true",
        ):
            with self.subTest(tag=tag), tempfile.TemporaryDirectory() as temporary:
                result, values = self.prepare_version(tag, Path(temporary))
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(values, {})

    def test_prerelease_version_override(self):
        build = self.jobs["build"]
        step = step_named(build, "Apply pre-release wheel version")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            metadata = directory / "mooncake-wheel/pyproject.toml"
            metadata.parent.mkdir()
            metadata.write_text(
                '[project]\nname = "mooncake-transfer-engine"\nversion = "0.3.13"\n'
            )
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", step["run"]],
                cwd=directory,
                env={**os.environ, "PACKAGE_VERSION": "0.3.14rc1"},
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('version = "0.3.14rc1"', metadata.read_text())


if __name__ == "__main__":
    unittest.main()
