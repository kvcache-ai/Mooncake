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

    def test_normalizes_stable_and_prerelease_tags(self):
        cases = (
            ("v0.3.14", "0.3.14", "false"),
            ("v0.3.13.post1", "0.3.13.post1", "false"),
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
            "v0.3.14.dev1",
            "v1!0.3.14",
            "v0.3.14\ninjected=true",
        ):
            with self.subTest(tag=tag), tempfile.TemporaryDirectory() as temporary:
                result, values = self.prepare_version(tag, Path(temporary))
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(values, {})

    def test_build_uses_validated_tag_for_push_and_manual_runs(self):
        triggers = self.workflow.get("on", self.workflow.get(True))
        self.assertEqual(triggers["push"]["tags"], ["v*"])
        self.assertTrue(triggers["workflow_dispatch"]["inputs"]["tag"]["required"])
        prepare = step_named(self.jobs["version-stamp"], "Normalize release tag")
        self.assertEqual(
            prepare["env"]["RELEASE_TAG"],
            "${{ github.event_name == 'workflow_dispatch' && inputs.tag || github.ref_name }}",
        )
        build = self.jobs["build"]
        self.assertEqual(build["needs"], "version-stamp")
        self.assertNotIn("if", build)
        self.assertEqual(
            step_named(build, "Checkout code")["with"]["ref"],
            "refs/tags/${{ needs.version-stamp.outputs.release_tag }}",
        )

    def test_prerelease_version_override_runs_before_packaging(self):
        build = self.jobs["build"]
        step = step_named(build, "Apply pre-release wheel version")
        self.assertEqual(
            step["if"], "${{ needs.version-stamp.outputs.is_prerelease == 'true' }}"
        )
        self.assertEqual(
            step["env"]["PACKAGE_VERSION"],
            "${{ needs.version-stamp.outputs.package_version }}",
        )
        self.assertLess(
            build["steps"].index(step),
            build["steps"].index(step_named(build, "Build Python wheel")),
        )
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

    def test_github_prereleases_are_separate_from_production_pypi(self):
        github = self.jobs["publish-release"]
        self.assertEqual(set(github["needs"]), {"version-stamp", "build"})
        self.assertNotIn("environment", github)
        self.assertNotIn("if", github)
        upload = step_named(github, "Upload wheels to GitHub Release")["with"]
        self.assertEqual(
            upload["tag_name"], "${{ needs.version-stamp.outputs.release_tag }}"
        )
        self.assertEqual(
            upload["prerelease"],
            "${{ needs.version-stamp.outputs.is_prerelease == 'true' }}",
        )
        self.assertIn("is_prerelease", upload["make_latest"])
        self.assertTrue(upload["fail_on_unmatched_files"])
        self.assertFalse(
            any("pypi-publish" in step.get("uses", "") for step in github["steps"])
        )
        pypi = self.jobs["publish-pypi"]
        self.assertEqual(set(pypi["needs"]), {"version-stamp", "publish-release"})
        self.assertEqual(pypi["environment"], "pypi")
        self.assertEqual(
            pypi["if"],
            "${{ github.repository == 'kvcache-ai/Mooncake' && needs.version-stamp.outputs.is_prerelease == 'false' }}",
        )
        self.assertEqual(pypi["permissions"]["id-token"], "write")
        self.assertEqual(
            step_named(pypi, "Publish package to PyPI")["with"]["packages-dir"],
            "mooncake-wheel/dist-release/",
        )


if __name__ == "__main__":
    unittest.main()
