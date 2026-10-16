from pathlib import Path
import os
import re
import subprocess
import tempfile
import unittest

import yaml


REPOSITORY = Path(__file__).resolve().parents[3]
WORKFLOWS = REPOSITORY / ".github" / "workflows"

STABLE_RELEASE_WORKFLOWS = (
    "release.yaml",
    "release-cuda13.yaml",
    "release-efa.yaml",
    "release-efa-cuda13.yaml",
    "release-efa-non-cuda.yaml",
    "release-musa.yaml",
    "release-non-cuda.yaml",
    "release-npu.yaml",
    "release-rocm.yaml",
)

STABLE_TAGS = ("v0.3.14", "v1.0.0", "v0.3.12.post1", "v0.3.10.post12")
PRERELEASE_TAGS = (
    "v0.3.14rc1",
    "v0.3.14-rc1",
    "v0.3.14.rc1",
    "v0.3.14alpha1",
    "v0.3.14-alpha.1",
    "v0.3.14beta2",
    "v0.3.14-pre1",
)
NON_RELEASE_TAGS = PRERELEASE_TAGS + (
    "v0.3.14a1",
    "v0.3.14b1",
    "v0.3.14.dev1",
    "v0.3.14+local",
    "v0.3",
    "vnext",
)


def github_filter_to_regex(pattern):
    """Translate a GitHub Actions branch/tag filter pattern to a regex."""
    regex = []
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if pattern.startswith("**", index):
            regex.append(".*")
            index += 2
            continue
        if char == "*":
            regex.append("[^/]*")
        elif char in "+?":
            regex.append(char)
        elif char == "[":
            end = pattern.index("]", index)
            regex.append(pattern[index : end + 1])
            index = end
        else:
            regex.append(re.escape(char))
        index += 1
    return re.compile("".join(regex))


def tag_patterns(name):
    with (WORKFLOWS / name).open() as workflow_file:
        workflow = yaml.safe_load(workflow_file)
    triggers = workflow.get("on", workflow.get(True))
    return triggers["push"]["tags"]


def matches(patterns, tag):
    return any(github_filter_to_regex(p).fullmatch(tag) for p in patterns)


# Stable workflows that build through a reusable workflow carry the version
# check there; the others run it in their own build job.
VERSION_CHECK_SOURCES = {
    "release.yaml": "_build-wheel.yaml",
    "release-cuda13.yaml": "_build-wheel.yaml",
    "release-non-cuda.yaml": "_build-wheel.yaml",
    "release-efa.yaml": "_build-efa-wheel.yaml",
    "release-efa-cuda13.yaml": "_build-efa-wheel.yaml",
    "release-efa-non-cuda.yaml": "_build-efa-wheel.yaml",
    "release-musa.yaml": "release-musa.yaml",
    "release-npu.yaml": "release-npu.yaml",
    "release-rocm.yaml": "release-rocm.yaml",
}
VERSION_CHECK_STEP = "Check tag matches wheel version"


def version_check_script(name):
    with (WORKFLOWS / name).open() as workflow_file:
        workflow = yaml.safe_load(workflow_file)
    scripts = [
        step["run"]
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if step.get("name") == VERSION_CHECK_STEP
    ]
    if len(scripts) != 1:
        raise AssertionError(f"{name}: expected one '{VERSION_CHECK_STEP}' step")
    return scripts[0]


def run_version_check(script, tag, pyproject_version):
    with tempfile.TemporaryDirectory() as root:
        wheel_dir = Path(root) / "mooncake-wheel"
        wheel_dir.mkdir()
        (wheel_dir / "pyproject.toml").write_text(
            "[project]\n"
            'name = "mooncake-transfer-engine"\n'
            f'version = "{pyproject_version}"\n'
        )
        env = dict(os.environ, RELEASE_TAG=tag)
        return subprocess.run(
            ["bash", "-c", script],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
        )


class ReleaseTagFilterTest(unittest.TestCase):
    def test_stable_release_workflows_only_trigger_on_stable_tags(self):
        for name in STABLE_RELEASE_WORKFLOWS:
            patterns = tag_patterns(name)
            for tag in STABLE_TAGS:
                with self.subTest(workflow=name, tag=tag):
                    self.assertTrue(matches(patterns, tag))
            for tag in NON_RELEASE_TAGS:
                with self.subTest(workflow=name, tag=tag):
                    self.assertFalse(matches(patterns, tag))

    def test_pre_release_workflow_triggers_on_prerelease_tags(self):
        patterns = tag_patterns("pre-release.yaml")
        for tag in PRERELEASE_TAGS:
            with self.subTest(tag=tag):
                self.assertTrue(matches(patterns, tag))
        for tag in STABLE_TAGS:
            with self.subTest(tag=tag):
                self.assertFalse(matches(patterns, tag))


class ReleaseVersionCheckTest(unittest.TestCase):
    def test_every_stable_workflow_has_a_version_check(self):
        self.assertEqual(set(STABLE_RELEASE_WORKFLOWS), set(VERSION_CHECK_SOURCES))

    def test_matching_tag_passes(self):
        for name in sorted(set(VERSION_CHECK_SOURCES.values())):
            script = version_check_script(name)
            for tag, version in (
                ("v0.3.14", "0.3.14"),
                ("v0.3.13.post1", "0.3.13.post1"),
            ):
                with self.subTest(workflow=name, tag=tag):
                    result = run_version_check(script, tag, version)
                    self.assertEqual(
                        0, result.returncode, result.stdout + result.stderr
                    )

    def test_valid_stable_tag_with_stale_pyproject_fails(self):
        for name in sorted(set(VERSION_CHECK_SOURCES.values())):
            script = version_check_script(name)
            for tag, version in (("v0.3.14", "0.3.13"), ("v0.3.13.post1", "0.3.13")):
                with self.subTest(workflow=name, tag=tag):
                    result = run_version_check(script, tag, version)
                    self.assertNotEqual(0, result.returncode)
                    self.assertIn("does not match", result.stdout)


if __name__ == "__main__":
    unittest.main()
