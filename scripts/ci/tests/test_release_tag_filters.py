from pathlib import Path
import re
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


if __name__ == "__main__":
    unittest.main()
