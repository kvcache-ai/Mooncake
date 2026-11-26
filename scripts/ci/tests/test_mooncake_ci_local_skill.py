"""Keep the local CI skill pointed at files and flags that exist in this tree."""

import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[3]
SKILL_DIR = REPO / ".claude" / "skills" / "mooncake-ci-local"
SKILL_MD = SKILL_DIR / "SKILL.md"
EXAMPLE_MD = SKILL_DIR / "examples" / "minimal.md"

# Commands and directories removed with scripts/run_ci_test.sh.
ABSENT = (
    "run_ci_test.sh",
    "build-ci-local",
    "build-wheel-local",
    "run-ci-logs",
    "check-prerequisites.sh",
)
ABSENT_OK = "not in this repository"

CALLERS = (
    REPO / "README.md",
    REPO / "docs" / "source" / "getting_started" / "quick-start.md",
    REPO / ".github" / "pull_request_template.md",
    REPO / ".claude-plugin" / "marketplace.json",
    SKILL_DIR / ".claude-plugin" / "plugin.json",
    EXAMPLE_MD,
)

PATH_RE = re.compile(
    r"(?<![A-Za-z0-9_./-])(?:\./)?"
    r"((?:[\w.-]+/)+[\w.-]+\.(?:sh|py|yml|yaml|md|txt|json))"
)
FENCE_RE = re.compile(r"```(?:bash|sh|text)?\n(.*?)```", re.S)
# Outputs created by a run. They are not source files.
GENERATED_PREFIXES = ("build/", "test_env/")


class MooncakeCiLocalSkillTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.skill = SKILL_MD.read_text()
        cls.example = EXAMPLE_MD.read_text()
        cls.ci = (REPO / ".github" / "workflows" / "ci.yml").read_text()
        cls.nightly = (REPO / ".github" / "workflows" / "nightly.yml").read_text()

    def test_frontmatter_names_the_skill(self):
        self.assertTrue(self.skill.startswith("---\n"))
        self.assertIn("\nname: mooncake-ci-local\n", self.skill)
        description = self.skill.split("---", 2)[1]
        self.assertIn("description:", description)
        self.assertLess(len(description), 1024)
        self.assertNotIn("run_ci_test.sh", description)

    def test_retired_runner_is_not_a_command(self):
        self.assertFalse((REPO / "scripts" / "run_ci_test.sh").exists())
        self.assertFalse((SKILL_DIR / "scripts" / "check-prerequisites.sh").exists())
        for label, text in ("skill", self.skill), ("example", self.example):
            for fence in FENCE_RE.findall(text):
                for token in ABSENT:
                    self.assertNotIn(
                        token, fence, f"{label} command still calls {token}"
                    )
        for path in CALLERS:
            text = path.read_text()
            for token in ABSENT:
                self.assertNotIn(token, text, f"{path} still mentions {token}")
        for line in self.skill.splitlines():
            if not any(token in line for token in ABSENT):
                continue
            self.assertIn(ABSENT_OK, line)
            self.assertTrue(line.startswith("Do not "))

    def test_skill_paths_exist(self):
        missing = []
        for text in (self.skill, self.example):
            for match in PATH_RE.findall(text):
                if match.startswith(GENERATED_PREFIXES):
                    continue
                if any(token in match for token in ABSENT):
                    continue
                if (REPO / match).exists() or (SKILL_DIR / match).exists():
                    continue
                missing.append(match)
        self.assertEqual(missing, [])

    def test_copied_ci_flags_still_match_the_workflow(self):
        copied = (
            "ruff==0.6.9",
            "crate-ci/typos@v1.30.2",
            "pyright==1.1.411",
            "pytest==8.3.5",
            "./scripts/code_format.sh --check --changed-lines --base",
            "label-exclude: '^nvlink_gpu$'",
            "scripts/ci/run_store_rs_smoke.sh",
            "scripts/ci/run_store_api_smoke.sh",
            "scripts/ci/run_rpc_smoke.sh",
            "scripts/ci/run_ssd_offload_smoke.sh",
            "scripts/ci/run_transfer_engine_rust_smoke.sh",
            "bash scripts/test_installation.sh",
        )
        for token in copied:
            self.assertIn(token, self.ci, token)
            if token == "label-exclude: '^nvlink_gpu$'":
                self.assertIn("CTEST_LABEL_EXCLUDE='^nvlink_gpu$'", self.skill)
            else:
                self.assertIn(token, self.skill, token)
        self.assertIn("scripts/run_tests.sh", self.nightly)
        self.assertIn("scripts/run_tests.sh", self.skill)
        self.assertIn("nightly.yml", self.skill)


if __name__ == "__main__":
    unittest.main()
