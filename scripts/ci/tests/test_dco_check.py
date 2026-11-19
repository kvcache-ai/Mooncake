"""Run the DCO workflow's shell step against real, isolated Git histories."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


REPOSITORY = Path(__file__).resolve().parents[3]


class DCOCheckTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workflow = yaml.safe_load(
            (REPOSITORY / ".github/workflows/dco-check.yml").read_text()
        )
        cls.script = next(
            step["run"]
            for step in workflow["jobs"]["dco"]["steps"]
            if step["name"] == "Check DCO sign-off"
        )

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.environment = {
            **{
                key: value
                for key, value in os.environ.items()
                if not key.startswith("GIT_")
            },
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_AUTHOR_NAME": "Test Author",
            "GIT_AUTHOR_EMAIL": "author@example.com",
            "GIT_COMMITTER_NAME": "Test Committer",
            "GIT_COMMITTER_EMAIL": "committer@example.com",
        }
        self.git("init", "--bare", "--quiet", ".")
        self.tree = self.git("mktree", input="")
        self.base = self.commit("Unsigned base", parents=[])

    def git(self, *arguments, input=None):
        return subprocess.run(
            ["git", *arguments],
            input=input,
            cwd=self.directory,
            env=self.environment,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()

    def commit(self, message, parents=None):
        arguments = ["commit-tree", self.tree]
        for parent in [self.base] if parents is None else parents:
            arguments.extend(["-p", parent])
        return self.git(*arguments, input=message)

    def check(self, head, *, base=None, success=True):
        result = subprocess.run(
            ["bash", "-c", self.script],
            cwd=self.directory,
            env={
                **self.environment,
                "BASE_SHA": self.base if base is None else base,
                "HEAD_SHA": head,
            },
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)
        if not success:
            self.assertNotIn("All non-merge commits have valid", result.stdout)
        return result

    def test_signed_author_passes_with_different_committer(self):
        self.check(
            self.commit("Change\n\nSigned-off-by: Test Author <author@example.com>")
        )

    def test_multiple_signoffs_and_unicode_name(self):
        self.environment["GIT_AUTHOR_NAME"] = "测试作者"
        self.check(
            self.commit(
                "Change\n\nSigned-off-by: Someone Else <other@example.com>\n"
                "Signed-off-by: 测试作者 <author@example.com>"
            )
        )

    def test_unsigned_commits_are_all_reported_even_with_signed_tip(self):
        first = self.commit("First unsigned")
        second = self.commit("Second unsigned", parents=[first])
        head = self.commit(
            "Signed tip\n\nSigned-off-by: Test Author <author@example.com>",
            parents=[second],
        )
        result = self.check(head, success=False)
        for sha in (first, second):
            self.assertIn(f"Commit {sha} is missing", result.stdout)

    def test_mismatched_or_malformed_signoffs_fail(self):
        for identity in (
            "Someone Else <author@example.com>",
            "Test Author <other@example.com>",
            "Test Author author@example.com",
            "Test Author <author@example.com> extra text",
        ):
            with self.subTest(identity=identity):
                self.check(
                    self.commit(f"Change\n\nSigned-off-by: {identity}"), success=False
                )

    def test_body_text_is_not_a_trailer(self):
        self.check(
            self.commit(
                "Change\n\nSigned-off-by: Test Author <author@example.com>\n\n"
                "This is body text after the example sign-off."
            ),
            success=False,
        )

    def test_invalid_author_email_fails_even_when_signoff_matches(self):
        self.environment["GIT_AUTHOR_EMAIL"] = "invalid-email"
        self.check(
            self.commit("Change\n\nSigned-off-by: Test Author <invalid-email>"),
            success=False,
        )

    def test_merge_commit_is_skipped_but_its_unsigned_parent_is_checked(self):
        signed = self.commit(
            "Change\n\nSigned-off-by: Test Author <author@example.com>"
        )
        self.check(self.commit("Unsigned merge", parents=[self.base, signed]))
        unsigned = self.commit("Unsigned branch")
        result = self.check(
            self.commit("Unsigned merge", parents=[signed, unsigned]), success=False
        )
        self.assertIn(f"Commit {unsigned} is missing", result.stdout)

    def test_unsigned_bot_is_not_exempt(self):
        self.environment["GIT_AUTHOR_NAME"] = "dependabot[bot]"
        self.environment["GIT_AUTHOR_EMAIL"] = (
            "dependabot[bot]@users.noreply.github.com"
        )
        self.check(self.commit("Bot change"), success=False)

    def test_unavailable_base_or_head_fails(self):
        self.check(self.base, base="0" * 40, success=False)
        self.check("0" * 40, success=False)

    def test_empty_range_passes(self):
        self.check(self.base)


if __name__ == "__main__":
    unittest.main()
