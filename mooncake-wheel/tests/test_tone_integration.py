"""Focused tests for the T-one integration orchestrator."""

from __future__ import annotations

import base64
import importlib.util
from pathlib import Path

import pytest


def _load_tone_module():
    repository = Path(__file__).resolve().parents[2]
    script = repository / "scripts" / "ci" / "tone_integration.py"
    spec = importlib.util.spec_from_file_location("tone_integration", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tone = _load_tone_module()


def test_source_context_preserves_existing_event_priority() -> None:
    assert tone.resolve_source_context(
        "workflow_dispatch",
        "push-sha",
        "explicit-sha",
        "event-pr-sha",
        "17",
        "18",
    ) == ("explicit-sha", "17")
    assert tone.resolve_source_context(
        "pull_request", "push-sha", "", "event-pr-sha", "", "18"
    ) == ("event-pr-sha", "18")
    assert tone.resolve_source_context(
        "push", "tag-sha", "explicit-sha", "event-pr-sha", "17", "18"
    ) == ("tag-sha", "")


def test_github_payload_selectors_keep_the_expected_ci_artifact() -> None:
    assert (
        tone.select_workflow_run(
            {
                "workflow_runs": [
                    {"id": 1, "name": "Other", "path": ".github/workflows/other.yml"},
                    {"id": 2, "name": "Build & Test (Linux)", "path": "dynamic"},
                ]
            }
        )
        == "2"
    )
    assert (
        tone.select_cuda13_artifact(
            {
                "artifacts": [
                    {"id": 3, "name": "mooncake-wheel-py312"},
                    {"id": 4, "name": "mooncake-wheel-cu130-py312"},
                ]
            }
        )
        == "4"
    )


def test_ci_artifact_lookup_retries_until_the_expected_artifact_exists() -> None:
    run_responses = iter(
        [
            {"workflow_runs": []},
            {
                "workflow_runs": [
                    {
                        "id": 42,
                        "name": "Build & Test (Linux)",
                        "path": ".github/workflows/ci.yml",
                    }
                ]
            },
        ]
    )
    sleeps = []

    def fetch_json(url: str) -> dict:
        if "/actions/runs?" in url:
            return next(run_responses)
        assert "/actions/runs/42/artifacts" in url
        return {"artifacts": [{"id": 99, "name": "mooncake-cu130-py312"}]}

    artifact_id = tone.find_ci_artifact(
        "kvcache-ai/Mooncake",
        "abc123",
        fetch_json,
        attempts=2,
        sleeper=sleeps.append,
    )

    assert artifact_id == "99"
    assert sleeps == [60]


def test_tone_environment_includes_only_the_active_source_context() -> None:
    assert tone.build_env_info(
        "99",
        "kvcache-ai/Mooncake",
        branch="v1.2.0-rc1",
    ) == ("ARTIFACT_ID=99 GIT_REPO=kvcache-ai/Mooncake BRANCH=v1.2.0-rc1")
    assert (
        tone.build_env_info("99", "kvcache-ai/Mooncake", pr_number="17")
        == "ARTIFACT_ID=99 GIT_REPO=kvcache-ai/Mooncake PR_ID=17"
    )


def test_main_passes_the_tag_and_exact_testpypi_artifact_to_tone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = {
        "TONE_USER_NAME": "user",
        "TONE_USER_TOKEN": "token",
        "GITHUB_REPOSITORY": "kvcache-ai/Mooncake",
        "GITHUB_SHA": "abc123",
        "GITHUB_EVENT_NAME": "push",
        "GITHUB_REF_NAME": "v1.2.0-rc1",
        "TESTPYPI_VERSION": "1.2.0rc1",
        "TESTPYPI_ARTIFACT_ID": "99",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    captured = {}

    class FakeClient:
        def post(self, _url: str, _payload: dict) -> dict:
            raise AssertionError("mocked create and wait functions must handle T-one")

    def create_job(_post, _user, _token, sha, env_info) -> str:
        captured["sha"] = sha
        captured["env_info"] = env_info
        return "123"

    monkeypatch.setattr(tone, "JsonClient", FakeClient)
    monkeypatch.setattr(
        tone,
        "find_ci_artifact",
        lambda *_args, **_kwargs: pytest.fail(
            "TestPyPI mode must not look up an ordinary CI artifact"
        ),
    )
    monkeypatch.setattr(tone, "create_tone_job", create_job)
    monkeypatch.setattr(tone, "wait_for_tone_job", lambda *_args: None)

    assert tone.main() == 0
    assert captured == {
        "sha": "abc123",
        "env_info": (
            "ARTIFACT_ID=99 GIT_REPO=kvcache-ai/Mooncake " "BRANCH=v1.2.0-rc1"
        ),
    }


def test_create_tone_job_builds_the_expected_request() -> None:
    requests = []

    def post_json(url: str, payload: dict) -> dict:
        requests.append((url, payload))
        return {"code": 200, "data": {"id": 123}}

    job_id = tone.create_tone_job(
        post_json,
        "user",
        "token",
        "abc123",
        "ARTIFACT_ID=99 GIT_REPO=kvcache-ai/Mooncake",
        clock=lambda: 10.5,
    )

    assert job_id == "123"
    url, payload = requests[0]
    assert url.endswith("/create/")
    assert payload["name"] == "mooncake-ci-abc123"
    assert payload["env_info"] == ("ARTIFACT_ID=99 GIT_REPO=kvcache-ai/Mooncake")
    assert base64.b64decode(payload["signature"]).decode() == "user|token|10.5"


def test_tone_polling_waits_through_pending_and_running_states() -> None:
    responses = iter(
        [
            {
                "code": 200,
                "data": {"job_state": "pending", "job_second_state": ""},
            },
            {
                "code": 200,
                "data": {"job_state": "running", "job_second_state": ""},
            },
            {
                "code": 200,
                "data": {"job_state": "finished", "job_second_state": "pass"},
            },
        ]
    )
    sleeps = []

    tone.wait_for_tone_job(
        lambda _url, _payload: next(responses),
        "user",
        "token",
        "123",
        max_total_attempts=3,
        max_running_attempts=1,
        interval=30,
        clock=lambda: 10.5,
        sleeper=sleeps.append,
    )

    assert sleeps == [30, 30]


def test_tone_polling_propagates_a_failed_result() -> None:
    with pytest.raises(tone.ToneIntegrationError, match="failed or stopped"):
        tone.wait_for_tone_job(
            lambda _url, _payload: {
                "code": 200,
                "data": {
                    "job_state": "finished",
                    "job_second_state": "fail",
                },
            },
            "user",
            "token",
            "123",
            max_total_attempts=1,
            clock=lambda: 10.5,
            sleeper=lambda _delay: None,
        )
