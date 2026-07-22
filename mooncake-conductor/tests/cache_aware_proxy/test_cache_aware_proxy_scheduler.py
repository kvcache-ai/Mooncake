from __future__ import annotations

import copy
import unittest
from unittest.mock import AsyncMock

import httpx

from _support import (
    RecordingClientFactory,
    proxy,
    registration_success_handler,
    valid_config,
)


class SchedulerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.factory = RecordingClientFactory(registration_success_handler)
        self.runtime = proxy.ProxyRuntime(valid_config(), self.factory)
        await self.runtime.start()
        self.runtime._tokenize = AsyncMock(return_value=[1, 2, 3])

    async def asyncTearDown(self) -> None:
        await self.runtime.close()

    async def test_unique_maximum_and_unknown_instances(self) -> None:
        self.runtime.conductor.query = AsyncMock(
            return_value={
                "instances": {
                    "unknown": {"longest_matched": 999},
                    "prefill-a": {"longest_matched": 16},
                    "prefill-b": {"longest_matched": 32},
                }
            }
        )

        selected = await self.runtime.select_prefill(
            {"model": "test-model", "prompt": "hello"}, "request-1"
        )

        self.assertEqual("prefill-b", selected.config.instance_id)

    async def test_positive_ties_rotate_independent_of_response_order(self) -> None:
        query = AsyncMock(
            side_effect=[
                {
                    "instances": {
                        "prefill-b": {"longest_matched": 32},
                        "prefill-a": {"longest_matched": 32},
                    }
                },
                {
                    "instances": {
                        "prefill-a": {"longest_matched": 32},
                        "prefill-b": {"longest_matched": 32},
                    }
                },
                {
                    "instances": {
                        "prefill-b": {"longest_matched": 32},
                        "prefill-a": {"longest_matched": 32},
                    }
                },
            ]
        )
        self.runtime.conductor.query = query

        selected = []
        for index in range(3):
            result = await self.runtime.select_prefill(
                {"model": "test-model"}, f"request-{index}"
            )
            selected.append(result.config.instance_id)

        self.assertEqual(["prefill-a", "prefill-b", "prefill-a"], selected)

    async def test_all_zero_fallback_has_independent_round_robin(self) -> None:
        self.runtime.conductor.query = AsyncMock(
            return_value={
                "instances": {
                    "prefill-a": {"longest_matched": 0},
                    "prefill-b": {"longest_matched": 0},
                }
            }
        )

        selected = []
        for index in range(3):
            result = await self.runtime.select_prefill(
                {"model": "test-model"}, f"request-{index}"
            )
            selected.append(result.config.instance_id)

        self.assertEqual(["prefill-a", "prefill-b", "prefill-a"], selected)

    async def test_tokenization_failure_falls_back_without_mutating_request(
        self,
    ) -> None:
        request_data = {
            "model": "test-model",
            "prompt": "hello",
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        original = copy.deepcopy(request_data)
        self.runtime._tokenize = AsyncMock(side_effect=httpx.ReadTimeout("timeout"))
        self.runtime.conductor.query = AsyncMock()

        selected = await self.runtime.select_prefill(request_data, "request-1")

        self.assertEqual("prefill-a", selected.config.instance_id)
        self.assertEqual(original, request_data)
        self.runtime.conductor.query.assert_not_awaited()

    async def test_query_failure_falls_back(self) -> None:
        self.runtime.conductor.query = AsyncMock(
            side_effect=httpx.ReadTimeout("timeout")
        )

        first = await self.runtime.select_prefill({"model": "test-model"}, "r1")
        second = await self.runtime.select_prefill({"model": "test-model"}, "r2")

        self.assertEqual(
            ["prefill-a", "prefill-b"],
            [first.config.instance_id, second.config.instance_id],
        )

    async def test_unusable_query_responses_fall_back(self) -> None:
        responses = [
            {},
            {"instances": []},
            {"instances": {"unknown": {"longest_matched": 100}}},
            {"instances": {"prefill-a": {"longest_matched": True}}},
            {"instances": {"prefill-a": {"longest_matched": -1}}},
            {"instances": {"prefill-a": {"longest_matched": "16"}}},
        ]
        self.runtime.conductor.query = AsyncMock(side_effect=responses)

        selected = []
        for index in range(len(responses)):
            result = await self.runtime.select_prefill(
                {"model": "test-model"}, f"request-{index}"
            )
            selected.append(result.config.instance_id)

        self.assertEqual(
            [
                "prefill-a",
                "prefill-b",
                "prefill-a",
                "prefill-b",
                "prefill-a",
                "prefill-b",
            ],
            selected,
        )

    async def test_selection_and_fallback_logs_are_request_scoped(self) -> None:
        self.runtime.conductor.query = AsyncMock(
            side_effect=[
                {
                    "instances": {
                        "prefill-a": {"longest_matched": 16},
                        "prefill-b": {"longest_matched": 32},
                    }
                },
                httpx.ReadTimeout("timeout"),
            ]
        )

        with self.assertLogs(proxy.logger, level="INFO") as captured:
            await self.runtime.select_prefill({"model": "test-model"}, "hit-id")
            await self.runtime.select_prefill({"model": "test-model"}, "fallback-id")

        output = "\n".join(captured.output)
        self.assertIn("request_id=hit-id", output)
        self.assertIn("cache_candidates=", output)
        self.assertIn("selected_instance=prefill-b", output)
        self.assertIn("request_id=fallback-id", output)
        self.assertIn("reason=conductor_query_failed", output)


if __name__ == "__main__":
    unittest.main()
