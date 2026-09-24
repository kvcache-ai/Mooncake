# Copyright 2026 Alibaba Cloud and its affiliates
# Licensed under the Apache License, Version 2.0.
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from recorder import RpcTraceWriter  # noqa: E402


class RecorderTest(unittest.TestCase):
    def test_multiclient_write_and_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rpc.jsonl"
            with RpcTraceWriter(path, metadata={"clock": "logical"}) as writer:
                start = writer.record(
                    timestamp_us=100,
                    client_id="a",
                    op="BatchPutStart",
                    keys=["shared", "private"],
                    value_sizes=[4096, 8192],
                )
                end = writer.record(
                    timestamp_us=200,
                    client_id="a",
                    op="BatchPutEnd",
                    keys=["shared", "private"],
                    put_start=start,
                )
                writer.record(
                    timestamp_us=300,
                    client_id="b",
                    op="BatchGetReplicaList",
                    keys=["shared"],
                    depends_on=[end],
                )
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(rows[0]["time_unit"], "us")
            self.assertEqual(rows[2]["put_start"], rows[1]["id"])
            self.assertEqual(rows[3]["depends_on"], [rows[2]["id"]])
            self.assertEqual(rows[3]["keys"], rows[1]["keys"][:1])
            with self.assertRaises(FileExistsError):
                RpcTraceWriter(path)

    def test_rejected_events_do_not_corrupt_writer_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rpc.jsonl"
            with RpcTraceWriter(path) as writer:
                start = writer.record(
                    timestamp_us=10,
                    client_id="a",
                    op="BatchPutStart",
                    keys=["k"],
                    value_sizes=[1],
                )
                bad = [
                    dict(timestamp_us=9, client_id="a", op="BatchExistKey", keys=["k"]),
                    dict(
                        timestamp_us=True, client_id="a", op="BatchExistKey", keys=["k"]
                    ),
                    dict(
                        timestamp_us=10,
                        client_id="a",
                        op="BatchExistKey",
                        keys=["k"],
                        depends_on=["future"],
                    ),
                    dict(
                        timestamp_us=11,
                        client_id="b",
                        op="BatchPutEnd",
                        keys=["k"],
                        put_start=start,
                    ),
                ]
                for event in bad:
                    with self.assertRaises(ValueError):
                        writer.record(**event)
                writer.record(
                    timestamp_us=12,
                    client_id="a",
                    op="BatchPutRevoke",
                    keys=["k"],
                    put_start=start,
                )
            self.assertEqual(len(path.read_text().splitlines()), 3)

    def test_unfinished_write_is_reported_and_file_is_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            writer = RpcTraceWriter(Path(directory) / "rpc.jsonl")
            writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutStart",
                keys=["k"],
                value_sizes=[1],
            )
            with self.assertRaisesRegex(ValueError, "unfinished"):
                writer.close()
            self.assertTrue(writer._file.closed)


if __name__ == "__main__":
    unittest.main()
