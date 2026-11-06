#!/usr/bin/env python3
"""Regression tests for cluster topology generation and latency matching."""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate_cluster_topology as topology  # noqa: E402


def endpoint(latency, src_dev="src0", dst_dev="dst0", src_numa=0, dst_numa=1):
    return {
        "src_dev": src_dev,
        "dst_dev": dst_dev,
        "src_numa": src_numa,
        "dst_numa": dst_numa,
        "bandwidth": 10.0,
        "latency": latency,
    }


class BuildPartitionMapTest(unittest.TestCase):
    def test_groups_finite_latencies_by_numa_pair(self):
        pairs = [endpoint(0), endpoint(2.5), endpoint(3, src_numa=1, dst_numa=0)]
        self.assertEqual(
            topology.build_partition_map(pairs),
            {"0-1": pairs[:2], "1-0": pairs[2:]},
        )

    def test_skips_invalid_latencies(self):
        for latency in (None, float("nan"), float("inf"), float("-inf")):
            with self.subTest(latency=latency):
                self.assertEqual(topology.build_partition_map([endpoint(latency)]), {})

    def test_skips_missing_latency(self):
        pair = endpoint(1.0)
        del pair["latency"]
        self.assertEqual(topology.build_partition_map([pair]), {})


class ProcessHostPairTest(unittest.TestCase):
    def test_matches_valid_endpoints_and_retains_raw_measurements(self):
        pairs = [
            endpoint(4, "src0", "dst0"),
            endpoint(1, "src0", "dst1"),
            endpoint(2, "src1", "dst0"),
            endpoint(5, "src1", "dst1"),
            endpoint(None, "src2", "dst2"),
        ]
        original = [pair.copy() for pair in pairs]
        record = {"endpoints": pairs}

        topology.process_host_pair(record)

        self.assertEqual(record["partition_matchings"], {"0-1": [pairs[1], pairs[2]]})
        self.assertEqual(record["endpoints"], original)

    def test_all_invalid_latencies_produce_empty_matchings(self):
        pairs = [
            endpoint(latency)
            for latency in (None, float("nan"), float("inf"), float("-inf"))
        ]
        missing = endpoint(1.0)
        del missing["latency"]
        pairs.append(missing)
        record = {"endpoints": pairs}

        topology.process_host_pair(record)

        self.assertEqual(record["partition_matchings"], {})
        self.assertIs(record["endpoints"], pairs)
        self.assertIsNone(record["endpoints"][0]["latency"])


class GenerateTopologyTest(unittest.TestCase):
    def test_saves_measurements_when_latency_test_fails(self):
        def ssh_output(host, port, command):
            if command == "cat /etc/machine-id":
                return f"{host}-id\n"
            if command.startswith("ibv_devices"):
                return "src0\nsrc1\n" if host == "source" else "dst0\n"
            if command.startswith("cat /sys/class/infiniband/"):
                return "0\n" if host == "source" else "1\n"
            if "nohup" in command:
                return ""
            if "ib_write_bw" in command:
                return "1 1000 0.0 10.0 0.0\n"
            if "ib_read_lat" in command:
                if "--ib-dev=src1" in command:
                    return "Latency test failed\n"
                return "1 1000 0.0 0.0 0.0 2.5\n"
            self.fail(f"Unexpected command on {host}:{port}: {command}")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cluster-topology.json"
            argv = [
                "generate_cluster_topology.py",
                "--src-host",
                "source",
                "--dst-host",
                "destination",
                "--file",
                str(path),
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(topology, "ssh_exec", side_effect=ssh_output),
                mock.patch.object(topology.time, "sleep"),
            ):
                topology.main()
            results = topology.load_results(path)

        valid = endpoint(2.5)
        failed = endpoint(None, src_dev="src1")
        self.assertEqual(
            results,
            [
                {
                    "src_host": "source-id",
                    "dst_host": "destination-id",
                    "endpoints": [valid, failed],
                    "partition_matchings": {"0-1": [valid]},
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
