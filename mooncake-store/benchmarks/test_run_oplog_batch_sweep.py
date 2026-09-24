import fcntl
import importlib.util
import json
import stat
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("run_oplog_batch_sweep.py")
SPEC = importlib.util.spec_from_file_location("run_oplog_batch_sweep", MODULE_PATH)
sweep = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sweep)


class SweepTest(unittest.TestCase):
    def test_expands_matrix_and_keeps_failed_runs(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            fake = root / "fake_bench.py"
            fake.write_text(
                "#!/usr/bin/env python3\n"
                "import json, pathlib, sys\n"
                "args=dict(x[2:].split('=', 1) for x in sys.argv[1:])\n"
                "attempts=pathlib.Path(__file__).with_name('attempts')\n"
                "count=int(attempts.read_text()) if attempts.exists() else 0\n"
                "attempts.write_text(str(count + 1))\n"
                "result={'schema_version':1,'entries_per_sec':int(args['max_entries'])*100}\n"
                "open(args['output_json'],'w').write(json.dumps(result))\n"
                "raise SystemExit(7 if args['max_entries']=='8' and count < 4 else 0)\n"
            )
            fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
            matrix = root / "matrix.json"
            matrix.write_text(
                json.dumps(
                    {
                        "cases": [
                            {"name": "b1", "mode": "writer", "max_entries": 1},
                            {"name": "b8", "mode": "writer", "max_entries": 8},
                        ]
                    }
                )
            )

            exit_code = sweep.run_sweep(
                benchmark=fake,
                output_dir=root / "out",
                repeat=2,
                matrix_path=matrix,
                endpoints="127.0.0.1:2379",
            )

            self.assertEqual(exit_code, 1)
            rows = [
                json.loads(line)
                for line in (root / "out/samples.jsonl").read_text().splitlines()
            ]
            self.assertEqual(len(rows), 4)
            self.assertEqual([row["case"] for row in rows], ["b1", "b1", "b8", "b8"])
            self.assertEqual([row["repeat"] for row in rows], [0, 1, 0, 1])
            self.assertEqual([row["exit_code"] for row in rows], [0, 0, 7, 7])
            self.assertEqual(len({row["cluster_id"] for row in rows}), 4)
            manifest = json.loads((root / "out/manifest.json").read_text())
            self.assertGreater(manifest["host"]["cpu_count"], 0)
            self.assertTrue(manifest["host"]["machine"])

            resume_exit_code = sweep.run_sweep(
                benchmark=fake,
                output_dir=root / "out",
                repeat=2,
                matrix_path=matrix,
                endpoints="127.0.0.1:2379",
                resume=True,
            )
            self.assertEqual(resume_exit_code, 0)
            resumed_rows = [
                json.loads(line)
                for line in (root / "out/samples.jsonl").read_text().splitlines()
            ]
            self.assertEqual(
                [
                    (row["case"], row["repeat"], row["exit_code"])
                    for row in resumed_rows
                ],
                [
                    ("b1", 0, 0),
                    ("b1", 1, 0),
                    ("b8", 0, 7),
                    ("b8", 1, 7),
                    ("b8", 0, 0),
                    ("b8", 1, 0),
                ],
            )

            with (root / "out/.sweep.lock").open("w") as lock_stream:
                fcntl.flock(lock_stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(RuntimeError, "already running"):
                    sweep.run_sweep(
                        benchmark=fake,
                        output_dir=root / "out",
                        repeat=2,
                        matrix_path=matrix,
                        endpoints="127.0.0.1:2379",
                        resume=True,
                    )


if __name__ == "__main__":
    unittest.main()
