import importlib.util
import io
import tempfile
import unittest
import unittest.mock
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("extract_alloc_trace.py")
SPEC = importlib.util.spec_from_file_location("extract_alloc_trace", MODULE_PATH)
extract = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(extract)


LOG_SAMPLE = """\
I0908 10:00:00.000001 1 master_service.cpp:4695] key=obj-a, value_length=65536, config=ReplicateConfig{...}, action=put_start_begin
I0908 10:00:00.000002 1 master_service.cpp:4695] key=obj, with comma, value_length=1342177280, config=ReplicateConfig{...}, action=put_start_begin
I0908 10:00:00.000003 1 master_service.cpp:4427] key=obj-c, value_length=1342177280, replica_num=1, segments=16, total_free=21474836480, largest_free=805306368, error=NO_AVAILABLE_HANDLE, action=put_start_alloc_failed
I0908 10:00:00.000004 1 master_service.cpp:6770] key=obj-a, size=65536, action=remove_object
I0908 10:00:00.000005 1 master_service.cpp:10460] key=obj, with comma, size=1342177280, replicas=1, action=evict_object
I0908 10:00:00.000006 1 master_service.cpp:1] unrelated line
"""


class ExtractAllocTraceTest(unittest.TestCase):
    def test_parses_sizes_and_events(self):
        with tempfile.TemporaryDirectory() as root:
            log = Path(root) / "master.INFO"
            log.write_text(LOG_SAMPLE)
            sizes, events = extract.parse_logs([str(log)])

        self.assertEqual(sizes, [65536, 1342177280])
        self.assertEqual(
            events,
            [
                ("put", "obj-a", 65536),
                ("put", "obj, with comma", 1342177280),
                ("remove", "obj-a", 65536),
                ("evict", "obj, with comma", 1342177280),
            ],
        )

    def test_main_writes_outputs_and_histogram(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            log = root / "master.INFO"
            log.write_text(LOG_SAMPLE)
            sizes_out = root / "sizes.txt"
            events_out = root / "events.txt"
            stderr = io.StringIO()
            with unittest.mock.patch("sys.stderr", stderr):
                rc = extract.main(
                    [str(log), "-o", str(sizes_out), "--events", str(events_out)]
                )
            self.assertEqual(rc, 0)
            size_lines = [
                line
                for line in sizes_out.read_text().splitlines()
                if not line.startswith("#")
            ]
            self.assertEqual(size_lines, ["65536", "1342177280"])
            self.assertIn("put obj-a 65536", events_out.read_text())
            self.assertIn("64K-128K", stderr.getvalue())
            self.assertIn("1G-2G", stderr.getvalue())

    def test_octave_label(self):
        self.assertEqual(extract.octave_label(65536)[0], "64K-128K")
        self.assertEqual(extract.octave_label(1342177280)[0], "1G-2G")
        self.assertEqual(extract.octave_label(1)[0], "1B-2B")

    def test_main_returns_error_without_puts(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            log = root / "master.INFO"
            log.write_text("nothing here\n")
            with unittest.mock.patch("sys.stderr", io.StringIO()):
                rc = extract.main([str(log), "-o", str(root / "sizes.txt")])
            self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
