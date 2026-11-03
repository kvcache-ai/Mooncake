"""Offline checks for snapshot refresh and upstream failure handling."""

import copy
import datetime as dt
import importlib.util
import io
import json
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/update_pypi_downloads.py"
spec = importlib.util.spec_from_file_location("downloads", SCRIPT)
downloads = importlib.util.module_from_spec(spec)
spec.loader.exec_module(downloads)


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = json.loads(downloads.DEFAULT_PATH.read_text())
        stamp = downloads.now().isoformat()
        self.snapshot["catalog"].update(fetchedAt=stamp, status="ok")
        for package in self.snapshot["packages"]:
            for result in package["endpoints"].values():
                result.update(fetchedAt=stamp, status="ok")

    def test_fresh_snapshot_makes_no_network_requests(self):
        with patch.object(downloads, "open_url") as request:
            result = downloads.update(self.snapshot)
        request.assert_not_called()
        self.assertEqual(result["packages"], self.snapshot["packages"])

    def test_failed_endpoint_preserves_payload_and_collection_time(self):
        endpoint = self.snapshot["packages"][0]["endpoints"]["overall"]
        endpoint["fetchedAt"] = "2020-01-01T00:00:00+00:00"
        before = copy.deepcopy(endpoint)
        with patch.object(downloads, "open_url", side_effect=OSError("offline")):
            with patch.object(downloads.time, "sleep"):
                updated = downloads.update(self.snapshot)
        result = updated["packages"][0]["endpoints"]["overall"]
        self.assertEqual(result["payload"], before["payload"])
        self.assertEqual(result["fetchedAt"], before["fetchedAt"])
        self.assertEqual(result["status"], "stale")
        self.assertEqual(endpoint, before)

    def test_rate_limit_stops_further_requests(self):
        for package in self.snapshot["packages"]:
            for result in package["endpoints"].values():
                result["fetchedAt"] = "2020-01-01T00:00:00+00:00"
        error = urllib.error.HTTPError("https://pypistats.org", 429, "limit", {}, None)
        with patch.object(downloads, "open_url", side_effect=error) as request:
            with patch.object(downloads.time, "sleep"):
                updated = downloads.update(self.snapshot)
        self.assertEqual(request.call_count, 1)
        self.assertTrue(
            all(
                r["status"] == "stale"
                for p in updated["packages"]
                for r in p["endpoints"].values()
            )
        )

    def test_discovery_finds_variants_and_excludes_near_matches(self):
        html = b"<a>mooncake-transfer-engine</a>\n<a>mooncake-transfer-engine-cuda13</a>\n<a>mooncake-transfer-engineering</a>\n"
        with patch.object(downloads, "open_url", return_value=io.BytesIO(html)):
            self.assertEqual(
                downloads.discover(), [downloads.PREFIX, downloads.PREFIX + "-cuda13"]
            )

    def test_snapshot_and_validation(self):
        self.assertEqual(
            self.snapshot["catalog"]["names"],
            [p["name"] for p in self.snapshot["packages"]],
        )
        for package in self.snapshot["packages"]:
            for endpoint in downloads.ENDPOINTS:
                downloads.validate(
                    package["endpoints"][endpoint]["payload"], package["name"], endpoint
                )
        invalid = {
            "package": downloads.PREFIX,
            "data": {"last_day": -1, "last_week": 1, "last_month": 2},
        }
        with self.assertRaises(ValueError):
            downloads.validate(invalid, downloads.PREFIX, "recent")
        future = (downloads.now() + dt.timedelta(days=1)).isoformat()
        self.assertFalse(downloads.fresh({"status": "ok", "fetchedAt": future}))


if __name__ == "__main__":
    unittest.main()
