"""Structural contract for the TE + Store + Master dashboard."""

from __future__ import annotations

import json
import unittest
from pathlib import Path


DASHBOARD = (
    Path(__file__).resolve().parents[1]
    / "grafana"
    / "dashboards"
    / "mooncake-core.json"
)
FORBIDDEN_METRICS = (
    "master_batch_get_replica_list_successful_bytes_total",
    "master_batch_put_end_successful_bytes_total",
)


class CoreDashboardContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
        cls.panels = cls.dashboard["panels"]
        cls.targets = [
            (panel, target)
            for panel in cls.panels
            for target in panel.get("targets", [])
        ]

    def test_identity_and_sections(self) -> None:
        self.assertEqual(self.dashboard["uid"], "mooncake-core")
        self.assertEqual(self.dashboard["schemaVersion"], 39)
        self.assertEqual(self.dashboard["time"], {"from": "now-1h", "to": "now"})
        rows = [panel["title"] for panel in self.panels if panel["type"] == "row"]
        self.assertEqual(
            rows,
            [
                "Overview",
                "Store",
                "TE task results and latency",
                "TE request rate and throughput",
                "Master",
            ],
        )
        self.assertEqual(len(self.panels), len(rows) + 43)

    def test_panel_ids_and_grid_do_not_overlap(self) -> None:
        ids = [panel["id"] for panel in self.panels]
        self.assertEqual(len(ids), len(set(ids)))
        occupied = set()
        for panel in self.panels:
            pos = panel["gridPos"]
            self.assertGreaterEqual(pos["x"], 0)
            self.assertGreater(pos["w"], 0)
            self.assertLessEqual(pos["x"] + pos["w"], 24)
            self.assertGreater(pos["h"], 0)
            cells = {
                (x, y)
                for x in range(pos["x"], pos["x"] + pos["w"])
                for y in range(pos["y"], pos["y"] + pos["h"])
            }
            self.assertFalse(cells & occupied, panel["title"])
            occupied |= cells

    def test_targets_have_queries_and_unique_refs(self) -> None:
        for panel, _target in self.targets:
            refs = [target["refId"] for target in panel["targets"]]
            self.assertEqual(len(refs), len(set(refs)), panel["title"])
        for panel, target in self.targets:
            self.assertTrue(target["expr"].strip(), panel["title"])
            if "rate(" in target["expr"]:
                self.assertIn("$__rate_interval", target["expr"])

    def test_variables_are_multi_value_and_scoped(self) -> None:
        variables = {
            item["name"]: item for item in self.dashboard["templating"]["list"]
        }
        self.assertEqual(
            set(variables), {"DS_PROMETHEUS", "job", "instance", "op_name"}
        )
        for name in ("job", "instance", "op_name"):
            self.assertTrue(variables[name]["multi"])
            self.assertTrue(variables[name]["includeAll"])
        self.assertEqual(variables["job"]["query"], "label_values(up, job)")
        self.assertEqual(
            variables["instance"]["query"], 'label_values(up{job=~"$job"}, instance)'
        )
        self.assertEqual(
            variables["op_name"]["query"],
            'label_values({__name__=~"mooncake_transfer_(read|write)_operation_count"}, op_name)',
        )

    def test_all_queries_preserve_component_scope(self) -> None:
        for panel, target in self.targets:
            if target["expr"].startswith("up{"):
                continue
            self.assertIn('job=~"$job"', target["expr"], panel["title"])
            self.assertIn('instance=~"$instance"', target["expr"], panel["title"])

    def test_store_queries_preserve_operation_name(self) -> None:
        store_titles = {
            "Recorded read operations/s",
            "Recorded write operations/s",
            "Read logical payload bytes/s",
            "Write logical payload bytes/s",
            "Read client-operation p99",
            "Write client-operation p99",
        }
        for panel, target in self.targets:
            if panel["title"] in store_titles:
                self.assertIn('op_name=~"$op_name"', target["expr"])

    def test_overview_and_rate_units_cover_rfc_signals(self) -> None:
        panels = {panel["title"]: panel for panel in self.panels}
        for direction in ("Read", "Write"):
            self.assertIn(f"{direction} completed throughput", panels)
            self.assertEqual(
                panels[f"{direction} completed throughput"]["fieldConfig"]["defaults"][
                    "unit"
                ],
                "Bps",
            )
            self.assertIn(f"{direction} terminal observations/s", panels)
        self.assertEqual(
            panels["Active clients"]["fieldConfig"]["defaults"]["unit"],
            "suffix:clients",
        )
        self.assertEqual(
            panels["BatchGetReplicaList items/s"]["fieldConfig"]["defaults"]["unit"],
            "suffix:items/s",
        )
        self.assertEqual(
            panels["Evicted keys/s"]["fieldConfig"]["defaults"]["unit"], "suffix:keys/s"
        )
        self.assertEqual(
            panels["Read terminal task outcomes/s"]["fieldConfig"]["defaults"]["unit"],
            "suffix:tasks/s",
        )

    def test_target_and_heartbeat_states_remain_per_instance(self) -> None:
        panels = {panel["title"]: panel for panel in self.panels}
        expected = {
            "Scrape target status": 'up{job=~"$job",instance=~"$instance"}',
            "Latest heartbeat status": 'mooncake_client_master_heartbeat_status_ok{job=~"$job",instance=~"$instance"}',
            "Heartbeat observation age": 'time() - mooncake_client_master_heartbeat_observation_timestamp_seconds{job=~"$job",instance=~"$instance"}',
            "Active clients": 'master_active_clients{job=~"$job",instance=~"$instance"}',
        }
        for title, expr in expected.items():
            self.assertIn(title, panels)
            self.assertTrue(
                any(target["expr"] == expr for target in panels[title]["targets"])
            )
            if title != "Active clients":
                stat_panel = next(
                    panel
                    for panel in self.panels
                    if panel["title"] == title and panel["type"] == "stat"
                )
                self.assertEqual(
                    stat_panel["fieldConfig"]["defaults"]["noValue"], "Unknown"
                )

    def test_master_batch_requests_include_combined_non_success(self) -> None:
        for panel in self.panels:
            if panel["title"].startswith("Batch") and panel["title"].endswith(
                "requests/s"
            ):
                legends = [target["legendFormat"] for target in panel["targets"]]
                self.assertTrue(
                    any("non-successful" in legend for legend in legends),
                    panel["title"],
                )

    def test_latency_and_non_success_ratio_contracts(self) -> None:
        for panel, target in self.targets:
            if (
                "latency" in panel["title"].lower()
                or "p50" in panel["title"]
                or "p95" in panel["title"]
                or "p99" in panel["title"]
            ):
                self.assertIn("histogram_quantile", target["expr"])
                self.assertIn("/ 1000", target["expr"])
            if "non-success ratio" in panel["title"]:
                self.assertIn("clamp_min", target["expr"])
                self.assertIn("> 0", target["expr"])

    def test_aggregated_stats_do_not_reference_dropped_labels(self) -> None:
        for panel in self.panels:
            if panel["type"] != "stat":
                continue
            for target in panel["targets"]:
                if any(
                    function in target["expr"]
                    for function in ("sum(", "sum by", "min(", "max(", "count(")
                ):
                    self.assertNotIn("{{job}}", target["legendFormat"], panel["title"])
                    self.assertNotIn(
                        "{{instance}}", target["legendFormat"], panel["title"]
                    )

    def test_absent_master_metrics_are_not_used(self) -> None:
        serialized = json.dumps(self.dashboard)
        for metric in FORBIDDEN_METRICS:
            self.assertNotIn(metric, serialized)

    def test_one_unit_per_panel_and_no_private_labels(self) -> None:
        for panel in self.panels:
            if panel["type"] == "row":
                continue
            self.assertIn("unit", panel["fieldConfig"]["defaults"], panel["title"])
            serialized = json.dumps(panel)
            for forbidden in (
                "request_id",
                "segment_id",
                "peer_address",
                "raw_pointer",
            ):
                self.assertNotIn(forbidden, serialized)

    def test_overview_stats_use_instant_queries(self) -> None:
        # Range queries with lastNotNull keep showing stale values after
        # heartbeat data disappears; instant queries go blank instead.
        for panel in self.panels:
            if panel["type"] != "stat":
                continue
            for target in panel["targets"]:
                self.assertTrue(target.get("instant"), panel["title"])
                self.assertFalse(target.get("range"), panel["title"])

    def test_blank_stats_report_no_data(self) -> None:
        for panel in self.panels:
            if panel["type"] != "stat":
                continue
            no_value = panel["fieldConfig"]["defaults"].get("noValue")
            self.assertIn(no_value, ("Unknown", "No data"), panel["title"])
            if "p99" in panel["title"]:
                self.assertEqual(no_value, "No data", panel["title"])

    def test_timeseries_colors_use_positional_palette(self) -> None:
        # palette-classic-by-name assigns the same colour to identically
        # named series across panels; positional palette keeps them apart.
        for panel in self.panels:
            if panel["type"] != "timeseries":
                continue
            color = panel["fieldConfig"]["defaults"]["color"]
            self.assertEqual(color["mode"], "palette-classic", panel["title"])

    def test_transfer_engine_is_named_te_consistently(self) -> None:
        serialized = json.dumps(self.dashboard)
        self.assertNotIn("Classic TE", serialized)


if __name__ == "__main__":
    unittest.main()
