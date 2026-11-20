#!/usr/bin/env python3
"""Check TransferEngine.set_nic_selection().

The value checks and the error for a call after initialize() need no RDMA
hardware. The per-engine policy checks need an RDMA NIC and are skipped
without one.
"""

import os
import re
import subprocess
import sys
import textwrap
import unittest

from mooncake.engine import TransferEngine

# Three engines in one process: an override of each value and one without a
# call, then a call after initialize(), which is ignored.
_PER_ENGINE_SCRIPT = textwrap.dedent(
    """
    from mooncake.engine import TransferEngine

    engines = []
    for selection in ("local", "all", None):
        engine = TransferEngine()
        if selection is not None:
            engine.set_nic_selection(selection)
        if engine.initialize("127.0.0.1", "P2PHANDSHAKE", "rdma", "") != 0:
            raise SystemExit("initialize() failed")
        engines.append(engine)
    engines[0].set_nic_selection("all")
    """
)


class SetNicSelectionTest(unittest.TestCase):
    def test_accepts_all_and_local(self) -> None:
        engine = TransferEngine()
        for value in ("all", "local", "LOCAL"):
            self.assertIsNone(engine.set_nic_selection(value))

    def test_invalid_value_raises_value_error(self) -> None:
        engine = TransferEngine()
        for value in ("bogus", ""):
            with self.assertRaisesRegex(ValueError, "nic_selection"):
                engine.set_nic_selection(value)

    def test_engines_choose_independently(self) -> None:
        # Drop variables that change the policy, the transport or where the
        # INFO logs checked below go.
        dropped = (
            "MC_RDMA_NIC_SELECTION",
            "MC_USE_TENT",
            "MC_USE_TEV1",
            "MC_FORCE_TCP",
            "MC_CUSTOM_TOPO_JSON",
            "MC_LOG_DIR",
            "GLOG_minloglevel",
        )
        env = {k: v for k, v in os.environ.items() if k not in dropped}
        proc = subprocess.run(
            [sys.executable, "-c", _PER_ENGINE_SCRIPT],
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr[-4000:])
        self.assertIn("setNicSelection() must be called before init()", proc.stderr)
        # Without an HCA, discovery falls back to TCP and initialize() still
        # succeeds, so skip the RDMA checks on the HCA count the engine logs.
        hcas = re.search(r"Found (\d+) HCAs", proc.stderr)
        if not hcas or int(hcas.group(1)) == 0:
            self.skipTest("no RDMA HCA on this host")
        # Each RDMA transport logs the policy it uses and where it came from.
        used = re.findall(r"rdma_nic_selection = (all|local) \(([^)]+)\)", proc.stderr)
        self.assertEqual(
            used,
            [
                ("local", "engine override"),
                ("all", "engine override"),
                ("all", "MC_RDMA_NIC_SELECTION"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
