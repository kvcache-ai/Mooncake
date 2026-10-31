"""Exercise impact selection and real CMake/CTest groups without native SDKs."""

import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "select_test_components", ROOT / "scripts/ci/select_test_components.py"
)
SELECTOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SELECTOR)


class ComponentSelectionTest(unittest.TestCase):
    def select(self, *paths):
        return SELECTOR.select_components(paths)

    def test_te_changes_stop_at_client_and_pg_not_master(self):
        for path in (
            "mooncake-transfer-engine/src/transfer_engine.cpp",
            "mooncake-transfer-engine/src/transport/tcp_transport/tcp_transport.cpp",
            "mooncake-transfer-engine/tests/tcp_transport_test.cpp",
            "mooncake-transfer-engine/tent/src/runtime/runtime.cpp",
        ):
            with self.subTest(path=path):
                components = self.select(path)
                self.assertIn("te", components)
                self.assertIn("store-client", components)
                self.assertNotIn("store-master", components)
                outputs = SELECTOR.selection_outputs(components)
                self.assertEqual(outputs["te_tests"], "true")
                # PG CPU tests run over the TE TCP transport.
                self.assertEqual(outputs["pg_tests"], "true")
                self.assertNotIn("pg", outputs["unit_test_labels"])
                self.assertNotIn("mooncake-tests-pg", outputs["unit_test_targets"])

    def test_every_master_source_excludes_transport_and_pg_tests(self):
        for path, owner in SELECTOR.store_source_owners().items():
            if owner != "store-master":
                continue
            with self.subTest(path=path):
                components = self.select(path)
                self.assertIn("store-master", components)
                self.assertIn("client-master-contract", components)
                self.assertNotIn("store-client", components)
                self.assertNotIn("te", components)
                outputs = SELECTOR.selection_outputs(components)
                for flag in ("te_tests", "pg_tests", "store_client_tests"):
                    self.assertEqual(outputs[flag], "false")

    def test_master_named_client_source_is_not_master(self):
        components = self.select("mooncake-store/src/master_client.cpp")
        self.assertIn("store-client", components)
        self.assertNotIn("store-master", components)
        self.assertIn("client-master-contract", components)

    def test_private_master_header_and_nested_test_use_declared_ownership(self):
        for path in (
            "mooncake-store/src/config/master_metadata_config.h",
            "mooncake-store/tests/master_service_test.cpp",
            "mooncake-store/tests/ha/snapshot/batch_oplog/codec_test.cpp",
            "mooncake-store/tests/e2e/oplog_batch_e2e_test.cpp",
        ):
            with self.subTest(path=path):
                self.assertIn("store-master", self.select(path))
                self.assertNotIn("te", self.select(path))

    def test_public_shared_build_and_unknown_changes_fall_back_to_full(self):
        for path in (
            "mooncake-transfer-engine/include/transfer_engine.h",
            "mooncake-transfer-engine/tent/include/tent.h",
            "mooncake-store/include/types.h",
            "mooncake-store/src/types.cpp",
            "mooncake-common/include/default_config.h",
            "mooncake-store/src/new_file.cpp",
            "mooncake-store/tests/new_test.cpp",
            "mooncake-store/src/CMakeLists.txt",
            "cmake/StoreTestComponents.cmake",
            "CMakeLists.txt",
            "pyproject.toml",
            "python/mooncake/async_store.py",
            ".github/actions/run-ctest/run.sh",
        ):
            with self.subTest(path=path):
                self.assertEqual(self.select(path), {"all"})

    def test_multiple_components_take_union_and_shared_change_wins(self):
        te = "mooncake-transfer-engine/src/transfer_engine.cpp"
        master = "mooncake-store/src/master_service.cpp"
        self.assertEqual(self.select(te, master), self.select(te) | self.select(master))
        self.assertEqual(self.select(te, "CMakeLists.txt"), {"all"})
        self.assertEqual(self.select(te, "README.md"), self.select(te))

    def test_pg_and_ep_changes_select_only_their_own_tests(self):
        for path, own, other, pg_tests in (
            ("mooncake-pg/tests/test_cpu.py", "pg", "ep", "true"),
            ("mooncake-pg/src/pg_utils.cpp", "pg", "ep", "true"),
            ("mooncake-ep/src/ep.cpp", "ep", "pg", "false"),
        ):
            with self.subTest(path=path):
                components = self.select(path)
                self.assertIn(own, components)
                self.assertNotIn(other, components)
                self.assertNotIn("te", components)
                self.assertNotIn("store-master", components)
                outputs = SELECTOR.selection_outputs(components)
                self.assertEqual(outputs["pg_tests"], pg_tests)

    def test_pg_has_no_native_label_or_target(self):
        outputs = SELECTOR.selection_outputs(
            self.select("mooncake-pg/src/pg_utils.cpp")
        )
        self.assertEqual(
            outputs["unit_test_labels"], "^component-(common|unclassified)$"
        )
        self.assertEqual(
            outputs["unit_test_targets"],
            "mooncake-tests-common mooncake-tests-unclassified",
        )

    def test_manual_push_and_empty_inputs_cannot_silently_select_nothing(self):
        self.assertEqual(self.select(), {"all"})
        self.assertEqual(
            SELECTOR.select_components(["mooncake-store/src/master.cpp"], full=True),
            {"all"},
        )
        outputs = SELECTOR.selection_outputs({"all"})
        self.assertEqual(outputs["unit_test_labels"], "")
        self.assertEqual(outputs["unit_test_targets"], "")
        self.assertEqual(outputs["pg_tests"], "true")

    def test_all_current_store_tests_have_explicit_nonoverlapping_ownership(self):
        text = (ROOT / "cmake/StoreTestComponents.cmake").read_text()
        groups = [
            SELECTOR.cmake_list(text, f"MOONCAKE_STORE_{owner.upper()}_TESTS")
            for owner in ("client", "master", "shared")
        ]
        self.assertEqual(sum(map(len, groups)), len(set.union(*groups)))
        declared = set()
        for directory in ("mooncake-store/tests", "mooncake-store/tests/e2e"):
            text = (ROOT / directory / "CMakeLists.txt").read_text()
            declared.update(re.findall(r"add_(?:store|ha)_test\(\s*(\w+)", text))
            declared.update(re.findall(r"add_test\(\s*NAME\s+(\w+)", text))
        declared.remove("client_master_contract_test")
        self.assertEqual(declared, set.union(*groups))

    def test_contract_filters_still_name_real_client_cases(self):
        cmake = (ROOT / "mooncake-store/tests/CMakeLists.txt").read_text()
        contract = cmake.split("NAME client_master_contract_test", 1)[1]
        filters = re.search(r"--gtest_filter=([^\s)]+)", contract)[1].split(":")
        source = (ROOT / "mooncake-store/tests/client_integration_test.cpp").read_text()
        cases = {
            "ClientIntegrationTest." + name
            for name in re.findall(
                r"TEST_F\(\s*ClientIntegrationTest,\s*(\w+)\)", source
            )
        }
        self.assertEqual(len(filters), 3)
        self.assertLessEqual(set(filters), cases)
        self.assertIn("RESOURCE_LOCK store_client_integration", contract)

    def test_cli_records_selection_and_rejects_malformed_path_lists(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            summary = Path(directory) / "summary"
            environment = {
                **os.environ,
                "GITHUB_OUTPUT": str(output),
                "GITHUB_STEP_SUMMARY": str(summary),
            }
            command = [
                sys.executable,
                str(ROOT / "scripts/ci/select_test_components.py"),
            ]
            result = subprocess.run(
                command + ["--files-json", '["mooncake-store/src/master.cpp"]'],
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("te_tests=false\n", output.read_text())
            self.assertIn("store-master", summary.read_text())
            self.assertNotIn("store-client", summary.read_text())
            for invalid in ('"not a list"', "[1]"):
                result = subprocess.run(
                    command + ["--files-json", invalid],
                    env=environment,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 2, result.stderr)

    def test_workflow_uses_selection_and_enforces_optional_job_result(self):
        jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
        self.assertIn("te_tests", jobs["build-flags"]["if"])
        self.assertIn("src", jobs["build-flags"]["if"])
        pg = next(
            s
            for s in jobs["test-wheel-ubuntu"]["steps"]
            if "PG CPU" in s.get("name", "")
        )
        self.assertIn("pg_tests", pg["if"])
        gate = next(
            s
            for s in jobs["ci-gate"]["steps"]
            if s.get("name") == "Check Transfer Engine build job"
        )
        self.assertIn("te_tests", gate["env"]["EXPECTED"])
        for expected in ("success", "skipped"):
            for result in ("success", "skipped", "failure", "cancelled", ""):
                checked = subprocess.run(
                    ["bash", "-c", gate["run"]],
                    env={"EXPECTED": expected, "RESULT": result},
                    capture_output=True,
                )
                self.assertEqual(checked.returncode == 0, expected == result)


@unittest.skipUnless(shutil.which("cmake") and shutil.which("ctest"), "CMake required")
class CMakeComponentTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.build = self.root / "build"
        (self.root / "main.cpp").write_text("int main() { return 0; }\n")
        (self.root / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.16)\n"
            "project(component_fixture CXX)\nenable_testing()\n"
            f'include("{ROOT}/cmake/TestComponents.cmake")\n'
            "add_subdirectory(te)\nadd_subdirectory(store)\nadd_subdirectory(ep)\n"
        )
        for directory, component, names in (
            ("te", "te", ["tcp_transport_test"]),
            (
                "store",
                "store",
                [
                    "master_config_test",
                    "transfer_task_test",
                    "client_integration_test",
                    "future_test",
                ],
            ),
            ("ep", "ep", ["ep_dispatch_slots_test"]),
        ):
            subdirectory = self.root / directory
            subdirectory.mkdir()
            content = ""
            for name in names:
                content += (
                    f"add_executable({name} ../main.cpp)\n"
                    f"add_test(NAME {name} COMMAND {name})\n"
                )
            content += f"set_property(TEST {names[0]} PROPERTY LABELS existing-hardware-label)\n"
            if component == "store":
                content += (
                    "add_test(NAME client_master_contract_test COMMAND client_integration_test)\n"
                    "add_dependencies(mooncake-tests-client-master-contract client_integration_test)\n"
                )
            content += f"mooncake_label_tests({component})\n"
            (subdirectory / "CMakeLists.txt").write_text(content)
        self.command("cmake", "-S", str(self.root), "-B", str(self.build))

    def command(self, *args):
        result = subprocess.run(args, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def build_selection(self, path):
        outputs = SELECTOR.selection_outputs(SELECTOR.select_components([path]))
        self.command(
            "cmake",
            "--build",
            str(self.build),
            "--target",
            *outputs["unit_test_targets"].split(),
        )
        self.command(
            "ctest",
            "--test-dir",
            str(self.build),
            "-L",
            outputs["unit_test_labels"],
            "--no-tests=error",
            "--output-on-failure",
        )
        return json.loads(
            self.command("ctest", "--test-dir", str(self.build), "--show-only=json-v1")
        )["tests"]

    def executable_exists(self, directory, name):
        return (self.build / directory / name).exists()

    def test_te_build_does_not_compile_master_or_ep_tests(self):
        tests = self.build_selection("mooncake-transfer-engine/src/transfer_engine.cpp")
        self.assertTrue(self.executable_exists("te", "tcp_transport_test"))
        self.assertTrue(self.executable_exists("store", "transfer_task_test"))
        self.assertTrue(self.executable_exists("store", "future_test"))
        self.assertFalse(self.executable_exists("store", "master_config_test"))
        self.assertFalse(self.executable_exists("ep", "ep_dispatch_slots_test"))
        te = next(t for t in tests if t["name"] == "tcp_transport_test")
        labels = next(p["value"] for p in te["properties"] if p["name"] == "LABELS")
        self.assertIn("existing-hardware-label", labels)
        self.assertIn("component-te", labels)

    def test_master_build_only_includes_master_and_boundary_test_targets(self):
        self.build_selection("mooncake-store/src/master_service.cpp")
        self.assertTrue(self.executable_exists("store", "master_config_test"))
        self.assertTrue(self.executable_exists("store", "client_integration_test"))
        self.assertTrue(self.executable_exists("store", "future_test"))
        self.assertFalse(self.executable_exists("store", "transfer_task_test"))
        self.assertFalse(self.executable_exists("te", "tcp_transport_test"))
        self.assertFalse(self.executable_exists("ep", "ep_dispatch_slots_test"))


if __name__ == "__main__":
    unittest.main()
