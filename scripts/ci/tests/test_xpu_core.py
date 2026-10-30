"""CPU-only regressions for the hardware consumer; these are not GPU coverage."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
import zipfile

import yaml


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "xpu_core", ROOT / "scripts/ci/xpu_core.py"
)
CORE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CORE)
WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/xpu-core.yml").read_text())


def write_sums(root, excluded=()):
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{CORE.digest(path)}  ./{path.relative_to(root)}\n"
            for path in sorted(root.rglob("*"))
            if path.is_file() and path.name not in (*excluded, "SHA256SUMS")
        )
    )


class ArtifactTest(unittest.TestCase):
    def test_exact_artifact_identity_digest_sha_and_complete_manifest(self):
        sha = "a" * 40
        image = "intel/pytorch@sha256:" + "b" * 64
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            provenance = f"source_sha={sha}\nimage={image}\nci_run=123/2\n"
            wheel = "mooncake_transfer_engine_xpu-0.0.0-cp312-cp312-linux_x86_64.whl"
            payload = {wheel: b"wheel", "native-tests.tar.gz": b"native"}
            sums = "".join(
                f"{hashlib.sha256(data).hexdigest()}  ./{name}\n"
                for name, data in payload.items()
            )
            payload.update(
                {"provenance.txt": provenance.encode(), "SHA256SUMS": sums.encode()}
            )
            with zipfile.ZipFile(root / "artifact.zip", "w") as archive:
                for name, data in payload.items():
                    archive.writestr(name, data)
            checksum = CORE.digest(root / "artifact.zip")
            original = {
                "id": 42,
                "name": f"mooncake-xpu-ci-py312-{sha}-123-2",
                "expired": False,
                "workflow_run": {"id": 123},
                "digest": f"sha256:{checksum}",
            }
            environment = {
                "GITHUB_RUN_ID": "123",
                "GITHUB_RUN_ATTEMPT": "2",
                "XPU_IMAGE": image,
            }
            for change in (
                {"id": 43},
                {"name": "latest"},
                {"expired": True},
                {"workflow_run": {"id": 122}},
                {"digest": "sha256:" + "0" * 64},
            ):
                with self.subTest(change=change), patch.dict(os.environ, environment):
                    (root / "artifact.json").write_text(
                        json.dumps({**original, **change})
                    )
                    with self.assertRaises(RuntimeError):
                        CORE.unpack(root, sha, "42", checksum)
            (root / "artifact.json").write_text(json.dumps(original))
            with patch.dict(os.environ, environment):
                with self.assertRaises(RuntimeError):
                    CORE.unpack(root, sha, "42", "0" * 64)
                CORE.unpack(root, sha, "42", checksum)
            bundle = root / "bundle"
            with self.assertRaises(RuntimeError):
                CORE.verify_provenance(bundle, "c" * 40, image, "123/2")
            with self.assertRaises(RuntimeError):
                CORE.verify_provenance(bundle, sha, image + "changed", "123/2")
            (bundle / wheel).write_bytes(b"corrupted")
            with self.assertRaises(RuntimeError):
                CORE.verify_hashes(bundle, {"provenance.txt"})

    def test_zip_rejects_traversal_and_duplicate_entries(self):
        for names in (
            ["../escape", "a", "b", "c"],
            ["/escape", "a", "b", "c"],
            ["nested/file", "a", "b", "c"],
        ):
            with self.subTest(names=names), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with zipfile.ZipFile(root / "artifact.zip", "w") as archive:
                    for name in names:
                        archive.writestr(name, b"data")
                checksum = CORE.digest(root / "artifact.zip")
                (root / "artifact.json").write_text(
                    json.dumps(
                        {
                            "id": 1,
                            "name": f"mooncake-xpu-ci-py312-{'a' * 40}-1-1",
                            "expired": False,
                            "workflow_run": {"id": 1},
                            "digest": f"sha256:{checksum}",
                        }
                    )
                )
                with patch.dict(
                    os.environ, {"GITHUB_RUN_ID": "1", "GITHUB_RUN_ATTEMPT": "1"}
                ):
                    with self.assertRaises(RuntimeError):
                        CORE.unpack(root, "a" * 40, "1", checksum)

    def test_hash_inventory_rejects_unhashed_missing_duplicate_and_unsafe_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "a").write_bytes(b"a")
            write_sums(root)
            valid = (root / "SHA256SUMS").read_text()
            CORE.verify_hashes(root)
            for content in (
                "",
                valid * 2,
                valid.replace("./a", "../a"),
                valid + "invalid\n",
            ):
                with self.subTest(content=content):
                    (root / "SHA256SUMS").write_text(content)
                    with self.assertRaises(RuntimeError):
                        CORE.verify_hashes(root)
            (root / "SHA256SUMS").write_text(valid)
            (root / "unhashed").write_text("extra")
            with self.assertRaises(RuntimeError):
                CORE.verify_hashes(root)

    def test_native_tar_roundtrip_and_rejection_of_links_and_traversal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            native = root / "native-tests"
            native.mkdir()
            (native / "provenance.txt").write_text("source provenance")
            (native / "test").write_bytes(b"executable")
            (native / "test").chmod(0o755)
            write_sums(native)
            (root / "provenance.txt").write_bytes(
                (native / "provenance.txt").read_bytes()
            )
            with tarfile.open(root / "native-tests.tar.gz", "w:gz") as archive:
                archive.add(native, arcname="native-tests")
            output = root / "valid"
            output.mkdir()
            extracted = CORE.extract_native(root, output)
            self.assertEqual((extracted / "test").stat().st_mode & 0o777, 0o755)
            for index, (name, kind) in enumerate(
                (
                    ("../escape", tarfile.REGTYPE),
                    ("native-tests/link", tarfile.SYMTYPE),
                    ("native-tests/link", tarfile.LNKTYPE),
                    ("native-tests/device", tarfile.CHRTYPE),
                )
            ):
                with self.subTest(name=name, kind=kind):
                    entry = tarfile.TarInfo(name)
                    entry.type = kind
                    with tarfile.open(root / "native-tests.tar.gz", "w:gz") as archive:
                        archive.addfile(entry, io.BytesIO())
                    output = root / str(index)
                    output.mkdir()
                    with self.assertRaises(RuntimeError):
                        CORE.extract_native(root, output)


class ReportTest(unittest.TestCase):
    def test_suite_requires_store_report_and_ep_completion_before_success(self):
        import importlib.metadata

        def write_report(path, identities):
            root = ET.Element("testsuites")
            suite = ET.SubElement(root, "testsuite")
            for identity in identities:
                classname, name = identity.split(".", 1)
                ET.SubElement(
                    suite,
                    "testcase",
                    classname=classname,
                    name=name,
                    status="run",
                    result="completed",
                )
            ET.ElementTree(root).write(path)

        def inventory_text(identities):
            return "".join(
                f"{identity.split('.', 1)[0]}.\n  {identity.split('.', 1)[1]}\n"
                for identity in sorted(identities)
            )

        for store_report, ep_complete, valid in (
            (True, True, True),
            (False, True, False),
            (True, False, False),
        ):
            with self.subTest(store_report=store_report, ep_complete=ep_complete):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    for name, (inventory, _) in CORE.SUITES.items():
                        (root / f"{name}.list").write_text(inventory_text(inventory))

                    def run_command(args, log, timeout=600):
                        if "--gtest_list_tests" in args:
                            return inventory_text(CORE.SUITES[Path(args[0]).name][0])
                        for argument in args:
                            if argument.startswith("--gtest_output=xml:"):
                                write_report(
                                    Path(argument.split("xml:", 1)[1]),
                                    CORE.SUITES[Path(args[0]).name][1],
                                )
                        if len(args) > 2 and args[2] == "store" and store_report:
                            write_report(
                                Path(args[4]), {"StorePython.test_xpu_local_copy_paths"}
                            )
                        if "test_mooncake_ep_xpu.py" in args[1]:
                            if not ep_complete:
                                return ""
                            return "".join(
                                f"round {i}: tokens=0 ok\n" for i in range(8)
                            ) + (
                                "PASSED: 8 rounds + 2 empty-rank rounds + 4 two-batch rounds"
                                " + duplicate experts + reused src_info + rejected handle"
                                " + cross-stream + fallback agreement"
                                " on 2 ranks (hidden=1024, experts=8, topk=2)\n"
                            )
                        return ""

                    with (
                        patch.object(CORE, "preflight"),
                        patch.object(CORE, "command", side_effect=run_command),
                        patch.object(CORE.socket, "create_connection"),
                        patch.object(CORE.subprocess, "Popen") as process,
                        patch.object(
                            importlib.metadata, "distribution"
                        ) as distribution,
                        patch.dict(os.environ, {"XPU_SOURCE_SHA": "a" * 40}),
                    ):
                        distribution.return_value.locate_file.return_value = root
                        process.return_value.__enter__.return_value.poll.return_value = None
                        if valid:
                            CORE.suite(root, ROOT, root)
                            result = json.loads((root / "result.json").read_text())
                            self.assertEqual(result["cpp_cases"], 149)
                            self.assertEqual(result["ep_rounds"], 8)
                        else:
                            with self.assertRaises((RuntimeError, FileNotFoundError)):
                                CORE.suite(root, ROOT, root)
                            self.assertFalse((root / "result.json").exists())

    def test_exact_case_identity_count_and_no_skips_or_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "report.xml"
            for attributes, child, valid in (
                ({"status": "run", "result": "completed"}, None, True),
                ({"status": "notrun", "result": "suppressed"}, None, False),
                ({"status": "run", "result": "skipped"}, "skipped", False),
                ({"status": "run", "result": "completed"}, "failure", False),
                ({"status": "run", "result": "completed"}, "error", False),
            ):
                root = ET.Element("testsuites")
                suite = ET.SubElement(root, "testsuite")
                case = ET.SubElement(
                    suite, "testcase", classname="Suite", name="Case", **attributes
                )
                if child:
                    ET.SubElement(case, child)
                ET.ElementTree(root).write(report)
                with self.subTest(attributes=attributes, child=child):
                    if valid:
                        CORE.validate_report(report, {"Suite.Case"})
                        for expected in (
                            set(),
                            {"Suite.Other"},
                            {"Suite.Case", "Suite.Missing"},
                        ):
                            with self.assertRaises(RuntimeError):
                                CORE.validate_report(report, expected)
                        suite.append(ET.fromstring(ET.tostring(case)))
                        ET.ElementTree(root).write(report)
                        with self.assertRaises(RuntimeError):
                            CORE.validate_report(report, {"Suite.Case"})
                    else:
                        with self.assertRaises(RuntimeError):
                            CORE.validate_report(report, {"Suite.Case"})

    def test_inventory_parser(self):
        self.assertEqual(
            CORE.parse_inventory(
                "noise\nModes/Suite.\n  Case/0  # GetParam() = false\n  Case/1\n"
            ),
            {"Modes/Suite.Case/0", "Modes/Suite.Case/1"},
        )
        for text in ("", "noise\n", "  Case\n", "Suite.\n  Case\n  Case\n"):
            with self.subTest(text=text), self.assertRaises(RuntimeError):
                CORE.parse_inventory(text)

    def test_required_inventory_matches_cpp_sources(self):
        tent = ROOT / "mooncake-transfer-engine/tent/tests/xpu"
        for filename, names in (
            ("xpu_platform_test.cpp", CORE.PLATFORM),
            ("xpu_transport_test.cpp", CORE.TRANSPORT),
            ("xpu_engine_test.cpp", CORE.ENGINE | CORE.RDMA),
            ("xpu_ipc_process_test.cpp", CORE.IPC),
        ):
            declared = set(
                re.findall(
                    r"TEST_[FP]\(\s*(\w+),\s*(\w+)\s*\)", (tent / filename).read_text()
                )
            )
            selected = {
                (
                    identity.split(".")[0].split("/")[-1],
                    identity.split(".")[1].split("/")[0],
                )
                for identity in names
            }
            self.assertEqual(selected, declared, filename)
        self.assertEqual(len(CORE.ENGINE), 124)
        self.assertEqual(len(CORE.INAPPLICABLE), 10)
        self.assertEqual(
            sum(len(selected) for _, selected in CORE.SUITES.values()), 149
        )
        selected_engine = CORE.SUITES["tent_xpu_engine_test"][1]
        self.assertFalse(selected_engine & CORE.RDMA)
        self.assertFalse(selected_engine & CORE.INAPPLICABLE.keys())

    def test_command_exit_and_timeout_propagate(self):
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "command.log"
            diagnostics = io.StringIO()
            with (
                patch.object(CORE.sys, "stderr", diagnostics),
                self.assertRaises(subprocess.CalledProcessError),
            ):
                CORE.command(["bash", "-c", "echo failed; exit 2"], log)
            self.assertIn("failed", log.read_text())
            self.assertIn(str(log), diagnostics.getvalue())
            self.assertIn("failed", diagnostics.getvalue())
            diagnostics = io.StringIO()
            with (
                patch.object(CORE.sys, "stderr", diagnostics),
                self.assertRaises(subprocess.TimeoutExpired),
            ):
                CORE.command(["sleep", "2"], log, timeout=0.01)
            self.assertIn(str(log), diagnostics.getvalue())

    def test_command_failure_diagnostics_are_bounded(self):
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "command.log"
            diagnostics = io.StringIO()
            with (
                patch.object(CORE.sys, "stderr", diagnostics),
                self.assertRaises(subprocess.CalledProcessError),
            ):
                CORE.command(["bash", "-c", "seq 1 100; exit 2"], log)
            lines = diagnostics.getvalue().splitlines()
            self.assertEqual(len(lines), 41)
            self.assertEqual(lines[1:], [str(i) for i in range(61, 101)])

    def test_device_preflight_rejects_missing_wrong_and_unexecuted_devices(self):
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            for count, name, total, valid in (
                (0, "Intel Arc Pro B60", 2080, False),
                (1, "Intel Arc Pro B60", 2080, False),
                (2, "CPU fallback", 2080, False),
                (2, "Intel Arc Pro B60", 0, False),
                (2, "Intel Arc Pro B60", 2080, True),
            ):
                tensor = unittest.mock.MagicMock()
                tensor.__add__.return_value.sum.return_value.cpu.return_value = total
                torch = SimpleNamespace(
                    __version__="2.13.0+xpu",
                    xpu=SimpleNamespace(
                        is_available=lambda: count > 0,
                        device_count=lambda: count,
                        get_device_properties=lambda index: SimpleNamespace(
                            name=name, total_memory=24 << 30
                        ),
                        synchronize=lambda index: None,
                    ),
                    arange=lambda *args, **kwargs: tensor,
                )
                read_text = Path.read_text

                def runtime_text(path, *args, **kwargs):
                    if str(path) == "/proc/self/status":
                        return "CapEff:\t00000000\n"
                    if str(path) == "/proc/sys/kernel/yama/ptrace_scope":
                        return "1"
                    return read_text(path, *args, **kwargs)

                with (
                    self.subTest(count=count, name=name, total=total),
                    patch.dict("sys.modules", {"torch": torch}),
                    patch.dict(
                        os.environ,
                        {
                            "ONEAPI_DEVICE_SELECTOR": "level_zero:gpu",
                            "ZE_AFFINITY_MASK": "0,1",
                            "XPU_IMAGE": "image",
                            "XPU_SOURCE_SHA": "a" * 40,
                        },
                    ),
                    patch.object(Path, "read_text", runtime_text),
                ):
                    if valid:
                        CORE.preflight(output)
                    else:
                        with self.assertRaises(RuntimeError):
                            CORE.preflight(output)


class ShellLifecycleTest(unittest.TestCase):
    def test_network_isolation_sets_store_and_ep_loopback_interfaces(self):
        script = (ROOT / "scripts/ci/run_xpu_core.sh").read_text()
        start = script.index("container_id=$(docker create")
        stop = script.index("timeout --signal=TERM", start)
        docker_command = script[start:stop]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            docker = root / "docker"
            docker.write_text("""#!/bin/bash
set -eu
printf '%s\\n' "$@" > "$DOCKER_ARGUMENTS"
echo container-id
""")
            docker.chmod(0o755)
            arguments = root / "arguments"
            result = subprocess.run(
                ["bash", "-ec", "device_args=()\n" + docker_command],
                env={
                    **os.environ,
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "DOCKER_ARGUMENTS": str(arguments),
                    "GITHUB_RUN_ID": "123",
                    "GITHUB_RUN_ATTEMPT": "1",
                    "XPU_IMAGE": "image",
                    "ZE_AFFINITY_MASK": "4,5",
                    "CHECKOUT_SHA": "a" * 40,
                    "run_directory": str(root),
                    "source_directory": str(ROOT),
                    "control_directory": str(ROOT / "scripts"),
                },
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            args = arguments.read_text().splitlines()
            self.assertEqual(args[args.index("--network") + 1], "none")
            for variable in (
                "MOONCAKE_LOCAL_HOSTNAME=127.0.0.1",
                "CCL_KVS_IFACE=lo",
                "GLOO_SOCKET_IFNAME=lo",
                "LOCAL_HOSTNAME=127.0.0.1",
                "MASTER_SERVER=127.0.0.1:50051",
                "ZE_AFFINITY_MASK=4,5",
            ):
                self.assertIn(variable, args)
                self.assertEqual(args[args.index(variable) - 1], "--env")
            self.assertNotIn("--privileged", args)

    def test_fixed_runner_masks_need_no_lock_or_profile_variables(self):
        script = (ROOT / "scripts/ci/run_xpu_core.sh").read_text()
        start = script.index(': "${ZE_AFFINITY_MASK:')
        stop = script.index("# Each runner owns a fixed pair.")
        for mask, valid in (
            ("0,1", True),
            ("2,3", True),
            ("4,5", True),
            ("6,7", True),
            ("0,0", False),
            ("00,0", False),
            ("0", False),
            ("0.0,0.1", False),
            ("", False),
        ):
            environment = {
                key: value
                for key, value in os.environ.items()
                if key
                not in {
                    "XPU_LOCK_DIRECTORY",
                    "XPU_SECCOMP_PROFILE",
                    "XPU_RENDER_DEVICES",
                }
            }
            environment.update(
                {
                    "ZE_AFFINITY_MASK": mask,
                    "XPU_IMAGE": "image@sha256:" + "a" * 64,
                    "CHECKOUT_SHA": "b" * 40,
                }
            )
            with self.subTest(mask=mask):
                result = subprocess.run(
                    [
                        "bash",
                        "-ec",
                        'fail() { echo "$*" >&2; exit 1; }\n' + script[start:stop],
                    ],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode == 0, valid, result.stderr)
        self.assertNotIn("XPU_LOCK_DIRECTORY", script)
        self.assertNotIn("XPU_SECCOMP_PROFILE", script)
        self.assertNotIn("XPU_RENDER_DEVICES", script)
        self.assertNotIn("flock", script)
        self.assertIn("devices=(/dev/dri/renderD*)", script)
        self.assertIn('--env "ZE_AFFINITY_MASK=$ZE_AFFINITY_MASK"', script)
        self.assertIn(
            '--security-opt "seccomp=$run_directory/output/seccomp.json"', script
        )

    def test_cleanup_recheck_propagates_docker_query_failure(self):
        for query_status in (0, 2):
            with self.subTest(
                query_status=query_status
            ), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / "output").mkdir()
                docker = root / "docker"
                docker.write_text("""#!/bin/bash
set -eu
case "$1" in
  ps) exit "$QUERY_STATUS" ;;
  info) exit 0 ;;
  *) exit 99 ;;
esac
""")
                docker.chmod(0o755)
                result = subprocess.run(
                    [
                        "bash",
                        str(ROOT / "scripts/ci/run_xpu_core.sh"),
                        "--cleanup",
                        str(root),
                    ],
                    env={
                        **os.environ,
                        "PATH": f"{root}:{os.environ['PATH']}",
                        "GITHUB_RUN_ID": "123",
                        "GITHUB_RUN_ATTEMPT": "1",
                        "QUERY_STATUS": str(query_status),
                    },
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, query_status, result.stderr)
                self.assertEqual(
                    (root / "output/cleanup-recheck.json").exists(), query_status == 0
                )

    def test_owned_container_cleanup_and_failures_propagate(self):
        script = (ROOT / "scripts/ci/run_xpu_core.sh").read_text()
        cleanup = (
            script[
                script.index("cleanup_container()") : script.index(
                    "# Actions may interrupt"
                )
            ]
            + script[
                script.index("container_id=\n") : script.index(
                    'docker pull "$XPU_IMAGE"'
                )
            ]
        )
        for suite_status, cleanup_failure, expected in (
            (0, "no", 0),
            (2, "no", 2),
            (0, "yes", 1),
        ):
            with self.subTest(
                suite_status=suite_status, cleanup_failure=cleanup_failure
            ):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    (root / "output").mkdir()
                    docker = root / "docker"
                    docker.write_text("""#!/bin/bash
set -eu
echo "$*" >> "$RUN_DIRECTORY/docker.commands"
case "$1" in
  info) echo daemon-ready ;;
  stop) echo owned-container ;;
  inspect)
    [ ! -e "$RUN_DIRECTORY/removed" ] || exit 1
    if [ "${2:-}" = --format ]; then echo false; else echo '{}'; fi ;;
  rm)
    [ "$CLEANUP_FAILURE" = no ] || exit 1
    touch "$RUN_DIRECTORY/removed" ;;
  *) exit 99 ;;
esac
""")
                    docker.chmod(0o755)
                    result = subprocess.run(
                        [
                            "bash",
                            "-ec",
                            f"{cleanup}\ncontainer_id=owned-container\nexit {suite_status}",
                        ],
                        env={
                            **os.environ,
                            "PATH": f"{root}:{os.environ['PATH']}",
                            "run_directory": str(root),
                            "RUN_DIRECTORY": str(root),
                            "CLEANUP_FAILURE": cleanup_failure,
                        },
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(
                        result.returncode, expected, result.stdout + result.stderr
                    )
                    commands = (root / "docker.commands").read_text()
                    self.assertIn("stop --time 10 owned-container", commands)
                    self.assertIn("rm owned-container", commands)
                    self.assertNotIn("prune", commands)
                    report = json.loads((root / "output/cleanup.json").read_text())
                    self.assertEqual(
                        report["cleanup_status"] == 0, cleanup_failure == "no"
                    )

    def test_host_preflight_rejects_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = subprocess.run(
                [
                    "bash",
                    str(ROOT / "scripts/ci/run_xpu_core.sh"),
                    str(root),
                    str(ROOT),
                    str(ROOT / "scripts"),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if os.getuid() == 0:
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must not run as root", result.stdout + result.stderr)


class DependencyPackagingTest(unittest.TestCase):
    def test_hosted_wheelhouse_is_hashed_inside_the_native_bundle(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/ci_xpu.yml").read_text())
        steps = {
            step.get("name"): step
            for step in workflow["jobs"]["build-wheel-xpu"]["steps"]
        }
        script = steps["Package Python dependencies for hardware"]["run"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = root / "xpu-ci-output"
            payload.mkdir()
            wheel = payload / "fixture.whl"
            wheel.write_bytes(b"wheel")
            python = root / "python"
            python.write_text("""#!/bin/bash
set -eu
[ "$*" = "-m pip download --only-binary=:all: --dest xpu-ci-output/python-deps xpu-ci-output/fixture.whl" ]
cp xpu-ci-output/fixture.whl xpu-ci-output/python-deps/
echo dependency > xpu-ci-output/python-deps/dependency.whl
""")
            python.chmod(0o755)
            result = subprocess.run(
                ["bash", "-ec", script],
                cwd=root,
                env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}"},
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                [path.name for path in (payload / "python-deps").iterdir()],
                ["dependency.whl"],
            )
        self.assertIn(
            'cp -R xpu-ci-output/python-deps "$out/"',
            steps["Package native tests"]["run"],
        )


class SeccompTest(unittest.TestCase):
    def test_only_required_syscalls_are_unconditionally_allowed(self):
        original = {
            "defaultAction": "SCMP_ACT_ERRNO",
            "defaultErrnoRet": 1,
            "archMap": [{"architecture": "SCMP_ARCH_X86_64", "subArchitectures": []}],
            "syscalls": [
                {"names": ["read", "write", "pidfd_open"], "action": "SCMP_ACT_ALLOW"},
                {
                    "names": ["pidfd_getfd", "ptrace"],
                    "action": "SCMP_ACT_ALLOW",
                    "includes": {"caps": ["CAP_SYS_PTRACE"]},
                },
                {
                    "names": ["io_uring_setup"],
                    "action": "SCMP_ACT_ERRNO",
                    "errnoRet": 1,
                },
                {
                    "names": ["mount"],
                    "action": "SCMP_ACT_ALLOW",
                    "includes": {"caps": ["CAP_SYS_ADMIN"]},
                },
            ],
        }
        content = json.dumps(original).encode()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "seccomp.json"
            with (
                patch.object(CORE.urllib.request, "urlopen") as download,
                patch.object(
                    CORE, "SECCOMP_SHA256", hashlib.sha256(content).hexdigest()
                ),
            ):
                download.return_value.__enter__.return_value.read.return_value = content
                CORE.prepare_seccomp(output)
                download.assert_called_once_with(CORE.SECCOMP_URL, timeout=30)
            actual = json.loads(output.read_text())
            self.assertEqual(actual["defaultAction"], original["defaultAction"])
            self.assertEqual(actual["archMap"], original["archMap"])
            self.assertEqual(
                actual["syscalls"][-1],
                {
                    "names": sorted(CORE.XPU_SYSCALLS),
                    "action": "SCMP_ACT_ALLOW",
                },
            )
            remaining = []
            for rule in original["syscalls"]:
                names = [
                    name for name in rule["names"] if name not in CORE.XPU_SYSCALLS
                ]
                if names:
                    remaining.append({**rule, "names": names})
            self.assertEqual(actual["syscalls"][:-1], remaining)

    def test_download_checksum_and_default_action_failures_abort(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "seccomp.json"
            with patch.object(
                CORE.urllib.request, "urlopen", side_effect=OSError("download failed")
            ):
                with self.assertRaisesRegex(OSError, "download failed"):
                    CORE.prepare_seccomp(output)
            with patch.object(CORE.urllib.request, "urlopen") as download:
                download.return_value.__enter__.return_value.read.return_value = (
                    b"corrupted"
                )
                with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                    CORE.prepare_seccomp(output)
                content = b'{"defaultAction":"SCMP_ACT_ALLOW"}'
                download.return_value.__enter__.return_value.read.return_value = content
                with patch.object(
                    CORE, "SECCOMP_SHA256", hashlib.sha256(content).hexdigest()
                ):
                    with self.assertRaisesRegex(RuntimeError, "deny-by-default"):
                        CORE.prepare_seccomp(output)
            self.assertFalse(output.exists())


class WorkflowTest(unittest.TestCase):
    def test_isolated_trigger_and_matching_artifact_handoff(self):
        jobs = WORKFLOW["jobs"]
        event = WORKFLOW.get("on", WORKFLOW.get(True))
        self.assertEqual(event["pull_request_target"]["types"], ["labeled"])
        self.assertNotIn("synchronize", event["pull_request_target"]["types"])
        self.assertIn("run-xpu-ci", jobs["authorize"]["if"])
        self.assertFalse(WORKFLOW["concurrency"]["cancel-in-progress"])
        self.assertEqual(
            jobs["build"]["with"]["checkout_ref"], "${{ needs.authorize.outputs.sha }}"
        )
        for name in ("artifact_id", "artifact_digest", "source_sha"):
            self.assertEqual(
                jobs["core"]["with"][name], "${{ needs.build.outputs." + name + " }}"
            )
        self.assertEqual(jobs["gate"]["needs"], ["authorize", "build", "core"])
        integration = yaml.safe_load(
            (ROOT / ".github/workflows/integration-test-xpu.yml").read_text()
        )
        job = integration["jobs"]["core"]
        self.assertEqual(job["runs-on"], "xpu")
        build = yaml.safe_load((ROOT / ".github/workflows/ci_xpu.yml").read_text())
        self.assertEqual(
            job["env"]["XPU_IMAGE"], build["jobs"]["build-wheel-xpu"]["container"]
        )
        self.assertIn("github.workflow_sha", job["steps"][0]["with"]["ref"])
        self.assertTrue(
            all(
                not step.get("with", {}).get("persist-credentials", False)
                for step in job["steps"]
            )
        )

    def run_javascript(self, job, environment, state):
        script = WORKFLOW["jobs"][job]["steps"][0]["with"]["script"]
        harness = r"""
        const state = JSON.parse(process.env.TEST_STATE);
        const context = {
          repo: {owner: 'owner', repo: 'repo'}, actor: 'maintainer',
          ref: state.ref || 'refs/heads/main', eventName: state.event || 'workflow_dispatch',
          payload: state.payload || {}, serverUrl: 'https://github.com', runId: 123
        };
        const outputs = {};
        const core = {
          setOutput: (key, value) => outputs[key] = value,
          setFailed: message => { throw new Error(message); },
          summary: {addRaw() {return this;}, async write() {}}
        };
        const github = {rest: {
          repos: {
            getCollaboratorPermissionLevel: async () => ({data: {permission: state.permission || 'write'}}),
            getCommit: async ({ref}) => ({data: {sha: ref}})
          },
          pulls: {get: async () => ({data: {state: 'open', base: {ref: 'main'}, head: {sha: state.head}}})},
          checks: {
            create: async () => ({data: {id: 456}}),
            update: async request => { outputs.check = request; }
          }
        }};
        (async () => {
        SCRIPT
        })().then(() => console.log(JSON.stringify(outputs)))
          .catch(error => { console.error(error.message); process.exitCode = 1; });
        """
        return subprocess.run(
            ["node", "-e", harness.replace("SCRIPT", script)],
            env={**os.environ, **environment, "TEST_STATE": json.dumps(state)},
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_authorization_and_stale_head(self):
        sha = "a" * 40
        environment = {"PR_NUMBER": "1", "CHECKOUT_SHA": sha}
        for state, valid in (
            ({"head": sha}, True),
            ({"head": sha, "permission": "read"}, False),
            ({"head": "b" * 40}, False),
            ({"head": sha, "ref": "refs/heads/untrusted"}, False),
            (
                {
                    "head": sha,
                    "event": "pull_request_target",
                    "payload": {
                        "pull_request": {"number": 1, "head": {"sha": "b" * 40}}
                    },
                },
                False,
            ),
        ):
            with self.subTest(state=state):
                result = self.run_javascript("authorize", environment, state)
                self.assertEqual(result.returncode == 0, valid, result.stderr)

    def test_gate_requires_success_and_current_head(self):
        sha = "a" * 40
        for result, head, valid in (
            ("success", sha, True),
            ("failure", sha, False),
            ("cancelled", sha, False),
            ("skipped", sha, False),
            ("success", "b" * 40, False),
        ):
            needs = {
                "authorize": {
                    "result": "success",
                    "outputs": {"sha": sha, "pr": "1", "check_id": "456"},
                },
                "build": {"result": "success"},
                "core": {"result": result},
            }
            with self.subTest(result=result, head=head):
                completed = self.run_javascript(
                    "gate", {"NEEDS_JSON": json.dumps(needs)}, {"head": head}
                )
                self.assertEqual(completed.returncode == 0, valid, completed.stderr)


if __name__ == "__main__":
    unittest.main()
