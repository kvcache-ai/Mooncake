"""Verify XPU wheels and hardware suites; no arguments run host-only checks."""

from collections import deque
import hashlib
import importlib
import importlib.util
from importlib import metadata
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import time
import unittest
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

from verify_rocm_wheel import find_extension_record, record_digest


MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
MODEL_REVISION = "7ae557604adf67be50417f59c2c2f167def9a775"
APPLICATION_IMAGE = (
    "vllm/vllm-openai-xpu:v0.29.0@sha256:"
    "1db27a8b75ae1d6b3cbf16ebbb88310c2b5548221c8df1335082b2a54dba209a"
)
APPLICATION_VLLM_VERSION = "0.29.0+xpu"
APPLICATION_THREAD_ENV = {
    "OMP_NUM_THREADS": "2",
    "MKL_NUM_THREADS": "2",
    "OPENBLAS_NUM_THREADS": "2",
    "NUMEXPR_NUM_THREADS": "2",
    "RAYON_NUM_THREADS": "2",
}


def main() -> None:
    os.environ["MC_USE_TENT"] = "1"
    os.environ["MOONCAKE_STORE_BACKEND"] = "cpp"
    distribution = metadata.distribution("mooncake-transfer-engine-xpu")
    package = importlib.import_module("mooncake")
    package_dir = Path(package.__file__).resolve().parent
    records = {str(item): item for item in distribution.files or ()}
    for stem in ("engine.", "_store.", "_ep."):
        relative = find_extension_record(records, stem)
        record = records[relative]
        installed = Path(distribution.locate_file(record)).resolve()
        if installed != package_dir / Path(relative).name:
            raise RuntimeError(
                f"Extension resolves outside installed package: {installed}"
            )
        if (
            record.hash is None
            or record_digest(installed, record.hash.mode) != record.hash.value
        ):
            raise RuntimeError(f"Wheel RECORD mismatch: {installed}")
        module = importlib.import_module(f"mooncake.{stem.rstrip('.')}")
        if Path(module.__file__).resolve() != installed:
            raise RuntimeError(f"Extension loaded outside wheel: {module.__file__}")

    engine = importlib.import_module("mooncake.engine")
    if not engine.SUPPORT_XPU or not engine.SUPPORT_TENT or engine.SUPPORT_CUDA:
        raise RuntimeError("Wheel must provide native XPU/TENT, not CUDA")
    ep = importlib.import_module("mooncake.ep")
    if ep.DEVICE_TYPE != "xpu":
        raise RuntimeError("Wheel did not load the native XPU EP implementation")
    store = importlib.import_module("mooncake.store")
    private_store = importlib.import_module("mooncake._store")
    if (
        store._BACKEND != "cpp"
        or store.MooncakeDistributedStore is not private_store.MooncakeDistributedStore
    ):
        raise RuntimeError("Store facade did not select the installed C++ extension")

    master = package_dir / "mooncake_master"
    record = records.get("mooncake/mooncake_master")
    if (
        record is None
        or record.hash is None
        or record_digest(master, record.hash.mode) != record.hash.value
    ):
        raise RuntimeError("Packaged master does not match wheel RECORD")
    subprocess.run([str(master), "--version"], check=True, timeout=15)
    with subprocess.Popen([str(master), "--port=50051"]) as process:
        try:
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError(f"Packaged master exited: {process.returncode}")
                try:
                    with socket.create_connection(("127.0.0.1", 50051), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Packaged master did not start within 10 seconds")
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise
    print(
        f"Verified mooncake-transfer-engine-xpu {distribution.version}; host-only coverage"
    )


SECCOMP_URL = (
    "https://raw.githubusercontent.com/moby/profiles/"
    "4b8214fc4971e06172938a1f1554ccff6647ee80/seccomp/default.json"
)


SECCOMP_SHA256 = "6416b47770785a41ac59073cdc77d9fe98517df2799dc83ef207e622de3053f6"


XPU_SYSCALLS = {
    "pidfd_open",
    "pidfd_getfd",
    "io_uring_setup",
    "io_uring_enter",
    "io_uring_register",
}


def prepare_seccomp(output: Path) -> None:
    with urllib.request.urlopen(SECCOMP_URL, timeout=30) as response:
        content = response.read()
    require(
        hashlib.sha256(content).hexdigest() == SECCOMP_SHA256,
        "Docker default seccomp profile checksum mismatch",
    )
    profile = json.loads(content)
    require(
        profile["defaultAction"] == "SCMP_ACT_ERRNO", "Expected deny-by-default seccomp"
    )
    rules = []
    for rule in profile["syscalls"]:
        names = [name for name in rule["names"] if name not in XPU_SYSCALLS]
        if names:
            rules.append({**rule, "names": names})
    rules.append({"names": sorted(XPU_SYSCALLS), "action": "SCMP_ACT_ALLOW"})
    profile["syscalls"] = rules
    output.write_text(json.dumps(profile, indent=2))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def relative_path(name: str) -> Path:
    path = PurePosixPath(name)
    require(not path.is_absolute() and ".." not in path.parts, f"Unsafe path: {name}")
    require(bool(path.parts), f"Empty path: {name}")
    return Path(*path.parts)


def verify_hashes(
    root: Path, excluded: set[str] | frozenset[str] = frozenset()
) -> None:
    expected = set()
    for line in (root / "SHA256SUMS").read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64}) [ *](.+)", line)
        if match is None:
            raise RuntimeError(f"Invalid checksum entry: {line}")
        checksum, name = match.groups()
        path = relative_path(name)
        require(
            path not in expected and path != Path("SHA256SUMS"), f"Duplicate: {name}"
        )
        expected.add(path)
        require(digest(root / path) == checksum, f"Checksum mismatch: {name}")
    actual = {
        path.relative_to(root)
        for path in root.rglob("*")
        if path.is_file() and path.name not in excluded and path != root / "SHA256SUMS"
    }
    require(bool(expected) and actual == expected, "Checksum inventory mismatch")


def verify_provenance(root: Path, sha: str, image: str, run: str) -> None:
    lines = (root / "provenance.txt").read_text().splitlines()
    for item in (f"source_sha={sha}", f"image={image}", f"ci_run={run}"):
        require(lines.count(item) == 1, f"Missing or mismatched provenance: {item}")


def unpack(directory: Path, sha: str, artifact_id: str, checksum: str) -> None:
    info = json.loads((directory / "artifact.json").read_text())
    run = f"{os.environ['GITHUB_RUN_ID']}/{os.environ['GITHUB_RUN_ATTEMPT']}"
    expected_name = f"mooncake-xpu-ci-py312-{sha}-{run.replace('/', '-')}"
    checksum = checksum.removeprefix("sha256:")
    require(
        re.fullmatch(r"[0-9a-f]{64}", checksum) is not None, "Invalid artifact digest"
    )
    require(str(info["id"]) == artifact_id, "Artifact ID mismatch")
    require(
        info["name"] == expected_name and not info["expired"],
        "Artifact identity mismatch",
    )
    require(
        str(info["workflow_run"]["id"]) == run.split("/")[0], "Artifact run mismatch"
    )
    require(info["digest"] == f"sha256:{checksum}", "Artifact metadata digest mismatch")
    require(
        digest(directory / "artifact.zip") == checksum, "Artifact ZIP digest mismatch"
    )
    bundle = directory / "bundle"
    bundle.mkdir()
    with zipfile.ZipFile(directory / "artifact.zip") as archive:
        names = archive.namelist()
        require(
            len(names) == len(set(names)) == 4, "Expected exactly four artifact files"
        )
        for member in archive.infolist():
            path = relative_path(member.filename)
            require(
                len(path.parts) == 1 and not member.is_dir(), "Artifact must be flat"
            )
            require(
                not stat.S_ISLNK(member.external_attr >> 16),
                "Artifact contains a symlink",
            )
            with archive.open(member) as source, (bundle / path).open("wb") as target:
                shutil.copyfileobj(source, target)
    require(
        {path.name for path in bundle.iterdir() if path.suffix != ".whl"}
        == {"native-tests.tar.gz", "provenance.txt", "SHA256SUMS"},
        "Unexpected artifact payload",
    )
    wheels = list(bundle.glob("mooncake_transfer_engine_xpu-*-cp312-cp312-*.whl"))
    require(len(wheels) == 1, "Expected one CPython 3.12 XPU wheel")
    verify_hashes(bundle, {"provenance.txt"})
    verify_provenance(
        bundle, sha, os.environ.get("XPU_BUILD_IMAGE", os.environ["XPU_IMAGE"]), run
    )


def extract_native(bundle: Path, output: Path) -> Path:
    destination = output / "native"
    destination.mkdir()
    seen = set()
    with tarfile.open(bundle / "native-tests.tar.gz", "r:gz") as archive:
        for member in archive:
            path = relative_path(member.name)
            require(path.parts[0] == "native-tests", "Unexpected native archive root")
            require(path not in seen, f"Duplicate native entry: {path}")
            seen.add(path)
            target = destination / path
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                require(
                    member.isfile(),
                    f"Native archive contains a link or special file: {path}",
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise RuntimeError(f"Native archive file has no data: {path}")
                with source, target.open("wb") as stream:
                    shutil.copyfileobj(source, stream)
                target.chmod(member.mode & 0o777)
    native = destination / "native-tests"
    verify_hashes(native)
    require(
        (native / "provenance.txt").read_bytes()
        == (bundle / "provenance.txt").read_bytes(),
        "Native and wheel provenance differ",
    )
    return native


def case_names(suite: str, names: str, count: int = 0) -> set[str]:
    return {
        f"{suite}.{name}" + (f"/{index}" if count else "")
        for name in names.split()
        for index in range(count or 1)
    }


PLATFORM = case_names(
    "XpuPlatformTest",
    """TypeString ProbeRegistersOneMemEntryPerDevice ProbeDoesNotDuplicateGpusAcrossAdapters
    DeviceAllocCopyFreeRoundTrip HostPointerClassifiesAsCpu ForeignUsmClassifiesAsXpuAndCopies
    InteriorPointerClassifiesAsXpu BareXpuLocationAllocatesDeviceZero MalformedXpuOrdinalIsRejected
    HostPointerIsNotExportable DeviceAllocationExportsAsDmabuf SeparateAllocationsExportDistinctDmabufs
    DeviceToDeviceCopyOnSameDevice DeviceToDeviceCopyAcrossDevices CopyRejectsRangePastAllocationEnd
    IpcImportOnSameDevice IpcImportOnPeerDevice HostPointerIsNotIpcExportable ImportRejectsBadArguments""",
)


TRANSPORT = case_names(
    "XpuTransportTest",
    """LocalStageCopiesFromInteriorDeviceOffset RemoteStageCopiesToInteriorDeviceOffset
    ReadStageCopiesHostToDevice FailsWhenNeitherSideIsDeviceMemory CopiesDeviceToDevice
    RejectsUnmappedNonLocalTarget ReRegistrationPublishesDistinctIpcAttribute""",
)


ENGINE = (
    case_names(
        "NetworkAndDispatchModes/XpuEngineTest",
        """DefaultRegistrationSelectsLocalXpuCopy StagesDeviceToDeviceReadAndWrite
        StagesOneDeviceSideReadAndWrite RetryKeepsHostTransportBehindStaging
        RejectsXpuHintForPeerSegment UnstagedHostSourceMustCarryHostTransport
        DisabledXpuTransportRoutesByCapability""",
        8,
    )
    | case_names(
        "NetworkAndDispatchModes/XpuLocalDeviceCopyTest", "SameDevice AcrossDevices", 8
    )
    | case_names(
        "DispatchModes/XpuIpcEngineTest",
        """DeviceToPeerDeviceReadAndWrite HostToPeerDeviceReadAndWrite XpuHintForPeerSegmentIsHonoured
        ImportsArePerLocalDevice FailoverRouteIsStaged PolicyWithoutXpuStaysStaged
        StaleSnapshotKeepsNewerImports ClosingSegmentReleasesImports UnpublishedPeerBufferIsReleased
        InteriorOffsetsRelocateInsideMapping PeerHostBufferStaysStaged PeerWithoutIpcStaysStaged""",
        4,
    )
    | case_names(
        "SelectionModes/XpuIoUringFileTest",
        "DeviceWriteReachesFile DeviceReadReturnsFileContents",
        2,
    )
)


RDMA = case_names(
    "SelectionModes/XpuDirectRdmaTest",
    """DeviceToDeviceReadAndWrite OneDeviceSideReadAndWrite HostTransportHintKeepsStaging
    HostOnlyPolicyKeepsStaging""",
    2,
)


INAPPLICABLE = {
    **{
        f"NetworkAndDispatchModes/XpuEngineTest.RetryKeepsHostTransportBehindStaging/{i}": "TCP recovers inside the device stage"
        for i in range(4)
    },
    **{
        f"NetworkAndDispatchModes/XpuEngineTest.DisabledXpuTransportRoutesByCapability/{i}": "HP TCP alone cannot register device memory"
        for i in range(4, 8)
    },
    **{
        f"DispatchModes/XpuIpcEngineTest.PolicyWithoutXpuStaysStaged/{i}": "Policies apply to selector mode only"
        for i in (2, 3)
    },
}


IPC = case_names(
    "SelectionModes/XpuIpcProcessTest",
    "DeviceToPeerDeviceReadAndWrite HostToPeerDeviceReadAndWrite",
    2,
) | case_names(
    "SelectionModes/XpuIpcFallbackTest",
    "DeviceToPeerDeviceFallsBackToStaging HostToPeerDeviceFallsBackToStaging",
    2,
)


STORE = {"RealClientTest.XpuDeviceBufferPutGetRoundTrips"}


SUITES = {
    "tent_xpu_platform_test": (PLATFORM, PLATFORM),
    "tent_xpu_transport_test": (TRANSPORT, TRANSPORT),
    "tent_xpu_engine_test": (ENGINE | RDMA, ENGINE - INAPPLICABLE.keys()),
    "tent_xpu_ipc_process_test": (IPC, IPC),
    "pybind_client_test": (STORE, STORE),
}


def parse_inventory(text: str) -> set[str]:
    suite = ""
    names = []
    for line in text.splitlines():
        value = line.split("#", 1)[0].strip()
        if not value:
            continue
        if not line.startswith(" ") and value.endswith("."):
            suite = value
        elif line.startswith("  ") and re.fullmatch(r"[A-Za-z0-9_/]+", value):
            require(bool(suite), "Test without suite")
            names.append(suite + value)
    require(
        bool(names) and len(names) == len(set(names)),
        "Empty or duplicate test inventory",
    )
    return set(names)


def validate_report(path: Path, expected: set[str]) -> None:
    cases = ET.parse(path).findall(".//testcase")
    identities = [f"{case.get('classname')}.{case.get('name')}" for case in cases]
    require(
        len(identities) == len(expected) and set(identities) == expected,
        f"Test identity mismatch: {path}",
    )
    for case in cases:
        require(
            (case.get("status"), case.get("result")) == ("run", "completed")
            and not any(
                case.find(tag) is not None for tag in ("failure", "error", "skipped")
            ),
            f"Test failed, disabled or skipped: {case.attrib}",
        )


def command(args: list[str], log: Path, timeout: int = 600) -> str:
    try:
        with log.open("w") as stream:
            subprocess.run(
                args,
                stdout=stream,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=timeout,
            )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        print(f"XPU command failed; last 40 lines of {log}:", file=sys.stderr)
        with log.open() as stream:
            for line in deque(stream, maxlen=40):
                print(line, end="", file=sys.stderr)
        raise
    return log.read_text()


def preflight(output: Path) -> None:
    import torch

    require(sys.version_info[:2] == (3, 12), "Runtime must use CPython 3.12")
    require(torch.__version__.split("+")[0] == "2.13.0", "Unexpected PyTorch runtime")
    require(
        os.environ["ONEAPI_DEVICE_SELECTOR"] == "level_zero:gpu",
        "Level Zero GPU backend required",
    )
    available = torch.xpu.is_available()
    count = torch.xpu.device_count()
    properties = [torch.xpu.get_device_properties(index) for index in range(count)]
    devices = [
        {"index": index, "name": props.name, "memory": props.total_memory}
        for index, props in enumerate(properties)
    ]
    inventory = {
        "devices": devices,
        "available": available,
        "count": count,
        "torch": torch.__version__,
        "python": sys.version,
        "affinity": os.environ["ZE_AFFINITY_MASK"],
        "image": os.environ["XPU_IMAGE"],
        "source_sha": os.environ["XPU_SOURCE_SHA"],
        "verified": False,
    }
    (output / "devices.json").write_text(json.dumps(inventory, indent=2))
    require(
        available and count == 2, f"Exactly two XPU devices required; found {count}"
    )
    for index, props in enumerate(properties):
        require("B60" in props.name, f"Expected B60, found {props.name}")
        tensor = torch.arange(64, device=f"xpu:{index}")
        require(int((tensor + 1).sum().cpu()) == 2080, "XPU execution failed")
        torch.xpu.synchronize(index)
    status = Path("/proc/self/status").read_text()
    match = re.search(r"^CapEff:\s+([0-9a-f]+)$", status, re.MULTILINE)
    if match is None:
        raise RuntimeError("Cannot verify process capabilities")
    caps = int(match.group(1), 16)
    require(
        not caps & (1 << 19),
        "CAP_SYS_PTRACE invalidates permission-denied IPC coverage",
    )
    scope = Path("/proc/sys/kernel/yama/ptrace_scope")
    require(
        not scope.exists() or int(scope.read_text()) <= 1,
        "IPC requires ptrace_scope <= 1",
    )
    inventory["verified"] = True
    (output / "devices.json").write_text(json.dumps(inventory, indent=2))


def store_test(source: Path, report: Path) -> None:
    spec = importlib.util.spec_from_file_location(
        "xpu_store_tensor_test", source / "mooncake-wheel/tests/test_put_get_tensor.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Store test module has no loader")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    test = module.TestDistributedObjectStore("test_xpu_local_copy_paths")
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([test]))
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite", name="StorePython")
    case = ET.SubElement(
        suite,
        "testcase",
        classname="StorePython",
        name="test_xpu_local_copy_paths",
        status="run",
        result="completed",
    )
    if not result.wasSuccessful() or result.skipped or result.testsRun != 1:
        ET.SubElement(case, "failure").text = repr(result.__dict__)
    ET.ElementTree(root).write(report)
    validate_report(report, {"StorePython.test_xpu_local_copy_paths"})


def run(bundle: Path, source: Path, output: Path) -> None:
    selected_suite = os.environ.get("XPU_SUITE", "core")
    require(selected_suite in {"core", "nightly", "applications"}, "Invalid XPU suite")
    verify_hashes(bundle, {"provenance.txt"})
    native = extract_native(bundle, output)
    # Intel PyTorch ships SYCL/UR in its interpreter prefix, not ldconfig.
    os.environ["LD_LIBRARY_PATH"] = (
        f"{native / 'lib'}:{Path(sys.prefix) / 'lib'}:{os.environ.get('LD_LIBRARY_PATH', '')}"
    )
    python = sys.executable
    wheel = next(bundle.glob("*.whl"))
    command(
        [
            python,
            "-m",
            "pip",
            "install",
            "--no-index",
            "--ignore-installed",
            "--find-links",
            str(native / "python-deps"),
            "--target",
            "/tmp/xpu-installed",
            str(wheel),
        ],
        output / "install.log",
    )
    os.environ["PYTHONPATH"] = "/tmp/xpu-installed"
    command(
        [
            python,
            __file__,
            "applications" if selected_suite == "applications" else "suite",
            str(native),
            str(source),
            str(output),
        ],
        output / "suite.log",
        timeout=4000,
    )


def suite(native: Path, source: Path, output: Path) -> None:
    started = time.monotonic()
    python = sys.executable
    preflight(output)
    command([python, "-m", "pip", "freeze"], output / "runtime-packages.log")
    command([python, __file__], output / "wheel.log")
    (output / "selection.json").write_text(
        json.dumps(
            {
                "required": {
                    name: sorted(selected) for name, (_, selected) in SUITES.items()
                },
                "excluded": {
                    **dict.fromkeys(sorted(RDMA), "No RDMA hardware"),
                    **INAPPLICABLE,
                },
                "outside_scope": [
                    "GDS",
                    "multi-node",
                    "EP > 2 ranks",
                    "vLLM",
                ],
            },
            indent=2,
        )
    )
    for name, (inventory, selected) in SUITES.items():
        binary = str(native / name)
        libraries = command(["ldd", binary], output / f"{name}.ldd")
        require("not found" not in libraries, f"Missing native dependencies: {name}")
        discovered = parse_inventory(
            command([binary, "--gtest_list_tests"], output / f"{name}.list")
        )
        require(
            discovered == parse_inventory((native / f"{name}.list").read_text()),
            f"Build/runtime inventory differs: {name}",
        )
        require(
            inventory <= discovered
            if name == "pybind_client_test"
            else inventory == discovered,
            f"Unexpected test inventory: {name}",
        )
        report = output / f"{name}.xml"
        command(
            [
                binary,
                f"--gtest_filter={':'.join(sorted(selected))}",
                f"--gtest_output=xml:{report}",
            ],
            output / f"{name}.log",
            timeout=1200,
        )
        validate_report(report, selected)

    package = Path(
        metadata.distribution("mooncake-transfer-engine-xpu").locate_file("mooncake")
    )
    master_log = (output / "master.log").open("w")
    with master_log, subprocess.Popen(
        [str(package / "mooncake_master"), "--port=50051"],
        stdout=master_log,
        stderr=subprocess.STDOUT,
    ) as master:
        try:
            for _ in range(100):
                require(master.poll() is None, "Store master exited before readiness")
                try:
                    with socket.create_connection(("127.0.0.1", 50051), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Store master did not start")
            command(
                [
                    python,
                    __file__,
                    "store",
                    str(source),
                    str(output / "store-python.xml"),
                ],
                output / "store-python.log",
            )
            validate_report(
                output / "store-python.xml", {"StorePython.test_xpu_local_copy_paths"}
            )
        finally:
            master.terminate()
            try:
                master.wait(timeout=10)
            except subprocess.TimeoutExpired:
                master.kill()
                master.wait()
                raise
    nightly = os.environ.get("XPU_SUITE", "core") == "nightly"
    max_tokens, hidden, experts, topk, rounds = (
        (128, 4096, 16, 4, 24) if nightly else (32, 1024, 8, 2, 8)
    )
    ep = command(
        [
            python,
            str(source / "python/tests/ep/test_mooncake_ep_xpu.py"),
            "--num-ranks",
            "2",
            "--max-tokens",
            str(max_tokens),
            "--hidden",
            str(hidden),
            "--num-experts",
            str(experts),
            "--num-topk",
            str(topk),
            "--rounds",
            str(rounds - 3),
        ],
        output / "ep.log",
        timeout=1200,
    )
    require(
        re.findall(r"^round (\d+): .* ok$", ep, re.MULTILINE)
        == [str(i) for i in range(rounds)],
        "Missing EP correctness rounds",
    )
    expected = (
        f"PASSED: {rounds} rounds + 2 empty-rank rounds + 4 two-batch rounds + duplicate experts"
        " + reused src_info + rejected handle + cross-stream + fallback agreement"
        f" on 2 ranks (hidden={hidden}, experts={experts}, topk={topk})"
    )
    require(
        ep.splitlines().count(expected) == 1, "Missing native two-rank EP completion"
    )
    (output / "result.json").write_text(
        json.dumps(
            {
                "source_sha": os.environ["XPU_SOURCE_SHA"],
                "status": "passed",
                "cpp_cases": sum(len(selected) for _, selected in SUITES.values()),
                "store_python_cases": 1,
                "ep_ranks": 2,
                "suite": "nightly" if nightly else "core",
                "ep_rounds": rounds,
                "ep_max_tokens": max_tokens,
                "ep_hidden": hidden,
                "ep_experts": experts,
                "ep_topk": topk,
                "elapsed_seconds": time.monotonic() - started,
            },
            indent=2,
        )
    )


def validate_application_report(path: Path, scenario: str) -> dict:
    report = json.loads(path.read_text())
    require(report["scenario"] == scenario, "Wrong application scenario")
    require(
        bool(report["tokens"]) and report["tokens"] == report["reference_tokens"],
        "Application returned empty or mismatched tokens",
    )
    require(
        report["saved"] > 0
        and report["loaded"] > 0
        and report["save_failed"] == report["load_failed"] == 0,
        "Missing KV save/load or failed keys",
    )
    if scenario == "pd":
        require(report["saved"] == report["loaded"], "PD key counts differ")
    memory = [report["memory"]]
    if scenario == "pd":
        memory.append(report["prefill_memory"])
    for item in memory:
        require(
            "B60" in item["device"]
            and item["peak_reserved_bytes"] >= item["peak_allocated_bytes"] > 0,
            "Missing B60 memory measurements",
        )
    return report


def applications(native: Path, source: Path, output: Path) -> None:
    started = time.monotonic()
    os.environ.update(APPLICATION_THREAD_ENV)
    (output / "cpu-resources.json").write_text(
        json.dumps(
            {
                "logical_cpus": os.cpu_count(),
                "cpu_affinity": sorted(os.sched_getaffinity(0)),
                "thread_environment": APPLICATION_THREAD_ENV,
            },
            indent=2,
        )
    )
    preflight(output)
    require(os.environ["XPU_IMAGE"] == APPLICATION_IMAGE, "Unexpected vLLM image")
    command([sys.executable, "-m", "pip", "freeze"], output / "runtime-packages.log")
    vllm_version = metadata.version("vllm")
    require(
        vllm_version == APPLICATION_VLLM_VERSION,
        f"Unexpected vLLM version: {vllm_version}; expected {APPLICATION_VLLM_VERSION}",
    )
    command([sys.executable, __file__], output / "wheel.log")
    model = native / "model"
    require(
        json.loads((model / "model-provenance.json").read_text())
        == {"model": MODEL_ID, "revision": MODEL_REVISION},
        "Unexpected model revision",
    )
    require(bool(list(model.glob("*.safetensors"))), "Missing model weights")
    os.environ.update(
        HF_HUB_OFFLINE="1",
        TRANSFORMERS_OFFLINE="1",
        HF_HOME="/tmp/huggingface",
        VLLM_CACHE_ROOT="/tmp/vllm",
        MC_FORCE_TCP="1",
        MC_TENT_CONF=json.dumps({"transports": {"xpu": {"disable_ipc": True}}}),
        TMPDIR=str(output),
    )
    master = Path(
        metadata.distribution("mooncake-transfer-engine-xpu").locate_file(
            "mooncake/mooncake_master"
        )
    )
    results = {}
    for scenario, script, completion in (
        (
            "store",
            "vllm_store_smoke.py",
            "PASS: vLLM-XPU prefill -> Mooncake Store -> decode",
        ),
        (
            "pd",
            "vllm_store_pd_smoke.py",
            "PASS: vLLM-XPU prefill (XPU A) -> Mooncake Store -> decode (XPU B)",
        ),
    ):
        report = output / f"{scenario}.json"
        args = [
            sys.executable,
            str(source / "scripts/xpu" / script),
            "--model",
            str(model),
            "--master-bin",
            str(master),
            "--protocol",
            "tcp",
            "--report",
            str(report),
        ]
        if scenario == "pd":
            args += ["--prefill-xpu", "0", "--decode-xpu", "1"]
        log = command(args, output / f"{scenario}.log", timeout=1500)
        require(
            log.splitlines().count(completion) == 1, f"Missing {scenario} completion"
        )
        results[scenario] = validate_application_report(report, scenario)
    (output / "result.json").write_text(
        json.dumps(
            {
                "source_sha": os.environ["XPU_SOURCE_SHA"],
                "status": "passed",
                "suite": "applications",
                "image": APPLICATION_IMAGE,
                "vllm": vllm_version,
                "model": MODEL_ID,
                "model_revision": MODEL_REVISION,
                "scenarios": results,
                "elapsed_seconds": time.monotonic() - started,
            },
            indent=2,
        )
    )


def cli() -> None:
    if len(sys.argv) == 1:
        main()
        return
    mode, *args = sys.argv[1:]
    if mode == "unpack":
        directory, sha, artifact_id, checksum = args
        unpack(Path(directory), sha, artifact_id, checksum)
    elif mode == "run":
        run(*(Path(arg) for arg in args))
    elif mode == "suite":
        suite(*(Path(arg) for arg in args))
    elif mode == "applications":
        applications(*(Path(arg) for arg in args))
    elif mode == "store":
        store_test(*(Path(arg) for arg in args))
    elif mode == "seccomp":
        prepare_seccomp(Path(args[0]))
    else:
        raise RuntimeError(f"Unknown XPU verification command: {mode}")


if __name__ == "__main__":
    cli()
