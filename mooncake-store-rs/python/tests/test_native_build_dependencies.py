"""Exercise native shim builds with explicit CMake source/build paths."""

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
CRATE = ROOT / "crates/mooncake-transport-sys"
WHEEL_SCRIPT = ROOT / "scripts/build/build-wheel.sh"
DOCKER_WRAPPER = ROOT / "scripts/build/build-wheel-ubuntu-docker.sh"


def test_native_shims_use_explicit_paths_without_configuring_cmake(tmp_path):
    commands = tmp_path / "commands"
    commands.mkdir()
    driver = (
        f"#!{sys.executable}\n"
        + r"""
import os
from pathlib import Path
import sys

args = sys.argv[1:]
root = Path(os.environ["NATIVE_TEST_ROOT"])
with (root / "commands.log").open("a") as log:
    log.write(Path(sys.argv[0]).name + "\n")
if Path(sys.argv[0]).name == "cmake":
    sys.exit(97)

includes = [Path(args[i + 1]) for i, arg in enumerate(args) if arg == "-I"]
upstream = root / "upstream"
build = root / "build"
ylt = build / "_deps/yalantinglibs-src/include"
assert upstream / "mooncake-transfer-engine/include" in includes
assert upstream / "mooncake-common/include" in includes
assert ylt in includes
assert ylt / "ylt/thirdparty" in includes
assert ylt / "ylt/standalone" in includes
source = Path(args[-3])
if source.name == "tent_shim.cc":
    assert upstream / "mooncake-transfer-engine/tent/include" in includes
output = Path(args[args.index("-o") + 1])
output.parent.mkdir(parents=True, exist_ok=True)
output.touch()
"""
    )
    for command in ("cmake", "c++"):
        path = commands / command
        path.write_text(driver)
        path.chmod(0o755)

    harness = tmp_path / "native_test.rs"
    harness.write_text(
        f'include!(r#"{CRATE / "build.rs"}"#);\n'
        + r"""
use std::fs;

#[test]
fn consumes_explicit_paths_and_never_runs_cmake() {
    let root = PathBuf::from(env::var_os("NATIVE_TEST_ROOT").unwrap());
    let upstream = root.join("upstream");
    let build = root.join("build");
    let out = root.join("out");
    fs::create_dir_all(&upstream).unwrap();
    fs::create_dir_all(&build).unwrap();
    fs::create_dir_all(&out).unwrap();

    for key in ["MOONCAKE_ROOT_DIR", "MOONCAKE_BUILD_DIR"] {
        env::remove_var(key);
        assert!(std::panic::catch_unwind(|| required_directory(key)).is_err());
        env::set_var(key, "");
        assert!(std::panic::catch_unwind(|| required_directory(key)).is_err());
    }

    env::set_var("MOONCAKE_ROOT_DIR", &upstream);
    env::set_var("MOONCAKE_BUILD_DIR", &build);
    env::set_var("MOONCAKE_SKIP_CLASSIC_TE", "0");
    env::set_var("MOONCAKE_SKIP_NATIVE_BUILD", "0");
    env::set_var("OUT_DIR", &out);
    main();

    let commands = fs::read_to_string(root.join("commands.log")).unwrap();
    assert_eq!(commands.lines().collect::<Vec<_>>(), ["c++", "c++"]);
    assert!(out.join("libmooncake_classic_shim.so").is_file());
    assert!(out.join("libmooncake_tent_shim.so").is_file());
}
"""
    )
    binary = tmp_path / "native_test"
    subprocess.run(
        ["rustc", "--edition=2021", "--test", str(harness), "-o", str(binary)],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [str(binary)],
        env={
            **os.environ,
            "PATH": str(commands) + os.pathsep + os.environ["PATH"],
            "NATIVE_TEST_ROOT": str(tmp_path),
            "CARGO_MANIFEST_DIR": str(CRATE),
        },
        check=True,
        capture_output=True,
        text=True,
    )


def test_build_wheel_requires_explicit_source_paths(tmp_path):
    store_rs = tmp_path / "store-rs"
    upstream = tmp_path / "upstream"
    build = tmp_path / "build"
    (store_rs / "crates/mooncake-store-py").mkdir(parents=True)
    (upstream / "mooncake-transfer-engine").mkdir(parents=True)
    (upstream / "mooncake-common").mkdir()

    values = {
        "MOONCAKE_STORE_RS_DIR": str(store_rs),
        "MOONCAKE_ROOT_DIR": str(upstream),
        "MOONCAKE_BUILD_DIR": str(build),
    }
    help_result = subprocess.run(
        ["bash", str(WHEEL_SCRIPT), "--help"],
        env={key: value for key, value in os.environ.items() if key not in values},
        capture_output=True,
        text=True,
    )
    assert help_result.returncode == 0
    assert "MOONCAKE_STORE_RS_DIR" in help_result.stdout

    for missing_key in values:
        for missing_value in (None, ""):
            env = {**os.environ, **values}
            if missing_value is None:
                env.pop(missing_key)
            else:
                env[missing_key] = missing_value
            result = subprocess.run(
                ["bash", str(WHEEL_SCRIPT)],
                env=env,
                capture_output=True,
                text=True,
            )
            assert result.returncode != 0
            assert f"{missing_key} must be set" in result.stderr


def test_docker_wrapper_maps_explicit_paths_without_building_an_image(tmp_path):
    upstream = tmp_path / "Mooncake"
    store_rs = upstream / "mooncake-store-rs"
    (store_rs / "crates/mooncake-store-py").mkdir(parents=True)
    docker_dir = tmp_path / "bin"
    docker_dir.mkdir()
    docker_args = tmp_path / "docker-args.json"
    docker = docker_dir / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        + r"""
import json
import os
import sys

args = sys.argv[1:]
if args[:2] == ["image", "inspect"]:
    sys.exit(0)
with open(os.environ["DOCKER_ARGS_PATH"], "w") as output:
    json.dump(args, output)
"""
    )
    docker.chmod(0o755)

    subprocess.run(
        ["bash", str(DOCKER_WRAPPER)],
        env={
            **os.environ,
            "PATH": str(docker_dir) + os.pathsep + os.environ["PATH"],
            "DOCKER_ARGS_PATH": str(docker_args),
            "DOCKER_IMAGE": "test-image",
            "CN_MIRROR": "0",
            "MOONCAKE_STORE_RS_DIR": str(store_rs),
            "MOONCAKE_ROOT_DIR": str(upstream),
        },
        check=True,
        capture_output=True,
        text=True,
    )

    args = json.loads(docker_args.read_text())
    environment = {
        args[index + 1]
        for index, value in enumerate(args[:-1])
        if value == "-e"
    }
    assert "MOONCAKE_STORE_RS_DIR=/work/mooncake-store-rs" in environment
    assert "MOONCAKE_ROOT_DIR=/work" in environment
    assert any(value.startswith("MOONCAKE_BUILD_DIR=/cache/") for value in environment)
    assert "-v" in args
    assert f"{upstream}:/work" in args
    assert args[args.index("-w") + 1] == "/work/mooncake-store-rs"
