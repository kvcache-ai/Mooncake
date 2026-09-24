"""Exercise the native build script with isolated CMake/compiler processes."""

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
CRATE = ROOT / "crates/mooncake-transport-sys"


def test_fresh_native_build_uses_cmake_fetched_headers(tmp_path):
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
build = root / "upstream/build"
with (root / "commands.log").open("a") as log:
    log.write(Path(sys.argv[0]).name + " " + " ".join(args) + "\n")
if Path(sys.argv[0]).name == "cmake":
    assert "--install" not in args
    assert all("yalantinglibs_DIR" not in arg for arg in args)
    if "-S" in args:
        assert Path(args[args.index("-S") + 1]) == root / "upstream"
        header = build / "_deps/yalantinglibs-src/include/ylt/easylog.hpp"
        header.parent.mkdir(parents=True, exist_ok=True)
        header.touch()
    else:
        assert "--build" in args
        for name in ("mooncake-transfer-engine/src/libtransfer_engine.a",
                     "mooncake-transfer-engine/tent/src/libtent_shared.so"):
            artifact = build / name
            artifact.parent.mkdir(parents=True, exist_ok=True)
            artifact.touch()
else:
    includes = [Path(args[i + 1]) for i, arg in enumerate(args) if arg == "-I"]
    ylt = build / "_deps/yalantinglibs-src/include"
    assert ylt in includes
    assert (ylt / "ylt/easylog.hpp").is_file()
    assert ylt / "ylt/thirdparty" in includes
    assert ylt / "ylt/standalone" in includes
    assert root / "upstream/mooncake-common/include" in includes
    Path(args[args.index("-o") + 1]).touch()
"""
    )
    for command in ("cmake", "c++"):
        path = commands / command
        path.write_text(driver)
        path.chmod(0o755)
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    (upstream / "CMakeLists.txt").write_text("# configured by the test driver\n")
    harness = tmp_path / "native_test.rs"
    harness.write_text(
        f'include!(r#"{CRATE / "build.rs"}"#);\n'
        + r"""
#[test]
fn configures_before_shim_compilation() {
    let root = PathBuf::from(env::var_os("NATIVE_TEST_ROOT").unwrap());
    let upstream = root.join("upstream");
    let build = upstream.join("build");
    let out = root.join("out");
    fs::create_dir(&out).unwrap();
    let python = PythonConfig {
        executable: root.join("python"), include_dir: root.join("python/include"),
        library_path: root.join("python/lib/libpython.so"), library_name: "python".into(),
    };
    let jsoncpp = JsonCppConfig {
        include_dir: root.join("jsoncpp/include"), library_path: root.join("jsoncpp/lib.so"),
    };
    ensure_upstream_native_artifacts(&upstream, &build, &python, &jsoncpp);
    build_native_shims(&upstream, &build, &out);
    let log = fs::read_to_string(root.join("commands.log")).unwrap();
    let commands: Vec<_> = log.lines().collect();
    assert!(commands[0].starts_with("cmake -S "));
    assert!(commands[1].starts_with("cmake --build "));
    assert!(commands[2].starts_with("c++ "));
    assert!(commands[3].starts_with("c++ "));
    assert!(commands[0].contains("-DCMAKE_PREFIX_PATH=/custom/deps;/second/deps"));
    let artifacts = [build.join("mooncake-transfer-engine/src/libtransfer_engine.a")];
    assert!(native_artifacts_are_fresh(&upstream, &build, &[&artifacts[0]]));
    fs::remove_file(build.join("_deps/yalantinglibs-src/include/ylt/easylog.hpp")).unwrap();
    assert!(!native_artifacts_are_fresh(&upstream, &build, &[&artifacts[0]]));
    ensure_upstream_native_artifacts(&upstream, &build, &python, &jsoncpp);
    assert!(build.join("_deps/yalantinglibs-src/include/ylt/easylog.hpp").exists());
    let default_root = default_upstream_dir();
    assert_eq!(default_root, default_root.canonicalize().unwrap());
    assert!(default_root.join("mooncake-common/FindYLT.cmake").is_file());
    assert!(ignore_source_path(&store_rs_root().unwrap().join("target"), &build));
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
            "MOONCAKE_EXTRA_CMAKE_PREFIX_PATH": "/custom/deps;/second/deps",
            "MOONCAKE_SKIP_CLASSIC_TE": "0",
        },
        check=True,
        capture_output=True,
        text=True,
    )
