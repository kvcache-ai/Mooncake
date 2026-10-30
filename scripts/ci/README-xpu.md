# Intel XPU CI

Implements [RFC #5036](https://github.com/kvcache-ai/Mooncake/issues/5036)
in three stages:

1. **Hosted builds and packaging (this change).** Build on an Ubuntu 24.04
   CPU runner using a pinned Intel PyTorch job container, oneAPI DPC++ 2026.1 and
   CPython 3.12. Build native TENT, Store and EP, package the distinct
   `mooncake-transfer-engine-xpu` wheel, run host-only C++ tests and verify a
   clean wheel installation. Relevant PR updates/main pushes and the nightly
   workflow invoke the same reusable builder. `CI Gate` and `nightly-gate`
   require its success. This is **not GPU coverage**.
2. **Hardware core.** Provision isolated two-GPU B60 allocations; add the
   maintainer-only `run-xpu-ci`/manual trigger for an exact PR head SHA.
   Consume the matching wheel and native bundle, then run non-RDMA C++
   platform/transport/engine/IPC, Store C++/Python and synthetic native EP
   checks. Require actual GPU identity, selected test identities/counts,
   strict skip handling, failure/cancellation propagation and resource cleanup.
3. **Nightly hardware and release readiness.** Expand core and require both
   minimal vLLM scenarios: single-instance Store KV save/load and two-GPU PD.
   Pin the application image/model and collect execution, memory and queue
   measurements. Only after quality validation, integrate the reusable builder
   with the official release workflow and publish the exact verified wheels.

RDMA, GDS, cross-node tests, EP with more than two ranks and performance/model
quality certification remain out of scope. Stage 1 does not add a GPU runner,
`run-xpu-ci` handling, image promotion, PyPI or TestPyPI publication.

## Entry points and checks

- [Reusable workflow](../../.github/workflows/ci_xpu.yml): may also be run
  manually. `checkout_ref` selects a revision for a reusable-workflow caller;
  the resolved SHA, artifact ID and artifact digest are returned as outputs.
- Like [ROCm CI](../../.github/workflows/ci_rocm.yml), the reusable workflow
  runs checkout, dependency installation, CMake, wheel packaging and upload
  directly in its job container. It uses the existing
  [wheel builder](../build_wheel.sh), not a nested Docker build/run or separate
  orchestration scripts. The development Dockerfile is unchanged and is not
  used by CI. The image is declared only in the workflow's `container` field;
  its identity and installed compiler/runtime versions are recorded in provenance.
- Workflow steps build four XPU TENT test executables and `pybind_client_test`
  without executing their hardware tests. They run all 19
  configuration-lifecycle, six EP dispatch-slot and five CRC-32C cases; zero
  tests, missing cases, disabled cases, failures and skips fail the build.
  Update the explicit counts when intentionally adding/removing those host cases.
- [Wheel check](../e2e/python/verify_xpu_wheel.py): runs in a clean virtual
  environment from outside the source directory, without build-library search
  paths or a CMake installation. Installs only the produced wheel (plus its declared
  dependencies), checks distribution RECORD hashes/native imports, XPU/TENT
  capability bits, native XPU EP identity, the C++ Store facade and packaged
  master version/startup. No GPU is passed to the job container.

`version_override` mirrors ROCm's input and stamps nightly wheels without
publishing them. Initial CI tests CPython 3.12 only; no broader Python matrix
is implied.

The initial wheel ABI is CPython 3.12, x86-64, Ubuntu 24.04/glibc 2.39.
It is not a manylinux_2_28 build. Intel SYCL/Level Zero/compiler runtimes are
intentionally excluded from the wheel: consumers must provide the matching
runtime rather than loading a second vendored SYCL runtime beside PyTorch.
The toolchain also supplies `libibverbs`, `librdmacm` and `libnuma` runtime
libraries; these software dependencies remain required without RDMA hardware.
`XPU_BUILD=1 bash scripts/build_wheel.sh` selects the XPU distribution name
after a compatible native build; the public Python import remains `mooncake`.
It is mutually exclusive with the other wheel build variants.

## Artifacts and hardware handoff

Each successful run uploads one uniquely named artifact containing the wheel,
`native-tests.tar.gz`, `SHA256SUMS` and provenance. Consumers
must use the returned artifact ID/digest, verify the expected source SHA and
check the hashes; never fetch a latest/indexed replacement. Actions retains step
logs; host-test XML, available CMake configuration, native test inventories and
provenance are uploaded even on failure.

The native bundle contains the four XPU executables, `pybind_client_test`,
their GoogleTest inventories, resolved dependency inventories and non-core
shared libraries. Intel GPU, glibc and C++ runtimes are supplied by the matching
runtime environment recorded in provenance. After verifying the outer artifact
hashes and extracting into a job-owned directory:

```bash
cd native-tests
sha256sum --check SHA256SUMS
source /opt/intel/oneapi/setvars.sh
export LD_LIBRARY_PATH="$PWD/lib:${LD_LIBRARY_PATH:-}"
export MC_USE_TENT=1
```

Stage 2 must supply its own strict non-RDMA case selection and master/service
lifecycle; do not blindly run the complete binaries or treat their CPU fallback
or skips as successful XPU coverage. A host-only green build does not authorize
release or establish B60 correctness.
