# ROCm external PD tests

This is the independent two-node MI350X controller used by
`.github/workflows/integration-test-rocm.yml`. It does not source or execute
`scripts/tone_tests/`: that directory remains the CUDA / T-one controller.
Platform-local copies of the test harness are intentional. Changes to Docker,
RDMA, process tracking, model selection, or teardown must be validated on the
corresponding platform rather than propagated through accelerator switches.

## Runner setup

The workflow runs as a non-root user on a trusted, dedicated self-hosted runner.
Install the host-owned profile from `runner.env.example` as
`/etc/mooncake-ci/runner.env`, with real allocation settings and pinned SSH host
keys. Keep credentials outside the repository. The existing profile location,
remote work directory, allocation lock, image pins, and wheel artifact names
are unchanged by the split.

The workflow performs local/remote preflight checks, locks the allocation,
downloads the Python 3.10/3.12 ROCm wheels, and calls:

```sh
bash scripts/rocm_tests/scripts/run_test.sh run-all
# Or skip SGLang:
bash scripts/rocm_tests/scripts/run_test.sh run-all VLLM
```

Do not invoke the controller outside that lock/preflight lifecycle on a shared
host. `CI_ACCELERATOR=rocm` and `MOONCAKE_CI_TIER=core-4gpu` are required by the
runner profile. Docker receives only the allocated render/RDMA devices and CPU
set; the controller uses the host-matched Ionic provider and image-scoped runtime
cache. Cleanup verifies container removal, GPU memory drain, and KFD processes
before the workflow releases the allocation lock. It never kills host GPU PIDs.

## Coverage

- SGLang HiCache with the ROCm Qwen3 model and accuracy evaluation budget.
- SGLang two-node prefill/decode and encoder/prefill/decode.
- vLLM Mooncake connector serialized smoke coverage (`num_workers=1`), not
  concurrent sender coverage while
  [vLLM #44238](https://github.com/vllm-project/vllm/issues/44238) is unresolved.

CUDA-only Elastic EP and the eight-GPU heterogeneous-TP suite are not included.
Generated state and logs live in `run/`; the workflow uploads `run/logs`.
`python/verify_rocm_wheel.py` is also used by ROCm wheel build/release validation.

The existing mocked regression checks moved with their platform implementation:

```sh
bash scripts/rocm_tests/tests/test_common_cleanup.sh
python3 scripts/rocm_tests/tests/test_hicache_model_selection.py
```

These checks do not replace a real two-node MI350X E2E run.
