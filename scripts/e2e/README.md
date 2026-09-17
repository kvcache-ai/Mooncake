# E2E test layout

- `scripts/tone_tests/scripts/run_test.sh`: TONE CUDA/ERDMA entry point (unchanged).
- `scripts/rocm_tests/scripts/run_test.sh`: self-hosted ROCm/RoCE entry point.
  The runner profile example is `scripts/rocm_tests/runner.env.example`.
- Each suite owns its configuration, test inventory and `scripts/common.sh`
  platform lifecycle. ROCm supports the `core-4gpu` inventory; TONE uses `full`.
- This directory owns shared cases, Python helpers, assets and orchestration.
  Suite links keep the existing `/test_run` layout. Local containers mount this
  directory read-only at `/e2e`; `rsync -L` materializes links on remote workers,
  which therefore need no repository checkout or extra mount.

Both entries accept `run-all [SGLANG|VLLM]` and `run-single <test_name.sh>`.
Run them from a complete repository checkout, not a copy of just one suite.
Generated environment, wheels and logs remain under the selected suite's `run/`.
Each case runs in a subshell to avoid leaking functions/configuration into the
next case. Setup always refreshes the wheel and environment; it does not reuse
an old `.shrc` as a cache key.

ROCm retains its allocation-scoped devices, pinned SSH configuration, per-image
runtime caches, between-case reset, and fail-closed cleanup/postflight. A failed
reset or postflight stops scheduling further work. The workflow holds the
allocation lock until final cleanup completes.

CPU-only regression tests:

```bash
bash scripts/tone_tests/tests/test_common_cleanup.sh
python3 -m unittest discover -s scripts/tone_tests/tests -p 'test_*.py'
```
