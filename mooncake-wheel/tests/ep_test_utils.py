import json
import os
import re
import sys
import tempfile
import numpy as np
import torch
import torch.distributed as dist
from pathlib import Path
from typing import Optional

from mooncake import pg


def init_dist(local_rank: int, num_local_ranks: int):
    # NOTES: you may rewrite this function with your own cluster settings
    ip = os.getenv('MASTER_ADDR', '127.0.0.1')
    port = int(os.getenv('MASTER_PORT', '8361'))
    num_nodes = int(os.getenv('WORLD_SIZE', 1))
    node_rank = int(os.getenv('RANK', 0))
    assert (num_local_ranks < 8 and num_nodes == 1) or num_local_ranks == 8

    torch.cuda.set_device(local_rank)
    pg.set_host_ip("10.12.11.242")
    dist.init_process_group(
        backend='mooncake',
        init_method=f'tcp://{ip}:{port}',
        world_size=num_nodes * num_local_ranks,
        rank=node_rank * num_local_ranks + local_rank
    )
    torch.set_default_dtype(torch.bfloat16)
    torch.set_default_device('cuda')

    return dist.get_rank(), dist.get_world_size(), dist.new_group(list(range(num_local_ranks * num_nodes))), dist.new_group(list(range(num_local_ranks * num_nodes)), backend="mooncake-cpu")


def calc_diff(x: torch.Tensor, y: torch.Tensor):
    x, y = x.double() + 1, y.double() + 1
    denominator = (x * x + y * y).sum()
    sim = 2 * (x * y).sum() / denominator
    return (1 - sim).item()


def per_token_cast_to_fp8(x: torch.Tensor):
    assert x.dim() == 2 and x.size(1) % 128 == 0
    m, n = x.shape
    x_view = x.view(m, -1, 128)
    x_amax = x_view.abs().float().amax(dim=2).view(m, -1).clamp(1e-4)
    return (x_view * (448.0 / x_amax.unsqueeze(2))).to(torch.float8_e4m3fn).view(m, n), (x_amax / 448.0).view(m, -1)


def per_token_cast_back(x_fp8: torch.Tensor, x_scales: torch.Tensor):
    x_fp32 = x_fp8.to(torch.float32).view(x_fp8.size(0), -1, 128)
    x_scales = x_scales.view(x_fp8.size(0), -1, 1)
    return (x_fp32 * x_scales).view(x_fp8.shape).to(torch.bfloat16)


def inplace_unique(x: torch.Tensor, num_slots: int):
    assert x.dim() == 2
    mask = x < 0
    x_padded = x.masked_fill(mask, num_slots)
    bin_count = torch.zeros((x.size(0), num_slots + 1), dtype=x.dtype, device=x.device)
    bin_count.scatter_add_(1, x_padded, torch.ones_like(x_padded))
    bin_count = bin_count[:, :num_slots]
    sorted_bin_count, sorted_bin_idx = torch.sort(bin_count, dim=-1, descending=True)
    sorted_bin_idx.masked_fill_(sorted_bin_count == 0, -1)
    sorted_bin_idx = torch.sort(sorted_bin_idx, descending=True, dim=-1).values
    x[:, :].fill_(-1)
    valid_len = min(num_slots, x.size(1))
    x[:, :valid_len] = sorted_bin_idx[:, :valid_len]


def create_grouped_scores(scores: torch.Tensor, group_idx: torch.Tensor, num_groups: int):
    num_tokens, num_experts = scores.shape
    scores = scores.view(num_tokens, num_groups, -1)
    mask = torch.zeros((num_tokens, num_groups), dtype=torch.bool, device=scores.device)
    mask = mask.scatter_(1, group_idx, True).unsqueeze(-1).expand_as(scores)
    return (scores * mask).view(num_tokens, num_experts)


def bench(fn, num_warmups: int = 20, num_tests: int = 30, post_fn=None):
    # Flush L2 cache with 256 MB data
    torch.cuda.synchronize()
    cache = torch.empty(int(256e6 // 4), dtype=torch.int, device='cuda')

    # Warmup
    for _ in range(num_warmups):
        fn()

    # Flush L2
    cache.zero_()

    # Testing
    start_events = [torch.cuda.Event(enable_timing=True) for _ in range(num_tests)]
    end_events = [torch.cuda.Event(enable_timing=True) for _ in range(num_tests)]
    for i in range(num_tests):
        # Record
        start_events[i].record()
        fn()
        end_events[i].record()
        if post_fn is not None:
            post_fn()
    torch.cuda.synchronize()

    times = np.array([s.elapsed_time(e) / 1e3 for s, e in zip(start_events, end_events)])[1:]
    return np.average(times), np.min(times), np.max(times)


class empty_suppress:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class suppress_stdout_stderr:
    def __enter__(self):
        self.outnull_file = open(os.devnull, 'w')
        self.errnull_file = open(os.devnull, 'w')

        self.old_stdout_fileno_undup = sys.stdout.fileno()
        self.old_stderr_fileno_undup = sys.stderr.fileno()

        self.old_stdout_fileno = os.dup(sys.stdout.fileno())
        self.old_stderr_fileno = os.dup(sys.stderr.fileno())

        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr

        os.dup2(self.outnull_file.fileno(), self.old_stdout_fileno_undup)
        os.dup2(self.errnull_file.fileno(), self.old_stderr_fileno_undup)

        sys.stdout = self.outnull_file
        sys.stderr = self.errnull_file
        return self

    def __exit__(self, *_):
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

        os.dup2(self.old_stdout_fileno, self.old_stdout_fileno_undup)
        os.dup2(self.old_stderr_fileno, self.old_stderr_fileno_undup)

        os.close(self.old_stdout_fileno)
        os.close(self.old_stderr_fileno)

        self.outnull_file.close()
        self.errnull_file.close()


def bench_kineto(fn, kernel_names, num_tests: int = 30, suppress_kineto_output: bool = False,
                 trace_path: Optional[str] = None, barrier_comm_profiling: bool = False,
                 num_kernels_per_period: int = 1, profile_enabled: bool = True):
    assert isinstance(kernel_names, str) or isinstance(kernel_names, tuple)
    is_tupled = isinstance(kernel_names, tuple)
    kernel_names = (kernel_names, ) if isinstance(kernel_names, str) else kernel_names
    assert all([isinstance(name, str) for name in kernel_names])
    assert num_kernels_per_period >= 1

    def run_profile_body(prof=None):
        dummy = torch.ones(1, dtype=torch.float, device='cuda')
        for _ in range(2):
            for _ in range(num_tests):
                # Match DeepEP's profile window: each measured operation starts
                # after an L2 flush and a device-side cross-rank rendezvous.
                torch.empty(int(256e6 // 4), dtype=torch.int, device='cuda').zero_()
                if barrier_comm_profiling:
                    torch.cuda._sleep(int(2e7))
                    dist.all_reduce(dummy)
                fn()
            torch.cuda.synchronize()
            if prof is not None:
                prof.step()

    # Prime lazy initialization before the profiler schedule begins.
    fn()
    torch.cuda.synchronize()

    if not profile_enabled:
        run_profile_body()
        kernel_times = [
            [0.0] * num_kernels_per_period
            if num_kernels_per_period > 1 else 0.0
            for _ in kernel_names
        ]
        return tuple(kernel_times) if is_tupled else kernel_times[0]

    # Profile
    suppress = suppress_stdout_stderr if suppress_kineto_output else empty_suppress
    with suppress():
        schedule = torch.profiler.schedule(wait=0, warmup=1, active=1, repeat=1)
        with torch.profiler.profile(
            activities=[torch.profiler.ProfilerActivity.CUDA],
            schedule=schedule,
            acc_events=True,
        ) as prof:
            run_profile_body(prof)

    # Save chrome traces
    if trace_path is not None:
        prof.export_chrome_trace(trace_path)

    # Return average kernel times for the normal, single-kernel path.
    kernel_times = []
    events = prof.key_averages()
    sort_by = "cuda_time_total" if any(
        float(getattr(event, "cuda_time_total", 0.0)) > 0 for event in events
    ) else "device_time_total"
    prof_lines = prof.key_averages().table(sort_by=sort_by, max_name_column_width=160).split('\n')

    def cuda_time_us(event):
        cuda_time = float(getattr(event, "cuda_time_total", 0.0))
        if cuda_time > 0:
            return cuda_time
        return float(getattr(event, "device_time_total", 0.0))

    def table_avg_time_s(name):
        for line in prof_lines:
            if name not in line:
                continue
            time_matches = re.findall(r"([0-9]+(?:\.[0-9]+)?)(ns|us|ms|s)", line)
            if len(time_matches) == 0:
                continue
            value, unit = time_matches[-1]
            value = float(value)
            if unit == "ms":
                return value / 1e3
            if unit == "us":
                return value / 1e6
            if unit == "ns":
                return value / 1e9
            if unit == "s":
                return value
        return 0.0

    for name in kernel_names:
        matched_events = [event for event in events if name in event.key and cuda_time_us(event) > 0]
        if len(matched_events) == 0:
            fallback_time = table_avg_time_s(name)
            if fallback_time > 0:
                kernel_times.append(fallback_time)
                continue
            import warnings
            warnings.warn(f'Kernel {name} was not captured by Kineto', UserWarning)
            kernel_times.append(0.0)
            continue

        total_cuda_time_us = sum(cuda_time_us(event) for event in matched_events)
        total_count = sum(getattr(event, "count", 0) for event in matched_events)
        assert total_count > 0
        kernel_times.append(total_cuda_time_us / total_count / 1e6)

    if num_kernels_per_period > 1:
        with tempfile.NamedTemporaryFile(suffix='.json') as trace_file:
            prof.export_chrome_trace(trace_file.name)
            trace_events = json.loads(Path(trace_file.name).read_text()).get('traceEvents', [])

        period_times = []
        for name in kernel_names:
            durations = [
                event['dur'] / 1e6
                for event in trace_events
                if f'::{name}' in event.get('name', '') and 'dur' in event
            ]
            if len(durations) == 0 or len(durations) % num_kernels_per_period:
                import warnings
                warnings.warn(
                    f'Kineto captured {len(durations)} {name} kernels; expected a multiple of '
                    f'{num_kernels_per_period} for hook phase timing',
                    UserWarning,
                )
                period_times.append([0.0] * num_kernels_per_period)
                continue
            period_count = len(durations) // num_kernels_per_period
            period_times.append([
                sum(durations[offset::num_kernels_per_period]) / period_count
                for offset in range(num_kernels_per_period)
            ])
        kernel_times = period_times

    if os.getenv("MOONCAKE_EP_KINETO_DUMP", "").upper() in {"1", "ON", "TRUE", "YES"}:
        for name, kernel_time in zip(kernel_names, kernel_times):
            event_matches = [
                (event.key, cuda_time_us(event), getattr(event, "count", 0))
                for event in events if name in event.key
            ]
            print(f"[bench_kineto] name={name!r} result_s={kernel_time} event_matches={event_matches}", flush=True)
        print("\n".join(prof_lines[:80]), flush=True)

    return tuple(kernel_times) if is_tupled else kernel_times[0]


def hash_tensor(t: torch.Tensor):
    return t.view(torch.int64).sum().item()
