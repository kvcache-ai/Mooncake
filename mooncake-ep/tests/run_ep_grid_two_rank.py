"""Run a bounded two-rank EP dispatch/combine correctness gate."""

import argparse
import os

import torch.multiprocessing as mp

from test_ep_grid import worker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--disable-p2p", action="store_true")
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--hidden", type=int, default=2048)
    parser.add_argument("--num-experts", type=int, default=256)
    parser.add_argument("--top-k", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ.setdefault("MASTER_PORT", "29664")
    if args.disable_p2p:
        os.environ["MOONCAKE_EP_DISABLE_P2P"] = "1"

    config = {
        "max_tokens": args.max_tokens,
        "hidden": args.hidden,
        "num_experts": args.num_experts,
        "top_k": args.top_k,
        "use_fp8": False,
        "zero_copy": False,
        "async_finish": False,
        "return_recv_hook": True,
        "use_fallback": False,
        "fail_rank": -1,
    }
    mp.spawn(worker, args=(2, config), nprocs=2, join=True, daemon=False)
    print("MOONCAKE_EP_GRID_2_RANK_OK", flush=True)


if __name__ == "__main__":
    main()
