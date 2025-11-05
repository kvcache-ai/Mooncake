#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from typing import List, Tuple, Dict, Any, Optional
import torch
from kvbench_common import get_local_ip

class RDMAEngineBackend:
    def __init__(self):
        from mooncake.engine import TransferEngine
        self.te = TransferEngine()
        rc = self.te.initialize(get_local_ip(), "P2PHANDSHAKE", "rdma", "")
        if rc != 0:
            raise RuntimeError(f"TransferEngine init failed: {rc}")

    def register_memory(self, addr: int, size: int):
        self.te.register_memory(addr, size)

    def transfer_write(self, target_addr: str, src_addrs: List[int], dst_addrs: List[int], lengths: List[int]):
        rc = self.te.batch_transfer_sync_write(target_addr, src_addrs, dst_addrs, lengths)
        if rc != 0:
            raise RuntimeError(f"TransferEngine write failed: {rc}")
        torch.cuda.synchronize()


class NIXLBackend:
    def __init__(self):
        from nixl._api import nixl_agent, nixl_agent_config
        self.agent = nixl_agent("initiator", nixl_agent_config(backends=["UCX"]))
        self.remote_name: Optional[str] = None
        self._local_reg = None
        self._local_xfers = None

        self._local_prep_handle: Optional[int] = None
        self._remote_prep_handle_per_rtp: Dict[int, int] = {}

        self._remote_xfers_per_gpu: Optional[List[List[Any]]] = None

    def attach_remote(self, handshake: Dict[str, Any]):
        import base64
        meta_str = handshake["nixl_meta"]
        desc_str = handshake["nixl_xfer_descs"]
        meta = base64.b64decode(meta_str)
        desc = base64.b64decode(desc_str)
        self.remote_name = self.agent.add_remote_agent(meta)
        self.remote_descs = self.agent.deserialize_descs(desc)


    def register_memory(self, ptr: int, size: int):
        self._local_reg = self.agent.get_reg_descs([(ptr, size, 0, "cuda:" + str(0))], "DRAM")
        self.agent.register_memory(self._local_reg)
        
    
    def transfer_write(self, target_addr: str, src_addrs: List[int], dst_addrs: List[int], lengths: List[int]):
        assert self.remote_name, "NIXL: remote agent not attached."
        assert len(src_addrs) == len(dst_addrs) == len(lengths), "length mismatch"
        src_triplets = [(s, l, 0) for s, l in zip(src_addrs, lengths)]
        dst_triplets = [(d, l, 0) for d, l in zip(dst_addrs, lengths)]
        src_descs = self.agent.get_xfer_descs(src_triplets, "DRAM")
        dst_descs = self.agent.get_xfer_descs(dst_triplets, "DRAM")
        xh = self.agent.initialize_xfer(
            "WRITE", src_descs, dst_descs, self.remote_name, b"NIXLWRITE"
        )
        if not xh:
            raise RuntimeError("NIXL initialize_xfer failed")
        state = self.agent.transfer(xh)
        if state == "ERR":
            raise RuntimeError("NIXL transfer error")
        while True:
            s = self.agent.check_xfer_state(xh)
            if s == "ERR":
                raise RuntimeError("NIXL check_xfer_state error")
            if s == "DONE":
                break
            time.sleep(0.001)
        self.agent.release_xfer_handle(xh)
