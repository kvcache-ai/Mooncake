import torch
import torch_npu
import sys
import time
import os
rank = int(sys.argv[1])
torch.npu.set_device(rank)

from mooncake.engine import TransferEngine
te = TransferEngine()
port = 12345+rank
te.initialize(f"127.0.0.1:{port}", "P2PHANDSHAKE", "ascend", "")

rank = int(sys.argv[1])
tensor = torch.ones(16, 61, 144*1024, dtype=torch.int8).npu()
print(f"init tensor:{tensor.cpu()}")
size = 16*61*144*1024
local_addr = tensor.data_ptr()
print(f"local addr:{local_addr}")
te.register_memory(local_addr, size)

if rank == 1:
    local_port = te.get_rpc_port()
    with open("port.txt", "w") as ff:
        ff.writelines([str(local_port)])

remote_port=None
while True:
    if os.path.exists("port.txt"):
        with open("port.txt", "r") as ff:
            lines = ff.readlines()
            if len(lines) == 0:
                continue
            remote_port=int(lines[0].strip())
        break
print(f"remote port:{remote_port}")

import torch.distributed as dist
os.environ['MASTER_ADDR'] = "localhost"
os.environ['MASTER_PORT'] = "29000"
dist.init_process_group(
    backend='gloo',
    rank=rank,
    world_size=2
)
dist.barrier()

import time
if rank==1:
    time.sleep(3)
    print(f"final tensor:{tensor.cpu()}")
    exit(0)
remote_seg = "127.0.0.1:"+str(remote_port)
remote_addr = te.get_first_buffer_address(remote_seg)
print(f"remote addr:{remote_addr}")
local_addrs = []
remote_addrs = []
sizes = []
for i in range(16):
    local_addrs.append(local_addr+i*61*144*1024)
    remote_addrs.append(remote_addr+i*61*144*1024)
    sizes.append(61*144*1024)
ret = te.batch_transfer_sync_write(remote_seg, local_addrs, remote_addrs, sizes)
print(f"ret:{ret}")