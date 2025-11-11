from mooncake.engine import TransferEngine
te = TransferEngine()
te.initialize("127.0.0.1:12345", "P2PHANDSHAKE", "tcp", "")

import torch
import sys
import os
rank = int(sys.argv[1])
tensor = torch.ones(1, 61, 144*1024, dtype=torch.int8).cpu() if rank==0 else torch.zeros(1, 61, 144*1024, dtype=torch.int8).cpu()
print(f"init tensor:{tensor}")
size = 61*144*1024
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
            remote_port=int(ff.readlines()[0].strip())
        break
print(f"remote port:{remote_port}")

import torch.distributed as dist
os.environ['MASTER_ADDR'] = "localhost"
os.environ['MASTER_PORT'] = "12345"
dist.init_process_group(
        backend='gloo',
        rank=rank,
        world_size=2
    )
dist.barrier()

import time
if rank==1:
    time.sleep(3)
    print(f"final tensor:{tensor}")
    exit(0)
#remote_seg = "100.125.211.155:"+str(remote_port)
remote_seg = "127.0.0.1:"+str(remote_port)
remote_addr = te.get_first_buffer_address(remote_seg)
print(f"remote addr:{remote_addr}")
ret = te.transfer_sync_write(remote_seg, local_addr, remote_addr, size)
print(f"ret:{ret}")
