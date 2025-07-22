# Copyright 2025 Huawei Technologies Co., Ltd
# Copyright 2024 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Instructions:
# llmdatadist cross-machine bandwidth measurement demo. Change PROMPT_IP_LIST and DECODER_IP_LIST to the NPU deviceIp information of your machines. 
# device_id is the physical card number of the current NPU. 
# target_device_id is the physical card number of the peer NPU.
# block_size is the size of a single data block. block_num is the number of data blocks to be transmitted. 
# block_pattern indicates the continuity of the data blocks. 1 means all transmitted data blocks are continuous. 
#                                                            2 means all transmitted data blocks are discontinuous, sending one block every other block.
# e.g. Prefill：python llmdatadist_bandwidth_test_cross_machine_demo.py --device_id 2 --target_device_id 2 --cluster_id 1 --block_size 64 --block_num 20 --block_pattern 2
#      Decode: python llmdatadist_bandwidth_test_cross_machine_demo.py --device_id 2 --target_device_id 2 --cluster_id 2 --block_size 64 --block_num 20 --block_pattern 2

import argparse
import json
import logging
import time
from llm_datadist import LLMDataDist, LLMRole, LLMConfig, CacheDesc, Cache, DataType, RegisterMemStatus, BlocksCacheKey, \
    Placement
import torch
import torch_npu
import torchair

PROMPT_IP_LIST = ['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4',
                  '192.168.1.5', '192.168.1.6', '192.168.1.7', '192.168.1.8']
DECODER_IP_LIST = ['192.168.2.1', '192.168.2.2', '192.168.2.3', '192.168.2.4',
                  '192.168.2.5', '192.168.2.6', '192.168.2.7', '192.168.2.8']
BLOCK_SIZE = 64 * 1024
TARGRT_DEVICE_ID = 1
BLOCK_NUM = 20
BLOCK_PATTERN = 2

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


def init_llm_datadist(role: LLMRole, cluster_id, device_id: int) -> LLMDataDist:
    datadist = LLMDataDist(role, cluster_id)
    llm_config = LLMConfig()
    llm_config.device_id = device_id
    llm_config.enable_cache_manager = True
    llm_options = llm_config.generate_options()
    datadist.init(llm_options)
    return datadist


def link(datadist, prefill_device_id, decode_device_id):
    rank_table_dict = {
        "server_count": "2",
        "status": "completed",
        "version": "1.0",
        "server_list": [
            {
                "device": [
                    {
                        "device_id": str(prefill_device_id),
                        "device_ip": PROMPT_IP_LIST[prefill_device_id],
                        "rank_id": "0"
                    }
                ],
                "server_id": "1"
            },
            {
                "device": [
                    {
                        "device_id": str(decode_device_id),
                        "device_ip": DECODER_IP_LIST[decode_device_id],
                        "rank_id": "1"
                    }
                ],
                "server_id": "2"
            }
        ]
    }

    cluster_rank_info = {1: 0, 2: 1}
    rank_table = json.dumps(rank_table_dict)
    comm_id = datadist.link("link", cluster_rank_info, rank_table)
    while True:
        ret = datadist.query_register_mem_status(comm_id)
        if ret == RegisterMemStatus.OK:
            logging.info('query_register_mem_status ok')
            break
        elif ret == RegisterMemStatus.FAILED:
            logging.info('query_register_mem_status failed')
            raise RuntimeError("link failed")
        logging.info("need check again")
        time.sleep(1)
    return comm_id


def _allocate_cpu_cache(block_size, num_block, num_tensors):
    cpu_addrs = []
    cpu_tensors = []
    for _ in range(num_tensors):
        kv_tensor = torch.rand(size=(num_block, block_size), dtype=torch.float32, device="cpu")
        cpu_addrs.append(kv_tensor.data_ptr())
        cpu_tensors.append(kv_tensor)
    cpu_cache_desc = CacheDesc(num_tensors=num_tensors, shape=[num_block, block_size],
                               data_type=DataType.DT_FLOAT, placement=Placement.HOST)
    return Cache.create_cpu_cache(cpu_cache_desc, cpu_addrs), cpu_tensors


def run_decoder_sample(datadist, device_id: int):
    cache_manager = datadist.cache_manager
    cache_desc = CacheDesc(num_tensors=1, shape=[BLOCK_NUM, BLOCK_SIZE // 4], data_type=DataType.DT_FLOAT,
                           placement=Placement.DEVICE)
    tensor = torch.ones(BLOCK_NUM, BLOCK_SIZE // 4, dtype=torch.float).npu()
    addr = int(tensor.data_ptr())
    cache = cache_manager.register_blocks_cache(cache_desc, [addr])
    logging.info('[register_blocks_cache] success')

    comm_id = link(datadist, TARGRT_DEVICE_ID, device_id)

    # Define src_blocks and dst_blocks here or pass them as arguments to the function.
    if BLOCK_PATTERN == 1:
        src_blocks = list(range(BLOCK_NUM))
        dst_blocks = list(range(BLOCK_NUM))
    else:
        src_blocks = list(range(0, BLOCK_NUM, BLOCK_PATTERN))
        dst_blocks = list(range(0, BLOCK_NUM, BLOCK_PATTERN))

    cache_manager.pull_blocks(BlocksCacheKey(1, 0), cache, src_blocks=src_blocks, dst_blocks=dst_blocks)

    # Start timing
    start_time = time.time()
    cache_manager.pull_blocks(BlocksCacheKey(1, 0), cache, src_blocks=src_blocks, dst_blocks=dst_blocks)

    # End timing
    end_time = time.time()

    # Calculate bandwidth
    # Each block is of size BLOCK_NUM * 1024*1024 elements of float32 (4 bytes each)
    num_blocks = len(src_blocks)  # Assuming src_blocks and dst_blocks have the same length
    element_size = 4  # Size of float32 in bytes
    total_data_transferred = tensor.numel() * element_size / BLOCK_PATTERN  # in bytes
    print(num_blocks, tensor.numel())
    duration = end_time - start_time  # in seconds
    bandwidth = total_data_transferred / duration / (1000** 3)  # in GB/s
    print(total_data_transferred)
    logging.info(f"after pull, tensor={tensor}")
    logging.info(f"[pull_blocks] duration: {duration * 1000:.4f} ms, bandwidth: {bandwidth:.2f} GB/s")

    # # swap blocks
    # cpu_cache, cpu_tensors = _allocate_cpu_cache(BLOCK_SIZE, BLOCK_NUM, 1)
    # # swap out
    # cache_manager.swap_blocks(cache, cpu_cache, {0: 0, 1: 1})
    # # swap in
    # cache_manager.swap_blocks(cpu_cache, cache, {0: 0, 1: 1})
    datadist.unlink(comm_id)
    datadist.finalize()


def run_prompt_sample(datadist, device_id: int):
    cache_manager = datadist.cache_manager
    cache_desc = CacheDesc(num_tensors=1, shape=[BLOCK_NUM, BLOCK_SIZE // 4], data_type=DataType.DT_FLOAT,
                           placement=Placement.DEVICE)
    tensor = torch.ones(BLOCK_NUM, BLOCK_SIZE // 4, dtype=torch.float).npu()
    addr = int(tensor.data_ptr())
    cache = cache_manager.register_blocks_cache(cache_desc, [addr], BlocksCacheKey(1, 0))
    logging.info('[register_blocks_cache] success')

    comm_id = link(datadist, device_id, TARGRT_DEVICE_ID)
    logging.info('wait for 5 seconds')
    time.sleep(5)
    logging.info('wait ended')
    datadist.unlink(comm_id)
    datadist.finalize()
    logging.info('[finalize] success')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--device_id", type=int, default=0, help='device id')
    parser.add_argument("--target_device_id", type=int, default=1, help='target device id')
    parser.add_argument("--cluster_id", type=int, default=1, help='cluster id')
    parser.add_argument("--block_size", type=int, default=64, help='block')
    parser.add_argument("--block_num", type=int, default=20, help='block')
    parser.add_argument("--block_pattern", type=int, default=2, help='block')
    args = parser.parse_args()
    TARGRT_DEVICE_ID = args.target_device_id
    BLOCK_SIZE = args.block_size * 1024
    BLOCK_NUM = args.block_num * args.block_pattern
    BLOCK_PATTERN = args.block_pattern
    if args.cluster_id not in [1, 2]:
        raise RuntimeError("Not supported cluster id")
    logging.info(f'Sample start, device_id = {args.device_id}, cluster_id = {args.cluster_id}')
    torch.npu.set_device(args.device_id)
    role = LLMRole.PROMPT if args.cluster_id == 1 else LLMRole.DECODER
    datadist = init_llm_datadist(role, args.cluster_id, args.device_id)
    if role == LLMRole.PROMPT:
        run_prompt_sample(datadist, args.device_id)
    else:
        run_decoder_sample(datadist, args.device_id)
    logging.info('Sample end')

