# Copyright 2025 Huawei Technologies Co., Ltd
# Copyright 2024 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# llmdatadist跨机带宽测速demo，要求两机使用的deviceId相同，PROMPT_IP_LIST和DECODER_IP_LIST改为当前机器NPU deviceIp信息，
# block_size代表pull_blocks一次的数据块大小，预热后拉取20次统计时延和带宽。
# e.g. Prefill侧：python datadist.py --device_id 2 --cluster_id 1 --block_size=64
#      Decode侧: python datadist.py --device_id 2 --cluster_id 2 --block_size=64

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
DECODER_IP_LIST = ['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4',
                  '192.168.1.5', '192.168.1.6', '192.168.1.7', '192.168.1.8']
BLOCK_SIZE = 64 * 1024

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


def init_llm_datadist(role: LLMRole, cluster_id, device_id: int) -> LLMDataDist:
    datadist = LLMDataDist(role, cluster_id)
    llm_config = LLMConfig()
    llm_config.device_id = device_id
    llm_config.enable_cache_manager = True
    llm_options = llm_config.generate_options()
    datadist.init(llm_options)
    return datadist

def link(datadist, device_id):
    rank_table_dict = {
        "server_count": "2",
        "status": "completed",
        "version": "1.0",
        "server_list": [
            {
                "device": [
                    {
                        "device_id": str(device_id),
                        "device_ip": PROMPT_IP_LIST[device_id],
                        "rank_id": "0"
                    }
                ],
                "server_id": "1"
            },
            {
                "device": [
                    {
                        "device_id": str(device_id),
                        "device_ip": DECODER_IP_LIST[device_id],
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
    cache_desc = CacheDesc(num_tensors=1, shape=[20, BLOCK_SIZE // 4], data_type=DataType.DT_FLOAT,
                           placement=Placement.DEVICE)
    tensor = torch.ones(20, BLOCK_SIZE // 4, dtype=torch.float).npu()
    addr = int(tensor.data_ptr())
    cache = cache_manager.register_blocks_cache(cache_desc, [addr])
    logging.info('[register_blocks_cache] success')

    comm_id = link(datadist, device_id)

    # Define src_blocks and dst_blocks here or pass them as arguments to the function.
    src_blocks = list(range(1))
    dst_blocks = list(range(1))

    cache_manager.pull_blocks(BlocksCacheKey(1, 0), cache, src_blocks=src_blocks, dst_blocks=dst_blocks)

    # Start timing
    start_time = time.time()
    for i in range(20):
        cache_manager.pull_blocks(BlocksCacheKey(1, 0), cache, src_blocks=src_blocks, dst_blocks=dst_blocks)

    # End timing
    end_time = time.time()

    # Calculate bandwidth
    # Each block is of size 20 * 1024*1024 elements of float32 (4 bytes each)
    num_blocks = len(src_blocks)  # Assuming src_blocks and dst_blocks have the same length
    element_size = 4  # Size of float32 in bytes
    total_data_transferred = tensor.numel() * element_size  # in bytes
    print(num_blocks, tensor.numel())
    duration = end_time - start_time  # in seconds
    bandwidth = total_data_transferred / duration / (1024 ** 3)  # in GB/s
    print(total_data_transferred)
    logging.info(f"after pull, tensor={tensor}")
    logging.info(f"[pull_blocks] duration: {duration * 1000:.4f} ms, bandwidth: {bandwidth:.2f} GB/s")

    datadist.unlink(comm_id)
    datadist.finalize()


def run_prompt_sample(datadist, device_id: int):
    cache_manager = datadist.cache_manager
    cache_desc = CacheDesc(num_tensors=1, shape=[20, BLOCK_SIZE // 4], data_type=DataType.DT_FLOAT,
                           placement=Placement.DEVICE)
    tensor = torch.ones(20, BLOCK_SIZE // 4, dtype=torch.float).npu()
    addr = int(tensor.data_ptr())
    cache = cache_manager.register_blocks_cache(cache_desc, [addr], BlocksCacheKey(1, 0))
    logging.info('[register_blocks_cache] success')

    comm_id = link(datadist, device_id)
    logging.info('wait for 10 seconds')
    time.sleep(10)
    logging.info('wait ended')
    datadist.unlink(comm_id)
    datadist.finalize()
    logging.info('[finalize] success')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--device_id", type=int, default=0, help='device id')
    parser.add_argument("--cluster_id", type=int, default=1, help='cluster id')
    parser.add_argument("--block_size", type=int, default=64, help='block')

    args = parser.parse_args()
    BLOCK_SIZE = args.block_size * 1024
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
                                                         