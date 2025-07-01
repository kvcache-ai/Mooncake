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

"""generate hccl config file script"""
import os
import sys
import json
import socket
from argparse import ArgumentParser
from typing import Dict, Any


def parse_args():
    """
    parse args .

    Args:

    Returns:
        args.

    Examples:
        >>> parse_args()
    """
    parser = ArgumentParser(description="mindspore distributed training launch "
                                        "helper utility that will generate hccl"
                                        " config file")
    parser.add_argument("--device_num", type=str, default="[0,16)",
                        help="The number of the Ascend accelerators used. please note that the Ascend accelerators"
                             "used must be continuous, such [0,4) means using four chips "
                             "0，1，2，3; [0,1) means using chip 0; In the most Ascend system, "
                             "the first four chips belong to one group, and the last four chips belong to another one."
                             "Only full chips are allowed to cross-group such as [0,8), other cross-group such as [3,6)"
                             "are prohibited.")
    parser.add_argument("--visible_devices", type=str, default="0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15",
                        help="The visible devices according to the software system. "
                             "Usually used in the virtual system or docker container "
                             "that makes the device_id dismatch logic_id. --device_num uses logic_id. "
                             "For example \"4,5,6,7\" means the system has 4 logic chips "
                             "which are actually the last 4 chips in hardware "
                             "while `--device_num` could only be set to \"[0, 8)\" instead of \"[8, 16)\"")
    parser.add_argument("--server_ip", type=str, default="",
                        help="Set the server_ip manually, to avoid errors in auto detection.")
    args = parser.parse_args()
    return args


def get_host_ip():
    """
    get host ip
    """
    ip = None

    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
    except EOFError:
        pass

    return ip


def main():
    print("start", __file__)
    args = parse_args()

    # visible_devices
    visible_devices = args.visible_devices.split(',')
    print('visible_devices:{}'.format(visible_devices))

    # server_id
    ip = get_host_ip()
    if args.server_ip:
        server_id = args.server_ip
    elif ip:
        server_id = ip
    else:
        raise ValueError("please input server ip!")
    print('server_id:{}'.format(server_id))

    # device_num
    device_str = args.device_num
    first_num, last_num = map(int, device_str[1:-1].split(','))
    if first_num < 0 or last_num > 16:
        raise ValueError("device num {} must be in range [0,8] !".format(args.device_num))
    if first_num > last_num:
        raise ValueError("First num {} of device num {} must less than last num {} !".format(first_num, args.device_num,
                                                                                             last_num))
    if first_num < 8 < last_num:
        if first_num == 0 and last_num == 16:
            pass
        else:
            raise ValueError("device num {} must be in the same group of [0,8] or [8,16] !".format(args.device_num))

    device_num_list = list(range(first_num, last_num))
    print("device_num_list:", device_num_list)

    assert len(visible_devices) >= len(device_num_list)

    # construct hccn_table
    device_ips: Dict[Any, Any] = {}
    try:
        for device_id in device_num_list:
            ret = os.popen("hccn_tool -i %d -ip -g" % device_id).readlines()
            device_ips[str(device_id)] = ret[0].split(":")[1].replace('\n', '')
    except IndexError:
        print("Failed to call hccn_tool, try to read /etc/hccn.conf instead")
        try:
            with open('/etc/hccn.conf', 'r') as fin:
                for hccn_item in fin.readlines():
                    if hccn_item.strip().startswith('address_'):
                        device_id, device_ip = hccn_item.split('=')
                        device_id = device_id.split('_')[1]
                        device_ips[device_id] = device_ip.strip()
        except OSError:
            print("Failed to read /etc/hccn.conf")
            raise SystemError("Failed to find information for hccl")

    hccn_table = {'version': '1.0',
                  'server_count': '1',
                  'server_list': []}
    device_list = []
    rank_id = 0
    for instance_id in device_num_list:
        device_id = visible_devices[instance_id]
        device_ip = device_ips[device_id]
        device = {'device_id': device_id,
                  'device_ip': device_ip,
                  'rank_id': str(rank_id)}
        print('rank_id:{}, device_id:{}, device_ip:{}'.format(rank_id, device_id, device_ip))
        rank_id += 1
        device_list.append(device)
    hccn_table['server_list'].append({
        'server_id': server_id,
        'device': device_list,
        'host_nic_ip': 'reserve'
    })
    hccn_table['status'] = 'completed'

    # save hccn_table to file
    table_path = "/etc"
    table_fn = os.path.join(table_path,
                            'hccl_16p.json')
    with open(table_fn, 'w') as table_fp:
        json.dump(hccn_table, table_fp, indent=4)
    sys.stdout.flush()
    print("Completed: hccl file was save in :", table_fn)


if __name__ == "__main__":
    main()
