# /usr/bin/python
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

import etcd3
import sys
import json
import os
import socket

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python mount.py <etcd_server> <segment_name> <file_path> ...")
        sys.exit(1)

    os.environ.pop("HTTP_PROXY", None)
    os.environ.pop("HTTPS_PROXY", None)
    os.environ.pop("http_proxy", None)
    os.environ.pop("https_proxy", None)
    etcd_host = sys.argv[1]
    segment_name = "mooncake/nvmeof/" + sys.argv[2]
    files = sys.argv[3:]
    local_server_name = socket.gethostname()

    server_name = socket.gethostname()
    etcd = etcd3.client(host=etcd_host, port=2379)
    value = {}
    value['server_name'] = server_name
    value['protocol'] = "nvmeof"
    value['buffers'] = []
    for file in files:
      # TODO: check file path existence
      buffer = {}
      buffer['length'] = os.path.getsize(file)
      buffer['file_path'] = file
      local_path_map = {}
      local_path_map[server_name] = file
      buffer['local_path_map'] = local_path_map
      value['buffers'].append(buffer)
    
    print(json.dumps(value))
    etcd.put(segment_name, json.dumps(value))

