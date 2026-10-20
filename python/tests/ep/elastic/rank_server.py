# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import socket
import time
from collections import defaultdict
from socketserver import StreamRequestHandler, ThreadingTCPServer
from threading import Lock

import torch


# --- Request handler ---
class RankServerHandler(StreamRequestHandler):
    _counts: defaultdict[str, list[int]] = defaultdict(
        list
    )  # List of used local ranks per host
    _global: int = 0
    _lock: Lock = Lock()
    _rank_to_host: dict[
        int, tuple[str, int]
    ] = {}  # Maps global rank to (host, local_rank)
    _user_context: dict[str, str | None] = {}
    _all_global_ranks: set[int] = set()
    _removed_global_ranks: set[int] = set()

    def handle(self):
        with self._lock:
            line = self.rfile.readline().strip().decode()

            if line.startswith("RELEASE_RANK"):
                # Handle rank release - remove from used ranks list
                if len(self._all_global_ranks) > 0:
                    # Find the highest assigned rank and release it
                    rank_to_release = int(line.split()[1])
                    self._all_global_ranks.remove(rank_to_release)
                    self._removed_global_ranks.add(rank_to_release)
                    self._user_context[str(rank_to_release)] = line.split()[2]

                    if rank_to_release in self._rank_to_host:
                        host, local_rank = self._rank_to_host[rank_to_release]
                        # Remove this local rank from the used list
                        if local_rank in self._counts[host]:
                            self._counts[host].remove(local_rank)
                        # Remove the mapping
                        del self._rank_to_host[rank_to_release]
                    self.wfile.write("OK\n".encode())
                else:
                    self.wfile.write("ERROR: No ranks to release\n".encode())
            else:
                # Handle rank assignment - find lowest unused local rank
                host = line
                # Find the lowest unused local rank for this host
                used_ranks = set(self._counts[host])
                local = 0
                while local in used_ranks:
                    local += 1

                # Add this local rank to the used list
                self._counts[host].append(local)
                if len(self._removed_global_ranks) == 0:
                    global_rank = len(self._all_global_ranks)
                else:
                    global_rank = min(self._removed_global_ranks)
                    self._removed_global_ranks.remove(global_rank)

                self._all_global_ranks.add(global_rank)
                # Record which host and local rank this global rank maps to
                self._rank_to_host[global_rank] = (host, local)
                if str(global_rank) not in self._user_context:
                    self._user_context[str(global_rank)] = None
                (
                    self.wfile.write(
                        f"{local} {global_rank} {self._user_context[str(global_rank)]}\n".encode()
                    )
                    if self._user_context[str(global_rank)] is not None
                    else self.wfile.write(f"{local} {global_rank}\n".encode())
                )


# --- TCPServer subclass to reuse port immediately ---
class ReusableTCPServer(ThreadingTCPServer):
    allow_reuse_address = True


# --- Lazy-start server ---
def start_server(port: int = 9999) -> None:
    try:
        server = ReusableTCPServer(("0.0.0.0", port), RankServerHandler)
        server.serve_forever()
    except OSError:
        pass  # another process already started the server


def start_server_process(port: int = 9999):
    server_process = torch.multiprocessing.Process(
        target=start_server, args=(port,), daemon=False
    )
    server_process.start()
    time.sleep(1)
    return server_process


# --- Client API ---
class RankClient:
    def __init__(self, server: str = "127.0.0.1", port: int = 9999):
        self.self_global_rank = None
        self.server = server
        self.port = port

    def get_rank(self):
        if self.self_global_rank is not None:
            print(
                f"WARNING: rank already assigned - returning existing rank {self.self_global_rank}",
                flush=True,
            )
            return self.self_global_rank
        s = socket.create_connection((self.server, self.port))
        s.sendall(f"{os.uname().nodename}\n".encode())
        server_response = s.recv(1024).decode().split()
        if len(server_response) == 2:
            local_rank, global_rank = tuple(map(int, server_response))
            user_context = None
        else:
            local_rank, global_rank, user_context = tuple(map(int, server_response))
        s.close()
        self.self_global_rank = global_rank
        return local_rank, global_rank, user_context

    def release_rank(self, user_context: str | None = None) -> bool:
        """Release a rank (decrement the global counter by 1)"""
        s = socket.create_connection((self.server, self.port))
        s.sendall(
            f"RELEASE_RANK {self.self_global_rank if self.self_global_rank is not None else -1} {user_context}\n".encode()
        )
        response = s.recv(1024).decode().strip()
        s.close()
        self.self_global_rank = None
        return response == "OK"


# --- Example usage ---
if __name__ == "__main__":
    start_server()
