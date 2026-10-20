# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

from datetime import timedelta

import torch.distributed as dist


def create_master_store(
    port: int = 9999,
    timeout_sec: float = 300.0,
) -> dist.TCPStore:
    return dist.TCPStore(
        host_name="0.0.0.0",
        port=port,
        is_master=True,
        wait_for_workers=False,
        timeout=timedelta(seconds=timeout_sec),
    )


def create_client_store(
    master_addr: str = "127.0.0.1",
    port: int = 9999,
    timeout_sec: float = 300.0,
) -> dist.TCPStore:
    return dist.TCPStore(
        host_name=master_addr,
        port=port,
        is_master=False,
        wait_for_workers=False,
        timeout=timedelta(seconds=timeout_sec),
    )
