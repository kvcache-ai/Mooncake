# SPDX-License-Identifier: Apache-2.0
"""Minimal connector ABCs for remote model weight I/O (engine-agnostic)."""

from __future__ import annotations

import os
import shutil
import signal
import tempfile
from abc import ABC, abstractmethod
from typing import Generator, List, Optional, Tuple

import torch


class BaseConnector(ABC):
    """Base remote connector with a temporary local scratch directory."""

    def __init__(self, url: str):
        self.url = url
        self.closed = False
        self.local_dir = tempfile.mkdtemp()
        for sig in (signal.SIGINT, signal.SIGTERM):
            existing_handler = signal.getsignal(sig)
            signal.signal(sig, self._close_by_signal(existing_handler))

    def get_local_dir(self) -> str:
        return self.local_dir

    @abstractmethod
    def weight_iterator(
        self, rank: int = 0
    ) -> Generator[Tuple[str, torch.Tensor], None, None]:
        raise NotImplementedError()

    @abstractmethod
    def pull_files(
        self,
        allow_pattern: Optional[List[str]] = None,
        ignore_pattern: Optional[List[str]] = None,
    ) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def _close_by_signal(self, existing_handler=None):
        def new_handler(signum, frame):
            self.close()
            if callable(existing_handler):
                existing_handler(signum, frame)

        return new_handler


class BaseKVConnector(BaseConnector):
    """Key/value connector for tensor and string blobs."""

    @abstractmethod
    def get(self, key: str) -> Optional[torch.Tensor]:
        raise NotImplementedError()

    @abstractmethod
    def getstr(self, key: str) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def set(self, key: str, obj: torch.Tensor) -> None:
        raise NotImplementedError()

    @abstractmethod
    def setstr(self, key: str, obj: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def list(self, prefix: str) -> List[str]:
        raise NotImplementedError()
