"""
Simple SSD Hash Table Storage

Each key maps to a complete page entry.
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

from .interface import Storage, KVKey, KVValue


def calc_percentiles(data):
    """Calculate latency percentiles"""
    if not data:
        return {'avg_ms': 0, 'p50_ms': 0, 'p95_ms': 0, 'p99_ms': 0}
    import statistics
    sorted_data = sorted(data)
    n = len(sorted_data)
    def get_percentile(p):
        idx = int(n * p / 100)
        if idx >= n: idx = n - 1
        return sorted_data[idx]
    return {
        'avg_ms': statistics.mean(data),
        'p50_ms': get_percentile(50),
        'p95_ms': get_percentile(95),
        'p99_ms': get_percentile(99),
    }


class DiskHashTable(Storage):
    """Simple disk-based hash table

    Architecture:
    -----------
    - In-memory map: key -> page_id (offset)
    - On-disk: fixed-size pages at page_id * page_size

    Storage:
    --------
    - Each entry is ONE complete page
    - Key = (sequence_id, layer_id)
    - Value = complete page data for that layer

    """

    def __init__(self, storage_dir: str, page_size: int,
                 max_pages: int = 100000,
                 fsync_mode: str = 'batch', fsync_batch_size: int = 100):
        """Initialize disk hash table

        Args:
            storage_dir: Storage directory
            page_size: Size of each entry (page) in bytes
            max_pages: Maximum number of entries
            fsync_mode: When to fsync ('batch', 'always', 'end', 'none')
            fsync_batch_size: Writes between fsync
        """
        self.storage_dir = Path(storage_dir)
        self.page_size = page_size
        self.max_pages = max_pages
        self.fsync_mode = fsync_mode
        self.fsync_batch_size = fsync_batch_size

        # In-memory index: key -> page_id
        self.index: Dict[tuple, int] = {}

        # Next page ID
        self.next_page_id = 0

        # Storage file
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.storage_file = self.storage_dir / "data.bin"

        # Pre-allocate
        self._allocate_file()

        # File descriptor
        self.fd = None

        # Write buffer
        # Write buffer (use random bytes to prevent SSD compression/deduplication from skewing results)
        self._buffer = os.urandom(page_size)

        # Stats
        self.stats = {
            'read_count': 0,
            'write_count': 0,
            'read_bytes': 0,
            'write_bytes': 0,
            'read_latencies_ms': [],
            'write_latencies_ms': [],
            'sync_count': 0,
            'hit': 0,
            'miss': 0,
        }

        self._pending_syncs = 0

    def _allocate_file(self):
        """Pre-allocate disk space"""
        file_size = self.max_pages * self.page_size
        if not self.storage_file.exists():
            with open(self.storage_file, 'wb') as f:
                f.seek(file_size - 1)
                f.write(b'\0')
                f.flush()
                os.fsync(f.fileno())

    def _get_fd(self):
    def _get_fd(self):
        if self.fd is None:
            self.fd = os.open(self.storage_file, os.O_RDWR | os.O_CREAT, 0o644)
        return self.fd

    # ========================================================================
    # Core operations
    # ========================================================================

    def read(self, key: tuple) -> float:
        """Read entry from disk

        Args:
            key: Tuple (sequence_id, layer_id)

        Returns:
            Read latency in ms if found, 0.0 if cache miss (not found)
        """
        page_id = self.index.get(key)
        if page_id is None:
            return 0.0

        offset = page_id * self.page_size
        start = time.perf_counter()

        try:
            fd = self._get_fd()
            os.pread(fd, self.page_size, offset)

            latency = (time.perf_counter() - start) * 1000.0
            self.stats['read_count'] += 1
            self.stats['read_bytes'] += self.page_size
            self.stats['read_latencies_ms'].append(latency)
            self.stats['hit'] += 1
            return latency
        except OSError as e:
            print(f"Read error: {e}")
            return 0.0

    def write(self, key: tuple) -> float:
        """Write entry to disk

        Args:
            key: Tuple (sequence_id, layer_id)

        Returns:
            Write latency in ms, 0.0 if capacity exceeded
        """
        # Check capacity
        if self.next_page_id >= self.max_pages:
            raise OSError(f"Storage capacity exceeded (max_pages={self.max_pages})")

        page_id = self.next_page_id
        self.next_page_id += 1
        self.index[key] = page_id

        offset = page_id * self.page_size
        start = time.perf_counter()

        try:
            fd = self._get_fd()
            os.pwrite(fd, self._buffer, offset)
            write_done = time.perf_counter()

            # Fsync
            if self.fsync_mode == 'always':
                os.fsync(fd)
                self.stats['sync_count'] += 1
                self._pending_syncs = 0
                latency = (time.perf_counter() - start) * 1000.0

            elif self.fsync_mode == 'batch':
                self._pending_syncs += 1
                if self._pending_syncs >= self.fsync_batch_size:
                    os.fsync(fd)
                    self.stats['sync_count'] += 1
                    self._pending_syncs = 0
                latency = (write_done - start) * 1000.0

            elif self.fsync_mode == 'none':
                latency = (write_done - start) * 1000.0

            else:  # 'end'
                latency = (write_done - start) * 1000.0

            # Evict from cache
            if hasattr(os, 'posix_fadvise'):
                os.posix_fadvise(fd, offset, self.page_size, os.POSIX_FADV_DONTNEED)

            self.stats['write_count'] += 1
            self.stats['write_bytes'] += self.page_size
            self.stats['write_latencies_ms'].append(latency)
            self.stats['miss'] += 1
            return latency
        except OSError as e:
            print(f"Write error: {e}")
            return 0.0

    # ========================================================================
    # Storage interface
    # ========================================================================

    def get(self, key) -> Optional[KVValue]:
        """Get entry for a key

        Args:
            key: Either KVKey or tuple (sequence_id, layer_id)

        Returns:
            KVValue if found, None otherwise
        """
        # Handle both KVKey and tuple keys
        if hasattr(key, 'sequence_id'):
            tuple_key = (key.sequence_id, key.layer_id)
        else:
            tuple_key = key
        latency = self.read(tuple_key)
        if latency > 0:
            return KVValue(data=b'', page_count=1, token_count=64)
        return None

    def put(self, key: KVKey, value: KVValue) -> None:
        """Store entry for a key"""
        tuple_key = (key.sequence_id, key.layer_id)
        self.write(tuple_key)

    def exists(self, key) -> bool:
        """Check if entry exists

        Args:
            key: Either KVKey or tuple (sequence_id, layer_id)

        Returns:
            True if key exists in index
        """
        # Handle both KVKey and tuple keys
        if hasattr(key, 'sequence_id'):
            tuple_key = (key.sequence_id, key.layer_id)
        else:
            tuple_key = key
        return tuple_key in self.index

    def delete(self, key) -> bool:
        """Delete entry

        Args:
            key: Either KVKey or tuple (sequence_id, layer_id)

        Returns:
            True if key was deleted, False if not found
        """
        # Handle both KVKey and tuple keys
        if hasattr(key, 'sequence_id'):
            tuple_key = (key.sequence_id, key.layer_id)
        else:
            tuple_key = key
        if tuple_key in self.index:
            del self.index[tuple_key]
            return True
        return False

    def get_sequence_keys(self, sequence_id: int) -> List[KVKey]:
        """Get all keys for a sequence"""
        keys = []
        for k in self.index.keys():
            if k[0] == sequence_id:
                keys.append(KVKey(sequence_id=k[0], layer_id=k[1]))
        return keys

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics"""
        import statistics

        def calc_stats(latencies):
            if not latencies:
                return {'avg_ms': 0, 'p50_ms': 0, 'p95_ms': 0, 'p99_ms': 0}
            return {
                'avg_ms': statistics.mean(latencies),
                **calc_percentiles(latencies),
            }

        return {
            'read': {
                'count': self.stats['read_count'],
                'mb': self.stats['read_bytes'] / 1024 / 1024,
                **calc_stats(self.stats['read_latencies_ms'])
            },
            'write': {
                'count': self.stats['write_count'],
                'mb': self.stats['write_bytes'] / 1024 / 1024,
                **calc_stats(self.stats['write_latencies_ms'])
            },
            'sync_count': self.stats['sync_count'],
            'total_pages': self.next_page_id,
            'total_keys': len(self.index),
            'page_hits': self.stats['hit'],
            'page_misses': self.stats['miss'],
        }

    # ========================================================================
    # Resource management
    # ========================================================================

    def close(self, force_sync: bool = True):
        """Close file"""
        if force_sync and self.fsync_mode in ['end', 'batch']:
            if self.fd is not None:
                try:
                    os.fsync(self.fd)
                    self.stats['sync_count'] += 1
                except OSError:
                    pass

        if self.fd is not None:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# Alias
SSDStorage = DiskHashTable
