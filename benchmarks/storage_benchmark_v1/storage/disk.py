"""
Simple SSD Hash Table Storage

Each key maps to a complete page entry.
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

from .interface import Storage


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

    File Layout:
    Page 0: offset = 0 * page_size
    Page 1: offset = 1 * page_size
    Page 2: offset = 2 * page_size
    ...
    Page N: offset = N * page_size

    When max_pages < actual page_id range, uses modulo mapping:
    actual_page_id = page_id % max_pages
    This allows simulating large traces with limited storage.
    """

    def __init__(self, storage_dir: str, page_size: int,
                 max_pages: int = 100000,
                 fsync_mode: str = 'batch', fsync_batch_size: int = 100):
        """Initialize disk hash table

        Args:
            storage_dir: Storage directory
            page_size: Size of each entry (page) in bytes
            max_pages: Maximum number of entries (creates circular mapping if trace is larger)
            fsync_mode: When to fsync ('batch', 'always', 'end', 'none')
            fsync_batch_size: Writes between fsync
        """
        self.storage_dir = Path(storage_dir)
        self.page_size = page_size
        self.max_pages = max_pages
        self.max_page_id = max_pages - 1
        self.fsync_mode = fsync_mode
        self.fsync_batch_size = fsync_batch_size
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.storage_file = self.storage_dir / "data.bin"
        self._allocate_file()
        self.fd = None
        self._buffer = os.urandom(page_size)
        self.stats = {
            'read_count': 0,
            'write_count': 0,
            'read_bytes': 0,
            'write_bytes': 0,
            'read_latencies_ms': [],
            'write_latencies_ms': [],
            'read_time_s': 0.0,
            'write_time_s': 0.0,
            'sync_count': 0,
            'hit': 0,
            'miss': 0,
        }
        self._pending_syncs = 0
        self._written_pages: set = set()

    def _allocate_file(self):
        """Pre-allocate disk space efficiently"""
        file_size = self.max_pages * self.page_size
        if not self.storage_file.exists():
            print(f"  [Storage] Creating file: {self.storage_file}")
            print(f"  [Storage] Requested size: {file_size / (1024**3):.2f} GB ({self.max_pages:,} pages × {self.page_size} bytes = {file_size:,} bytes)")
            # Use fallocate for efficient preallocation (Linux)
            fd = os.open(self.storage_file, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                # Try fallocate first (Linux specific, much faster)
                try:
                    import fcntl
                    fcntl.fallocate(fd, 0, file_size)
                    method = "fallocate"
                except (ImportError, AttributeError, OSError):
                    # Fallback to seek+write method
                    os.lseek(fd, file_size - 1, os.SEEK_SET)
                    os.write(fd, b'\0')
                    os.fsync(fd)
                    method = "seek+write"
            finally:
                os.close(fd)
            actual_size = self.storage_file.stat().st_size if self.storage_file.exists() else 0
            print(f"  [Storage] Pre-allocated {actual_size / (1024**3):.2f} GB using {method}")
        else:
            actual_size = self.storage_file.stat().st_size
            actual_pages = actual_size // self.page_size
            print(f"  [Storage] Reusing existing file: {self.storage_file}")
            print(f"  [Storage] Current file size: {actual_size / (1024**3):.2f} GB ({actual_pages:,} pages × {self.page_size} bytes = {actual_size:,} bytes)")

    def _get_fd(self):
        if self.fd is None:
            print(f"  [Storage] Opening file: {self.storage_file}")
            self.fd = os.open(self.storage_file, os.O_RDWR | os.O_CREAT, 0o644)
            if self.storage_file.exists():
                actual_size = self.storage_file.stat().st_size
                actual_pages = actual_size // self.page_size
                print(f"  [Storage] File size: {actual_size / (1024**3):.2f} GB ({actual_pages:,} pages × {self.page_size} bytes = {actual_size:,} bytes)")
        return self.fd

    # ========================================================================
    # Core operations
    # ========================================================================

    def _map_page_id(self, page_id: int) -> int:
        """Map logical page_id to physical page_id using modulo

        This allows simulating large traces with limited storage space.

        Args:
            page_id: Logical page_id (from trace)

        Returns:
            Physical page_id in storage (0 to max_pages-1)
        """
        return page_id % self.max_pages

    def read(self, page_id: int, offset_in_page: int = 0, length: int = None) -> float:
        """Read entry from disk

        Args:
            page_id: Logical page ID (hash_id from trace)
            offset_in_page: Offset within the page (default: 0)
            length: Number of bytes to read (default: entire page)

        Returns:
            Read latency in ms
        """
        if length is None:
            length = self.page_size - offset_in_page

        # Validate parameters
        if offset_in_page < 0 or offset_in_page >= self.page_size:
            raise ValueError(f"offset_in_page {offset_in_page} out of range [0, {self.page_size})")
        if length <= 0 or offset_in_page + length > self.page_size:
            raise ValueError(f"length {length} invalid with offset_in_page {offset_in_page} (page_size={self.page_size})")

        # Map to physical page_id and calculate offset
        physical_page_id = self._map_page_id(page_id)
        offset = physical_page_id * self.page_size + offset_in_page
        start = time.perf_counter()

        try:
            fd = self._get_fd()
            os.pread(fd, length, offset)

            latency = (time.perf_counter() - start) * 1000.0
            self.stats['read_count'] += 1
            self.stats['read_bytes'] += length
            self.stats['read_latencies_ms'].append(latency)
            self.stats['read_time_s'] += latency / 1000.0
            self.stats['hit'] += 1
            return latency
        except OSError as e:
            print(f"Read error (page_id={page_id}, physical_page_id={physical_page_id}): {e}")
            return 0.0

    def write(self, page_id: int, offset_in_page: int = 0, length: int = None) -> float:
        """Write entry to disk

        Args:
            page_id: Logical page ID (hash_id from trace)
            offset_in_page: Offset within the page (default: 0)
            length: Number of bytes to write (default: entire page)

        Returns:
            Write latency in ms
        """
        if length is None:
            length = self.page_size - offset_in_page

        # Validate parameters
        if offset_in_page < 0 or offset_in_page >= self.page_size:
            raise ValueError(f"offset_in_page {offset_in_page} out of range [0, {self.page_size})")
        if length <= 0 or offset_in_page + length > self.page_size:
            raise ValueError(f"length {length} invalid with offset_in_page {offset_in_page} (page_size={self.page_size})")

        # Map to physical page_id and calculate offset
        physical_page_id = self._map_page_id(page_id)
        offset = physical_page_id * self.page_size + offset_in_page
        start = time.perf_counter()

        try:
            fd = self._get_fd()
            # Use corresponding portion of buffer
            os.pwrite(fd, self._buffer[:length], offset)
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
            else:
                latency = (write_done - start) * 1000.0

            self._written_pages.add(page_id)
            self.stats['write_count'] += 1
            self.stats['write_bytes'] += length
            self.stats['write_latencies_ms'].append(latency)
            self.stats['write_time_s'] += latency / 1000.0
            self.stats['miss'] += 1
            return latency
        except OSError as e:
            print(f"Write error (page_id={page_id}, offset_in_page={offset_in_page}, length={length}): {e}")
            return 0.0

    # ========================================================================
    # Storage interface methods
    # ========================================================================

    def exists(self, page_id: int) -> bool:
        """Check if logical page has been written

        Args:
            page_id: Logical page ID (hash_id from trace)

        Returns:
            True if this logical page has been written before
        """
        return page_id in self._written_pages

    def delete(self, page_id: int) -> bool:
        """Delete page (no-op in direct mapping)

        Args:
            page_id: Page ID to delete

        Returns:
            True (always succeeds in direct mapping)
        """
        # No-op since we don't track which pages have been written
        return True

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
                'time_s': self.stats['read_time_s'],
                **calc_stats(self.stats['read_latencies_ms'])
            },
            'write': {
                'count': self.stats['write_count'],
                'mb': self.stats['write_bytes'] / 1024 / 1024,
                'time_s': self.stats['write_time_s'],
                **calc_stats(self.stats['write_latencies_ms'])
            },
            'sync_count': self.stats['sync_count'],
            'max_pages': self.max_pages,
            'written_pages': len(self._written_pages),
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
