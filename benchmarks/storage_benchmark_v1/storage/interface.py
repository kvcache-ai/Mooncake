"""
KVCache Storage Interface

Simplified storage interface for KVCache benchmark.
Key = page_id (int), Value = fixed-size bytes.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class Storage(ABC):
    """Abstract base class for KVCache storage

    Simplified design:
    - Key: page_id (int, hash_id from trace)
    - Value: fixed-size bytes (page_size)
    - Direct mapping: page_id -> offset -> payload

    Core operations: read, write, exists, delete.
    """

    @abstractmethod
    def read(self, page_id: int, offset_in_page: int = 0, length: int = None) -> float:
        """Read page from disk

        Args:
            page_id: Page ID (hash_id from trace)
            offset_in_page: Offset within the page (default: 0)
            length: Number of bytes to read (default: entire page)

        Returns:
            Read latency in milliseconds
        """
        pass

    @abstractmethod
    def write(self, page_id: int, offset_in_page: int = 0, length: int = None) -> float:
        """Write page to disk

        Args:
            page_id: Page ID (hash_id from trace)
            offset_in_page: Offset within the page (default: 0)
            length: Number of bytes to write (default: entire page)

        Returns:
            Write latency in milliseconds
        """
        pass

    @abstractmethod
    def exists(self, page_id: int) -> bool:
        """Check if page exists

        Args:
            page_id: Page ID (hash_id from trace)

        Returns:
            True if page_id is within valid range
        """
        pass

    @abstractmethod
    def delete(self, page_id: int) -> bool:
        """Delete page (no-op in direct mapping)

        Args:
            page_id: Page ID to delete

        Returns:
            True (always succeeds in direct mapping)
        """
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics

        Returns:
            Dictionary containing storage statistics
        """
        pass

    def close(self):
        """Close the storage and release resources"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
