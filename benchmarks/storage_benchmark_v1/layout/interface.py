"""
KVCache Layout Interface

Defines the abstract interface for KVCache storage layouts.
"""

from abc import ABC, abstractmethod
from typing import Iterator, Any

from storage.interface import KVKey



class KVLayout(ABC):
    """Abstract interface for KVCache storage layout

    Different model architectures have different KVCache access patterns.
    This captures both key generation and value sizing.
    """

    @abstractmethod
    def get_entries(self, request: Any) -> Iterator:
        """Generate storage entries for a request

        Args:
            request: KVCache request with hash_ids, input_length, output_length

        Yields:
            KVEntry: Each entry with key and value size
        """
        pass

    @abstractmethod
    def get_key_count(self, request: Any) -> int:
        """Get the total number of keys for a request

        Args:
            request: KVCache request

        Returns:
            int: Total number of storage keys
        """
        pass

    @abstractmethod
    def get_value_size(self, key: KVKey) -> int:
        """Get value size for a given key

        Args:
            key: KVKey

        Returns:
            int: Value size in bytes
        """
        pass

    @abstractmethod
    def keys_per_layer(self) -> int:
        """Number of keys per (sequence, layer) pair

        Returns:
            int: Keys per layer
        """
        pass
