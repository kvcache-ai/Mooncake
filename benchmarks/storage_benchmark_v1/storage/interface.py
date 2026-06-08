"""
KVCache Storage Interface

Defines the key-value interface for KVCache storage systems.
This provides a clean abstraction for different storage backends.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class KVKey:
    """Key for KVCache storage

    Represents a unique identifier for a KVCache entry.
    """
    sequence_id: int      # Sequence identifier (hash_id)
    layer_id: int         # Layer number (0 to num_layers-1)

    def __hash__(self):
        return hash((self.sequence_id, self.layer_id))

    def __eq__(self, other):
        if not isinstance(other, KVKey):
            return False
        return (self.sequence_id == other.sequence_id and
                self.layer_id == other.layer_id)

    def __repr__(self):
        return f"KVKey(seq={self.sequence_id}, layer={self.layer_id})"


@dataclass
class KVValue:
    """Value for KVCache storage

    Contains the KV tensor data and metadata.
    """
    data: bytes           # Raw KV data bytes
    page_count: int       # Number of pages
    token_count: int      # Number of tokens

    def size_bytes(self) -> int:
        """Get size in bytes"""
        return len(self.data)


class Storage(ABC):
    """Abstract base class for KVCache storage

    Manages key-value storage for KVCache entries.
    Key: (sequence_id, layer_id)
    Value: KV tensor data for that sequence and layer

    Core operations: get, put, exists, delete.
    """

    @abstractmethod
    def get(self, key: KVKey) -> Optional[KVValue]:
        """Get KVCache value for a given key

        Args:
            key: KVKey containing (sequence_id, layer_id)

        Returns:
            KVValue if found, None otherwise
        """
        pass

    @abstractmethod
    def put(self, key: KVKey, value: KVValue) -> None:
        """Store KVCache value for a given key

        Args:
            key: KVKey containing (sequence_id, layer_id)
            value: KVValue to store
        """
        pass

    @abstractmethod
    def exists(self, key: KVKey) -> bool:
        """Check if KVCache entry exists

        Args:
            key: KVKey containing (sequence_id, layer_id)

        Returns:
            True if exists, False otherwise
        """
        pass

    @abstractmethod
    def delete(self, key: KVKey) -> bool:
        """Delete KVCache entry

        Args:
            key: KVKey containing (sequence_id, layer_id)

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    def get_sequence_keys(self, sequence_id: int) -> list[KVKey]:
        """Get all keys for a sequence

        Args:
            sequence_id: Sequence identifier

        Returns:
            List of KVKeys for all layers of the sequence
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
