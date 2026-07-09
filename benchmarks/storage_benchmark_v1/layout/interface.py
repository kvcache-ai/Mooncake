"""
KVCache Layout Interface

Defines the abstract interface for KVCache storage layouts.
Layout layer converts normalized requests into storage access requirements.
"""

from abc import ABC, abstractmethod
from typing import Iterator, Any
from dataclasses import dataclass


@dataclass
class StorageAccess:
    """Storage access requirement

    Represents a need to access a page. Whether to READ or WRITE is determined
    by the upper layer based on whether the page already exists.
    """
    page_id: int             # Page ID (hash_id from trace)
    offset_in_page: int = 0  # Offset within the page (default: 0)
    length: int = None       # Number of bytes (default: entire page)

    def __repr__(self):
        if self.offset_in_page == 0 and self.length is None:
            return f"Access(page_id={self.page_id})"
        else:
            return f"Access(page_id={self.page_id}, offset={self.offset_in_page}, length={self.length})"


class KVLayout(ABC):
    """Abstract interface for KVCache storage layout

    Converts normalized KVCache requests into storage access requirements.

    Request format:
        - hash_ids: List[int] - chunk/page identifiers
        - input_length: int - input token count
        - output_length: int - output token count

    Output:
        - Iterator of StorageAccess (page access requirements)
    """

    @abstractmethod
    def get_operations(self, request: Any) -> Iterator[StorageAccess]:
        """Generate storage access requirements for a request

        Args:
            request: KVCache request with hash_ids, input_length, output_length

        Yields:
            StorageAccess: Page access requirements (READ vs WRITE decided by upper layer)
        """
        pass
