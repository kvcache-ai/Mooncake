"""
KVCache Storage Module

Provides storage backend implementations for KVCache systems.
"""

from .interface import Storage
from .disk import DiskHashTable

__all__ = [
    'Storage',
    'DiskHashTable',
]
