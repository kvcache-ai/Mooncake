# Import for backward compatibility

"""Mooncake Transfer Engine Python Bindings."""

__version__ = "1.0.0"
VERSION = __version__

# Version compatibility check
def check_version_compatibility(client_version: str, server_version: str) -> bool:
    """
    Check if client and server versions are compatible.
    
    Args:
        client_version: Client version string
        server_version: Server version string
    
    Returns:
        bool: True if versions are compatible
    """
    # Simple exact match for now, can be extended to semantic versioning
    return client_version == server_version


class VersionMismatchError(Exception):
    """Exception raised when client and server versions don't match."""
    pass

