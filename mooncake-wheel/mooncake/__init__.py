# Import for backward compatibility
import sys
"""Mooncake Transfer Engine Python Bindings."""

__version__ = "1.0.0"
VERSION = __version__

# Version compatibility check
def check_version_compatibility(client_version: str, server_version: str) -> bool:
    """
    Check if client and server versions are compatible based on major version.
    
    This allows for non-breaking minor and patch version differences.
    
    Args:
        client_version: Client version string
        server_version: Server version string
    
    Returns:
        bool: True if versions are compatible
    """
    try:
        from packaging import version
        
        # Parse version strings
        client_ver = version.parse(client_version)
        server_ver = version.parse(server_version)
        
        # Compare major versions (allow minor/patch differences)
        return client_ver.major == server_ver.major
    except ImportError:
        sys.stderr.write("Warning: packaging module not available, using simple version comparison\n")
        return client_version == server_version
    except Exception:
        # Fallback to exact match if parsing fails
        return client_version == server_version


class VersionMismatchError(Exception):
    """Exception raised when client and server versions don't match."""
    pass