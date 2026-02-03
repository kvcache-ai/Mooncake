"""Version utilities for setuptools_scm integration."""
import os


def local_version(version):
    """
    Custom local_scheme for setuptools_scm that allows overriding
    via MOONCAKE_LOCAL_VERSION environment variable.
    
    Args:
        version: The version object from setuptools_scm
        
    Returns:
        Local version string (the part after '+')
    """
    # Check if user wants to override local version
    custom_local = os.environ.get("MOONCAKE_LOCAL_VERSION")
    if custom_local:
        return custom_local
    
    # Use setuptools_scm's default 'node-and-date' local_scheme
    from setuptools_scm.version import get_local_node_and_date
    return get_local_node_and_date(version)
