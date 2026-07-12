"""Mooncake public Python package."""

from mooncake.buffer_pool import BufferPool, RegisteredBufferPool

__all__ = ["BufferPool", "RegisteredBufferPool"]

from mooncake.store_file_io import patch_store_file_io_support


# NOTE: patch_store_file_io_support() only imports the mooncake.store module
# and attaches methods to the MooncakeDistributedStore class.  It does NOT
# instantiate any store or initialise the C++ engine -- that happens when
# MooncakeDistributedStore.__init__() is called by the user.  The side effect
# here is limited to module import and attribute assignment.
patch_store_file_io_support()
