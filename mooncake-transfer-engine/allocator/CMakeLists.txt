# Allocator support libraries that are shared by several components (classic
# transport, Store, TENT) instead of belonging to one transport.
if(USE_ASCEND_DIRECT OR USE_UBSHMEM)
  add_subdirectory(ascend_allocator)
endif()

if(USE_UB)
  add_subdirectory(ub_allocator)
endif()

# Non-peermem requires cuMem allocated memory pointers; since WITH_NVIDIA_PEERMEM
# is now a runtime environment variable, always build when GPU support is present.
if(USE_MNNVL
   OR USE_CUDA
   OR USE_HIP
   OR USE_MUSA
   OR USE_MACA
   OR USE_SUPA)
  add_subdirectory(nvlink_allocator)
endif()

if(USE_UBSHMEM)
  add_subdirectory(ubshmem_allocator)
endif()
