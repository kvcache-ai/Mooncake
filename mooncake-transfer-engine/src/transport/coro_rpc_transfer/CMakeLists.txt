file(GLOB CORO_RPC_SOURCES "coro_rpc_transfer.cpp")

pybind11_add_module(coro_rpc_transfer coro_rpc_transfer.cpp ${CACHE_ALLOCATOR_SOURCES})

target_include_directories(coro_rpc_transfer PUBLIC  CUDA::cudart "/usr/local/cuda/include")
