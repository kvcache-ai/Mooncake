file(GLOB CORO_RPC_SOURCES "coro_rpc_transfer.cpp")


add_library(coro_rpc_transfer OBJECT ${CORO_RPC_SOURCES})
target_include_directories(coro_rpc_transfer PUBLIC  CUDA::cudart "/usr/local/cuda/include")