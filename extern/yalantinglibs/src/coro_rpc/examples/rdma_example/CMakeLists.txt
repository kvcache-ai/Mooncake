set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/examples/coro_rpc)
add_executable(coro_rpc_rdma_example
    rdma_example.cpp
  )
target_link_libraries(coro_rpc_rdma_example -libverbs)