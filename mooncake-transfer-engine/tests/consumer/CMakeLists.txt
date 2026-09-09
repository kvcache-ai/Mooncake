# Configure outside the TE directory scope to exercise exported usage
# requirements, as an application linking the library would.
add_executable(rdma_consumer_abi_test rdma_consumer_abi_test.cpp)
target_link_libraries(rdma_consumer_abi_test PRIVATE tent_link_group gtest_main)
add_test(NAME rdma_consumer_abi_test COMMAND rdma_consumer_abi_test)
