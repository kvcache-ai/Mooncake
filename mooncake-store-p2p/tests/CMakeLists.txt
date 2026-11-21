set(STORE_LIBRARY mooncake_store_p2p)
set(STORE_TEST_SUFFIX _p2p)
include("${MOONCAKE_STORE_SOURCE_ROOT}/tests/SharedStoreTests.cmake")
add_common_store_tests(p2p)

add_subdirectory("${MOONCAKE_STORE_SOURCE_ROOT}/tests/p2p" p2p)
add_subdirectory(e2e)
