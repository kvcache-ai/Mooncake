set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
file(GLOB SRCS_PATHS ./*.cpp)
add_executable(struct_pack_test ${SRCS_PATHS})
add_executable(struct_pack_test_with_optimize ${SRCS_PATHS})
add_test(NAME struct_pack_test COMMAND struct_pack_test)
add_test(NAME struct_pack_test_with_optimize COMMAND struct_pack_test_with_optimize)
include (TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)
if(NOT IS_BIG_ENDIAN)
target_compile_definitions(struct_pack_test PRIVATE STRUCT_PACK_ENABLE_UNPORTABLE_TYPE  TEST_IN_LITTLE_ENDIAN)
target_compile_definitions(struct_pack_test_with_optimize PRIVATE STRUCT_PACK_ENABLE_UNPORTABLE_TYPE  STRUCT_PACK_OPTIMIZE TEST_IN_LITTLE_ENDIAN )
else()
target_compile_definitions(struct_pack_test PRIVATE STRUCT_PACK_ENABLE_UNPORTABLE_TYPE )
target_compile_definitions(struct_pack_test_with_optimize PRIVATE STRUCT_PACK_ENABLE_UNPORTABLE_TYPE STRUCT_PACK_OPTIMIZE)
endif()
add_custom_command(
        TARGET struct_pack_test PRE_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/binary_data
        ${CMAKE_BINARY_DIR}/output/tests/binary_data)
add_custom_command(
        TARGET struct_pack_test PRE_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/binary_data
        ${CMAKE_BINARY_DIR}/src/struct_pack/tests/binary_data)