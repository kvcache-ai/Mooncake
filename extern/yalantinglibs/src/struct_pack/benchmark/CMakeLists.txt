set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/benchmark)
find_package(Protobuf QUIET)
add_executable(struct_pack_benchmark benchmark.cpp no_op.cpp)
if (Protobuf_FOUND)
    message(STATUS "Protobuf_FOUND: ${Protobuf_FOUND}")
    protobuf_generate_cpp(STRUCT_PACK_BENCHMARK_PROTO_SRCS
            STRUCT_PACK_BENCHMARK_PROTO_HDRS
            data_def.proto
            )
    target_include_directories(struct_pack_benchmark PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
    target_sources(struct_pack_benchmark PRIVATE
            ${STRUCT_PACK_BENCHMARK_PROTO_SRCS}
            ${STRUCT_PACK_BENCHMARK_PROTO_HDRS}
            )
    target_link_libraries(struct_pack_benchmark PRIVATE protobuf::libprotobuf)

    target_compile_definitions(struct_pack_benchmark PRIVATE HAVE_PROTOBUF)
endif ()

target_compile_definitions(struct_pack_benchmark PRIVATE MSGPACK_NO_BOOST)
target_compile_definitions(struct_pack_benchmark PRIVATE STRUCT_PACK_OPTIMIZE)
find_package(Flatbuffers QUIET)
if (Flatbuffers_FOUND)
    message(STATUS "Flatbuffers_FOUND: ${Flatbuffers_FOUND}")
    execute_process(
            COMMAND flatc ./data_def.fbs --cpp
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    target_compile_definitions(struct_pack_benchmark PRIVATE HAVE_FLATBUFFER)
endif()
