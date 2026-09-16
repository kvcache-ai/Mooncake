cmake_minimum_required(VERSION 3.20)
project(oplog_codec_evidence LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
get_filename_component(MOONCAKE_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/../.."
                       ABSOLUTE)
set(MOONCAKE_SOURCE
    "${MOONCAKE_SOURCE}"
    CACHE PATH "W02 source checkout")
set(YLT_INCLUDE_DIR
    ""
    CACHE PATH "Repository-pinned yalantinglibs include directory")
set(ETCD_WRAPPER_DIR
    ""
    CACHE PATH "Directory containing libetcd_wrapper.h and library")
find_package(glog CONFIG REQUIRED)
find_package(folly CONFIG REQUIRED)
find_package(jsoncpp CONFIG REQUIRED)
find_library(XXHASH_LIBRARY xxhash REQUIRED)
find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp REQUIRED)
find_path(MSGPACK_INCLUDE_DIR msgpack.hpp REQUIRED)
find_library(
  ETCD_WRAPPER_LIBRARY
  NAMES etcd_wrapper libetcd_wrapper.so
  PATHS "${ETCD_WRAPPER_DIR}"
  NO_DEFAULT_PATH REQUIRED)
set(STORE "${MOONCAKE_SOURCE}/mooncake-store")
execute_process(
  COMMAND git -C "${MOONCAKE_SOURCE}" rev-parse HEAD
  OUTPUT_VARIABLE W02_COMMIT
  OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ERROR_IS_FATAL ANY)
file(SHA256 "${STORE}/src/ha/oplog/ordered_oplog_writer.cpp" W02_WRITER_SHA256)
file(SHA256 "${NLOHMANN_JSON_INCLUDE_DIR}/nlohmann/json.hpp" JSON_HEADER_SHA256)
configure_file(build_metadata.json.in build_metadata.json @ONLY)
add_library(
  batch_codec_runtime STATIC
  ${STORE}/src/ha/oplog/oplog_types.cpp
  ${STORE}/src/ha/oplog/oplog_batch_types.cpp
  ${STORE}/src/ha/oplog/oplog_batch_codec.cpp ${STORE}/src/types.cpp)
add_library(
  w02_runtime STATIC
  ${STORE}/src/ha/oplog/ordered_oplog_writer.cpp
  ${STORE}/src/ha/oplog/oplog_test_failpoint.cpp
  ${STORE}/src/ha/oplog/oplog_batch_storage.cpp
  ${STORE}/src/ha/snapshot/batch_oplog/metadata.cpp
  ${STORE}/src/ha/kv/etcd_ha_kv_backend.cpp
  ${STORE}/src/etcd_helper.cpp)
target_compile_definitions(batch_codec_runtime PUBLIC STORE_USE_ETCD)
target_include_directories(
  batch_codec_runtime
  PUBLIC ${STORE}/include
         ${STORE}/include/cachelib_memory_allocator
         ${MOONCAKE_SOURCE}/mooncake-common/include
         ${YLT_INCLUDE_DIR}
         ${YLT_INCLUDE_DIR}/ylt/thirdparty
         ${YLT_INCLUDE_DIR}/ylt/standalone
         ${MSGPACK_INCLUDE_DIR}
         ${ETCD_WRAPPER_DIR})
target_link_libraries(
  batch_codec_runtime PUBLIC glog::glog Folly::folly JsonCpp::JsonCpp
                             ${XXHASH_LIBRARY})
add_library(
  replay_runtime STATIC
  ${STORE}/src/ha/oplog/oplog_applier.cpp
  ${STORE}/src/ha/standby_metadata_store.cpp ${STORE}/src/metadata_store.cpp
  ${STORE}/src/ha_metric_manager.cpp)
target_link_libraries(replay_runtime PUBLIC batch_codec_runtime)
target_link_libraries(w02_runtime PUBLIC replay_runtime ${ETCD_WRAPPER_LIBRARY})
add_library(candidate_codecs STATIC candidate_codecs.cpp payload_workloads.cpp)
target_include_directories(candidate_codecs PUBLIC ${NLOHMANN_JSON_INCLUDE_DIR})
target_link_libraries(candidate_codecs PUBLIC replay_runtime)
add_executable(oplog_codec_bench codec_bench.cpp)
target_link_libraries(oplog_codec_bench PRIVATE candidate_codecs)
add_executable(oplog_codec_etcd_e2e codec_etcd_e2e.cpp)
target_link_libraries(oplog_codec_etcd_e2e PRIVATE candidate_codecs w02_runtime)
enable_testing()
add_test(NAME codec_validation COMMAND oplog_codec_bench --validate-only)
set_tests_properties(codec_validation PROPERTIES TIMEOUT 90)
