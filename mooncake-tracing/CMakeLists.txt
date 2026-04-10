project(mooncake-tracing)

add_library(mooncake-tracing
    src/trace_context.cpp
    src/trace_exporter_inmemory.cpp
    src/trace_exporter_jsonl.cpp
    src/trace_exporter_otlp_http.cpp
    src/trace_config.cpp
    src/tracing_facade.cpp)

find_package(CURL REQUIRED)

target_include_directories(mooncake-tracing
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${JsonCpp_INCLUDE_DIRS})

target_link_libraries(
    mooncake-tracing
    PUBLIC
        ${JsonCpp_LIBRARY}
        ${CURL_LIBRARIES}
        glog::glog)
