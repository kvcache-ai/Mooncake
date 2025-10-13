cmake_minimum_required(VERSION 3.12)
project(tent_metastore LANGUAGES CXX)

function(add_metastore_module name)
    set(options)
    set(oneValueArgs)
    set(multiValueArgs DEPS)
    cmake_parse_arguments(RES "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    set(SRCS ${name}.cpp)

    add_library(${name}_static STATIC ${SRCS})
    target_compile_definitions(${name}_static PUBLIC METASTORE_${name}_STATIC)
    if(RES_DEPS)
        target_link_libraries(${name}_static PUBLIC ${RES_DEPS})
    endif()

    add_library(${name}_shared SHARED ${SRCS})
    target_compile_definitions(${name}_shared PUBLIC METASTORE_${name}_SHARED)
    target_compile_definitions(${name}_shared PUBLIC WITH_PLUGIN_HOOK)
    if(RES_DEPS)
        target_link_libraries(${name}_shared PUBLIC ${RES_DEPS})
    endif()

    install(TARGETS ${name}_static ${name}_shared
            EXPORT metastoreTargets
            ARCHIVE DESTINATION lib/metastore
            LIBRARY DESTINATION lib/metastore
            RUNTIME DESTINATION bin)
endfunction()

add_subdirectory(${CMAKE_SOURCE_DIR}/mooncake-common/etcd
                 ${CMAKE_BINARY_DIR}/mooncake-common/etcd)
message(STATUS "Etcd metastore support: Enabled")
add_metastore_module(etcd DEPS ${CMAKE_BINARY_DIR}/mooncake-common/etcd/libetcd_wrapper.so)
add_dependencies(etcd_static build_etcd_wrapper)
add_dependencies(etcd_shared build_etcd_wrapper)

find_package(CURL)
if(CURL_FOUND)
    message(STATUS "Http metastore support: Enabled")
    add_metastore_module(http DEPS CURL::libcurl)
else()
    message(STATUS "Http metastore support: Disabled")
endif()

find_library(HIREDIS_LIB hiredis)
if(HIREDIS_LIB)
    message(STATUS "Redis metastore support: Enabled")
    add_metastore_module(redis DEPS ${HIREDIS_LIB})
else()
    message(STATUS "Redis metastore support: Disabled")
endif()