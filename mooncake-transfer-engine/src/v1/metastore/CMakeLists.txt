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
    if(RES_DEPS)
        target_link_libraries(${name}_shared PUBLIC ${RES_DEPS})
    endif()

    install(TARGETS ${name}_static ${name}_shared
            EXPORT metastoreTargets
            ARCHIVE DESTINATION lib
            LIBRARY DESTINATION lib
            RUNTIME DESTINATION bin)
endfunction()

find_library(ETCD_WRAPPER_LIB etcd_wrapper
             PATHS ${CMAKE_CURRENT_BINARY_DIR}/../../../mooncake-common/etcd
             NO_DEFAULT_PATH)
if(ETCD_WRAPPER_LIB)
    message(STATUS "Found etcd_wrapper: ${ETCD_WRAPPER_LIB}")
    add_metastore_module(etcd DEPS ${ETCD_WRAPPER_LIB})
    add_dependencies(etcd_static build_etcd_wrapper)
    add_dependencies(etcd_shared build_etcd_wrapper)
else()
    message(STATUS "etcd_wrapper not found, skipping etcd metastore")
endif()

find_package(CURL)
if(CURL_FOUND)
    message(STATUS "Found libcurl: ${CURL_LIBRARIES}")
    add_metastore_module(http DEPS CURL::libcurl)
else()
    message(STATUS "libcurl not found, skipping http metastore")
endif()

find_library(HIREDIS_LIB hiredis)
if(HIREDIS_LIB)
    message(STATUS "Found hiredis: ${HIREDIS_LIB}")
    add_metastore_module(redis DEPS ${HIREDIS_LIB})
else()
    message(STATUS "hiredis not found, skipping redis metastore")
endif()