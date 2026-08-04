cmake_minimum_required(VERSION 3.12)
project(tent_metastore LANGUAGES CXX)

function(add_metastore_module name)
    set(options)
    set(oneValueArgs)
    set(multiValueArgs DEPS)
    cmake_parse_arguments(RES "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    set(SRCS ${name}.cpp)

    add_library(metastore_${name} STATIC ${SRCS})
    if(RES_DEPS)
        target_link_libraries(metastore_${name} PUBLIC ${RES_DEPS} tent_common)
    endif()
endfunction()

if (USE_ETCD)
    message(STATUS "Etcd metastore support: Enabled")
    add_metastore_module(etcd DEPS ${CMAKE_BINARY_DIR}/mooncake-common/etcd/libetcd_wrapper.so)
    add_dependencies(metastore_etcd build_etcd_wrapper)
endif()

find_package(CURL)
if(CURL_FOUND)
    message(STATUS "Http metastore support: Enabled")
    add_metastore_module(http DEPS CURL::libcurl)
else()
    message(STATUS "Http metastore support: Disabled")
endif()

if(NOT TARGET hiredis::hiredis)
    find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h)
    find_library(HIREDIS_LIBRARY hiredis)
    if(HIREDIS_INCLUDE_DIR AND HIREDIS_LIBRARY)
        add_library(hiredis::hiredis UNKNOWN IMPORTED)
        set_target_properties(
            hiredis::hiredis
            PROPERTIES IMPORTED_LOCATION "${HIREDIS_LIBRARY}"
                       INTERFACE_INCLUDE_DIRECTORIES "${HIREDIS_INCLUDE_DIR}")
    endif()
endif()
if(TARGET hiredis::hiredis)
    message(STATUS "Redis metastore support: Enabled")
    add_metastore_module(redis DEPS hiredis::hiredis)
else()
    message(STATUS "Redis metastore support: Disabled")
endif()
