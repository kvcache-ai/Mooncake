find_library(URING_LIB uring PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)
find_path(URING_INCLUDE liburing.h PATHS /usr/include /usr/local/include)

if (URING_LIB AND URING_INCLUDE)
    message(STATUS "Found io_uring at ${URING_LIB}")

    file(GLOB TEV1_XPORT_URING_SOURCES "*.cpp")

    add_library(transport_uring_obj OBJECT ${TEV1_XPORT_URING_SOURCES})
    target_include_directories(transport_uring_obj PUBLIC ${URING_INCLUDE})

    add_library(transport_uring_static STATIC $<TARGET_OBJECTS:transport_uring_obj>)
    target_link_libraries(transport_uring_static PUBLIC ${URING_LIB})

    add_library(transport_uring_shared SHARED $<TARGET_OBJECTS:transport_uring_obj>)
    target_link_libraries(transport_uring_shared PUBLIC ${URING_LIB})
else()
    message(STATUS "io_uring not found, skipping transport_uring")
endif()
