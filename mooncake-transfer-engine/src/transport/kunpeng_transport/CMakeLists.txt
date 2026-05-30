file(GLOB UB_SOURCES "*.cpp" "urma/urma_endpoint.cpp")

# Check if liburma.so exists
find_library(URMA_LIBRARY urma PATHS /usr/lib64)

# Build ub_transport with real URMA library if available
# Never include mock_urma_api.cpp in ub_transport library
# For test, we will use mock_urma_api.cpp directly
if (URMA_LIBRARY)
    add_library(ub_transport OBJECT ${UB_SOURCES})
    target_link_libraries(ub_transport
            PUBLIC
            ${URMA_LIBRARY}
    )
    message(STATUS "Using real URMA library: ${URMA_LIBRARY}")
else()
    # If liburma.so not found, we'll need to handle this differently
    # For now, just build without mock_urma_api.cpp
    list(APPEND UB_SOURCES "urma/mock_urma.cpp")
    add_library(ub_transport OBJECT ${UB_SOURCES})
    message(WARNING "Not Found liburma.so building ub_transport with Mock URMA library")
endif()
target_include_directories(ub_transport
        PUBLIC
        ${urma_INCLUDE_DIR}
)
target_link_libraries(ub_transport
        PRIVATE
        JsonCpp::JsonCpp
        glog::glog
        pthread
)