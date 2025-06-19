file(GLOB ASCEND_SOURCES "hccl_transport/*.cpp")
add_subdirectory(hccl_transport/ascend_transport_c)

add_library(ascend_transport OBJECT ${ASCEND_SOURCES})

target_link_libraries(ascend_transport PRIVATE ascend_transport_mem)

set_target_properties(ascend_transport
                      PROPERTIES
                      PREFIX ""
                    )

target_compile_options(ascend_transport PRIVATE
                       -O2
                       -std=c++17
                       -Xlinker -export-dynamic     
                    )

target_link_options(ascend_transport PRIVATE
                    -s
                    )
