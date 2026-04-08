file(GLOB UB_SOURCES "*.cpp")

add_library(ub_transport OBJECT ${UB_SOURCES})


target_include_directories(ub_transport
            PUBLIC
            /usr/include/umdk
)

target_link_libraries(ub_transport
            PUBLIC
            /usr/lib64/liburma.so
            PRIVATE
            JsonCpp::JsonCpp
            glog::glog
            pthread
)