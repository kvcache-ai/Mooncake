file(GLOB TCP_SOURCES "*.cpp")

if (USE_HIP)
  hipify_files(TCP_SOURCES)
endif()

add_library(tcp_transport OBJECT ${TCP_SOURCES})
target_link_libraries(tcp_transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs)

if (USE_CUDA)
  target_link_libraries(tcp_transport PRIVATE cuda_loader)
endif()
