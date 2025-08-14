add_library(mooncake_ep mooncake_backend.cpp)

target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine)
