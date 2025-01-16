file(GLOB SHM_SOURCES "*.cpp")

add_library(shm_transport OBJECT ${SHM_SOURCES})
# target_link_libraries(shm_transport PUBLIC transport)