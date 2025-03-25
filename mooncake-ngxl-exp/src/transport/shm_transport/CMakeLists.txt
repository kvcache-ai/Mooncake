file(GLOB SHM_SOURCES "*.cpp")

add_library(shm_ngxl_transport OBJECT ${SHM_SOURCES})
