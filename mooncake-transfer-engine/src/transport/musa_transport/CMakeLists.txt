file(GLOB MUSA_TRANSPORT_SOURCES "*.cpp")

add_library(musa_transport OBJECT ${MUSA_TRANSPORT_SOURCES})

target_include_directories(musa_transport PUBLIC /usr/local/musa/include)
