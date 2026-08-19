file(GLOB MACA_TRANSPORT_SOURCES "*.cpp")

add_library(maca_transport OBJECT ${MACA_TRANSPORT_SOURCES})

target_include_directories(maca_transport PUBLIC ${MACA_INCLUDE_DIR})
