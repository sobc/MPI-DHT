add_library(DHT DHT.c)

target_link_libraries(DHT PUBLIC MPI::MPI_C)
target_include_directories(DHT PUBLIC ${PROJECT_SOURCE_DIR}/include)
