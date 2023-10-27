add_library(DHT DHT.c ucx_functions.c)

target_link_libraries(DHT PUBLIC MPI::MPI_C ucx::ucp ucx::ucs -lm)
target_include_directories(DHT PUBLIC ${PROJECT_SOURCE_DIR}/include)
