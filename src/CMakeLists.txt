add_library(DHT_UCX DHT.c ucx_init_deinit.c ucx_communication.c)

target_link_libraries(DHT_UCX PUBLIC MPI::MPI_C ucx::ucp ucx::ucs -lm ${LIB_XXHASH})
target_include_directories(DHT_UCX PUBLIC ${PROJECT_SOURCE_DIR}/include)
