option(DHT_WITH_UCX "Build DHT based on Unified Communication X" ON)

if (DHT_WITH_UCX)
  add_library(DHT_UCX DHT.c ucx_init_deinit.c ucx_communication.c ucx_init.c ucx_worker_bcast.c)

  target_link_libraries(DHT_UCX PUBLIC MPI::MPI_C ucx::ucp ucx::ucs -lm)
  target_include_directories(DHT_UCX PUBLIC ${PROJECT_SOURCE_DIR}/include)

  add_library(DHT_UCX_LOCKING DHT.c ucx_init_deinit.c ucx_communication.c ucx_init.c ucx_worker_bcast.c)

  target_link_libraries(DHT_UCX_LOCKING PUBLIC MPI::MPI_C ucx::ucp ucx::ucs -lm)
  target_include_directories(DHT_UCX_LOCKING PUBLIC ${PROJECT_SOURCE_DIR}/include)
  target_compile_definitions(DHT_UCX_LOCKING PRIVATE DHT_WITH_LOCKING)
endif()
