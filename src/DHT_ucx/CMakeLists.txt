option(DHT_WITH_UCX "Build DHT based on Unified Communication X" ON)

file(GLOB UCX_SRC "ucx_*.c")
add_library(ucx_communication ${UCX_SRC})
target_link_libraries(ucx_communication PUBLIC ucx::ucp ucx::ucs)
target_include_directories(ucx_communication PUBLIC ${PROJECT_SOURCE_DIR}/include)

if (DHT_WITH_UCX)
  add_library(DHT_UCX DHT.c)

  target_link_libraries(DHT_UCX PUBLIC MPI::MPI_C ucx_communication -lm)
  target_include_directories(DHT_UCX PUBLIC ${PROJECT_SOURCE_DIR}/include)

  add_library(DHT_UCX_LOCKING DHT.c)

  target_link_libraries(DHT_UCX_LOCKING PUBLIC MPI::MPI_C ucx_communication -lm)
  target_include_directories(DHT_UCX_LOCKING PUBLIC ${PROJECT_SOURCE_DIR}/include)
  target_compile_definitions(DHT_UCX_LOCKING PRIVATE DHT_WITH_LOCKING)
endif()
