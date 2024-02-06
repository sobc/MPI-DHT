option(DHT_WITH_UCX "Build DHT based on Unified Communication X" ON)

file(GLOB UCX_SRC "ucx/ucx_*.c")
add_library(ucx_communication ${UCX_SRC})
target_link_libraries(ucx_communication PUBLIC MPI::MPI_C ucx::ucp ucx::ucs)
target_include_directories(ucx_communication PUBLIC ${PROJECT_SOURCE_DIR}/include)

file(GLOB BSTRAP_SRC "bootstrap_funcs/*.c")
add_library(bstrap_functions ${BSTRAP_SRC})
target_link_libraries(bstrap_functions PUBLIC MPI::MPI_C ucx::ucp)
target_include_directories(bstrap_functions PUBLIC ${PROJECT_SOURCE_DIR}/include)

if (DHT_WITH_UCX)
  add_library(DHT_UCX DHT.c)

  target_link_libraries(DHT_UCX PUBLIC MPI::MPI_C ucx_communication bstrap_functions)
  target_include_directories(DHT_UCX PUBLIC ${PROJECT_SOURCE_DIR}/include)

  add_library(DHT_UCX_LOCKING DHT.c)

  target_link_libraries(DHT_UCX_LOCKING PUBLIC MPI::MPI_C ucx_communication bstrap_functions)
  target_include_directories(DHT_UCX_LOCKING PUBLIC ${PROJECT_SOURCE_DIR}/include)
  target_compile_definitions(DHT_UCX_LOCKING PRIVATE DHT_WITH_LOCKING)


  target_compile_definitions(DHT_UCX PRIVATE DHT_STATISTICS)
  target_compile_definitions(DHT_UCX_LOCKING PRIVATE DHT_STATISTICS)

  if (DHT_STATISTICS)
    target_compile_definitions(DHT_UCX PRIVATE DHT_DISTRIBUTION DHT_STATISTICS)
    target_compile_definitions(DHT_UCX_LOCKING PRIVATE DHT_DISTRIBUTION DHT_STATISTICS)
  endif()

endif()