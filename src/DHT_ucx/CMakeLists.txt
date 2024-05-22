option(DHT_WITH_UCX "Build DHT based on Unified Communication X" ON)

if (DHT_WITH_UCX)
  file(GLOB UCX_SRC "ucx/ucx_*.c")
  add_library(ucx_communication ${UCX_SRC})
  target_link_libraries(ucx_communication PUBLIC MPI::MPI_C ucx::ucp ucx::ucs)
  target_include_directories(ucx_communication PUBLIC ${PROJECT_SOURCE_DIR}/include)

  file(GLOB BSTRAP_SRC "bootstrap_funcs/*.c")
  add_library(bstrap_functions ${BSTRAP_SRC})
  target_link_libraries(bstrap_functions PUBLIC MPI::MPI_C ucx::ucp)
  target_include_directories(bstrap_functions PUBLIC ${PROJECT_SOURCE_DIR}/include)

  set(DHT_UCX_SRC 
    access.c
    accumulated.c
    cleanup.c
    collectives.c
    file.c
    init.c
  )

  add_library(DHT_UCX ${DHT_UCX_SRC})

  target_link_libraries(DHT_UCX PUBLIC MPI::MPI_C ucx_communication bstrap_functions -lm)
  target_include_directories(DHT_UCX PUBLIC ${PROJECT_SOURCE_DIR}/include)

  install(
    TARGETS DHT_UCX 
    DESTINATION lib
  )

  # add_library(DHT_UCX_LOCKING ${DHT_UCX_SRC})

  # target_link_libraries(DHT_UCX_LOCKING PUBLIC MPI::MPI_C ucx_communication bstrap_functions -lm)
  # target_include_directories(DHT_UCX_LOCKING PUBLIC ${PROJECT_SOURCE_DIR}/include)
  # target_compile_definitions(DHT_UCX_LOCKING PRIVATE DHT_WITH_LOCKING)

  if (DHT_STATISTICS)
    target_compile_definitions(DHT_UCX PRIVATE DHT_DISTRIBUTION DHT_STATISTICS)
    # target_compile_definitions(DHT_UCX_LOCKING PRIVATE DHT_DISTRIBUTION DHT_STATISTICS)
  endif()

endif()