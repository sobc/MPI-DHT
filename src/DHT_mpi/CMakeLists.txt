if (DHT_WITH_MPI)
  add_library(DHT_MPI DHT.c)

  target_link_libraries(DHT_MPI PUBLIC MPI::MPI_C -lm)
  target_include_directories(DHT_MPI PUBLIC ${PROJECT_SOURCE_DIR}/include)

  if (DHT_STATISTICS)
    target_compile_definitions(DHT_MPI PRIVATE DHT_DISTRIBUTION DHT_STATISTICS)
  endif()

endif()
