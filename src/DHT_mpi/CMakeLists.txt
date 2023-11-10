if (DHT_WITH_MPI)
  add_library(DHT_MPI DHT.c)

  target_link_libraries(DHT_MPI PUBLIC MPI::MPI_C)
  target_include_directories(DHT_MPI PUBLIC ${PROJECT_SOURCE_DIR}/include)
endif()
