if (DHT_WITH_UCX)
  add_executable(ReadWrite testReadWrite.c)
  target_link_libraries(ReadWrite PRIVATE DHT_UCX ${LIB_XXHASH} ucx::ucp)
endif()
