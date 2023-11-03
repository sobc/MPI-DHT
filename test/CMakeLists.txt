add_executable(ASanInitDeinit testInitDeinit.c)

target_compile_options(ASanInitDeinit
  PRIVATE -fsanitize=address -fsanitize=leak -fsanitize=undefined)
target_link_options(ASanInitDeinit
  PRIVATE -fsanitize=address -fsanitize=leak -fsanitize=undefined)

target_link_libraries(ASanInitDeinit PRIVATE DHT)

add_executable(ReadWrite testReadWrite.c)
target_link_libraries(ReadWrite PRIVATE DHT ${LIB_XXHASH} ucx::ucp)
