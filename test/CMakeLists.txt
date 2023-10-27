add_executable(ASanInitDeinit testInitDeinit.c)

target_compile_options(ASanInitDeinit
  PRIVATE -fsanitize=address -fsanitize=leak -fsanitize=undefined)
target_link_options(ASanInitDeinit
  PRIVATE -fsanitize=address -fsanitize=leak -fsanitize=undefined)

target_link_libraries(ASanInitDeinit PRIVATE DHT)
