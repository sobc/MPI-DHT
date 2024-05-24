find_package(Doxygen 
    REQUIRED dot
)

set(DOXYGEN_GENERATE_XML YES)
set(DOXYGEN_GENERATE_HTML NO)

doxygen_add_docs(doxygen 
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
    COMMENT "Generating doxygen documentation"
)

find_program(SPHINX_EXECUTABLE 
    NAMES sphinx-build
    DOC "Sphinx documentation builder"
)

include(FindPackageHandleStandardArgs)

#Handle standard arguments to find_package like REQUIRED and QUIET
find_package_handle_standard_args(Sphinx
                                  "Failed to find sphinx-build executable"
                                  SPHINX_EXECUTABLE)


add_custom_target(sphinx
    COMMAND ${SPHINX_EXECUTABLE} -b html -Dbreathe_projects.LUCX-DHT=${CMAKE_CURRENT_BINARY_DIR}/xml ${PROJECT_SOURCE_DIR}/docs ${CMAKE_CURRENT_BINARY_DIR}/sphinx
    DEPENDS doxygen
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating sphinx documentation"
)