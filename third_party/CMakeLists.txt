add_subdirectory(simsimd)
add_subdirectory(hdrhistogram_c)
add_subdirectory(hnswlib)
add_subdirectory(snowball)
add_subdirectory(icu)

# SVS is compiled as a git submodule; supported on x86_64 Linux only.
# The C API bindings subdirectory is registered EXCLUDE_FROM_ALL so its
# CMake configure_file step generates svs_c_version.h without adding the
# shared library (and its libgomp dependency) to the module's link set.
if(UNIX AND NOT APPLE
   AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    add_subdirectory(svs ${CMAKE_CURRENT_BINARY_DIR}/svs-build)
    add_subdirectory(svs/bindings/c ${CMAKE_CURRENT_BINARY_DIR}/svs-c-api-build
                     EXCLUDE_FROM_ALL)
endif()
