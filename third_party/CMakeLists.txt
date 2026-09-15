add_subdirectory(simsimd)
add_subdirectory(hdrhistogram_c)
add_subdirectory(hnswlib)
add_subdirectory(snowball)
add_subdirectory(icu)

# SVS is compiled as a git submodule; supported on x86_64 Linux only.
if(UNIX AND NOT APPLE
   AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    add_subdirectory(svs ${CMAKE_CURRENT_BINARY_DIR}/svs-build)
endif()
