add_subdirectory(simsimd)
add_subdirectory(hdrhistogram_c)
add_subdirectory(hnswlib)
add_subdirectory(snowball)
add_subdirectory(icu)

# SVS is compiled as a git submodule; supported on x86_64 Linux only.
# The C API bindings subdirectory is registered EXCLUDE_FROM_ALL so its
# CMake configure_file step generates svs_c_version.h without adding the
# shared library to the module's link set. EXCLUDE_FROM_ALL still configures
# it, so OpenMP is switched off first: the module builds the C API without
# OpenMP (src/indexes/CMakeLists.txt), and the bindings would otherwise
# require it at configure time.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux"
   AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    set(SVS_ENABLE_OMP OFF CACHE BOOL "" FORCE)
    add_subdirectory(svs ${CMAKE_CURRENT_BINARY_DIR}/svs-build)
    add_subdirectory(svs/bindings/c ${CMAKE_CURRENT_BINARY_DIR}/svs-c-api-build
                     EXCLUDE_FROM_ALL)
endif()
