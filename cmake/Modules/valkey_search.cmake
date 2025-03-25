# Determine if we are building in Release or Debug mode
if(CMAKE_BUILD_TYPE MATCHES Release)
  set(VALKEY_SEARCH_DEBUG_BUILD 0)
  set(VALKEY_SEARCH_RELEASE_BUILD 1)
  message(STATUS "Building in release mode")
else()
  set(VALKEY_SEARCH_DEBUG_BUILD 1)
  set(VALKEY_SEARCH_RELEASE_BUILD 0)
  message(STATUS "Building in debug mode")
endif()

if(CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64")
  message(STATUS "Current platform is x86_64")
  set(VALKEY_SEARCH_IS_ARM 0)
  set(VALKEY_SEARCH_IS_X86 1)
else()
  message(STATUS "Current platform is aarch64")
  set(VALKEY_SEARCH_IS_ARM 1)
  set(VALKEY_SEARCH_IS_X86 0)
endif()

# Check for compiler compatibility
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  # We require GCC v12 and later
  string(REGEX MATCH "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" GCC_VERSION_MATCH
               ${CMAKE_CXX_COMPILER_VERSION})
  set(GCC_MAJOR_VERSION ${CMAKE_MATCH_1})
  if(GCC_MAJOR_VERSION LESS 12)
    message(
      FATAL_ERROR
        "Minimum GCC required is 12 and later. Your GCC version is ${CMAKE_CXX_COMPILER_VERSION}"
    )
  endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  # We require Clang v16 and later
  string(REGEX MATCH "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" CLANG_VERSION_MATCH
               ${CMAKE_CXX_COMPILER_VERSION})
  set(CLANG_MAJOR_VERSION ${CMAKE_MATCH_1})
  if(CLANG_MAJOR_VERSION LESS 16)
    message(
      FATAL_ERROR
        "Minimum Clang required is 16 and later. Your Clang version is ${CMAKE_CXX_COMPILER_VERSION}"
    )
  endif()
else()
  message(
    WARN
    "Using unknown compiler ${CMAKE_CXX_COMPILER_ID}. Version: ${CMAKE_CXX_COMPILER_VERSION}"
  )
endif()

# A wrapper around "add_library" (STATIC) that enables the various build flags
function(valkey_search_add_static_library name sources)
  message(STATUS "Adding static library ${name}")
  add_library(${name} STATIC ${sources})
  valkey_search_target_update_compile_flags(${name})
endfunction()

# A wrapper around "add_library" (SHARED) that enables the various build flags
function(valkey_search_add_shared_library name sources)
  message(STATUS "Adding shared library ${name}")
  add_library(${name} SHARED ${sources})
  if(VALKEY_SEARCH_RELEASE_BUILD)
    # Enable full LTO (takes longer to link, but produces faster code)
    target_link_options(${name} PRIVATE -flto)
  endif()
  valkey_search_target_update_compile_flags(${name})
  set_target_properties(${name} PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                           "${CMAKE_BINARY_DIR}")
endfunction()

# Setup global compile flags
function(_add_global_build_flag _FLAG)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${_FLAG}")
endfunction()

function(valkey_search_target_update_compile_flags TARGET)
  target_compile_options(${TARGET} PRIVATE -falign-functions=5)
  target_compile_options(${TARGET} PRIVATE -fmath-errno)
  target_compile_options(${TARGET} PRIVATE -ffp-contract=off)
  target_compile_options(${TARGET} PRIVATE -fno-rounding-math)
  if(VALKEY_SEARCH_IS_X86)
    target_compile_options(${TARGET} PRIVATE -mcx16)
    target_compile_options(${TARGET} PRIVATE -msse4.2)
    target_compile_options(${TARGET} PRIVATE -mpclmul)
    target_compile_options(${TARGET} PRIVATE -mavx)
    target_compile_options(${TARGET} PRIVATE -mavx2)
    target_compile_options(${TARGET} PRIVATE -maes)
    target_compile_options(${TARGET} PRIVATE -mfma)
    target_compile_options(${TARGET} PRIVATE -mprfchw)
  endif()
  target_compile_options(${TARGET} PRIVATE -mtune=generic)
  target_compile_options(${TARGET} PRIVATE -gdwarf-5)
  target_compile_options(${TARGET} PRIVATE -gz=zlib)
  target_compile_options(${TARGET} PRIVATE -ffast-math)
  target_compile_options(${TARGET} PRIVATE -funroll-loops)
  target_compile_options(${TARGET} PRIVATE -ftree-vectorize)
  target_compile_options(${TARGET} PRIVATE -fopenmp)
  target_compile_options(${TARGET} PRIVATE -ffp-contract=off)
  target_compile_options(${TARGET} PRIVATE -flax-vector-conversions)
  target_compile_options(${TARGET} PRIVATE -Wno-unknown-pragmas)
  target_compile_options(${TARGET} PRIVATE -fPIC)
  target_compile_definitions(${TARGET} PRIVATE BAZEL_BUILD)
  if(VALKEY_SEARCH_DEBUG_BUILD)
    target_compile_options(${TARGET} PRIVATE -O0)
    target_compile_options(${TARGET} PRIVATE -fno-omit-frame-pointer)
    target_compile_definitions(${TARGET} PRIVATE NDEBUG)
    target_compile_options(${TARGET} PRIVATE -fno-lto)
  else()
    # Release build, dump both IR & obj so we can defer the lto to the link time
    target_compile_options(${TARGET} PRIVATE -ffat-lto-objects)
  endif()
  target_compile_options(${TARGET} PRIVATE -Wno-sign-compare)
  target_compile_options(${TARGET} PRIVATE -Wno-uninitialized)
endfunction()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-requires")

include(protobuf_generate)
include(linux_utils)

set(TESTING_LIBS_EXTRA ${THIRD_PARTY_LIBS})
list(APPEND TESTING_LIBS_EXTRA ${GTEST_LIBS})

# HACK: in order to force CMake to put "-Wl,--end-group" as the last argument we
# use a fake library "lib_to_add_end_group_flag"
add_library(lib_to_add_end_group_flag INTERFACE "")
target_link_libraries(lib_to_add_end_group_flag INTERFACE "-Wl,--end-group")

macro(finalize_test_flags __TARGET)
  # --end-group will added by our fake target "lib_to_add_end_group_flag"
  target_link_options(${__TARGET} PRIVATE "LINKER:--start-group"
                      "${TESTING_LIBS_EXTRA}")
  target_link_options(${__TARGET} PRIVATE "LINKER:--allow-multiple-definition")
  target_link_options(${__TARGET} PRIVATE "LINKER:-S")
  target_compile_options(${__TARGET} PRIVATE -O1)
  valkey_search_target_update_compile_flags(${__TARGET})
  set_target_properties(${__TARGET} PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                               "${CMAKE_BINARY_DIR}/tests")
  target_link_libraries(${__TARGET} PRIVATE lib_to_add_end_group_flag)
  if (VALKEY_SEARCH_IS_ARM)
    target_link_libraries(${__TARGET} PRIVATE pthread)
  endif()
endmacro()
