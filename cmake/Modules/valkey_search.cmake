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

find_package(GTest CONFIG REQUIRED)

# A wrapper around "add_library" (STATIC) that enables the various build flags
function(valkey_search_add_static_library name sources)
  message(STATUS "Adding static library ${name}")
  add_library(${name} STATIC ${sources})
  valkey_search_target_update_compile_flags(${name})
  # Needed for gtest_prod.h
  target_link_libraries(${name} PRIVATE GTest::gtest)
endfunction()

function(valkey_search_target_update_san_flags TARGET)
  if(SAN_BUILD)
    # For sanitizer build, it is recommended to have at least -O1 and enable
    # -fno-omit-frame-pointer to get nicer stack traces
    target_compile_options(${TARGET} PRIVATE -O1)
    target_compile_options(${TARGET} PRIVATE -fno-omit-frame-pointer)
    target_compile_options(${TARGET} PRIVATE "-fsanitize=${SAN_BUILD}")
   
    target_compile_options(${TARGET} PRIVATE -fno-lto)
    target_compile_definitions(${TARGET} PRIVATE "SAN_BUILD=${SAN_BUILD}")
  endif()
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
  # Needed for gtest_prod.h
  target_link_libraries(${name} PRIVATE GTest::gtest)
  if(SAN_BUILD)
    target_link_options(${name} PRIVATE "-fsanitize=${SAN_BUILD}")
  endif()
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
  target_compile_options(${TARGET} PRIVATE -ffile-prefix-map=${CMAKE_SOURCE_DIR}=)
  target_compile_options(${TARGET} PRIVATE -ffast-math)
  target_compile_options(${TARGET} PRIVATE -funroll-loops)
  target_compile_options(${TARGET} PRIVATE -ftree-vectorize)
  if(UNIX AND NOT APPLE)
    target_compile_options(${TARGET} PRIVATE -fopenmp)
  endif()
  target_compile_options(${TARGET} PRIVATE -ffp-contract=off)
  target_compile_options(${TARGET} PRIVATE -flax-vector-conversions)
  target_compile_options(${TARGET} PRIVATE -Wno-unknown-pragmas)
  target_compile_options(${TARGET} PRIVATE -fPIC)
  target_compile_definitions(${TARGET} PRIVATE TESTING_TMP_DISABLED)
  if(SAN_BUILD)
    valkey_search_target_update_san_flags(${TARGET})
  elseif(VALKEY_SEARCH_DEBUG_BUILD)
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
  if(ABSL_INCLUDE_PATH)
    target_include_directories(${TARGET} PRIVATE ${ABSL_INCLUDE_PATH})
  endif()
  if(PROTOBUF_INCLUDE_PATH)
    target_include_directories(${TARGET} PRIVATE ${PROTOBUF_INCLUDE_PATH})
  endif()
  if(HIGHWAY_HASH_INCLUDE_PATH)
    target_include_directories(${TARGET} PRIVATE ${HIGHWAY_HASH_INCLUDE_PATH})
  endif()
endfunction()

if(UNIX AND NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-requires")
endif()

message(
  STATUS
    "Including file ${CMAKE_SOURCE_DIR}/cmake/Modules/protobuf_generate.cmake")
include(${CMAKE_SOURCE_DIR}/cmake/Modules/protobuf_generate.cmake)
include(linux_utils)

# HACK: in order to force CMake to put "-Wl,--end-group" as the last argument we
# use a fake library "lib_to_add_end_group_flag"
add_library(lib_to_add_end_group_flag INTERFACE "")
if(UNIX AND NOT APPLE)
  target_link_libraries(lib_to_add_end_group_flag INTERFACE "-Wl,--end-group")
endif()

macro(finalize_test_flags __TARGET)
  # --end-group will added by our fake target "lib_to_add_end_group_flag"
  if(UNIX AND NOT APPLE)
    target_link_options(${__TARGET} PRIVATE "LINKER:--start-group")
  endif()
  foreach(__lib ${THIRD_PARTY_LIBS})
    target_link_libraries(${__TARGET} PRIVATE ${__lib})
  endforeach()

  if(UNIX AND NOT APPLE)
    target_link_options(${__TARGET} PRIVATE
                        "LINKER:--allow-multiple-definition")
  endif()

  target_compile_options(${__TARGET} PRIVATE -O1)
  valkey_search_target_update_compile_flags(${__TARGET})
  set_target_properties(${__TARGET} PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                               "${CMAKE_BINARY_DIR}/tests")
  if(UNIX AND NOT APPLE)
    target_link_libraries(${__TARGET} PRIVATE lib_to_add_end_group_flag)
  endif()

  if(VALKEY_SEARCH_IS_ARM)
    target_link_libraries(${__TARGET} PRIVATE pthread)
  endif()
  target_link_libraries(${__TARGET} PRIVATE GTest::gtest GTest::gtest_main
                                            GTest::gmock)
  if(SAN_BUILD)
  target_link_options(${__TARGET} PRIVATE "-fsanitize=${SAN_BUILD}")
   
  endif()
endmacro()
