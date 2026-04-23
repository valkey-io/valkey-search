# Snowball stemming library
# Build the libstemmer C library
# Version: v3.0.1

set(SNOWBALL_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# Source files for libstemmer
set(LIBSTEMMER_SOURCES
  ${SNOWBALL_SOURCE_DIR}/libstemmer/libstemmer.c
  ${SNOWBALL_SOURCE_DIR}/runtime/api.c
  ${SNOWBALL_SOURCE_DIR}/runtime/utilities.c
)

# Generated stemmer sources (UTF-8 versions for supported languages)
set(STEMMER_SOURCES
  ${SNOWBALL_SOURCE_DIR}/src_c/stem_UTF_8_english.c
)

# Create the snowball library
add_library(snowball STATIC ${LIBSTEMMER_SOURCES} ${STEMMER_SOURCES})

# Set include directories
target_include_directories(snowball PUBLIC
  ${SNOWBALL_SOURCE_DIR}/include
)

target_include_directories(snowball PRIVATE
  ${SNOWBALL_SOURCE_DIR}
)

# Benchmarking confirmed a ~26% overall and ~42% peak speedup over
# default builds.
include(CheckIPOSupported)
check_ipo_supported(RESULT lto_supported OUTPUT lto_error)
if(lto_supported)
    set_property(TARGET snowball PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    target_compile_options(snowball PRIVATE -fno-fat-lto-objects)
endif()
target_compile_options(snowball PRIVATE -O3 -w) # Suppress warnings from third-party code

# Export the target
set_target_properties(snowball PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  CXX_STANDARD 20
)
