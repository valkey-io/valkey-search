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
  ${SNOWBALL_SOURCE_DIR}
)

# Set compile flags to match the original build
target_compile_options(snowball PRIVATE -w) # Suppress warnings from third-party code

# Export the target
set_target_properties(snowball PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  CXX_STANDARD 20
)
