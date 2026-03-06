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

# Create the snowball library using valkey_search function to inherit optimization flags
valkey_search_add_static_library(snowball "${LIBSTEMMER_SOURCES};${STEMMER_SOURCES}")

# Set include directories
target_include_directories(snowball PUBLIC ${SNOWBALL_SOURCE_DIR}/include)
target_include_directories(snowball PRIVATE ${SNOWBALL_SOURCE_DIR})

# Suppress warnings from third-party code
target_compile_options(snowball PRIVATE -w)
