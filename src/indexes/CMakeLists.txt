# Proto library for index schema (stays as a generated target)
valkey_search_create_proto_library("src/index_schema.proto"
                                   "index_schema_cc_proto")

# Small C helper for rax (separate target due to C vs C++ compile flags)
set(SRCS_RAX_LIB ${CMAKE_CURRENT_LIST_DIR}/text/rax/rax.c)
valkey_search_add_static_library(rax_lib "${SRCS_RAX_LIB}")
target_include_directories(rax_lib PUBLIC ${CMAKE_CURRENT_LIST_DIR}/text)

# =============================================================================
# libvs_indexes: All index implementations (numeric, tag, text, vector)
# =============================================================================
set(SRCS_VS_INDEXES
    # Top-level index sources
    ${CMAKE_CURRENT_LIST_DIR}/numeric.cc
    ${CMAKE_CURRENT_LIST_DIR}/tag.cc
    ${CMAKE_CURRENT_LIST_DIR}/text.cc
    ${CMAKE_CURRENT_LIST_DIR}/universal_set_fetcher.cc
    ${CMAKE_CURRENT_LIST_DIR}/vector_base.cc
    ${CMAKE_CURRENT_LIST_DIR}/vector_flat.cc
    ${CMAKE_CURRENT_LIST_DIR}/vector_hnsw.cc
    # Text index subdirectory sources
    ${CMAKE_CURRENT_LIST_DIR}/text/text_index.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/posting.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/textinfocmd.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/text_fetcher.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/proximity.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/orproximity.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/term.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/lexer.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/unicode_normalizer.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/flat_position_map.cc
    ${CMAKE_CURRENT_LIST_DIR}/text/rax_wrapper.cc)

valkey_search_add_static_library(libvs_indexes "${SRCS_VS_INDEXES}")

target_include_directories(libvs_indexes PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(libvs_indexes PUBLIC ${CMAKE_CURRENT_LIST_DIR}/text)

# Dependency on core library
target_link_libraries(libvs_indexes PUBLIC libvs_core)

# Third-party dependencies
target_link_libraries(libvs_indexes PUBLIC hnswlib_vmsdk)
target_link_libraries(libvs_indexes PUBLIC snowball)
target_link_libraries(libvs_indexes PUBLIC icu)
target_link_libraries(libvs_indexes PUBLIC rax_lib)
target_link_libraries(libvs_indexes PUBLIC iostream)

# Proto and SDK dependencies
target_link_libraries(libvs_indexes PUBLIC index_schema_cc_proto)
target_link_libraries(libvs_indexes PUBLIC vmsdklib)
target_link_libraries(libvs_indexes PUBLIC valkey_module)
