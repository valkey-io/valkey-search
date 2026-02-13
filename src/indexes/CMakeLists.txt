valkey_search_create_proto_library("src/index_schema.proto"
                                   "index_schema_cc_proto")

add_library(index_base INTERFACE ${SRCS_INDEX_BASE})
target_include_directories(index_base INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(index_base INTERFACE index_schema_cc_proto)
target_link_libraries(index_base INTERFACE rdb_serialization)
target_link_libraries(index_base INTERFACE string_interning)
target_link_libraries(index_base INTERFACE vmsdklib)
target_link_libraries(index_base INTERFACE valkey_module)

set(SRCS_VECTOR_BASE ${CMAKE_CURRENT_LIST_DIR}/vector_base.cc
                     ${CMAKE_CURRENT_LIST_DIR}/vector_base.h)

valkey_search_add_static_library(vector_base "${SRCS_VECTOR_BASE}")
target_include_directories(vector_base PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_base PUBLIC index_base)
target_link_libraries(vector_base PUBLIC numeric)
target_link_libraries(vector_base PUBLIC tag)
target_link_libraries(vector_base PUBLIC attribute_data_type)
target_link_libraries(vector_base PUBLIC index_schema_cc_proto)
target_link_libraries(vector_base PUBLIC rdb_serialization)
target_link_libraries(vector_base PUBLIC vector_externalizer)
target_link_libraries(vector_base PUBLIC predicate)
target_link_libraries(vector_base PUBLIC allocator)
target_link_libraries(vector_base PUBLIC intrusive_ref_count)
target_link_libraries(vector_base PUBLIC string_interning)
target_link_libraries(vector_base PUBLIC hnswlib_vmsdk)
target_link_libraries(vector_base PUBLIC iostream)
target_link_libraries(vector_base PUBLIC valkey_module)
target_link_libraries(vector_base PUBLIC vmsdklib)

set(SRCS_VECTOR_HNSW ${CMAKE_CURRENT_LIST_DIR}/vector_hnsw.cc
                     ${CMAKE_CURRENT_LIST_DIR}/vector_hnsw.h)

valkey_search_add_static_library(vector_hnsw "${SRCS_VECTOR_HNSW}")
target_include_directories(vector_hnsw PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_hnsw PUBLIC index_base)
target_link_libraries(vector_hnsw PUBLIC vector_base)
target_link_libraries(vector_hnsw PUBLIC attribute_data_type)
target_link_libraries(vector_hnsw PUBLIC metrics)
target_link_libraries(vector_hnsw PUBLIC rdb_serialization)
target_link_libraries(vector_hnsw PUBLIC string_interning)
target_link_libraries(vector_hnsw PUBLIC hnswlib_vmsdk)
target_link_libraries(vector_hnsw PUBLIC vmsdklib)
target_link_libraries(vector_hnsw PUBLIC valkey_module)

set(SRCS_NUMERIC ${CMAKE_CURRENT_LIST_DIR}/numeric.cc
                 ${CMAKE_CURRENT_LIST_DIR}/numeric.h)

valkey_search_add_static_library(numeric "${SRCS_NUMERIC}")
target_include_directories(numeric PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(numeric PUBLIC index_base)
target_link_libraries(numeric PUBLIC rdb_serialization)
target_link_libraries(numeric PUBLIC predicate_header)
target_link_libraries(numeric PUBLIC segment_tree)
target_link_libraries(numeric PUBLIC string_interning)
target_link_libraries(numeric PUBLIC valkey_module)

set(SRCS_UNIVERSAL_SET_FETCHER ${CMAKE_CURRENT_LIST_DIR}/universal_set_fetcher.cc
                               ${CMAKE_CURRENT_LIST_DIR}/universal_set_fetcher.h)

valkey_search_add_static_library(universal_set_fetcher "${SRCS_UNIVERSAL_SET_FETCHER}")
target_link_libraries(universal_set_fetcher PUBLIC index_base)

set(SRCS_TAG ${CMAKE_CURRENT_LIST_DIR}/tag.cc ${CMAKE_CURRENT_LIST_DIR}/tag.h)

valkey_search_add_static_library(tag "${SRCS_TAG}")
target_include_directories(tag PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(tag PUBLIC index_base)
target_link_libraries(tag PUBLIC rdb_serialization)
target_link_libraries(tag PUBLIC predicate_header)
target_link_libraries(tag PUBLIC patricia_tree)
target_link_libraries(tag PUBLIC string_interning)
target_link_libraries(tag PUBLIC valkey_module)
target_link_libraries(tag PUBLIC vmsdklib)
target_link_libraries(tag PUBLIC ${INDEX_SCHEMA_PROTO_LIB})

set(SRCS_VECTOR_FLAT ${CMAKE_CURRENT_LIST_DIR}/vector_flat.cc
                     ${CMAKE_CURRENT_LIST_DIR}/vector_flat.h)

valkey_search_add_static_library(vector_flat "${SRCS_VECTOR_FLAT}")
target_include_directories(vector_flat PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_flat PUBLIC index_base)
target_link_libraries(vector_flat PUBLIC vector_base)
target_link_libraries(vector_flat PUBLIC attribute_data_type)
target_link_libraries(vector_flat PUBLIC metrics)
target_link_libraries(vector_flat PUBLIC rdb_serialization)
target_link_libraries(vector_flat PUBLIC string_interning)
target_link_libraries(vector_flat PUBLIC hnswlib_vmsdk)
target_link_libraries(vector_flat PUBLIC vmsdklib)
target_link_libraries(vector_flat PUBLIC valkey_module)

set(SRCS_TEXT ${CMAKE_CURRENT_LIST_DIR}/text/text_index.h
              ${CMAKE_CURRENT_LIST_DIR}/text/text_index.cc
              ${CMAKE_CURRENT_LIST_DIR}/text.cc
              ${CMAKE_CURRENT_LIST_DIR}/text.h
              ${CMAKE_CURRENT_LIST_DIR}/text/posting.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/posting.h
              ${CMAKE_CURRENT_LIST_DIR}/text/textinfocmd.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/text_fetcher.h
              ${CMAKE_CURRENT_LIST_DIR}/text/text_fetcher.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/text_iterator.h
              ${CMAKE_CURRENT_LIST_DIR}/text/proximity.h
              ${CMAKE_CURRENT_LIST_DIR}/text/proximity.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/orproximity.h
              ${CMAKE_CURRENT_LIST_DIR}/text/orproximity.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/term.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/term.h
              ${CMAKE_CURRENT_LIST_DIR}/text/lexer.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/lexer.h
              ${CMAKE_CURRENT_LIST_DIR}/text/unicode_normalizer.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/unicode_normalizer.h
              ${CMAKE_CURRENT_LIST_DIR}/text/fuzzy.h
              ${CMAKE_CURRENT_LIST_DIR}/text/flat_position_map.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/flat_position_map.h
              ${CMAKE_CURRENT_LIST_DIR}/text/rax_wrapper.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/rax/rax.c)

valkey_search_add_static_library(text "${SRCS_TEXT}")
target_include_directories(text PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(text PUBLIC index_base)
target_link_libraries(text PUBLIC index_schema_cc_proto)
target_link_libraries(text PUBLIC rdb_serialization)
target_link_libraries(text PUBLIC string_interning)
target_link_libraries(text PUBLIC valkey_module)
target_link_libraries(text PUBLIC snowball)
target_link_libraries(text PUBLIC scanner)
target_link_libraries(text PUBLIC icu) 
