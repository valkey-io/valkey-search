valkey_search_create_proto_library("src/index_schema.proto"
                                   "index_schema_cc_proto")

valkey_search_create_proto_library("src/indexes/svs_index.proto"
                                   "svs_index_cc_proto")
target_link_libraries(svs_index_cc_proto PUBLIC index_schema_cc_proto)

set(SRCS_SCORING
    ${CMAKE_CURRENT_LIST_DIR}/scoring/scorer.h
    ${CMAKE_CURRENT_LIST_DIR}/scoring/scorer.cc
    ${CMAKE_CURRENT_LIST_DIR}/scoring/bm25std_scorer.h
    ${CMAKE_CURRENT_LIST_DIR}/scoring/bm25std_scorer.cc)

valkey_search_add_static_library(scoring "${SRCS_SCORING}")
target_include_directories(scoring PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(scoring PUBLIC vmsdklib)

add_library(index_base INTERFACE ${SRCS_INDEX_BASE})
target_include_directories(index_base INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(index_base INTERFACE index_schema_cc_proto)
target_link_libraries(index_base INTERFACE svs_index_cc_proto)
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
target_link_libraries(vector_base PUBLIC predicate)
target_link_libraries(vector_base PUBLIC query_context)
target_link_libraries(vector_base PUBLIC allocator)
target_link_libraries(vector_base PUBLIC intrusive_ref_count)
target_link_libraries(vector_base PUBLIC string_interning)
target_link_libraries(vector_base PUBLIC hnswlib_vmsdk)
target_link_libraries(vector_base PUBLIC iostream)
target_link_libraries(vector_base PUBLIC valkey_module)
target_link_libraries(vector_base PUBLIC vmsdklib)

set(SRCS_VECTOR_TYPE ${CMAKE_CURRENT_LIST_DIR}/vector_type.cc
                     ${CMAKE_CURRENT_LIST_DIR}/vector_type.h)

valkey_search_add_static_library(vector_type "${SRCS_VECTOR_TYPE}")
target_include_directories(vector_type PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_type PUBLIC index_base)
target_link_libraries(vector_type PUBLIC vector_base)
target_link_libraries(vector_type PUBLIC index_schema_cc_proto)
target_link_libraries(vector_type PUBLIC hnswlib_vmsdk)
target_link_libraries(vector_type PUBLIC vmsdklib)
target_link_libraries(vector_type PUBLIC valkey_module)

set(SRCS_VECTOR_HNSW ${CMAKE_CURRENT_LIST_DIR}/vector_hnsw.cc
                     ${CMAKE_CURRENT_LIST_DIR}/vector_hnsw.h)

valkey_search_add_static_library(vector_hnsw "${SRCS_VECTOR_HNSW}")
target_include_directories(vector_hnsw PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_hnsw PUBLIC index_base)
target_link_libraries(vector_hnsw PUBLIC vector_base)
target_link_libraries(vector_hnsw PUBLIC vector_type)
target_link_libraries(vector_hnsw PUBLIC attribute_data_type)
target_link_libraries(vector_hnsw PUBLIC metrics)
target_link_libraries(vector_hnsw PUBLIC rdb_serialization)
target_link_libraries(vector_hnsw PUBLIC scoring)
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
target_link_libraries(numeric PUBLIC numeric_btree)
target_link_libraries(numeric PUBLIC string_interning)
target_link_libraries(numeric PUBLIC valkey_module)

set(SRCS_UNIVERSAL_SET_FETCHER ${CMAKE_CURRENT_LIST_DIR}/universal_set_fetcher.cc
                               ${CMAKE_CURRENT_LIST_DIR}/universal_set_fetcher.h)

valkey_search_add_static_library(universal_set_fetcher "${SRCS_UNIVERSAL_SET_FETCHER}")
target_link_libraries(universal_set_fetcher PUBLIC index_base)

set(SRCS_RAX_LIB ${CMAKE_CURRENT_LIST_DIR}/text/rax/rax.c)
valkey_search_add_static_library(rax_lib "${SRCS_RAX_LIB}")
target_include_directories(rax_lib PUBLIC ${CMAKE_CURRENT_LIST_DIR}/text)

set(SRCS_TAG ${CMAKE_CURRENT_LIST_DIR}/tag.cc ${CMAKE_CURRENT_LIST_DIR}/tag.h)

valkey_search_add_static_library(tag "${SRCS_TAG}")
target_include_directories(tag PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(tag PUBLIC index_base)
target_link_libraries(tag PUBLIC rdb_serialization)
target_link_libraries(tag PUBLIC predicate_header)
target_link_libraries(tag PUBLIC string_interning)
target_link_libraries(tag PUBLIC valkey_module)
target_link_libraries(tag PUBLIC vmsdklib)
target_link_libraries(tag PUBLIC ${INDEX_SCHEMA_PROTO_LIB})
target_link_libraries(tag PUBLIC rax_lib)

set(SRCS_VECTOR_SVS ${CMAKE_CURRENT_LIST_DIR}/vector_svs.cc
                    ${CMAKE_CURRENT_LIST_DIR}/vector_svs.h)

valkey_search_add_static_library(vector_svs "${SRCS_VECTOR_SVS}")
target_include_directories(vector_svs PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_svs PUBLIC index_base)
target_link_libraries(vector_svs PUBLIC vector_base)
target_link_libraries(vector_svs PUBLIC vector_type)
target_link_libraries(vector_svs PUBLIC attribute_data_type)
target_link_libraries(vector_svs PUBLIC metrics)
target_link_libraries(vector_svs PUBLIC rdb_serialization)
target_link_libraries(vector_svs PUBLIC string_interning)
target_link_libraries(vector_svs PUBLIC hnswlib_vmsdk)
target_link_libraries(vector_svs PUBLIC vmsdklib)
target_link_libraries(vector_svs PUBLIC valkey_module)

# The SVS submodule (third_party/svs) is compiled on x86_64 Linux only.
# The SVS C API implementation sources compile with SVS's own strict
# warnings-as-errors flags (via svs_compile_options), which would cascade
# to vector_svs.cc's compile line if the two were mixed. Keep them in a
# dedicated static library so those flags stay scoped, and have vector_svs
# link it. On other platforms vector_svs.cc compiles as a stub whose
# factories return absl::UnimplementedError; arch is detected in the
# source via __linux__ && __x86_64__ rather than a project flag.
#
# Built without OpenMP: threading comes from the module through the C API's
# threadpool vtable, so libgomp must not enter DT_NEEDED (ci/check_svs_link.sh).
if(UNIX AND NOT APPLE
   AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    set(SVS_C_API_DIR "${CMAKE_SOURCE_DIR}/third_party/svs/bindings/c")
    set(SRCS_SVS_C_API
        ${SVS_C_API_DIR}/src/error.cpp
        ${SVS_C_API_DIR}/src/svs_c.cpp
        ${SVS_C_API_DIR}/src/dispatcher_vamana.cpp
        ${SVS_C_API_DIR}/src/dispatcher_dynamic_vamana.cpp
        ${SVS_C_API_DIR}/src/index_builder.cpp
        ${SVS_C_API_DIR}/src/index_vamana.cpp
        ${SVS_C_API_DIR}/src/leanvec_training_data.cpp
        ${SVS_C_API_DIR}/src/data_builder.cpp)
    valkey_search_add_static_library(svs_c_api_static "${SRCS_SVS_C_API}")
    target_include_directories(svs_c_api_static PUBLIC
        ${SVS_C_API_DIR}/include
        ${CMAKE_BINARY_DIR}/third_party/svs-c-api-build/include)
    target_include_directories(svs_c_api_static PRIVATE ${SVS_C_API_DIR}/src)
    target_compile_options(svs_c_api_static PRIVATE -fvisibility=hidden)
    get_target_property(_svs_c_api_opts svs_c_api_static COMPILE_OPTIONS)
    list(REMOVE_ITEM _svs_c_api_opts -fopenmp)
    set_target_properties(svs_c_api_static PROPERTIES
        COMPILE_OPTIONS "${_svs_c_api_opts}")
    # Interim; see the header.
    target_compile_options(svs_c_api_static PRIVATE
        "SHELL:-include ${CMAKE_CURRENT_LIST_DIR}/svs_shim/svs_omp_shim.h")
    target_link_libraries(svs_c_api_static PRIVATE
        svs::svs
        svs_compile_options
        svs_x86_options_base)

    target_link_libraries(vector_svs PRIVATE svs_c_api_static)
endif()

set(SRCS_VECTOR_FLAT ${CMAKE_CURRENT_LIST_DIR}/vector_flat.cc
                     ${CMAKE_CURRENT_LIST_DIR}/vector_flat.h)

valkey_search_add_static_library(vector_flat "${SRCS_VECTOR_FLAT}")
target_include_directories(vector_flat PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_flat PUBLIC index_base)
target_link_libraries(vector_flat PUBLIC vector_base)
target_link_libraries(vector_flat PUBLIC vector_type)
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
              ${CMAKE_CURRENT_LIST_DIR}/text/rax_target_mutex_pool.h
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
              ${CMAKE_CURRENT_LIST_DIR}/text/language_registry.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/language_registry.h
              ${CMAKE_CURRENT_LIST_DIR}/text/snowball_language.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/snowball_language.h
              ${CMAKE_CURRENT_LIST_DIR}/text/snowball_stem.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/snowball_stem.h
              ${CMAKE_CURRENT_LIST_DIR}/text/unicode_normalizer.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/unicode_normalizer.h
              ${CMAKE_CURRENT_LIST_DIR}/text/fuzzy.h
              ${CMAKE_CURRENT_LIST_DIR}/text/flat_position_map.cc
              ${CMAKE_CURRENT_LIST_DIR}/text/flat_position_map.h
              ${CMAKE_CURRENT_LIST_DIR}/text/rax_wrapper.cc)

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
target_link_libraries(text PUBLIC rax_lib)
target_link_libraries(text PUBLIC scoring)
