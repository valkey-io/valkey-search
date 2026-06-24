include (object_library_helper)

add_library(index_common OBJECT
    index_base.h
    numeric.cc
    numeric.h
    tag.cc
    tag.h
    universal_set_fetcher.cc
    universal_set_fetcher.h
)
target_link_libraries(index_common PRIVATE mydeps)

add_library(index_vector OBJECT
    vector_base.cc
    vector_base.h
    vector_flat.cc
    vector_flat.h
    vector_hnsw.cc
    vector_hnsw.h
)

target_link_libraries(index_vector PRIVATE mydeps)
target_link_libraries(index_vector PRIVATE hnswlib)

add_library(index_text OBJECT
    text/text_index.h
    text/text_index.cc
    text.cc
    text.h
    text/rax_target_mutex_pool.h
    text/posting.cc
    text/posting.h
    text/textinfocmd.cc
    text/text_fetcher.h
    text/text_fetcher.cc
    text/text_iterator.h
    text/proximity.h
    text/proximity.cc
    text/orproximity.h
    text/orproximity.cc
    text/term.cc
    text/term.h
    text/lexer.cc
    text/lexer.h
    text/unicode_normalizer.cc
    text/unicode_normalizer.h
    text/fuzzy.h
    text/flat_position_map.cc
    text/flat_position_map.h
    text/rax_wrapper.cc
    text/rax/rax.c
    text/rax/rax.h
)

target_link_libraries(index_text PRIVATE mydeps)
target_link_libraries(index_text PRIVATE ICU::uc ICU::i18n ICU::data)
target_link_libraries(index_text PRIVATE snowball)

create_object_interface(indexes index_common index_vector index_text)