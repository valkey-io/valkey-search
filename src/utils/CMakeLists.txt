add_library(vlks_utils OBJECT
    patricia_tree.h
    string_interning.cc
    string_interning.h
    allocator.cc
    allocator.h
    cancel.h
    cancel.cc
    intrusive_list.h
    lru.h
    intrusive_ref_count.h
    scanner.h)

target_include_directories(vlks_utils PUBLIC .)
add_library(valkey_search::utils ALIAS vlks_utils)
target_link_libraries(vlks_utils PUBLIC mydeps)