set(SRCS_PATRICIA_TREE ${CMAKE_CURRENT_LIST_DIR}/patricia_tree.h)

add_library(patricia_tree INTERFACE ${SRCS_PATRICIA_TREE})
target_include_directories(patricia_tree INTERFACE ${CMAKE_CURRENT_LIST_DIR})

set(SRCS_SEGMENT_TREE ${CMAKE_CURRENT_LIST_DIR}/segment_tree.h)

add_library(segment_tree INTERFACE ${SRCS_SEGMENT_TREE})
target_include_directories(segment_tree INTERFACE ${CMAKE_CURRENT_LIST_DIR})

set(SRCS_STRING_INTERNING ${CMAKE_CURRENT_LIST_DIR}/string_interning.cc
                          ${CMAKE_CURRENT_LIST_DIR}/string_interning.h)

valkey_search_add_static_library(string_interning "${SRCS_STRING_INTERNING}")
target_include_directories(string_interning PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(string_interning PUBLIC allocator)
target_link_libraries(string_interning PUBLIC vmsdklib)

set(SRCS_ALLOCATOR
    ${CMAKE_CURRENT_LIST_DIR}/allocator.cc
    ${CMAKE_CURRENT_LIST_DIR}/allocator.h
    ${CMAKE_CURRENT_LIST_DIR}/cancel.h
    ${CMAKE_CURRENT_LIST_DIR}/cancel.cc)

valkey_search_add_static_library(allocator "${SRCS_ALLOCATOR}")
target_include_directories(allocator PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(allocator PUBLIC intrusive_list)
target_link_libraries(allocator PUBLIC intrusive_ref_count)
if(APPLE)
  target_link_libraries(allocator PUBLIC absl::base)
else()
  target_link_libraries(allocator PUBLIC ${GRPC_LIB})
endif()

set(SRCS_INTRUSIVE_LIST ${CMAKE_CURRENT_LIST_DIR}/intrusive_list.h)

add_library(intrusive_list INTERFACE ${SRCS_INTRUSIVE_LIST})
target_include_directories(intrusive_list INTERFACE ${CMAKE_CURRENT_LIST_DIR})

set(SRCS_LRU ${CMAKE_CURRENT_LIST_DIR}/lru.h)

add_library(lru INTERFACE ${SRCS_LRU})
target_include_directories(lru INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(lru INTERFACE intrusive_list)

set(SRCS_INTRUSIVE_REF_COUNT ${CMAKE_CURRENT_LIST_DIR}/intrusive_ref_count.h)

add_library(intrusive_ref_count INTERFACE ${SRCS_INTRUSIVE_REF_COUNT})
target_include_directories(intrusive_ref_count
                           INTERFACE ${CMAKE_CURRENT_LIST_DIR})
