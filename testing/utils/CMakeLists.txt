set(SRCS_ALLOCATOR_TEST ${CMAKE_CURRENT_LIST_DIR}/allocator_test.cc)

add_executable(allocator_test ${SRCS_ALLOCATOR_TEST})
target_include_directories(allocator_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(allocator_test PUBLIC allocator)
target_link_libraries(allocator_test PUBLIC log)
target_link_libraries(allocator_test PUBLIC intrusive_ref_count)
target_link_libraries(allocator_test PUBLIC vmsdk_testing_infra)
finalize_test_flags(allocator_test)

set(SRCS_INTRUSIVE_LIST_TEST ${CMAKE_CURRENT_LIST_DIR}/intrusive_list_test.cc)

add_executable(intrusive_list_test ${SRCS_INTRUSIVE_LIST_TEST})
target_include_directories(intrusive_list_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(intrusive_list_test PUBLIC intrusive_list)
finalize_test_flags(intrusive_list_test)

set(SRCS_INTRUSIVE_REF_COUNT_TEST
    ${CMAKE_CURRENT_LIST_DIR}/intrusive_ref_count_test.cc)

add_executable(intrusive_ref_count_test ${SRCS_INTRUSIVE_REF_COUNT_TEST})
target_include_directories(intrusive_ref_count_test
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(intrusive_ref_count_test PUBLIC intrusive_ref_count)
finalize_test_flags(intrusive_ref_count_test)

set(SRCS_LRU_TEST ${CMAKE_CURRENT_LIST_DIR}/lru_test.cc)

add_executable(lru_test ${SRCS_LRU_TEST})
target_include_directories(lru_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(lru_test PUBLIC lru)
finalize_test_flags(lru_test)

set(SRCS_PATRICIA_TREE_TEST ${CMAKE_CURRENT_LIST_DIR}/patricia_tree_test.cc)

add_executable(patricia_tree_test ${SRCS_PATRICIA_TREE_TEST})
target_include_directories(patricia_tree_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(patricia_tree_test PUBLIC patricia_tree)
finalize_test_flags(patricia_tree_test)

set(SRCS_SEGMENT_TREE_TEST ${CMAKE_CURRENT_LIST_DIR}/segment_tree_test.cc)

add_executable(segment_tree_test ${SRCS_SEGMENT_TREE_TEST})
target_include_directories(segment_tree_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(segment_tree_test PUBLIC segment_tree)
finalize_test_flags(segment_tree_test)

set(SRCS_STRING_INTERNING_TEST
    ${CMAKE_CURRENT_LIST_DIR}/string_interning_test.cc)

add_executable(string_interning_test ${SRCS_STRING_INTERNING_TEST})
target_include_directories(string_interning_test
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(string_interning_test PUBLIC allocator)
target_link_libraries(string_interning_test PUBLIC intrusive_ref_count)
target_link_libraries(string_interning_test PUBLIC string_interning)
target_link_libraries(string_interning_test PUBLIC utils)
target_link_libraries(string_interning_test PUBLIC log)
finalize_test_flags(string_interning_test)
