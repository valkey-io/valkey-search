set(SRCS_METADATA_MANAGER_TEST
    ${CMAKE_CURRENT_LIST_DIR}/metadata_manager_test.cc)

add_executable(metadata_manager_test ${SRCS_METADATA_MANAGER_TEST})
target_include_directories(metadata_manager_test
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(metadata_manager_test PUBLIC coordinator_common)
target_link_libraries(metadata_manager_test PUBLIC client)
target_link_libraries(metadata_manager_test PUBLIC client_pool)
target_link_libraries(metadata_manager_test PUBLIC coordinator_cc_proto)
target_link_libraries(metadata_manager_test PUBLIC metadata_manager)
target_link_libraries(metadata_manager_test PUBLIC util)
target_link_libraries(metadata_manager_test PUBLIC testing_common)
target_link_libraries(metadata_manager_test PUBLIC managed_pointers)
target_link_libraries(metadata_manager_test PUBLIC module)
target_link_libraries(metadata_manager_test PUBLIC utils)
target_link_libraries(metadata_manager_test PUBLIC valkey_module)
finalize_test_flags(metadata_manager_test)

# Add client test
set(SRCS_CLIENT_TEST
    ${CMAKE_CURRENT_LIST_DIR}/client_test.cc)

add_executable(client_test ${SRCS_CLIENT_TEST})
target_include_directories(client_test
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(client_test PUBLIC coordinator_common)
target_link_libraries(client_test PUBLIC client)
target_link_libraries(client_test PUBLIC coordinator_cc_proto)
target_link_libraries(client_test PUBLIC testing_common)
target_link_libraries(client_test PUBLIC managed_pointers)
target_link_libraries(client_test PUBLIC module)
target_link_libraries(client_test PUBLIC utils)
target_link_libraries(client_test PUBLIC valkey_module)
finalize_test_flags(client_test)

# Add info index partition client test
set(SRCS_INFO_INDEX_PARTITION_CLIENT_TEST
    ${CMAKE_CURRENT_LIST_DIR}/info_index_partition_client_test.cc)

add_executable(info_index_partition_client_test ${SRCS_INFO_INDEX_PARTITION_CLIENT_TEST})
target_include_directories(info_index_partition_client_test
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(info_index_partition_client_test PUBLIC coordinator_common)
target_link_libraries(info_index_partition_client_test PUBLIC client)
target_link_libraries(info_index_partition_client_test PUBLIC coordinator_cc_proto)
target_link_libraries(info_index_partition_client_test PUBLIC testing_common)
target_link_libraries(info_index_partition_client_test PUBLIC managed_pointers)
target_link_libraries(info_index_partition_client_test PUBLIC module)
target_link_libraries(info_index_partition_client_test PUBLIC utils)
target_link_libraries(info_index_partition_client_test PUBLIC valkey_module)
target_link_libraries(info_index_partition_client_test PUBLIC metrics)
finalize_test_flags(info_index_partition_client_test)

# Add info index partition integration test
set(SRCS_INFO_INDEX_PARTITION_INTEGRATION_TEST
    ${CMAKE_CURRENT_LIST_DIR}/info_index_partition_integration_test.cc)

add_executable(info_index_partition_integration_test ${SRCS_INFO_INDEX_PARTITION_INTEGRATION_TEST})
target_include_directories(info_index_partition_integration_test
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(info_index_partition_integration_test PUBLIC coordinator_common)
target_link_libraries(info_index_partition_integration_test PUBLIC client)
target_link_libraries(info_index_partition_integration_test PUBLIC coordinator_cc_proto)
target_link_libraries(info_index_partition_integration_test PUBLIC testing_common)
target_link_libraries(info_index_partition_integration_test PUBLIC managed_pointers)
target_link_libraries(info_index_partition_integration_test PUBLIC module)
target_link_libraries(info_index_partition_integration_test PUBLIC utils)
target_link_libraries(info_index_partition_integration_test PUBLIC valkey_module)
target_link_libraries(info_index_partition_integration_test PUBLIC metrics)
finalize_test_flags(info_index_partition_integration_test)

set(SRCS_COMMON ${CMAKE_CURRENT_LIST_DIR}/common.h)

add_library(coordinator_common INTERFACE ${SRCS_COMMON})
target_include_directories(coordinator_common
                           INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(coordinator_common INTERFACE client)
target_link_libraries(coordinator_common INTERFACE client_pool)
target_link_libraries(coordinator_common INTERFACE server)
