add_subdirectory(valkey_module_api)
add_subdirectory(testing_infra)

set(VMSDKLIB_SRCS
    ${CMAKE_CURRENT_LIST_DIR}/log.cc
    ${CMAKE_CURRENT_LIST_DIR}/log.h
    ${CMAKE_CURRENT_LIST_DIR}/concurrency.cc
    ${CMAKE_CURRENT_LIST_DIR}/concurrency.h
    ${CMAKE_CURRENT_LIST_DIR}/debug.cc
    ${CMAKE_CURRENT_LIST_DIR}/debug.h
    ${CMAKE_CURRENT_LIST_DIR}/info.h
    ${CMAKE_CURRENT_LIST_DIR}/info.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils.h
    ${CMAKE_CURRENT_LIST_DIR}/cluster_map.cc
    ${CMAKE_CURRENT_LIST_DIR}/cluster_map.h
    ${CMAKE_CURRENT_LIST_DIR}/latency_sampler.h
    ${CMAKE_CURRENT_LIST_DIR}/module_type.cc
    ${CMAKE_CURRENT_LIST_DIR}/module_type.h
    ${CMAKE_CURRENT_LIST_DIR}/thread_pool.cc
    ${CMAKE_CURRENT_LIST_DIR}/thread_pool.h
    ${CMAKE_CURRENT_LIST_DIR}/thread_monitoring.h
    ${CMAKE_CURRENT_LIST_DIR}/thread_monitoring.cc
    ${CMAKE_CURRENT_LIST_DIR}/time_sliced_mrmw_mutex.cc
    ${CMAKE_CURRENT_LIST_DIR}/time_sliced_mrmw_mutex.h
    ${CMAKE_CURRENT_LIST_DIR}/command_parser.h
    ${CMAKE_CURRENT_LIST_DIR}/module.cc
    ${CMAKE_CURRENT_LIST_DIR}/module.h
    ${CMAKE_CURRENT_LIST_DIR}/module_config.cc
    ${CMAKE_CURRENT_LIST_DIR}/module_config.h
    ${CMAKE_CURRENT_LIST_DIR}/memory_allocation_overrides.cc
    ${CMAKE_CURRENT_LIST_DIR}/memory_allocation_overrides.h
    ${CMAKE_CURRENT_LIST_DIR}/memory_allocation.cc
    ${CMAKE_CURRENT_LIST_DIR}/memory_allocation.h
    ${CMAKE_CURRENT_LIST_DIR}/malloc_capture.cc
    ${CMAKE_CURRENT_LIST_DIR}/malloc_capture.h
    ${CMAKE_CURRENT_LIST_DIR}/pooled_memory.cc
    ${CMAKE_CURRENT_LIST_DIR}/pooled_memory.h
    ${CMAKE_CURRENT_LIST_DIR}/type_conversions.h
    ${CMAKE_CURRENT_LIST_DIR}/managed_pointers.h
    ${CMAKE_CURRENT_LIST_DIR}/blocked_client.cc
    ${CMAKE_CURRENT_LIST_DIR}/blocked_client.h
    ${CMAKE_CURRENT_LIST_DIR}/memory_tracker.cc
    ${CMAKE_CURRENT_LIST_DIR}/memory_tracker.h
    ${CMAKE_CURRENT_LIST_DIR}/status/status_builder.cc
    ${CMAKE_CURRENT_LIST_DIR}/status/source_location.h
    ${CMAKE_CURRENT_LIST_DIR}/status/status_builder.h
    ${CMAKE_CURRENT_LIST_DIR}/status/status_macros.h)

valkey_search_add_static_library(vmsdklib "${VMSDKLIB_SRCS}")
target_include_directories(vmsdklib PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(vmsdklib PUBLIC ${CMAKE_CURRENT_LIST_DIR}/status)
target_link_libraries(vmsdklib INTERFACE hdrhistogram_c)

if(APPLE)
  target_link_libraries(vmsdklib PUBLIC absl::core_headers absl::base
                                        absl::any_invocable)
else()
  target_link_libraries(vmsdklib PUBLIC ${GRPC_LIB})
endif()
