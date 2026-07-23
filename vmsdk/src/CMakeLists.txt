include (object_library_helper)

add_subdirectory(valkey_module_api)
add_subdirectory(testing_infra)

set(VMSDKLIB_SRCS
    log.cc
    log.h
    concurrency.cc
    concurrency.h
    debug.cc
    debug.h
    info.h
    info.cc
    utils.cc
    utils.h
    cluster_map.cc
    cluster_map.h
    latency_sampler.h
    module_type.cc
    module_type.h
    thread_pool.cc
    thread_pool.h
    thread_monitoring.h
    thread_monitoring.cc
    thread_group_cpu_monitor.h
    thread_group_cpu_monitor.cc
    time_sliced_mrmw_mutex.cc
    time_sliced_mrmw_mutex.h
    command_parser.h
    module.cc
    module.h
    module_config.cc
    module_config.h
    memory_allocation_overrides.cc
    memory_allocation_overrides.h
    memory_allocation.cc
    memory_allocation.h
    type_conversions.h
    managed_pointers.h
    blocked_client.cc
    blocked_client.h
    memory_tracker.cc
    memory_tracker.h
    status/status_builder.cc
    status/source_location.h
    status/status_builder.h
    status/status_macros.h)

add_library(vmsdk OBJECT ${VMSDKLIB_SRCS})

target_include_directories(vmsdk PUBLIC .)
target_include_directories(vmsdk PRIVATE status)
target_include_directories(vmsdk PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_include_directories(vmsdk PRIVATE ${PROJECT_SOURCE_DIR})

target_link_libraries(vmsdk PUBLIC hdrhistogram_c)
target_link_libraries(vmsdk PUBLIC absl::core_headers
        absl::base
        absl::flat_hash_map
        absl::any_invocable
        absl::log
        absl::statusor
        absl::strings
        absl::string_view
        absl::synchronization
        absl::random_random
        absl::check)
target_link_libraries(vmsdk PUBLIC ${GRPC_LIB})
target_link_libraries(vmsdk PUBLIC highwayhash::highwayhash)

if (NOT VALKEY_VENDORED_DEPS)
    add_library(vmsdk_dso SHARED)
    target_link_libraries(vmsdk_dso PUBLIC vmsdk)
    target_sources(vmsdk_dso PRIVATE $<TARGET_OBJECTS:vmsdk>)
endif ()