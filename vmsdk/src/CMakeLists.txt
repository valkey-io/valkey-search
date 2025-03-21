add_subdirectory(status)
add_subdirectory(valkey_module_api)
add_subdirectory(testing_infra)

set(SRCS_LOG ${CMAKE_CURRENT_LIST_DIR}/log.cc ${CMAKE_CURRENT_LIST_DIR}/log.h)

valkey_search_add_static_library(log "${SRCS_LOG}")
target_include_directories(log PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(log PUBLIC managed_pointers)
target_link_libraries(log PUBLIC status_macros)
target_link_libraries(log PUBLIC valkey_module)

set(SRCS_CONCURRENCY ${CMAKE_CURRENT_LIST_DIR}/concurrency.cc ${CMAKE_CURRENT_LIST_DIR}/concurrency.h)

valkey_search_add_static_library(concurrency "${SRCS_CONCURRENCY}")
target_include_directories(concurrency PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(concurrency PUBLIC valkey_module)
if(APPLE)
  target_link_libraries(concurrency PUBLIC absl::core_headers)
else()
  target_link_libraries(concurrency PUBLIC ${GRPC_LIB})
endif()

set(SRCS_UTILS ${CMAKE_CURRENT_LIST_DIR}/utils.cc
               ${CMAKE_CURRENT_LIST_DIR}/utils.h)

valkey_search_add_static_library(utils "${SRCS_UTILS}")
target_include_directories(utils PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(utils PUBLIC valkey_module)
if(APPLE)
  target_link_libraries(utils PUBLIC absl::core_headers)
else()
  target_link_libraries(utils PUBLIC ${GRPC_LIB})
endif()

set(SRCS_LATENCY_SAMPLER ${CMAKE_CURRENT_LIST_DIR}/latency_sampler.h)

add_library(latency_sampler INTERFACE ${SRCS_LATENCY_SAMPLER})
target_include_directories(latency_sampler INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(latency_sampler INTERFACE utils)
target_link_libraries(latency_sampler INTERFACE hdrhistogram_c)

set(SRCS_MODULE_TYPE ${CMAKE_CURRENT_LIST_DIR}/module_type.cc
                     ${CMAKE_CURRENT_LIST_DIR}/module_type.h)

valkey_search_add_static_library(module_type "${SRCS_MODULE_TYPE}")
target_include_directories(module_type PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(module_type PUBLIC log)
target_link_libraries(module_type PUBLIC managed_pointers)
target_link_libraries(module_type PUBLIC valkey_module)

set(SRCS_THREAD_POOL ${CMAKE_CURRENT_LIST_DIR}/thread_pool.cc
                     ${CMAKE_CURRENT_LIST_DIR}/thread_pool.h)

valkey_search_add_static_library(thread_pool "${SRCS_THREAD_POOL}")
target_include_directories(thread_pool PUBLIC ${CMAKE_CURRENT_LIST_DIR})
if(APPLE)
  target_link_libraries(thread_pool PUBLIC absl::base absl::any_invocable)
else()
  target_link_libraries(thread_pool PUBLIC ${GRPC_LIB})
endif()

set(SRCS_TIME_SLICED_MRMW_MUTEX
    ${CMAKE_CURRENT_LIST_DIR}/time_sliced_mrmw_mutex.cc
    ${CMAKE_CURRENT_LIST_DIR}/time_sliced_mrmw_mutex.h)

valkey_search_add_static_library(time_sliced_mrmw_mutex
                                 "${SRCS_TIME_SLICED_MRMW_MUTEX}")
target_include_directories(time_sliced_mrmw_mutex
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(time_sliced_mrmw_mutex PUBLIC utils)

set(SRCS_COMMAND_PARSER ${CMAKE_CURRENT_LIST_DIR}/command_parser.h)

add_library(command_parser INTERFACE ${SRCS_COMMAND_PARSER})
target_include_directories(command_parser INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(command_parser INTERFACE managed_pointers)
target_link_libraries(command_parser INTERFACE type_conversions)
target_link_libraries(command_parser INTERFACE status_macros)
target_link_libraries(command_parser INTERFACE valkey_module)

set(SRCS_MODULE ${CMAKE_CURRENT_LIST_DIR}/module.cc
                ${CMAKE_CURRENT_LIST_DIR}/module.h)

valkey_search_add_static_library(module "${SRCS_MODULE}")
target_include_directories(module PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(module PUBLIC log)
# target_link_libraries(module PUBLIC memory_allocation)
target_link_libraries(module PUBLIC utils)
target_link_libraries(module PUBLIC valkey_module)

set(SRCS_MEMORY_ALLOCATION_OVERRIDES
    ${CMAKE_CURRENT_LIST_DIR}/memory_allocation_overrides.cc
    ${CMAKE_CURRENT_LIST_DIR}/memory_allocation_overrides.h)

add_library(memory_allocation_overrides STATIC
            ${SRCS_MEMORY_ALLOCATION_OVERRIDES})
target_include_directories(memory_allocation_overrides
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(memory_allocation_overrides PUBLIC memory_allocation)
target_link_libraries(memory_allocation_overrides PUBLIC valkey_module)

set(SRCS_MEMORY_ALLOCATION ${CMAKE_CURRENT_LIST_DIR}/memory_allocation.cc
                           ${CMAKE_CURRENT_LIST_DIR}/memory_allocation.h)

valkey_search_add_static_library(memory_allocation "${SRCS_MEMORY_ALLOCATION}")
target_include_directories(memory_allocation PUBLIC ${CMAKE_CURRENT_LIST_DIR})

set(SRCS_TYPE_CONVERSIONS ${CMAKE_CURRENT_LIST_DIR}/type_conversions.h)

add_library(type_conversions INTERFACE ${SRCS_TYPE_CONVERSIONS})
target_include_directories(type_conversions INTERFACE ${CMAKE_CURRENT_LIST_DIR})

set(SRCS_MANAGED_POINTERS ${CMAKE_CURRENT_LIST_DIR}/managed_pointers.h)

add_library(managed_pointers INTERFACE ${SRCS_MANAGED_POINTERS})
target_include_directories(managed_pointers INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(managed_pointers INTERFACE utils)
target_link_libraries(managed_pointers INTERFACE valkey_module)
