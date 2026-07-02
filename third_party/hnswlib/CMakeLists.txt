set(SRCS_HNSWLIB_VMSDK
    ${CMAKE_CURRENT_LIST_DIR}/bruteforce.h
    ${CMAKE_CURRENT_LIST_DIR}/hnswalg.h
    ${CMAKE_CURRENT_LIST_DIR}/hnswlib.h
    ${CMAKE_CURRENT_LIST_DIR}/space_ip.h
    ${CMAKE_CURRENT_LIST_DIR}/space_l2.h
    ${CMAKE_CURRENT_LIST_DIR}/stop_condition.h
    ${CMAKE_CURRENT_LIST_DIR}/visited_list_pool.h)

valkey_search_create_proto_library("third_party/hnswlib/index.proto"
                                   "index_cc_proto")

add_library(hnswlib_vmsdk INTERFACE ${SRCS_HNSWLIB_VMSDK})
target_include_directories(hnswlib_vmsdk INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(hnswlib_vmsdk INTERFACE iostream)
# Link the compiled SimSIMD dispatch unit (simsimd_c builds c/lib.c exactly
# once and PUBLIC-links the simsimd headers). simsimd.h now includes only
# declarations, so the simsimd_* definitions must come from here -- otherwise
# they'd be emitted into every TU and collide at link time (macOS hard error).
target_link_libraries(hnswlib_vmsdk INTERFACE simsimd_c)
target_link_libraries(hnswlib_vmsdk INTERFACE vmsdklib)
target_compile_definitions(hnswlib_vmsdk
                           INTERFACE VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
                                     USE_SIMSIMD)
target_link_libraries(hnswlib_vmsdk INTERFACE index_cc_proto)

set(SRCS_SIMSIMD ${CMAKE_CURRENT_LIST_DIR}/simsimd.h)

add_library(simsimd INTERFACE ${SRCS_SIMSIMD})
target_include_directories(simsimd INTERFACE ${CMAKE_CURRENT_LIST_DIR})

set(SRCS_IOSTREAM ${CMAKE_CURRENT_LIST_DIR}/iostream.h)

add_library(iostream INTERFACE ${SRCS_IOSTREAM})
target_include_directories(iostream INTERFACE ${CMAKE_CURRENT_LIST_DIR})
