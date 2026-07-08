# =============================================================================
# libvs_core: Core infrastructure (src/*.cc + src/utils/*.cc)
# Must be defined first — other subdirectory libraries depend on it.
# =============================================================================
set(SRCS_VS_CORE
    # src/ sources
    ${CMAKE_CURRENT_LIST_DIR}/acl.cc
    ${CMAKE_CURRENT_LIST_DIR}/attribute.cc
    ${CMAKE_CURRENT_LIST_DIR}/attribute_data_type.cc
    ${CMAKE_CURRENT_LIST_DIR}/index_schema.cc
    ${CMAKE_CURRENT_LIST_DIR}/keyspace_event_manager.cc
    ${CMAKE_CURRENT_LIST_DIR}/rdb_serialization.cc
    ${CMAKE_CURRENT_LIST_DIR}/schema_manager.cc
    ${CMAKE_CURRENT_LIST_DIR}/server_events.cc
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search.cc
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search_options.cc
    ${CMAKE_CURRENT_LIST_DIR}/vector_externalizer.cc
    # src/utils/ sources (folded in)
    ${CMAKE_CURRENT_LIST_DIR}/utils/allocator.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils/cancel.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils/string_interning.cc)

valkey_search_add_static_library(libvs_core "${SRCS_VS_CORE}")

target_include_directories(libvs_core PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(libvs_core PUBLIC ${CMAKE_CURRENT_LIST_DIR}/utils)

# Proto dependencies
valkey_search_create_proto_library("src/rdb_section.proto"
                                   "rdb_section_cc_proto")

# Third-party dependencies
target_link_libraries(libvs_core PUBLIC iostream)
target_link_libraries(libvs_core PUBLIC highwayhash)

# SDK dependencies
target_link_libraries(libvs_core PUBLIC vmsdklib)
target_link_libraries(libvs_core PUBLIC valkey_module)

# Platform-conditional gRPC (needed by allocator)
if(APPLE)
  target_link_libraries(libvs_core PUBLIC absl::base)
else()
  target_link_libraries(libvs_core PUBLIC ${GRPC_LIB})
endif()

# Subdirectories define their own libraries in dependency order:
#   indexes depends on core
#   query depends on indexes
#   commands depends on query
#   coordinator depends on query
add_subdirectory(indexes)
add_subdirectory(coordinator)
add_subdirectory(query)
add_subdirectory(commands)

# Now wire up proto cross-dependencies that require targets from subdirectories
target_link_libraries(rdb_section_cc_proto PUBLIC index_schema_cc_proto)
target_link_libraries(rdb_section_cc_proto PUBLIC coordinator_cc_proto)

target_link_libraries(libvs_core PUBLIC index_schema_cc_proto)
target_link_libraries(libvs_core PUBLIC rdb_section_cc_proto)
target_link_libraries(libvs_core PUBLIC coordinator_cc_proto)

# =============================================================================
# libsearch: The shared module loaded by Valkey
# =============================================================================
set(SRCS_MODULE_LOADER ${CMAKE_CURRENT_LIST_DIR}/module_loader.cc)

valkey_search_add_shared_library(libsearch ${SRCS_MODULE_LOADER})
target_include_directories(libsearch PUBLIC ${CMAKE_CURRENT_LIST_DIR})

if(UNIX AND NOT APPLE)
  target_link_options(
    libsearch PRIVATE
    "LINKER:--version-script=${CMAKE_SOURCE_DIR}/vmsdk/versionscript.lds")
endif()

# Link the 5 consolidated libraries
target_link_libraries(libsearch PUBLIC libvs_commands)
target_link_libraries(libsearch PUBLIC libvs_coordinator)
target_link_libraries(libsearch PUBLIC vmsdklib)
target_link_libraries(libsearch PUBLIC valkey_module)

# Third-party libs linked with --start/--end group for circular dependency
# resolution at link time
list(APPEND THIRD_PARTY_LIBS coordinator_grpc_proto)
list(APPEND THIRD_PARTY_LIBS coordinator_cc_proto)

if(UNIX AND NOT APPLE)
  target_link_options(libsearch PRIVATE -Wl,--start-group)
endif()
target_link_libraries(libsearch PRIVATE ${THIRD_PARTY_LIBS})

# Remove the "lib" prefix from the module
set_target_properties(libsearch PROPERTIES PREFIX "")

if(UNIX AND NOT APPLE)
  target_link_options(libsearch PRIVATE -Wl,--allow-multiple-definition)
endif()

if(UNIX AND NOT APPLE)
  # Append -Wl,--end-group as the LAST argument
  target_link_libraries(libsearch PRIVATE lib_to_add_end_group_flag)
endif()
