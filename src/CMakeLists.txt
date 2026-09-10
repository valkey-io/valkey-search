add_subdirectory(coordinator)
add_subdirectory(commands)
add_subdirectory(indexes)
add_subdirectory(expr)
add_subdirectory(query)
add_subdirectory(utils)

# The module's C allocator entry points are a direct object of the shared
# library rather than a member of vmsdklib. Two reasons: a static archive member
# is only pulled in to satisfy an existing undefined reference, which makes
# whether malloc gets defined depend on link order; and vmsdklib is also linked
# into the unit test executables, where defining malloc would override the
# allocator for the whole test process, including libc's own startup
# allocations. Tests therefore run entirely on the system allocator.
set(SRCS_MODULE_LOADER
    ${CMAKE_CURRENT_LIST_DIR}/module_loader.cc
    ${CMAKE_SOURCE_DIR}/vmsdk/src/memory_allocation_c_api.cc)

# This is the module target
valkey_search_add_shared_library(libsearch "${SRCS_MODULE_LOADER}")
target_include_directories(libsearch PUBLIC ${CMAKE_CURRENT_LIST_DIR})
if(UNIX AND NOT APPLE)
  target_link_options(
    libsearch PRIVATE
    "LINKER:--version-script=${CMAKE_SOURCE_DIR}/vmsdk/versionscript.lds")
  set_property(
    TARGET libsearch APPEND PROPERTY LINK_DEPENDS
    ${CMAKE_SOURCE_DIR}/vmsdk/versionscript.lds)
endif()

# Defer the module's C++ static initializers until ValkeyModule_Alloc has been
# established, so that they allocate from Valkey rather than the system
# allocator. See vmsdk/deferred_init.lds and vmsdk/src/deferred_init.cc.
#
# Sanitizer builds are excluded: the sanitizer emits its own constructor into
# .init_array to register global redzones, and that must run at dlopen(). They
# do not use the Valkey allocator at all, so there is nothing to defer for.
# macOS is excluded because Mach-O has no linker scripts; it is a build-only
# target and likewise does not use the Valkey allocator.
# Link libstdc++ into the module rather than using libstdc++.so.
#
# REQUIRED for correctness, not an optimization. The module replaces global
# operator new/delete so that C++ allocation goes to ValkeyModule_Alloc, but a
# replacement only covers code linked into this .so -- the module is dlopen'd
# with local scope and versionscript.lds hides _Znwm/_ZdlPv, so libstdc++.so
# keeps using libc malloc. Any heap object allocated inside libstdc++.so and
# destroyed here would then be passed to ValkeyModule_Free, which is a jemalloc
# free of a non-jemalloc pointer -- an immediate crash.
#
# That is not hypothetical: std::getline lives in libstdc++.so and grows the
# caller's std::string with libc malloc. vmsdk::helper::ParseCPUInfo, reached
# from a static initializer, does exactly this and segfaults at module load
# without this flag. It only appeared once the allocator switch moved to the top
# of ValkeyModule_OnLoad; previously these paths ran before the switch.
#
# Linking libstdc++ statically puts that code inside the module, where the
# operator new/delete replacements apply, and leaves the module with zero
# undefined GLIBCXX symbols -- so no C++ heap object can cross a DSO boundary.
# ci/check_module_allocators.sh enforces that.
if(UNIX AND NOT APPLE AND NOT SAN_BUILD)
  target_link_options(libsearch PRIVATE -static-libstdc++)

  # ...and hide everything that came from a static archive, libstdc++.a
  # included. Static linking alone is not enough, because versionscript.lds is
  # "global: *" and so exports the statically linked libstdc++ symbols -- among
  # them the locale facet ids, such as std::num_put<char>::id.
  #
  # Those ids are STB_GNU_UNIQUE ("u" in nm output), which the dynamic linker
  # unifies to a single instance process-wide, RTLD_LOCAL notwithstanding. As
  # soon as any other module that links libstdc++.so.6 dynamically is loaded --
  # valkey-json does, and the integration tests load it before search -- our
  # facet id object and libstdc++.so.6's become the same object. std::locale::
  # id::_M_id() then hands us an index into that library's facet numbering while
  # our own facet array is a separate one, and the first ostream insertion
  # dereferences the wrong facet and segfaults. It reproduces as a crash inside
  # std::ostream::_M_insert<unsigned long> during module load.
  #
  # Hiding the archive symbols keeps them out of .dynsym, so they are no longer
  # unique and each library keeps a self-consistent set of facet ids. Only
  # ValkeyModule_OnLoad/OnUnload need to be exported, and they come from
  # module_loader.cc.o rather than an archive, so they are unaffected. The
  # module consumes JSON's shared API through ValkeyModule_GetSharedAPI (a
  # function pointer from the server) and exports none of its own, so nothing
  # depends on these symbols being visible.
  #
  # ci/check_module_allocators.sh enforces this.
  target_link_options(libsearch PRIVATE "LINKER:--exclude-libs,ALL")
endif()

if(UNIX AND NOT APPLE AND NOT SAN_BUILD)
  target_link_options(
    libsearch PRIVATE
    "LINKER:-T,${CMAKE_SOURCE_DIR}/vmsdk/deferred_init.lds")
  set_property(
    TARGET libsearch APPEND PROPERTY LINK_DEPENDS
    ${CMAKE_SOURCE_DIR}/vmsdk/deferred_init.lds)

  add_custom_command(
    TARGET libsearch
    POST_BUILD
    COMMAND ${CMAKE_SOURCE_DIR}/ci/check_module_allocators.sh
            $<TARGET_FILE:libsearch> ${CMAKE_BINARY_DIR}
    COMMENT "Verifying static-init deferral and allocator usage")
endif()

target_link_libraries(libsearch PUBLIC keyspace_event_manager)
target_link_libraries(libsearch PUBLIC valkey_search)
target_link_libraries(libsearch PUBLIC commands)
target_link_libraries(libsearch PUBLIC vmsdklib)
target_link_libraries(libsearch PUBLIC valkey_module)
target_link_libraries(libsearch PUBLIC server_events)
target_link_libraries(libsearch PUBLIC expr)

list(APPEND THIRD_PARTY_LIBS coordinator_grpc_proto)
list(APPEND THIRD_PARTY_LIBS coordinator_cc_proto)

valkey_search_create_proto_library("src/rdb_section.proto"
                                   "rdb_section_cc_proto")
target_link_libraries(rdb_section_cc_proto PUBLIC index_schema_cc_proto)
target_link_libraries(rdb_section_cc_proto PUBLIC coordinator_cc_proto)

if(UNIX AND NOT APPLE)
  # Wrap ${THIRD_PARTY_LIBS} with "--start/--end group"
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

set(SRCS_VALKEY_SEARCH
    ${CMAKE_CURRENT_LIST_DIR}/attribute.cc
    ${CMAKE_CURRENT_LIST_DIR}/attribute.h
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search.cc
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search.h
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search_options.cc
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search_options.h)

valkey_search_add_static_library(valkey_search "${SRCS_VALKEY_SEARCH}")
target_include_directories(valkey_search PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(valkey_search PUBLIC attribute_data_type)
target_link_libraries(valkey_search PUBLIC index_schema)
target_link_libraries(valkey_search PUBLIC metrics)
target_link_libraries(valkey_search PUBLIC schema_manager)
target_link_libraries(valkey_search PUBLIC vector_registry)
target_link_libraries(valkey_search PUBLIC client_pool)
target_link_libraries(valkey_search PUBLIC grpc_suspender)
target_link_libraries(valkey_search PUBLIC metadata_manager)
target_link_libraries(valkey_search PUBLIC server)
target_link_libraries(valkey_search PUBLIC util)
target_link_libraries(valkey_search PUBLIC string_interning)
target_link_libraries(valkey_search PUBLIC vmsdklib)
target_link_libraries(valkey_search PUBLIC valkey_module)
target_link_libraries(valkey_search PUBLIC acl)

set(SRCS_KEYSPACE_EVENT_MANAGER
    ${CMAKE_CURRENT_LIST_DIR}/keyspace_event_manager.cc
    ${CMAKE_CURRENT_LIST_DIR}/keyspace_event_manager.h)

valkey_search_add_static_library(keyspace_event_manager
                                 "${SRCS_KEYSPACE_EVENT_MANAGER}")
target_include_directories(keyspace_event_manager
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(keyspace_event_manager PUBLIC attribute_data_type)
target_link_libraries(keyspace_event_manager PUBLIC patricia_tree)
target_link_libraries(keyspace_event_manager PUBLIC vmsdklib)
target_link_libraries(keyspace_event_manager PUBLIC valkey_module)

set(SRCS_ATTRIBUTE ${CMAKE_CURRENT_LIST_DIR}/attribute.h)

add_library(attribute INTERFACE ${SRCS_ATTRIBUTE})
target_include_directories(attribute INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(attribute INTERFACE index_schema_cc_proto)
target_link_libraries(attribute INTERFACE index_base)
target_link_libraries(attribute INTERFACE vmsdklib)
target_link_libraries(attribute INTERFACE valkey_module)

set(SRCS_METRICS ${CMAKE_CURRENT_LIST_DIR}/metrics.h)

add_library(metrics INTERFACE ${SRCS_METRICS})
target_include_directories(metrics INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(metrics INTERFACE vmsdklib)

set(SRCS_INDEX_SCHEMA ${CMAKE_CURRENT_LIST_DIR}/index_schema.cc
                      ${CMAKE_CURRENT_LIST_DIR}/index_schema.h)

valkey_search_add_static_library(index_schema "${SRCS_INDEX_SCHEMA}")
target_include_directories(index_schema PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(index_schema PUBLIC attribute)
target_link_libraries(index_schema PUBLIC attribute_data_type)
target_link_libraries(index_schema PUBLIC vmsdklib)
target_link_libraries(index_schema PUBLIC index_schema_cc_proto)
target_link_libraries(index_schema PUBLIC keyspace_event_manager)
target_link_libraries(index_schema PUBLIC metrics)
target_link_libraries(index_schema PUBLIC rdb_serialization)
target_link_libraries(index_schema PUBLIC index_base)
target_link_libraries(index_schema PUBLIC numeric)
target_link_libraries(index_schema PUBLIC tag)
target_link_libraries(index_schema PUBLIC text)

target_link_libraries(index_schema PUBLIC vector_base)
target_link_libraries(index_schema PUBLIC vector_flat)
target_link_libraries(index_schema PUBLIC vector_hnsw)
target_link_libraries(index_schema PUBLIC string_interning)
target_link_libraries(index_schema PUBLIC valkey_module)

set(SRCS_ATTRIBUTE_DATA_TYPE ${CMAKE_CURRENT_LIST_DIR}/attribute_data_type.cc
                             ${CMAKE_CURRENT_LIST_DIR}/attribute_data_type.h)

valkey_search_add_static_library(attribute_data_type
                                 "${SRCS_ATTRIBUTE_DATA_TYPE}")
target_include_directories(attribute_data_type PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(attribute_data_type PUBLIC index_schema_cc_proto)
target_link_libraries(attribute_data_type PUBLIC valkey_module)
target_link_libraries(attribute_data_type PUBLIC vmsdklib)

set(SRCS_RDB_SERIALIZATION ${CMAKE_CURRENT_LIST_DIR}/rdb_serialization.cc
                           ${CMAKE_CURRENT_LIST_DIR}/rdb_serialization.h)

valkey_search_add_static_library(rdb_serialization ${SRCS_RDB_SERIALIZATION})
target_include_directories(rdb_serialization PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(rdb_serialization PUBLIC iostream)
target_link_libraries(rdb_serialization PUBLIC valkey_module)
target_link_libraries(rdb_serialization PUBLIC vmsdklib)
target_link_libraries(rdb_serialization PUBLIC rdb_section_cc_proto)

set(SRCS_SCHEMA_MANAGER ${CMAKE_CURRENT_LIST_DIR}/schema_manager.cc
                        ${CMAKE_CURRENT_LIST_DIR}/schema_manager.h)

valkey_search_add_static_library(schema_manager "${SRCS_SCHEMA_MANAGER}")
target_include_directories(schema_manager PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(schema_manager PUBLIC vmsdklib)
target_link_libraries(schema_manager PUBLIC index_schema)
target_link_libraries(schema_manager PUBLIC index_schema_cc_proto)
target_link_libraries(schema_manager PUBLIC metrics)
target_link_libraries(schema_manager PUBLIC rdb_serialization)
target_link_libraries(schema_manager PUBLIC coordinator_cc_proto)
target_link_libraries(schema_manager PUBLIC metadata_manager)
target_link_libraries(schema_manager PUBLIC valkey_module)

set(SRCS_SERVER_EVENTS ${CMAKE_CURRENT_LIST_DIR}/server_events.cc
                       ${CMAKE_CURRENT_LIST_DIR}/server_events.h)

valkey_search_add_static_library(server_events "${SRCS_SERVER_EVENTS}")
target_include_directories(server_events PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(server_events PUBLIC schema_manager)
target_link_libraries(server_events PUBLIC valkey_search)
target_link_libraries(server_events PUBLIC metadata_manager)
target_link_libraries(server_events PUBLIC valkey_module)

set(SRCS_vector_registry ${CMAKE_CURRENT_LIST_DIR}/vector_registry.cc
                               ${CMAKE_CURRENT_LIST_DIR}/vector_registry.h)
valkey_search_add_static_library(vector_registry
                                 "${SRCS_vector_registry}")
target_include_directories(vector_registry PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_registry PUBLIC string_interning)
target_link_libraries(vector_registry PUBLIC vector_base)
target_link_libraries(vector_registry PUBLIC vmsdklib)

set(SRCS_ACL ${CMAKE_CURRENT_LIST_DIR}/acl.cc ${CMAKE_CURRENT_LIST_DIR}/acl.h)
valkey_search_add_static_library(acl "${SRCS_ACL}")
target_include_directories(acl PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(acl PUBLIC index_schema_cc_proto)
