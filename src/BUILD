load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("//vmsdk:redis_module_library.bzl", "redis_module_library")

package(
    default_applicable_licenses = ["//:license"],
    default_visibility = [
        "//:__subpackages__",
        "//third_party/managed_redis:__subpackages__",
    ],
)

# To include the symbol table, specific parameters are required:
#    --strip=never --config=online_debug_backtraces
# For a release build, add the following parameter: --config=release
redis_module_library(
    name = "valkeysearch",
    deps = [
        ":module_loader",
        ":server_events",
        "//src/commands",
    ],
)

cc_library(
    name = "module_loader",
    srcs = [
        "module_loader.cc",
    ],
    deps = [
        ":keyspace_event_manager",
        ":valkey_search",
        "//src/commands",
        "//vmsdk/src:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
    ],
)

cc_library(
    name = "acl",
    srcs = [
        "acl.cc",
    ],
    hdrs = [
        "acl.h",
    ],
    deps = [
        "//src:index_schema_cc_proto",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "valkey_search",
    srcs = [
        "valkey_search.cc",
    ],
    hdrs = [
        "valkey_search.h",
    ],
    linkopts = [
        # The Redis module SDK defines many global variables in the header file
        # which violates One Definition Rule (ODR). This causes linker errors
        # if we don't allow multiple definitions.
        "-z muldefs",
    ],
    deps = [
        ":acl",
        ":attribute_data_type",
        ":index_schema",
        ":metrics",
        ":schema_manager",
        ":vector_externalizer",
        "//src/coordinator:client_pool",
        "//src/coordinator:grpc_suspender",
        "//src/coordinator:metadata_manager",
        "//src/coordinator:server",
        "//src/coordinator:util",
        "//src/utils:string_interning",
        "//vmsdk/src:command_parser",
        "//vmsdk/src:latency_sampler",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:memory_allocation",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:utils",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "keyspace_event_manager",
    srcs = ["keyspace_event_manager.cc"],
    hdrs = [
        "keyspace_event_manager.h",
    ],
    deps = [
        ":attribute_data_type",
        ":vector_externalizer",
        "//src/utils:patricia_tree",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src:utils",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "attribute",
    srcs = [],
    hdrs = [
        "attribute.h",
    ],
    deps = [
        ":index_schema_cc_proto",
        "//src/indexes:index_base",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "metrics",
    srcs = [],
    hdrs = ["metrics.h"],
    deps = [
        "//vmsdk/src:latency_sampler",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "index_schema",
    srcs = ["index_schema.cc"],
    hdrs = [
        "index_schema.h",
    ],
    deps = [
        ":attribute",
        ":attribute_data_type",
        ":index_schema_cc_proto",
        ":keyspace_event_manager",
        ":metrics",
        ":rdb_io_stream",
        ":vector_externalizer",
        "//src/indexes:index_base",
        "//src/indexes:numeric",
        "//src/indexes:tag",
        "//src/indexes:vector_base",
        "//src/indexes:vector_flat",
        "//src/indexes:vector_hnsw",
        "//src/utils:string_interning",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:module_type",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:time_sliced_mrmw_mutex",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src:utils",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_prod",
    ],
)

cc_library(
    name = "attribute_data_type",
    srcs = ["attribute_data_type.cc"],
    hdrs = ["attribute_data_type.h"],
    deps = [
        ":index_schema_cc_proto",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "rdb_io_stream",
    hdrs = ["rdb_io_stream.h"],
    deps = [
        "//third_party/hnswlib:iostream",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "schema_manager",
    srcs = ["schema_manager.cc"],
    hdrs = ["schema_manager.h"],
    deps = [
        ":index_schema",
        ":index_schema_cc_proto",
        ":metrics",
        ":rdb_io_stream",
        ":vector_externalizer",
        "//src/coordinator:coordinator_cc_proto",
        "//src/coordinator:metadata_manager",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:utils",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@highwayhash",
    ],
)

cc_library(
    name = "server_events",
    srcs = ["server_events.cc"],
    hdrs = ["server_events.h"],
    deps = [
        ":schema_manager",
        ":valkey_search",
        "//src/coordinator:metadata_manager",
        "//vmsdk/src/valkey_module_api:valkey_module",
    ],
)

cc_library(
    name = "vector_externalizer",
    srcs = ["vector_externalizer.cc"],
    hdrs = ["vector_externalizer.h"],
    deps = [
        ":attribute_data_type",
        ":index_schema_cc_proto",
        "//src/utils:lru",
        "//src/utils:string_interning",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:string_view",
    ],
)

proto_library(
    name = "index_schema_proto",
    srcs = ["index_schema.proto"],
)

cc_proto_library(
    name = "index_schema_cc_proto",
    deps = [":index_schema_proto"],
)
