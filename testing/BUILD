load("//vmsdk:redis_module_cc_test.bzl", "redis_module_cc_test")

package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//testing:__subpackages__"],
)

redis_module_cc_test(
    name = "valkey_search_test",
    srcs = ["valkey_search_test.cc"],
    tags = [
        "no_opt_test",
    ],
    deps = [
        ":common",
        "//src:attribute_data_type",
        "//src:index_schema",
        "//src:metrics",
        "//src:schema_manager",
        "//src:valkey_search",
        "//src/coordinator:metadata_manager",
        "//src/utils:string_interning",
        "//testing/coordinator:common",
        "//vmsdk/src:module",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "acl_test",
    srcs = ["acl_test.cc"],
    deps = [
        ":common",
        "//src:acl",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "ft_create_parser_test",
    srcs = ["ft_create_parser_test.cc"],
    deps = [
        "//src:index_schema_cc_proto",
        "//src/commands:ft_create_parser",
        "//src/indexes:index_base",
        "//vmsdk/src:log",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "ft_search_parser_test",
    srcs = ["ft_search_parser_test.cc"],
    deps = [
        ":common",
        "//src:index_schema_cc_proto",
        "//src:schema_manager",
        "//src/commands:ft_search_parser",
        "//src/indexes:vector_flat",
        "//src/query:search",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "ft_search_test",
    srcs = ["ft_search_test.cc"],
    deps = [
        ":common",
        "//src:schema_manager",
        "//src:valkey_search",
        "//src:vector_externalizer",
        "//src/commands",
        "//src/coordinator:client",
        "//src/coordinator:coordinator_cc_proto",
        "//src/coordinator:util",
        "//src/indexes:vector_base",
        "//src/query:search",
        "//src/utils:string_interning",
        "//testing/coordinator:common",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:module",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_main",
        "@com_googlesource_code_re2//:re2",
    ],
)

redis_module_cc_test(
    name = "keyspace_event_manager_test",
    srcs = ["keyspace_event_manager_test.cc"],
    deps = [
        ":common",
        "//src:keyspace_event_manager",
        "//src/indexes:index_base",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:optional",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "index_schema_test",
    srcs = ["index_schema_test.cc"],
    deps = [
        ":common",
        "//src:attribute_data_type",
        "//src:index_schema",
        "//src:index_schema_cc_proto",
        "//src:keyspace_event_manager",
        "//src:schema_manager",
        "//src/indexes:index_base",
        "//src/indexes:vector_flat",
        "//src/indexes:vector_hnsw",
        "//src/utils:string_interning",
        "//third_party/hnswlib:hnswlib_vmsdk",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:optional",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "ft_dropindex_test",
    srcs = ["ft_dropindex_test.cc"],
    deps = [
        ":common",
        "//src:index_schema",
        "//src:keyspace_event_manager",
        "//src:schema_manager",
        "//src/commands",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "common",
    testonly = True,
    srcs = ["common.cc"],
    hdrs = ["common.h"],
    deps = [
        "//src:acl",
        "//src:attribute_data_type",
        "//src:index_schema",
        "//src:keyspace_event_manager",
        "//src:rdb_serialization",
        "//src:schema_manager",
        "//src:server_events",
        "//src:valkey_search",
        "//src:vector_externalizer",
        "//src/coordinator:client_pool",
        "//src/coordinator:metadata_manager",
        "//src/indexes:index_base",
        "//src/indexes:vector_base",
        "//src/indexes:vector_hnsw",
        "//src/query:search",
        "//src/utils:string_interning",
        "//third_party/hnswlib:iostream",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest",
    ],
)

redis_module_cc_test(
    name = "vector_test",
    srcs = ["vector_test.cc"],
    deps = [
        ":common",
        "//src:attribute_data_type",
        "//src:index_schema_cc_proto",
        "//src/indexes:index_base",
        "//src/indexes:vector_base",
        "//src/indexes:vector_flat",
        "//src/indexes:vector_hnsw",
        "//src/utils:string_interning",
        "//third_party/hnswlib:hnswlib_vmsdk",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "ft_list_test",
    srcs = ["ft_list_test.cc"],
    deps = [
        ":common",
        "//src:index_schema_cc_proto",
        "//src:schema_manager",
        "//src/commands",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

redis_module_cc_test(
    name = "ft_info_test",
    srcs = ["ft_info_test.cc"],
    deps = [
        ":common",
        "//src:index_schema_cc_proto",
        "//src:schema_manager",
        "//src/commands",
        "//vmsdk/src:module",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

redis_module_cc_test(
    name = "ft_create_test",
    srcs = ["ft_create_test.cc"],
    deps = [
        ":common",
        "//src:schema_manager",
        "//src/commands",
        "//src/indexes:index_base",
        "//vmsdk/src:module",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "rdb_serialization_test",
    srcs = ["rdb_serialization_test.cc"],
    deps = [
        ":common",
        "//src:rdb_serialization",
        "//third_party/hnswlib:iostream",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "filter_test",
    srcs = ["filter_test.cc"],
    deps = [
        ":common",
        "//src/commands:filter_parser",
        "//src/indexes:numeric",
        "//src/indexes:tag",
        "//src/indexes:vector_base",
        "//src/query:predicate",
        "//src/utils:string_interning",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "numeric_index_test",
    srcs = ["numeric_index_test.cc"],
    deps = [
        ":common",
        "//src/indexes:index_base",
        "//src/indexes:numeric",
        "//src/query:predicate",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "tag_index_test",
    srcs = ["tag_index_test.cc"],
    deps = [
        ":common",
        "//src/indexes:index_base",
        "//src/indexes:tag",
        "//src/query:predicate",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "search_test",
    srcs = ["search_test.cc"],
    deps = [
        ":common",
        "//src:attribute_data_type",
        "//src:index_schema_cc_proto",
        "//src/commands:filter_parser",
        "//src/indexes:index_base",
        "//src/indexes:numeric",
        "//src/indexes:tag",
        "//src/indexes:vector_base",
        "//src/indexes:vector_flat",
        "//src/indexes:vector_hnsw",
        "//src/query:predicate",
        "//src/query:search",
        "//src/utils:patricia_tree",
        "//src/utils:string_interning",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "schema_manager_test",
    srcs = ["schema_manager_test.cc"],
    deps = [
        ":common",
        "//src:schema_manager",
        "//src/coordinator:metadata_manager",
        "//testing/coordinator:common",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

redis_module_cc_test(
    name = "server_events_test",
    srcs = ["server_events_test.cc"],
    deps = [
        ":common",
        "//src:server_events",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "attribute_data_type_test",
    srcs = ["attribute_data_type_test.cc"],
    deps = [
        ":common",
        "//src:attribute_data_type",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "multi_exec_test",
    srcs = ["multi_exec_test.cc"],
    deps = [
        ":common",
        "//src:valkey_search",
        "//src/commands",
        "//src/utils:string_interning",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "vector_externalizer_test",
    srcs = ["vector_externalizer_test.cc"],
    deps = [
        ":common",
        "//src:vector_externalizer",
        "//src/indexes:vector_base",
        "//src/utils:allocator",
        "//src/utils:intrusive_ref_count",
        "//src/utils:string_interning",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)
