load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")

package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//:__subpackages__"],
)

proto_library(
    name = "coordinator_proto",
    srcs = ["coordinator.proto"],
    deps = ["@com_google_protobuf//:any_proto"],
)

cc_proto_library(
    name = "coordinator_cc_proto",
    deps = [":coordinator_proto"],
)

cc_grpc_library(
    name = "coordinator_cc_grpc_proto",
    srcs = [":coordinator_proto"],
    grpc_only = True,
    deps = [":coordinator_cc_proto"],
)

cc_library(
    name = "server",
    srcs = ["server.cc"],
    hdrs = ["server.h"],
    deps = [
        ":coordinator_cc_grpc_proto",
        ":coordinator_cc_proto",
        ":grpc_suspender",
        ":metadata_manager",
        ":search_converter",
        ":util",
        "//src:metrics",
        "//src/indexes:vector_base",
        "//src/query:response_generator",
        "//src/query:search",
        "//vmsdk/src:latency_sampler",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc_security_base",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "client",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    deps = [
        ":coordinator_cc_grpc_proto",
        ":coordinator_cc_proto",
        ":grpc_suspender",
        "//src:metrics",
        "//vmsdk/src:latency_sampler",
        "//vmsdk/src:managed_pointers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc_security_base",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "client_pool",
    hdrs = ["client_pool.h"],
    deps = [
        ":client",
        "//vmsdk/src:managed_pointers",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "util",
    hdrs = ["util.h"],
    deps = [
        ":coordinator_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "metadata_manager",
    srcs = ["metadata_manager.cc"],
    hdrs = ["metadata_manager.h"],
    deps = [
        ":client_pool",
        ":coordinator_cc_proto",
        ":util",
        "//src:rdb_serialization",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src:utils",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_protobuf//:protobuf",
        "@highwayhash",
    ],
)

cc_library(
    name = "search_converter",
    srcs = ["search_converter.cc"],
    hdrs = ["search_converter.h"],
    deps = [
        ":coordinator_cc_proto",
        "//src:index_schema",
        "//src:schema_manager",
        "//src/indexes:index_base",
        "//src/indexes:numeric",
        "//src/indexes:tag",
        "//src/query:predicate_header",
        "//src/query:search",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/status:status_macros",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "grpc_suspender",
    srcs = ["grpc_suspender.cc"],
    hdrs = ["grpc_suspender.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/synchronization",
    ],
)
