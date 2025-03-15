# Hnswlib - fast approximate nearest neighbor search: https://github.com/nmslib/hnswlib

load("@rules_license//rules:license.bzl", "license")

package(
    default_applicable_licenses = [":license"],
    default_visibility = ["//visibility:public"],
)

license(name = "license")

licenses(["notice"])

exports_files(["LICENSE"])

cc_library(
    name = "hnswlib_vmsdk",
    defines = [
        "VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES",
        "USE_SIMSIMD",
    ],
    textual_hdrs = [
        "bruteforce.h",
        "hnswalg.h",
        "hnswlib.h",
        "space_ip.h",
        "space_l2.h",
        "stop_condition.h",
        "visited_list_pool.h",
    ],
    deps = [
        ":iostream",
        ":simsimd",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "//vmsdk/src:memory_allocation_overrides",
        "//vmsdk/src/status:status_macros",
        ":index_cc_proto"
    ],
)

cc_library(
    name = "simsimd",
    textual_hdrs = ["simsimd.h"],
    deps = [
        "//third_party/simsimd/c:lib",
        "//third_party/simsimd/include/simsimd",
    ],
    copts = [
        "-Wno-unused-function",
        "-Wno-unknown-pragmas",
    ],
)

cc_library(
    name = "iostream",
    hdrs = ["iostream.h"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

proto_library(
    name = "index_proto",
    srcs = ["index.proto"],
)

cc_proto_library(
    name = "index_cc_proto",
    deps = [":index_proto"],
)