package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//:__subpackages__"],
)

cc_library(
    name = "patricia_tree",
    hdrs = [
        "patricia_tree.h",
    ],
    deps = [
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "segment_tree",
    hdrs = ["segment_tree.h"],
)

cc_library(
    name = "string_interning",
    srcs = ["string_interning.cc"],
    hdrs = ["string_interning.h"],
    deps = [
        ":allocator",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "allocator",
    srcs = ["allocator.cc"],
    hdrs = ["allocator.h"],
    deps = [
        ":intrusive_list",
        ":intrusive_ref_count",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "intrusive_list",
    hdrs = ["intrusive_list.h"],
    deps = [
        "@com_google_absl//absl/log:check",
    ],
)

cc_library(
    name = "lru",
    hdrs = ["lru.h"],
    deps = [
        ":intrusive_list",
        "@com_google_absl//absl/log:check",
    ],
)

cc_library(
    name = "intrusive_ref_count",
    hdrs = ["intrusive_ref_count.h"],
)

cc_library(
    name = "scanner",
    hdrs = ["scanner.h"],
    deps = [
        "//vmsdk/src/valkey_module_api:valkey_module",
    ]
)

