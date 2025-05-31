package(
    default_applicable_licenses = ["//:license"],
    default_visibility = [
        "//:__subpackages__",
    ],
)

cc_library(
    name = "value",
    srcs = ["value.cc"],
    hdrs = [
        "value.h",
    ],
    deps = [
        "//src/utils:scanner",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/container:inlined_vector",
    ],
)

cc_library(
    name = "expr",
    srcs = ["expr.cc"],
    hdrs = [
        "expr.h",
    ],
    deps = [
        ":value",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "//vmsdk/src/status:status_macros",
        "//src/utils:scanner",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)


