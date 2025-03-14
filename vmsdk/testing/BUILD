package(
    default_applicable_licenses = ["//vmsdk:license"],
    default_visibility = ["//visibility:public"],
)

cc_test(
    name = "command_parser_test",
    srcs = ["command_parser_test.cc"],
    deps = [
        "//vmsdk/src:command_parser",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "concurrency_test",
    srcs = ["concurrency_test.cc"],
    deps = [
        "//vmsdk/src:concurrency",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "module_type_test",
    srcs = ["module_type_test.cc"],
    deps = [
        "//vmsdk/src:module_type",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "thread_pool_test",
    srcs = ["thread_pool_test.cc"],
    deps = [
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:utils",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "time_sliced_mrmw_mutex_test",
    srcs = ["mrmw_mutex_test.cc"],
    deps = [
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:time_sliced_mrmw_mutex",
        "//vmsdk/src:utils",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "status_macros_test",
    srcs = ["status_macros_test.cc"],
    deps = [
        "//vmsdk/src/status:status_macros",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "memory_allocation_test",
    srcs = ["memory_allocation_test.cc"],
    deps = [
        "//vmsdk/src:memory_allocation",
        "//vmsdk/src:memory_allocation_overrides",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "utils_test",
    srcs = ["utils_test.cc"],
    deps = [
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:utils",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "log_test",
    srcs = ["log_test.cc"],
    deps = [
        "//vmsdk/src:log",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/log:log_entry",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)
