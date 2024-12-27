load("//vmsdk:redis_module_cc_test.bzl", "redis_module_cc_test")

package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//testing:__subpackages__"],
)

redis_module_cc_test(
    name = "allocator_test",
    srcs = ["allocator_test.cc"],
    deps = [
        ":intrusive_list_test",
        "//src/utils:allocator",
        "//src/utils:intrusive_ref_count",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "intrusive_list_test",
    srcs = ["intrusive_list_test.cc"],
    deps = [
        "//src/utils:intrusive_list",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "intrusive_ref_count_test",
    srcs = ["intrusive_ref_count_test.cc"],
    deps = [
        "//src/utils:intrusive_ref_count",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "lru_test",
    srcs = ["lru_test.cc"],
    deps = [
        "//src/utils:lru",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "patricia_tree_test",
    srcs = ["patricia_tree_test.cc"],
    deps = [
        "//src/utils:patricia_tree",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "segment_tree_test",
    srcs = ["segment_tree_test.cc"],
    deps = [
        "//src/utils:segment_tree",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "string_interning_test",
    srcs = ["string_interning_test.cc"],
    deps = [
        "//src/utils:allocator",
        "//src/utils:intrusive_ref_count",
        "//src/utils:string_interning",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_googletest//:gtest_main",
    ],
)

redis_module_cc_test(
    name = "scanner_test",
    srcs = ["scanner_test.cc"],
    deps = [
        "//src/utils:scanner",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_googletest//:gtest_main",
    ],
)
