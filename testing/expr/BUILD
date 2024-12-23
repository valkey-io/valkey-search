load("//vmsdk:redis_module_cc_test.bzl", "redis_module_cc_test")

package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//testing:__subpackages__"],
)

redis_module_cc_test(
    name = "value_test",
    srcs = ["value_test.cc"],
    deps = [
        "//src/expr:value",
        "@com_google_googletest//:gtest_main",
    ],
)
