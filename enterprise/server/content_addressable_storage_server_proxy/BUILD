load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "content_addressable_storage_server_proxy",
    srcs = ["content_addressable_storage_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/content_addressable_storage_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/util/status",
        "@org_golang_google_grpc//codes",
    ],
)

go_test(
    name = "content_addressable_storage_server_proxy_test",
    size = "small",
    srcs = ["content_addressable_storage_server_proxy_test.go"],
    embed = [":content_addressable_storage_server_proxy"],
    deps = [
        "//server/remote_cache/content_addressable_storage_server",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
