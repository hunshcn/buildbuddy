load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "nix",
    srcs = ["nix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/nix",
    deps = [
        "//enterprise/server/util/oci",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
    ],
)
