load("@pybind11_bazel//:build_defs.bzl", "pybind_extension", "pybind_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

pybind_library(
    name = "gnn_api",
    srcs = ["gnn_api.cc"],
    hdrs = ["gnn_api.h"],
    deps = [
        "//base/common",
        "//base/hash",
        "//chrono_graph/src/base:graph_base",
        "//chrono_graph/src/client",
    ],
)

pybind_extension(
    name = "libgnn_pybind",
    srcs = ["gnn_pybind.cc"],
    deps = [
        ":gnn_api",
    ],
)
