load("@rules_proto//proto:defs.bzl", "proto_library")
load("@grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

package(default_visibility = ["//visibility:public"])

proto_library(
  name = "gnn_kv_service_proto",
  srcs = ["gnn_kv_service.proto"],
)

cc_proto_library(
  name = "gnn_kv_service_cc_proto",
  deps = [":gnn_kv_service_proto"],
)

cc_grpc_library(
  name = "gnn_kv_service_cc_grpc",
  srcs = [":gnn_kv_service_proto"],
  grpc_only = True,
  deps = [":gnn_kv_service_cc_proto"],
)
