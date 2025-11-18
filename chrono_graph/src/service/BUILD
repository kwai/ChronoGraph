cc_binary(name = "gnn_storage_service",
          srcs = glob(["*.cc", "*.h"]),
          deps = [
             "//base/common",
             "//base/jansson",
             "//base/util",
             "//chrono_graph/src/base:graph_base",
             "//chrono_graph/src/proto:gnn_kv_service_cc_grpc",
             "//chrono_graph/src/storage:gnn_storage",
             "@abseil-cpp//absl/flags:parse",
             "@boost.asio",
             "@grpc//:grpc++",
             "@grpc//:grpc++_reflection",
          ],
          copts = [
              "-DOPENSSL_NO_ASM",
              "-DBORINGSSL_DISABLE_AVX512",
          ],
          )
