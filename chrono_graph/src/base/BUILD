cc_library(name = "graph_base",
           srcs = glob(["*.cc",],
              exclude = [
                "*_test.cc",
              ]
           ),
           hdrs = glob(["*.h",]),
           deps = [
             "//base/common",
             "//base/random",
             "//base/util",
             "//chrono_graph/src/proto:gnn_kv_service_cc_proto",
           ],
           visibility = ["//visibility:public"],
           )


cc_test(name = "block_storage_test",
        srcs = ["block_storage_test.cc"],
        deps = [
          ":graph_base",
          "//base/jansson",
          "//base/testing",
        ],
        )

cc_test(name = "config_helper_test",
        srcs = ["config_helper_test.cc"],
        deps = [
          ":graph_base",
          "//base/jansson",
          "//base/testing",
        ],
        )

cc_test(name = "util_test",
        srcs = ["util_test.cc"],
        deps = [
          ":graph_base",
          "//base/testing",
        ])
