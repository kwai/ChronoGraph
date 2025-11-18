cc_library(name = "gnn_storage",
           srcs = glob([
                "*.cc",
                "edge_list/*.cc",
                "monitor/*.cc",
                "persistence/*.cc",
                "sample/*.cc",
              ],
              exclude = [
                "*_test.cc",
                "edge_list/*_test.cc",
                "persistence/*_test.cc",
                "sample/*_test.cc",
              ],
           ),
           hdrs = glob([
                "*.h",
                "edge_list/*.h",
                "monitor/*.h",
                "persistence/*.h",
                "sample/*.h",
              ],
           ),
           deps = [
             "//base/common",
             "//base/jansson",
             "//base/random",
             "//base/thread",
             "//base/util",
             "//chrono_graph/src/base:graph_base",
             "//chrono_graph/src/proto:gnn_kv_service_cc_proto",
             "//memtable:memtable",
           ],
           visibility = ["//chrono_graph/src/service:__pkg__"],
           )

cc_test(name = "graph_kv_test",
        srcs = ["graph_kv_test.cc"],
        deps = [
          ":gnn_storage",
          "//base/common",
          "//base/testing",
        ],
        )

cc_test(name = "sample_cache_test",
        srcs = ["sample/hot_sample_cache_test.cc"],
        deps = [
          ":gnn_storage",
          "//base/common",
          "//base/jansson",
          "//base/testing",
        ],
        )

cc_test(name = "cdf_edge_list_test",
        srcs = ["edge_list/cdf_edge_list_test.cc"],
        deps = [
          ":gnn_storage",
          "//base/common",
          "//base/testing",
        ],
        )

cc_test(name = "cpt_edge_list_test",
        srcs = ["edge_list/cpt_edge_list_test.cc",
        ],
        deps = [
          ":gnn_storage",
          "//base/common",
          "//base/jansson",
          "//base/testing",
        ],
        )
