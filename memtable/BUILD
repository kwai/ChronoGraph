cc_library(name = "memtable",
  srcs = glob(["util/*.cc", "pool/*.cc", "hash_map/*.cc", "slab/*.cc", "mem_kv.cc", "mem_kv-inl.h"]),
  hdrs = glob(["util/*.h", "pool/*.h", "hash_map/*.h", "slab/*.h", "mem_kv.h"]),
  deps = [
    "//base/common",
    "//base/hash",
    "//base/perf",
    "//base/random",
    "//base/thread",
  ],
  visibility = ["//visibility:public"],
)

cc_test(name = "memtable_test",
  srcs = [
    "test/loop_hash_map_test.cc",
    "test/slab_mempool32_test.cc",
    "test/mem_kv_test.cc",
  ],
  deps = [
    ":memtable",
    "//base/random",
    "//base/testing",
  ],
)
