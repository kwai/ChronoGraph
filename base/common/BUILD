cc_library(name = "common",
           srcs = glob(["sleep.cc", "file_util.cc", "logging.cc", "time.cc"]),
           hdrs = glob(["basic_types.h", "file_util.h", "gflags.h", "logging.h", "sleep.h", "string.h", "time.h", "timer.h"]),
           deps = [
                "@abseil-cpp//absl/flags:flag",
                "@abseil-cpp//absl/log",
                "@abseil-cpp//absl/log:check",
                "@abseil-cpp//absl/log:initialize",
                "@abseil-cpp//absl/strings",
                "@abseil-cpp//absl/time",],
           visibility = ["//visibility:public"],
           )
