// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "base/perf/perf_wrapper.h"

// monitor namespace
#define M_NS "chrono_graph"

#define PERF_SUM(count, relation_name, ...) \
  base::PerfWrapper::CountLogStash(count, M_NS, chrono_graph::global_exp_name, relation_name, __VA_ARGS__);

#define PERF_AVG(count, relation_name, ...) \
  base::PerfWrapper::IntervalLogStash(count, M_NS, chrono_graph::global_exp_name, relation_name, __VA_ARGS__);

#define GLOBAL_RELATION "global"
#define P_RQ "request"
#define P_RQ_T "request.time"
#define P_CKPT "checkpoint"
#define P_EL "edge_list"
#define P_STORAGE "storage"
#define P_EXCEPTION "exception"
