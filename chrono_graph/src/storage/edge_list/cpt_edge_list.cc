// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/edge_list/cpt_edge_list.h"
#include "base/common/gflags.h"

ABSL_FLAG(int32, cpt_dynamic_growth_base, 2, "Initial allocated space can store `X` items.");
ABSL_FLAG(double, cpt_dynamic_growth_factor, 2, "Expand factor when length exceeds limit");
/** IMPORTANT NOTE:
 * Weight in each insertion is restricted to (cpt_weight_min, cpt_weight_max).
 * This is because cpt uses float to store accumulated weight.
 * Without this restriction, the weight may overflow and cause treap stucture damaged.
 * e.g., set cpt_weight_max to 10000 and some UnitTest will fail.
 */
ABSL_FLAG(double, cpt_weight_max, 1000, "Insert weight must be smaller than this");
ABSL_FLAG(double, cpt_weight_min, 0.001, "Insert weight must be larger than this");
ABSL_FLAG(int32,
          cpt_edge_buffer_capacity,
          -1,
          "pending edge buffer capacity, -1 means disable buffer update"
          "max capacity limit is 65534(UINT16_MAX - 1)");

namespace chrono_graph {}  // namespace chrono_graph
