// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "base/common/basic_types.h"
#include "base/common/string.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/base/proto_helper.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"

namespace chrono_graph {

using IdVec = std::vector<uint64>;
using WeightVec = std::vector<float>;

// Edge kickout strategy when edge list is full.
enum EdgeListReplaceStrategy {
  // kick edge with smallest timestamp
  LRU = 0,
  // kick random edge
  RANDOM = 1,
  // kick edge with min weight
  WEIGHT = 2,
  // no kick, cancel insert
  CANCEL = 3,
};

struct GraphNode {
  uint64 node_id;
  std::string relation_name;
};

// All vectors should have the same length N, each correspond to an edge.
struct BatchInsertInfo {
  std::vector<uint64_t> src_ids;
  std::vector<std::string> src_attrs;
  std::vector<uint64> dest_ids;
  std::vector<float> dest_weights;
  std::vector<uint32_t> dst_ts;
  std::vector<bool> expire_ts_update_flags;
  std::vector<bool> use_item_ts_for_expires;
  std::vector<std::string> dest_attrs;
  int64 timeout_ms = 5000;
  InsertEdgeWeightAct iewa = InsertEdgeWeightAct::UNKNOWN_ACT;
  void Clear() {
    src_ids.clear();
    src_attrs.clear();
    dest_ids.clear();
    dest_weights.clear();
    dst_ts.clear();
    expire_ts_update_flags.clear();
    use_item_ts_for_expires.clear();
    dest_attrs.clear();
    timeout_ms = 5000;
    iewa = InsertEdgeWeightAct::UNKNOWN_ACT;
  }
  std::string ToString() const {
    std::ostringstream oss;
    oss << "src_ids(" << src_ids.size() << ") = [" << absl::StrJoin(src_ids, ",") << "];";
    oss << "src_attrs(" << src_attrs.size() << ");";
    oss << "dst_ids(" << dest_ids.size() << ") = [" << absl::StrJoin(dest_ids, ",") << "];";
    oss << "dst_w(" << dest_weights.size() << ") = [" << absl::StrJoin(dest_weights, ",") << "];";
    oss << "dst_ts(" << dst_ts.size() << ") = [" << absl::StrJoin(dst_ts, ",") << "]";
    oss << "expire_ts_update_flags(" << expire_ts_update_flags.size() << ")";
    oss << "use_item_ts_for_expires(" << use_item_ts_for_expires.size() << ")";
    oss << "dst_attrs(" << dest_attrs.size() << ");";
    oss << "iewa = " << InsertEdgeWeightAct_Name(iewa) << ";";
    return oss.str();
  }
  BatchInsertInfo() {}
  BatchInsertInfo(const BatchInsertInfo &info) {
    src_ids = info.src_ids;
    src_attrs = info.src_attrs;
    dest_ids = info.dest_ids;
    dest_weights = info.dest_weights;
    dst_ts = info.dst_ts;
    expire_ts_update_flags = info.expire_ts_update_flags;
    use_item_ts_for_expires = info.use_item_ts_for_expires;
    dest_attrs = info.dest_attrs;
    timeout_ms = info.timeout_ms;
    iewa = info.iewa;
  }
  BatchInsertInfo(BatchInsertInfo &&info) {
    src_ids = std::move(info.src_ids);
    src_attrs = std::move(info.src_attrs);
    dest_ids = std::move(info.dest_ids);
    dest_weights = std::move(info.dest_weights);
    dest_attrs = std::move(info.dest_attrs);
    dst_ts = info.dst_ts;
    expire_ts_update_flags = std::move(info.expire_ts_update_flags);
    use_item_ts_for_expires = std::move(info.use_item_ts_for_expires);
    timeout_ms = info.timeout_ms;
    iewa = info.iewa;
  }
};

}  // namespace chrono_graph
