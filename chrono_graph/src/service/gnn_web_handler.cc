// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/service/gnn_web_handler.h"

#include "base/common/string.h"
#include "chrono_graph/src/storage/graph_kv.h"

namespace chrono_graph {

ParamParser::ParamParser(const std::string &query) {
  size_t start = 0;
  while (start < query.size()) {
    size_t end = query.find('&', start);
    if (end == std::string::npos) { end = query.size(); }

    std::string pair = query.substr(start, end - start);
    size_t eq_pos = pair.find('=');
    if (eq_pos != std::string::npos) {
      std::string key = pair.substr(0, eq_pos);
      std::string value = pair.substr(eq_pos + 1);
      param_map_[key] = value;
    }
    start = end + 1;
  }
}

std::string ParamParser::GetValue(const std::string &key) {
  if (param_map_.count(key)) { return param_map_[key]; }
  return "";
}

std::string GNNInfoHandler::ProcessRequest(const std::string &param_string) {
  return graph_kv_->GetStatInfo();
}

std::string GNNGetHandler::ProcessRequest(const std::string &param_string) {
  std::string ret_str;
  ParamParser parser(param_string);
  auto id_str = parser.GetValue("id");
  if (id_str.empty()) {
    ret_str = "param: id is not set!\n";
    return ret_str;
  }
  uint64 id = 0;
  if (!absl::SimpleAtoi(id_str, &id)) {
    ret_str = "param: id not legal! v = " + id_str + "\n";
    return ret_str;
  }

  auto relation_name = parser.GetValue("relation_name");
  if (relation_name.empty()) {
    ret_str = "param: relation_name is not set!\n";
    return ret_str;
  }

  NodeAttr node_attr;
  ProtoPtrList<EdgeInfo> edge_info;
  graph_kv_->QueryNeighbors(id, {}, relation_name, &node_attr, &edge_info);

  std::ostringstream stream;
  stream << ">>> param = " << param_string << std::endl;
  if (node_attr.id() == 0) {
    stream << "empty get info result, do you request an wrong shard?";
  } else {
    stream << "id = " << node_attr.id() << ", degree = " << node_attr.degree() << ", attr = ["
           << graph_kv_->get_attr_op(relation_name)->DebugInfo(node_attr.attr_info().data()) << "]"
           << std::endl;
    if (edge_info.size() > 0) {
      stream << "linked node size: " << edge_info.size() << " = [" << std::endl;
      for (size_t i = 0; i < edge_info.size(); ++i) {
        auto &edge = edge_info.Get(i);
        std::string t_s = base::TimestampToString(edge.timestamp());
        stream << "  (id = " << edge.id() << ", weight = " << edge.weight() << ", timestamp = " << t_s << "["
               << edge.timestamp() << "], attr = ["
               << graph_kv_->get_edge_attr_op(relation_name)->DebugInfo(edge.edge_attr().data()) << "])"
               << std::endl;
      }
      stream << "]";
    }
  }
  stream << std::endl;

  ret_str = stream.str();
  return ret_str;
}

}  // namespace chrono_graph