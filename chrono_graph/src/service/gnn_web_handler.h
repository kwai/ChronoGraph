// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace chrono_graph {

class GraphKV;

class ParamParser {
 public:
  explicit ParamParser(const std::string &query);
  std::string GetValue(const std::string &key);

 private:
  std::unordered_map<std::string, std::string> param_map_;
};

class GNNInfoHandler {
 public:
  explicit GNNInfoHandler(GraphKV *graph_kv) : graph_kv_(graph_kv) {}

  std::string ProcessRequest(const std::string &param_string);

 private:
  GraphKV *graph_kv_;
};

class GNNGetHandler {
 public:
  explicit GNNGetHandler(GraphKV *graph_kv) : graph_kv_(graph_kv) {}

  std::string ProcessRequest(const std::string &param_string);

 private:
  GraphKV *graph_kv_;
};

}  // namespace chrono_graph
