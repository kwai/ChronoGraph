// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include "base/common/gflags.h"
#include "chrono_graph/src/base/proto_helper.h"
#include "chrono_graph/src/base/util.h"
#include "chrono_graph/src/client/rgraph_client.h"

ABSL_DECLARE_FLAG(int32, int_block_max_size);
ABSL_DECLARE_FLAG(int32, float_block_max_size);

namespace chrono_graph {

class RGraphClient;
class NodesIterator;

/**
 * \brief Use RGraphClient or NodesIterator to get results.
 */
class GnnClientHelper {
 public:
  struct ClientInfo {
    std::shared_ptr<RGraphClient> rgraph_client;
    std::shared_ptr<NodesIterator> graph_iter;
  };

  struct BatchNodeAttr {
    std::vector<bool> exist;
    std::vector<uint64_t> degrees;
    std::vector<int64> int_attrs;
    std::vector<float> float_attrs;
    std::string info() const {
      std::ostringstream oss;
      oss << "exist = [";
      for (auto i : exist) { oss << i << ","; }
      oss << "]; degrees = [";
      for (auto i : degrees) { oss << i << ","; }
      oss << "]; int_attrs = [";
      for (auto i : int_attrs) { oss << i << ","; }
      oss << "]; float_attrs = [";
      for (auto f : float_attrs) { oss << f << ","; }
      oss << "]";
      return oss.str();
    }
  };

  struct EdgeVec {
    std::vector<uint64> ids;
    std::vector<uint32> timestamps;
    std::vector<double> weights;
    std::string info() const {
      std::ostringstream oss;
      oss << "id = [";
      for (auto i : ids) { oss << i << ","; }
      oss << "]; timestamp = [";
      for (auto i : timestamps) { oss << i << ","; }
      oss << "]; weight = [";
      for (auto i : weights) { oss << i << ","; }
      oss << "]";
      return oss.str();
    }
  };

  /**
   * \brief Initialize a gnn client.
   * \param relation_name: Identify a client.
   * \param config: Json config string, including relation name, service name, and other config.
   * \return true if success.
   */
  bool AddClient(const std::string &relation_name, const std::string &config);

  /**
   * \brief Sample nodes by a gnn client.
   * \param relation_name: Identify a client.
   * \param count: Sample node num.
   * \return vector of sampled IDs.
   */
  std::vector<uint64_t> SampleNodes(const std::string &relation_name,
                                    const int count,
                                    const std::string &pool_name) const;

  GnnClientHelper::EdgeVec SampleNeighbors(const std::string &relation_name,
                                           const std::vector<uint64_t> &source_nodes,
                                           int fanout,
                                           int sample_type,
                                           int padding_type,
                                           bool sampling_without_replacement,
                                           uint32 max_timestamp_s,
                                           uint32 min_timestamp_s,
                                           float min_weight_required) const;

  GnnClientHelper::BatchNodeAttr NodesAttr(const std::string &relation_name,
                                           const std::vector<uint64_t> ids,
                                           int int64_attr_len,
                                           int float_attr_len) const;

  bool BatchInsert(const std::string &relation_name,
                   const BatchInsertInfo &insert_info,
                   bool is_delete) const;

  // Iterate all nodes in a relation, with stateful iterator.
  std::vector<uint64_t> IterateNodes(const std::string &relation_name);

 private:
  // {relation_name, client & iter instance}
  std::unordered_map<std::string, ClientInfo> client_map_;
};

/**
 * \brief Pybind interface.
 */
class GnnPyApi {
 public:
  static bool AddClient(const std::string &client_name, const std::string &config) {
    return helper_.AddClient(client_name, config);
  }

  // This interface can do both insert and deletion.
  static bool BatchInsert(const std::string &client_name,
                          bool is_delete,
                          const std::vector<uint64_t> &src_ids,
                          int int64_attr_len,
                          int float_attr_len,
                          const std::vector<int64_t> &src_int_attrs,
                          const std::vector<float> &src_float_attrs,
                          const std::vector<uint64_t> &dst_ids,
                          const std::vector<float> &dst_weights,
                          const std::vector<int64_t> &dst_timestamps);

  static std::vector<uint64_t> SampleNodes(const std::string &client_name,
                                           const int count,
                                           const std::string &pool_name = "") {
    return helper_.SampleNodes(client_name, count, pool_name);
  }

  static GnnClientHelper::EdgeVec SampleNeighbors(const std::string &client_name,
                                                  const std::vector<uint64_t> &source_nodes,
                                                  int fanout,
                                                  int sample_type,
                                                  int padding_type,
                                                  bool sampling_without_replacement,
                                                  int64_t max_timestamp,
                                                  int64_t min_timestamp,
                                                  float min_weight_required) {
    return helper_.SampleNeighbors(client_name,
                                   source_nodes,
                                   fanout,
                                   sample_type,
                                   padding_type,
                                   sampling_without_replacement,
                                   chrono_graph::ConvertTimestampToSec(max_timestamp),
                                   chrono_graph::ConvertTimestampToSec(min_timestamp),
                                   min_weight_required);
  }

  static GnnClientHelper::BatchNodeAttr NodesAttr(const std::string &client_name,
                                                  const std::vector<uint64_t> ids,
                                                  int int64_attr_len,
                                                  int float_attr_len) {
    return helper_.NodesAttr(client_name, ids, int64_attr_len, float_attr_len);
  }

  static std::vector<uint64_t> IterateNodes(const std::string &client_name) {
    return helper_.IterateNodes(client_name);
  }

  // Transform python string to C++ enum.
  static int SampleTypeByName(const std::string &sample_type);
  static int PaddingTypeByName(const std::string &padding_type);

  static std::vector<uint64_t> StringKeysToIds(const std::vector<std::string> &keys);

 private:
  static GnnClientHelper helper_;
};
}  // namespace chrono_graph
