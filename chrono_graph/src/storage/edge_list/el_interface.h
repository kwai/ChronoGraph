// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "base/common/basic_types.h"
#include "base/common/string.h"
#include "memtable/mem_kv.h"
#include "chrono_graph/src/base/block_storage_api.h"
#include "chrono_graph/src/base/gnn_type.h"
#include "chrono_graph/src/base/proto_helper.h"
#include "chrono_graph/src/storage/config.h"

namespace chrono_graph {

class MemAllocator;

#pragma pack(push, 1)

// Metadata for edge list.
struct EdgeListMeta {
  uint64_t out_degree;
  uint32 attr_size;
  uint32 capacity;
  uint32 edge_attr_size;
  // When edge list is expired due to size expand, we will move the edge list to another address.
  bool expand_mark;

  void Initialize(BlockStorageApi *attr_op, BlockStorageApi *edge_attr_op, int cap) {
    out_degree = 0;
    capacity = cap;
    attr_size = attr_op->MaxSize();
    if (edge_attr_op) {
      edge_attr_size = edge_attr_op->MaxSize();
    } else {
      edge_attr_size = 0;
    }
    expand_mark = false;
  }

  std::string ToString() const {
    std::ostringstream oss;
    oss << ", out_degree = " << out_degree;
    oss << ", attr_size = " << attr_size;
    oss << ", edge_attr_size = " << edge_attr_size;
    oss << ", capacity = " << capacity;
    return oss.str();
  }
};

#pragma pack(pop)

struct CommonUpdateItems {
  std::string ToString() const {
    std::ostringstream oss;
    oss << "src_id = " << src_id;
    oss << "; overwriter = " << overwrite;
    oss << "; ids = [" << absl::StrJoin(ids, ",") << "]";
    oss << "; id_w = [" << absl::StrJoin(id_weights, ",") << "]";
    oss << "; id_ts = [" << absl::StrJoin(id_ts, ",") << "]";
    return oss.str();
  }

  uint64 src_id;
  bool overwrite = false;
  bool expire_ts_update_flag = true;
  bool use_item_ts_for_expire = false;
  std::vector<uint64> ids;
  std::string attr_update_info;
  std::vector<std::string> id_attr_update_infos;
  std::vector<float> id_weights;
  std::vector<uint32_t> id_ts;
  InsertEdgeWeightAct iewa = ADD;

  void AddEdge(uint64 id, float weight, uint32_t ts, const std::string &id_attr_update_info) {
    ids.emplace_back(id);
    id_weights.emplace_back(weight);
    id_ts.emplace_back(ts);
    id_attr_update_infos.emplace_back(id_attr_update_info);
  }

  bool Set(
      // graph config
      const GraphStorageDBConfig *db_config,
      uint64 in_id,
      std::string &&attr_info,
      // dest info
      const std::vector<uint64> &out_ids,
      std::vector<float> &&out_weights,
      std::vector<uint32_t> &&out_ts,
      std::vector<std::string> &&edge_attrs,
      // insert mode
      std::vector<bool> &&expire_ts_update_flags,
      InsertEdgeWeightAct insert_iewa,
      bool insert_overwrite,
      bool item_ts_for_expire) {
    if (out_weights.size() != 0 && out_weights.size() != out_ids.size()) {
      LOG_EVERY_N_SEC(ERROR, 5) << "out_weights size error, out_weights size: " << out_weights.size()
                                << " vs out_ids size: " << out_ids.size();
      return false;
    }
    if (out_ts.size() != 0 && out_ts.size() != out_ids.size()) {
      LOG_EVERY_N_SEC(ERROR, 5) << "out_ts size error, out_ts size: " << out_ts.size()
                                << " vs out_ids size: " << out_ids.size();
      return false;
    }
    if (edge_attrs.size() != 0 && edge_attrs.size() != out_ids.size()) {
      LOG_EVERY_N_SEC(ERROR, 5) << "edge_attrs size error, edge_attrs size: " << edge_attrs.size()
                                << " vs out_ids size: " << out_ids.size();
      return false;
    }
    if (expire_ts_update_flags.size() != 0 && expire_ts_update_flags.size() != out_ids.size()) {
      LOG_EVERY_N_SEC(ERROR, 5) << "expire_ts_update_flags size error, expire_ts_update_flags size: "
                                << expire_ts_update_flags.size() << " vs out_ids size: " << out_ids.size();
      return false;
    }
    if (insert_iewa == UNKNOWN_ACT) {
      LOG_EVERY_N_SEC(ERROR, 5) << "insert_iewa is UNKNOWN_ACT";
      return false;
    }
    // set src info
    src_id = in_id;
    attr_update_info = attr_info;
    use_item_ts_for_expire = item_ts_for_expire;

    // set dest info
    ids.reserve(out_ids.size());
    id_weights.reserve(out_weights.size());
    id_ts.reserve(out_ts.size());
    id_attr_update_infos.reserve(edge_attrs.size());
    expire_ts_update_flag = true;
    if (!expire_ts_update_flags.empty()) { expire_ts_update_flag = false; }
    for (size_t i = 0; i < out_ids.size(); ++i) {
      if (out_ids[i] == 0) {
        LOG_EVERY_N_SEC(WARNING, 5) << "Insert a invalid edge key, ignore it. "
                                    << ", src id = " << in_id << ", dest id = " << out_ids[i]
                                    << ". Reason: dest id = 0.";
        continue;
      }
      ids.emplace_back(out_ids[i]);
      if (!out_weights.empty()) { id_weights.emplace_back(out_weights[i]); }
      if (!out_ts.empty()) { id_ts.emplace_back(out_ts[i]); }
      if (!edge_attrs.empty()) { id_attr_update_infos.emplace_back(edge_attrs[i]); }
      if (!expire_ts_update_flags.empty()) { expire_ts_update_flag |= expire_ts_update_flags[i]; }
    }
    if (ids.empty() && attr_update_info.empty()) {
      LOG_EVERY_N_SEC(WARNING, 5) << "No valid dest id and no side info. Skip.";
      return false;
    }
    // set insert mode
    iewa = insert_iewa;
    overwrite = insert_overwrite;
    return true;
  }
};

struct EdgeListConfig {
  std::string relation_name;
  EdgeListReplaceStrategy oversize_replace_strategy;
  int edge_capacity_max_num;
};

class EdgeListInterface {
 public:
  EdgeListInterface() = delete;
  // This class will NOT manage lifetime of attr_op and edge_attr_op.
  EdgeListInterface(BlockStorageApi *attr_op, BlockStorageApi *edge_attr_op, const EdgeListConfig &config)
      : attr_op_(attr_op), edge_attr_op_(edge_attr_op), config_(config) {}
  virtual ~EdgeListInterface() {}

  virtual size_t GetMemorySize(int capacity = -1) const = 0;

  /*!
    \brief Initialize an edge list.
    \param new_addr Virtual address of the returned KVData, returned by `mem_allocator`.
    \return KVData, contains pointer of the new edge list.
  */
  virtual base::KVData *InitBlock(MemAllocator *mem_allocator,
                                  uint32_t *new_addr,
                                  int add_size = 0,
                                  char *old_edge_list = nullptr) const = 0;

  /*!
    \brief Merge `old_edge_list` and `items` to construct `edge_list`。
    \param edge_list Target edge list to be contructed.
    \param old_edge_list Can be nullptr.
  */
  virtual void Fill(char *edge_list, char *old_edge_list, const CommonUpdateItems &items) = 0;

  /*!
    \brief Construct a new edge_list by `items`.
    \param edge_list Uninitialized edge list.
    \param slab_size Items capacity of the memory.
  */
  virtual void FillRaw(char *edge_list, int slab_size, const CommonUpdateItems &items) = 0;

  /*!
   \brief Whether can use the current space after length changed.

   \param edge_list Pointer to edge list.
   \param appending_size Number of edges to be added, negative number represents deletion.
  */
  virtual bool CanReuse(const char *edge_list, int appending_size) const = 0;

  virtual void Clean(char *edge_list) const = 0;

  /*!
    \brief Sample some edges.

    \param edge_list Pointer to edge list.
    \param param Metadata for sample.
    \param result_writer Result container. Spaces should be allocated before call.
    \return success or not.
  */
  virtual bool Sample(const char *edge_list,
                      const SamplingParam &param,
                      EdgeListSlice *result_writer) const = 0;

  virtual bool GetEdges(const char *edge_list,
                        const std::vector<uint64> &keys,
                        NodeAttr *node_attr,
                        ProtoPtrList<EdgeInfo> *edge_info) const = 0;

  /*! \brief Delete edges by ids.

    \param edge_list Pointer to edge list after operation.
    \param keys ids to be deleted.
    \return success or not.
   */
  virtual bool DeleteItems(char *edge_list, const std::vector<uint64> &keys) = 0;

  /*!
    \brief Decay degree or weight of all edges.
    \param old_edge_list If not null, replace edge_list with old_edge_list, then decay.
    \param delete_threshold_weight If weight < delete_threshold_weight, delete this edge.
    \return Number of edges deleted.
  */
  virtual int TimeDecay(char *edge_list,
                        char *old_edge_list,
                        float degree_decay_ratio,
                        float weight_decay_ratio,
                        float delete_threshold_weight) const = 0;

  /*!
    \brief Check all edges and delete expired edges.
    \return Number of edges deleted.
  */
  virtual int EdgeExpire(char *edge_list, int expire_interval) const = 0;

  virtual void ListExpandSchema(std::vector<uint32_t> *schema) const = 0;

  /** \brief Get all info of an edge_list.
   * If node_attr or edge_info is not needed, just pass nullptr.
   */
  virtual bool GetEdgeListInfo(const char *edge_list,
                               NodeAttr *node_attr,
                               ProtoPtrList<EdgeInfo> *edge_info) const = 0;
  size_t AttrSize() const { return attr_op_->MaxSize(); }

  virtual uint32 GetSize(const char *edge_list) const = 0;
  // Return history degree, not the current degree.
  virtual uint32 GetDegree(const char *edge_list) const = 0;
  virtual std::string AttrInfo(const char *edge_list) const = 0;

  static std::unique_ptr<EdgeListInterface> NewInstance(std::string type,
                                                        BlockStorageApi *attr_op,
                                                        BlockStorageApi *edge_attr_op,
                                                        const EdgeListConfig &config);

  BlockStorageApi *attr_op_;
  BlockStorageApi *edge_attr_op_;
  EdgeListConfig config_;
};

}  // namespace chrono_graph
