// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "base/jansson/json.h"
#include "memtable/mem_kv.h"
#include "chrono_graph/src/base/block_storage_api.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/base/gnn_type.h"
#include "chrono_graph/src/base/perf_util.h"
#include "chrono_graph/src/storage/config.h"
#include "chrono_graph/src/storage/edge_list/el_interface.h"
#include "chrono_graph/src/storage/monitor/graph_monitor.h"

namespace chrono_graph {

class MemAllocator;

/**
 * \brief GraphKvUpdater rely on MemKV's implementation.
 * In GraphKV, each relation has an updater.
 */
class GraphKvUpdater {
 public:
  explicit GraphKvUpdater(const GraphStorageDBConfig &db_config)
      : attr_operator_(BlockStorageApi::NewInstance(db_config.attr_op_config))
      , edge_attr_operator_(BlockStorageApi::NewInstance(db_config.edge_attr_op_config))
      , relation_name_(db_config.relation_name)
      , expire_interval_s_(db_config.expire_interval) {
    if (expire_interval_s_ == 0) { expire_interval_s_ = DEFAULT_EXPIRE_TIME_SEC_; }
    LOG(INFO) << "Init a new relation db, config = " << db_config.ToString();
    EdgeListConfig config;
    config.relation_name = relation_name_;
    config.oversize_replace_strategy =
        static_cast<EdgeListReplaceStrategy>(db_config.oversize_replace_strategy);
    config.edge_capacity_max_num = db_config.edge_max_num;
    edge_list_adaptor_ = EdgeListInterface::NewInstance(
        db_config.elst, attr_operator_.get(), edge_attr_operator_.get(), config);
  }

  /**
    \brief malloc some space and initialize an edge list.
    \return KVData in which is the initialized edge list; its virtual address is put in new_addr。
  */
  base::KVData *InitKVData(uint32_t *new_addr,
                           MemAllocator *malloc,
                           int add_size,
                           char *edge_list = nullptr) const;

  EdgeListInterface *Adaptor() const { return edge_list_adaptor_.get(); }

  /**
   * \brief UpdateHandler for MemKV. Given a src node, insert specified edges.
   * NOTE: As the size of value changes, the value may need to move to another slab,
   * that's why InitKVData() need a MemAllocator.
   * \param old_addr: old address of edge list.
   * \return Address of edge list after insertion, may or may not the same with old_addr.
   */
  uint32 UpdateHandler(uint32 old_addr,
                       uint64 key,
                       const char *log,
                       int log_size,
                       int dummy_expire_timet,
                       base::MemKV *mem_kv);

  /**
    \brief UpdateHandler for MemKV. Given a src node, delete specified edges.
     log: `CommonUpdateItems::ids` contains all dest nodes to be removed in edge list.
    \return Address of edge list after deletion.
  */
  uint32 DeleteEdgeHandler(uint32 addr,
                           uint64 storage_key,
                           const char *log,
                           int log_size,
                           int dummy_expire_timet,
                           base::MemKV *mem_kv);

  /**
    \brief RecycleHandler for MemKV.
    After each Update(), Recycle() is triggered, find expired node and do this function for it.
  */
  void RecycleHandler(uint64 storage_key, base::KVData *cache) const {
    if (!edge_list_adaptor_->GetExpandMark(cache->data())) {
      PERF_SUM(1, relation_name_, "expire_key.count");
    }
  }

  uint32 TimeDecayHandler(uint32 addr,
                          uint64 storage_key,
                          int *delete_count,
                          float degree_decay_ratio,
                          float weight_decay_ratio,
                          float delete_threshold_weight,
                          base::MemKV *mem_kv);

  uint32 EdgeExpireHandler(
      uint32 addr, uint64 storage_key, int *delete_count, int expire_interval, base::MemKV *mem_kv);

 private:
  friend class GraphKV;
  std::unique_ptr<BlockStorageApi> attr_operator_;
  std::unique_ptr<BlockStorageApi> edge_attr_operator_;
  std::string relation_name_;
  double expire_interval_s_;
  std::unique_ptr<EdgeListInterface> edge_list_adaptor_;
  const int DEFAULT_EXPIRE_TIME_SEC_ = 86400 * 365 * 10;
  DISALLOW_COPY_AND_ASSIGN(GraphKvUpdater);
};

}  // namespace chrono_graph
