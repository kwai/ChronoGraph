// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/common/file_util.h"
#include "base/common/logging.h"
#include "base/jansson/json.h"
#include "base/thread/thread_pool.h"
#include "chrono_graph/src/base/gnn_type.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"
#include "chrono_graph/src/storage/config.h"
#include "chrono_graph/src/storage/edge_list/kv_updater.h"
#include "chrono_graph/src/storage/monitor/graph_monitor.h"
#include "chrono_graph/src/storage/persistence/checkpoint.h"
#include "chrono_graph/src/storage/persistence/graph_init.h"
#include "chrono_graph/src/storage/sample/hot_sample_cache.h"

ABSL_DECLARE_FLAG(std::string, gnn_storage_shm_dir);

namespace chrono_graph {

class MonitorManager;
class GnnBaseIniter;
/***
 * Maintains a db for each relation type.
 * Multi read-write safe.
 */
class GraphKV {
 public:
  explicit GraphKV(const GraphStorageConfig *config = nullptr);

  ~GraphKV() {
    stop_ = true;
    global_rand_sampler_.reset(nullptr);
    detection_thread_pool_->JoinAll();
    monitor_manager_->StopMonitor();
    LOG(INFO) << "GraphKV safe quit.";
  }

  /**
   * \param shm_dirs storage path, seperated by ",".
   * \return
   */
  bool Init(const std::string &shm_dirs = absl::GetFlag(FLAGS_gnn_storage_shm_dir));

  /**
   * \brief rpc insert request validation unified interface
   */
  static bool ValidateBatchInsertRequest(const ::chrono_graph::BatchInsertRequest *request);

  // rpc batch update unified interface
  ErrorCode BatchInsert(const ::chrono_graph::BatchInsertRequest *request);
  ErrorCode BatchDelete(const ::chrono_graph::BatchInsertRequest *request);

  /**
   * \brief Insert edges of a relation, with attributes (if exist).
   */
  ErrorCode InsertRelations(uint64 in_id,
                            const std::vector<uint64> &out_ids,
                            std::string relation_name,
                            std::string &&attr_info = "",
                            std::vector<float> &&out_weights = {},
                            std::vector<uint32_t> &&out_ts = {},
                            std::vector<std::string> &&edge_attrs = {},
                            std::vector<bool> &&expire_ts_update_flags = {},
                            InsertEdgeWeightAct iewa = InsertEdgeWeightAct::ADD,
                            bool insert_overwrite = false,
                            bool item_ts_for_expire = false);
  /**
   * \brief Delete edges of a relation.
   */
  ErrorCode DeleteRelations(uint64 in_id, const std::vector<uint64> &out_ids, std::string relation_name);

  /**
   * \brief Sample some src ids from a relation db.
   * \param relation_name A relation determines the type of nodes.
   * \param node_count Expected count of sample result.
   * \param node_ids Return sampled node ids.
   * \param node_attr_list Return node info of sampled ids, if not null.
   * \param pool_name Sampling pool name, if empty, use default global pool.
   */
  ErrorCode RandomSample(std::string relation_name,
                         int node_count,
                         ProtoList<uint64> *node_ids,
                         ProtoPtrList<NodeAttr> *node_attr_list = nullptr,
                         const std::string &pool_name = "") const;

  /**
   * \brief Sample neighbors of given id.
   * \param result_writer Returned sampled edges, each represented by a dst id.
   * \param degree Return src id's edge list's degree.
   */
  ErrorCode SampleNeighbors(uint64 id,
                            std::string relation_name,
                            const SamplingParam &sampling_param,
                            const std::string &caller_service_name,
                            EdgeListSlice *result_writer,
                            int64 *degree = nullptr);

  /**
   * \brief Get NodeAttrs for multi src nodes.
   * \param src_ids Id list.
   * \param result length = src_ids.size()
   */
  ErrorCode GetNodeInfos(const ProtoList<uint64> &src_ids,
                         std::string relation_name,
                         ProtoPtrList<NodeAttr> *result) const;

  /**
   * \brief Get a src node's attr and the queried edges info.
   * If either node_attr or edge_info is not needed, just pass nullptr.
   * \param dst_ids If empty, return all edges of src_id.
   * \param node_attr Return src id's attr.
   * \param edge_info Return dst id's edge info.
   */
  ErrorCode QueryNeighbors(uint64 src_id,
                           const std::vector<uint64> &dst_ids,
                           std::string relation_name,
                           NodeAttr *node_attr,
                           ProtoPtrList<EdgeInfo> *edge_info);

  /**
   * Traverse nodes of a relation.
   * \param part_num split nodes into parts
   * \param part_id traverse in one of the splitted parts.
   * \param result Return all nodes in this part.
   */
  ErrorCode TraversalByPart(std::string relation_name,
                            int64 part_num,
                            int64 part_id,
                            ProtoList<uint64> *result);

  ErrorCode GetRelationNodeCount(std::string relation_name, int64 *count);

  /**
   * \param info {src_id, relation_name}
   * \return degree is 0 when node not exist.
   */
  uint64 GetNodeDegree(GraphNode info);

  /**
   * \param info {src_id, relation_name}
   * \return length is 0 when node not exist.
   */
  uint64 GetNodeEdgeLength(GraphNode info);

  /**
   * Provide some basic statistic information.
   * \return Human readable string.
   */
  std::string GetStatInfo() const;

  // Erase all keys.
  bool Cleanup();

  void WaitReady() {
    while (!ready_) {
      LOG(INFO) << "GraphKV is initing, Not Ready...";
      base::SleepForSeconds(1);
    }
  }

  void WaitSamplerReady() {
    while (true) {
      bool sampler_ready = global_rand_sampler_->IsReady();
      for (const auto &pair : custom_rand_samplers_) {
        sampler_ready = sampler_ready && pair.second->IsReady();
      }
      if (sampler_ready) { break; }
      LOG(INFO) << "Sampler is initing, Not Ready...";
      base::SleepForSeconds(1);
    }
  }

  BlockStorageApi *get_attr_op(std::string relation_name) {
    return updaters_[relation_name]->attr_operator_.get();
  }

  BlockStorageApi *get_edge_attr_op(std::string relation_name) {
    return updaters_[relation_name]->edge_attr_operator_.get();
  }

  const GraphStorageDBConfig &RelationConfig(std::string relation_name) {
    return *db_configs_[relation_name];
  }

  const std::unordered_map<std::string, std::unique_ptr<base::MultiMemKV>> &Dbs() { return dbs_; }
  const std::unordered_map<std::string, std::unique_ptr<GraphKvUpdater>> &Updaters() { return updaters_; }

 private:
  void InitConfig();
  GraphStorageDBConfig ParseDBConfig(base::Json *config);
  void ReduceDbMemory(const GraphStorageDBConfig *config, base::MultiMemKV *db, GraphKvUpdater *updater);
  void DetectionDb(std::string relation);
  void TimeDecayThread(std::string relation);
  void EdgeExpireThread(std::string relation);
  // Meta info describes edge list format.
  std::string GenerateMetaInfo(const GraphStorageDBConfig &db_config) {
    std::ostringstream oss;
    oss << "edge_max_num: " << db_config.edge_max_num << ", elst: " << db_config.elst
        << ", attr_op: " << db_config.attr_op_config->ToString()
        << ", edge_attr_op: " << db_config.edge_attr_op_config->ToString();
    return oss.str();
  }
  // Whether meta_info the same with that in shm_dirs. If not, remove old data.
  void CheckMetaInfo(const std::string &meta_info, std::vector<std::string> shm_dirs) {
    if (shm_dirs.empty() || shm_dirs[0].empty()) {
      LOG(WARNING) << "empty shm_dirs, pure memory mode, skip checking meta info";
      return;
    }
    LOG(INFO) << "meta info: " << meta_info;
    std::string saved_meta_info;
    base::ReadFileToString(shm_dirs[0] + "/meta_info", &saved_meta_info);
    LOG(INFO) << "saved meta info: " << saved_meta_info;
    if (!saved_meta_info.empty() && meta_info != saved_meta_info) {
      LOG(ERROR) << "meta info check failed, deleting all data";
      for (const auto &dir : shm_dirs) { CHECK(fs::remove_all(dir)) << "fail to delete dir: " << dir; }
    } else {
      LOG(INFO) << "meta info check pass";
      return;
    }
    CHECK(fs::create_directories(shm_dirs[0])) << "fail to create dir: " << shm_dirs[0];
    CHECK(base::WriteFile(shm_dirs[0] + "/meta_info", meta_info.c_str(), meta_info.size()));
  }

  // common config for all relations
  GraphStorageConfig config_;
  // each relation_name has a db, updater, and db_config.
  std::unordered_map<std::string, std::unique_ptr<base::MultiMemKV>> dbs_;
  std::unordered_map<std::string, std::unique_ptr<GraphKvUpdater>> updaters_;
  std::unordered_map<std::string, const GraphStorageDBConfig *> db_configs_;

  std::unique_ptr<GraphCheckpoint> checkpoint_manager_;
  std::unique_ptr<thread::ThreadPool> detection_thread_pool_;
  std::vector<std::unique_ptr<GnnBaseIniter>> gnn_initers_;
  std::unique_ptr<MonitorManager> monitor_manager_;
  // sample pools
  std::unique_ptr<WeightSampleCache> global_rand_sampler_;
  std::unordered_map<std::string, std::unique_ptr<CustomSampleCache>> custom_rand_samplers_;

  std::atomic_bool ready_{false};
  std::atomic_bool stop_{false};

 private:
  DISALLOW_COPY_AND_ASSIGN(GraphKV);
};

}  // namespace chrono_graph
