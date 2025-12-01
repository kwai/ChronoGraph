// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/graph_kv.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <iomanip>
#include <set>
#include <sstream>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/common/gflags.h"
#include "base/common/logging.h"
#include "base/common/string.h"
#include "base/common/timer.h"
#include "base/random/pseudo_random.h"
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/base/perf_util.h"
#include "chrono_graph/src/base/util.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"
#include "chrono_graph/src/storage/config.h"
#include "chrono_graph/src/storage/edge_list/kv_updater.h"
#include "chrono_graph/src/storage/monitor/graph_monitor.h"
#include "chrono_graph/src/storage/persistence/graph_init.h"

// Support multiple paths like "/dev/shm/gnn1,/dev/shm/gnn2"
ABSL_FLAG(std::string, gnn_storage_shm_dir, "/dev/shm/gnn", "shared memory dir path");
ABSL_FLAG(std::string, global_rand_sample_type, "normal", "Used for basic random sampling");
ABSL_FLAG(int32, detect_db_interval_in_seconds, 120, "db compact interval in seconds");

namespace chrono_graph {

static base::MultiMemKV *GetKVByType(
    const std::unordered_map<std::string, std::unique_ptr<base::MultiMemKV>>
        &db,
    std::string relation_name);

#define SAFE_DB_BY_RELATION_RETURN_CODE()                                      \
  auto db_kernel = GetKVByType(dbs_, relation_name);                           \
  if (!db_kernel) {                                                            \
    LOG_EVERY_N(ERROR, 1000)                                                   \
        << "Invalid Relation type, get null db, relation = " << relation_name; \
    return ErrorCode::RELATION_NAME_NOT_AVAILABLE;                             \
  }

#define SAFE_DB_BY_RELATION_RETURN_FALSE()                                     \
  auto db_kernel = GetKVByType(dbs_, relation_name);                           \
  if (!db_kernel) {                                                            \
    LOG_EVERY_N(ERROR, 1000)                                                   \
        << "Invalid Relation type, get null db, relation = " << relation_name; \
    return false;                                                              \
  }

#define READ_AUTO_LOCK_BY_RELATION_AND_KEY(key)                        \
  auto part_id = db_kernel->GetPart(key);                              \
  auto *rw_mutex_manager = rw_mutex_managers_.at(relation_name).get(); \
  auto *mutex = rw_mutex_manager->GetMutexByPartKey(part_id, key);     \
  absl::ReaderMutexLock lock(mutex);

#define WRITE_AUTO_LOCK_BY_RELATION_AND_KEY(key)                       \
  auto part_id = db_kernel->GetPart(key);                              \
  auto *rw_mutex_manager = rw_mutex_managers_.at(relation_name).get(); \
  auto *mutex = rw_mutex_manager->GetMutexByPartKey(part_id, key);     \
  absl::WriterMutexLock lock(mutex);

GraphKV::GraphKV(const GraphStorageConfig *config) {
  if (config == nullptr) {
    InitConfig();
  } else {
    config_ = *config;
  }
}

GraphStorageDBConfig GraphKV::ParseDBConfig(base::Json *config) {
  GraphStorageDBConfig db_config;
  db_config.kv_shard_num =
      chrono_graph::GetInt64ConfigSafe(config, "kv_shard_num");
  db_config.kv_dict_capacity =
      chrono_graph::GetInt64ConfigSafe(config, "kv_dict_size");
  db_config.shard_memory_size =
      chrono_graph::GetInt64ConfigSafe(config, "kv_size");

  auto relation_name =
      chrono_graph::GetStringConfigSafe(config, "relation_name");
  db_config.relation_name = relation_name;

  static base::Json default_attr_op_config(base::StringToJson("{}"));
  default_attr_op_config.set("type_name", "EmptyBlock");
  db_config.attr_op_config = config->Get("attr_op_config");
  if (!db_config.attr_op_config) {
    LOG(INFO) << relation_name
              << " attr_op_config not found, use default empty op.";
    db_config.attr_op_config = &default_attr_op_config;
  }
  db_config.edge_attr_op_config = config->Get("edge_attr_op_config");
  if (!db_config.edge_attr_op_config) {
    LOG(INFO) << relation_name
              << " edge_attr_op_config not found, use default empty op.";
    db_config.edge_attr_op_config = &default_attr_op_config;
  }
  db_config.edge_max_num =
      chrono_graph::GetInt64ConfigSafe(config, "edge_max_num");
  db_config.expire_interval =
      chrono_graph::GetInt64ConfigSafe(config, "expire_interval");
  db_config.edge_expire_interval_s = config->GetInt(
      "edge_expire_interval_s", db_config.edge_expire_interval_s);
  db_config.edge_expire_check_interval_s = config->GetInt(
      "edge_expire_check_interval_s", db_config.edge_expire_check_interval_s);
  db_config.oversize_replace_strategy = config->GetNumber(
      "oversize_replace_strategy", db_config.oversize_replace_strategy);
  db_config.max_occupancy_rate =
      config->GetNumber("max_occupancy_rate", db_config.max_occupancy_rate);
  db_config.reduce_occupancy_rate = config->GetNumber(
      "reduce_occupancy_rate", db_config.reduce_occupancy_rate);
  db_config.reduce_occupancy_strategy = config->GetInt(
      "reduce_occupancy_strategy", db_config.reduce_occupancy_strategy);
  db_config.elst = config->GetString("elst", "cpt");
  db_config.weight_decay_ratio =
      config->GetNumber("weight_decay_ratio", db_config.weight_decay_ratio);
  CHECK(db_config.weight_decay_ratio > 0 && db_config.weight_decay_ratio <= 1.0)
      << "weight_decay_ratio not in (0, 1]: " << db_config.weight_decay_ratio;
  db_config.degree_decay_ratio =
      config->GetNumber("degree_decay_ratio", db_config.degree_decay_ratio);
  CHECK(db_config.degree_decay_ratio > 0 && db_config.degree_decay_ratio <= 1.0)
      << "degree_decay_ratio not in (0, 1]: " << db_config.degree_decay_ratio;
  db_config.decay_interval_s =
      config->GetInt("decay_interval_s", db_config.decay_interval_s);
  db_config.delete_threshold_weight = config->GetNumber(
      "delete_threshold_weight", db_config.delete_threshold_weight);
  return db_config;
}

void GraphKV::InitConfig() {
  auto json_config = GnnDynamicJsonConfig::GetConfig()->GetHostConfig();
  CHECK(json_config) << "config not exists, check it!";
  LOG(INFO) << "config = " << json_config->ToString();

  auto service_name =
      chrono_graph::GetStringConfigSafe(json_config, "service_name");
  CHECK(!service_name.empty());

  config_.service_name = service_name;
  config_.init_config = json_config->Get("init");
  config_.checkpoint_config = json_config->Get("checkpoint");
  config_.exporter_config = json_config->Get("exporter");
  config_.sample_pool_config = json_config->Get("sample_pool");

  auto dbs_config = chrono_graph::GetArrayConfigSafe(json_config, "dbs");
  for (auto db_config : dbs_config) {
    config_.db_configs.push_back(ParseDBConfig(db_config));
  }
}

bool GraphKV::Init(const std::string &flag_shm_dirs) {
  if (ready_) {
    return true;
  }

  std::vector<std::string> shm_dirs = absl::StrSplit(flag_shm_dirs, ',');

  // Init each relation db.
  for (auto &db_config : config_.db_configs) {
    auto relation_name = db_config.relation_name;
    updaters_.emplace(relation_name,
                      std::make_unique<GraphKvUpdater>(db_config));
    db_configs_.emplace(relation_name, &db_config);
    auto generate_slab_handler = [&](std::vector<uint32_t> *slabs) {
      updaters_[relation_name]->Adaptor()->ListExpandSchema(slabs);
      CHECK(slabs->size() > 0) << "Unexpected slabs size: " << slabs->size()
                               << ", check your config!";
      for (size_t i = 0; i < slabs->size(); ++i) {
        // each slab contains metadata from KVData, and the actual data.
        (*slabs)[i] += sizeof(base::KVData);
      }
      LOG(INFO) << "size of data in slabs = " << absl::StrJoin(*slabs, ",")
                << " for " << relation_name;
    };
    int expire_interval = (int)db_config.expire_interval;
    // Set expire time to 10 years by default.
    if (expire_interval <= 0) {
      expire_interval = 86400 * 365 * 10;
    }

    std::vector<std::string> final_shm_paths;
    for (auto shm_dir : shm_dirs) {
      final_shm_paths.push_back(shm_dir + "/" + relation_name);
    }

    CheckMetaInfo(GenerateMetaInfo(db_config), final_shm_paths);

    dbs_.emplace(
        relation_name,
        std::make_unique<base::MultiMemKV>(
            final_shm_paths, db_config.kv_shard_num,
            (uint64_t)db_config.kv_dict_capacity * db_config.kv_shard_num,
            expire_interval,
            (uint64_t)db_config.shard_memory_size * db_config.kv_shard_num,
            generate_slab_handler));

    rw_mutex_managers_.emplace(
        relation_name,
        std::make_unique<thread::RWMutexManager>(db_config.kv_shard_num));
  }

  auto all_db_empty = [this]() -> bool {
    for (const auto &pair : dbs_) {
      int64 count = 0;
      this->GetRelationNodeCount(pair.first, &count);
      if (count > 0) {
        return false;
      }
    }
    return true;
  };

  detection_thread_pool_ =
      std::make_unique<thread::ThreadPool>(dbs_.size() * 3);
  std::vector<std::string> available_relations;
  for (const auto &pair : dbs_) {
    std::string key = pair.first;
    available_relations.emplace_back(key);
    detection_thread_pool_->AddTask([this, key]() { this->DetectionDb(key); });
    detection_thread_pool_->AddTask(
        [this, key]() { this->TimeDecayThread(key); });
    detection_thread_pool_->AddTask(
        [this, key]() { this->EdgeExpireThread(key); });
  }
  LOG(INFO) << "Init DB Relation Set: ["
            << absl::StrJoin(available_relations, ", ") << "]";

  monitor_manager_ = std::make_unique<MonitorManager>(this);

  if (config_.checkpoint_config != nullptr) {
    GraphCheckpoint::CheckpointConfig config{this, config_.checkpoint_config};
    checkpoint_manager_ = std::make_unique<GraphCheckpoint>(config);
    if (!checkpoint_manager_->Init(all_db_empty())) {
      LOG(INFO) << "checkpoint init fail";
    }
  }

  if (config_.init_config) {
    CHECK(config_.init_config->IsArray())
        << "init config is not an array, init fail, check your config, config "
           "= "
        << config_.init_config->ToString() << ", type is "
        << config_.init_config->Type();
    gnn_initers_.resize(config_.init_config->size());
    for (size_t i = 0; i < config_.init_config->size(); ++i) {
      base::Json *config = config_.init_config->array()[i];
      gnn_initers_[i] = GnnBaseIniter::NewInstance(config, this);
      CHECK(gnn_initers_[i].get())
          << "Initer init fail, check your type name! i = " << i
          << ", config = " << config->ToString();
    }
  }

  // Initialize sample pool.
  if (config_.sample_pool_config != nullptr) {
    CHECK(config_.sample_pool_config->IsArray())
        << "sample_pool config is not an array, init fail, check your config, "
           "config = "
        << config_.sample_pool_config->ToString() << ", type is "
        << config_.sample_pool_config->Type();
    auto pool_size = config_.sample_pool_config->size();
    for (size_t i = 0; i < pool_size; ++i) {
      auto pool_conf = config_.sample_pool_config->array()[i];
      std::string name = pool_conf->GetString("name", "");
      CHECK(!name.empty())
          << "Invalid config: sample pool name must be specified.";
      custom_rand_samplers_.emplace(name,
                                    new CustomSampleCache(this, pool_conf));
      CHECK(custom_rand_samplers_.at(name)->Init())
          << "Custom rand sampler init fail";
    }
  }
  // Initialize a global sampler.
  bool need_init_global = true;
  if (absl::GetFlag(FLAGS_global_rand_sample_type) == "normal") {
    global_rand_sampler_ = std::make_unique<FlattenWeightSampleCache>(this);
    LOG(INFO) << "global_rand_sampler use flatten_weight!";
  } else if (absl::GetFlag(FLAGS_global_rand_sample_type) == "degree") {
    global_rand_sampler_ = std::make_unique<DegreeBaseSampleCache>(this);
    LOG(INFO) << "global_rand_sampler use degree!";
  } else {
    need_init_global = false;
  }
  if (need_init_global) {
    CHECK(global_rand_sampler_->Init()) << "global_rand_sampler init fail";
  } else {
    LOG(INFO) << "Skip initialize global_rand_sampler.";
  }

  return (ready_ = true);
}

bool GraphKV::ValidateBatchInsertRequest(
    const ::chrono_graph::BatchInsertRequest *request) {
  size_t insert_size = request->src_ids().size();
  size_t dest_size = request->dest_ids_size();
  if (!request->attrs().empty() && (request->attrs().size() != insert_size)) {
    LOG_EVERY_N_SEC(WARNING, 5)
        << "batch insert error, src id size = " << insert_size
        << ", attr size = " << request->attrs().size();
    return false;
  }
  if (!request->dest_ids().empty() &&
      (request->dest_ids_size() != insert_size)) {
    LOG_EVERY_N_SEC(WARNING, 5)
        << "batch insert error, src id size = " << insert_size
        << ", dst_ids size = " << request->dest_ids_size();
    return false;
  }
  if (!request->dest_weights().empty() &&
      (request->dest_weights_size() != request->dest_ids_size())) {
    LOG_EVERY_N_SEC(WARNING, 5)
        << "batch insert error, dest ids size = " << dest_size
        << ", dest weights size = " << request->dest_weights_size();
    return false;
  }
  if (!request->dst_ts().empty() &&
      (request->dst_ts_size() != request->dest_ids_size())) {
    LOG_EVERY_N_SEC(WARNING, 5)
        << "batch insert error, dst ids size = " << dest_size
        << ", dst ts size = " << request->dst_ts_size();
    return false;
  }
  if (!request->edge_attrs().empty() &&
      (request->edge_attrs_size() != request->dest_ids_size())) {
    LOG_EVERY_N_SEC(WARNING, 5)
        << "batch insert error, dst ids size = " << dest_size
        << ", dst attrs size = " << request->edge_attrs_size();
    return false;
  }
  if (!request->expire_ts_update_flags().empty() &&
      (request->expire_ts_update_flags_size() != request->dest_ids_size())) {
    LOG_EVERY_N_SEC(WARNING, 5)
        << "batch insert error, dst ids size = " << dest_size
        << ", expire_ts_update_flags size = "
        << request->expire_ts_update_flags_size();
    return false;
  }
  if (!request->use_item_ts_for_expires().empty() &&
      (request->use_item_ts_for_expires_size() != request->src_ids_size())) {
    LOG_EVERY_N_SEC(WARNING, 5)
        << "batch insert error, src ids size = " << insert_size
        << ", use_item_ts_for_expires size = "
        << request->use_item_ts_for_expires_size();
    return false;
  }
  return true;
}

ErrorCode GraphKV::BatchInsert(
    const ::chrono_graph::BatchInsertRequest *request) {
  auto begin = base::GetTimestamp();
  CHECK(!request->is_delete());
  std::vector<uint64> tmp_dests;
  std::vector<float> tmp_w;
  std::vector<uint32_t> tmp_ts;
  std::vector<std::string> tmp_edge_attrs;
  std::vector<bool> tmp_expire_ts_update_flags;
  tmp_dests.reserve(request->dest_ids_size());
  tmp_w.reserve(request->dest_weights_size());
  tmp_ts.reserve(request->dst_ts_size());
  tmp_edge_attrs.reserve(request->edge_attrs_size());
  tmp_expire_ts_update_flags.reserve(request->expire_ts_update_flags_size());
  int insert_time = 0;
  for (auto i = 0; i < request->src_ids().size(); ++i) {
    base::Timer timer;
    auto in_id = request->src_ids(i);
    if (!request->dest_ids().empty()) {
      tmp_dests.push_back(request->dest_ids(i));
    }
    if (!request->dest_weights().empty()) {
      tmp_w.push_back(request->dest_weights(i));
    }
    if (!request->dst_ts().empty()) {
      tmp_ts.push_back(request->dst_ts(i));
    }
    if (!request->edge_attrs().empty()) {
      tmp_edge_attrs.push_back(request->edge_attrs(i));
    }
    if (!request->expire_ts_update_flags().empty()) {
      tmp_expire_ts_update_flags.push_back(request->expire_ts_update_flags(i));
    }
    // Edges with the same src_id will be inserted together.
    if ((i < request->src_ids().size() - 1) &&
        (request->src_ids(i) == request->src_ids(i + 1))) {
      continue;
    }

    bool use_item_ts_for_expire_flag = false;
    if (!request->use_item_ts_for_expires().empty()) {
      use_item_ts_for_expire_flag = request->use_item_ts_for_expires(i);
    }

    std::string src_attr =
        request->attrs().empty() ? "" : request->attrs().Get(i);
    timer.AppendCostMs("prepare params");
    auto code = InsertRelations(
        in_id, tmp_dests, request->relation_name(), std::move(src_attr),
        std::move(tmp_w), std::move(tmp_ts), std::move(tmp_edge_attrs),
        std::move(tmp_expire_ts_update_flags), request->iewa(),
        false,  // overwrite
        use_item_ts_for_expire_flag);
    timer.AppendCostMs("insert relations");
    if (code != ErrorCode::OK) {
      return code;
    }

    if (timer.Stop() > 10000) {
      LOG_EVERY_N_SEC(WARNING, 3)
          << "insert key: " << in_id << " long time cost: " << timer.display()
          << ",  out_id size = " << tmp_dests.size();
    } else {
      LOG_EVERY_N_SEC(INFO, 5)
          << "insert key: " << in_id << " time cost: " << timer.display()
          << ", out_id size = " << tmp_dests.size();
    }

    insert_time++;
    tmp_dests.clear();
    tmp_w.clear();
    tmp_ts.clear();
    tmp_edge_attrs.clear();
    tmp_expire_ts_update_flags.clear();
  }

  auto end = base::GetTimestamp();
  if (insert_time > 0) {
    PERF_AVG(end - begin, request->relation_name(), P_RQ_T, "insert_batch");
    PERF_AVG((end - begin) / insert_time, request->relation_name(), P_RQ_T,
             "insert_once");
  }
  return ErrorCode::OK;
}

ErrorCode GraphKV::BatchDelete(
    const ::chrono_graph::BatchInsertRequest *request) {
  auto begin = base::GetTimestamp();
  CHECK(request->is_delete());
  size_t delete_size = request->src_ids().size();
  std::vector<uint64> tmp_dests;
  tmp_dests.reserve(request->dest_ids_size());
  int delete_time = 0;
  for (auto i = 0; i < request->src_ids().size(); ++i) {
    if (!request->dest_ids().empty()) {
      tmp_dests.push_back(request->dest_ids(i));
    }
    // Edges with the same src_id will be deleted together.
    if ((i < request->src_ids().size() - 1) &&
        (request->src_ids(i) == request->src_ids(i + 1))) {
      continue;
    }

    auto code = DeleteRelations(request->src_ids(i), tmp_dests,
                                request->relation_name());
    if (code != ErrorCode::OK) {
      return code;
    }
    delete_time++;
    tmp_dests.clear();
  }
  auto end = base::GetTimestamp();
  if (delete_size > 0) {
    PERF_AVG(end - begin, request->relation_name(), P_RQ_T, "delete_batch");
    PERF_AVG((end - begin) / delete_time, request->relation_name(), P_RQ_T,
             "delete_once");
  }
  return ErrorCode::OK;
}

ErrorCode GraphKV::InsertRelations(
    uint64 in_id, const std::vector<uint64> &out_ids, std::string relation_name,
    std::string &&attr_info, std::vector<float> &&out_weights,
    std::vector<uint32_t> &&out_ts, std::vector<std::string> &&edge_attrs,
    std::vector<bool> &&expire_ts_update_flags, InsertEdgeWeightAct iewa,
    bool insert_overwrite, bool item_ts_for_expire) {
  SAFE_DB_BY_RELATION_RETURN_CODE()
  auto db_config = db_configs_.at(relation_name);
  base::Timer timer;
  CommonUpdateItems items;
  if (!items.Set(db_config, in_id, std::move(attr_info), out_ids,
                 std::move(out_weights), std::move(out_ts),
                 std::move(edge_attrs), std::move(expire_ts_update_flags), iewa,
                 insert_overwrite, item_ts_for_expire)) {
    LOG_EVERY_N_SEC(ERROR, 5) << "CommonUpdateItems set error";
    return ErrorCode::ARGS_ILLEGAL;
  }
  timer.AppendCostMs("items set");
  auto updater = updaters_.at(relation_name).get();
  WRITE_AUTO_LOCK_BY_RELATION_AND_KEY(in_id);
  db_kernel->Update(
      in_id, reinterpret_cast<const char *>(&items), sizeof(items), 0,
      [&](uint32 old_addr, uint64 key, const char *log, int log_size,
          int dummy_t, base::MemKV *mem_kv) -> uint32 {
        return updater->UpdateHandler(old_addr, key, log, log_size, 0, mem_kv);
      },
      nullptr,
      [&](uint64 cur_key, base::KVData *data) -> void {
        updater->RecycleHandler(cur_key, data);
      });
  timer.AppendCostMs("db kernel update");
  LOG_EVERY_N_SEC(INFO, 5) << "insert key: " << in_id
                           << " time cost: " << timer.display()
                           << ", out_id size = " << out_ids.size();
  return ErrorCode::OK;
}

ErrorCode GraphKV::DeleteRelations(uint64 in_id,
                                   const std::vector<uint64> &out_ids,
                                   std::string relation_name) {
  SAFE_DB_BY_RELATION_RETURN_CODE()
  if (in_id == 0) {
    LOG_EVERY_N_SEC(WARNING, 1) << "Delete a zero src node, ignore it. "
                                << ", relation_name = " << relation_name;
    return ErrorCode::OK;
  }
  CommonUpdateItems items;
  for (size_t i = 0; i < out_ids.size(); ++i) {
    if (out_ids[i] == 0) {
      LOG_EVERY_N_SEC(WARNING, 1)
          << "Delete an invalid edge key, ignore it. "
          << ", relation_name = " << relation_name << ", src id = " << in_id
          << ", dest id = " << out_ids[i] << ". Reason: dest id = 0.";
      return ErrorCode::OK;
    }
    items.ids.emplace_back(out_ids[i]);
  }

  auto updater = updaters_.at(relation_name).get();
  WRITE_AUTO_LOCK_BY_RELATION_AND_KEY(in_id);
  db_kernel->Update(
      in_id, reinterpret_cast<const char *>(&items), sizeof(items), 0,
      [&](uint32 addr, uint64 key, const char *log, int log_size, int dummy_t,
          base::MemKV *mem_kv) -> uint32 {
        return updater->DeleteEdgeHandler(addr, key, log, log_size, 0, mem_kv);
      },
      nullptr,
      [&](uint64 cur_key, base::KVData *data) -> void {
        updater->RecycleHandler(cur_key, data);
      });
  return ErrorCode::OK;
}

ErrorCode GraphKV::RandomSample(std::string relation_name, int node_count,
                                ProtoList<uint64> *node_ids,
                                ProtoPtrList<NodeAttr> *node_attr_list,
                                const std::string &pool_name) const {
  SAFE_DB_BY_RELATION_RETURN_CODE()
  ErrorCode status;
  // Sample nodes from sample pool.
  bool used_custom_pool = false;
  if (!pool_name.empty()) {
    if (custom_rand_samplers_.find(pool_name) != custom_rand_samplers_.end()) {
      LOG_EVERY_N_SEC(INFO, 10) << "Sample from custom pool: " << pool_name;
      node_ids->Resize(node_count, 0);
      status = custom_rand_samplers_.at(pool_name)->Sample(
          relation_name, node_count, node_ids->mutable_data());
      used_custom_pool = true;
    } else {
      LOG_EVERY_N_SEC(WARNING, 10)
          << "custom pool " << pool_name
          << " not found. Try use default global pool instead.";
    }
  }
  if (!used_custom_pool) {
    if (global_rand_sampler_.get()) {
      LOG_EVERY_N_SEC(INFO, 10) << "Sample from global_rand_sampler.";
      node_ids->Resize(node_count, 0);
      status = global_rand_sampler_->Sample(relation_name, node_count,
                                            node_ids->mutable_data());
    }
  }
  // Get node attr according to sampled ids.
  if (node_attr_list) {
    auto updater = updaters_.at(relation_name).get();
    node_attr_list->Clear();
    CHECK_EQ(node_ids->size(), node_count);
    for (size_t i = 0; i < node_count; ++i) {
      auto *info = node_attr_list->Add();
      auto id = node_ids->Get(i);
      info->set_id(id);
      READ_AUTO_LOCK_BY_RELATION_AND_KEY(id);
      base::KVReadData value = db_kernel->Get(id);
      if (value.size <= 0) {
        LOG_EVERY_N_SEC(INFO, 1) << "miss node " << id;
        continue;
      }
      updater->edge_list_adaptor_->GetEdgeListInfo(value.data, info, nullptr);
    }
  }
  return status;
}

// Even if sampling failed, the client usually will padding the result to make
// sure the total number of result is equal to the sampling num.
ErrorCode GraphKV::SampleNeighbors(uint64 id, std::string relation_name,
                                   const SamplingParam &sampling_param,
                                   const std::string &caller_service_name,
                                   EdgeListSlice *result_writer,
                                   int64 *degree) {
  SAFE_DB_BY_RELATION_RETURN_CODE()

  base::Timer timer;
  READ_AUTO_LOCK_BY_RELATION_AND_KEY(id);
  base::KVReadData value = db_kernel->Get(id);
  // key not exist or expired.
  PERF_SUM(1, relation_name, P_RQ, "sample.type", caller_service_name);
  PERF_SUM(1, relation_name, P_RQ,
           "sample.type-" +
               SamplingParam_SamplingStrategy_Name(sampling_param.strategy()),
           caller_service_name);
  if (value.data == nullptr || value.expired()) {
    PERF_SUM(1, relation_name, P_RQ, "sample.not_exist", caller_service_name);
    LOG_EVERY_N_SEC(INFO, 2) << "sample node not exist or expired, id = " << id
                             << ", relation_name = " << relation_name
                             << ", expired = " << value.expired();
    return ErrorCode::KEY_NOT_EXIST;
  }
  // key exist but value is empty, early stop.
  if (value.size == 0) {
    PERF_SUM(1, relation_name, P_RQ, "sample.empty_node", caller_service_name);
    LOG_EVERY_N_SEC(INFO, 2)
        << "sample node has empty node list, stop early, id = " << id
        << ", relation_name = " << relation_name;
    return ErrorCode::OK;
  }
  timer.AppendCostMs("get value");
  auto prev = result_writer->offset;
  auto updater = updaters_.at(relation_name).get();
  bool ret = updater->edge_list_adaptor_->Sample(value.data, sampling_param,
                                                 result_writer);
  timer.AppendCostMs("sample one id");
  monitor_manager_->AddSampleResultCount(result_writer->offset - prev,
                                         relation_name, caller_service_name);
  timer.AppendCostMs("result perf");
  if (!ret) {
    PERF_SUM(1, relation_name, P_RQ, "sample.inner_error", caller_service_name);
    LOG_EVERY_N_SEC(INFO, 2) << "sample inner error, stop early, id = " << id
                             << ", relation_name = " << relation_name;
    return ErrorCode::UNKNOWN;
  }
  if (degree) {
    *degree = updater->edge_list_adaptor_->GetDegree(value.data);
    timer.AppendCostMs("get degree");
  }
  auto eps = timer.Stop();
  if (eps > 2000) {
    LOG_EVERY_N_SEC(WARNING, 3)
        << "sample key: " << id << " long time cost: " << timer.display()
        << ", sample type: " << sampling_param.strategy();

  } else {
    LOG_EVERY_N_SEC(INFO, 5)
        << "sample key: " << id << " time cost: " << timer.display()
        << ", sample type: " << sampling_param.strategy();
  }
  return ErrorCode::OK;
}

ErrorCode GraphKV::GetNodeInfos(const ProtoList<uint64> &src_ids,
                                std::string relation_name,
                                ProtoPtrList<NodeAttr> *result) const {
  auto begin = base::GetTimestamp() / 1000;
  SAFE_DB_BY_RELATION_RETURN_CODE()
  auto updater = updaters_.at(relation_name).get();
  result->Clear();
  for (size_t i = 0; i < src_ids.size(); ++i) {
    uint64 key = *(src_ids.begin() + i);
    READ_AUTO_LOCK_BY_RELATION_AND_KEY(key);
    base::KVReadData value = db_kernel->Get(key);
    auto info = result->Add();
    if (value.size <= 0) {
      LOG_EVERY_N_SEC(INFO, 1) << "miss node " << key;
      continue;
    }
    info->set_id(key);
    updater->edge_list_adaptor_->GetEdgeListInfo(value.data, info, nullptr);
  }
  auto end = base::GetTimestamp() / 1000;
  LOG_EVERY_N_SEC(INFO, 5) << "use_ms: " << (end - begin)
                           << ", src_id num: " << src_ids.size();
  return ErrorCode::OK;
}

ErrorCode GraphKV::QueryNeighbors(uint64 src_id,
                                  const std::vector<uint64> &dst_ids,
                                  std::string relation_name,
                                  NodeAttr *node_attr,
                                  ProtoPtrList<EdgeInfo> *edge_info) {
  SAFE_DB_BY_RELATION_RETURN_CODE()
  auto updater = updaters_.at(relation_name).get();
  READ_AUTO_LOCK_BY_RELATION_AND_KEY(src_id);
  base::KVReadData value = db_kernel->Get(src_id);
  bool key_not_exist =
      value.data == nullptr || value.size == 0 || value.expired();
  // Get all edges.
  if (dst_ids.empty()) {
    if (key_not_exist) {
      return ErrorCode::KEY_NOT_EXIST;
    }
    node_attr->set_id(src_id);
    bool succ = updater->edge_list_adaptor_->GetEdgeListInfo(
        value.data, node_attr, edge_info);
    if (!succ) {
      return ErrorCode::UNKNOWN;
    }
    return ErrorCode::OK;
  }

  // Query edges.
  if (key_not_exist) {
    // fill 0 edge
    for (size_t i = 0; i < dst_ids.size(); ++i) {
      auto edge = edge_info->Add();
      edge->set_id(0);
      edge->set_weight(0);
      edge->set_timestamp(0);
      *(edge->mutable_edge_attr()) = std::move("");
    }
    return ErrorCode::KEY_NOT_EXIST;
  }
  bool succ = updater->edge_list_adaptor_->GetEdges(value.data, dst_ids,
                                                    node_attr, edge_info);
  if (!succ) {
    return ErrorCode::UNKNOWN;
  }
  return ErrorCode::OK;
}

ErrorCode GraphKV::TraversalByPart(std::string relation_name, int64 part_num,
                                   int64 part_id, ProtoList<uint64> *result) {
  SAFE_DB_BY_RELATION_RETURN_CODE()
  std::vector<std::vector<uint64>> shard_keys(db_kernel->PartNum(),
                                              std::vector<uint64>{});
  size_t key_size = 0;
  for (int shard_id = 0; shard_id < db_kernel->PartNum(); ++shard_id) {
    db_kernel->GetPartKeys(shard_id, part_num, part_id, &shard_keys[shard_id]);
    key_size += shard_keys[shard_id].size();
  }
  result->Clear();
  result->Reserve(key_size);
  size_t offset = 0;
  for (auto &keys : shard_keys) {
    for (auto key : keys) {
      result->AddAlreadyReserved(key);
    }
    offset += keys.size();
  }

  return ErrorCode::OK;
}

ErrorCode GraphKV::GetRelationNodeCount(std::string relation_name,
                                        int64 *count) {
  SAFE_DB_BY_RELATION_RETURN_CODE()
  *count = db_kernel->ValidKeyNum();
  LOG_EVERY_N_SEC(INFO, 10) << relation_name << " has " << *count << " keys.";
  return ErrorCode::OK;
}

uint64 GraphKV::GetNodeExpireTime(GraphNode info) {
  auto relation_name = info.relation_name;
  SAFE_DB_BY_RELATION_RETURN_FALSE()
  READ_AUTO_LOCK_BY_RELATION_AND_KEY(info.node_id);
  auto value = db_kernel->Get(info.node_id);
  if (value.size <= 0 || value.expired()) {
    return 0;
  }
  return value.expire_timet;
}

uint64 GraphKV::GetNodeDegree(chrono_graph::GraphNode info) {
  auto relation_name = info.relation_name;
  SAFE_DB_BY_RELATION_RETURN_FALSE()
  READ_AUTO_LOCK_BY_RELATION_AND_KEY(info.node_id);
  auto value = db_kernel->Get(info.node_id);
  if (value.size <= 0 || value.expired()) {
    return 0;
  }
  auto updater = updaters_.at(relation_name).get();
  return updater->edge_list_adaptor_->GetDegree(value.data);
}

uint64 GraphKV::GetNodeEdgeLength(chrono_graph::GraphNode info) {
  auto relation_name = info.relation_name;
  SAFE_DB_BY_RELATION_RETURN_FALSE()
  READ_AUTO_LOCK_BY_RELATION_AND_KEY(info.node_id);
  auto value = db_kernel->Get(info.node_id);
  if (value.size <= 0 || value.expired()) {
    return 0;
  }
  auto updater = updaters_.at(relation_name).get();
  return updater->edge_list_adaptor_->GetSize(value.data);
}

std::string GraphKV::GetStatInfo() const {
  std::ostringstream buffer;
  buffer << ">>>> Graph Relation Num : " << this->dbs_.size() << std::endl;
  for (const auto &pair : dbs_) {
    auto relation_name = pair.first;
    auto kv = pair.second.get();
    if (!kv) {
      continue;
    }
    buffer << std::endl
           << ">>>> \nRelation = " << relation_name
           << ", keys num = " << kv->ValidKeyNum()
           << ", TotalMemInFreeList = " << kv->TotalMemInFreeList() << std::endl
           << kv->GetInfo();
  }

  return buffer.str();
}

void GraphKV::ReduceDbMemory(const GraphStorageDBConfig *config,
                             base::MultiMemKV *mem_kv,
                             GraphKvUpdater *updater) {
  auto occupancy_rate = mem_kv->ValidOccupancyRate();
  // try to compact every time
  auto begin_ts = base::GetTimestamp() / 1000;
  LOG(INFO) << config->relation_name << " occupancy_rate: " << occupancy_rate
            << " reduce threshold: " << config->max_occupancy_rate
            << " alloc_rate: " << mem_kv->MemAllocRate();

  static const int kMaxSearchTime = 3;
  static const int kScoreBucketNum = 1 << 15;
  static std::atomic<uint64_t> buckets[kScoreBucketNum];
  int64_t key_num = mem_kv->ValidKeyNum();
  int64_t key_num_threshold =
      key_num * (1 - config->reduce_occupancy_rate / occupancy_rate);
  float bucket_interval = 1.0, threshold_score = 0, left_threshold = 0,
        right_threshold = 1e9;
  uint64_t sum = 0, search_time = 0;
  begin_ts = base::GetTimestamp() / 1000;
  if (config->reduce_occupancy_strategy == 1) {
    // Returned score = expire_timet（uint32, in seconds）
    left_threshold = base::GetTimestamp() / 1000 / 1000;
    right_threshold = 5e9;
    // If kScoreBucketNum = 32768, and every 1 minute's data will be put into
    // the same bucket, then we can cover about 22.75 days' data. If all data
    // will not expire in the future 22.75 days, the expiration mechanism will
    // fail.
    bucket_interval = 60.0;
  }
  auto calc_score_func = [&](base::KVData *data) {
    if (config->reduce_occupancy_strategy == 0) {
      NodeAttr attr;
      updater->Adaptor()->GetEdgeListInfo(data->data(), &attr, nullptr);
      return static_cast<float>(attr.degree());
    }
    return static_cast<float>(data->expire_timet);
  };
  int loop_time = 0;
  while (sum < key_num_threshold) {
    for (size_t i = 0; i < kScoreBucketNum; ++i) {
      buckets[i] = 0;
    }
    for (int part_i = 0; part_i < mem_kv->PartNum(); ++part_i) {
      mem_kv->EraseIf(part_i, [&](uint64 key, base::KVData *data) {
        float score = calc_score_func(data);
        if (score >= left_threshold && score < right_threshold) {
          float bucket_score = (score - left_threshold) / bucket_interval;
          int bucket_number =
              std::min(bucket_score, static_cast<float>(kScoreBucketNum - 1));
          bucket_number = std::max(0, bucket_number);
          buckets[bucket_number]++;
        }
        return false;
      });
    }
    int i = kScoreBucketNum - 1;
    while (i > 0 && sum + buckets[i] < key_num_threshold) {
      sum += buckets[i--];
    }
    if (++search_time >= kMaxSearchTime) {
      threshold_score = left_threshold + (i + 1) * bucket_interval + 0.01;
      break;
    }
    left_threshold = left_threshold + i * bucket_interval;
    if (i != kScoreBucketNum - 1) {
      right_threshold = left_threshold + bucket_interval;
      bucket_interval /= kScoreBucketNum;
    }
    ++loop_time;
    base::SleepForSeconds(1);
  }

  uint64 max_erase_num =
      mem_kv->ValidKeyNum() *
      (config->reduce_occupancy_rate / mem_kv->ValidOccupancyRate());
  std::atomic<uint64_t> erase_sum(0);
  int total_erase_num = 0;
  for (int part_i = 0; part_i < mem_kv->PartNum(); ++part_i) {
    erase_sum = 0;
    total_erase_num += mem_kv->EraseIf(
        part_i,
        [&, max_erase_num, threshold_score](uint64 key, base::KVData *data) {
          float score = calc_score_func(data);
          return score < threshold_score &&
                 erase_sum++ < max_erase_num / mem_kv->PartNum();
        });
  }

  base::SleepForSeconds(2);
  auto use_ms = base::GetTimestamp() / 1000 - begin_ts;
  PERF_SUM(total_erase_num, config->relation_name, "reduce_db.erase_num");
  LOG(INFO) << config->relation_name
            << " Before occupancy_rate: " << occupancy_rate
            << ", key_num: " << key_num
            << ", threshold_score: " << std::setprecision(12) << threshold_score
            << ", erase_num: " << total_erase_num << ". use_ms: " << use_ms
            << ". loop_time: " << loop_time;
}

void GraphKV::DetectionDb(std::string relation) {
  while (!stop_) {
    auto db = dbs_[relation].get();
    if (!db) {
      continue;
    }
    auto updater = updaters_[relation].get();
    LOG_EVERY_N_SEC(INFO, 600)
        << "Relation " << relation << " info: " << db->GetInfo();
    ReduceDbMemory(db_configs_[relation], db, updater);
    for (size_t i = 0; i < absl::GetFlag(FLAGS_detect_db_interval_in_seconds);
         ++i) {
      if (stop_) {
        break;
      }
      base::SleepForSeconds(1);
    }
  }
}

void GraphKV::TimeDecayThread(std::string relation_name) {
  auto db_kernel = GetKVByType(dbs_, relation_name);
  CHECK(db_kernel);
  while (!stop_) {
    auto mem_kv = dbs_[relation_name].get();
    if (!mem_kv || !db_configs_[relation_name]->need_time_decay()) {
      LOG_EVERY_N_SEC(INFO, 300)
          << "TimeDecayThread for " << relation_name << " has nothing to do.";
      for (size_t i = 0; i < 5; ++i) {
        if (stop_) {
          break;
        }
        base::SleepForSeconds(1);
      }
      continue;
    }
    auto updater = updaters_[relation_name].get();
    uint64 interval = db_configs_[relation_name]->decay_interval_s * 1000000ul;
    uint64 next_ts = base::GetTimestamp() / interval * interval + interval;
    while (base::GetTimestamp() < next_ts) {
      if (stop_) return;
      LOG_EVERY_N_SEC(INFO, 60)
          << "TimeDecayThread for " << relation_name
          << " waiting, remaining seconds: "
          << ConvertTimestampToSec(next_ts) -
                 ConvertTimestampToSec(base::GetTimestamp());
      base::SleepForSeconds(1);
    }
    int delete_count = 0;
    for (int part_i = 0; part_i < mem_kv->PartNum(); ++part_i) {
      mem_kv->TraverseOp(part_i, [&](uint64 key, base::KVData *data) {
        WRITE_AUTO_LOCK_BY_RELATION_AND_KEY(key);
        mem_kv->Update(
            key, nullptr, 0, 0,
            [&](uint32 old_addr, uint64 key, const char *dummy, int zero_size,
                int dummy_t, base::MemKV *mem_kv) -> uint32 {
              return updater->TimeDecayHandler(
                  old_addr, key, &delete_count,
                  db_configs_[relation_name]->degree_decay_ratio,
                  db_configs_[relation_name]->weight_decay_ratio,
                  db_configs_[relation_name]->delete_threshold_weight, mem_kv);
            },
            nullptr,
            [&](uint64 cur_key, base::KVData *data) -> void {
              updater->RecycleHandler(cur_key, data);
            });
        return false;
      });
    }
    LOG(INFO) << "TimeDecayThread for " << relation_name
              << " delete edge num: " << delete_count;
    PERF_SUM(delete_count, relation_name, "time_decay.delete_count");
  }
}

// Similar to TimeDecayThread
void GraphKV::EdgeExpireThread(std::string relation_name) {
  auto db_kernel = GetKVByType(dbs_, relation_name);
  CHECK(db_kernel);
  while (!stop_) {
    auto mem_kv = dbs_[relation_name].get();
    if (!mem_kv || !db_configs_[relation_name]->need_edge_expire()) {
      LOG_EVERY_N_SEC(INFO, 300)
          << "EdgeExpireThread for " << relation_name << " has nothing to do.";
      for (size_t i = 0; i < 5; ++i) {
        if (stop_) {
          break;
        }
        base::SleepForSeconds(1);
      }
      continue;
    }
    auto updater = updaters_[relation_name].get();
    uint64 interval =
        db_configs_[relation_name]->edge_expire_check_interval_s * 1000000ul;
    uint64 next_ts = base::GetTimestamp() / interval * interval + interval;
    while (base::GetTimestamp() < next_ts) {
      if (stop_) return;
      LOG_EVERY_N_SEC(INFO, 60)
          << "EdgeExpireThread for " << relation_name
          << " waiting, remaining seconds: "
          << ConvertTimestampToSec(next_ts) -
                 ConvertTimestampToSec(base::GetTimestamp());
      base::SleepForSeconds(1);
    }
    int delete_count = 0;
    for (int part_i = 0; part_i < mem_kv->PartNum(); ++part_i) {
      mem_kv->TraverseOp(part_i, [&](uint64 key, base::KVData *data) {
        WRITE_AUTO_LOCK_BY_RELATION_AND_KEY(key);
        mem_kv->Update(
            key, nullptr, 0, 0,
            [&](uint32 old_addr, uint64 key, const char *dummy, int zero_size,
                int dummy_t, base::MemKV *mem_kv) -> uint32 {
              return updater->EdgeExpireHandler(
                  old_addr, key, &delete_count,
                  db_configs_[relation_name]->edge_expire_interval_s, mem_kv);
            },
            nullptr,
            [&](uint64 cur_key, base::KVData *data) -> void {
              updater->RecycleHandler(cur_key, data);
            });
        return false;
      });
    }
    LOG(INFO) << "EdgeExpireThread for " << relation_name
              << " delete edge num: " << delete_count;
    PERF_SUM(delete_count, relation_name, "edge_expire.delete_count");
  }
}

bool GraphKV::Cleanup() {
  for (const auto &pair : dbs_) {
    auto mem_kv = pair.second.get();
    for (int part_i = 0; part_i < mem_kv->PartNum(); ++part_i) {
      mem_kv->EraseIf(part_i, [](uint64 key, base::KVData *) { return true; });
    }
  }
  return true;
}

static base::MultiMemKV *GetKVByType(
    const std::unordered_map<std::string, std::unique_ptr<base::MultiMemKV>>
        &db,
    std::string relation_name) {
  if (db.find(relation_name) != db.end()) {
    return db.at(relation_name).get();
  }
  return nullptr;
}

}  // namespace chrono_graph
