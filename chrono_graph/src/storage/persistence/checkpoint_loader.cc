// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/persistence/checkpoint_loader.h"

#include <algorithm>
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/storage/edge_list/cpt_edge_list.h"
#include "chrono_graph/src/storage/edge_list/el_interface.h"

ABSL_DECLARE_FLAG(int32, cpt_dynamic_growth_base);

namespace chrono_graph {

CheckpointLoader::CheckpointLoader(const GraphStorageDBConfig &db_config, const base::Json *conf_json) {
  CHECK(conf_json);

  std::string new_elst = db_config.elst;
  int new_edge_max_num = db_config.edge_max_num;
  new_attr_operator_ = BlockStorageApi::NewInstance(db_config.attr_op_config);
  new_edge_attr_operator_ = BlockStorageApi::NewInstance(db_config.edge_attr_op_config);

  std::string old_elst = conf_json->GetString("elst", new_elst);
  int old_edge_max_num = conf_json->GetInt("edge_max_num", new_edge_max_num);
  delta_expire_s_ = conf_json->GetInt("delta_expire_s", 0);

  base::Json *target_config = db_config.attr_op_config;
  if (conf_json->Get("attr_op_config")) { target_config = conf_json->Get("attr_op_config"); }
  old_attr_operator_ = BlockStorageApi::NewInstance(target_config);

  target_config = db_config.edge_attr_op_config;
  if (conf_json->Get("edge_attr_op_config")) { target_config = conf_json->Get("edge_attr_op_config"); }
  old_edge_attr_operator_ = BlockStorageApi::NewInstance(target_config);

  EdgeListConfig config;
  config.relation_name = db_config.relation_name;
  config.oversize_replace_strategy =
      static_cast<EdgeListReplaceStrategy>(db_config.oversize_replace_strategy);
  config.edge_capacity_max_num = old_edge_max_num;
  old_adaptor_ = EdgeListInterface::NewInstance(
      old_elst, old_attr_operator_.get(), old_edge_attr_operator_.get(), config);
  config.edge_capacity_max_num = new_edge_max_num;
  new_adaptor_ = EdgeListInterface::NewInstance(
      new_elst, new_attr_operator_.get(), new_edge_attr_operator_.get(), config);

  size_list_.clear();
  int size = absl::GetFlag(FLAGS_cpt_dynamic_growth_base);
  while (size <= new_edge_max_num) {
    size_list_.emplace_back(size);
    auto old_size = size;
    size = std::min((uint32_t)new_edge_max_num,
                    (uint32_t)(size * absl::GetFlag(FLAGS_cpt_dynamic_growth_factor)));
    // Make sure size keeps increasing, prevent infinite loop.
    size += (old_size == size);
  }
}

char *CheckpointLoader::ParseAppend(
    const char *el_data, uint64 *key, int *size, uint32 *expire_timet, std::string *result) {
  // Extract node_attr and edge info.
  NodeAttr node_attr;
  ProtoPtrList<EdgeInfo> edge_info;
  old_adaptor_->GetEdgeListInfo(el_data, &node_attr, &edge_info);

  CommonUpdateItems items;
  items.src_id = *key;
  items.iewa = chrono_graph::ADD;
  items.attr_update_info.resize(new_attr_operator_->MaxSize());

  std::vector<int64> int_attrs;
  std::vector<float> float_attrs;
  old_attr_operator_->GetAll(node_attr.attr_info().data(), &int_attrs, &float_attrs);
  char *update_info = const_cast<char *>(items.attr_update_info.data());
  new_attr_operator_->InitialBlock(update_info, new_attr_operator_->MaxSize());
  new_attr_operator_->SetAll(update_info, &int_attrs, &float_attrs);

  // Find correspond slab's item num.
  int true_size = absl::GetFlag(FLAGS_cpt_dynamic_growth_base);
  for (size_t i = 0; i < size_list_.size(); ++i) {
    true_size = size_list_[i];
    if (size_list_[i] >= edge_info.size()) { break; }
  }

  uint32_t length = new_adaptor_->GetMemorySize(true_size);
  result->resize(length);
  char *data_ptr = const_cast<char *>(result->data());

  LOG_EVERY_N_SEC(INFO, 1) << "key: " << *key << ", edge_num: " << edge_info.size()
                           << ", slab item num: " << true_size
                           << ", slab size: " << new_adaptor_->GetMemorySize(true_size)
                           << ". Prev size: " << *size << ", new size: " << length;
  if (edge_info.size() == 0) {
    LOG_EVERY_N_SEC(WARNING, 1) << "Got empty edge list. Some config might be wrong.";
  }
  *size = length;

  *expire_timet = std::max(0, (int32)(*expire_timet) + delta_expire_s_);

  for (size_t i = 0; i < edge_info.size(); ++i) {
    auto edge = edge_info.Get(i);
    items.AddEdge(edge.id(), edge.weight(), edge.timestamp(), edge.edge_attr());
  }
  new_adaptor_->FillRaw(data_ptr, true_size, items);

  return data_ptr;
}

}  // namespace chrono_graph
