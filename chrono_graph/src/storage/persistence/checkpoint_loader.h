// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "base/jansson/json.h"
#include "chrono_graph/src/base/block_storage_api.h"
#include "chrono_graph/src/storage/config.h"
#include "chrono_graph/src/storage/edge_list/el_interface.h"

namespace chrono_graph {

// Checkpoint saves data in binary format. If you change configs that determines data structure (e.g., attr_op
// or edge_attr_op), old checkpoint with previous config will be broken. In this case, use CheckpointLoader to
// parse the previous checkpoint in old format, extract all edges, and re-insert them in the current config.
// Thus the server with new edge_list config can load the old checkpoint.
class CheckpointLoader {
 public:
  /**
   * \param dbconfig Current config of a relation
   * \param conf_json Config when checkpoint is saved.
   */
  explicit CheckpointLoader(const GraphStorageDBConfig &db_config, const base::Json *conf_json);

  ~CheckpointLoader() {}
  /**
   * Parse `el_data` and append parsed edges to result.
   * May modify `size` and `expire_timet` during parse.
   * \return pointer to the result edge_list.
   */
  char *ParseAppend(const char *el_data, uint64 *key, int *size, uint32 *expire_timet, std::string *result);

 private:
  std::unique_ptr<BlockStorageApi> new_attr_operator_;
  std::unique_ptr<BlockStorageApi> new_edge_attr_operator_;
  std::unique_ptr<BlockStorageApi> old_attr_operator_;
  std::unique_ptr<BlockStorageApi> old_edge_attr_operator_;
  std::unique_ptr<EdgeListInterface> new_adaptor_;
  std::unique_ptr<EdgeListInterface> old_adaptor_;
  int delta_expire_s_;  // Add extra expire time.

  std::vector<int> size_list_;
  DISALLOW_COPY_AND_ASSIGN(CheckpointLoader);
};

}  // namespace chrono_graph
