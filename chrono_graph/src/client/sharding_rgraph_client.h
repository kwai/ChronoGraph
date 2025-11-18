// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <vector>

#include "base/common/logging.h"
#include "base/hash/hash.h"
#include "base/random/pseudo_random.h"
#include "chrono_graph/src/client/rgraph_client.h"

namespace chrono_graph {
/**
 * \brief Maintains multiple RGraphClients, each corresponds to a shard.
 * Most functions basically divide the input by shard, and use RGraphClients to handle them.
 */
class RemoteShardingRGraphClient : public RGraphClient {
 public:
  bool Init(const base::Json *config) override;

  bool BatchInsert(const BatchInsertInfo &batch_insert_info) override;

  bool BatchDelete(const BatchInsertInfo &batch_insert_info) override;

  bool SampleNodes(SampleNodeParam param, IDWithNodesAttrProcFn proc_fn) override {
    if (sharding_num() == 1) { return shard_clients_[0]->SampleNodes(param, proc_fn); }
    // Pick a random shard for each request
    thread_local base::PseudoRandom random(base::RandomSeed());
    int shard = random.GetInt(0, sharding_num() - 1);
    return shard_clients_[shard]->SampleNodes(param, proc_fn);
  }

  // Things to do here:
  // 1. Split input ids into several batches, each batch is sent to a shard.
  // 2. Fill response, the results need to keep the same order as input ids.
  // 3. Call proc_fn to process the results (usually transfer contents to external variable).
  bool SampleNeighbors(const IdVec &source_nodes, SampleNeighborParam param, NodesProcFn proc_fn) override;

  bool GetEdgesInfo(const IdVec &src_ids,
                    const IdVec &dest_ids,
                    EdgesProcFn proc_fn,
                    const int64 timeout_ms) override;

  int64 NodeCount() override {
    int result = 0;
    for (size_t i = 0; i < sharding_num(); ++i) { result += shard_clients_[i]->NodeCount(); }
    return result;
  }

  bool LookupNodes(const IdVec &ids, NodesAttrProcFn func, const int64 timeout_ms) override;

  NodesIterator *Traversal(int part_size) override {
    if (sharding_num() == 1) { return shard_clients_[0]->Traversal(part_size); }
    return new ShardingNodesIterator(this, part_size);
  }

  NodesIterator *ShardedTraversal(int shard_num, int shard_id, int part_size) override {
    if (sharding_num() == 1) { return shard_clients_[0]->ShardedTraversal(shard_num, shard_id, part_size); }
    return new ShardingNodesIterator(this, part_size, shard_num, shard_id);
  }

  int sharding_pos(uint64 id) { return id % sharding_num(); }
  int sharding_num() { return shard_clients_.size(); }

 private:
  class ShardingNodesIterator : public NodesIterator {
   public:
    // \param part_size Returned node num in each iteration.
    // Not guaranteed the actual result size is equal to `part_size`.
    ShardingNodesIterator(RemoteShardingRGraphClient *client, int part_size) : client_(client) {
      shard_iterators_.resize(client_->sharding_num());
      for (size_t i = 0; i < shard_iterators_.size(); ++i) {
        shard_iterators_[i].reset(client_->shard_clients_[i]->Traversal(part_size));
      }
    }

    // \param part_size: Returned node num in each iteration.
    // \param shard_num: Split the whole graph into shard_num parts.
    // \param shard_id: Iterate in one of the parts
    ShardingNodesIterator(RemoteShardingRGraphClient *client, int part_size, int shard_num, int shard_id)
        : client_(client), shard_num_(shard_num), shard_id_(shard_id) {
      shard_iterators_.resize(client_->sharding_num());
      for (size_t i = 0; i < shard_iterators_.size(); ++i) {
        shard_iterators_[i].reset(
            client_->shard_clients_[i]->ShardedTraversal(shard_num, shard_id, part_size));
      }
    }

    bool HasNext() const override {
      bool has_next = false;
      std::for_each(shard_iterators_.begin(),
                    shard_iterators_.end(),
                    [&](const std::unique_ptr<NodesIterator> &ptr) { has_next |= ptr->HasNext(); });
      return has_next;
    }

    void Next(IDProcFn proc_fn) override {
      for (const auto &iterator : shard_iterators_) {
        if (!iterator->HasNext()) { continue; }
        return iterator->Next(proc_fn);
      }
    }

    void Reset() override {
      std::for_each(shard_iterators_.begin(),
                    shard_iterators_.end(),
                    [](std::unique_ptr<NodesIterator> &ptr) { ptr->Reset(); });
    }

   private:
    RemoteShardingRGraphClient *client_;
    std::vector<std::unique_ptr<NodesIterator>> shard_iterators_;
    int shard_num_ = 1;
    int shard_id_ = 0;
  };
  friend class ShardingNodesIterator;

  std::vector<std::unique_ptr<RemoteRGraphClient>> shard_clients_;
  // restrict the max id num of response, which will affect the request splitting.
  int response_id_batch_size_;
};

}  // namespace chrono_graph
