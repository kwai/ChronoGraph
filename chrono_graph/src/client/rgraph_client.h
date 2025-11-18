// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "base/common/timer.h"
#include "base/hash/hash.h"
#include "base/jansson/json.h"
#include "base/random/pseudo_random.h"
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/base/gnn_type.h"
#include "chrono_graph/src/base/proto_helper.h"
#include "chrono_graph/src/proto/gnn_kv_service.grpc.pb.h"

namespace chrono_graph {

// Iterate all nodes in a relation. All interface should be thread safe.
class NodesIterator {
 public:
  virtual ~NodesIterator() {}
  virtual bool HasNext() const = 0;
  virtual void Next(IDProcFn proc_fn) = 0;
  // reset iterator to offset = 0.
  virtual void Reset() = 0;

  int GetPartNum() { return part_num_; }

 protected:
  // Split the whole dataset
  int64 part_num_ = 0;
};

struct SampleNodeParam {
  int count;
  bool need_attr;  // whether get node_attr
  int64 timeout_ms = 5000;
  // custom sample pool to use. Use global sample pool on default empty name.
  std::string pool_name = "";
};

// Relation-GraphClient: each RGraphClient corresponds to a relation.
// It provides Graph Storage API from algorithm perspective, hiding storage system details.
// RGraphClient maintains some status, and interface should be thread safety.
// We assume id = 0 is the invalid node.
class RGraphClient {
 public:
  virtual ~RGraphClient() {}
  virtual bool Init(const base::Json *config) {
    LOG(INFO) << "RGraphClient Init...";
    CHECK(config) << "RGraphClient init config is null, check config file";
    relation_name_ = config->GetString("relation_name", "");
    return true;
  }

  // Insert some edges into graph.
  // \param batch_insert_info `src_ids` and `dest_ids` must have the same length.
  virtual bool BatchInsert(const BatchInsertInfo &batch_insert_info) = 0;

  // \brief Delete some edges from graph.
  // \param batch_insert_info `src_ids` and `dest_ids` are required.
  virtual bool BatchDelete(const BatchInsertInfo &batch_insert_info) = 0;

  // Given src and dest IDs, query edges' info, and use proc_fn to process them.
  // If `src_ids` length > 1, then should ensure src_ids.size() == dest_ids.size()
  virtual bool GetEdgesInfo(const IdVec &src_ids,
                            const IdVec &dest_ids,
                            EdgesProcFn proc_fn,
                            const int64 timeout_ms) = 0;

  // Sample some nodes randomly, and use proc_fn to process them.
  virtual bool SampleNodes(SampleNodeParam param, IDWithNodesAttrProcFn proc_fn) = 0;

  struct SampleNeighborParam {
    // Sampling neighbor num for each node.
    int fanout;
    // Valid sample result may be smaller than expected, padding makes sure the result length is equal to
    // `src_id_size * fanout`, which can be significant for training.
    enum PaddingStrategy {
      ZERO_FILLING,   // append 0 to the result
      SELF_ON_EMPTY,  // append src_id to the result
      LOOP_NEIGHBOR,  // append sampled neighbors in loop to the result
      NO_PADDING      // disable padding
    };
    PaddingStrategy padding_strategy = ZERO_FILLING;
    SamplingParam_SamplingStrategy sample_type;
    bool sampling_without_replacement;
    uint32 max_timestamp_s = 0;
    uint32 min_timestamp_s = 0;
    int64 timeout_ms = 5000;
    float min_weight_required = 0;
  };

  virtual bool SampleNeighbors(const IdVec &source_nodes, SampleNeighborParam param, NodesProcFn proc_fn) {
    NOT_REACHED() << "Unimplemented interface";
    return false;
  }

  // A traverse interface with state, return an iterator to traverse nodes.
  // Do NOT manage the returned pointer's lifetime.
  // \param part_size Node num in each iteration.
  virtual NodesIterator *Traversal(int part_size) = 0;

  // Split the whole graph into shards, and do traversal on the specific shard.
  // Do NOT manage the returned pointer's lifetime.
  // \param part_size Node num in each iteration.
  virtual NodesIterator *ShardedTraversal(int shard_num, int shard_id, int part_size) = 0;

  // Query nodes' attributes, and call fn to process them.
  virtual bool LookupNodes(const IdVec &ids, NodesAttrProcFn fn, const int64 timeout_ms) = 0;

  // For one relation.
  virtual int64 NodeCount() = 0;

  // Should create instance only by this
  static std::unique_ptr<RGraphClient> NewInstance(const std::string &type = "remote");

  static void ResizeEdgeList(const IdVec &source_nodes, const SampleNeighborParam &param, EdgeList *results) {
    results->Clear();
    results->mutable_node_id()->Resize(source_nodes.size() * param.fanout, 0);
    results->mutable_timestamp_s()->Resize(source_nodes.size() * param.fanout, 0);
    results->mutable_edge_weight()->Resize(source_nodes.size() * param.fanout, 0.0f);
    results->mutable_edge_attrs()->Reserve(source_nodes.size() * param.fanout);
    for (size_t i = 0; i < source_nodes.size() * param.fanout; ++i) { results->mutable_edge_attrs()->Add(); }
    results->mutable_node_attrs()->Reserve(source_nodes.size());
    for (size_t i = 0; i < source_nodes.size(); ++i) { results->mutable_node_attrs()->Add(); }
  }

 protected:
  void RequestAllStubs(std::function<void(chrono_graph::GNNKVService::Stub *)> func) {
    NOT_REACHED() << "Unimplemented interface";
    return;
  }
  virtual chrono_graph::GNNKVService::Stub *GetOneStub() {
    NOT_REACHED() << "Unimplemented interface";
    return nullptr;
  }

  std::string service_name_;
  std::string relation_name_;
};

// Remote rpc service client for a given relation and shard_id.
// A relation may has multiple replica. Read request a random replica, write request all.
class RemoteRGraphClient : public RGraphClient {
 public:
  bool Init(const base::Json *config) override;

  bool BatchInsert(const BatchInsertInfo &batch_insert_info) override;

  bool BatchDelete(const BatchInsertInfo &batch_insert_info) override;

  bool SampleNodes(SampleNodeParam param, IDWithNodesAttrProcFn proc_fn) override;

  bool SampleNeighbors(const IdVec &source_nodes, SampleNeighborParam param, NodesProcFn proc_fn) override;
  // Should do padding before calling proc_fn.
  bool SampleNeighborsAsyncDone(SampleNeighborResponse *response,
                                const IdVec &source_nodes,
                                SampleNeighborParam param,
                                NodesProcFn proc_fn);

  bool GetNodesAttr(const IdVec &ids, GetNodesAttrResponse *response, const int64 timeout_ms);

  bool LookupNodes(const IdVec &ids, NodesAttrProcFn func, const int64 timeout_ms) override {
    thread_local GetNodesAttrResponse response;
    response.Clear();
    if (!GetNodesAttr(ids, &response, timeout_ms)) { return false; }
    func(response.nodes());
    return true;
  }

  bool GetEdgesInfo(const IdVec &src_ids,
                    const IdVec &dest_ids,
                    EdgesProcFn proc_fn,
                    const int64 timeout_ms) override;

  int64 NodeCount() override;

  NodesIterator *Traversal(int part_size) override { return new NodesIteratorImpl(this, part_size); }

  NodesIterator *ShardedTraversal(int shard_num, int shard_id, int part_size) override {
    return new NodesIteratorImpl(this, part_size, shard_num, shard_id);
  }

 private:
  class NodesIteratorImpl : public NodesIterator {
   public:
    // \param iter_size: Returned node num in each iteration.
    // \param shard_num: Split the whole graph into shard_num parts.
    // \param shard_id: Iterate in one of the parts
    NodesIteratorImpl(RemoteRGraphClient *client, int iter_size, int shard_num = 1, int shard_id = 0)
        : client_(client), shard_num_(shard_num), shard_id_(shard_id) {
      part_id_ = shard_id;
      int64 total_node_size = client_->NodeCount();
      part_num_ = (total_node_size + iter_size - 1) / iter_size;
      LOG(INFO) << "shard_id:" << shard_id << ", shard_num:" << shard_num;
    }

    bool HasNext() const override { return part_id_.load() < part_num_; }

    void Next(IDProcFn proc_fn) override {
      base::Timer timer;
      int64 part_id = part_id_.fetch_add(shard_num_);
      if (part_id >= part_num_) { return; }
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
      thread_local TraversalByPartRequest request;
      thread_local TraversalByPartResponse response;
      request.Clear();
      response.Clear();
      LOG(INFO) << "traversal part:" << part_id << "/" << part_num_ << ", shard_id:" << shard_id_
                << ", shard_num:" << shard_num_;
      request.set_relation_name(client_->relation_name_);
      request.set_part_id(part_id);
      request.set_part_num(part_num_);
      timer.AppendCostMs("node iterator build request");
      auto rpc_channel = client_->GetOneStub()->TraversalByPart(&context, request, &response);
      timer.AppendCostMs("node iterator request");
      proc_fn(response.ids());
      timer.AppendCostMs("node iterator process response");
      LOG_EVERY_N_SEC(INFO, 10) << "client " << client_->client_name_ << ", cost:" << timer.display();
    }

    void Reset() override {
      absl::MutexLock l(&reset_mutex);
      if (part_id_ >= part_num_) { part_id_.exchange(shard_id_); }
    }

   private:
    RemoteRGraphClient *client_ = nullptr;
    int shard_num_ = 1;
    int shard_id_ = 0;
    std::atomic<int64> part_id_{0};  // State machine
    absl::Mutex reset_mutex;
  };
  friend class NodesIteratorImpl;

 protected:
  void RequestAllStubs(std::function<void(chrono_graph::GNNKVService::Stub *)> func) {
    size_t size = all_stubs_.size();
    if (size == 0) {
      LOG(ERROR) << "No available stub for " << relation_name_;
      return;
    }
    for (size_t i = 0; i < size; ++i) { func(all_stubs_[i].get()); }
  }
  chrono_graph::GNNKVService::Stub *GetOneStub() {
    size_t size = all_stubs_.size();
    if (size == 0) {
      LOG(ERROR) << "No available stub for " << relation_name_;
      return nullptr;
    }
    base::PseudoRandom s_random(base::RandomSeed());
    return all_stubs_[s_random.GetInt(0, size - 1)].get();
  }

  // list of stubs for current shard
  std::vector<std::unique_ptr<chrono_graph::GNNKVService::Stub>> all_stubs_;

  size_t shard_id_ = 0;
  std::string client_name_;
};

}  // namespace chrono_graph
