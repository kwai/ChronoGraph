// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <vector>

#include "base/common/logging.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/base/proto_helper.h"
#include "chrono_graph/src/proto/gnn_kv_service.grpc.pb.h"
#include "chrono_graph/src/storage/config.h"
#include "chrono_graph/src/storage/graph_kv.h"

namespace chrono_graph {

class GNNServiceImpl final : public GNNKVService::Service {
 public:
  virtual ~GNNServiceImpl() { LOG(INFO) << "GNNServiceImpl safe quit."; }

  ::grpc::Status BatchInsert(::grpc::ServerContext *context,
                             const ::chrono_graph::BatchInsertRequest *request,
                             ::chrono_graph::BatchInsertResponse *response) override {
    if (!GraphKV::ValidateBatchInsertRequest(request)) {
      response->set_code(ErrorCode::ARGS_ILLEGAL);
      return ::grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "batch insert request invalid");
    }
    auto code = db_->BatchInsert(request);
    if (code != ErrorCode::OK) {
      response->set_code(code);
      return ::grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "batch insert error");
    }
    response->set_code(code);
    return grpc::Status::OK;
  }

  ::grpc::Status BatchDelete(::grpc::ServerContext *context,
                             const ::chrono_graph::BatchInsertRequest *request,
                             ::chrono_graph::BatchInsertResponse *response) override {
    size_t delete_size = request->src_ids().size();
    if (!request->dest_ids().empty() && (request->dest_ids_size() != delete_size)) {
      LOG_EVERY_N_SEC(WARNING, 5) << "batch delete_size error, src id size = " << delete_size
                                  << ", dst_ids size = " << request->dest_ids_size();
      response->set_code(ErrorCode::ARGS_ILLEGAL);
      return ::grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dest size not match with src_ids");
    }
    auto code = db_->BatchDelete(request);
    response->set_code(code);
    return grpc::Status::OK;
  }

  ::grpc::Status RandomSample(::grpc::ServerContext *context,
                              const ::chrono_graph::RandomSampleRequest *request,
                              ::chrono_graph::RandomSampleResponse *response) override {
    auto node_attr_ptr = response->mutable_nodes();
    if (!request->need_attr()) {
      node_attr_ptr = nullptr;
    } else {
      for (size_t i = 0; i < request->node_count(); ++i) { response->add_nodes(); }
    }
    ErrorCode code = db_->RandomSample(request->relation_name(),
                                       request->node_count(),
                                       response->mutable_ids(),
                                       node_attr_ptr,
                                       request->pool_name());
    response->set_code(code);
    switch (code) {
      case ErrorCode::RELATION_NAME_NOT_AVAILABLE:
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "sampling wrong relation type, or server not ready");
      default:
        break;
    }
    return grpc::Status::OK;
  }

  ::grpc::Status SampleNeighbor(::grpc::ServerContext *context,
                                const ::chrono_graph::SampleNeighborRequest *request,
                                ::chrono_graph::SampleNeighborResponse *response) override {
    // Can't get caller service name yet
    std::string caller_service_name = "";

    auto begin = base::GetTimestamp();
    auto result_list = response->mutable_edge_list();
    auto sample_count = request->sample_param().sampling_num();
    int result_length = request->node_id_size() * sample_count;
    EdgeListWriter writer(result_list, result_length);
    response->mutable_degree()->Reserve(request->node_id_size());
    for (size_t i = 0; i < request->node_id_size(); ++i) {
      auto slice = writer.Slice(i * sample_count);
      int64 degree = 0;
      db_->SampleNeighbors(request->node_id(i),
                           request->relation_name(),
                           request->sample_param(),
                           caller_service_name,
                           &slice,
                           &degree);
      response->add_degree(degree);
    }
    auto end = base::GetTimestamp();
    auto relation_name = request->relation_name();
    if (request->node_id_size() > 0) {
      PERF_AVG(end - begin, relation_name, P_RQ_T, "sample_batch");
      PERF_AVG((end - begin) / request->node_id_size(), relation_name, P_RQ_T, "sample_once");
    }
    return grpc::Status::OK;
  }

  ::grpc::Status GetNodesAttr(::grpc::ServerContext *context,
                              const ::chrono_graph::GetNodesAttrRequest *request,
                              ::chrono_graph::GetNodesAttrResponse *response) override {
    response->set_code(
        db_->GetNodeInfos(request->ids(), request->relation_name(), response->mutable_nodes()));
    return grpc::Status::OK;
  }

  ::grpc::Status GetNodeEdges(::grpc::ServerContext *context,
                              const ::chrono_graph::GetNodeEdgesRequest *request,
                              ::chrono_graph::GetNodeEdgesResponse *response) override {
    if (request->src_ids_size() == 0) {
      response->set_code(ErrorCode::OK);
      return grpc::Status::OK;
    }
    std::vector<uint64> tmp_dests;
    if (request->src_ids_size() == 1) {
      response->add_nodes();
      for (auto i = 0; i < request->dest_ids_size(); ++i) { tmp_dests.push_back(request->dest_ids(i)); }
      auto ret = db_->QueryNeighbors(request->src_ids(0),
                                     tmp_dests,
                                     request->relation_name(),
                                     response->mutable_nodes(0),
                                     response->mutable_edges());
      response->set_code(ret);
      return grpc::Status::OK;
    }
    // src_ids_size > 1, then src and dest should in one-to-one correspondence
    if (request->src_ids_size() != request->dest_ids_size()) {
      LOG_EVERY_N_SEC(ERROR, 5) << "src_ids size not match with dest_ids size: " << request->src_ids_size()
                                << " vs " << request->dest_ids_size();
      response->set_code(ErrorCode::UNKNOWN);
      return grpc::Status::OK;
    }
    response->set_code(ErrorCode::OK);
    for (auto i = 0; i < request->src_ids_size(); ++i) {
      response->add_nodes();
      tmp_dests.push_back(request->dest_ids(i));
      // Gather all dests for the same src.
      if ((i < request->src_ids_size() - 1) && (request->src_ids(i) == request->src_ids(i + 1))) { continue; }

      auto ret = db_->QueryNeighbors(request->src_ids(i),
                                     tmp_dests,
                                     request->relation_name(),
                                     response->mutable_nodes(i),
                                     response->mutable_edges());
      tmp_dests.clear();
      if (ret != ErrorCode::OK) {
        response->set_code(ret);
        return grpc::Status::OK;
      }
    }
    return grpc::Status::OK;
  }

  ::grpc::Status TraversalByPart(::grpc::ServerContext *context,
                                 const ::chrono_graph::TraversalByPartRequest *request,
                                 ::chrono_graph::TraversalByPartResponse *response) override {
    if (request->part_id() >= request->part_num()) {
      response->set_code(ErrorCode::ARGS_ILLEGAL);
      LOG(ERROR) << "TraversalByPart arg error, part_num = " << request->part_num()
                 << ", part_id = " << request->part_id();
      return grpc::Status::OK;
    }
    response->set_code(db_->TraversalByPart(
        request->relation_name(), request->part_num(), request->part_id(), response->mutable_ids()));
    return grpc::Status::OK;
  }

  ::grpc::Status GetRelationNodeCount(::grpc::ServerContext *context,
                                      const ::chrono_graph::GetRelationNodeCountRequest *request,
                                      ::chrono_graph::GetRelationNodeCountResponse *response) override {
    int64 count = 0;
    response->set_code(db_->GetRelationNodeCount(request->relation_name(), &count));
    response->set_count(count);
    return ::grpc::Status::OK;
  }

  void InitAndWaitReady() {
    db_ = std::make_unique<chrono_graph::GraphKV>();

    CHECK(db_->Init()) << "gnn db init fail";

    db_->WaitReady();
  }

  GraphKV *db() { return db_.get(); }

 private:
  bool CheckQuota(std::string caller) { return true; }

  std::unique_ptr<GraphKV> db_;
};

}  // namespace chrono_graph
