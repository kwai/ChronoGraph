// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/client/rgraph_client.h"

#include <grpcpp/grpcpp.h>
#include "absl/synchronization/mutex.h"
#include "base/common/gflags.h"
#include "base/common/string.h"
#include "base/common/timer.h"
#include "base/hash/hash.h"
#include "base/random/pseudo_random.h"
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/client/sharding_rgraph_client.h"

namespace chrono_graph {

std::unique_ptr<RGraphClient> RGraphClient::NewInstance(const std::string &type) {
  if (type == "remote") { return std::make_unique<RemoteShardingRGraphClient>(); }
  NOT_REACHED() << "unsupported client type: " << type;
  return nullptr;
}

bool RemoteRGraphClient::Init(const base::Json *config) {
  RGraphClient::Init(config);
  service_name_ = config->GetString("gnn_service", "");
  CHECK(!service_name_.empty()) << "gnn_service is empty, RemoteRGraphClient Init fail, config: "
                                << config->ToString();
  client_name_ = absl::StrFormat("R-%s", relation_name_.data());
  shard_id_ = config->GetInt("shard_id", (int64)shard_id_);
  LOG(INFO) << "service_name = " << service_name_ << ", client name = " << client_name_
            << ", shard_id = " << shard_id_;
  // Initialize stubs using ip_ports info from config.
  std::vector<std::string> ip_ports =
      chrono_graph::RouteTableJsonConfig::GetInstance()->GetRouteConfig(relation_name_, shard_id_);
  all_stubs_.resize(ip_ports.size());
  for (size_t i = 0; i < ip_ports.size(); ++i) {
    all_stubs_[i] = std::make_unique<chrono_graph::GNNKVService::Stub>(
        grpc::CreateChannel(ip_ports[i], grpc::InsecureChannelCredentials()));
  }
  return true;
}

bool RemoteRGraphClient::BatchInsert(const BatchInsertInfo &batch_insert_info) {
  base::Timer timer;
  if (batch_insert_info.src_ids.empty()) { return true; }
  if (!batch_insert_info.src_attrs.empty()) {
    CHECK_EQ(batch_insert_info.src_ids.size(), batch_insert_info.src_attrs.size());
  }
  if (!batch_insert_info.dest_ids.empty()) {
    CHECK_EQ(batch_insert_info.src_ids.size(), batch_insert_info.dest_ids.size());
    // Without input weight, edge list will generate random weight for it
    if (batch_insert_info.dest_weights.size() > 0) {
      CHECK_EQ(batch_insert_info.dest_ids.size(), batch_insert_info.dest_weights.size());
    }
    if (batch_insert_info.expire_ts_update_flags.size() > 0) {
      CHECK_EQ(batch_insert_info.dest_ids.size(), batch_insert_info.expire_ts_update_flags.size());
    }
    if (batch_insert_info.use_item_ts_for_expires.size() > 0) {
      CHECK_EQ(batch_insert_info.src_ids.size(), batch_insert_info.use_item_ts_for_expires.size());
    }
  }
  thread_local BatchInsertRequest request;
  thread_local BatchInsertResponse response;
  request.Clear();
  response.Clear();
  request.set_relation_name(relation_name_);
  request.set_iewa(batch_insert_info.iewa);
  size_t total_src_id_size = batch_insert_info.src_ids.size();
  request.mutable_src_ids()->Reserve(total_src_id_size);
  for (size_t i = 0; i < total_src_id_size; ++i) {
    request.mutable_src_ids()->AddAlreadyReserved(batch_insert_info.src_ids[i]);
  }

  for (size_t i = 0; i < total_src_id_size; ++i) {
    if (batch_insert_info.src_attrs.empty()) { break; }
    request.mutable_attrs()->Reserve(total_src_id_size);
    if (i == total_src_id_size - 1) {
      *(request.mutable_attrs()->Add()) = batch_insert_info.src_attrs[i];
    } else {
      *(request.mutable_attrs()->Add()) = (batch_insert_info.src_ids[i] != batch_insert_info.src_ids[i + 1])
                                              ? batch_insert_info.src_attrs[i]
                                              : "";
    }
  }

  for (size_t i = 0; i < total_src_id_size; ++i) {
    if (batch_insert_info.dest_ids.empty()) { break; }
    request.mutable_dest_ids()->Reserve(total_src_id_size);
    if (batch_insert_info.dest_weights.size() > 0) {
      request.mutable_dest_weights()->Reserve(total_src_id_size);
      request.mutable_dest_weights()->AddAlreadyReserved(batch_insert_info.dest_weights[i]);
    }
    if (batch_insert_info.dst_ts.size() > 0) {
      request.mutable_dst_ts()->Reserve(total_src_id_size);
      request.mutable_dst_ts()->AddAlreadyReserved(batch_insert_info.dst_ts[i]);
    }
    if (batch_insert_info.dest_attrs.size() > 0) {
      request.mutable_edge_attrs()->Reserve(total_src_id_size);
      *(request.mutable_edge_attrs()->Add()) = batch_insert_info.dest_attrs[i];
    }
    if (batch_insert_info.expire_ts_update_flags.size() > 0) {
      request.mutable_expire_ts_update_flags()->Reserve(total_src_id_size);
      *(request.mutable_expire_ts_update_flags()->Add()) = batch_insert_info.expire_ts_update_flags[i];
    }
    if (batch_insert_info.use_item_ts_for_expires.size() > 0) {
      request.mutable_use_item_ts_for_expires()->Reserve(total_src_id_size);
      *(request.mutable_use_item_ts_for_expires()->Add()) = batch_insert_info.use_item_ts_for_expires[i];
    }
    request.mutable_dest_ids()->AddAlreadyReserved(batch_insert_info.dest_ids[i]);
  }

  timer.AppendCostMs("build insert request");
  RequestAllStubs([&](chrono_graph::GNNKVService::Stub *stub) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(batch_insert_info.timeout_ms));
    grpc::Status status = stub->BatchInsert(&context, request, &response);
    if (!status.ok()) {
      LOG(ERROR) << "RPC failed code " << status.error_code() << ": " << status.error_message()
                 << ". Response ErrorCode = " << response.code();
    }
  });
  timer.AppendCostMs("insert request");
  LOG_EVERY_N_SEC(INFO, 10) << "batch insert from " << service_name_ << ", client: " << client_name_
                            << ", cost: " << timer.display();
  return true;
}

bool RemoteRGraphClient::BatchDelete(const BatchInsertInfo &batch_insert_info) {
  base::Timer timer;
  if (!batch_insert_info.dest_ids.empty()) {
    CHECK_EQ(batch_insert_info.src_ids.size(), batch_insert_info.dest_ids.size());
  }
  thread_local BatchInsertRequest request;
  thread_local BatchInsertResponse response;
  request.Clear();
  response.Clear();
  request.set_relation_name(relation_name_);
  request.set_is_delete(true);
  size_t total_src_id_size = batch_insert_info.src_ids.size();
  request.mutable_src_ids()->Reserve(total_src_id_size);
  for (size_t i = 0; i < total_src_id_size; ++i) {
    request.mutable_src_ids()->AddAlreadyReserved(batch_insert_info.src_ids[i]);
  }

  request.mutable_dest_ids()->Reserve(total_src_id_size);
  for (size_t i = 0; i < total_src_id_size; ++i) {
    if (batch_insert_info.dest_ids.empty()) { break; }
    request.mutable_dest_ids()->AddAlreadyReserved(batch_insert_info.dest_ids[i]);
  }
  timer.AppendCostMs("build delete request");
  RequestAllStubs([&](chrono_graph::GNNKVService::Stub *stub) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(batch_insert_info.timeout_ms));
    grpc::Status status = stub->BatchDelete(&context, request, &response);
    if (!status.ok()) {
      LOG(ERROR) << "RPC failed code " << status.error_code() << ": " << status.error_message()
                 << ". Response ErrorCode = " << response.code();
    }
  });
  timer.AppendCostMs("delete request");
  LOG_EVERY_N_SEC(INFO, 10) << "batch delete from " << service_name_ << ", client: " << client_name_
                            << ", cost: " << timer.display();
  return true;
}

bool RemoteRGraphClient::SampleNodes(SampleNodeParam param, IDWithNodesAttrProcFn proc_fn) {
  thread_local RandomSampleRequest request;
  thread_local RandomSampleResponse response;
  request.Clear();
  response.Clear();
  request.set_node_count(param.count);
  request.set_relation_name(relation_name_);
  request.set_need_attr(param.need_attr);
  request.set_pool_name(param.pool_name);
  base::Timer timer;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(param.timeout_ms));
  grpc::Status status = GetOneStub()->RandomSample(&context, request, &response);
  timer.AppendCostMs("SampleNodesRpc");
  if (!status.ok() || response.ids_size() != param.count) {
    LOG_EVERY_N_SEC(WARNING, 5) << "rpc call failed:" << service_name_ << ", client:" << client_name_
                                << ", request:" << param.count << ", returned:" << response.ids_size()
                                << ", error_msg:" << status.error_message() << ", cost:" << timer.display();
    return false;
  }
  LOG_EVERY_N_SEC(INFO, 10) << "sample nodes from: " << service_name_ << ", client:" << client_name_
                            << ", count:" << param.count << ", cost:" << timer.display();
  proc_fn(response.ids(), response.nodes());
  return true;
}

bool RemoteRGraphClient::SampleNeighbors(const IdVec &source_nodes,
                                         SampleNeighborParam param,
                                         NodesProcFn proc_fn) {
  thread_local chrono_graph::SampleNeighborRequest request;
  thread_local chrono_graph::SampleNeighborResponse response;
  request.Clear();
  request.set_relation_name(relation_name_);
  auto request_param = request.mutable_sample_param();
  request_param->set_sampling_num(param.fanout);
  request_param->set_strategy(param.sample_type);
  request_param->set_sampling_without_replacement(param.sampling_without_replacement);
  request_param->set_max_timestamp_s(param.max_timestamp_s);
  request_param->set_min_timestamp_s(param.min_timestamp_s);
  request_param->set_min_weight_required(param.min_weight_required);
  request.mutable_node_id()->Reserve(source_nodes.size());
  for (auto id : source_nodes) { request.add_node_id(id); }
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(param.timeout_ms));

  auto future = GetOneStub()->SampleNeighbor(&context, request, &response);
  SampleNeighborsAsyncDone(&response, source_nodes, param, proc_fn);
  return true;
}

bool RemoteRGraphClient::SampleNeighborsAsyncDone(chrono_graph::SampleNeighborResponse *response,
                                                  const IdVec &source_nodes,
                                                  SampleNeighborParam param,
                                                  NodesProcFn proc_fn) {
  if (response == nullptr) {
    LOG_EVERY_N_SEC(WARNING, 5) << "response is null";
    return false;
  }
  if (response->edge_list().node_id_size() != param.fanout * source_nodes.size()) {
    LOG_EVERY_N_SEC(WARNING, 5) << "rpc call failed:" << service_name_ << ", client:" << client_name_
                                << ", request:" << source_nodes.size() * param.fanout
                                << ", returned:" << response->edge_list().node_id_size();
    return false;
  }

  if (param.padding_strategy == param.LOOP_NEIGHBOR) {
    for (size_t i = 0; i < source_nodes.size(); ++i) {
      auto source_id = source_nodes[i];
      auto neighbor_offset = i * param.fanout;
      // Edge case: if a node has no neighbor, use SELF_ON_EMPTY instead.
      auto source_start_neighbor = response->edge_list().node_id(neighbor_offset);
      if (source_start_neighbor == 0) {
        response->mutable_edge_list()->set_node_id(neighbor_offset, source_id);
      }
      int padding_follow_offset = i * param.fanout;
      for (int j = 1; j < param.fanout; ++j) {
        if (response->edge_list().node_id(++neighbor_offset) == 0) {
          response->mutable_edge_list()->set_node_id(neighbor_offset,
                                                     response->edge_list().node_id(padding_follow_offset));
          response->mutable_edge_list()->set_timestamp_s(
              neighbor_offset, response->edge_list().timestamp_s(padding_follow_offset));
          response->mutable_edge_list()->set_edge_weight(
              neighbor_offset, response->edge_list().edge_weight(padding_follow_offset));
          if (response->edge_list().edge_attrs_size() > 0) {
            response->mutable_edge_list()->set_edge_attrs(
                neighbor_offset, response->edge_list().edge_attrs(padding_follow_offset));
          }
          padding_follow_offset++;
        }
      }
    }
  } else if (param.padding_strategy == param.SELF_ON_EMPTY) {
    for (size_t i = 0; i < source_nodes.size(); ++i) {
      auto source_id = source_nodes[i];
      for (int j = 0; j < param.fanout; ++j) {
        if (response->edge_list().node_id(i * param.fanout + j) == 0) {
          response->mutable_edge_list()->set_node_id(i * param.fanout + j, source_id);
          // timestamp or weight has no meaning and will not be filled
        }
      }
    }
  } else if (param.padding_strategy == param.NO_PADDING) {
    // do nothing here is no padding, according to EdgeListWriter's constuct logic
  }

  proc_fn(response->edge_list());

  LOG_EVERY_N_SEC(INFO, 10) << "client " << client_name_ << ", source:" << source_nodes.size()
                            << ", fanout:" << param.fanout;
  return true;
}

int64 RemoteRGraphClient::NodeCount() {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(5000));
  thread_local GetRelationNodeCountRequest request;
  thread_local GetRelationNodeCountResponse response;
  request.Clear();
  response.Clear();
  request.set_relation_name(relation_name_);
  grpc::Status status = GetOneStub()->GetRelationNodeCount(&context, request, &response);
  if (!status.ok()) {
    LOG_EVERY_N_SEC(INFO, 5) << "GetRelationNodeCount rpc status error " << status.error_message();
    return 0;
  }
  return response.count();
}

bool RemoteRGraphClient::GetEdgesInfo(const IdVec &src_ids,
                                      const IdVec &dest_ids,
                                      EdgesProcFn proc_fn,
                                      const int64 timeout_ms) {
  NOT_REACHED() << "not implementd yet";
  return true;
}

bool RemoteRGraphClient::GetNodesAttr(const IdVec &ids,
                                      GetNodesAttrResponse *response,
                                      const int64 timeout_ms) {
  base::Timer timer;
  if (ids.empty()) {
    LOG_EVERY_N_SEC(WARNING, 2) << "GetNodesAttr get empty input ids.";
    return false;
  }
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  thread_local GetNodesAttrRequest request;
  request.Clear();
  for (auto &id : ids) { request.add_ids(id); }
  request.set_relation_name(relation_name_);
  timer.AppendCostMs("build lookup nodes request");
  grpc::Status status = GetOneStub()->GetNodesAttr(&context, request, response);
  if (!status.ok()) {
    LOG_EVERY_N_SEC(INFO, 2) << "GetNodesAttr error: " << status.error_details()
                             << "; service = " << service_name_ << ", relation_name = " << relation_name_
                             << ", ids = " << absl::StrJoin(ids, ",");
    return false;
  }
  timer.AppendCostMs("lookup request");
  return true;
}

}  // namespace chrono_graph
