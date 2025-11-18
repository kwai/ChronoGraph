// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <string>
#include <utility>
#include <vector>
#include "base/common/basic_types.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"

namespace chrono_graph {

template <typename T>
using ProtoList = ::google::protobuf::RepeatedField<T>;
// RepeatedPtrField is like RepeatedField, but used for repeated strings or Messages.
template <typename T>
using ProtoPtrList = ::google::protobuf::RepeatedPtrField<T>;

typedef std::function<void(ProtoList<uint64>)> IDProcFn;
typedef std::function<void(ProtoPtrList<NodeAttr>)> NodesAttrProcFn;
typedef std::function<void(ProtoList<uint64>, ProtoPtrList<NodeAttr>)> IDWithNodesAttrProcFn;
typedef std::function<void(const EdgeList &)> NodesProcFn;
typedef std::function<void(const EdgeInfo &)> EdgesProcFn;

struct EdgeListSlice {
  void Add(uint64 nid, uint32 time = 0, double weight = 0, const std::string &attr = "") {
    node_id->Set(offset, nid);
    timestamp_s->Set(offset, time);
    edge_weight->Set(offset, weight);
    *(edge_attr->Mutable(offset)) = std::move(attr);
    offset++;
  }
  int offset;
  ProtoList<uint64> *node_id;
  ProtoList<uint32> *timestamp_s;
  ProtoList<double> *edge_weight;
  ProtoPtrList<std::string> *edge_attr;
};

class EdgeListWriter {
 public:
  EdgeListWriter() = delete;

  EdgeListWriter(EdgeList *list, int length) : list_(list) {
    node_id_ = list->mutable_node_id();
    timestamp_s_ = list->mutable_timestamp_s();
    edge_weight_ = list->mutable_edge_weight();
    edge_attr_ = list->mutable_edge_attrs();
    Resize(length);
  }
  explicit EdgeListWriter(EdgeList *list) : EdgeListWriter(list, 0) {}

  void Resize(int length) {
    node_id_->Clear();
    timestamp_s_->Clear();
    edge_weight_->Clear();
    edge_attr_->Clear();
    node_id_->Resize(length, 0);
    timestamp_s_->Resize(length, 0);
    edge_weight_->Resize(length, 0.0f);
    edge_attr_->Reserve(length);
    for (size_t i = 0; i < length; ++i) { edge_attr_->Add(); }
  }

  EdgeListSlice Slice(int offset) {
    return {.offset = offset,
            .node_id = node_id_,
            .timestamp_s = timestamp_s_,
            .edge_weight = edge_weight_,
            .edge_attr = edge_attr_};
  }

 private:
  EdgeList *list_ = nullptr;
  ProtoList<uint64> *node_id_ = nullptr;
  ProtoList<uint32> *timestamp_s_ = nullptr;
  ProtoList<double> *edge_weight_ = nullptr;
  ProtoPtrList<std::string> *edge_attr_ = nullptr;
};

}  // namespace chrono_graph
