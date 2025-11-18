// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/edge_list/el_interface.h"
#include <memory>
#include <utility>
#include "chrono_graph/src/storage/edge_list/cdf_edge_list.h"
#include "chrono_graph/src/storage/edge_list/cpt_edge_list.h"

namespace chrono_graph {
std::unique_ptr<EdgeListInterface> EdgeListInterface::NewInstance(std::string type,
                                                                  BlockStorageApi *attr_op,
                                                                  BlockStorageApi *edge_attr_op,
                                                                  const EdgeListConfig &config) {
  if (type == "cdf") { return std::make_unique<CDFAdaptor>(attr_op, edge_attr_op, config); }
  if (type == "cpt") {
    return std::make_unique<CPTreapAdaptor<CPTreapItemStandard>>(attr_op, edge_attr_op, config);
  }
  if (type == "cpt_precise_weight_indexed") {
    return std::make_unique<CPTreapAdaptor<CPTreapItemPreciseWeightIndexed>>(attr_op, edge_attr_op, config);
  }
  NOT_REACHED() << "edge list storage type not support: " << type;
  return std::unique_ptr<EdgeListInterface>();
}

}  // namespace chrono_graph
