// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

namespace chrono_graph {

typedef uint16_t offsetT;

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::TimeRemoveNode(CPTreapItem *node, offsetT offset) {
  CHECK(IsTimeIndexed());
  if (node->prev_offset() != CPTreapItem::invalid_offset) {
    auto time_prev_node = items(node->prev_offset());
    time_prev_node->set_next_offset(node->next_offset());
  }
  if (node->next_offset() != CPTreapItem::invalid_offset) {
    auto time_next_node = items(node->next_offset());
    time_next_node->set_prev_offset(node->prev_offset());
  }
  if (meta.time_head == offset) { meta.time_head = node->next_offset(); }
  if (meta.time_tail == offset) { meta.time_tail = node->prev_offset(); }
}

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::TimeAddNode(CPTreapItem *node,
                                               uint32_t timestamp_s,
                                               offsetT node_offset,
                                               offsetT insert_offset) {
  CHECK(IsTimeIndexed());
  offsetT cursor = insert_offset;
  if (insert_offset == meta.time_tail && meta.size > OPTIMIZE_SIZE_THRESHOLD) {
    FindTsOffset(timestamp_s, node_offset, &cursor);
  }
  int count = 0;
  while (cursor != CPTreapItem::invalid_offset && items(cursor)->timestamp_s() > timestamp_s) {
    cursor = items(cursor)->prev_offset();
    CHECK_LE(++count, meta.size) << "circle detected in time index list!";
  }

  // Added node at the head of the linked list.
  if (cursor == CPTreapItem::invalid_offset) {
    node->set_prev_offset(CPTreapItem::invalid_offset);
    node->set_next_offset(meta.time_head);
    items(meta.time_head)->set_prev_offset(node_offset);
    meta.time_head = node_offset;
    return;
  }
  // Added node at the tail of the linked list.
  if (cursor == meta.time_tail) {
    items(meta.time_tail)->set_next_offset(node_offset);
    node->set_prev_offset(meta.time_tail);
    node->set_next_offset(CPTreapItem::invalid_offset);
    meta.time_tail = node_offset;
    return;
  }
  // Added node at the middle of the linked list.
  auto cursor_next = items(cursor)->next_offset();
  items(cursor)->set_next_offset(node_offset);
  items(cursor_next)->set_prev_offset(node_offset);
  node->set_next_offset(cursor_next);
  node->set_prev_offset(cursor);
}

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::FindTsOffset(uint32 timestamp_s,
                                                offsetT cur_node_offset,
                                                offsetT *cursor) {
  CHECK(IsTimeIndexed());
  if (items(meta.time_tail)->timestamp_s() <= timestamp_s) {
    *cursor = meta.time_tail;
    return;
  }
  int step = floor(sqrt(meta.size));
  CHECK_GT(step, 0);
  offsetT start = meta.time_tail;
  uint32 cur_diff = UINT_MAX;
  for (size_t i = 0; i < meta.size; i += step) {
    // current node is not in time index list
    if (i == cur_node_offset) { continue; }
    int diff = items(i)->timestamp_s() - timestamp_s;
    if (diff < 0) { continue; }
    if (diff < cur_diff) {
      cur_diff = diff;
      start = i;
    }
    if (diff == 0) { break; }
  }
  *cursor = start;
  int count = 0;
  if (cur_diff > 0) {
    while (*cursor != CPTreapItem::invalid_offset && items(*cursor)->timestamp_s() > timestamp_s) {
      *cursor = items(*cursor)->prev_offset();
      CHECK_LE(++count, meta.size) << "circle detected in time index list!";
    }
  } else {  // cur_diff == 0
    while (*cursor != CPTreapItem::invalid_offset && items(*cursor)->timestamp_s() <= timestamp_s) {
      *cursor = items(*cursor)->next_offset();
      CHECK_LE(++count, meta.size) << "circle detected in time index list!";
    }
    CHECK(*cursor < meta.size) << "Unexpected cursor value: " << *cursor << ", start: " << start << ", "
                               << ToString();
    *cursor = items(*cursor)->prev_offset();
  }
}

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::WeightRemoveNode(CPTreapItem *node, offsetT offset) {
  CHECK(IsWeightIndexed());
  if (node->weight_prev_offset() != CPTreapItem::invalid_offset) {
    auto weight_prev_node = items(node->weight_prev_offset());
    weight_prev_node->set_weight_next_offset(node->weight_next_offset());
  }
  if (node->weight_next_offset() != CPTreapItem::invalid_offset) {
    auto weight_next_node = items(node->weight_next_offset());
    weight_next_node->set_weight_prev_offset(node->weight_prev_offset());
  }
  if (meta.weight_head() == offset) { meta.set_weight_head(node->weight_next_offset()); }
  if (meta.weight_tail() == offset) { meta.set_weight_tail(node->weight_prev_offset()); }
}

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::WeightAddNode(CPTreapItem *node, float weight, offsetT node_offset) {
  CHECK(IsWeightIndexed());
  offsetT cursor = meta.weight_tail();
  if (meta.size > OPTIMIZE_SIZE_THRESHOLD) { FindWeightOffset(weight, node_offset, &cursor); }
  int count = 0;
  while (cursor != CPTreapItem::invalid_offset && this->weight(items(cursor)) > weight) {
    cursor = items(cursor)->weight_prev_offset();
    CHECK_LE(++count, meta.size) << "circle detected in weight index list!";
  }

  // Added node at the head of the linked list.
  if (cursor == CPTreapItem::invalid_offset) {
    node->set_weight_prev_offset(CPTreapItem::invalid_offset);
    node->set_weight_next_offset(meta.weight_head());
    items(meta.weight_head())->set_weight_prev_offset(node_offset);
    meta.set_weight_head(node_offset);
    return;
  }
  // Added node at the tail of the linked list.
  if (cursor == meta.weight_tail()) {
    items(meta.weight_tail())->set_weight_next_offset(node_offset);
    node->set_weight_prev_offset(meta.weight_tail());
    node->set_weight_next_offset(CPTreapItem::invalid_offset);
    meta.set_weight_tail(node_offset);
    return;
  }
  // Added node at the middle of the linked list.
  auto cursor_next = items(cursor)->weight_next_offset();
  items(cursor)->set_weight_next_offset(node_offset);
  items(cursor_next)->set_weight_prev_offset(node_offset);
  node->set_weight_next_offset(cursor_next);
  node->set_weight_prev_offset(cursor);
}

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::FindWeightOffset(float weight, offsetT cur_node_offset, offsetT *cursor) {
  CHECK(IsWeightIndexed());
  if (this->weight(items(meta.weight_tail())) <= weight) {
    *cursor = meta.weight_tail();
    return;
  }
  int step = floor(sqrt(meta.size));
  CHECK_GT(step, 0);
  offsetT start = meta.weight_tail();
  float cur_diff = std::numeric_limits<float>::max();
  for (size_t i = 0; i < meta.size; i += step) {
    // current node is not in weight index list
    if (i == cur_node_offset) { continue; }
    float diff = this->weight(items(i)) - weight;
    if (diff < 0) { continue; }
    if (diff < cur_diff) {
      cur_diff = diff;
      start = i;
    }
    if (diff == 0.0) { break; }
  }
  *cursor = start;
  int count = 0;
  while (*cursor != CPTreapItem::invalid_offset && this->weight(items(*cursor)) > weight) {
    *cursor = items(*cursor)->weight_prev_offset();
    CHECK_LE(++count, meta.size) << "circle detected in weight index list!";
  }
}

template <typename CPTreapItem>
int CPTreapEdgeList<CPTreapItem>::TimeRangeCountNode(uint32 start_ts, uint32 end_ts, offsetT *cursor) {
  CHECK(IsTimeIndexed());
  CHECK_LE(start_ts, end_ts);
  *cursor = meta.time_tail;
  if (meta.size > OPTIMIZE_SIZE_THRESHOLD) { FindTsOffset(end_ts, -1, cursor); }
  int count = 0;
  while (*cursor != CPTreapItem::invalid_offset && items(*cursor)->timestamp_s() > end_ts) {
    *cursor = items(*cursor)->prev_offset();
    CHECK_LE(++count, meta.size) << "circle detected in weight index list!";
  }
  // traverse node list by timestamp descending order
  int cnt = 0;
  offsetT offset = *cursor;
  while (offset != CPTreapItem::invalid_offset) {
    auto edge = items(offset);
    if (edge->timestamp_s() >= start_ts) {
      cnt++;
    } else if (edge->timestamp_s() < start_ts) {
      break;
    }
    offset = edge->prev_offset();
  }
  return cnt;
}

template <typename CPTreapItem>
bool CPTreapEdgeList<CPTreapItem>::Add(const std::vector<uint64> &ids,
                                       const std::vector<float> &weights,
                                       EdgeListReplaceStrategy replace_type,
                                       const std::vector<uint32_t> &timestamp_s,
                                       InsertEdgeWeightAct iewa,
                                       const std::vector<std::string> &id_attrs,
                                       BlockStorageApi *edge_attr_op) {
  if (meta.size > capacity) {
    // Data corrupted, should never happen.
    LOG_EVERY_N_SEC(ERROR, 5) << "loop list in a unsafe state, capacity = " << capacity
                              << ", size = " << meta.size << ". Clear data.";
    meta.size = 0;
    meta.root = CPTreapItem::invalid_offset;
    return false;
  }
  CHECK(weights.empty() || ids.size() == weights.size())
      << "Add edge item list error, ids and weights size not match, ids size = " << ids.size()
      << ", weights size = " << weights.size();
  CHECK(timestamp_s.empty() || ids.size() == timestamp_s.size())
      << "Add edge item list error, ids and ts size not match, ids size = " << ids.size()
      << ", ts size = " << timestamp_s.size();
  CHECK(id_attrs.empty() || ids.size() == id_attrs.size())
      << "Add edge item list error, ids and edge_attr size not match, ids size = " << ids.size()
      << ", edge_attr size = " << id_attrs.size();

  CHECK_NE(iewa, InsertEdgeWeightAct::UNKNOWN_ACT) << "iewa should not be unknown";

  thread_local base::PseudoRandom pr(base::RandomSeed());
  for (size_t i = 0; i < ids.size(); ++i) {
    Add(ids[i],
        (weights.empty() ? (0.1 + 0.9 * pr.GetDouble()) : weights[i]),
        replace_type,
        timestamp_s.empty() ? 0 : timestamp_s[i],
        iewa,
        id_attrs.empty() ? "" : id_attrs[i],
        edge_attr_op);
  }
  return true;
}

template <typename CPTreapItem>
bool CPTreapEdgeList<CPTreapItem>::Add(const uint64 id,
                                       const float weight,
                                       EdgeListReplaceStrategy replace_type,
                                       const uint32 timestamp_s,
                                       InsertEdgeWeightAct iewa,
                                       const std::string &attr,
                                       BlockStorageApi *edge_attr_op) {
  if (id == 0) {
    LOG_EVERY_N_SEC(WARNING, 5) << "insert id = 0, skip. weight = " << weight;
    return false;
  }
  if (weight >= absl::GetFlag(FLAGS_cpt_weight_max) || weight <= absl::GetFlag(FLAGS_cpt_weight_min)) {
    LOG_EVERY_N_SEC(WARNING, 5)
        << "cptreap only insert id > 0 and weight between(" << absl::GetFlag(FLAGS_cpt_weight_min) << ", "
        << absl::GetFlag(FLAGS_cpt_weight_max) << "); given id = " << id << ", weight = " << weight
        << ". You may want to adjust flag:cpt_weight_min or flag:cpt_weight_max, but be aware that this may "
           "cause float overflow and the structure of treap will be damaged.";
    return false;
  }
  // If attr size and attr_op not match, this insertion is invalid.
  if (!attr.empty()) {
    if (!edge_attr_op) {
      LOG_EVERY_N_SEC(ERROR, 5) << "attr op not specified when attr info exists!";
      return false;
    }
    if (attr.size() != edge_attr_size || edge_attr_op->MaxSize() != edge_attr_size) {
      LOG_EVERY_N_SEC(ERROR, 5) << "Input attr size: " << attr.size()
                                << ", op size: " << edge_attr_op->MaxSize()
                                << ", expected size: " << edge_attr_size;
      return false;
    }
  }

  if (Full()) {
    if (replace_type == EdgeListReplaceStrategy::WEIGHT) {
      PopHeap();
    } else if (replace_type == EdgeListReplaceStrategy::RANDOM) {
      PopRandom();
    } else if (replace_type == EdgeListReplaceStrategy::LRU) {
      if (!IsTimeIndexed()) { return false; }
      // discard the edge if it is older than current oldest edge
      uint32 oldest_ts = items(meta.time_head)->timestamp_s();
      if (timestamp_s <= oldest_ts && oldest_ts > 0) { return true; }
      PopOldest();
    } else if (replace_type == EdgeListReplaceStrategy::CANCEL) {
      return true;
    } else {
      LOG(ERROR) << "illegal replace_type: " << replace_type;
    }
  }

  PseudoRecursiveAdd(id, weight, timestamp_s, iewa, attr, edge_attr_op);
  out_degree++;
  return true;
}

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::PseudoRecursiveAdd(uint64_t id,
                                                      float weight,
                                                      uint32 timestamp_s,
                                                      InsertEdgeWeightAct iewa,
                                                      const std::string &attr,
                                                      BlockStorageApi *edge_attr_op) {
  if (meta.size == 0) {
    meta.root = AllocateNode();
    CPTreapItem *new_node = items(meta.root);
    if (IsTimeIndexed()) {
      meta.time_head = meta.root;
      meta.time_tail = meta.root;
    }
    if (IsWeightIndexed()) {
      meta.set_weight_head(meta.root);
      meta.set_weight_tail(meta.root);
    }
    new_node->set_id(id);
    new_node->cum_weight = weight;
    new_node->set_weight(weight);
    new_node->set_timestamp_s(timestamp_s);
    if (!attr.empty()) { edge_attr_op->UpdateBlock(attr_of_item(new_node), attr.c_str(), attr.size()); }
    PERF_SUM(1, GLOBAL_RELATION, P_EL, "edge_inserted");
    return;
  }

  offsetT cur_node_offset = meta.root;
  thread_local std::vector<offsetT> offsets;
  thread_local std::vector<bool> directions;
  offsets.clear();
  directions.clear();
  float delta = 0.0;
  bool weight_updated = true;
  while (true) {
    if (offsets.size() > meta.size) {
      meta.size = 0;
      meta.root = CPTreapItem::invalid_offset;
      LOG(ERROR) << "circle detected in treap, re-initialize the edge list";
      return;
    }
    CPTreapItem *node = items(cur_node_offset);
    if (node->id() == id) {
      if (timestamp_s < node->timestamp_s()) { return; }
      // Need rotate after weight updated.
      float new_weight = 0.0;
      if (iewa == InsertEdgeWeightAct::ADD) {
        delta = weight;
        new_weight = this->weight(node) + delta;
        // set precise weight
        node->set_weight(new_weight);
      } else if (iewa == InsertEdgeWeightAct::REPLACE) {
        delta = weight - this->weight(node);
        new_weight = weight;
        node->set_weight(new_weight);
      } else if (iewa == InsertEdgeWeightAct::MAX) {
        if (weight > this->weight(node)) {
          delta = weight - this->weight(node);
          new_weight = weight;
          node->set_weight(new_weight);
        } else {
          weight_updated = false;
        }
      } else if (iewa == InsertEdgeWeightAct::AVG) {
        float old_weight = this->weight(node);
        new_weight = node->update_weight_avg(weight);
        delta = new_weight - old_weight;
      }

      // size is 1, no change need in weight list
      // weight list change must be done before topological change
      if (meta.size > 1 && IsWeightIndexed() && weight_updated) {
        WeightRemoveNode(node, cur_node_offset);
        WeightAddNode(node, new_weight, cur_node_offset);
      }

      // topological change
      node->cum_weight += delta;
      if (!Rotate(node, cur_node_offset)) {
        meta.size = 0;
        meta.root = CPTreapItem::invalid_offset;
        LOG(ERROR) << "Rotate failed, re-initialize the edge list";
        return;
      }

      if (!attr.empty()) { edge_attr_op->UpdateBlock(attr_of_item(node), attr.c_str(), attr.size()); }

      node->set_timestamp_s(timestamp_s);
      if (meta.size > 1 && IsTimeIndexed()) {
        TimeRemoveNode(node, cur_node_offset);
        TimeAddNode(node, timestamp_s, cur_node_offset, meta.time_tail);
      }
      PERF_SUM(1, GLOBAL_RELATION, P_EL, "edge_updated");
      // recursive ends
      break;
    }

    offsets.push_back(cur_node_offset);
    if (node->id() < id) {
      directions.push_back(true);
      if (node->has_right_son()) {
        cur_node_offset = node->right;
      } else {
        offsetT insert_offset = meta.time_tail;
        auto new_node_offset = AllocateNode();
        auto new_node = items(new_node_offset);
        new_node->set_id(id);
        new_node->cum_weight = weight;
        new_node->set_weight(weight);
        new_node->set_father_offset(cur_node_offset);
        new_node->set_timestamp_s(timestamp_s);
        if (!attr.empty()) { edge_attr_op->UpdateBlock(attr_of_item(new_node), attr.c_str(), attr.size()); }
        // update weight list before topological change
        if (IsWeightIndexed()) { WeightAddNode(new_node, weight, new_node_offset); }
        // toppological change
        node->right = new_node_offset;
        if (IsTimeIndexed()) { TimeAddNode(new_node, timestamp_s, new_node_offset, insert_offset); }
        delta = weight;
        cur_node_offset = new_node_offset;
        PERF_SUM(1, GLOBAL_RELATION, P_EL, "edge_inserted");
        // recursive ends
        break;
      }
    } else {
      directions.push_back(false);
      if (node->has_left_son()) {
        cur_node_offset = node->left;
      } else {
        offsetT insert_offset = meta.time_tail;
        auto new_node_offset = AllocateNode();
        auto new_node = items(new_node_offset);
        new_node->set_id(id);
        new_node->cum_weight = weight;
        new_node->set_weight(weight);
        new_node->set_father_offset(cur_node_offset);
        new_node->set_timestamp_s(timestamp_s);
        if (!attr.empty()) { edge_attr_op->UpdateBlock(attr_of_item(new_node), attr.c_str(), attr.size()); }
        // update weight list before topological change
        if (IsWeightIndexed()) { WeightAddNode(new_node, weight, new_node_offset); }
        // toppological change
        node->left = new_node_offset;
        if (IsTimeIndexed()) { TimeAddNode(new_node, timestamp_s, new_node_offset, insert_offset); }
        delta = weight;
        cur_node_offset = new_node_offset;
        PERF_SUM(1, GLOBAL_RELATION, P_EL, "edge_inserted");
        // recursive ends
        break;
      }
    }
  }

  LOG_EVERY_N_SEC(INFO, 10) << "pseudo recursive depth: " << offsets.size()
                            << ", edge list size: " << meta.size;

  // update root node or weight is not changed
  if (offsets.size() == 0 || !weight_updated) { return; }

  CHECK_EQ(offsets.size(), directions.size());
  CPTreapItem *node = items(cur_node_offset);
  CHECK_EQ(node->id(), id);

  // trace back
  bool rotate = true;
  for (int i = offsets.size() - 1; i >= 0; i--) {
    cur_node_offset = offsets[i];
    node = items(cur_node_offset);
    // update cumulative weight
    node->cum_weight += delta;
    if (rotate) {
      if (directions[i]) {
        // right child
        if (this->weight(node) > this->weight(items(node->right))) {
          if (!RotateLeft(node, cur_node_offset)) {
            meta.size = 0;
            meta.root = CPTreapItem::invalid_offset;
            LOG(ERROR) << "RotateLeft failed, re-initialize the edge list";
            return;
          }
        } else {
          rotate = false;
        }
      } else {
        // left child
        if (this->weight(node) > this->weight(items(node->left))) {
          if (!RotateRight(node, cur_node_offset)) {
            meta.size = 0;
            meta.root = CPTreapItem::invalid_offset;
            LOG(ERROR) << "RotateRight failed, re-initialize the edge list";
            return;
          }
        } else {
          rotate = false;
        }
      }
    }
  }
}

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::DeleteById(uint64_t id) {
  if (meta.size == 0) { return; }

  offsetT cur_node_offset = meta.root;
  offsetT target_offset = CPTreapItem::invalid_offset;
  thread_local std::vector<offsetT> ancestor_offsets;
  ancestor_offsets.clear();
  float delta = 0.0;
  int step = 0;
  while (cur_node_offset != CPTreapItem::invalid_offset) {
    if (step++ > meta.size) {
      meta.size = 0;
      meta.root = CPTreapItem::invalid_offset;
      LOG(ERROR) << "circle detected in treap, delete failed, size: " << meta.size << ", step: " << step
                 << ", re-initialize the edge list";
      return;
    }
    auto cur_node = items(cur_node_offset);
    if (id < cur_node->id()) {
      ancestor_offsets.push_back(cur_node_offset);
      cur_node_offset = cur_node->left;
    } else if (id > cur_node->id()) {
      ancestor_offsets.push_back(cur_node_offset);
      cur_node_offset = cur_node->right;
    } else {  // cur_node is the node we want to delete
      delta = weight(cur_node);
      target_offset = cur_node_offset;
      if (cur_node->is_leaf()) {
        if (cur_node->has_father()) {
          auto father = items(cur_node->father_offset());
          if (cur_node_offset == father->left) {
            father->left = CPTreapItem::invalid_offset;
          } else {
            father->right = CPTreapItem::invalid_offset;
          }
        } else {
          // single node treap
          meta.root = CPTreapItem::invalid_offset;
        }
      } else if (cur_node->has_one_son()) {
        auto son_offset = cur_node->has_left_son() ? cur_node->left : cur_node->right;
        if (cur_node->has_father()) {
          auto father = items(cur_node->father_offset());
          if (cur_node_offset == father->left) {
            father->left = son_offset;
          } else {
            father->right = son_offset;
          }
        } else {
          meta.root = son_offset;
        }
        items(son_offset)->set_father_offset(cur_node->father_offset());
      } else {  // root has two non-empty children, then using rotation to make it a
                // leaf node or a node with only one child.
        while (cur_node->has_left_son() && cur_node->has_right_son()) {
          if (weight(items(cur_node->left)) < weight(items(cur_node->right))) {
            if (!RotateRight(cur_node, cur_node_offset)) {
              meta.size = 0;
              meta.root = CPTreapItem::invalid_offset;
              LOG(ERROR) << "RotateRight failed, re-initialize the edge list";
              return;
            }
          } else {
            if (!RotateLeft(cur_node, cur_node_offset)) {
              meta.size = 0;
              meta.root = CPTreapItem::invalid_offset;
              LOG(ERROR) << "RotateLeft failed, re-initialize the edge list";
              return;
            }
          }
          ancestor_offsets.push_back(cur_node->father_offset());
          if (step++ > meta.size) {
            meta.size = 0;
            meta.root = CPTreapItem::invalid_offset;
            LOG(ERROR) << "circle detected in treap, delete failed, size: " << meta.size << ", step: " << step
                       << ", re-initialize the edge list";
            return;
          }
        }
        auto father = items(cur_node->father_offset());
        if (cur_node->is_leaf()) {
          if (father->left == cur_node_offset) {
            father->left = CPTreapItem::invalid_offset;
          } else {
            father->right = CPTreapItem::invalid_offset;
          }
        } else {  // only one son
          auto son_offset = cur_node->has_left_son() ? cur_node->left : cur_node->right;
          items(son_offset)->set_father_offset(cur_node->father_offset());
          if (father->left == cur_node_offset) {
            father->left = son_offset;
          } else {
            father->right = son_offset;
          }
        }
      }
      // loop termination
      break;
    }
  }

  // not exist
  if (target_offset == CPTreapItem::invalid_offset) { return; }

  // adjust cum weight
  for (size_t i = 0; i < ancestor_offsets.size(); ++i) {
    auto node = items(ancestor_offsets[i]);
    node->cum_weight -= delta;
  }

  if (IsTimeIndexed()) { TimeRemoveNode(items(target_offset), target_offset); }
  if (IsWeightIndexed()) { WeightRemoveNode(items(target_offset), target_offset); }
  // RecycleNode must be last step
  RecycleNode(target_offset);
}

template <typename CPTreapItem>
bool CPTreapEdgeList<CPTreapItem>::Rotate(CPTreapItem *item, offsetT item_offset) {
  while (true) {
    // no son
    if (item->is_leaf()) { break; }
    // one son
    if (item->has_one_son()) {
      if (item->has_left_son() && weight(item) > weight(items(item->left))) {
        if (!RotateRight(item, item_offset)) { return false; }
      } else if (item->has_right_son() && weight(item) > weight(items(item->right))) {
        if (!RotateLeft(item, item_offset)) { return false; }
      } else {
        break;
      }
      continue;
    }
    // two sons
    if (weight(item) <= weight(items(item->left)) && weight(item) <= weight(items(item->right))) {
      // Smallest weight, no rotate needed.
      break;
    } else if (weight(items(item->left)) < weight(items(item->right))) {
      if (!RotateRight(item, item_offset)) { return false; }
    } else {
      if (!RotateLeft(item, item_offset)) { return false; }
    }
  }
  return true;
}

template <typename CPTreapItem>
bool CPTreapEdgeList<CPTreapItem>::RotateLeft(CPTreapItem *item, offsetT item_offset) {
  if (!item->is_root()) {
    CPTreapItem *father_ptr = items(item->father_offset());
    if (item_offset != father_ptr->left && item_offset != father_ptr->right) {
      LOG(ERROR) << "item offset not match with father, item = [" << item->info() << "], father = ["
                 << father_ptr->info() << "], item_offset = " << item_offset << ", clean corrupted data";
      return false;
    }

    // Need to set father offset
    // step1: Set father's son
    if (item_offset == father_ptr->left) {
      father_ptr->left = item->right;
    } else {
      father_ptr->right = item->right;
    }
    items(item->right)->set_father_offset(item->father_offset());
  } else {
    meta.root = item->right;
    items(item->right)->set_father_offset(CPTreapItem::invalid_offset);
  }

  // step2: Set right son to right son's left son.
  auto right_offset = item->right;
  // Collect weight info for step4.
  auto item_weight = item->cum_weight;
  auto right_weight = items(right_offset)->cum_weight;
  auto right_left_weight =
      items(right_offset)->has_left_son() ? items(items(right_offset)->left)->cum_weight : 0;

  item->right = items(right_offset)->left;
  if (items(right_offset)->has_left_son()) {
    items(items(right_offset)->left)->set_father_offset(item_offset);
  }

  // step3: Set this to right son's left son.
  items(right_offset)->left = item_offset;
  item->set_father_offset(right_offset);

  // step4: Maintain cumulative weight
  item->cum_weight -= (right_weight - right_left_weight);
  items(right_offset)->cum_weight = item_weight;
  return true;
}

template <typename CPTreapItem>
bool CPTreapEdgeList<CPTreapItem>::RotateRight(CPTreapItem *item, offsetT item_offset) {
  if (!item->is_root()) {
    CPTreapItem *father_ptr = items(item->father_offset());
    if (item_offset != father_ptr->left && item_offset != father_ptr->right) {
      LOG(ERROR) << "item offset not match with father, item = [" << item->info() << "], father = ["
                 << father_ptr->info() << "], item_offset = " << item_offset << ", clean corrupted data";
      return false;
    }

    // Need to set father offset
    // step1: Set father's son
    if (item_offset == father_ptr->left) {
      father_ptr->left = item->left;
    } else {
      father_ptr->right = item->left;
    }
    items(item->left)->set_father_offset(item->father_offset());
  } else {
    meta.root = item->left;
    items(item->left)->set_father_offset(CPTreapItem::invalid_offset);
  }

  // step2: Set left son to left son's right son.
  auto left_offset = item->left;
  // Collect weight info for step4.
  auto item_weight = item->cum_weight;
  auto left_weight = items(left_offset)->cum_weight;
  auto left_right_weight =
      items(left_offset)->has_right_son() ? items(items(left_offset)->right)->cum_weight : 0;

  item->left = items(left_offset)->right;
  if (items(left_offset)->has_right_son()) {
    items(items(left_offset)->right)->set_father_offset(item_offset);
  }

  // step3: Set this to left son's right son.
  items(left_offset)->right = item_offset;
  item->set_father_offset(left_offset);

  // step4: Maintain cumulative weight
  item->cum_weight -= (left_weight - left_right_weight);
  items(left_offset)->cum_weight = item_weight;
  return true;
}

template <typename CPTreapItem>
void CPTreapEdgeList<CPTreapItem>::RecycleNode(offsetT offset) {
  CHECK_LT(offset, meta.size) << "cpt recycle error, offset > size: " << offset << " vs " << meta.size;
  if (offset == meta.size - 1) {
    meta.size--;
    return;
  }
  // Now we have `offset` != the last one. We need to move the last node to the offset.
  memcpy(static_cast<void *>(items(offset)), static_cast<void *>(items(meta.size - 1)), item_size());
  items(meta.size - 1)->set_father_offset(offset);
  auto last_node = items(offset);
  if (last_node->has_father()) {
    auto last_node_father = items(last_node->father_offset());
    if ((meta.size - 1) == last_node_father->left) {
      last_node_father->left = offset;
    } else {
      last_node_father->right = offset;
    }
  } else {
    meta.root = offset;
  }
  if (last_node->has_left_son()) { items(last_node->left)->set_father_offset(offset); }
  if (last_node->has_right_son()) { items(last_node->right)->set_father_offset(offset); }
  // update time linked list
  if (IsTimeIndexed()) {
    if (last_node->prev_offset() != CPTreapItem::invalid_offset) {
      auto time_prev_node = items(last_node->prev_offset());
      time_prev_node->set_next_offset(offset);
    }
    if (last_node->next_offset() != CPTreapItem::invalid_offset) {
      auto time_next_node = items(last_node->next_offset());
      time_next_node->set_prev_offset(offset);
    }
    if (meta.time_head == meta.size - 1) { meta.time_head = offset; }
    if (meta.time_tail == meta.size - 1) { meta.time_tail = offset; }
  }

  // update weight linked list
  if (IsWeightIndexed()) {
    if (last_node->weight_prev_offset() != CPTreapItem::invalid_offset) {
      auto weight_prev_node = items(last_node->weight_prev_offset());
      weight_prev_node->set_weight_next_offset(offset);
    }
    if (last_node->weight_next_offset() != CPTreapItem::invalid_offset) {
      auto weight_next_node = items(last_node->weight_next_offset());
      weight_next_node->set_weight_prev_offset(offset);
    }
    if (meta.weight_head() == meta.size - 1) { meta.set_weight_head(offset); }
    if (meta.weight_tail() == meta.size - 1) { meta.set_weight_tail(offset); }
  }

  meta.size--;
}

template <typename CPTreapItem>
offsetT CPTreapEdgeList<CPTreapItem>::GetById(uint64_t id, CPTreapItem *result) const {
  if (meta.size == 0) { return CPTreapItem::invalid_offset; }
  offsetT offset = meta.root;
  auto node = items(meta.root);
  while (true) {
    if (id == node->id()) {
      if (result) { *result = *node; }
      return offset;
    }
    if (node->is_leaf()) { return CPTreapItem::invalid_offset; }
    if (id > node->id()) {
      if (node->has_right_son()) {
        offset = node->right;
        node = items(node->right);
      } else {
        return CPTreapItem::invalid_offset;
      }
    } else {
      if (node->has_left_son()) {
        offset = node->left;
        node = items(node->left);
      } else {
        return CPTreapItem::invalid_offset;
      }
    }
  }
  NOT_REACHED() << "GetById implement error!";
  return CPTreapItem::invalid_offset;
}

template <typename CPTreapItem>
const CPTreapItem *CPTreapEdgeList<CPTreapItem>::SampleEdge() const {
  if (meta.size == 0) { return nullptr; }
  auto node = items(meta.root);
  thread_local base::PseudoRandom pr(base::RandomSeed());
  float random = pr.GetDouble() * node->cum_weight;

  if (std::isnan(random)) {
    LOG(ERROR) << "weight is nan, use random sample.";
    return items(pr.GetInt(0, meta.size - 1));
  }

  while (!node->is_leaf()) {
    float node_w = this->weight(node);
    if (random <= node_w) { break; }
    random -= node_w;
    if (node->has_left_son()) {
      float left_son_weight_sum = items(node->left)->cum_weight;
      if (random <= left_son_weight_sum) {
        node = items(node->left);
        continue;
      }
      random -= left_son_weight_sum;
    }
    // A kind of precision loss is, random > left_son_weight_sum, but no right son exist.
    // Return self in this case.
    if (node->has_right_son()) {
      node = items(node->right);
    } else {
      break;
    }
  }
  return node;
}

template <typename CPTreapItem>
bool CPTreapEdgeList<CPTreapItem>::IsValid() const {
  if (meta.size == 0) { return true; }

  bool check_passed = false;
  base::Timer timer;
  // step 0
  if (!(meta.root <= meta.size && meta.size <= capacity)) {
    LOG_EVERY_N_SEC(INFO, 1) << "cpt IsValid step 0 fail, root = " << meta.root << ", size = " << meta.size
                             << ", capacity = " << capacity;
    return false;
  }
  timer.AppendCostMs("step0");
  // step 1
  base::ScopeExit t([&] {
    if (check_passed) {
      LOG_EVERY_N_SEC(INFO, 2) << "cpt valid check time: " << timer.display() << ", size = " << meta.size;
    } else {
      LOG(ERROR) << "cpt valid check failed: " << timer.display() << ", size = " << meta.size;
    }
  });
  auto root_ptr = items(meta.root);
  if (!root_ptr->is_root()) { return false; }
  timer.AppendCostMs("step1");

  // step 2
  auto s2_pass = true;
  for (size_t i = 0; i < meta.size; ++i) {
    auto node = items(i);
    // check father.
    if (!node->is_root()) {
      if (node->father_offset() == CPTreapItem::invalid_offset || node->father_offset() >= meta.size ||
          node->father_offset() == i) {
        LOG(ERROR) << "Invalid father offset.";
        s2_pass = false;
        break;
      }
      auto father_ptr = items(node->father_offset());
      auto t = (i == father_ptr->left) || (i == father_ptr->right);
      if (!t) {
        LOG(ERROR) << "Node is not father's son.";
        s2_pass = false;
        break;
      }
    }
    // check left.
    if (node->has_left_son()) {
      auto left_son = items(node->left);
      if (node->left == i || node->left >= meta.size || i != left_son->father_offset()) {
        LOG(ERROR) << "Node's left son error.";
        s2_pass = false;
        break;
      }
      // check tree and heap
      if (left_son->id() >= node->id() || weight(left_son) < weight(node)) {
        LOG(ERROR) << "Node's left son id or weight error. Left son id = " << left_son->id()
                   << ", weight = " << weight(left_son) << ", node id = " << node->id()
                   << ", weight = " << weight(node);
        s2_pass = false;
        break;
      }
    }
    // check right.
    if (node->has_right_son()) {
      auto right_son = items(node->right);
      if (node->right == i || node->right >= meta.size || i != right_son->father_offset()) {
        LOG(ERROR) << "Node's right son error.";
        s2_pass = false;
        break;
      }
      // check tree and heap
      if (right_son->id() <= node->id() || weight(right_son) < weight(node)) {
        LOG(ERROR) << "Node's right son id or weight error. Right son id = " << right_son->id()
                   << ", weight = " << weight(right_son) << ", node id = " << node->id()
                   << ", weight = " << weight(node);
        s2_pass = false;
        break;
      }
    }
  }
  if (!s2_pass) { return false; }
  timer.AppendCostMs("step2");

  // step 3, check tree traverse.
  std::vector<bool> all_ids(meta.size, 0);
  std::queue<offsetT> q;
  q.emplace(meta.root);
  int visited_node = 0;
  while (!q.empty()) {
    auto offset = q.front();
    q.pop();
    if (all_ids[offset]) { return false; }
    all_ids[offset] = true;
    visited_node++;
    if (items(offset)->has_left_son()) { q.emplace(items(offset)->left); }
    if (items(offset)->has_right_son()) { q.emplace(items(offset)->right); }
  }
  if (visited_node != meta.size) { return false; }
  if (all_ids.size() != meta.size) { return false; }
  timer.AppendCostMs("step3");

  auto s4_pass = IsTimeChainValid();
  if (!s4_pass) { return false; }
  timer.AppendCostMs("step4");

  auto s5_pass = IsWeightChainValid();
  if (!s5_pass) { return false; }
  timer.AppendCostMs("step5");

  check_passed = true;
  return true;
}

template <typename CPTreapItem>
base::KVData *CPTreapAdaptor<CPTreapItem>::InitBlock(MemAllocator *mem_allocator,
                                                     uint32_t *new_addr,
                                                     int add_size,
                                                     char *old_edge_list) const {
  base::KVData *ptr = nullptr;
  bool buffered = false;
  int new_size = add_size;
  EdgeListType *old_edge_list_ptr = nullptr;
  if (old_edge_list != nullptr) {
    old_edge_list_ptr = reinterpret_cast<EdgeListType *>(old_edge_list);
    new_size += old_edge_list_ptr->meta.size;
  }

  int capacity = absl::GetFlag(FLAGS_cpt_dynamic_growth_base);
  // this only happended in batch merge enabled case
  if (old_edge_list_ptr != nullptr && old_edge_list_ptr->capacity == config_.edge_capacity_max_num) {
    CHECK_GT(absl::GetFlag(FLAGS_cpt_edge_buffer_capacity), 0);
    ptr = mem_allocator->New(GetBufferedEdgeListMemorySize(), new_addr);
    capacity = config_.edge_capacity_max_num;
    buffered = true;
  } else {
    while (new_size > capacity && capacity < config_.edge_capacity_max_num) {
      // growing
      float t = capacity * absl::GetFlag(FLAGS_cpt_dynamic_growth_factor);
      capacity = ((int)t == capacity) ? capacity + 1 : (int)t;
      capacity = std::min(config_.edge_capacity_max_num, capacity);
    }
    ptr = mem_allocator->New(GetMemorySize(capacity), new_addr);
  }

  if (ptr == nullptr) { return nullptr; }
  auto edge_list_ptr = reinterpret_cast<EdgeListType *>(ptr->data());
  edge_list_ptr->Initialize(this->attr_op_, this->edge_attr_op_, capacity, buffered);
  return ptr;
}

template <typename CPTreapItem>
void CPTreapAdaptor<CPTreapItem>::Fill(char *edge_list, char *old_edge_list, const CommonUpdateItems &items) {
  base::Timer timer;
  auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
  base::ScopeExit se([&]() {
    LOG_EVERY_N_SEC(INFO, 5) << "fill cost: " << timer.display() << ", size = " << edge_list_ptr->meta.size;
  });

  if (old_edge_list != nullptr && items.overwrite) { this->Clean(old_edge_list); }

  bool replace = false;
  // Init from old_edge_list
  if (edge_list != old_edge_list && old_edge_list != nullptr) {
    replace = true;
    auto old_edge_list_ptr = reinterpret_cast<EdgeListType *>(old_edge_list);
    int capacity = edge_list_ptr->capacity;
    offsetT buffer_size = edge_list_ptr->meta.buffer_size;
    // check capacity is calculated correctly
    CHECK(capacity >= old_edge_list_ptr->meta.size + items.ids.size() ||
          capacity == config_.edge_capacity_max_num);
    // no need to copy pending edges and checksum
    memcpy(edge_list_ptr, old_edge_list, GetCoreMemorySize(old_edge_list_ptr->meta.size));
    // recover capacity, buffer_size
    edge_list_ptr->capacity = capacity;
    edge_list_ptr->meta.buffer_size = buffer_size;
    SetExpandMark(old_edge_list);
    timer.AppendCostMs("insert mem copy");
  } else if (old_edge_list == nullptr) {
    // new allocated
    CHECK(edge_list_ptr->capacity >= items.ids.size() ||
          edge_list_ptr->capacity == config_.edge_capacity_max_num);
  } else {
    // reuse
    CHECK(CanReuse(edge_list, items.ids.size())) << "items id size: " << items.ids.size();
  }

  // attr will always be updated immediately
  // need redesign attr part later
  if (!items.attr_update_info.empty()) {
    this->attr_op_->UpdateBlock(
        edge_list_ptr->attr_block(), items.attr_update_info.c_str(), items.attr_update_info.size());
  }

  // batch merge enabled
  if (edge_list_ptr->BatchMergeEnabled()) {
    // batch merge triggered
    if (replace) {
      CHECK_EQ(edge_list_ptr->meta.buffer_size, 0);
      // merge old edge list buffer first if have
      auto old_edge_list_ptr = reinterpret_cast<EdgeListType *>(old_edge_list);
      if (old_edge_list_ptr->BatchMergeEnabled()) {
        for (size_t i = 0; i < old_edge_list_ptr->meta.buffer_size; ++i) {
          const PendingEdge *edge = old_edge_list_ptr->edges(i);
          std::string edge_attr(edge_list_ptr->attr_of_pending_edge(edge), edge_attr_op_->MaxSize());
          edge_list_ptr->Add(edge->id,
                             edge->weight,
                             config_.oversize_replace_strategy,
                             edge->timestamp,
                             edge->iewa,
                             edge_attr,
                             edge_attr_op_);
        }
      }
      // add new items
      edge_list_ptr->Add(items.ids,
                         items.id_weights,
                         config_.oversize_replace_strategy,
                         items.id_ts,
                         items.iewa,
                         items.id_attr_update_infos,
                         edge_attr_op_);
    } else {
      // buffer new items
      CHECK_LE(edge_list_ptr->meta.buffer_size + items.ids.size(),
               absl::GetFlag(FLAGS_cpt_edge_buffer_capacity));
      thread_local base::PseudoRandom pr(base::RandomSeed());
      for (size_t i = 0; i < items.ids.size(); ++i) {
        PendingEdge *edge = edge_list_ptr->edges(edge_list_ptr->meta.buffer_size);
        edge_list_ptr->meta.buffer_size++;
        edge->id = items.ids[i];
        edge->weight = items.id_weights.empty() ? (0.1 + 0.9 * pr.GetDouble()) : items.id_weights[i];
        edge->timestamp = items.id_ts.empty() ? 0 : items.id_ts[i];
        edge->iewa = items.iewa;
        if (!items.id_attr_update_infos.empty() && !items.id_attr_update_infos[i].empty()) {
          if (items.id_attr_update_infos[i].size() != edge_attr_op_->MaxSize()) {
            LOG_EVERY_N_SEC(ERROR, 5) << "input attr size: " << items.id_attr_update_infos[i].size()
                                      << ", expect: " << edge_attr_op_->MaxSize();
          } else {
            edge_attr_op_->UpdateBlock(edge_list_ptr->attr_of_pending_edge(edge),
                                       items.id_attr_update_infos[i].c_str(),
                                       edge_attr_op_->MaxSize());
          }
        }
      }
    }
    timer.AppendCostMs("batch merge");
  } else {
    edge_list_ptr->Add(items.ids,
                       items.id_weights,
                       config_.oversize_replace_strategy,
                       items.id_ts,
                       items.iewa,
                       items.id_attr_update_infos,
                       edge_attr_op_);
    timer.AppendCostMs("insert add");
  }
  SetCheckSumIfNeed(edge_list);
  timer.AppendCostMs("checksum calc");
}

template <typename CPTreapItem>
bool CPTreapAdaptor<CPTreapItem>::Sample(const char *edge_list,
                                         const SamplingParam &param,
                                         EdgeListSlice *slice) const {
  base::Timer timer;
  auto ptr = reinterpret_cast<const EdgeListType *>(edge_list);
  if (ptr->meta.size == 0) { return true; }
  auto origin_size = ptr->meta.size;

  EdgeListType *new_ptr = nullptr;

  base::ScopeExit scope_exit([&] {
    CHECK_NE(ptr, new_ptr);
    free(new_ptr);
    auto eps = timer.Stop();
    // 2ms
    if (eps > 2000) {
      LOG_EVERY_N_SEC(WARNING, 3) << "long cost sample, edge list size: " << origin_size
                                  << ", sample result list size: " << slice->node_id->size()
                                  << ", sample type: "
                                  << SamplingParam_SamplingStrategy_Name(param.strategy())
                                  << ", sample cost: " << timer.display() << ", total cost: " << eps << "us";
    } else {
      LOG_EVERY_N_SEC(INFO, 5) << "edge list size: " << origin_size
                               << ", sample result list size: " << slice->node_id->size()
                               << ", sample type: " << SamplingParam_SamplingStrategy_Name(param.strategy())
                               << ", sample cost: " << timer.display() << ", total cost: " << eps << "us";
    }
  });
  size_t list_size = GetCoreMemorySize(origin_size);
  new_ptr = static_cast<EdgeListType *>(malloc(list_size));
  timer.AppendCostMs("mem alloc");

  // Make a copy for irreversible operations
  memcpy(new_ptr, ptr, list_size);
  timer.AppendCostMs("mem copy");

  // checksum validation, no need check in batch update mode
  if (!new_ptr->BatchMergeEnabled() &&
      GetCheckSum(edge_list) != CalcCheckSum(reinterpret_cast<char *>(new_ptr))) {
    LOG_EVERY_N_SEC(WARNING, 10) << "checksum validation failed, the data is corrupt";
    return false;
  }
  timer.AppendCostMs("checksum validation");

  // Filter by timestamp
  auto max_timestamp_s = 0;
  if (param.max_timestamp_s() > 0) {
    max_timestamp_s = param.max_timestamp_s();
    if (new_ptr->IsTimeIndexed() && new_ptr->meta.size > 0 &&
        max_timestamp_s < new_ptr->items(new_ptr->meta.time_head)->timestamp_s()) {
      std::string t_s = base::TimestampToString(param.max_timestamp_s());
      LOG_EVERY_N_SEC(INFO, 10) << "cpt sample early stop because all edge's timestamp is bigger than given; "
                                << "req.timestamp = " << t_s << "[" << param.max_timestamp_s() << "]"
                                << ", earliest edge timestamp = "
                                << new_ptr->items(new_ptr->meta.time_head)->timestamp_s();
      return true;
    }
  }
  auto min_timestamp_s = 0;
  if (param.min_timestamp_s() > 0) {
    min_timestamp_s = param.min_timestamp_s();
    if (new_ptr->IsTimeIndexed() && new_ptr->meta.size > 0 &&
        min_timestamp_s > new_ptr->items(new_ptr->meta.time_tail)->timestamp_s()) {
      std::string t_s = base::TimestampToString(param.min_timestamp_s());
      LOG_EVERY_N_SEC(INFO, 10)
          << "cpt sample early stop because all edge's timestamp is smaller than given; "
          << "req.timestamp = " << t_s << "[" << param.min_timestamp_s() << "]"
          << ", latest edge timestamp = " << new_ptr->items(new_ptr->meta.time_tail)->timestamp_s();
      return true;
    }
  }

  if (param.strategy() == SamplingParam_SamplingStrategy_MOST_RECENT) {
    if (!new_ptr->IsTimeIndexed()) { return false; }
    // Time complexity: O(X) + O(T) where X is sample_num, T is the number of edges whose timestamp is bigger
    // than max_ts. Worst case: O(N)
    auto offset = new_ptr->meta.time_tail;
    for (size_t i = 0; i < param.sampling_num() && offset != CPTreapItem::invalid_offset;) {
      auto edge = new_ptr->items(offset);
      if (min_timestamp_s > 0 && edge->timestamp_s() < min_timestamp_s) { return true; }
      if (max_timestamp_s == 0 || edge->timestamp_s() <= max_timestamp_s) {
        if (new_ptr->weight(edge) >= param.min_weight_required()) {
          std::string edge_attr(new_ptr->attr_of_item(edge), new_ptr->edge_attr_size);
          slice->Add(edge->id(), edge->timestamp_s(), new_ptr->weight(edge), edge_attr);
          i++;
        }
      }
      offset = edge->prev_offset();
    }
    timer.AppendCostMs("most-recent sample cost");
    return true;
  }

  if (param.strategy() == SamplingParam_SamplingStrategy_LEAST_RECENT) {
    if (!new_ptr->IsTimeIndexed()) { return false; }
    auto offset = new_ptr->meta.time_head;
    for (size_t i = 0; i < param.sampling_num() && offset != CPTreapItem::invalid_offset;) {
      auto edge = new_ptr->items(offset);
      if (max_timestamp_s > 0 && edge->timestamp_s() > max_timestamp_s) { return true; }
      if (min_timestamp_s == 0 || edge->timestamp_s() >= min_timestamp_s) {
        if (new_ptr->weight(edge) >= param.min_weight_required()) {
          std::string edge_attr(new_ptr->attr_of_item(edge), new_ptr->edge_attr_size);
          slice->Add(edge->id(), edge->timestamp_s(), new_ptr->weight(edge), edge_attr);
          i++;
        }
      }
      offset = edge->next_offset();
    }
    timer.AppendCostMs("least-recent sample cost");
    return true;
  }

  if (param.strategy() == SamplingParam_SamplingStrategy_RANDOM) {
    thread_local base::PseudoRandom pr(base::RandomSeed());
    thread_local std::vector<int> candidates;
    candidates.reserve(new_ptr->meta.size);
    candidates.clear();
    for (size_t i = 0; i < new_ptr->meta.size; ++i) { candidates.push_back(i); }
    timer.AppendCostMs("random-sample candidates init cost");

    int cand_size = candidates.size();
    for (size_t i = 0; i < param.sampling_num() && cand_size > 0;) {
      int idx = pr.GetInt(0, cand_size - 1);
      int offset = candidates[idx];
      auto node = new_ptr->items(offset);
      // filter timestamp
      bool filtered = max_timestamp_s > 0 && node->timestamp_s() > max_timestamp_s;
      filtered |= min_timestamp_s > 0 && node->timestamp_s() < min_timestamp_s;
      // filter weight
      filtered |= param.min_weight_required() > 0 && new_ptr->weight(node) < param.min_weight_required();
      if (filtered) {
        std::swap(candidates[idx], candidates[--cand_size]);
        continue;
      }
      std::string edge_attr(new_ptr->attr_of_item(node), new_ptr->edge_attr_size);
      slice->Add(node->id(), node->timestamp_s(), new_ptr->weight(node), edge_attr);
      if (param.sampling_without_replacement()) { std::swap(candidates[idx], candidates[--cand_size]); }
      ++i;
    }
    timer.AppendCostMs("random-sample sample");
    return true;
  }

  if (param.strategy() == SamplingParam_SamplingStrategy_EDGE_WEIGHT) {
    // Time complexity: O(LogN * X) + O(T) where X is sample_num, T is the number of edges whose timestamp is
    // bigger than max_ts. Worst case: O(N)

    // For discarded node, use BuryNode to reduce their weight to 0 is much faster than PopHeap
    if (param.min_weight_required() > 0) {
      CHECK_NE(ptr, new_ptr);
      new_ptr->LayerOrderBury(param.min_weight_required());
    }
    timer.AppendCostMs("weight-sample filter min-weight");
    auto offset = new_ptr->meta.time_tail;
    if (max_timestamp_s > 0 && new_ptr->IsTimeIndexed()) {
      CHECK_NE(ptr, new_ptr);
      while (offset != CPTreapItem::invalid_offset &&
             new_ptr->items(offset)->timestamp_s() > max_timestamp_s) {
        new_ptr->BuryNode(offset);
        offset = new_ptr->items(offset)->prev_offset();
      }
      timer.AppendCostMs("weight-sample filter max_timestamp");
    }
    offset = new_ptr->meta.time_head;
    if (min_timestamp_s > 0 && new_ptr->IsTimeIndexed()) {
      CHECK_NE(ptr, new_ptr);
      while (offset != CPTreapItem::invalid_offset &&
             new_ptr->items(offset)->timestamp_s() < min_timestamp_s) {
        new_ptr->BuryNode(offset);
        offset = new_ptr->items(offset)->next_offset();
      }
      timer.AppendCostMs("weight-sample filter min_timestamp");
    }

    for (size_t i = 0; i < param.sampling_num(); ++i) {
      const CPTreapItem *edge = new_ptr->SampleEdge();
      // SampleEdge may return node that is buried.
      if (edge != nullptr && edge->cum_weight > 1e-6) {
        std::string edge_attr(new_ptr->attr_of_item(edge), new_ptr->edge_attr_size);
        slice->Add(edge->id(), edge->timestamp_s(), new_ptr->weight(edge), edge_attr);
        if (param.sampling_without_replacement()) {
          CHECK_NE(ptr, new_ptr);
          new_ptr->BuryNode(new_ptr->offset(edge));
        }
      } else {
        slice->Add(0, 0, 0.0f, "");
      }
    }
    timer.AppendCostMs("weight-sample sample");
    return true;
  }

  if (param.strategy() == SamplingParam_SamplingStrategy_TOPN_WEIGHT) {
    // weight index enabled
    if (new_ptr->IsWeightIndexed()) {
      thread_local Bitmap bitmap;
      // resize and reset must be called because bitmap is thread local variable
      bitmap.Resize(new_ptr->meta.size);
      bitmap.Reset();
      if (max_timestamp_s > 0 && new_ptr->IsTimeIndexed()) {
        auto offset = new_ptr->meta.time_tail;
        while (offset != CPTreapItem::invalid_offset) {
          auto edge = new_ptr->items(offset);
          if (edge->timestamp_s() <= max_timestamp_s) { break; }
          bitmap.Set(offset);
          offset = edge->prev_offset();
        }
      }
      if (min_timestamp_s > 0 && new_ptr->IsTimeIndexed()) {
        auto offset = new_ptr->meta.time_head;
        while (offset != CPTreapItem::invalid_offset) {
          auto edge = new_ptr->items(offset);
          if (edge->timestamp_s() >= min_timestamp_s) { break; }
          bitmap.Set(offset);
          offset = edge->next_offset();
        }
      }
      timer.AppendCostMs("topn-sample timestamp bitmap");
      auto offset = new_ptr->meta.weight_tail();
      const CPTreapItem *edge = nullptr;
      for (size_t i = 0; i < param.sampling_num() && offset != CPTreapItem::invalid_offset;
           offset = edge->weight_prev_offset()) {
        edge = new_ptr->items(offset);
        if (param.min_weight_required() > 0 && new_ptr->weight(edge) < param.min_weight_required()) { break; }
        if ((max_timestamp_s > 0 || min_timestamp_s > 0) && bitmap.Get(offset)) { continue; }
        std::string edge_attr(new_ptr->attr_of_item(edge), new_ptr->edge_attr_size);
        slice->Add(edge->id(), edge->timestamp_s(), new_ptr->weight(edge), edge_attr);
        i++;
      }
      timer.AppendCostMs("topn-sample sample weight index");
      return true;
    }

    // Without `weight index`, we need to Delete discarded nodes.
    // Each deletion has O(log n) time complexity,
    // VERY SLOW when size is about 1000 or larger!
    if (max_timestamp_s > 0 && new_ptr->IsTimeIndexed()) {
      CHECK_NE(ptr, new_ptr);
      auto edge = new_ptr->items(new_ptr->meta.time_tail);
      while (new_ptr->meta.time_tail < new_ptr->meta.size && edge->timestamp_s() > max_timestamp_s) {
        new_ptr->DeleteById(edge->id());
        edge = new_ptr->items(new_ptr->meta.time_tail);
      }
      timer.AppendCostMs("topn-sample filter max_timestamp");
    }
    if (min_timestamp_s > 0 && new_ptr->IsTimeIndexed()) {
      CHECK_NE(ptr, new_ptr);
      auto edge = new_ptr->items(new_ptr->meta.time_head);
      while (new_ptr->meta.time_head < new_ptr->meta.size && edge->timestamp_s() < min_timestamp_s) {
        new_ptr->DeleteById(edge->id());
        edge = new_ptr->items(new_ptr->meta.time_head);
      }
      timer.AppendCostMs("topn-sample filter min_timestamp");
    }

    for (int i = 0, del_num = (int)new_ptr->meta.size - (int)param.sampling_num(); i < del_num; ++i) {
      CHECK_NE(ptr, new_ptr);
      new_ptr->PopHeap();
    }
    timer.AppendCostMs("topn-sample pop min-weight");
    while (new_ptr->meta.size > 0 &&
           new_ptr->weight(new_ptr->items(new_ptr->meta.root)) < param.min_weight_required()) {
      CHECK_NE(ptr, new_ptr);
      new_ptr->PopHeap();
    }
    timer.AppendCostMs("topn-sample filter min-weight");
    for (size_t i = 0; i < new_ptr->meta.size; ++i) {
      const CPTreapItem *edge = new_ptr->items(i);
      std::string edge_attr(new_ptr->attr_of_item(edge), new_ptr->edge_attr_size);
      slice->Add(edge->id(), edge->timestamp_s(), new_ptr->weight(edge), edge_attr);
    }
    timer.AppendCostMs("topn-sample add node");
    return true;
  }
  LOG(ERROR) << "cpt doesn't support sample type: " << SamplingParam_SamplingStrategy_Name(param.strategy());
  return false;
};

template <typename CPTreapItem>
bool CPTreapAdaptor<CPTreapItem>::GetEdgeListInfo(const char *edge_list,
                                                  NodeAttr *node_attr,
                                                  ProtoPtrList<EdgeInfo> *edge_info) const {
  auto edge_list_ptr = reinterpret_cast<const EdgeListType *>(edge_list);
  if (node_attr) {
    node_attr->set_degree(GetDegree(edge_list));
    node_attr->set_edge_num(GetSize(edge_list));
    *(node_attr->mutable_attr_info()) = std::move(this->AttrInfo(edge_list));
  }

  if (edge_info) {
    for (size_t i = 0; i < edge_list_ptr->meta.size; ++i) {
      auto edge = edge_info->Add();
      auto *item = edge_list_ptr->items(i);
      edge->set_id(item->id());
      edge->set_weight(edge_list_ptr->weight(item));
      edge->set_timestamp(item->timestamp_s());
      std::string edge_attr(edge_list_ptr->IthEdgeAttrBlock(i), edge_list_ptr->edge_attr_size);
      edge->set_edge_attr(std::move(edge_attr));
    }
  }

  return true;
}

}  // namespace chrono_graph