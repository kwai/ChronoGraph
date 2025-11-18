# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

from collections import defaultdict
from typing import Dict, List, Tuple
import torch
from torch_geometric import seed_everything
import torch_geometric.transforms as T
from torch_geometric.data import HeteroData
from torch_geometric.nn import SAGEConv, to_hetero
import tqdm
import torch.nn.functional as F
from sklearn.metrics import roc_auc_score
import os

from chrono_graph import graph

ALREADY_INITIALIZED = False


class GraphSageSampler:

  def __init__(self,
               gnn_service: str,
               shard_num: int,
               data: HeteroData,
               pred_type_to_relation: Tuple[Tuple, str],
               node_type_to_relation: Dict[str, List[Tuple[Tuple, str]]],
               fanouts: List[int],
               neg_sampling_num: int,
               batch_size: int,
               id_base: int = 1,
               already_initialized: bool = False):
    self.g = None
    self.gnn_service = gnn_service
    self.shard_num = shard_num
    self.id_base = id_base
    self.fanouts = fanouts
    self.num_pos = 1
    self.num_neg = neg_sampling_num
    self.batch_size = batch_size
    self.pred_type_to_relation = pred_type_to_relation
    self.node_type_to_relation = node_type_to_relation
    self.initialized = already_initialized
    self.data = data

  def initialize_graph(self, batch_size: int = 10240):
    """Init client and push data to server"""

    self.g = graph.Graph(self.gnn_service, self.shard_num)
    for _, relation_list in self.node_type_to_relation.items():
      for _, relation in relation_list:
        self.g.add_client(relation)
    if self.initialized:
      return
    for _, relation_list in self.node_type_to_relation.items():
      for edge_type, relation in relation_list:
        self._add_edges_to_graph(relation, self.data[edge_type].edge_index, batch_size)
    self.initialized = True

  def _add_edges_to_graph(self, relation: str, edge_index: torch.Tensor, batch_size: int = 10240):
    for i in tqdm.tqdm(range(0, edge_index.size(1), batch_size)):
      batch_edges = edge_index[:, i:i + batch_size]
      self.g.insert_edges(relation).more_edges(
          batch_edges[0] + self.id_base,
          batch_edges[1] + self.id_base,
      ).emit()

  def sample(self):
    src_node_type = self.pred_type_to_relation[0][0]
    dst_node_type = self.pred_type_to_relation[0][2]
    src_relation = self.pred_type_to_relation[1]

    if len(self.node_type_to_relation.get(dst_node_type, [])) == 0:
      raise ValueError(f"No relation found for node type {dst_node_type}")

    dst_relation = self.node_type_to_relation[dst_node_type][0][1]

    # Random sample nodes as src
    nodes = self.g.random_nodes(src_relation, "src_nodes").batch(self.batch_size) \
        .emit()["src_nodes"]["ids"]
    # src's neighbor as positive samples
    pos_nodes = self.g.set_nodes(src_relation, nodes, "src_nodes") \
        .sample_neighbor(src_relation, "pos_nodes").sample(self.num_pos).by("random", "self", False) \
        .emit()["pos_nodes"]["ids"]
    # global sample as negative samples
    neg_nodes = self.g.random_nodes(dst_relation, "neg_nodes").batch(self.batch_size * self.num_neg) \
        .emit()["neg_nodes"]["ids"]

    src_nodes = nodes[:]

    dst_nodes = pos_nodes[:]
    dst_nodes.extend(neg_nodes)

    # sample for build sub graph
    edge_index_dict: Dict[Tuple, List[List[int], List[int]]] = defaultdict(lambda: [[], []])
    self._sample_neighbor_hop(node_type=src_node_type, nodes=src_nodes, edge_index_dict=edge_index_dict)
    self._sample_neighbor_hop(node_type=dst_node_type, nodes=dst_nodes, edge_index_dict=edge_index_dict)

    edge_index_tensor_dict: Dict[Tuple, torch.Tensor] = {}
    for key, value in edge_index_dict.items():
      edge_index_tensor_dict[key] = torch.tensor(value)

    edge_label_index, edge_label = self._build_label_index(nodes, pos_nodes, neg_nodes)

    return self._reindex_and_build_data(edge_index_dict=edge_index_tensor_dict,
                                        edge_label_index=edge_label_index,
                                        edge_label=edge_label)

  def _sample_neighbor_hop(self, node_type: str, nodes: List[int], edge_index_dict):
    now_node_types = [node_type]
    now_nodes = [nodes]
    for fanout in self.fanouts:
      next_node_types = []
      next_nodes = []
      for index, node_t in enumerate(now_node_types):
        if node_t not in self.node_type_to_relation:
          continue

        for edge_info, relation in self.node_type_to_relation[node_t]:

          neighbor_nodes = self.g.set_nodes(relation, now_nodes[index], "src_nodes") \
              .sample_neighbor(relation, "neighbor_nodes").sample(fanout).by("random", "self", False) \
              .emit()["neighbor_nodes"]["ids"]

          edge_index_dict[edge_info][0].extend([element for element in now_nodes[index] for _ in range(fanout)])
          edge_index_dict[edge_info][1].extend(neighbor_nodes)

          next_node_types.append(edge_info[2])
          next_nodes.append(neighbor_nodes)

      now_node_types = next_node_types
      now_nodes = next_nodes

  def _build_label_index(self, src_nodes: List[int], pos_nodes: List[int], neg_nodes: List[int]):
    src_tensor, pos_tensor, neg_tensor = torch.tensor(src_nodes), torch.tensor(pos_nodes), torch.tensor(neg_nodes)
    expanded_src_pos = torch.repeat_interleave(src_tensor, self.num_pos)
    expanded_src_neg = torch.repeat_interleave(src_tensor, self.num_neg)

    expanded_src = torch.cat([expanded_src_pos, expanded_src_neg])
    combined_pos_neg = torch.cat([pos_tensor, neg_tensor])

    edge_label_index = torch.stack([expanded_src, combined_pos_neg])
    edge_label = torch.cat([torch.ones_like(pos_tensor), torch.zeros_like(neg_tensor)]).float()

    return edge_label_index, edge_label

  def _reindex_and_build_data(self, edge_index_dict: Dict[Tuple, torch.Tensor], edge_label_index: torch.Tensor,
                              edge_label: torch.Tensor):

    node_type_infos = defaultdict(lambda: {"node_ids": [], "edge_types": [], "pos": [], "shape": []})
    node_type_count = defaultdict(lambda: 0)

    edge_index_dict[(self.pred_type_to_relation[0][0], "#pred", self.pred_type_to_relation[0][2])] = edge_label_index

    for edge_type, node_ids in edge_index_dict.items():

      src_type, _, dst_type = edge_type

      old_src_count = node_type_count[src_type]
      old_dst_count = node_type_count[dst_type]
      node_type_count[src_type] += len(node_ids[0])
      node_type_count[dst_type] += len(node_ids[1])

      # Add source node information
      node_type_infos[src_type]["node_ids"].append(node_ids[0])
      node_type_infos[src_type]["edge_types"].append(edge_type)
      node_type_infos[src_type]["pos"].append(0)
      node_type_infos[src_type]["shape"].append((old_src_count, node_type_count[src_type]))

      # Add dest node information
      node_type_infos[dst_type]["node_ids"].append(node_ids[1])
      node_type_infos[dst_type]["edge_types"].append(edge_type)
      node_type_infos[dst_type]["pos"].append(1)
      node_type_infos[dst_type]["shape"].append((old_dst_count, node_type_count[dst_type]))

    data = HeteroData()

    encoded_edge_index_dict = {}

    # Initialize the encoded edge index dictionary
    for edge_type, edge_index in edge_index_dict.items():
      encoded_edge_index_dict[edge_type] = torch.zeros_like(edge_index)

    # Handling each node type
    for node_type, info in node_type_infos.items():
      # Convert list to tensor and get unique nodes
      all_node_ids = torch.cat(info["node_ids"])
      unique_ids, inverse_indices = torch.unique(all_node_ids, return_inverse=True)

      # Set node features
      for key, value in self.data[node_type].items():
        data[node_type][key] = value[unique_ids - self.id_base]

      # Update edge index
      for i, (edge_type, pos, (start, end)) in enumerate(zip(info["edge_types"], info["pos"], info["shape"])):
        encoded_edge_index_dict[edge_type][pos] = inverse_indices[start:end]

    # Set edge data
    for edge_type, edge_index in encoded_edge_index_dict.items():
      if edge_type[1] == "#pred":
        pred_edge_type = self.pred_type_to_relation[0]
        data[pred_edge_type].edge_label_index = edge_index
        data[pred_edge_type].edge_label = edge_label
      data[edge_type].edge_index = edge_index

    return data


class GraphSageDataLoader:

  def __init__(self, node_sampler: GraphSageSampler, num_iterations: int = 10):
    self.sampler = node_sampler
    self.num_iterations = num_iterations

  def __iter__(self):
    self.current_iteration = 0
    return self

  def __next__(self):
    if self.current_iteration < self.num_iterations:
      self.current_iteration += 1
      return self.sampler.sample()
    else:
      raise StopIteration


model_dir = os.path.dirname(os.path.abspath(__file__))
data_path = model_dir + "/data/MovieLens/Small/processed/data.pt"
data = torch.load(data_path, weights_only=False)
seed_everything(42)

transform = T.RandomLinkSplit(
    num_val=0.1,
    num_test=0.1,
    disjoint_train_ratio=0.3,
    neg_sampling_ratio=2.0,
    add_negative_train_samples=False,
    edge_types=("user", "rates", "movie"),
    rev_edge_types=("movie", "rev_rates", "user"),
)

train_data, val_data, test_data = transform(data)

sampler = GraphSageSampler(
    gnn_service="grpc_gnn_graphasage_demo-U2I-I2U",
    shard_num=1,
    data=train_data,
    pred_type_to_relation=(("user", "rates", "movie"), "U2I"),
    node_type_to_relation={
        "user": [(("user", "rates", "movie"), "U2I")],
        "movie": [(("movie", "rev_rates", "user"), "I2U")],
    },
    fanouts=[20, 10],
    neg_sampling_num=2,
    batch_size=128,
    id_base=1,
    already_initialized=ALREADY_INITIALIZED,
)
sampler.initialize_graph()
train_loader = GraphSageDataLoader(sampler, num_iterations=100)


class GNN(torch.nn.Module):

  def __init__(self, hidden_channels):
    super().__init__()

    self.conv1 = SAGEConv(hidden_channels, hidden_channels)
    self.conv2 = SAGEConv(hidden_channels, hidden_channels)

  def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
    x = self.conv1(x, edge_index)
    x = x.relu()
    x = self.conv2(x, edge_index)
    return x


class Classifier(torch.nn.Module):

  def forward(self, x_user: torch.Tensor, x_movie: torch.Tensor, edge_label_index: torch.Tensor) -> torch.Tensor:
    # Convert node embeddings to edge-level representations:
    edge_feat_user = x_user[edge_label_index[0]]
    edge_feat_movie = x_movie[edge_label_index[1]]

    # Apply dot-product to get a prediction per supervision edge:
    return (edge_feat_user * edge_feat_movie).sum(dim=-1)


class Model(torch.nn.Module):

  def __init__(self, hidden_channels):
    super().__init__()
    # Since the dataset does not come with rich features, we also learn two
    # embedding matrices for users and movies:
    self.movie_lin = torch.nn.Linear(20, hidden_channels)
    self.user_emb = torch.nn.Embedding(data["user"].num_nodes, hidden_channels)
    self.movie_emb = torch.nn.Embedding(data["movie"].num_nodes, hidden_channels)

    # Instantiate homogeneous GNN:
    self.gnn = GNN(hidden_channels)

    # Convert GNN model into a heterogeneous variant:
    self.gnn = to_hetero(self.gnn, metadata=data.metadata())

    self.classifier = Classifier()

  def forward(self, data: HeteroData) -> torch.Tensor:
    x_dict = {
        "user": self.user_emb(data["user"].node_id),
        "movie": self.movie_lin(data["movie"].x) + self.movie_emb(data["movie"].node_id),
    }

    # `x_dict` holds feature matrices of all node types
    # `edge_index_dict` holds all edge indices of all edge types
    x_dict = self.gnn(x_dict, data.edge_index_dict)

    pred = self.classifier(
        x_dict["user"],
        x_dict["movie"],
        data["user", "rates", "movie"].edge_label_index,
    )

    return pred


model = Model(hidden_channels=64)

print(f"Model: '{model}'")

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Device: '{device}'")

model = model.to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

for epoch in range(1, 6):
  total_loss = total_examples = 0
  for sampled_data in tqdm.tqdm(train_loader):
    optimizer.zero_grad()
    sampled_data.to(device)
    pred = model(sampled_data)
    ground_truth = sampled_data["user", "rates", "movie"].edge_label
    loss = F.binary_cross_entropy_with_logits(pred, ground_truth)

    loss.backward()
    optimizer.step()
    total_loss += float(loss) * pred.numel()
    total_examples += pred.numel()

  with torch.no_grad():
    pred = model(val_data).cpu().numpy()
    ground_truth = val_data["user", "rates", "movie"].edge_label.cpu().numpy()

    auc = roc_auc_score(ground_truth, pred)
    print()
    print(f"Validation AUC: {auc:.4f}")

  print(f"Epoch: {epoch:03d}, Loss: {total_loss / total_examples:.4f}")
