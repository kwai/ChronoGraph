# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

import torch
from torch_geometric.datasets import Planetoid
from torch_geometric.transforms import NormalizeFeatures
import torch.nn.functional as F
from torch_geometric.nn import GCNConv

from chrono_graph import graph
import tqdm
from torch_geometric.data import Data
import os

ALREADY_INITIALIZED = False


class GCNSampler:
  def __init__(self,
               gnn_service: str,
               shard_num: int,
               relation_name: str,
               data: Data,
               fanouts: int,
               batch_size: int = 1024,
               id_base: int = 1,
               already_initialized: bool = False):
    self.g = None
    self.gnn_service = gnn_service
    self.shard_num = shard_num
    self.relation_name = relation_name
    self.id_base = id_base
    self.fanouts = fanouts
    self.batch_size = batch_size
    self.data = data
    self.initialized = already_initialized

  def initialize_graph(self, batch_size: int = 10240):
    self.g = graph.Graph(self.gnn_service, self.shard_num)
    self.g.add_client(self.relation_name)
    if self.initialized:
      return
    self._add_edges_to_graph(self.data.edge_index, batch_size)
    self.initialized = True

  def _add_edges_to_graph(self, edge_index: torch.Tensor, batch_size: int = 10240):
    for i in tqdm.tqdm(range(0, edge_index.size(1), batch_size)):
      batch_edges = edge_index[:, i:i + batch_size]
      self.g.insert_edges(self.relation_name).more_edges(
          batch_edges[0] + self.id_base,
          batch_edges[1] + self.id_base,
      ).emit()

  def sample(self):
    if not self.initialized:
      return None
    # Random sample nodes as src
    results = self.g.random_nodes(self.relation_name, "src_nodes").batch(self.batch_size) \
        .sample_neighbor(self.relation_name, "neighbor_nodes").sample(self.fanouts).by("random", "self", True) \
        .emit()

    src_tensor = torch.tensor(results["src_nodes"]["ids"])
    neighbor_tensor = torch.tensor(results["neighbor_nodes"]["ids"])

    edge_index = self._build_edge_index(src_tensor, neighbor_tensor)

    return self._reindex_and_build_data(edge_index)

  def _build_edge_index(self, src_tensor: torch.Tensor, neighbor_tensor: torch.Tensor):
    sampler_out_row = src_tensor.repeat_interleave(self.fanouts)
    sampler_out_col = neighbor_tensor

    edge_index = torch.stack([sampler_out_row, sampler_out_col])
    return edge_index

  def _reindex_and_build_data(self, edge_index: torch.Tensor):

    unique_ids, encoded_edge_index = torch.unique(edge_index, return_inverse=True)
    index_ids = unique_ids - self.id_base

    data = Data(x=self.data.x[index_ids],
                edge_index=encoded_edge_index,
                y=self.data.y[index_ids],
                train_mask=self.data.train_mask[index_ids])
    return data


class GCNDataLoader:

  def __init__(self, node_sampler: GCNSampler, num_iterations: int = 10):
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


class GCN(torch.nn.Module):

  def __init__(self, hidden_channels):
    super(GCN, self).__init__()
    torch.manual_seed(12345)
    self.conv1 = GCNConv(dataset.num_node_features, hidden_channels)
    self.conv2 = GCNConv(hidden_channels, dataset.num_classes)

  def forward(self, x, edge_index):
    x = self.conv1(x, edge_index)
    x = x.relu()
    x = F.dropout(x, p=0.5, training=self.training)
    x = self.conv2(x, edge_index)
    return x


model_dir = os.path.dirname(os.path.abspath(__file__))
dataset = Planetoid(root=model_dir + '/data/Planetoid', name='PubMed', transform=NormalizeFeatures())
data = dataset[0]  # Get the first graph object.
print(data)

sampler = GCNSampler(gnn_service="grpc_gnn_gcn_demo-A2A",
                     shard_num=1,
                     relation_name="A2A",
                     data=data,
                     fanouts=10,
                     batch_size=1024,
                     id_base=1,
                     already_initialized=ALREADY_INITIALIZED)
sampler.initialize_graph()
train_loader = GCNDataLoader(sampler, num_iterations=10)

model = GCN(hidden_channels=16)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)
criterion = torch.nn.CrossEntropyLoss()


def train():
  model.train()

  for sub_data in train_loader:  # Iterate over each mini-batch.
    # Perform a single forward pass.
    out = model(sub_data.x, sub_data.edge_index)
    loss = criterion(out[sub_data.train_mask],
                     sub_data.y[sub_data.train_mask])  # Compute the loss solely based on the training nodes.
    loss.backward()  # Derive gradients.
    optimizer.step()  # Update parameters based on gradients.
    optimizer.zero_grad()  # Clear gradients.


def test():
  model.eval()
  out = model(data.x, data.edge_index)
  pred = out.argmax(dim=1)  # Use the class with highest probability.

  accs = []
  for mask in [data.train_mask, data.val_mask, data.test_mask]:
    # Check against ground-truth labels.
    correct = pred[mask] == data.y[mask]
    # Derive ratio of correct predictions.
    accs.append(int(correct.sum()) / int(mask.sum()))
  return accs


for epoch in range(1, 21):
  loss = train()
  train_acc, val_acc, test_acc = test()
  print(f'Epoch: {epoch:03d}, Train: {train_acc:.4f}, Val Acc: {val_acc:.4f}, Test Acc: {test_acc:.4f}')
