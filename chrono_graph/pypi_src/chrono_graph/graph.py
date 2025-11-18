# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

import json
import sys
from typing import Dict, List, Any
from chrono_graph.util import strict_types
from chrono_graph.query import VertexQuery, AddEQuery, DelEQuery
import chrono_graph.libgnn_pybind as libgnn_pybind

sys.path.append(".")


class Graph:
  """Graph unifies the declaration and calling methods of multiple backend relation stores
  Design principle and usage:
  add_client initializes the connection with the backend storage and can be called with relation_name for sampling.


  Attributes:
    client_config: Dict[str, Any], Store basic config of graph, including gnn service name and shard num.
    edge_map: set[str], store the added client relation name.
  """
  _client_config: Dict[str, Any]
  _edge_map: set[str]

  @strict_types
  def __init__(self, gnn_service_name: str, service_shard_num: int):
    self._edge_map = {}
    self._client_config = {}
    self._client_config['gnn_service'] = gnn_service_name
    self._client_config['shard_num'] = service_shard_num

  @strict_types
  def add_client(self, relation_name: str, iterator_config: Dict = None) -> bool:
    """
    Add a relation client.
    example:
    ```python
    graph.Graph().add_client('U2I',  # each client has identical name
    {
      # Iterator params
      "iter_part_size": 1024,  # Return size in each iteration
      "iter_shard_num": 10, # Split the whole graph into several shards
      "iter_shard_id" : 5, # Iterate in one of the shards
    })
    ```

    Args:
      relation_name: Gnn service relation_name
      config: Iterator params, see above.

    Returns:
      bool, success.
    """
    if relation_name in self._edge_map:
      raise Exception(f"{relation_name} is already registered, table = {self._edge_map}")

    config = {}
    config['relation_name'] = relation_name
    config.update(self._client_config)
    if iterator_config is not None:
      config.update(iterator_config)

    config_str: str = json.dumps(config)
    return libgnn_pybind.add_client(relation_name, config_str)

  @strict_types
  def insert_edges(self, relation_name: str, int64_attr_len: int = 0, float_attr_len: int = 0) -> 'AddEQuery':
    """Insert node attributes and edges to a relation.
    example:
    ```python
    g.insert_edges("U2I").more_edges([10] * 3, dst_ids=[100, 200, 300]).emit()
    g.insert_edges("U2I", int64_attr_len=2, float_attr_len=2).more_edges([111, 222],
                                                                         src_int64_attrs=[88, 99] * 2,
                                                                         src_float_attrs=[2.2, 3.3] * 2).emit()
    ```

    Args:
      relation_name: Gnn service relation_name to insert
      int64_attr_len: int64 attr num for src node
      float_attr_len: float attr num for src node

    Returns:
      AddEQuery, for add more edges to insert.
    """
    return AddEQuery(relation_name, int64_attr_len, float_attr_len)

  @strict_types
  def delete_edges(self, relation_name: str) -> 'DelEQuery':
    """Delete edges of a relation.
    example:
    ```python
    g.delete_edges("U2I").more_edges([10], dst_ids=[100]).emit()
    ```

    Args:
      relation_name: Gnn service relation_name to delete

    Returns:
      DelEQuery, for add more edges to delete.
    """
    return DelEQuery(relation_name)

  @strict_types
  def set_nodes(self, relation_name: str, ids: List, result_key_name: str) -> 'VertexQuery':
    """Set node ids for later query under the relation in the graph

    A query always start with some src nodes
    This function specify the src node ids for query. 
    You can then use these ids as src nodes to query attr or neighbors.


    example:
    ```python
    g.set_nodes("U2I", [111, 222], "x")
    ```

    Args:
      relation_name: Gnn service relation_name to query
      ids: Node query id list
      result_key_name: Result returned after query emit is a Dict. 
          This parameter specifies the key of subquery is stored in the dict.

    Returns:
      VertexQuery, Record the query action information and add query actions to its continued operation.
    """
    return VertexQuery(relation_name, result_key_name, ids)

  @strict_types
  def random_nodes(self, relation_name: str, result_key_name: str) -> 'VertexQuery':
    """Random get some nodes from a relation.

      Default random 1 node from global pool. Use chain calls for more nodes or specific pool
      After random get some nodes, you can then use these ids as src nodes to query attr or neighbors.

    example:
    ```python
    g.random_nodes("U2I", "x").batch(3)
    ```

    Args:
      relation_name: Gnn service relation_name to query
      result_key_name: Result returned after query emit is a Dict. 
          This parameter specifies the key of subquery is stored in the dict.
          
    Returns:
      VertexQuery, Record the query action information and add query actions to its continued operation.
    """
    return VertexQuery(relation_name, result_key_name)

  @strict_types
  def iter(self, relation_name: str, result_key_name: str) -> 'VertexQuery':
    """ iterate nodes on a relation.

      Iterator operation is stateful. The state is stored internally within the client generated by add_client .
      Calling iter multiple times on a graph is equivalent to traversing it, and the cursor automatically moves. 
      Calling iter on different graphs for different cursor states.

    example:
    ```python
    g.iter("U2I", "x")
    ```

    Args:
      relation_name: iterator relation
      result_key_name: Result returned after query emit is a Dict. 
          This parameter specifies the key of subquery is stored in the dict.

    Returns:
      VertexQuery, Record the query action information and add query actions to its continued operation.
    """
    return VertexQuery(relation_name, result_key_name, iter_mode=True)
