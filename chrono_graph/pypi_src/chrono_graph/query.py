# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

from abc import abstractmethod
from typing import List
from chrono_graph.util import strict_types
from chrono_graph.query_result import QueryResults, LayerQueryResult
import chrono_graph.libgnn_pybind as libgnn_pybind


class Query:
  """Query is a description of the query process defined under the gnn graph abstraction.

  Users generate a query to describe the query plan.
  And execute the query to initiate a query request within the graph and return the results.
  Query features:
  1. Single linked list cascade is used to describe multi-hop operations. 
      Each query can obtain execution information from the upstream node.
  2. Defines the information required for basic sampling requests, and subclasses define specific execution methods.
  """
  # The upstream query node.
  prev_node: 'Query'
  # Downstream query node
  next_node: 'Query'
  # The alias of the query. In the final returned results, use name as the result key to get the results.
  name: str
  # Result cache often needs to be referenced by downstream nodes.
  result: LayerQueryResult
  # In relation this query operate.
  relation_name: str

  def __init__(self, relation_name: str, result_key_name: str, ids: List = None):
    self.prev_node = None
    self.next_node = None
    self.result = None
    self.relation_name = relation_name
    self.name = result_key_name
    if ids is not None:
      self.result = LayerQueryResult()
      self.result.name = self.name
      self.result.ids = self.trans_ids(ids)

  def execute(self) -> QueryResults:
    node: Query = self
    while node.prev_node:
      node = node.prev_node
    results = QueryResults()
    while node:
      result = node.run()
      results.results[result.name] = result
      node = node.next_node
    return results

  @strict_types
  def emit(self) -> QueryResults:
    """ Executes the cascade query and returns the final result.
    """
    if self.next_node is not None:
      raise RuntimeError("emit call stack is not from last layer")
    return self.execute()

  def run(self) -> LayerQueryResult:
    self.result = self._run()
    return self.result

  def trans_ids(self, ids: List[int]) -> List[int]:
    """ Convert id from string to int64
    """
    if len(ids) > 0 and isinstance(ids[0], str):
      return libgnn_pybind.string_keys_to_ids(ids)
    else:
      return ids

  # The detailed execution class for each query.
  @abstractmethod
  def _run(self):
    pass

  def __repr__(self):
    return f"relation_name = {self.relation_name};"


class AttrMixin:
  """ AttrMixin defines the behavior of the current query triggering AttrQuery
  """

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  @strict_types
  def with_attr(self: Query, relation_name: str, int64_attr_len: int = 0, float_attr_len: int = 0) -> 'AttrQuery':
    """ Gives information about the relation to query and the number of attributes
    """
    node = AttrQuery(relation_name, self.name, int64_attr_len, float_attr_len)
    self.next_node = node
    node.prev_node = self
    return node


class OutVMixin:
  """ OutVMixin defines the behavior of the current query triggering NeighborQuery
  """

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  @strict_types
  def sample_neighbor(self: Query, relation_name: str, result_key_name: str) -> 'NeighborQuery':
    """ sample_neighbor Specifies the relation for neighbor sampling.
    """
    node = NeighborQuery(relation_name, result_key_name)
    self.next_node = node
    node.prev_node = self
    return node


class AttrQuery(OutVMixin, Query):
  """ Get node attrs for previous query result ids.
  """
  int64_attr_len: int
  float_attr_len: int

  @strict_types
  def __init__(self, relation_name: str, result_key_name: str, int64_attr_len: int, float_attr_len: int):
    super().__init__(relation_name, result_key_name)
    self.int64_attr_len = int64_attr_len
    self.float_attr_len = float_attr_len

  def _run(self) -> LayerQueryResult:
    prev_node_result: LayerQueryResult = self.prev_node.result
    if prev_node_result is None:
      raise RuntimeError("attr query prev node has not result")
    attrs = libgnn_pybind.nodes_attr(self.relation_name, prev_node_result.ids, self.int64_attr_len, self.float_attr_len)

    self.result = prev_node_result
    self.result.attr_exist = attrs.exist
    self.result.degrees = attrs.degrees
    self.result.int_attrs = attrs.int_attrs
    self.result.float_attrs = attrs.float_attrs
    return self.result


class NeighborQuery(AttrMixin, OutVMixin, Query):

  max_t: int
  min_t: int
  sample_count: int
  sample_type: libgnn_pybind.sample_type
  padding_type: libgnn_pybind.padding_type
  sampling_without_replacement: bool
  min_weight_required: float

  @strict_types
  def __init__(self, relation_name: str, result_key_name: str):
    super().__init__(relation_name, result_key_name)
    self.max_t = 0
    self.min_t = 0
    self.sample_count = 1
    self.sample_type = libgnn_pybind.sample_type("random")
    self.padding_type = libgnn_pybind.padding_type("self")
    self.sampling_without_replacement = False
    self.min_weight_required = 0

  def _run(self) -> LayerQueryResult:
    prev_node_result: LayerQueryResult = self.prev_node.result
    if prev_node_result is None:
      raise RuntimeError("neigh query prev node has not result")
    edges = libgnn_pybind.sample_neighbors(self.relation_name, prev_node_result.ids, self.sample_count,
                                           self.sample_type, self.padding_type, self.sampling_without_replacement,
                                           self.max_t, self.min_t, self.min_weight_required)
    if self.result is None:
      self.result = LayerQueryResult()
    self.result.name = self.name
    self.result.ids = edges.ids
    self.result.timestamps = edges.timestamps
    self.result.weights = edges.weights
    return self.result

  @strict_types
  def by(self,
         sample_type: str,
         padding_type: str,
         sampling_without_replacement: bool = False,
         min_weight_required: float = 0) -> 'NeighborQuery':
    """Sampling method parameters.

    sample_type: sampling method.
      Valid types:
              random: Random Sampling. <- default value
              weight: Weighted random sampling.
              most_recent:  Sort by time and retrieve the most recent added edge.
              least_recent: Sort by time and retrieve the least recent added edge.
              topn_weight: Sort by weight and retrieve the largest top-n edge.
    padding_type: Sampling options, default behavior when the number of neighbors is less than sample_count / the node does not exist.
      Valid types:
              zero: Fill in with 0.
              self: Fill in the node id itself. <- default value.
              neigh_loop: Use some neighbors to do loop filling. If there are no neighbors, it will go through the self logic.
    sampling_without_replacement: Whether to perform non-replacement sampling.
        Note that only some storage formats in the backend support non-replacement sampling.
    min_weight_required: Before sampling, remove all edges with weights less than this value. 
        Note that only cpt currently supports this function, and the time complexity is high.
        So it is not advisable to remove a large number of edges.
    """
    self.sample_type = libgnn_pybind.sample_type(sample_type)
    self.padding_type = libgnn_pybind.padding_type(padding_type)
    self.sampling_without_replacement = sampling_without_replacement
    self.min_weight_required = min_weight_required
    return self

  @strict_types
  def sample(self, sample_count: int) -> 'NeighborQuery':
    """ Number of samples.
    """
    self.sample_count = sample_count
    return self

  @strict_types
  def max_timestamp(self, time_s: int) -> 'NeighborQuery':
    """ Set the maximum sampling timestamp (seconds)
    """
    self.max_t = time_s
    return self

  @strict_types
  def min_timestamp(self, time_s: int) -> 'NeighborQuery':
    """ Set the minimum sampling timestamp (seconds)
    """
    self.min_t = time_s
    return self


class VertexQuery(AttrMixin, OutVMixin, Query):
  """ VertexQuery is used to describe the src node of the sampling process.

  There are several possible calling scenarios:
  1. Randomly sample queries, usually used as seeds for training.
  2. Iterator query, usually used as a seed for training.
  3. In the case of a given id seed, this is usually done by dynamic training where the seed is obtained from the sample stream.
  """
  pool_name: str
  iter_mode: bool
  sample_count: int

  @strict_types
  def __init__(self, relation_name: str, result_key_name: str, ids: List = None, iter_mode: bool = False):
    super().__init__(relation_name, result_key_name, ids)
    self.iter_mode = iter_mode
    self.sample_count = 1
    self.pool_name = ""

  def _run(self) -> LayerQueryResult:
    if self.result is None:
      self.result = LayerQueryResult()
    self.result.name = self.name
    if self.result.ids is None:
      if self.iter_mode:
        self.result.ids = libgnn_pybind.iterate_nodes(self.relation_name)
      else:
        self.result.ids = libgnn_pybind.sample_nodes(self.relation_name, self.sample_count, self.pool_name)

    return self.result

  def __repr__(self) -> str:
    return super(VertexQuery, self).__repr__() \
      + f"seed_type = {self.seed_type};"

  @strict_types
  def batch(self, batch_size: int) -> 'VertexQuery':
    """ random sample number.
    """
    if self.prev_node is not None:
      raise RuntimeError("batch api can only call once on chain beginning")
    self.sample_count = batch_size
    return self

  @strict_types
  def sample_pool(self, pool_name: str) -> 'VertexQuery':
    """ set sample from the specific pool
    """
    if self.prev_node is not None:
      raise RuntimeError("sample_pool api can only call once on chain beginning")
    self.pool_name = pool_name
    return self


class AddEQuery(Query):
  """ AddEQuery is used to describe the end point of a layer insert request.
  """
  src_ids: List[int]
  dst_ids: List[int]
  dst_weights: List[float]
  dst_timestamps: List[int]
  # attrs info
  int64_attr_len: int
  float_attr_len: int
  src_int_attrs: List[int]
  src_float_attrs: List[float]

  @strict_types
  def __init__(self, relation_name: str, int64_attr_len: int, float_attr_len: int):
    super().__init__(relation_name, "tmp_name")
    self.src_ids = []
    self.dst_ids = []
    self.dst_weights = []
    self.dst_timestamps = []
    self.int64_attr_len = int64_attr_len
    self.float_attr_len = float_attr_len
    self.src_int_attrs = []
    self.src_float_attrs = []

  def more_edges(self,
                 src_ids: List[int],
                 dst_ids: List[int] = None,
                 dst_weights: List[float] = None,
                 dst_timestamps: List[int] = None,
                 src_int64_attrs: List[int] = None,
                 src_float_attrs: List[float] = None) -> 'AddEQuery':
    """ Add a batch of edges.
    """
    if dst_ids is not None and len(dst_ids) != len(src_ids) or \
       dst_weights is not None and len(dst_weights) != len(src_ids) or \
       dst_timestamps is not None and len(dst_timestamps) != len(src_ids) or \
       src_int64_attrs is not None and len(src_int64_attrs) != len(src_ids) * self.int64_attr_len or \
       src_float_attrs is not None and len(src_float_attrs) != len(src_ids) * self.float_attr_len:
      raise RuntimeError("length don't match")

    if (dst_ids is not None and len(self.src_ids) != len(self.dst_ids) or dst_ids is None and len(self.dst_ids) != 0) or \
       (dst_weights is not None and len(self.src_ids) != len(self.dst_weights) or dst_weights is None and len(self.dst_weights) != 0) or \
       (dst_timestamps is not None and len(self.src_ids) != len(self.dst_timestamps) or dst_timestamps is None and len(self.dst_timestamps) != 0) or \
       (src_int64_attrs is not None and len(self.src_ids) * self.int64_attr_len != len(self.src_int_attrs) or src_int64_attrs is None and len(self.src_int_attrs) != 0) or \
       (src_float_attrs is not None and len(self.src_ids) * self.float_attr_len != len(self.src_float_attrs) or src_float_attrs is None and len(self.src_float_attrs) != 0):
      raise RuntimeError("the optional fields must always be empty or not empty at one query")

    self.src_ids.extend(self.trans_ids(src_ids))
    if dst_ids is not None:
      self.dst_ids.extend(self.trans_ids(dst_ids))
    if dst_weights is not None:
      self.dst_weights.extend(dst_weights)
    if dst_timestamps is not None:
      self.dst_timestamps.extend(dst_timestamps)
    if src_int64_attrs is not None:
      self.src_int_attrs.extend(src_int64_attrs)
    if src_float_attrs is not None:
      self.src_float_attrs.extend(src_float_attrs)
    return self

  def _run(self) -> LayerQueryResult:
    if self.prev_node is not None or self.next_node is not None:
      raise RuntimeError("DelEQuery can't be in query chain")

    if len(self.src_ids) > 0:
      libgnn_pybind.batch_insert(self.relation_name, False, self.src_ids, self.int64_attr_len, self.float_attr_len,
                                 self.src_int_attrs, self.src_float_attrs, self.dst_ids, self.dst_weights,
                                 self.dst_timestamps)
    result = LayerQueryResult()
    result.name = self.name
    return result


class DelEQuery(Query):
  """ DelEQuery is used to describe the end-point side of a layer of delete requests.
  """

  src_ids: List[int]
  dst_ids: List[int]

  @strict_types
  def __init__(self, relation_name: str):
    super().__init__(relation_name, "tmp_name")
    self.src_ids = []
    self.dst_ids = []

  @strict_types
  def more_edges(self, src_ids: List, dst_ids: List) -> 'DelEQuery':
    """ Add more edges for delete.
    """
    if len(src_ids) != len(dst_ids):
      raise RuntimeError("src_ids and dst_ids should have same length")
    self.src_ids.extend(self.trans_ids(src_ids))
    self.dst_ids.extend(self.trans_ids(dst_ids))
    return self

  def _run(self) -> LayerQueryResult:
    if self.prev_node is not None or self.next_node is not None:
      raise RuntimeError("DelEQuery can't be in query chain")

    if len(self.src_ids) > 0:
      libgnn_pybind.batch_insert(self.relation_name, True, self.src_ids, 0, 0, [], [], self.dst_ids, [], [])
    result = LayerQueryResult()
    result.name = self.name
    return result
