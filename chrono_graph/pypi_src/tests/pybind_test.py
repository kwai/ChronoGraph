# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

"""
Some example scripts for local development.
"""
import faulthandler
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chrono_graph import graph

print("name = {}, package = {}".format(__name__, __package__))
faulthandler.enable()


def test_add_client():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.add_client("I2U")


def test_random_sample():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  print(g.random_nodes("U2I", "x").batch(10).emit())


def test_sample_neighbor():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.add_client("I2U")
  result = g.random_nodes("U2I", "x").batch(10).sample_neighbor("U2I", "y").sample(3).emit()
  print(result)
  result = g.random_nodes("U2I", "x").batch(10) \
      .sample_neighbor("U2I", "y").sample(3) \
      .sample_neighbor("I2U", "z").sample(3).emit()
  print(result)


def test_with_attr_l0():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.add_client("I2U")
  result = g.random_nodes("U2I", "x").batch(3)\
      .sample_neighbor("U2I", "y").sample(3).with_attr("I2U", int64_attr_len=4, float_attr_len=0).emit()
  print(result)


def test_with_attr_l2():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.add_client("I2U")
  result = g.random_nodes("U2I", "x").batch(3).with_attr("U2I", int64_attr_len=2, float_attr_len=2)\
      .sample_neighbor("U2I", "y").sample(3).with_attr("I2U", int64_attr_len=4, float_attr_len=0).emit()
  print(result)


def test_add_e():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.insert_edges("U2I").more_edges([10] * 3, dst_ids=[100, 200, 300]).emit()


def test_delete_e():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.insert_edges("U2I").more_edges([10] * 3, dst_ids=[100, 200, 300]).emit()
  g.delete_edges("U2I").more_edges([10], dst_ids=[100]).emit()


def test_write_attr():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.add_client("I2U")
  g.insert_edges("U2I", int64_attr_len=2, float_attr_len=2).more_edges([111, 222],
                                                                       dst_ids=[1111, 2222],
                                                                       src_int64_attrs=[88, 99] * 2,
                                                                       src_float_attrs=[2.2, 3.3] * 2).emit()
  g.insert_edges("I2U", int64_attr_len=4, float_attr_len=0).more_edges([1111, 2222, 3333],
                                                                       src_int64_attrs=[88, 99] * 6).emit()
  result = g.set_nodes("U2I", [111, 222], "x") \
      .sample_neighbor("U2I", "y").sample(3).by(sample_type="random", padding_type="self").with_attr("I2U", int64_attr_len=4, float_attr_len=0).emit()
  print(result)


def test_iterator():
  u2i_iter_config = {"iter_part_size": 1}
  i2u_iter_config = {"iter_part_size": 1}
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I", u2i_iter_config)
  g.add_client("I2U", i2u_iter_config)
  for i in range(11):
    result = g.iter("U2I", "x").emit()
    print(result)
  result = g.iter("U2I", "x") \
      .sample_neighbor("U2I", "y").sample(3).emit()
  print(result)


def test_string_keys():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.insert_edges("U2I").more_edges(["string_key_test"] * 3, dst_ids=["s100", "s200", "s300"]).emit()
  result = g.set_nodes("U2I", ["string_key_test"], "x") \
    .sample_neighbor("U2I", "y").sample(3).by(sample_type="random", padding_type="self").emit()
  print(result)


def test_write_a_lot():
  g = graph.Graph("grpc_gnn_test-U2I-I2U", 1)
  g.add_client("U2I")
  g.add_client("I2U")
  u2i_query = g.insert_edges("U2I", int64_attr_len=2, float_attr_len=2)
  i2u_query = g.insert_edges("I2U", int64_attr_len=4, float_attr_len=0)
  for i in range(100):
    u2i_query.more_edges([1000000 + i] * 20,
                         dst_ids=[2000000 + j % 100 for j in range(i, i + 20)],
                         src_int64_attrs=[i * 10, 99] * 20,
                         src_float_attrs=[i * 0.1, 3.3] * 20)
    i2u_query.more_edges([2000000 + i] * 20,
                         dst_ids=[1000000 + j % 100 for j in range(i + 10, i + 30)],
                         src_int64_attrs=[i * 20, 99] * 40)
  u2i_query.emit()
  i2u_query.emit()
