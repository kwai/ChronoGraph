# About ChronoGraph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

ChronoGraph originated from a GNN storage system for recommendation system from Kwai Inc. (快手).
It is a storage system for dynamic, distributed, heterogeneous graph.

ChronoGraph only stores graph structure, and does not store any feature or embedding. However, some extra attributes with node or edge is allowed.

The system was designed for large model training, so it is weak in data consistency in exchange for high performance reading and writing.

The core data stucture in ChronoGraph are KV tables, where each table stores all edges of a relation.
Inside a relation, each key is a node's id, and all edges starting from the node forms the value, with each edge represented by the dest node's id.
These edges are stored in a data structure called "edge list". In order to support different neighbor sampling strategies, meanwhile allow insertion and expiration with high efficiency, the most common used implementation of edge list is CPT (Cumulative Probability [Treap](https://en.wikipedia.org/wiki/Treap)). It is a binary search tree for ID, a min-heap for weight, and is the most complex data structure in ChronoGraph.

# Compile

The original developers compiled this project using bazel 7.6.1, gcc version 12.2.0 on CentOS.

# Notes

Run examples in `chrono_graph/examples`.

The RPC server implementation is synchronous currently.
