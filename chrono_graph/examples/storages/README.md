This directoy contains some examples of graph storage server config.
After you've built `chrono_graph/src/service:gnn_storage_service`, you can run the following command to start a GNN server.

# Run Server

Input files: `cluster_config.py` contains config for graph storage. `host_shard.json` contains hostnames for deploy.

```bash
# This will generate `config.json` which is used by graph storage server
python3 cluster_config.py
# Run server
./gnn_storage_service
```

# NOTES
Each relation is a KV store, and each KV store may be sharded to multiple machines.
Keys and Values are stored separatedly. Config `key_size` restricts the maximum number of keys to store,
while `total_memory` restricts the space that values can use.

For example, suppose there are 10M nodes in a relation, and each node has 100 edges.

Each node represented by a uint64 key needs 8B, so keys will take 80MB memory.

Each edge needs 26B (if using cpt_edge_list), then the total_memory for values is at least 10M * 100 * 26 = 26GB.
Considering each value's size is not fixed (determined by the length of edge_list), there will be some memory fragmentation,
thus the actual total_memory needed is somewhat larger than 26GB.
