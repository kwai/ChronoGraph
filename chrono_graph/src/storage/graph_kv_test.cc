// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include <algorithm>
#include <functional>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "base/testing/gtest.h"
#include "chrono_graph/src/base/proto_helper.h"
#include "chrono_graph/src/storage/config.h"
#include "chrono_graph/src/storage/graph_kv.h"

ABSL_DECLARE_FLAG(std::string, gnn_storage_shm_dir);

namespace chrono_graph {

#define INSERT_UIPAIR(x)                                                                               \
  do {                                                                                                 \
    std::vector<uint64> u##x{x};                                                                       \
    ASSERT_EQ(ErrorCode::OK, db->InsertRelations(x, uid##x##list, "CLICK"));                           \
    for (auto id : uid##x##list) ASSERT_EQ(ErrorCode::OK, db->InsertRelations(id, u##x, "I2U_CLICK")); \
  } while (0)

class GraphKVTestEnv : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    fs::remove_all(absl::GetFlag(FLAGS_gnn_storage_shm_dir));
    attr_op_config = new base::Json(base::StringToJson("{}"));
    attr_op_config->set("type_name", "EmptyBlock");
    edge_attr_op_config = new base::Json(base::StringToJson("{}"));
    edge_attr_op_config->set("type_name", "EmptyBlock");
    InitDB();
  }

  static void TearDownTestCase() {
    delete attr_op_config;
    delete edge_attr_op_config;
    fs::remove_all(absl::GetFlag(FLAGS_gnn_storage_shm_dir));
    db.reset();
  }

 public:
  static std::string GetShmPath() {
    static std::string path;
    if (!path.empty()) { return path; }
    std::ostringstream stream;
    stream << absl::GetFlag(FLAGS_gnn_storage_shm_dir) << "/graph_kv_test." << base::GetTimestamp();
    return (path = stream.str());
  }

  static void InsertRelations() {
    std::vector<uint64> uid1list = {100, 102, 106, 108, 110};
    INSERT_UIPAIR(1);
    std::vector<uint64> uid2list = {101, 106, 108};
    INSERT_UIPAIR(2);
    std::vector<uint64> uid3list = {102, 103, 105, 107, 109};
    INSERT_UIPAIR(3);
    std::vector<uint64> uid4list = {100, 102, 104, 106};
    INSERT_UIPAIR(4);
    std::vector<uint64> uid5list = {103, 108};
    INSERT_UIPAIR(5);
  }

  static void InitDB() {
    static GraphStorageConfig config;
    config.service_name = "unit_test";
    config.init_config = nullptr;
    GraphStorageDBConfig *config_ptr =
        new GraphStorageDBConfig({.relation_name = "CLICK",
                                  .kv_shard_num = 4,
                                  .shard_memory_size = 1 << 28,  // MemKV needs at least 16M memory.
                                  .kv_dict_capacity = 1 << 18,
                                  .attr_op_config = attr_op_config,
                                  .edge_attr_op_config = edge_attr_op_config,
                                  .edge_max_num = 10000,
                                  .expire_interval = 10000});
    config.db_configs.push_back(*config_ptr);
    // Reverse relation
    config_ptr->relation_name = "I2U_CLICK";
    config.db_configs.push_back(*config_ptr);
    for (const auto &db_config : config.db_configs) { LOG(INFO) << db_config.ToString(); }

    db = std::make_unique<GraphKV>(&config);

    ASSERT_EQ(true, GraphKVTestEnv::db->Init(GetShmPath()));
    GraphKVTestEnv::db->WaitReady();
    InsertRelations();
    GraphKVTestEnv::db->WaitSamplerReady();
  }
  static std::unique_ptr<GraphKV> db;
  static base::Json *attr_op_config;
  static base::Json *edge_attr_op_config;
};
// For linker to find definition.
std::unique_ptr<GraphKV> GraphKVTestEnv::db = nullptr;
base::Json *GraphKVTestEnv::attr_op_config = nullptr;
base::Json *GraphKVTestEnv::edge_attr_op_config = nullptr;

/**** Construct graph:
 *
 *  -------------------------------------------------------------------------------
 *  | uid / pid | 100 | 101 | 102 | 103 | 104 | 105 | 106 | 107 | 108 | 109 | 110 |
 *  |   1       |  1  |  0  |  1  |  0  |  0  |  0  |  1  |  0  |  1  |  0  |  1  |
 *  |-----------------------------------------------------------------------------|
 *  |   2       |  0  |  1  |  0  |  0  |  0  |  0  |  1  |  0  |  1  |  0  |  0  |
 *  |-----------------------------------------------------------------------------|
 *  |   3       |  0  |  0  |  1  |  1  |  0  |  1  |  0  |  1  |  0  |  1  |  0  |
 *  |-----------------------------------------------------------------------------|
 *  |   4       |  1  |  0  |  1  |  0  |  1  |  0  |  1  |  0  |  0  |  0  |  0  |
 *  |-----------------------------------------------------------------------------|
 *  |   5       |  0  |  0  |  0  |  1  |  0  |  0  |  0  |  0  |  1  |  0  |  0  |
 *  |-----------------------------------------------------------------------------|
 */

#define SHOW_NODELIST(result, relation_name)                                                   \
  do {                                                                                         \
    std::ostringstream s;                                                                      \
    s << "result: {";                                                                          \
    for (size_t i = 0; i < result.node_id_size(); ++i) {                                       \
      auto id = result.node_id(i);                                                             \
      s << "[id = " << id << ", degree = " << db->GetNodeDegree({id, relation_name}) << "], "; \
    }                                                                                          \
    s << "}.";                                                                                 \
    std::cout << s.str() << std::endl;                                                         \
  } while (0)

TEST_F(GraphKVTestEnv, SamplingTest2) {
  uint64 user_id = 3;
  EdgeList result;
  auto result_write = EdgeListWriter(&result, 3).Slice(0);
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_RANDOM);
  param.set_sampling_num(3);
  ASSERT_EQ(ErrorCode::OK, db->SampleNeighbors(user_id, "CLICK", param, "", &result_write));
  SHOW_NODELIST(result, "CLICK");
  std::set<uint64> id_list = {102, 103, 105, 107, 109};
  ASSERT_FALSE(id_list.end() == id_list.find(result.node_id(0)));
  ASSERT_FALSE(id_list.end() == id_list.find(result.node_id(1)));
  ASSERT_FALSE(id_list.end() == id_list.find(result.node_id(2)));
}

TEST_F(GraphKVTestEnv, SamplingTest3) {
  uint64 user_id = 4;
  EdgeList result;
  auto result_writer = EdgeListWriter(&result, 3).Slice(0);
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_MOST_RECENT);
  param.set_sampling_num(3);
  ASSERT_EQ(ErrorCode::OK, db->SampleNeighbors(user_id, "CLICK", param, "", &result_writer));
  SHOW_NODELIST(result, "CLICK");
  ASSERT_EQ(106, result.node_id(0));
  ASSERT_EQ(104, result.node_id(1));
  ASSERT_EQ(102, result.node_id(2));
}

TEST_F(GraphKVTestEnv, SamplingTest4) {
  uint64 user_id = 2;
  EdgeList result;
  auto result_writer = EdgeListWriter(&result, 3).Slice(0);
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_LEAST_RECENT);
  param.set_sampling_num(3);
  ASSERT_EQ(ErrorCode::OK, db->SampleNeighbors(user_id, "CLICK", param, "", &result_writer));
  SHOW_NODELIST(result, "CLICK");
  ASSERT_EQ(101, result.node_id(0));
  ASSERT_EQ(106, result.node_id(1));
  ASSERT_EQ(108, result.node_id(2));
}

TEST_F(GraphKVTestEnv, SampleNeighborTest) {
  std::vector<uint64> user_ids{1, 2, 3};
  int sampling_num = 3;
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_RANDOM);
  param.set_sampling_num(sampling_num);
  EdgeList list;
  EdgeListWriter writer(&list, user_ids.size() * sampling_num);
  for (int offset = 0; offset < user_ids.size(); ++offset) {
    auto slice = writer.Slice(offset * sampling_num);
    ASSERT_EQ(ErrorCode::OK, db->SampleNeighbors(user_ids[offset], "CLICK", param, "", &slice));
  }
  std::vector<std::set<uint64>> u_pid_list = {
      {100, 102, 106, 108, 110}, {101, 106, 108}, {102, 103, 105, 107, 109}};
  for (int offset = 0; offset < user_ids.size(); ++offset) {
    for (int sample_offset = 0; sample_offset < sampling_num; ++sample_offset) {
      auto node_id = list.node_id(offset * sampling_num + sample_offset);
      ASSERT_FALSE(u_pid_list[offset].end() == u_pid_list[offset].find(node_id));
    }
  }
}

TEST_F(GraphKVTestEnv, SamplingTestFail) {
  uint64 user_id = 100;
  EdgeList result;
  auto result_writer = EdgeListWriter(&result, 3).Slice(0);
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_LEAST_RECENT);
  param.set_sampling_num(3);
  ASSERT_EQ(ErrorCode::KEY_NOT_EXIST, db->SampleNeighbors(user_id, "CLICK", param, "", &result_writer));
}

TEST_F(GraphKVTestEnv, GetNodeInfoTest) {
  ProtoList<uint64> uid_list;
  ProtoList<int> uid_type_list;
  uid_list.Add(1);
  uid_list.Add(3);
  uid_list.Add(10);
  uid_list.Add(5);
  ProtoPtrList<NodeAttr> result;
  ASSERT_EQ(true, db->GetNodeInfos(uid_list, "CLICK", &result));
  ASSERT_EQ(4, result.size());
  ASSERT_EQ(1, result[0].id());
  ASSERT_EQ(5, result[0].degree());
  ASSERT_EQ(3, result[1].id());
  ASSERT_EQ(5, result[1].degree());
  ASSERT_EQ(0, result[2].id());
  ASSERT_EQ(0, result[2].degree());
  ASSERT_EQ(5, result[3].id());
  ASSERT_EQ(2, result[3].degree());

  ProtoList<uint64> pid_list;
  ProtoList<int> pid_type_list;
  pid_list.Add(103);
  pid_list.Add(104);
  pid_list.Add(106);
  ASSERT_EQ(true, db->GetNodeInfos(pid_list, "I2U_CLICK", &result));
  ASSERT_EQ(3, result.size());
  ASSERT_EQ(103, result[0].id());
  ASSERT_EQ(2, result[0].degree());
  ASSERT_EQ(104, result[1].id());
  ASSERT_EQ(1, result[1].degree());
  ASSERT_EQ(106, result[2].id());
  ASSERT_EQ(3, result[2].degree());
}

TEST_F(GraphKVTestEnv, RandomSampleTest) {
  std::set<uint64> pid_set = {100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110};
  std::set<uint64> uid_set = {1, 2, 3, 4, 5};

  ProtoList<uint64> result_ids;
  ASSERT_EQ(OK, db->RandomSample("CLICK", 3, &result_ids));
  ASSERT_EQ(3, result_ids.size());
  for (size_t i = 0; i < 3; ++i) { std::cout << "id = " << result_ids.Get(i) << std::endl; }
  for (size_t i = 0; i < 3; ++i) { ASSERT_NE(uid_set.find(result_ids.Get(i)), uid_set.end()); }

  result_ids.Clear();
  ASSERT_EQ(OK, db->RandomSample("I2U_CLICK", 3, &result_ids));
  ASSERT_EQ(3, result_ids.size());
  for (size_t i = 0; i < 3; ++i) { std::cout << "id = " << result_ids.Get(i) << std::endl; }
  for (size_t i = 0; i < 3; ++i) { ASSERT_NE(pid_set.find(result_ids.Get(i)), pid_set.end()); }
}

TEST_F(GraphKVTestEnv, TraversalByPartTest) {
  TraversalByPartResponse response;
  int count = 0;
  ASSERT_EQ(db->TraversalByPart("CLICK", 4, 0, response.mutable_ids()), ErrorCode::OK);
  count += response.ids_size();
  ASSERT_EQ(db->TraversalByPart("CLICK", 4, 1, response.mutable_ids()), ErrorCode::OK);
  count += response.ids_size();
  ASSERT_EQ(db->TraversalByPart("CLICK", 4, 2, response.mutable_ids()), ErrorCode::OK);
  count += response.ids_size();
  ASSERT_EQ(db->TraversalByPart("CLICK", 4, 3, response.mutable_ids()), ErrorCode::OK);
  count += response.ids_size();
  ASSERT_EQ(5, count);  // [1, 2, 3, 4, 5]
  count = 0;
  ASSERT_EQ(db->TraversalByPart("I2U_CLICK", 4, 0, response.mutable_ids()), ErrorCode::OK);
  count += response.ids_size();
  ASSERT_EQ(db->TraversalByPart("I2U_CLICK", 4, 1, response.mutable_ids()), ErrorCode::OK);
  count += response.ids_size();
  ASSERT_EQ(db->TraversalByPart("I2U_CLICK", 4, 2, response.mutable_ids()), ErrorCode::OK);
  count += response.ids_size();
  ASSERT_EQ(db->TraversalByPart("I2U_CLICK", 4, 3, response.mutable_ids()), ErrorCode::OK);
  count += response.ids_size();
  ASSERT_EQ(11, count);  // [100, 101, ..., 110]
}

TEST_F(GraphKVTestEnv, TestExpire) {
  uint64 user_id = 1;
  auto db_kernel = db->Dbs().at("CLICK").get();
  // auto value = db_kernel->Get(user_id);
  auto old_expire = 0;
  db_kernel->Update(
      user_id,
      nullptr,
      0,
      0,
      [&](uint32 addr, uint64 key, const char *log, int log_size, int dummy_expire_timet, base::MemKV *mem_kv)
          -> uint32 {
        base::KVData *data = mem_kv->GetKVDataByAddr(addr);
        old_expire = data->expire_timet;
        data->expire_timet = base::GetTimestamp() / base::Time::kMicrosecondsPerSecond + 1;
        return addr;
      });
  // Wait for expire
  base::SleepForSeconds(2);

  EdgeList result;
  auto result_writer = EdgeListWriter(&result, 4).Slice(0);

  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_RANDOM);
  // sampling_num = -1 doesn't break test when strategy is all.
  param.set_sampling_num(2);
  ASSERT_EQ(ErrorCode::KEY_NOT_EXIST, db->SampleNeighbors(user_id, "CLICK", param, "", &result_writer));
  db_kernel->Update(
      user_id,
      nullptr,
      0,
      0,
      [&](uint32 addr, uint64 key, const char *log, int log_size, int dummy_expire_timet, base::MemKV *mem_kv)
          -> uint32 {
        base::KVData *data = mem_kv->GetKVDataByAddr(addr);
        data->expire_timet = old_expire;
        return addr;
      });
  ASSERT_EQ(ErrorCode::OK, db->SampleNeighbors(user_id, "CLICK", param, "", &result_writer));
}

TEST_F(GraphKVTestEnv, SamplingTestUDFWeight) {
  uint64 in_id = 10000;
  std::vector<uint64> out_ids{1, 2, 3, 4};
  std::vector<float> out_weights = {1, 2, 3, 4};
  ASSERT_EQ(ErrorCode::OK, db->InsertRelations(in_id, out_ids, "CLICK", "", std::move(out_weights)));

  int sample_time = 100000, sample_num = 3;
  EdgeList list;
  EdgeListWriter writer(&list, sample_num * sample_time);
  SamplingParam param;
  std::unordered_map<int, int> counter;
  for (int i = 1; i < 5; ++i) { counter.insert({i, 0}); }

  param.set_strategy(SamplingParam_SamplingStrategy_EDGE_WEIGHT);
  param.set_sampling_num(sample_num);
  for (size_t i = 0; i < sample_time; ++i) {
    auto slice = writer.Slice(i * sample_num);
    ASSERT_EQ(ErrorCode::OK, db->SampleNeighbors(10000, "CLICK", param, "", &slice));
    ASSERT_EQ(slice.offset, i * sample_num + 3);
  }

  auto reader = writer.Slice(0);
  for (int offset = 0; offset < sample_time * sample_num; ++offset) {
    int node_id = reader.node_id->Get(offset);
    auto iter = counter.find(node_id);
    ASSERT_NE(iter, counter.end());
    iter->second += 1;
  }

  for (auto item : counter) {
    std::cout << "key = " << item.first << ", occur time = " << item.second
              << ", ratio = " << ((item.second + 0.0) / (sample_time * sample_num)) << std::endl;
  }

  float delta = 0.01;
  ASSERT_LE(std::fabs(((counter.find(1)->second + 0.0) / (sample_num * sample_time)) - 0.1), delta);
  ASSERT_LE(std::fabs(((counter.find(2)->second + 0.0) / (sample_num * sample_time)) - 0.2), delta);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / (sample_num * sample_time)) - 0.3), delta);
  ASSERT_LE(std::fabs(((counter.find(4)->second + 0.0) / (sample_num * sample_time)) - 0.4), delta);
  std::vector<base::KVSyncData> temp_sync_data;
  temp_sync_data.push_back(
      base::KVSyncData(10000, nullptr, 0, base::GetTimestamp() / base::Time::kMicrosecondsPerSecond + 1));
  db->Dbs().at("CLICK")->MultiSync(temp_sync_data);
}

}  // namespace chrono_graph
