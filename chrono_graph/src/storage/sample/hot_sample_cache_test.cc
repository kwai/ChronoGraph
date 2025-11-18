// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/sample/hot_sample_cache.h"
#include <unordered_map>
#include "base/common/gflags.h"
#include "base/common/logging.h"
#include "base/common/sleep.h"
#include "base/common/string.h"
#include "base/jansson/json.h"
#include "base/testing/gtest.h"
#include "chrono_graph/src/storage/graph_kv.h"

ABSL_DECLARE_FLAG(std::string, gnn_storage_shm_dir);
ABSL_DECLARE_FLAG(int32, double_buffer_switch_interval_s);

namespace chrono_graph {
class SampleTestEnv : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    attr_op_config = new base::Json(base::StringToJson("{}"));
    attr_op_config->set("type_name", "EmptyBlock");
    edge_attr_op_config = new base::Json(base::StringToJson("{}"));
    edge_attr_op_config->set("type_name", "EmptyBlock");
    fs::remove_all(absl::GetFlag(FLAGS_gnn_storage_shm_dir));
    InitDB();
    InitData();
  }

  static void TearDownTestCase() {
    delete attr_op_config;
    delete edge_attr_op_config;
    fs::remove_all(absl::GetFlag(FLAGS_gnn_storage_shm_dir));
    auto x = std::move(db);
  }

 public:
  static std::string GetShmPath() {
    static std::string path;
    if (!path.empty()) { return path; }
    std::ostringstream stream;
    stream << absl::GetFlag(FLAGS_gnn_storage_shm_dir) << "/graph_kv_test." << base::GetTimestamp();
    return (path = stream.str());
  }

  static void InitDB() {
    static GraphStorageConfig config;
    config.service_name = "unit_test";
    config.init_config = nullptr;
    config.db_configs.push_back({.relation_name = "CLICK",
                                 .kv_shard_num = 4,
                                 .shard_memory_size = 1 << 28,
                                 .kv_dict_capacity = 1 << 18,
                                 .attr_op_config = attr_op_config,
                                 .edge_attr_op_config = edge_attr_op_config,
                                 .edge_max_num = 100,
                                 .expire_interval = 10000});
    db = std::make_unique<GraphKV>(&config);
    ASSERT_EQ(true, db->Init(GetShmPath()));
  }

  static void InitData() {
#define INSERT_USER(x) \
  do { ASSERT_EQ(ErrorCode::OK, db->InsertRelations(x, uid##x##list, "CLICK")); } while (0)

    std::vector<uint64> uid1list = {1};
    INSERT_USER(1);
    std::vector<uint64> uid2list = {1, 2};
    INSERT_USER(2);
    std::vector<uint64> uid3list = {1, 2};
    INSERT_USER(3);
    std::vector<uint64> uid4list = {1, 2};
    INSERT_USER(4);
    std::vector<uint64> uid5list = {1, 2, 3};
    INSERT_USER(5);
  }

  static std::unique_ptr<GraphKV> db;
  static base::Json *attr_op_config;
  static base::Json *edge_attr_op_config;
};
// For linker.
std::unique_ptr<GraphKV> SampleTestEnv::db = nullptr;
base::Json *SampleTestEnv::attr_op_config = nullptr;
base::Json *SampleTestEnv::edge_attr_op_config = nullptr;

TEST_F(SampleTestEnv, TestFlattenSamplerSwitchAndSample) {
  FlattenWeightSampleCache sample_cache(SampleTestEnv::db.get());
  DoubleBufferSamplePool double_buffer;
  std::vector<uint64> result;
  ASSERT_EQ(ErrorCode::RELATION_NAME_NOT_AVAILABLE, double_buffer.Sample(10, result.data()));
  // test buffer 1.
  double_buffer.ClearBackupBuffer();
  double_buffer.InsertBackupBuffer(1);
  double_buffer.InsertBackupBuffer(2);
  double_buffer.InsertBackupBuffer(3);
  using std::placeholders::_1;
  double_buffer.CommitAndSwitch(std::bind(&FlattenWeightSampleCache::CalcItemWeight, &sample_cache, _1),
                                "CLICK");

  int sample_count = 100000;
  result.resize(sample_count);
  double_buffer.Sample(sample_count, result.data());
  std::unordered_map<int, int> counter{{1, 0}, {2, 0}, {3, 0}};
  for (auto i : result) {
    ASSERT_NE(counter.end(), counter.find(i));
    counter[i] += 1;
  }
  float delta = 0.01;
  ASSERT_LE(std::fabs(((counter.find(1)->second + 0.0) / sample_count) - 0.33), delta);
  ASSERT_LE(std::fabs(((counter.find(2)->second + 0.0) / sample_count) - 0.33), delta);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / sample_count) - 0.33), delta);

  double_buffer.ClearBackupBuffer();
  double_buffer.InsertBackupBuffer(4);
  double_buffer.InsertBackupBuffer(5);
  double_buffer.CommitAndSwitch(std::bind(&FlattenWeightSampleCache::CalcItemWeight, &sample_cache, _1),
                                "CLICK");
  counter.clear();
  counter.insert({4, 0});
  counter.insert({5, 0});
  double_buffer.Sample(sample_count, result.data());
  for (auto i : result) {
    ASSERT_NE(counter.end(), counter.find(i));
    counter[i] += 1;
  }
  ASSERT_LE(std::fabs(((counter.find(4)->second + 0.0) / sample_count) - 0.5), delta);
  ASSERT_LE(std::fabs(((counter.find(5)->second + 0.0) / sample_count) - 0.5), delta);

  base::MultiMemKV *db_kernel = db->Dbs().at("CLICK").get();
  uint64 storage_key = 1;
  auto value = db_kernel->Get(storage_key);
  std::vector<base::KVSyncData> temp_sync_data;
  temp_sync_data.push_back(
      {storage_key, value.data, value.size, base::GetTimestamp() / base::Time::kMicrosecondsPerSecond + 1});
  db_kernel->MultiSync(temp_sync_data);
  base::SleepForSeconds(1);
  // test buffer 1.
  double_buffer.ClearBackupBuffer();
  double_buffer.InsertBackupBuffer(1);
  double_buffer.InsertBackupBuffer(2);
  double_buffer.InsertBackupBuffer(3);
  double_buffer.CommitAndSwitch(std::bind(&FlattenWeightSampleCache::CalcItemWeight, &sample_cache, _1),
                                "CLICK");
  counter.clear();
  counter.insert({1, 0});
  counter.insert({2, 0});
  counter.insert({3, 0});
  double_buffer.Sample(sample_count, result.data());
  for (auto i : result) {
    ASSERT_NE(counter.end(), counter.find(i));
    counter[i] += 1;
  }
  ASSERT_LE(std::fabs(((counter.find(1)->second + 0.0) / sample_count) - 0), delta);
  ASSERT_LE(std::fabs(((counter.find(2)->second + 0.0) / sample_count) - 0.5), delta);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / sample_count) - 0.5), delta);
  // restore
  value = db_kernel->Get(storage_key);
  temp_sync_data.clear();
  temp_sync_data.push_back({storage_key, value.data, value.size, 0});
  db_kernel->MultiSync(temp_sync_data);
}

TEST_F(SampleTestEnv, TestDegreeSamplerSwitchAndSample) {
  DegreeBaseSampleCache sample_cache(SampleTestEnv::db.get());
  DoubleBufferSamplePool double_buffer;
  std::vector<uint64> result;
  ASSERT_EQ(ErrorCode::RELATION_NAME_NOT_AVAILABLE, double_buffer.Sample(10, result.data()));
  // test buffer 1.
  double_buffer.ClearBackupBuffer();
  double_buffer.InsertBackupBuffer(1);
  double_buffer.InsertBackupBuffer(2);
  double_buffer.InsertBackupBuffer(3);
  using std::placeholders::_1;
  double_buffer.CommitAndSwitch(std::bind(&DegreeBaseSampleCache::CalcItemWeight, &sample_cache, _1),
                                "CLICK");

  int sample_count = 100000;
  result.resize(sample_count);
  double_buffer.Sample(sample_count, result.data());
  std::unordered_map<int, int> counter{{1, 0}, {2, 0}, {3, 0}};
  for (auto i : result) {
    ASSERT_NE(counter.end(), counter.find(i));
    counter[i] += 1;
  }
  float delta = 0.01;
  ASSERT_LE(std::fabs(((counter.find(1)->second + 0.0) / sample_count) - 0.2), delta);
  ASSERT_LE(std::fabs(((counter.find(2)->second + 0.0) / sample_count) - 0.4), delta);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / sample_count) - 0.4), delta);

  double_buffer.ClearBackupBuffer();
  double_buffer.InsertBackupBuffer(4);
  double_buffer.InsertBackupBuffer(5);
  double_buffer.CommitAndSwitch(std::bind(&DegreeBaseSampleCache::CalcItemWeight, &sample_cache, _1),
                                "CLICK");
  counter.clear();
  counter.insert({4, 0});
  counter.insert({5, 0});
  double_buffer.Sample(sample_count, result.data());
  for (auto i : result) {
    ASSERT_NE(counter.end(), counter.find(i));
    counter[i] += 1;
  }
  ASSERT_LE(std::fabs(((counter.find(4)->second + 0.0) / sample_count) - 0.4), delta);
  ASSERT_LE(std::fabs(((counter.find(5)->second + 0.0) / sample_count) - 0.6), delta);
}

void InsertData() {
  int insert_time_ms = 0;
  int insert_interval_ms = 50;
  while (insert_time_ms < 6000) {
    std::vector<uint64> dest{1, 2, 3};
    SampleTestEnv::db->InsertRelations(insert_time_ms, dest, "CLICK");
    insert_time_ms += insert_interval_ms;
    base::SleepForMilliseconds(insert_interval_ms);
  }
}

TEST_F(SampleTestEnv, MultiThreadSample) {
  thread::ThreadPool pool(1);
  pool.AddTask([]() { InsertData(); });
  absl::SetFlag(&FLAGS_double_buffer_switch_interval_s, 20);
  DegreeBaseSampleCache cache(SampleTestEnv::db.get());
  ASSERT_EQ(cache.Init(), true);
  std::vector<uint64> result(10, 0);
  for (size_t i = 0; i < 600; ++i) {
    cache.Sample("CLICK", 10, result.data());
    LOG_EVERY_N_SEC(INFO, 1) << "result = " << absl::StrJoin(result, ",");
    base::SleepForMilliseconds(10);
  }
  pool.JoinAll();
}

}  // namespace chrono_graph
