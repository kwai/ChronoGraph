// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include "base/common/gflags.h"
#include "base/jansson/json.h"
#include "base/thread/thread_pool.h"
#include "chrono_graph/src/base/gnn_type.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"
#include "chrono_graph/src/storage/sample/alias_method.h"

namespace chrono_graph {

class GraphKV;

class DoubleBufferSamplePool {
 public:
  // input = db, item info, return = weight.
  using CalcWeightFunc = std::function<float(GraphNode)>;

  DoubleBufferSamplePool();

  bool ClearBackupBuffer();
  bool InsertBackupBuffer(uint64 key);
  bool CommitAndSwitch(const CalcWeightFunc &func, const std::string &relation_name = "test");

  ErrorCode Sample(int sample_count, uint64 *result) const;

  bool IsReady() const { return ready_; }

 private:
  struct Sampler {
    Sampler() = delete;
    explicit Sampler(const std::vector<float> &dummy) : alias_method(dummy) {}
    std::vector<uint64> ids;
    AliasMethod alias_method;
  };
  std::shared_ptr<Sampler> sampler_[2];
  std::atomic_int serving_buffer_{1};
  // Is ready after finish CommitAndSwitch at least once.
  std::atomic_bool ready_{false};
};

/**
 * All kinds of sample can be abstracted to weighted sample with different weight calculation.
 */
class WeightSampleCache {
 public:
  WeightSampleCache() = delete;
  explicit WeightSampleCache(GraphKV *db);

  virtual bool Init() {
    updater_.AddTask([this]() { this->UpdateDoubleBuffers(); });
    return true;
  }
  virtual ~WeightSampleCache() {
    stop_ = true;
    updater_.StopAll();
  }

  virtual float CalcItemWeight(GraphNode item_info) = 0;

  virtual ErrorCode Sample(std::string relation_name, int sample_count, uint64 *ret) const;

  bool IsReady() const { return ready_; }

 protected:
  GraphKV *db_;
  std::atomic_bool stop_{false};
  // Is ready after finish UpdateDoubleBuffers at least once.
  std::atomic_bool ready_{false};

 private:
  void UpdateDoubleBuffers();

  // async update double buffer
  thread::ThreadPool updater_;
  // each relation has a double buffer sample pool.
  std::unordered_map<std::string, std::unique_ptr<DoubleBufferSamplePool>> caches_;
};

/**
 * all node's weight is 1.
 */
class FlattenWeightSampleCache : public WeightSampleCache {
 public:
  FlattenWeightSampleCache() = delete;
  explicit FlattenWeightSampleCache(GraphKV *db) : WeightSampleCache(db) {}

  float CalcItemWeight(GraphNode item_info) override;
};

/**
 * node's weight = its degree.
 */
class DegreeBaseSampleCache : public WeightSampleCache {
 public:
  DegreeBaseSampleCache() = delete;
  explicit DegreeBaseSampleCache(GraphKV *db) : WeightSampleCache(db) {}

  float CalcItemWeight(GraphNode item_info) override;
};

class CustomSampleCache : public WeightSampleCache {
 public:
  CustomSampleCache() = delete;
  explicit CustomSampleCache(GraphKV *db, base::Json *conf) : WeightSampleCache(db) {
    base::Json *filter_conf = conf->Get("filter");
    if (filter_conf) {
      CHECK(filter_conf->IsArray())
          << "filter config is not an array, init fail, check your config, config = " << conf->ToString()
          << ", type is " << conf->Type();
      for (size_t i = 0; i < filter_conf->size(); ++i) {
        // add filter info
        std::string filter_type = filter_conf->array()[i]->GetString("type");
        if (filter_type == "degree") {
          has_degree_filter_ = true;
          degree_lower_bound_ = filter_conf->array()[i]->GetInt("lower_bound", 5);
          degree_upper_bound_ = filter_conf->array()[i]->GetInt("upper_bound", 10000);
        } else if (filter_type == "size") {
          has_size_filter_ = true;
          size_lower_bound_ = filter_conf->array()[i]->GetInt("lower_bound", 5);
          size_upper_bound_ = filter_conf->array()[i]->GetInt("upper_bound", 10000);
        } else if (filter_type == "timestamp") {
          expire_tolerate_second_ = filter_conf->array()[i]->GetInt("expire_tolerate_second", 0);
        } else {
          LOG(WARNING) << "Unknown filter type: " << filter_type << ". Ignore.";
        }
      }
    }

    base::Json *weight_conf = conf->Get("weight");
    CHECK(weight_conf) << "Invalid sample pool config, weight config not exist: " << conf->ToString();
    std::string value_type = weight_conf->GetString("value");
    if (value_type == "uniform") {
      weight_type_ = WeightType::UNIFORM;
    } else if (value_type == "size") {
      weight_type_ = WeightType::SIZE;
    } else if (value_type == "degree") {
      weight_type_ = WeightType::DEGREE;
    } else if (value_type == "attr") {
      weight_type_ = WeightType::ATTR;
      int_index_ = weight_conf->GetInt("int_index", -1);
      float_index_ = weight_conf->GetInt("float_index", -1);
      CHECK((int_index_ != -1) ^ (float_index_ != -1))
          << "Invalid config, one of int_index and float_index must be set: " << weight_conf->ToString();
    } else {
      LOG(WARNING) << "Unknown type of value: " << value_type
                   << ". Use uniform instead (all nodes have same weight).";
    }
    base::Json *trans_conf = weight_conf->Get("trans");
    if (trans_conf) {
      std::string trans_type = trans_conf->GetString("type");
      if (trans_type == "default") {
        trans_func_ = TransFunc::DEFAULT;
      } else if (trans_type == "log") {
        trans_func_ = TransFunc::LOG;
      } else if (trans_type == "log1p") {
        trans_func_ = TransFunc::LOG1P;
      } else if (trans_type == "power") {
        trans_func_ = TransFunc::POWER;
        pow_exp_ = trans_conf->GetFloat("value", 1);
      } else {
        LOG(WARNING) << "Unknown type of trans_func: " << trans_conf->Get("type")
                     << ". Use default (no transformation) instead.";
      }
    }
  }

  float CalcItemWeight(GraphNode item_info) override;

 private:
  float GetNodeAttrValue(GraphNode item_info, int int_index, int float_index, GraphKV *db);
  // degree filter, closed interval
  bool has_degree_filter_ = false;
  int64 degree_lower_bound_ = 5;
  int64 degree_upper_bound_ = 10000;
  // size filter, closed interval
  bool has_size_filter_ = false;
  int64 size_lower_bound_ = 5;
  int64 size_upper_bound_ = 10000;
  // the key is valid only when the remaining time to expire is larger than this value.
  // Useful when don't want to sample from nodes that will be expired soon.
  // disabled if 0 is set.
  int expire_tolerate_second_ = 0;

  enum WeightType {
    UNIFORM = 0,
    SIZE = 1,
    DEGREE = 2,
    ATTR = 3,
  };
  WeightType weight_type_ = WeightType::UNIFORM;
  int int_index_ = -1;
  int float_index_ = -1;
  enum TransFunc {
    DEFAULT = 0,
    LOG = 1,
    LOG1P = 2,
    POWER = 3,
  };
  TransFunc trans_func_ = TransFunc::DEFAULT;
  float pow_exp_ = 1;
};

}  // namespace chrono_graph
