// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <vector>
#include "base/common/file_util.h"
#include "base/common/logging.h"
#include "base/jansson/json.h"
#include "base/util/system_util.h"

namespace chrono_graph {

#define CHECK_JSON_CONFIG(config, name, type_method)                                                      \
  CHECK(config) << "check config: " << (name) << " fail, config is null";                                 \
  CHECK((config)->Get(name)) << "check config: " << (name) << " not in config: " << (config)->ToString(); \
  CHECK((config)->Get(name)->Is##type_method())                                                           \
      << "check config fail: " << (name) << " is not a " << #type_method;

inline int64 GetInt64ConfigSafe(const base::Json *config, const std::string &name) {
  CHECK_JSON_CONFIG(config, name, Integer);
  int64 ret = config->GetInt(name, 0);
  LOG(INFO) << name << " = " << ret;
  return ret;
}

inline std::string GetStringConfigSafe(const base::Json *config, const std::string &name) {
  CHECK_JSON_CONFIG(config, name, String);
  std::string ret = config->GetString(name, "");
  LOG(INFO) << name << " = " << ret;
  return ret;
}

inline const std::vector<base::Json *> &GetArrayConfigSafe(const base::Json *config,
                                                           const std::string &name) {
  CHECK_JSON_CONFIG(config, name, Array);
  CHECK(!config->Get(name)->array().empty()) << name << " is an empty array";
  return config->Get(name)->array();
}

inline base::Json *GetObjConfigSafe(const base::Json *config, const std::string &name) {
  CHECK_JSON_CONFIG(config, name, Object);
  return config->Get(name);
}

class GnnDynamicJsonConfig {
 public:
  static const GnnDynamicJsonConfig *GetConfig() {
    static GnnDynamicJsonConfig instance;
    return &instance;
  }

  const base::Json *GetHostConfig() const;

 protected:
  void ParseConfig(const base::Json *whole_json_config, const base::Json *host_service_config) const;
  GnnDynamicJsonConfig() : initialized_(false), host_config_(base::StringToJson("{}")) {}

 private:
  mutable bool initialized_;
  mutable base::Json host_config_;
  DISALLOW_COPY_AND_ASSIGN(GnnDynamicJsonConfig);
};

class RouteTableJsonConfig {
 public:
  static const RouteTableJsonConfig *GetInstance() {
    static RouteTableJsonConfig instance;
    return &instance;
  }

  const std::vector<std::string> &GetRouteConfig(const std::string &relation_name, size_t shard_id) const;

 private:
  RouteTableJsonConfig() : initialized_(false), route_config_(base::StringToJson("{}")) {}

  mutable bool initialized_;
  mutable base::Json route_config_;
  // hosts_[shard_id] = list of `ip:port`
  mutable std::vector<std::string> hosts_;
  DISALLOW_COPY_AND_ASSIGN(RouteTableJsonConfig);
};

}  // namespace chrono_graph
