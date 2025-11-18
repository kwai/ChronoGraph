// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/base/config_helper.h"

#include "base/common/logging.h"
#include "base/jansson/json.h"

namespace chrono_graph {

const base::Json *GnnDynamicJsonConfig::GetHostConfig() const {
  if (!initialized_) {
    std::string file_content;
    base::ReadFileToString("./config.json", &file_content);
    CHECK(!file_content.empty()) << "config.json is empty";
    LOG(INFO) << "Got config content: " << file_content;
    base::Json whole_json_config(base::StringToJson(file_content));
    auto map_config = whole_json_config.Get("host_service_config");
    CHECK(map_config) << "host_service_config is invalid";
    std::string host = base::GetHostName();
    LOG(INFO) << "Current hostname: " << host;
    auto host_conf = map_config->Get(host);
    if (host_conf == nullptr) host_conf = map_config->Get("default");

    bool config_valid = host_conf && host_conf->IsString();
    CHECK(config_valid) << "Host service config is invalid:" << host_conf->ToString();
    ParseConfig(&whole_json_config, host_conf);
  }
  return &host_config_;
}

void GnnDynamicJsonConfig::ParseConfig(const base::Json *whole_json_config,
                                       const base::Json *host_service_config) const {
  std::string host_config_key = host_service_config->StringValue("");
  CHECK(!host_config_key.empty()) << "Host should select one config key from service config";
  auto service_config_set = whole_json_config->Get("service_config");
  CHECK(service_config_set && service_config_set->IsObject()) << "Invalid service config.";
  auto db_list = whole_json_config->Get("db_list");
  CHECK(db_list && db_list->IsObject()) << "Invalid db list config.";
  auto service_config_by_host = service_config_set->Get(host_config_key);
  CHECK(service_config_by_host && service_config_by_host->IsObject());

  // process db config
  base::Json dbs(base::StringToJson("[]"));
  auto host_db_names = service_config_by_host->Get("dbs");
  CHECK(host_db_names && host_db_names->IsArray());
  for (auto elem : host_db_names->array()) {
    CHECK(elem && elem->IsString());
    auto db_object = db_list->Get(elem->StringValue(""));
    CHECK(db_object && db_object->IsObject());
    dbs.append(*db_object);
  }
  host_config_.set("dbs", base::Json(dbs.get()));

  // process service name
  auto service_name = service_config_by_host->GetString("service_name", "");
  host_config_.set("service_name", service_name);

  // process shard info
  host_config_.set("shard_id", service_config_by_host->GetInt("shard_id", 0));
  host_config_.set("shard_num", service_config_by_host->GetInt("shard_num", 1));

  // process exp name
  host_config_.set("exp_name", service_config_by_host->GetString("exp_name", ""));

  // process init method
  auto init_list = whole_json_config->Get("init_list");
  auto init = service_config_by_host->Get("init");
  if (!init) { init = service_config_set->Get("default_init"); }

  do {
    // no init config, just skip.
    if (!init) { break; }
    if (init->IsString() && init_list && init_list->IsObject()) {
      std::string init_name = init->StringValue("");
      if (!init_name.empty()) {
        auto init_method = init_list->Get(init_name);
        CHECK(init_method && init_method->IsObject()) << "Invalid init method";
        host_config_.set("init", base::Json(base::StringToJson(base::JsonToString(init_method->get()))));
      }
      break;
    }
    if (init->IsArray()) {
      host_config_.set("init", base::Json(base::StringToJson(base::JsonToString(init->get()))));
      break;
    }
    LOG(ERROR) << "init format error: " << init->ToString();
  } while (false);

  // process checkpoint.
  if (service_config_by_host->Get("checkpoint") && service_config_by_host->Get("checkpoint")->IsObject()) {
    host_config_.set(
        "checkpoint",
        base::Json(base::StringToJson(base::JsonToString(service_config_by_host->Get("checkpoint")->get()))));
  }
  // process exporter.
  if (service_config_by_host->Get("exporter") && service_config_by_host->Get("exporter")->IsObject()) {
    host_config_.set(
        "exporter",
        base::Json(base::StringToJson(base::JsonToString(service_config_by_host->Get("exporter")->get()))));
  }
  // process sample_pool.
  if (service_config_by_host->Get("sample_pool") && service_config_by_host->Get("sample_pool")->IsArray()) {
    host_config_.set("sample_pool",
                     base::Json(base::StringToJson(
                         base::JsonToString(service_config_by_host->Get("sample_pool")->get()))));
  }

  initialized_ = true;
}

const std::vector<std::string> &RouteTableJsonConfig::GetRouteConfig(const std::string &relation_name,
                                                                     size_t shard_id) const {
  std::string file_content;
  base::ReadFileToString("./route_config.json", &file_content);
  base::Json whole_json_config(base::StringToJson(file_content));
  if (!initialized_) {
    CHECK(!file_content.empty()) << "Invalid config.";

    auto relation_config = whole_json_config.Get(relation_name);
    CHECK(relation_config) << "Relation name not found in route config, relation_name: " << relation_name;
    CHECK(relation_config->IsArray()) << "Invalid relation config:" << relation_config->ToString();

    auto shard_config = relation_config->Get(shard_id);
    CHECK(shard_config) << "Shard ID exceeds the length of relation config array, shard_id: " << shard_id;
    CHECK(shard_config->IsArray()) << "Invalid shard_config:" << shard_config->ToString();

    for (size_t i = 0; i < shard_config->size(); ++i) {
      std::string ip_port;
      CHECK(shard_config->GetString(i, &ip_port)) << shard_config->ToString() << ", i = " << i;
      hosts_.emplace_back(ip_port);
    }
    initialized_ = true;
  }
  return hosts_;
}

}  // namespace chrono_graph
