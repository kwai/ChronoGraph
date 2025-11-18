// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include <cmath>

#include "base/jansson/jansson.h"
#include "base/testing/gtest.h"
#include "chrono_graph/src/base/config_helper.h"

namespace chrono_graph {

class TestGnnDynamicJsonConfig : public GnnDynamicJsonConfig {
 public:
  TestGnnDynamicJsonConfig(const base::Json &whole_json_config, const base::Json &host_config_key)
      : GnnDynamicJsonConfig() {
    ParseConfig(&whole_json_config, &host_config_key);
  }
};

bool JsonContentCompare(const base::Json *a, const base::Json *b) {
  if ((a == nullptr && b != nullptr) || (a != nullptr && b == nullptr)) {
    return false;
  } else if (a == nullptr && b == nullptr) {
    return true;
  } else {
    if (a->IsInteger() && b->IsInteger()) {
      auto a_content = a->IntValue(-1);
      auto b_content = b->IntValue(-1);
      return a_content == b_content;
    } else if (a->IsDouble() && b->IsDouble()) {
      auto a_content = a->FloatValue(-1.0);
      auto b_content = b->FloatValue(-1.0);
      return std::abs(a_content - b_content) < 0.000001;
    } else if (a->IsString() && b->IsString()) {
      auto a_content = a->StringValue("");
      auto b_content = b->StringValue("");
      return a_content == b_content;
    } else if (a->IsBoolean() && b->IsBoolean()) {
      auto a_content = a->BooleanValue(false);
      auto b_content = b->BooleanValue(false);
      return a_content == b_content;
    } else if (a->IsArray() && b->IsArray()) {
      auto &a_array = a->array();
      auto &b_array = b->array();
      if (a_array.size() != b_array.size()) { return false; }
      int sum = 0;
      for (size_t i = 0; i < a_array.size(); ++i) {
        for (int j = 0; j < b_array.size(); ++j) {
          bool result = JsonContentCompare(a_array[i], b_array[j]);
          if (result) { sum += 1; }
        }
      }
      return sum == a_array.size();
    } else if (a->IsObject() && b->IsObject()) {
      auto &a_map = a->objects();
      auto &b_map = b->objects();
      if (a_map.size() != b_map.size()) { return false; }
      for (auto &elem : a_map) {
        auto key = elem.first;
        if (b_map.find(key) == b_map.end()) { return false; }
        auto iter = b_map.find(key);
        bool ret = JsonContentCompare(elem.second, iter->second);
        if (!ret) { return false; }
      }
      return true;
    }
    return false;
  }
}

TEST(TestGnnDynamicJsonConfig, JsonCompare) {
  std::string string_a = R"({
      "a" : 23
  })";
  std::string string_b = R"({
      "a" : 24
  })";
  base::Json json_a(base::StringToJson(string_a));
  base::Json json_b(base::StringToJson(string_b));
  EXPECT_FALSE(JsonContentCompare(&json_a, &json_b));

  std::string string_c = R"({
      "a" : [23, 34]
  })";
  std::string string_d = R"({
      "a" : [23, 34]
  })";
  base::Json json_c(base::StringToJson(string_c));
  base::Json json_d(base::StringToJson(string_d));
  EXPECT_TRUE(JsonContentCompare(&json_c, &json_d));

  std::string string_e = R"({
      "a" : "23"
  })";
  std::string string_f = R"({
      "a" : "24"
  })";
  base::Json json_e(base::StringToJson(string_e));
  base::Json json_f(base::StringToJson(string_f));
  EXPECT_FALSE(JsonContentCompare(&json_e, &json_f));

  std::string string_g = R"({
      "a" : {
            "23" : 34
      }
  })";
  std::string string_h = R"({
      "a" : {
        "24" : 23
      }
  })";
  base::Json json_g(base::StringToJson(string_g));
  base::Json json_h(base::StringToJson(string_h));
  EXPECT_FALSE(JsonContentCompare(&json_g, &json_h));

  std::string string_i = R"({
      "a" : [ {
        "b" : 23
      }, {
        "c" : 24
      }]
  })";
  std::string string_j = R"({
      "a" : [ {
        "b" : 23
      }, {
        "c" : 24
      }]
  })";
  base::Json json_i(base::StringToJson(string_i));
  base::Json json_j(base::StringToJson(string_j));
  EXPECT_TRUE(JsonContentCompare(&json_i, &json_j));

  std::string string_k = R"({
      "a" : "23"
  })";
  std::string string_l = R"({
      "b" : "23"
  })";
  base::Json json_k(base::StringToJson(string_k));
  base::Json json_l(base::StringToJson(string_l));
  EXPECT_FALSE(JsonContentCompare(&json_k, &json_l));
}

TEST(TestGnnDynamicJsonConfig, normal) {
  std::string json_string = R"({
    "init_list" : {
      "init_1" : {"xxx" : 0},
      "init_2" : {"yyy" : 1},
      "init_3" : {"zzz" : 3},
      "init_4" : {"www" : 4}
    },
    "db_list": {
      "u2i":  {
        "oversize_replace_strategy": 1,
        "kv_size": 25843545600,
        "edge_max_num": 400,
        "kv_dict_size": 15625000,
        "expire_interval": 0,
        "relation_name": "U2I",
        "kv_shard_num": 16
      },
      "a2u": {
        "oversize_replace_strategy": 1,
        "kv_size": 25843545600,
        "edge_max_num": 400,
        "kv_dict_size": 15625000,
        "expire_interval": 0,
        "relation_name": "A2U",
        "kv_shard_num": 16
      },
      "i2u":  {
        "oversize_replace_strategy": 1,
        "kv_size": 25843545600,
        "edge_max_num": 400,
        "kv_dict_size": 15625000,
        "attr_op_config": {
          "type_name": "SimpleAttrBlock"
        },
        "kv_shard_num": 16,
        "expire_interval": 172800,
        "relation_name": "I2U"
      },
      "u2a": {
        "oversize_replace_strategy": 1,
        "kv_size": 25843545600,
        "edge_max_num": 400,
        "kv_dict_size": 15625000,
        "expire_interval": 0,
        "relation_name": "U2A",
        "kv_shard_num": 16
      }
    },
    "service_config" : {
      "default_init": "init_4",
      "u2i_service" : {
        "init" : "init_1",
        "dbs" : ["u2i"],
        "service_name" : "grpc_service_u2i_test"
      },
      "i2u_service" : {
        "init" : "init_2",
        "dbs" : ["i2u"],
        "service_name" : "grpc_service_i2u_test"
      },
      "a2u_service" : {
        "dbs" : ["a2u"],
        "service_name" : "grpc_service_a2u_test"
      },
      "u2a_service" : {
        "dbs" : ["u2a"],
        "service_name" : "grpc_service_u2a_test"
      }
    },
    "host_service_config" : {
      "host_1" : "u2i_service",
      "host_2" : "i2u_service",
      "host_3" : "a2u_service",
      "host_4" : "u2a_service"
    }
  })";
  base::Json whole_json_config(base::StringToJson(json_string));

  // Verify host_1 config
  base::Json *host_config_key = whole_json_config.Get("host_service_config")->Get("host_1");
  TestGnnDynamicJsonConfig obj(whole_json_config, *host_config_key);
  const base::Json *host_config = obj.GetHostConfig();
  ASSERT_NE(host_config, nullptr);
  std::string result = R"({
      "init": {
        "xxx": 0
      },
      "exp_name": "",
      "shard_id": 0,
      "shard_num": 1,
      "dbs": [
        {
          "oversize_replace_strategy": 1,
          "kv_dict_size": 15625000,
          "kv_shard_num": 16,
          "kv_size": 25843545600,
          "expire_interval": 0,
          "edge_max_num": 400,
          "relation_name": "U2I"
        }
      ],
      "service_name": "grpc_service_u2i_test"
  })";
  base::Json result_json(base::StringToJson(result));
  EXPECT_TRUE(JsonContentCompare(host_config, &result_json)) << host_config->ToString() << "\n vs \n"
                                                             << result_json.ToString();

  // Verify host_2 config
  host_config_key = whole_json_config.Get("host_service_config")->Get("host_2");
  TestGnnDynamicJsonConfig host_2_object(whole_json_config, *host_config_key);
  host_config = host_2_object.GetHostConfig();
  ASSERT_NE(host_config, nullptr);
  result = R"({
      "init": {
        "yyy": 1
      },
      "exp_name": "",
      "shard_id": 0,
      "shard_num": 1,
      "dbs": [
        {
          "oversize_replace_strategy": 1,
          "kv_dict_size": 15625000,
          "kv_shard_num": 16,
          "kv_size": 25843545600,
          "expire_interval": 172800,
          "edge_max_num": 400,
          "relation_name": "I2U",
          "attr_op_config": {
            "type_name": "SimpleAttrBlock"
          }
        }
      ],
      "service_name": "grpc_service_i2u_test"
  })";
  base::Json host_2_result_json(base::StringToJson(result));
  EXPECT_TRUE(JsonContentCompare(host_config, &host_2_result_json)) << host_config->ToString() << "\n vs \n"
                                                                    << host_2_result_json.ToString();

  // Verify host_3 config
  host_config_key = whole_json_config.Get("host_service_config")->Get("host_3");
  TestGnnDynamicJsonConfig host_3_object(whole_json_config, *host_config_key);
  host_config = host_3_object.GetHostConfig();
  ASSERT_NE(host_config, nullptr);
  result = R"({
      "init": {
        "www": 4
      },
      "exp_name": "",
      "shard_id": 0,
      "shard_num": 1,
      "dbs": [
        {
          "oversize_replace_strategy": 1,
          "kv_dict_size": 15625000,
          "kv_shard_num": 16,
          "kv_size": 25843545600,
          "expire_interval": 0,
          "edge_max_num": 400,
          "relation_name": "A2U"
        }
      ],
      "service_name": "grpc_service_a2u_test"
  })";
  base::Json host_3_result_json(base::StringToJson(result));
  EXPECT_TRUE(JsonContentCompare(host_config, &host_3_result_json)) << host_config->ToString() << "\n vs \n"
                                                                    << host_3_result_json.ToString();

  // Verify host_4 config
  host_config_key = whole_json_config.Get("host_service_config")->Get("host_4");
  TestGnnDynamicJsonConfig host_4_object(whole_json_config, *host_config_key);
  host_config = host_4_object.GetHostConfig();
  ASSERT_NE(host_config, nullptr);
  result = R"({
      "init": {
        "www": 4
      },
      "exp_name": "",
      "shard_id": 0,
      "shard_num": 1,
      "dbs": [
        {
          "oversize_replace_strategy": 1,
          "kv_dict_size": 15625000,
          "kv_shard_num": 16,
          "kv_size": 25843545600,
          "expire_interval": 0,
          "edge_max_num": 400,
          "relation_name": "U2A"
        }
      ],
      "service_name": "grpc_service_u2a_test"
  })";
  base::Json host_4_result_json(base::StringToJson(result));
  EXPECT_TRUE(JsonContentCompare(host_config, &host_4_result_json)) << host_config->ToString() << "\n vs \n"
                                                                    << host_4_result_json.ToString();
}

}  // namespace chrono_graph
