#pragma once

#include <stdlib.h>

#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common/basic_types.h"
#include "base/common/string.h"
#include "base/jansson/jansson.h"

namespace base {

inline json_t *StringToJson(const std::string &json_string) {
  json_error_t json_error;
  return json_loads(json_string.data(), &json_error);
}

inline json_t *BufferToJson(const char *buf, size_t len) {
  json_error_t json_error;
  return json_loadb(buf, len, &json_error);
}

inline std::string JsonToString(json_t *json, int indent = 0) {
  const char *json_char_array = json_dumps(json, JSON_INDENT(indent));
  if (json_char_array == nullptr) return "";
  std::string json_string(json_char_array);
  json_string.erase(
      std::find_if(json_string.rbegin(), json_string.rend(), [](int ch) { return !std::isspace(ch); }).base(),
      json_string.end());
  free((void *)json_char_array);  // NOLINT
  return json_string;
}

/**
 * \brief Wrapper for jansson
 */
class Json {
 public:
  Json() : json_(NULL) {}
  explicit Json(json_t *json);
  explicit Json(int64 value) : json_(json_integer(value)) {}
  explicit Json(int value) : json_(json_integer(value)) {}
  explicit Json(double value) : json_(json_real(value)) {}
  explicit Json(bool value) : json_(value ? json_true() : json_false()) {}
  explicit Json(const char *value) : json_(json_string(value)) {}
  explicit Json(const std::string &value) : json_(json_string(value.data())) {}
  ~Json();

  std::string ToString(int indent = 0) const { return JsonToString(json_, indent); }

  bool IsInteger() const { return json_is_integer(json_); }
  bool IsDouble() const { return json_is_real(json_); }
  bool IsString() const { return json_is_string(json_); }
  bool IsBoolean() const { return json_is_boolean(json_); }
  bool IsObject() const { return json_is_object(json_); }
  bool IsArray() const { return json_is_array(json_); }
  int Type() const { return (json_ != NULL) ? json_->type : -1; }

  bool StringValue(std::string *value) const {
    if (!json_is_string(json_)) return false;
    value->assign(json_string_value(json_));
    return true;
  }
  std::string StringValue(std::string default_value = "") const {
    StringValue(&default_value);
    return default_value;
  }
  bool IntValue(int64 *value) const {
    if (!json_is_integer(json_)) return false;
    *value = json_integer_value(json_);
    return true;
  }
  int64 IntValue(int64 default_value) const {
    IntValue(&default_value);
    return default_value;
  }
  bool FloatValue(double *value) const {
    if (!json_is_real(json_)) return false;
    *value = json_real_value(json_);
    return true;
  }
  double FloatValue(double default_value) const {
    FloatValue(&default_value);
    return default_value;
  }
  bool NumberValue(double *value) const {
    int64 int_value = -1;
    if (IntValue(&int_value)) {
      *value = int_value;
      return true;
    }
    return FloatValue(value);
  }
  double NumberValue(double default_value) const {
    int64 int_value = -1;
    if (IntValue(&int_value)) return int_value;
    return FloatValue(default_value);
  }
  bool BooleanValue(bool *value) const {
    if (!json_is_boolean(json_)) return false;
    *value = json_is_true(json_);
    return true;
  }
  bool BooleanValue(bool default_value) const {
    BooleanValue(&default_value);
    return default_value;
  }

  // object accessor
  const std::unordered_map<std::string, Json *> &objects() const { return object_map_; }
  std::unordered_map<std::string, Json *>::const_iterator object_begin() const { return object_map_.begin(); }
  std::unordered_map<std::string, Json *>::const_iterator object_end() const { return object_map_.end(); }
  Json *Get(const std::string &key) const;
  bool GetInt(const std::string &key, int64 *value) const;
  bool GetFloat(const std::string &key, double *value) const;
  bool GetNumber(const std::string &key, double *value) const;
  bool GetBoolean(const std::string &key, bool *value) const;
  bool GetString(const std::string &key, std::string *value) const;
  int64 GetInt(const std::string &key, int64 default_value) const {
    GetInt(key, &default_value);
    return default_value;
  }
  int64 GetInt(const std::string &key, int default_value) const { return GetInt(key, (int64)default_value); }
  double GetFloat(const std::string &key, double default_value) const {
    GetFloat(key, &default_value);
    return default_value;
  }
  double GetNumber(const std::string &key, double default_value) const {
    GetNumber(key, &default_value);
    return default_value;
  }
  bool GetBoolean(const std::string &key, bool default_value) const {
    GetBoolean(key, &default_value);
    return default_value;
  }
  std::string GetString(const std::string &key, std::string default_value = "") const {
    GetString(key, &default_value);
    return default_value;
  }

  // array accessor
  const std::vector<Json *> &array() const { return array_; }
  std::vector<Json *>::const_iterator array_begin() const { return array_.begin(); }
  std::vector<Json *>::const_iterator array_end() const { return array_.end(); }
  int size() const { return array_.size(); }
  Json *Get(int index) const;
  bool GetInt(int index, int64 *value) const;
  bool GetFloat(int index, double *value) const;
  bool GetNumber(int index, double *value) const;
  bool GetBoolean(int index, bool *value) const;
  bool GetString(int index, std::string *value) const;
  int64 GetInt(int index, int64 default_value) const {
    GetInt(index, &default_value);
    return default_value;
  }
  int64 GetInt(int index, int default_value) const { return GetInt(index, (int64)default_value); }
  double GetFloat(int index, double default_value) const {
    GetFloat(index, &default_value);
    return default_value;
  }
  double GetNumber(int index, double default_value) const {
    GetNumber(index, &default_value);
    return default_value;
  }
  bool GetBoolean(int index, bool default_value) const {
    GetBoolean(index, &default_value);
    return default_value;
  }
  std::string GetString(int index, std::string default_value = "") const {
    GetString(index, &default_value);
    return default_value;
  }

  // modifier
  bool set(const std::string &key, const Json &element);
  bool set(const std::string &key, int64 value) { return set(key, Json(value)); }
  bool set(const std::string &key, int value) { return set(key, Json(value)); }
  bool set(const std::string &key, double value) { return set(key, Json(value)); }
  bool set(const std::string &key, bool value) { return set(key, Json(value)); }
  bool set(const std::string &key, const char *value) { return set(key, Json(value)); }
  bool set(const std::string &key, const std::string &value) { return set(key, Json(value)); }
  bool append(const Json &element);
  bool append(int64 value) { return append(Json(value)); }
  bool append(int value) { return append(Json(value)); }
  bool append(double value) { return append(Json(value)); }
  bool append(bool value) { return append(Json(value)); }
  bool append(const char *value) { return append(Json(value)); }
  bool append(const std::string &value) { return append(Json(value)); }

  json_t *operator->() const { return json_; }
  json_t *get() const { return json_; }

 protected:
  json_t *json_;
  std::unordered_map<std::string, Json *> object_map_;
  std::vector<Json *> array_;

  friend std::ostream &operator<<(std::ostream &out, const Json &json) {
    if (json.get() != nullptr) {
      return out << json.ToString();
    } else {
      return out << "NULL";
    }
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(Json);
};

class JsonObject : public Json {
 public:
  JsonObject() : Json(json_object()) {}

 private:
  DISALLOW_COPY_AND_ASSIGN(JsonObject);
};

class JsonArray : public Json {
 public:
  JsonArray() : Json(json_array()) {}

 private:
  DISALLOW_COPY_AND_ASSIGN(JsonArray);
};

}  // namespace base
