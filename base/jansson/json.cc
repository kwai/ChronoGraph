#include "base/jansson/json.h"

#include <iostream>

namespace base {

Json::Json(json_t *json) {
  json_ = json;
  if (json_is_object(json_)) {
    for (auto it = json_object_iter(json_); it != NULL; it = json_object_iter_next(json_, it)) {
      auto value = json_object_iter_value(it);
      json_incref(value);
      object_map_[json_object_iter_key(it)] = new Json(value);
    }
  }
  if (json_is_array(json_)) {
    for (size_t i = 0; i < (int)json_array_size(json_); ++i) {
      auto value = json_array_get(json_, i);
      json_incref(value);
      array_.push_back(new Json(value));
    }
  }
}

Json::~Json() {
  for (auto it = object_map_.begin(); it != object_map_.end(); ++it) { delete it->second; }
  for (auto it = array_.begin(); it != array_.end(); ++it) { delete *it; }
  json_decref(json_);
}

Json *Json::Get(const std::string &key) const {
  auto it = object_map_.find(key);
  if (it != object_map_.end()) return it->second;
  return NULL;
}

bool Json::GetInt(const std::string &key, int64 *value) const {
  auto element = Get(key);
  if (element) return element->IntValue(value);
  return false;
}

bool Json::GetFloat(const std::string &key, double *value) const {
  auto element = Get(key);
  if (element) return element->FloatValue(value);
  return false;
}

bool Json::GetNumber(const std::string &key, double *value) const {
  auto element = Get(key);
  if (element) return element->NumberValue(value);
  return false;
}

bool Json::GetBoolean(const std::string &key, bool *value) const {
  auto element = Get(key);
  if (element) return element->BooleanValue(value);
  return false;
}

bool Json::GetString(const std::string &key, std::string *value) const {
  auto element = Get(key);
  if (element) return element->StringValue(value);
  return false;
}

Json *Json::Get(int index) const {
  if (0 <= index && index < (int)array_.size()) return array_[index];
  return NULL;
}

bool Json::GetInt(int index, int64 *value) const {
  auto element = Get(index);
  if (element) return element->IntValue(value);
  return false;
}

bool Json::GetFloat(int index, double *value) const {
  auto element = Get(index);
  if (element) return element->FloatValue(value);
  return false;
}

bool Json::GetNumber(int index, double *value) const {
  auto element = Get(index);
  if (element) return element->NumberValue(value);
  return false;
}

bool Json::GetBoolean(int index, bool *value) const {
  auto element = Get(index);
  if (element) return element->BooleanValue(value);
  return false;
}

bool Json::GetString(int index, std::string *value) const {
  auto element = Get(index);
  if (element) return element->StringValue(value);
  return false;
}

bool Json::set(const std::string &key, const Json &element) {
  if (!json_is_object(json_) || element.get() == NULL) return false;
  auto it = object_map_.find(key);
  if (it != object_map_.end()) delete it->second;
  json_incref(element.get());
  object_map_[key] = new Json(element.get());
  json_object_set(json_, key.data(), element.get());
  return true;
}

bool Json::append(const Json &element) {
  if (!json_is_array(json_) || element.get() == NULL) return false;
  json_incref(element.get());
  array_.push_back(new Json(element.get()));
  json_array_append(json_, element.get());
  return true;
}

}  // namespace base
