// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include "base/thread/thread_pool.h"

namespace chrono_graph {

class GraphKV;

// Run a web service allows you to use `curl localhost:port/path` to check server status or do some queries.
class GNNWebService {
 public:
  GNNWebService(GraphKV *graph_kv, int port) : graph_kv_(graph_kv) {
    thread_pool_ = std::make_unique<thread::ThreadPool>(1);
    thread_pool_.get()->AddTask([this, port]() { this->Run(port); });
  }
  void Stop() { stop_ = true; }

 private:
  std::string MakeResponse(const std::string &content);

  void HandleRequest(boost::asio::ip::tcp::socket &socket);

  void Run(int port);

  bool stop_ = false;
  std::unique_ptr<thread::ThreadPool> thread_pool_;
  GraphKV *graph_kv_;
  DISALLOW_COPY_AND_ASSIGN(GNNWebService);
};

}  // namespace chrono_graph
