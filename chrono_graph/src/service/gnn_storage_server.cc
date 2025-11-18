// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <exception>

#include "absl/flags/parse.h"
#include "base/common/basic_types.h"
#include "base/common/gflags.h"
#include "base/common/logging.h"
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/base/perf_util.h"
#include "chrono_graph/src/service/gnn_service_impl.h"
#include "chrono_graph/src/service/gnn_web_service.h"
#include "chrono_graph/src/storage/graph_kv.h"

ABSL_FLAG(int32, gnn_rpc_sync_thread, 64, "rpc sync call work thread num");
ABSL_FLAG(int32, gnn_rpc_port, 28012, "rpc port");
ABSL_FLAG(int32, gnn_http_port, 28013, "http port, for web server");

void RunServer(uint16_t grpc_port, uint16_t web_port) {
  std::string server_address = absl::StrFormat("0.0.0.0:%d", grpc_port);
  chrono_graph::GNNServiceImpl service;
  service.InitAndWaitReady();
  std::unique_ptr<chrono_graph::GNNWebService> http_server;
  try {
    http_server = std::make_unique<chrono_graph::GNNWebService>(service.db(), web_port);
  } catch (const std::exception &e) { LOG(ERROR) << "HTTP Server error: " << e.what(); }

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "Grpc server listening on " << server_address << ", HTTP server on localhost:" << web_port;
  std::cout << "Grpc server listening on " << server_address << ", HTTP server on localhost:" << web_port
            << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
  http_server->Stop();
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  InitLogging(argv[0]);

  PERF_SUM(1, GLOBAL_RELATION, "reboot");

  auto config = chrono_graph::GnnDynamicJsonConfig::GetConfig()->GetHostConfig();
  CHECK(config) << "config not exists on ../config.json, check it!";

  auto service_name = chrono_graph::GetStringConfigSafe(config, "service_name");
  CHECK(!service_name.empty());

  int shard_id = config->GetInt("shard_id", 0);
  int shard_num = config->GetInt("shard_num", 1);
  chrono_graph::SetShardId(shard_id);
  chrono_graph::SetShardNum(shard_num);
  std::string exp_name = config->GetString("exp_name", "");
  chrono_graph::SetExpName(exp_name);
  LOG(INFO) << "global shard_id = " << chrono_graph::global_shard_id
            << ", gloabl shard_num = " << chrono_graph::global_shard_num << ", exp_name = " << exp_name;

  int rpc_port = config->GetInt("biz_port", absl::GetFlag(FLAGS_gnn_rpc_port));
  int http_port = config->GetInt("http_port", absl::GetFlag(FLAGS_gnn_http_port));
  LOG(INFO) << "server rpc_port: " << rpc_port << ", web http_port: " << http_port;

  RunServer(rpc_port, http_port);
  return 0;
}
