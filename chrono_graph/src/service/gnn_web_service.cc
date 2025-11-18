// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/service/gnn_web_service.h"
#include <boost/asio.hpp>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include "chrono_graph/src/service/gnn_web_handler.h"

using boost::asio::ip::tcp;

namespace chrono_graph {

std::string GNNWebService::MakeResponse(const std::string &content) {
  return "HTTP/1.1 200 OK\r\n"
         "Content-Type: text/plain\r\n"
         "Content-Length: " +
         std::to_string(content.length()) +
         "\r\n"
         "Connection: close\r\n\r\n" +
         content;
}

void GNNWebService::HandleRequest(tcp::socket &socket) {
  try {
    boost::asio::streambuf buffer;
    boost::asio::read_until(socket, buffer, "\r\n\r\n");

    std::istream stream(&buffer);
    std::string request;
    std::getline(stream, request);

    // split the request into method, path, and HTTP version
    std::istringstream request_stream(request);
    std::string method, path, http_version;
    request_stream >> method >> path >> http_version;

    std::string path_only = path;
    std::string query_string;
    size_t query_pos = path.find('?');
    if (query_pos != std::string::npos) {
      path_only = path.substr(0, query_pos);
      query_string = path.substr(query_pos + 1);
    }

    std::string response_content;
    if (path_only == "/info") {
      GNNInfoHandler handler(graph_kv_);
      response_content = handler.ProcessRequest(query_string);
    } else if (path_only == "/get") {
      GNNGetHandler handler(graph_kv_);
      response_content = handler.ProcessRequest(query_string);
    } else {
      response_content = "Method " + path_only + " not supported. Valid method: info, get.";
    }

    std::string response = MakeResponse(response_content);
    boost::asio::write(socket, boost::asio::buffer(response));
  } catch (std::exception &e) { std::cerr << "Error: " << e.what() << std::endl; }
}

void GNNWebService::Run(int port) {
  try {
    boost::asio::io_context io_context;
    tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), port));

    while (!stop_) {
      tcp::socket socket(io_context);
      acceptor.accept(socket);
      HandleRequest(socket);
    }
  } catch (std::exception &e) { std::cerr << "Exception: " << e.what() << std::endl; }
}

}  // namespace chrono_graph