// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include <pybind11/chrono.h>
#include <pybind11/complex.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "base/common/logging.h"
#include "chrono_graph/src/pybind/gnn_api.h"

namespace py = pybind11;

PYBIND11_MODULE(libgnn_pybind, m) {
  InitLogging();
  m.def("add_client", &chrono_graph::GnnPyApi::AddClient, R"pbdoc(
      Add a client
      )pbdoc");

  m.def("batch_insert", &chrono_graph::GnnPyApi::BatchInsert, R"pbdoc(
    batch insert node, node attr and dst ids, dst weights.
    )pbdoc");

  m.def("sample_nodes", &chrono_graph::GnnPyApi::SampleNodes, R"pbdoc(
      sample nodes from a relation
      )pbdoc");

  m.def("sample_neighbors", &chrono_graph::GnnPyApi::SampleNeighbors, R"pbdoc(
      sample neighbors for some given nodes
      )pbdoc");

  py::class_<chrono_graph::GnnClientHelper::BatchNodeAttr>(m, "BatchNodeAttr")
      .def(py::init<>())
      .def("__repr__", [](const chrono_graph::GnnClientHelper::BatchNodeAttr &batch) { return batch.info(); })
      .def_readonly("exist", &chrono_graph::GnnClientHelper::BatchNodeAttr::exist)
      .def_readonly("degrees", &chrono_graph::GnnClientHelper::BatchNodeAttr::degrees)
      .def_readonly("int_attrs", &chrono_graph::GnnClientHelper::BatchNodeAttr::int_attrs)
      .def_readonly("float_attrs", &chrono_graph::GnnClientHelper::BatchNodeAttr::float_attrs);

  py::class_<chrono_graph::GnnClientHelper::EdgeVec>(m, "EdgeVec")
      .def(py::init<>())
      .def("__repr__", [](const chrono_graph::GnnClientHelper::EdgeVec &edge) { return edge.info(); })
      .def_readonly("ids", &chrono_graph::GnnClientHelper::EdgeVec::ids)
      .def_readonly("timestamps", &chrono_graph::GnnClientHelper::EdgeVec::timestamps)
      .def_readonly("weights", &chrono_graph::GnnClientHelper::EdgeVec::weights);

  m.def("nodes_attr", &chrono_graph::GnnPyApi::NodesAttr, R"pbdoc(
      get nodes attrs for given ids. Result is flattened.
      )pbdoc");

  m.def("iterate_nodes", &chrono_graph::GnnPyApi::IterateNodes, R"pbdoc(
      iterate node for given relation.
  )pbdoc");

  m.def("sample_type", &chrono_graph::GnnPyApi::SampleTypeByName, R"pbdoc(
      translate sample_type from string into int type, used internally.
  )pbdoc");

  m.def("padding_type", &chrono_graph::GnnPyApi::PaddingTypeByName, R"pbdoc(
      translate sample_type from string into int type, used internally.
  )pbdoc");

  m.def("string_keys_to_ids", &chrono_graph::GnnPyApi::StringKeysToIds, R"pbdoc(
      convert string keys to ids by hash, used internally.
      input: key list,  list of string
      output: id list, list of uint64.
  )pbdoc");
}
