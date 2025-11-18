# Python Client Wrapper

Python client wrapper for ChronoGraph.
Syntax is similar to Gremlin Query Language of Apache TinkerPop.

## Prerequisites

- python 3.8+ required
- pytest 6.2.3+ required

## Install

Run the following in `chrono_graph/pypi_src/`

```shell
pip3 install build
python3 -m build
pip3 install dist/chrono_graph-1.0.0-py3-none-any.whl
```

Try `import chrono_graph` in python code.

## Run Python Tests

Run tests in `chrono_graph/pypi_src/tests`

```shell
sh run.sh
```
