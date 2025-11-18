# coding=utf-8

# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

from typing import get_type_hints, Union, Dict, Callable
import pytest


@pytest.mark.skip(reason="function is not test body.")
def flatten_union(t):
  """
  When the type hint is union, the legal types need to be expanded.
  """
  ret = []
  if hasattr(t, '__origin__') and t.__origin__ is Union:
    for sub_t in t.__args__:
      ret.extend(flatten_union(sub_t))
  else:
    ret.append(t)
  return ret


@pytest.mark.skip(reason="function is not test body.")
def strict_types(function):
  """
  Use this function annotation to perform type annotation checking on function parameters.
  """

  def type_checker(*args, **kwargs):
    # key = arg name, value = arg type.
    hints: Dict[str, type] = get_type_hints(function)
    # copy inference, is ok.
    all_args = kwargs.copy()
    all_args.update(dict(zip(function.__code__.co_varnames, args)))
    for argument, argument_type in ((i, type(j)) for i, j in all_args.items()):
      if argument in hints:
        type_hint = hints[argument]
        allowed_types = flatten_union(type_hint)
        if not any(issubclass(argument_type, t) for t in allowed_types):
          raise TypeError(f"Type of {argument} is {argument_type}, which should be {type_hint}")
    result = function(*args, **kwargs)
    if 'return' in hints:
      if not issubclass(type(result), hints['return']):
        raise TypeError(f"Type of function ({function}) return is {type(result)}, which should be {hints['return']}")
    return result

  return type_checker


def test_type_checker():
  # Testing functions without a type checker
  @strict_types
  def test1(a, b):
    print(a + b)

  assert None == test1(1, 2)

  # Testing a normal type checker
  @strict_types
  def test2(a: int, b: int):
    print(a + b)

  assert None == test2(1, 2)

  # Testing types with return values
  @strict_types
  def test3(a: int, b: int) -> int:
    return a + b

  assert int == type(test3(1, 2))

  # Testing type mismatch
  @strict_types
  def test4(a: int, b: int) -> int:
    return "aa"

  with pytest.raises(TypeError):
    test4(1, 2)

  # Testing union types
  @strict_types
  def test5(a: int, b: Union[str, float]) -> int:
    print(type(a))
    print(type(b))
    return {}

  with pytest.raises(TypeError):
    test5(1, 2)

  # Testing more complex mixed types.
  @strict_types
  def test6(a: int, b: str, c: list, d: Dict) -> bool:
    return True

  with pytest.raises(TypeError):
    test6("hello", 1, c=[], d={})
  with pytest.raises(TypeError):
    test6("hello", 1, c={}, d=[])
  with pytest.raises(TypeError):
    test6(1, "", c={}, d={})

  test6(1, "a", [], {})
  test6(1, "a", c=[], d={})

  # Test Function
  @strict_types
  def test7(a: Callable):
    pass

  def _f():
    pass

  test7(_f)
