import sys
from typing import Iterable, Any


def echo(item: Any) -> Any:
    return item


def map_values(accumulator: dict, key: Any, value: Any, unique_only: bool = False):
    if unique_only:
        aggregator = accumulator.get(key, set())
        aggregator.add(value)
        accumulator[key] = aggregator
    else:
        aggregator = accumulator.get(key, list())
        aggregator.append(value)
        accumulator[key] = aggregator
    return accumulator


def append_to_list(accumulator: list, value: Any):
    accumulator.append(value)
    return accumulator


def put_to_dict(accumulator: dict, key: Any, value: Any):
    accumulator[key] = value
    return accumulator


def remove_from_dict(accumulator: dict, key: Any):
    accumulator.pop(key)
    return accumulator


def put_if_not_exists(d: dict, key: Any, init_value: Any = None):
    d[key] = d.get(key, init_value)
    return d


def deep_getsizeof(obj) -> int:
    result = 0
    if isinstance(obj, dict):
        for key, value in obj.items():
            result += deep_getsizeof(key) + deep_getsizeof(value)
    elif isinstance(obj, list):
        for value in obj:
            result += deep_getsizeof(value)
    else:
        result += sys.getsizeof(obj)
    return result


def group_by_memory_limit(data: Iterable, memory_limit_size=5_000_000):
    current_chunk = []
    chunk_size = 0
    for element in data:
        current_chunk.append(element)
        chunk_size += deep_getsizeof(element)
        if chunk_size >= memory_limit_size:
            result = current_chunk
            current_chunk = []
            chunk_size = 0
            yield result
    if current_chunk:
        yield current_chunk


def group_by_limit(data: Iterable, limit_size=16):
    current_chunk = []
    for element in data:
        current_chunk.append(element)
        if len(current_chunk) >= limit_size:
            result = current_chunk
            current_chunk = []
            yield result
    if current_chunk:
        yield current_chunk
