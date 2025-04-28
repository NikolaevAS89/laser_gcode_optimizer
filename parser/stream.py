from __future__ import annotations

import logging
from enum import Enum
from functools import reduce
from itertools import chain, dropwhile, count, takewhile, zip_longest
from multiprocessing import current_process, Queue, Process
from queue import Empty
from typing import Iterable, Any, Iterator, Callable

from more_itertools import pairwise

from parser.functions import group_by_limit, group_by_memory_limit, echo, append_to_list

logger = logging.getLogger(__name__)


# https://docs.python.org/3/library/itertools.html
# https://docs.python.org/3/library/functions.html

class JoinType(Enum):
    FULL = 0
    LEFT = 1
    RIGHT = 2
    INNER = 3


class Optional:
    __NO_VALUE__ = object()

    def __init__(self, value):
        self._value_ = value

    def is_empty(self):
        return self._value_ == Optional.__NO_VALUE__

    def get(self, default_value=None):
        return self._value_ if self._value_ != Optional.__NO_VALUE__ else default_value

    @staticmethod
    def empty() -> Optional:
        return Optional(Optional.__NO_VALUE__)


class Stream(Iterable):
    def __init__(self, iterable: Iterable):
        self._iter_ = iterable

    def __iter__(self) -> Iterator:
        return iter(self._iter_)

    def filter(self, predicate: Callable[[Any], bool]) -> Stream:
        """
        Returns a parser consisting of the elements of this parser that match the given predicate.
        NOT TERMINATED
        """
        return Stream(filter(predicate, self))

    def map(self, function: Callable[[Any], Any]) -> Stream:
        """
        Return a parser of results of applying the given function to the elements of the original parser.
        NOT TERMINATED
        """
        return Stream(map(function, self))

    def flat_map(self) -> Stream:
        """
        Gets chained inputs from a single iterable argument that is evaluated lazily
        NOT TERMINATED
        """
        return Stream(chain.from_iterable(self))

    def distinct(self) -> Stream:
        """
        Returns a parser consisting of the distinct elements according to '==' of this parser.
        !!! WARNING operation store all data to memory !!!
        TERMINATED
        """
        logger.warning("Stream distinct was used")
        return Stream(self.to_set())

    def sorted(self, key_function: Callable[[Any], Any]) -> list:
        """
        Returns a parser consisting of the elements of this parser, sorted according to the provided key_function.
        !!! WARNING operation store all data to memory !!!
        TERMINATED
        """
        return sorted(self, key=key_function)

    def to_buckets(self, size_limit: int) -> Stream[tuple]:
        """
        split the parser to a buckets parser by a buckets size
        NOT TERMINATED
        """
        groupped = group_by_limit(self, limit_size=size_limit)
        return Stream(iter(groupped))

    def to_pockets(self, mem_limit: int) -> Stream[list]:
        """
        split the parser to a pockets parser by a memory size in bytes
        NOT TERMINATED
        """
        groupped = group_by_memory_limit(self, memory_limit_size=mem_limit)
        return Stream(iter(groupped))

    def peek(self, function: Callable[[Any], Any]) -> Stream:
        """
        Returns a parser consisting of the elements of this parser, additionally performing the
        provided action on each element as elements are consumed from the resulting parser.
        NOT TERMINATED
        """

        def inner_func():
            x = None
            while True:
                item = yield x
                function(item)
                x = item

        func = inner_func()
        func.send(None)
        return self.map(lambda item: func.send(item))

    def pairwise(self) -> Stream:
        return Stream(pairwise(self))

    def limit(self, n: int) -> Stream:
        """
        Returns a parser consisting of the elements of this parser, truncated to be no longer than n in length.
        NOT TERMINATED
        """
        counter = count()
        return Stream(takewhile(lambda item: next(counter) < n, self))

    def skip(self, n: int) -> Stream:
        """
        Returns a parser consisting of the remaining elements of this parser after discarding the first n elements
        of the parser. If this parser contains fewer than n elements then an empty parser will be returned.
        NOT TERMINATED
        """
        counter = count()
        return Stream(dropwhile(lambda item: next(counter) < n, self))

    def for_each(self, function: Callable[[Any], Any]):
        """
        Performs an action for each element of this parser.
        TERMINATED
        """
        for item in self._iter_:
            function(item)

    def reduce(self, function: Callable[[Any, Any], Any], initial: Any = None) -> Any:
        """
        Performs a reduction on the elements of this parser, using the provided identity value and an associative
        accumulation function, and returns the reduced value.
        TERMINATED
        """
        if initial is None:
            return reduce(function, self)
        else:
            return reduce(function, self, initial)

    def parallelize(self, n: int = 4, max_queue_size: int = 5000) -> ParallelStream:
        """
        To parallelize some work
        NOT TERMINATED
        """
        return ParallelStream(self, n=n, max_queue_size=max_queue_size)

    def to_list(self) -> list:
        """
        collect the streams elements to list
        TERMINATED
        """
        return list(self)

    def to_set(self) -> set:
        """
        collect the streams elements to set
        TERMINATED
        """
        return set(self)

    def to_dict(self) -> dict:
        """
        collect the streams elements to dict
        TERMINATED
        """
        return dict(value for value in self)

    def min(self, comparator: Callable[[Any, Any], int]) -> Optional:
        """
        Returns the minimum element of this parser according to the provided Comparator or Optional.empty()
        if parser is empty.
        TERMINATED
        """
        result = Optional.empty()
        for item in self._iter_:
            if result.is_empty():
                result = Optional(item)
            elif comparator(result.get(), item) >= 0:
                result = Optional(item)
        return result

    def max(self, comparator: Callable[[Any, Any], int]) -> Optional:
        """
        Returns the maximum element of this parser according to the provided Comparator or Optional.empty()
        if parser is empty.
        TERMINATED
        """
        result = Optional.empty()
        for item in self._iter_:
            if result.is_empty():
                result = Optional(item)
            elif comparator(result.get(), item) <= 0:
                result = Optional(item)
        return result

    def count(self) -> int:
        """
        Returns the count of elements in this parser.
        TERMINATED
        """
        result = 0
        for item in self._iter_:
            result += 1
        return result

    def any_match(self, predicate: Callable[[Any], bool]) -> bool:
        """
        Returns whether any elements of this parser match the provided predicate. May not evaluate the predicate
        on all elements if not necessary for determining the result. If the parser is empty then false is returned
        and the predicate is not evaluated.
        TERMINATED
        """
        for item in self._iter_:
            if predicate(item):
                return True
        return False

    def all_match(self, predicate: Callable[[Any], bool]) -> bool:
        """
        Returns whether all elements of this parser match the provided predicate.
        TERMINATED
        """
        for item in self._iter_:
            if not predicate(item):
                return False
        return True

    def none_match(self, predicate: Callable[[Any], bool]) -> bool:
        """
        Returns whether no elements of this parser match the provided predicate.
        TERMINATED
        """
        for item in self._iter_:
            if predicate(item):
                return False
        return True

    def find_first(self, predicate: Callable[[Any], bool]) -> Optional:
        """
        Returns whether no elements of this parser match the provided predicate.
        TERMINATED
        """
        for item in self._iter_:
            if predicate(item):
                return Optional(item)
        return Optional.empty()

    def concat(self, another: Stream) -> Stream:
        """
        Creates a lazily concatenated parser whose elements are all the elements of the self parser followed by all
        the elements of the another parser
        NOT TERMINATED
        """
        return self.stream_of(self, another)

    def group_by(self,
                 key_function: Callable[[Any], Any],
                 value_function: Callable[[Any], Any] = echo,
                 combiner: Callable[[Any, Any], Any] = append_to_list,
                 init_function: Callable[[], Any] = list) -> dict:
        """
        collect elements to dictionary where key is taken by applying key_function,
        value is taken by applying combiner to result of init_combiner function and
        results of value_function applying to each element of the origin parser.
        TERMINATED
        """
        result = dict()
        for item in iter(self):
            key = key_function(item)
            value = value_function(item)
            agg = result.get(key)
            if agg is None:
                agg = init_function()
            new_value = combiner(agg, value)
            result[key] = new_value
        return result

    def consume(self, consumer: Callable[[Iterable], Any]) -> Any:
        """
        consume all items
        """
        return consumer(self)

    @classmethod
    def stream_of(cls, *suppliers: Iterable) -> Stream:
        """
        combine several streams to one.
        """
        return Stream(chain.from_iterable(suppliers))

    @staticmethod
    def zip_stream(stream1: Iterable,
                   stream2: Iterable,
                   key_func: Callable[[Any], str],
                   join_type: JoinType = JoinType.FULL) -> Stream:
        """
        zip items of several streams to one parser of pair items. Items bind by key_func!
        if one of parser not contain item then in pair their item will be None.
        !!! None keys ignored !!!
        """
        stream1_objects = dict()
        stream2_objects = dict()

        def find_pairs(obj1, obj2, func_key: Callable[[Any], str]) -> list[tuple]:
            key_obj1 = func_key(obj1) if obj1 else None
            key_obj2 = func_key(obj2) if obj2 else None
            if key_obj1 == "7c3cab01-9c25-4843-b38a-ff5f9ea90643" or key_obj2 == "7c3cab01-9c25-4843-b38a-ff5f9ea90643":
                print(str(key_obj1) + " : "+ str(key_obj2))

            if not key_obj2 and not key_obj1:
                return []
            elif key_obj1 != key_obj2:
                result = []
                if key_obj1 in stream2_objects:
                    value = stream2_objects.get(key_obj1)
                    stream2_objects.pop(key_obj1, None)
                    result.append((obj1, value))
                elif key_obj1:
                    stream1_objects[key_obj1] = obj1

                if key_obj2 in stream1_objects:
                    value = stream1_objects.get(key_obj2)
                    stream1_objects.pop(key_obj2, None)
                    result.append((value, obj2))
                elif key_obj2:
                    stream2_objects[key_obj2] = obj2
                return result
            else:
                return [(obj1, obj2)]

        zipped = Stream(zip_longest(stream1, stream2)) \
            .map(lambda item: find_pairs(item[0], item[1], func_key=key_func)) \
            .flat_map()
        stream1_rest = Stream([stream1_objects]) \
            .map(lambda item: item.values()) \
            .flat_map() \
            .filter(lambda item: item) \
            .map(lambda item: (item, None))
        stream2_rest = Stream([stream2_objects]) \
            .map(lambda item: item.values()) \
            .flat_map() \
            .filter(lambda item: item) \
            .map(lambda item: (None, item))
        if join_type == JoinType.FULL:
            return Stream.stream_of(zipped, stream1_rest, stream2_rest)
        elif join_type == JoinType.LEFT:
            return Stream.stream_of(zipped, stream1_rest)
        elif join_type == JoinType.RIGHT:
            return Stream.stream_of(zipped, stream2_rest)
        else:
            return Stream.stream_of(zipped)


class QueueReader(Iterable):

    def __init__(self, n: int, queue: Queue):
        self._queue_ = queue
        self._n_ = n

    def __iter__(self) -> Any:
        process = current_process()
        logger.info(
            f"Consumer is started in {str(process.pid)}.{process.name}. Demon {str(process.daemon)}")
        task_count = 0
        total_wait_time = 0
        while True:
            # If the queue is empty, queue.get() will block until the queue has data
            try:
                task = self._queue_.get(block=True, timeout=10)
            except Empty:
                total_wait_time += 10
                continue
            task_count += 1
            if task is None:
                self._n_ -= 1
                if self._n_ <= 0:
                    break
            else:
                yield task
        logger.info(
            f"Consumer is end {str(process.pid)}.{process.name}. Total consumed: {str(task_count)}. Total wait time: {str(total_wait_time)}s")


class ParallelStream:
    __END_OF_STREAM__ = None

    def __init__(self, iterable: Iterable, n: int, max_queue_size: int = 5000):
        self.__inner_iterable__ = iterable
        self.__n__ = n
        self.__max_queue_size__ = max_queue_size

    def consume(self, factory_combiner: Callable[[int], Callable[[Iterable], Any]]) -> Stream:
        """
        TERMINATED
        """

        def consumer(idx: int,
                     tasks: Queue,
                     results: Queue,
                     factory: Callable[[int], Callable[[Iterable], Any]]):
            process = current_process()
            print(f"Consumer is started in {str(process.pid)}.{process.name}. Demon {str(process.daemon)}", flush=True)
            combiner = factory(idx)
            res = Stream(QueueReader(1, tasks)) \
                .map(lambda item: item.get()) \
                .consume(combiner)
            results.put(res)
            results.put(ParallelStream.__END_OF_STREAM__)
            print(f"Consumer is ended in {str(process.pid)}.{process.name}. Demon {str(process.daemon)}", flush=True)
            return

        def supply(iterable: Iterable, tasks: Queue, parallelism):
            process = current_process()
            print(f"Supplier is started in {str(process.pid)}.{process.name}. Demon {str(process.daemon)}", flush=True)
            Stream(iterable).map(lambda item: Optional(item)) \
                .for_each(lambda item: tasks.put(item, block=True))
            Stream(range(parallelism)).for_each(lambda item: tasks.put(ParallelStream.__END_OF_STREAM__, block=True))
            print(f"Supplier is ended in {str(process.pid)}.{process.name}. Demon {str(process.daemon)}", flush=True)
            return

        tasks_queue = Queue(self.__max_queue_size__)
        results_queue = Queue()
        for i in range(self.__n__):
            p = Process(
                target=consumer,
                args=(i, tasks_queue, results_queue, factory_combiner)
            )
            # This is critical! The consumer function has an infinite loop
            # Which means it will never exit unless we set daemon to true
            p.daemon = True
            p.start()
        p = Process(target=supply,
                    args=(self.__inner_iterable__, tasks_queue, self.__n__))
        p.daemon = True
        p.start()
        return Stream(QueueReader(self.__n__, results_queue))
