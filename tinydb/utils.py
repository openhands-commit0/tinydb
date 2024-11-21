"""
Utility functions.
"""
from collections import OrderedDict, abc
from typing import List, Iterator, TypeVar, Generic, Union, Optional, Type, TYPE_CHECKING
K = TypeVar('K')
V = TypeVar('V')
D = TypeVar('D')
T = TypeVar('T')
__all__ = ('LRUCache', 'freeze', 'with_typehint')

def with_typehint(baseclass: Type[T]):
    """
    Add type hints from a specified class to a base class:

    >>> class Foo(with_typehint(Bar)):
    ...     pass

    This would add type hints from class ``Bar`` to class ``Foo``.

    Note that while PyCharm and Pyright (for VS Code) understand this pattern,
    MyPy does not. For that reason TinyDB has a MyPy plugin in
    ``mypy_plugin.py`` that adds support for this pattern.
    """
    return baseclass

class LRUCache(abc.MutableMapping, Generic[K, V]):
    """
    A least-recently used (LRU) cache with a fixed cache size.

    This class acts as a dictionary but has a limited size. If the number of
    entries in the cache exceeds the cache size, the least-recently accessed
    entry will be discarded.

    This is implemented using an ``OrderedDict``. On every access the accessed
    entry is moved to the front by re-inserting it into the ``OrderedDict``.
    When adding an entry and the cache size is exceeded, the last entry will
    be discarded.
    """

    def __init__(self, capacity=None) -> None:
        self.capacity = capacity
        self.cache: OrderedDict[K, V] = OrderedDict()

    def __len__(self) -> int:
        return len(self.cache)

    def __contains__(self, key: object) -> bool:
        return key in self.cache

    def __setitem__(self, key: K, value: V) -> None:
        if key in self.cache:
            del self.cache[key]
        self.cache[key] = value
        if self.capacity is not None and len(self.cache) > self.capacity:
            self.cache.popitem(last=False)  # Remove first item (least recently used)

    def __delitem__(self, key: K) -> None:
        del self.cache[key]

    def __getitem__(self, key) -> V:
        if key not in self.cache:
            raise KeyError(key)
        value = self.cache.pop(key)
        self.cache[key] = value  # Move to end
        return value

    def __iter__(self) -> Iterator[K]:
        return iter(self.cache)

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        try:
            return self[key]
        except KeyError:
            return default

    def clear(self) -> None:
        self.cache.clear()

    @property
    def lru(self) -> list:
        return list(self.cache.keys())

def _immutable(*args, **kw):
    """
    Function that raises a TypeError when trying to modify an immutable object.
    """
    raise TypeError('object is immutable')

class FrozenDict(dict):
    """
    An immutable dictionary.

    This is used to generate stable hashes for queries that contain dicts.
    Usually, Python dicts are not hashable because they are mutable. This
    class removes the mutability and implements the ``__hash__`` method.
    """

    def __hash__(self):
        return hash(tuple(sorted(self.items())))
    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    setdefault = _immutable
    popitem = _immutable

def freeze(obj):
    """
    Freeze an object by making it immutable and thus hashable.
    """
    if isinstance(obj, dict):
        return FrozenDict((k, freeze(v)) for k, v in obj.items())
    elif isinstance(obj, (list, tuple)):
        return tuple(freeze(el) for el in obj)
    elif isinstance(obj, set):
        return frozenset(freeze(el) for el in obj)
    return obj