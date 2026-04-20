"""Minimal subset of six used by python-dateutil in this workspace."""

from __future__ import annotations

import builtins
import sys
import types


PY2 = False
PY3 = True
string_types = (str,)
integer_types = (int,)
class_types = (type,)
text_type = str
binary_type = bytes


def iteritems(mapping):
    return iter(mapping.items())


def itervalues(mapping):
    return iter(mapping.values())


def with_metaclass(meta, *bases):
    class TemporaryClass(*bases, metaclass=meta):
        pass

    return TemporaryClass


def add_metaclass(metaclass):
    def wrapper(cls):
        attrs = dict(cls.__dict__)
        attrs.pop("__dict__", None)
        attrs.pop("__weakref__", None)
        return metaclass(cls.__name__, cls.__bases__, attrs)

    return wrapper


class _Moves(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("six.moves")
        self._thread = __import__("_thread")
        self.range = builtins.range
        self.zip = builtins.zip
        self.map = builtins.map
        self.filter = builtins.filter


moves = _Moves()
sys.modules[__name__ + ".moves"] = moves
