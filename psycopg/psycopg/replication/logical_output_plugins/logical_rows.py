from __future__ import annotations

import functools
from enum import Enum
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol, TypeAlias
from collections import namedtuple
from collections.abc import Callable, Sequence

from ..._compat import TypeVar
from ..._encodings import _as_python_identifier

if TYPE_CHECKING:
    from .abc import LogicalRowFactoryXLogDataDecoder

T = TypeVar("T", covariant=True)
LogicalRow = TypeVar("LogicalRow", covariant=True, default="LogicalTupleRow")


class RowValue(Enum):
    UNCHANGED = "u"


class LogicalRowMaker(Protocol[LogicalRow]):
    """
    Callable protocol taking a sequence of values and returning an object.

    The sequence of values is what is returned from a database query, already
    adapted to the right Python types. The return value is the object that your
    program would like to receive: by default (`tuple_row()`) it is a simple
    tuple, but it may be any type of object.

    Typically, `!LogicalRowMaker` functions are returned by `LogicalRowFactory`.
    """

    def __call__(self, __values: Sequence[Any]) -> LogicalRow: ...


class LogicalRowFactory(Protocol[LogicalRow]):
    """
    Callable protocol taking a
    `~psycopg.replication.logical_output_plugins.abc.LogicalRowFactoryXLogDataDecoder`
    and a `relation_id` and returning a `LogicalRowMaker`.
    """

    def __call__(
        self,
        __decoder: LogicalRowFactoryXLogDataDecoder[Any],
        relation_id: int,
    ) -> LogicalRowMaker[LogicalRow]: ...


LogicalTupleRow: TypeAlias = tuple[Any, ...]
"""
An alias for the type returned by `tuple_row()` (i.e. a tuple of any content).
"""


LogicalDictRow: TypeAlias = dict[str, Any]
"""
An alias for the type returned by `dict_row()` (i.e. a dictionary with keys as
string and any value returned by the database).
"""


def tuple_row(
    decoder: LogicalRowFactoryXLogDataDecoder[Any], relation_id: int
) -> LogicalRowMaker[LogicalTupleRow]:
    r"""Row factory to represent rows as simple tuples.

    This is the default factory, used when the builtin decoders
    are created without a `!row_factory` parameter.

    """
    return tuple


def dict_row(
    decoder: LogicalRowFactoryXLogDataDecoder[Any], relation_id: int
) -> LogicalRowMaker[LogicalDictRow]:
    """Row factory to represent rows as dictionaries.

    The dictionary keys are taken from the column names of the provided relation.
    In the builtin decoders, these are taken from the latest relation message.
    """

    names = get_names(decoder, relation_id)

    def dict_row_(values: Sequence[Any]) -> dict[str, Any]:
        return dict(zip(names, values))

    return dict_row_


def namedtuple_row(
    decoder: LogicalRowFactoryXLogDataDecoder[Any], relation_id: int
) -> LogicalRowMaker[NamedTuple]:
    """Row factory to represent rows as `~collections.namedtuple`.

    The field names are taken from the column names of the provided relation.
    """
    nt = _make_nt(*get_names(decoder, relation_id))
    return nt._make


def class_row(cls: type[T]) -> LogicalRowFactory[T]:
    r"""Generate a row factory to represent rows as instances of the class `!cls`.

    The class must support every output column name as a keyword parameter.

    :param cls: The class to return for each row. It must support the fields
        on the relation returned by logical decoding as keyword arguments.
    :rtype: `!Callable[[Cursor],` `RowMaker`\[~T]]
    """

    def class_row_(
        decoder: LogicalRowFactoryXLogDataDecoder[Any], relation_id: int
    ) -> LogicalRowMaker[T]:
        names = get_names(decoder, relation_id)

        def class_row__(values: Sequence[Any]) -> T:
            return cls(**dict(zip(names, values)))

        return class_row__

    return class_row_


def args_row(func: Callable[..., T]) -> LogicalRowFactory[T]:
    """Generate a row factory calling `!func` with positional parameters for every row.

    :param func: The function to call for each row. It must support the fields
        on the relation returned by logical decoding as positional arguments.
    """

    def args_row_(
        decoder: LogicalRowFactoryXLogDataDecoder[Any], relation_id: int
    ) -> LogicalRowMaker[T]:
        def args_row__(values: Sequence[Any]) -> T:
            return func(*values)

        return args_row__

    return args_row_


def kwargs_row(func: Callable[..., T]) -> LogicalRowFactory[T]:
    """Generate a row factory calling `!func` with keyword parameters for every row.

    :param func: The function to call for each row. It must support the fields
        on the relation returned by logical decoding as keyword arguments.
    """

    def kwargs_row_(
        decoder: LogicalRowFactoryXLogDataDecoder[Any], relation_id: int
    ) -> LogicalRowMaker[T]:

        names = get_names(decoder, relation_id)

        def kwargs_row__(values: Sequence[Any]) -> T:
            return func(**dict(zip(names, values)))

        return kwargs_row__

    return kwargs_row_


def get_names(
    decoder: LogicalRowFactoryXLogDataDecoder[Any], relation_id: int
) -> list[str]:
    relation = decoder.get_relation(relation_id)
    return [col.name for col in relation.columns]


@functools.lru_cache(512)
def _make_nt(*names: str) -> type[NamedTuple]:
    snames = tuple(_as_python_identifier(n) for n in names)
    return namedtuple("Row", snames)  # type: ignore[return-value]
