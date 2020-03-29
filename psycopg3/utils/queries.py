"""
Utility module to manipulate queries
"""

# Copyright (C) 2020 The Psycopg Team

import re
from codecs import CodecInfo
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Match,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from .. import exceptions as exc
from ..pq import Format
from .typing import Params


def query2pg(
    query: bytes, vars: Params, codec: CodecInfo
) -> Tuple[bytes, List[Format], Optional[List[str]]]:
    """
    Convert Python query and params into something Postgres understands.

    - Convert Python placeholders (``%s``, ``%(name)s``) into Postgres
      format (``$1``, ``$2``)
    - placeholders can be %s or %b (text or binary)
    - return ``query`` (bytes), ``formats`` (list of formats) ``order``
      (sequence of names used in the query, in the position they appear, in

    """
    if not isinstance(query, bytes):
        # encoding from str already happened
        raise TypeError(
            f"the query should be str or bytes,"
            f" got {type(query).__name__} instead"
        )

    parts = split_query(query, codec.name)
    order: Optional[List[str]] = None
    chunks: List[bytes] = []
    formats = []

    if isinstance(vars, Sequence) and not isinstance(vars, (bytes, str)):
        if len(vars) != len(parts) - 1:
            raise exc.ProgrammingError(
                f"the query has {len(parts) - 1} placeholders but"
                f" {len(vars)} parameters were passed"
            )
        if vars and not isinstance(parts[0].item, int):
            raise TypeError(
                "named placeholders require a mapping of parameters"
            )

        for part in parts[:-1]:
            assert isinstance(part.item, int)
            chunks.append(part.pre)
            chunks.append(b"$%d" % (part.item + 1))
            formats.append(part.format)

    elif isinstance(vars, Mapping):
        if vars and len(parts) > 1 and not isinstance(parts[0][1], str):
            raise TypeError(
                "positional placeholders (%s) require a sequence of parameters"
            )
        seen: Dict[str, Tuple[bytes, Format]] = {}
        order = []
        for part in parts[:-1]:
            assert isinstance(part.item, str)
            formats.append(part.format)
            chunks.append(part.pre)
            if part.item not in seen:
                ph = b"$%d" % (len(seen) + 1)
                seen[part.item] = (ph, part.format)
                order.append(part.item)
                chunks.append(ph)
            else:
                if seen[part.item][1] != part.format:
                    raise exc.ProgrammingError(
                        f"placeholder '{part.item}' cannot have"
                        f" different formats"
                    )
                chunks.append(seen[part.item][0])

    else:
        raise TypeError(
            f"query parameters should be a sequence or a mapping,"
            f" got {type(vars).__name__}"
        )

    # last part
    chunks.append(parts[-1].pre)

    return b"".join(chunks), formats, order


_re_placeholder = re.compile(
    rb"""(?x)
        %                       # a literal %
        (?:
            (?:
                \( ([^)]+) \)   # or a name in (braces)
                .               # followed by a format
            )
            |
            (?:.)               # or any char, really
        )
        """
)


class QueryPart(NamedTuple):
    pre: bytes
    item: Union[int, str]
    format: Format


def split_query(query: bytes, encoding: str = "ascii") -> List[QueryPart]:
    parts: List[Tuple[bytes, Optional[Match[bytes]]]] = []
    cur = 0

    # pairs [(fragment, match], with the last match None
    m = None
    for m in _re_placeholder.finditer(query):
        pre = query[cur : m.span(0)[0]]
        parts.append((pre, m))
        cur = m.span(0)[1]
    if m is None:
        parts.append((query, None))
    else:
        parts.append((query[cur:], None))

    rv = []

    # drop the "%%", validate
    i = 0
    phtype = None
    while i < len(parts):
        pre, m = parts[i]
        if m is None:
            # last part
            rv.append(QueryPart(pre, 0, Format.TEXT))
            break

        ph = m.group(0)
        if ph == b"%%":
            # unescape '%%' to '%' and merge the parts
            pre1, m1 = parts[i + 1]
            parts[i + 1] = (pre + b"%" + pre1, m1)
            del parts[i]
            continue

        if ph == b"%(":
            raise exc.ProgrammingError(
                f"incomplete placeholder:"
                f" '{query[m.span(0)[0]:].split()[0].decode(encoding)}'"
            )
        elif ph == b"% ":
            # explicit messasge for a typical error
            raise exc.ProgrammingError(
                "incomplete placeholder: '%'; if you want to use '%' as an"
                " operator you can double it up, i.e. use '%%'"
            )
        elif ph[-1:] not in b"bs":
            raise exc.ProgrammingError(
                f"only '%s' and '%b' placeholders allowed, got"
                f" {m.group(0).decode(encoding)}"
            )

        # Index or name
        item: Union[int, str]
        item = i if m.group(1) is None else m.group(1).decode(encoding)

        if phtype is None:
            phtype = type(item)
        else:
            if phtype is not type(item):  # noqa
                raise exc.ProgrammingError(
                    "positional and named placeholders cannot be mixed"
                )

        # Binary format
        format = Format(ph[-1:] == b"b")

        rv.append(QueryPart(pre, item, format))
        i += 1

    return rv


def reorder_params(
    params: Mapping[str, Any], order: Sequence[str]
) -> List[str]:
    """
    Convert a mapping of parameters into an array in a specified order
    """
    try:
        return [params[item] for item in order]
    except KeyError:
        raise exc.ProgrammingError(
            f"query parameter missing:"
            f" {', '.join(sorted(i for i in order if i not in params))}"
        )
