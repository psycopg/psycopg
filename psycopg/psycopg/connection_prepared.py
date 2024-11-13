from typing import Any
from .abc import AdaptContext, ConnParam
from .rows import RowFactory
from ._compat import Self
from .cursor import Cursor
from .connection import Connection


class PreparedConnection(Connection):
    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        context: AdaptContext | None = None,
        row_factory: RowFactory[tuple[Any, ...]] | None = None,
        cursor_factory: type[Cursor[tuple[Any, ...]]] | None = None,
        **kwargs: ConnParam
    ) -> Self:
        prepare_threshold = 0
        return super().connect(
            conninfo,
            autocommit=autocommit,
            prepare_threshold=prepare_threshold,
            context=context,
            row_factory=row_factory,
            cursor_factory=cursor_factory,
            **kwargs,
        )
