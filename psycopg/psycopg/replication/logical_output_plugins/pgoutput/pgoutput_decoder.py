from __future__ import annotations

from typing import Any, cast
from functools import lru_cache
from collections import defaultdict

from .... import errors as e
from .... import pq
from ..abc import LogicalRowFactoryXLogDataDecoder
from ....abc import Buffer, Transformer
from ..logical_rows import LogicalRow, LogicalRowFactory, LogicalRowMaker, tuple_row
from .pgoutput_messages import (
    OriginMessage,
    PgOutputMessage,
    RelationMessage,
    TypeMessage,
    _type_to_decode_map,
)


class PgOutputDecoder(
    LogicalRowFactoryXLogDataDecoder[PgOutputMessage[LogicalRow]],
):
    __module__ = "psycopg.replication.logical_output_plugins.pgoutput"

    output_plugin = "pgoutput"

    relations: dict[int, RelationMessage]
    # xact: {subxact: {relation_id: relation}}
    relations_by_xid: dict[int, dict[int, RelationMessage]]
    types: dict[int, TypeMessage]
    # xact: {subxact: {type_id: type}}
    types_by_xid: dict[int, dict[int, TypeMessage]]
    origin: OriginMessage | None
    origin_by_xid: dict[int, OriginMessage]
    server_encoding: str
    streaming: int | None
    _plugin_options: dict[str, Any]
    _tx: Transformer | None

    def __init__(
        self,
        row_factory: LogicalRowFactory[LogicalRow] = cast(
            LogicalRowFactory[LogicalRow], tuple_row
        ),
        _plugin_options: dict[str, Any] | None = None,
        _server_encoding: str | None = None,
        _tx: Transformer | None = None,
    ):
        super().__init__(
            _plugin_options=_plugin_options,
            _server_encoding=_server_encoding,
            _tx=_tx,
            row_factory=row_factory,
        )

        self.relations = {}
        self.relations_by_xid = defaultdict(dict)
        self.types = {}
        self.types_by_xid = defaultdict(dict)
        self.origin = None
        self.origin_by_xid = {}

        self.streaming = None

    @property
    def plugin_options(self) -> dict[str, Any]:
        return self._plugin_options

    @plugin_options.setter
    def plugin_options(self, options: dict[str, Any]) -> None:
        if options.get("binary", False):
            self.format = pq.Format.BINARY
        else:
            self.format = pq.Format.TEXT
        self._plugin_options = options

    def __call__(self, payload: Buffer) -> PgOutputMessage[LogicalRow]:
        msg_type = chr(payload[0])
        try:
            decode = _type_to_decode_map[msg_type].decode
        except KeyError as err:
            raise e.DataError(
                f"Unexpected message type for pgoutput plugin: '{msg_type}'"
            ) from err
        return decode(self, payload[1:])

    def get_relation(self, relation_id: int) -> RelationMessage:
        if self.streaming is None:
            return self.relations[relation_id]
        else:
            return self.relations_by_xid[self.streaming][relation_id]

    def get_row_maker(self, relation: RelationMessage) -> LogicalRowMaker[LogicalRow]:
        if self.row_factory is tuple_row:
            return tuple  # type: ignore[return-value]
        return self._get_row_maker(relation)

    @lru_cache(maxsize=128)
    def _get_row_maker(self, relation: RelationMessage) -> LogicalRowMaker[LogicalRow]:
        return self.row_factory(self, relation.relation_id)
