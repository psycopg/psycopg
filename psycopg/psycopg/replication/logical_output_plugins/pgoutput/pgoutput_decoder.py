from __future__ import annotations

from typing import Any, cast
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
        if self.format == pq.Format.TEXT:
            filter_initialized = lambda d: {
                key: value for key, value in d if key != "_initialized"
            }
            file = "./pgoutput_message_examples.json"
            try:
                with open(file, "x") as f:
                    pass
            except FileExistsError:
                pass
            with open(file, "r+") as f:
                # import pdb
                import os
                import json
                import base64
                from dataclasses import asdict

                # pdb.set_trace()
                if not f.read().strip():
                    msg_examples = {}
                else:
                    f.seek(0)
                    msg_examples = json.load(f)

                if (
                    streaming_opt := self.plugin_options.get("streaming", "off")
                ) == "parallel":
                    streaming_key = "example_msgs_stream_parallel"
                elif streaming_opt == "on":
                    streaming_key = "example_msgs_stream"
                else:
                    streaming_key = "example_msgs"
                if msg_type not in msg_examples.setdefault(
                    streaming_key, {}
                ).setdefault("msgs", {}):
                    msg_examples[streaming_key]["msgs"][msg_type] = base64.b64encode(
                        payload
                    ).decode()
                    msg_examples[streaming_key].setdefault("streaming_xid", {})[
                        msg_type
                    ] = self.streaming
                    if msg_type in "IUDT":
                        msg_examples[streaming_key].setdefault("relations", {}).update(
                            {
                                rel_id: asdict(val, dict_factory=filter_initialized)
                                for rel_id, val in self.relations.items()
                            }
                        )
                        for xid in self.relations_by_xid:
                            for subxid in self.relations_by_xid[xid]:
                                msg_examples[streaming_key].setdefault(
                                    "relations_by_xid", {}
                                ).setdefault(xid, {}).update(
                                    {
                                        rel_id: asdict(
                                            val, dict_factory=filter_initialized
                                        )
                                        for rel_id, val in self.relations_by_xid[
                                            xid
                                        ].items()
                                    }
                                )
                f.seek(0)
                json.dump(msg_examples, f, indent=4)
                f.flush()
                os.fsync(f)

        return decode(self, payload[1:])

    def get_relation(self, relation_id: int) -> RelationMessage:
        if self.streaming is None:
            return self.relations[relation_id]
        else:
            return self.relations_by_xid[self.streaming][relation_id]

    def get_row_maker(self, relation: RelationMessage) -> LogicalRowMaker[LogicalRow]:
        return self.row_factory(self, relation.relation_id)
