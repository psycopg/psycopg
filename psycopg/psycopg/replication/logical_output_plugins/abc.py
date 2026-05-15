from __future__ import annotations

from typing import Any, Protocol

from ... import adapt
from ..abc import XLogDataDecoder
from ...abc import Transformer
from .logical_rows import LogicalRow, LogicalRowFactory
from ..replication_options import ReplicaIdentity
from ..replication_messages import DecodedPayload


class ColumnDefinition(Protocol):
    """
    Protocol representing the expected shape of an element of
    `Relation.columns`.
    """

    @property
    def name(self) -> str: ...

    @property
    def type_id(self) -> int: ...

    @property
    def type_modifier(self) -> int: ...

    @property
    def is_key(self) -> bool:
        """Check if column is part of replica identity."""
        ...


class Relation(Protocol):
    """
    Protocol representing the expected shape of relation data
    returned by `LogicalRowFactoryXLogDataDecoder.get_relation()`
    """

    @property
    def relation_id(self) -> int: ...

    @property
    def namespace(self) -> str: ...

    @property
    def relation_name(self) -> str: ...

    @property
    def replica_identity(self) -> ReplicaIdentity: ...

    @property
    def columns(self) -> tuple[ColumnDefinition, ...]: ...


class LogicalXLogDataDecoder(
    XLogDataDecoder[DecodedPayload],
    Protocol[DecodedPayload],
):
    server_encoding: str
    output_plugin: str | None
    _tx: Transformer | None
    _plugin_options: dict[str, Any]

    def __init__(
        self,
        *,
        _plugin_options: dict[str, Any] | None = None,
        _server_encoding: str | None = None,
        _tx: Transformer | None = None,
        **kwargs: Any,
    ):
        self._tx = _tx
        if _plugin_options is not None:
            self.plugin_options = _plugin_options
        else:
            self.plugin_options = {}
        if _server_encoding is not None:
            self.server_encoding = _server_encoding

    def get_real_decoder(self) -> LogicalXLogDataDecoder[DecodedPayload] | None:
        """
        Called to get the actual decoder to support delegating decoders, e.g. based
        on `output_plugin`.
        """
        return self

    @property
    def plugin_options(self) -> dict[str, Any]:
        return self._plugin_options

    @plugin_options.setter
    def plugin_options(self, options: dict[str, Any]) -> None:
        self._plugin_options = options

    @property
    def tx(self) -> Transformer:
        if self._tx is None:
            self._tx = adapt.Transformer()
        return self._tx


class LogicalRowFactoryXLogDataDecoder(
    LogicalXLogDataDecoder[DecodedPayload], Protocol[DecodedPayload]
):
    row_factory: LogicalRowFactory[Any]

    def __init__(
        self,
        *,
        row_factory: LogicalRowFactory[LogicalRow],
        _plugin_options: dict[str, Any] | None = None,
        _server_encoding: str | None = None,
        _tx: Transformer | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            _plugin_options=_plugin_options, _server_encoding=_server_encoding, _tx=_tx
        )
        self.row_factory = row_factory

    def get_relation(self, relation_id: int) -> Relation: ...


class OutputPluginOptions(Protocol):
    #: The python level options
    opts: dict[str, Any] = {}
    #: The string options that will be passed to the `START_REPLICATION`
    #: command. The key is quoted as an SQL Identifier and the value is
    #: quoted as an SQL variable.
    string_opts: dict[str, str] = {}

    def __init__(self, opts: dict[str, Any]): ...

    def validate_opts(self) -> None:
        """
        Ensure that the provided options are valid.
        """
        ...
