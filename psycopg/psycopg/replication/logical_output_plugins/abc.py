from __future__ import annotations

from typing import Any, Protocol


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
