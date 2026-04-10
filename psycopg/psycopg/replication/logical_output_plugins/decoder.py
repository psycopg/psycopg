from typing import Any, ClassVar

from ... import errors as e
from .abc import LogicalXLogDataDecoder
from ...abc import Buffer, Transformer
from ..replication_messages import DecodedPayload


class DispatchingDecoder(LogicalXLogDataDecoder[DecodedPayload]):
    server_encoding: str
    output_plugin: str | None = None
    _plugin_options: dict[str, Any]
    _tx: Transformer | None
    _decoder_registry: ClassVar[dict[str, type[LogicalXLogDataDecoder[Any]]]] = {}

    @property
    def plugin_options(self) -> dict[str, Any]:
        return self._plugin_options

    @plugin_options.setter
    def plugin_options(self, options: dict[str, Any]) -> None:
        self._plugin_options = options

    @classmethod
    def register_decoder(
        cls,
        output_plugin: str,
        decoder: type[LogicalXLogDataDecoder[Any]],
        force: bool = False,
    ) -> None:
        if not force and output_plugin in cls._decoder_registry:
            raise ValueError(
                "Output plugin decoder already registered for '{output_plugin}'."
                " Use force=True to override."
            )
        cls._decoder_registry[output_plugin] = decoder

    def __init__(
        self,
        allow_unknown_output_plugins: bool = False,
        **kwargs: Any,
    ):
        self.allow_unknown_output_plugins = allow_unknown_output_plugins
        self.kwargs = kwargs

    def _get_decoder_cls(
        self, output_plugin: str | None
    ) -> type[LogicalXLogDataDecoder[DecodedPayload]] | None:
        if output_plugin is None:
            raise e.ProgrammingError(
                "DispatchingDecoder hasn't been initialized with an 'output_plugin'"
            )
        return self._decoder_registry.get(output_plugin)

    def _instantiate_decoder(
        self, decoder_cls: type[LogicalXLogDataDecoder[DecodedPayload]]
    ) -> LogicalXLogDataDecoder[DecodedPayload]:
        return decoder_cls(
            _plugin_options=self.plugin_options,
            _server_encoding=self.server_encoding,
            _tx=self._tx,
            **self.kwargs,
        )

    def get_real_decoder(self) -> LogicalXLogDataDecoder[DecodedPayload] | None:
        decoder_cls = self._get_decoder_cls(self.output_plugin)
        if decoder_cls is None:
            if self.allow_unknown_output_plugins:
                return None
            raise e.ProgrammingError(
                f"No decoder registered for '{self.output_plugin}'"
            )
        decoder = self._instantiate_decoder(decoder_cls)
        return decoder

    def __call__(
        self,
        payload: Buffer,
    ) -> DecodedPayload | Buffer:
        decoder = self.get_real_decoder()
        if decoder is None:
            return payload
        return decoder(payload)
