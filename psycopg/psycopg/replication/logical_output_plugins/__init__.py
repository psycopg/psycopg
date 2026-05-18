from typing import Any

from . import pgoutput
from .decoder import DispatchingDecoder, TextDecoder
from .output_plugin_options import OutputPluginOptionsBase, TestDecodingOptions
from .output_plugin_options import get_output_plugin_options
from .output_plugin_options import register_output_plugin_options


def register_default_output_plugins() -> None:
    register_output_plugin_options("pgoutput", pgoutput.PgOutputOptions)
    register_output_plugin_options("test_decoding", TestDecodingOptions)
    DispatchingDecoder.register_decoder("pgoutput", pgoutput.PgOutputDecoder[Any])
    DispatchingDecoder.register_decoder("test_decoding", TextDecoder)


__all__ = [
    "register_default_output_plugins",
    "get_output_plugin_options",
    "OutputPluginOptionsBase",
    "DispatchingDecoder",
    "TextDecoder",
    "TestDecodingOptions",
]
