from . import pgoutput
from .output_plugin_options import OutputPluginOptionsBase, get_output_plugin_options
from .output_plugin_options import register_output_plugin_options


def register_default_output_plugins() -> None:
    register_output_plugin_options("pgoutput", pgoutput.PgOutputOptions)


__all__ = [
    "register_default_output_plugins",
    "get_output_plugin_options",
    "OutputPluginOptionsBase",
]
