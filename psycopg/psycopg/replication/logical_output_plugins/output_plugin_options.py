from __future__ import annotations

from typing import Any, Callable, NoReturn
from collections.abc import Set

from ... import errors as e
from .abc import OutputPluginOptions

_options_registry: dict[str, type[OutputPluginOptions]] = {}


def register_output_plugin_options(
    name: str,
    options_class: type[OutputPluginOptions],
    force: bool = False,
) -> None:
    if not force and name in _options_registry:
        raise e.ProgrammingError(
            "Output plugin options already registered for '{name}'"
            "\nHINT: use force=True to override."
        )
    _options_registry[name] = options_class


def get_output_plugin_options(output_plugin: str) -> type[OutputPluginOptions]:
    try:
        options_cls = _options_registry[output_plugin]
    except KeyError as err:
        raise e.ProgrammingError(
            f"Output plugin options have not been registered for '{output_plugin}'"
            "\nHINT: you may want to use `raw_output_plugin_options`"
        ) from err
    return options_cls


class OutputPluginOptionsBase:
    name: str
    default_opts: dict[str, str] = {}
    required_opts: Set[str] = frozenset()
    boolean_opts: Set[str] = frozenset()
    integer_opts: Set[str] = frozenset()
    enum_opts: dict[str, Set[str]] = {}
    opt_to_str_transforms: dict[str, Callable[[Any], str]] = {}

    def __init__(self, opts: dict[str, Any]):
        self.opts: dict[str, Any] = {}
        self.string_opts: dict[str, str] = {}
        self._original_opts: dict[str, Any] = opts
        self.opts.update(self.default_opts, **opts)
        self.string_opts = {
            self.transform_opt_name(opt_name): self.opt_to_string(opt_name, opt)
            for opt_name, opt in self.opts.items()
        }

    def transform_opt_name(self, opt_name: str) -> str:
        return opt_name

    def opt_to_string(self, opt_name: str, opt: Any) -> str:
        if opt_name in self.opt_to_str_transforms:
            return self.opt_to_str_transforms[opt_name](opt)
        if opt_name in self.boolean_opts:
            return str(opt).lower()
        return str(opt)

    def validate_required_opts(self) -> None:
        if missing_opts := self.required_opts - self.opts.keys():
            self._raise_validation_error(
                f"{self.name} requires option{'s' if len(missing_opts) > 1 else ''}: "
                f"{', '.join(missing_opts)}"
            )

    def validate_boolean_opts(self) -> None:
        opts = self._original_opts
        for opt_name in self.boolean_opts & opts.keys():
            opt = opts[opt_name]
            if opt is not True and opt is not False:
                self.raise_option_validation_error(opt_name, opt, "must be boolean")

    def validate_integer_opts(self) -> None:
        opts = self._original_opts
        for opt_name in self.integer_opts - opts.keys():
            opt = opts[opt_name]
            if not isinstance(opt, int):
                self.raise_option_validation_error(opt_name, opt, "must be an integer")

    def validate_enum_opts(self) -> None:
        opts = self._original_opts
        for opt_name, valid_values in self.enum_opts.items():
            opt = opts.get(opt_name, None)
            if opt is not None and opt not in valid_values:
                self.raise_option_validation_error(
                    opt_name, opt, f"must be one of {', '.join(valid_values)}"
                )

    def validate_opts(self) -> None:
        if self.required_opts:
            self.validate_required_opts()
        if self.boolean_opts:
            self.validate_boolean_opts()
        if self.integer_opts:
            self.validate_integer_opts()
        if self.enum_opts:
            self.validate_enum_opts()

    def raise_option_validation_error(
        self, opt_name: str, opt: Any, msg: str
    ) -> NoReturn:
        self._raise_validation_error(
            f"{self.name} option '{opt_name}' {msg}: {opt!r} {type(opt)!r}"
        )

    def _raise_validation_error(self, msg: str) -> NoReturn:
        raise e.ProgrammingError(msg)


class TestDecodingOptions(OutputPluginOptionsBase):
    name = "test_decoding"

    boolean_opts = frozenset(
        (
            "include_xids",
            "include_timestamp",
            "force_binary",
            "skip_empty_xacts",
            "only_local",
            "include_rewrites",
            "stream_changes",
        )
    )

    def transform_opt_name(self, opt_name: str) -> str:
        return opt_name.replace("_", "-")
