from typing import Any

from psycopg import sql

from ..output_plugin_options import OutputPluginOptionsBase


class PgOutputOptions(OutputPluginOptionsBase):
    __module__ = "psycopg.replication.logical_output_plugins.pgoutput"

    name = "pgoutput"
    # TASK: update proto_version on PostgreSQL upgrade
    default_opts: dict[str, Any] = {"proto_version": 2}
    required_opts = frozenset(("publication_names",))
    boolean_opts = frozenset(("binary", "messages", "two_phase"))
    enum_opts = {
        "origin": frozenset(("none", "any")),
        "streaming": frozenset(("on", "off", "parallel")),
    }
    opt_to_str_transforms = {
        "publication_names": lambda opt: ", ".join(
            sql.Identifier(name).as_string()
            for name in (opt if not isinstance(opt, str) else [opt])
        ),
    }

    def validate_opts(self) -> None:
        super().validate_opts()
        opts = self.opts
        proto_version = int(opts["proto_version"])
        streaming_opt = opts.get("streaming")
        if proto_version < 2 and streaming_opt is not None:
            if streaming_opt == "off":
                del self.opts["streaming"]
            else:
                self._raise_validation_error(
                    f"Protocol version {proto_version} does not support streaming"
                )

        if proto_version < 4 and streaming_opt == "parallel":
            self._raise_validation_error(
                f"Protocol version {proto_version} does not support parallel streaming"
            )

        if proto_version < 3 and "two_phase" in opts:
            self._raise_validation_error(
                f"Protocol version {proto_version} does not support two phase "
                "transactions"
            )
