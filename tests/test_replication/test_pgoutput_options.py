from __future__ import annotations

import pytest

from psycopg import errors as e
from psycopg.replication.logical_output_plugins import (
    get_output_plugin_options,
)
from psycopg.replication.logical_output_plugins.pgoutput import (
    PgOutputOptions,
)


class TestPgOutputOptions:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.plugin_options = PgOutputOptions(
            {"publication_names": ["mypub", "pub2"], "binary": True}
        )
        self.plugin_options.validate_opts()

    def test_name(self):
        assert self.plugin_options.name == "pgoutput"

    def test_default_proto_version(self):
        opts = self.plugin_options
        assert opts.opts["proto_version"] == 2
        assert opts.string_opts["proto_version"] == "2"

    def test_provided_publication_names(self):
        opts = self.plugin_options
        assert opts.opts["publication_names"] == ["mypub", "pub2"]
        assert opts.string_opts["publication_names"] == '"mypub", "pub2"'

    def test_extra_opt_preserved(self):
        opts = self.plugin_options
        assert opts.opts["binary"] is True
        assert opts.string_opts["binary"] == "true"

    def test_missing_publication_names_raises(self):
        opts = PgOutputOptions({})
        with pytest.raises(e.ProgrammingError, match="publication_names"):
            opts.validate_opts()

    def test_valid_options_passes(self):
        opts = PgOutputOptions({"publication_names": "mypub"})
        opts.validate_opts()  # should not raise

    def test_streaming_requires_proto_version_2(self):
        opts = PgOutputOptions(
            {"publication_names": "mypub", "proto_version": "1", "streaming": "on"}
        )
        with pytest.raises(e.ProgrammingError, match="streaming"):
            opts.validate_opts()

    def test_streaming_off_with_proto_version_1_ok(self):
        opts = PgOutputOptions(
            {"publication_names": "mypub", "proto_version": "1", "streaming": "off"}
        )
        opts.validate_opts()  # should not raise

    def test_streaming_parallel_requires_proto_version_4(self):
        opts = PgOutputOptions(
            {
                "publication_names": "mypub",
                "proto_version": "3",
                "streaming": "parallel",
            }
        )
        with pytest.raises(e.ProgrammingError, match="parallel"):
            opts.validate_opts()

    def test_two_phase_requires_proto_version_3(self):
        opts = PgOutputOptions(
            {"publication_names": "mypub", "proto_version": "2", "two_phase": True}
        )
        with pytest.raises(e.ProgrammingError, match="two phase"):
            opts.validate_opts()

    def test_two_phase_with_proto_version_3_ok(self):
        opts = PgOutputOptions(
            {"publication_names": "mypub", "proto_version": "3", "two_phase": True}
        )
        opts.validate_opts()

    def test_invalid_boolean_opt_raises(self):
        opts = PgOutputOptions({"publication_names": "mypub", "binary": "true"})
        with pytest.raises(e.ProgrammingError):
            opts.validate_opts()

    def test_invalid_origin_option_raises(self):
        opts = PgOutputOptions(
            {"publication_names": "mypub", "origin": "invalid_origin"}
        )
        with pytest.raises(e.ProgrammingError):
            opts.validate_opts()

    @pytest.mark.parametrize("origin", ["any", "none"])
    def test_valid_origin_any(self, origin):
        opts = PgOutputOptions({"publication_names": "mypub", "origin": origin})
        opts.validate_opts()


class TestOutputPluginOptionsRegistry:
    def test_pgoutput_registered(self):
        """pgoutput should be in the registry after default plugins are registered."""
        subclass = get_output_plugin_options("pgoutput")
        assert subclass is PgOutputOptions

    def test_unknown_plugin_raises(self):
        with pytest.raises((e.ProgrammingError), match="not been registered"):
            get_output_plugin_options("no_such_plugin_xyz")
