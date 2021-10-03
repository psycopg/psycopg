import pickle

import pytest

from psycopg.adapt import PyFormat
from psycopg.types.range import Range
from psycopg.types.multirange import Multirange

pytestmark = pytest.mark.pg(">= 14")

mr_names = """int4multirange int8multirange nummultirange
    datemultirange tsmultirange tstzmultirange""".split()

mr_classes = """Int4Multirange Int8Multirange NumericMultirange
    DateMultirange TimestampMultirange TimestamptzMultirange""".split()


class TestMultirangeObject:
    def test_empty(self):
        mr = Multirange()
        assert not mr
        assert len(mr) == 0

        mr = Multirange([])
        assert not mr
        assert len(mr) == 0

    def test_sequence(self):
        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])
        assert mr
        assert len(mr) == 3
        assert mr[2] == Range(50, 60)
        assert mr[-2] == Range(30, 40)

    def test_setitem(self):
        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])
        mr[1] = Range(31, 41)
        assert mr == Multirange([Range(10, 20), Range(31, 41), Range(50, 60)])

    def test_delitem(self):
        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])
        del mr[1]
        assert mr == Multirange([Range(10, 20), Range(50, 60)])

        del mr[-2]
        assert mr == Multirange([Range(50, 60)])

    def test_relations(self):
        mr1 = Multirange([Range(10, 20), Range(30, 40)])
        mr2 = Multirange([Range(11, 20), Range(30, 40)])
        mr3 = Multirange([Range(9, 20), Range(30, 40)])
        assert mr1 <= mr1
        assert not mr1 < mr1
        assert mr1 >= mr1
        assert not mr1 > mr1
        assert mr1 < mr2
        assert mr1 <= mr2
        assert mr1 > mr3
        assert mr1 >= mr3
        assert mr1 != mr2
        assert not mr1 == mr2

    def test_pickling(self):
        r = Multirange([Range(0, 4)])
        assert pickle.loads(pickle.dumps(r)) == r

    def test_str(self):
        mr = Multirange([Range(10, 20), Range(30, 40)])
        assert str(mr) == "{[10, 20), [30, 40)}"

    def test_repr(self):
        mr = Multirange([Range(10, 20), Range(30, 40)])
        expected = "Multirange([Range(10, 20, '[)'), Range(30, 40, '[)')])"
        assert repr(mr) == expected


@pytest.mark.parametrize("pgtype", mr_names)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_empty(conn, pgtype, fmt_in):
    mr = Multirange()
    cur = conn.execute(f"select '{{}}'::{pgtype} = %{fmt_in}", (mr,))
    assert cur.fetchone()[0] is True
