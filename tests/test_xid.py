import psycopg


class TestXidObject:
    def test_xid_construction(self):
        x1 = psycopg.Xid(74, "foo", "bar")
        74 == x1.format_id
        "foo" == x1.gtrid
        "bar" == x1.bqual

    def test_xid_from_string(self):
        x2 = psycopg.Xid.from_string("42_Z3RyaWQ=_YnF1YWw=")
        42 == x2.format_id
        "gtrid" == x2.gtrid
        "bqual" == x2.bqual

        x3 = psycopg.Xid.from_string("99_xxx_yyy")
        None is x3.format_id
        "99_xxx_yyy" == x3.gtrid
        None is x3.bqual

    def test_xid_to_string(self):
        x1 = psycopg.Xid.from_string("42_Z3RyaWQ=_YnF1YWw=")
        str(x1) == "42_Z3RyaWQ=_YnF1YWw="

        x2 = psycopg.Xid.from_string("99_xxx_yyy")
        str(x2) == "99_xxx_yyy"
