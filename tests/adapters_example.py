from typing import Optional

from psycopg import pq
from psycopg.abc import Dumper, Loader, AdaptContext, PyFormat, Buffer


def f() -> None:
    d: Dumper = MyStrDumper(str, None)
    assert d.dump("abc") == b"abcabc"
    assert d.quote("abc") == b"'abcabc'"

    lo: Loader = MyTextLoader(0, None)
    assert lo.load(b"abc") == "abcabc"


class MyStrDumper:
    format = pq.Format.TEXT
    oid = 25  # text

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        self._cls = cls

    def dump(self, obj: str) -> bytes:
        return (obj * 2).encode()

    def quote(self, obj: str) -> bytes:
        value = self.dump(obj)
        esc = pq.Escaping()
        return b"'%s'" % esc.escape_string(value.replace(b"h", b"q"))

    def get_key(self, obj: str, format: PyFormat) -> type:
        return self._cls

    def upgrade(self, obj: str, format: PyFormat) -> "MyStrDumper":
        return self


class MyTextLoader:
    format = pq.Format.TEXT

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        pass

    def load(self, data: Buffer) -> str:
        return (bytes(data) * 2).decode()
