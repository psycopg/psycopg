from typing import Protocol

from ..abc import Buffer
from .replication_messages import DecodedPayload


class XLogDataDecoder(Protocol[DecodedPayload]):
    server_encoding: str

    def __call__(self, payload: Buffer) -> DecodedPayload | Buffer: ...
