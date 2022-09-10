"""
C optimized functions to manipulate arrays
"""

# Copyright (C) 2022 The Psycopg Team

def array_load_text(
    data: Buffer, load: LoadFunc, delimiter: bytes = b","
) -> List[Any]:
    raise NotImplementedError


def array_load_binary(data: Buffer, load: LoadFunc) -> List[Any]:
    raise NotImplementedError
