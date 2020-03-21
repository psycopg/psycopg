"""
Various functionalities to make easier to work with the libpq.
"""

# Copyright (C) 2020 The Psycopg Team


def error_message(obj):
    """
    Return an error message from a PGconn or PGresult.

    The return value is a str (unlike pq data which is usually bytes).
    """
    from psycopg3 import pq

    if isinstance(obj, pq.PGconn):
        msg = obj.error_message

        # strip severity and whitespaces
        if msg:
            msg = msg.splitlines()[0].split(b":", 1)[-1].strip()

    elif isinstance(obj, pq.PGresult):
        msg = obj.error_field(pq.DiagnosticField.MESSAGE_PRIMARY)
        if not msg:
            msg = obj.error_message

            # strip severity and whitespaces
            if msg:
                msg = msg.splitlines()[0].split(b":", 1)[-1].strip()

    else:
        raise TypeError(
            f"PGconn or PGresult expected, got {type(obj).__name__}"
        )

    if msg:
        msg = msg.decode("utf8", "replace")  # TODO: or in connection encoding?
    else:
        msg = "no details available"

    return msg
