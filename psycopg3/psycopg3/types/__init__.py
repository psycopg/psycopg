"""
psycopg3 types package
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import TYPE_CHECKING

from . import net
from . import bool
from . import json
from . import none
from . import uuid
from . import array
from . import range
from . import string
from . import numeric
from . import datetime
from . import composite

from .._typeinfo import TypeInfo as TypeInfo  # exported here

if TYPE_CHECKING:
    from ..proto import AdaptContext


def register_default_globals(ctx: "AdaptContext") -> None:
    net.register_default_globals(ctx)
    bool.register_default_globals(ctx)
    json.register_default_globals(ctx)
    none.register_default_globals(ctx)
    uuid.register_default_globals(ctx)
    array.register_default_globals(ctx)
    range.register_default_globals(ctx)
    string.register_default_globals(ctx)
    numeric.register_default_globals(ctx)
    datetime.register_default_globals(ctx)
    composite.register_default_globals(ctx)

    # Must come after all the types are registered
    array.register_all_arrays(ctx)
