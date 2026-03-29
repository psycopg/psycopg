from enum import StrEnum


class ReplicationType(StrEnum):
    LOGICAL = "LOGICAL"
    PHYSICAL = "PHYSICAL"


class SnapshotOption(StrEnum):
    all_values: frozenset[str]

    EXPORT = "export"
    USE = "use"
    NOTHING = "nothing"


SnapshotOption.all_values = frozenset(item.value for item in SnapshotOption)


class ReplicaIdentity(StrEnum):
    DEFAULT = "d"
    NOTHING = "n"
    FULL = "f"
    INDEX = "i"
