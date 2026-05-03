from enum import StrEnum


class CompressionMethod(StrEnum):
    """Compression methods for BASE_BACKUP command."""

    GZIP = "gzip"
    LZ4 = "lz4"
    ZSTD = "zstd"


class CheckpointMode(StrEnum):
    """Checkpoint modes for BASE_BACKUP command."""

    FAST = "fast"
    SPREAD = "spread"


class ManifestOption(StrEnum):
    """Manifest options for BASE_BACKUP command."""

    YES = "yes"
    NO = "no"
    FORCE_ENCODE = "force-encode"


class ManifestChecksums(StrEnum):
    """Manifest checksum algorithms for BASE_BACKUP command."""

    CRC32C = "CRC32C"
    SHA224 = "SHA224"
    SHA256 = "SHA256"
    SHA384 = "SHA384"
    SHA512 = "SHA512"


class BackupTarget(StrEnum):
    """Backup target options for BASE_BACKUP command."""

    CLIENT = "client"
    SERVER = "server"
