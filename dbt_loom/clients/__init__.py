_GZIP_MAGIC = b"\x1f\x8b"


def is_gzipped(data: bytes) -> bool:
    """Check the magic bytes of a file to determine if it has been gzipped"""
    return data[:2] == _GZIP_MAGIC
