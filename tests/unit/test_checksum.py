"""File checksums (lib/ingestion/object_storage_to_postgres.py): computed while the file is
read for loading, header line included, so it is read only once."""

import hashlib
import io

from lib.ingestion.object_storage_to_postgres import HashingStream
from lib.ingestion.schema_drift import read_header

CONTENT = b"id,amount\n1,10\n2,20\n" * 1000


class ObjectStorageFile:
    """Has the narrower interface of the files object storage returns (fsspec's
    AbstractBufferedFile): readline() takes no size."""

    def __init__(self, content):
        self._file = io.BytesIO(content)

    def read(self, length=-1):
        return self._file.read(length)

    def readline(self):
        return self._file.readline()


def test_the_hash_covers_the_header_and_every_chunk():
    stream = HashingStream(ObjectStorageFile(CONTENT))
    read_header(stream)  # reads the first line, as the loader does
    while stream.read(100):
        pass
    assert stream.hexdigest() == hashlib.sha256(CONTENT).hexdigest()


def test_different_files_have_different_hashes():
    first, second = HashingStream(io.BytesIO(CONTENT)), HashingStream(io.BytesIO(CONTENT + b"3,30\n"))
    for stream in (first, second):
        stream.read()
    assert first.hexdigest() != second.hexdigest()
