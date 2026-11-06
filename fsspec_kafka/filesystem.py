import io

from fsspec import AbstractFileSystem

from .fsspec_kafka import RustKafkaFs

__all__ = ("KafkaFile", "KafkaFileSystem")

# Storage options handled by AbstractFileSystem rather than librdkafka.
_FSSPEC_OPTIONS = frozenset({"use_listings_cache", "listings_expiry_time", "max_paths", "asynchronous", "loop"})


def _property(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class KafkaFileSystem(AbstractFileSystem):
    """Kafka topics and partitions as an fsspec filesystem.

    Storage options other than the named arguments are librdkafka properties, for example
    ``fsspec.filesystem("kafka", **{"bootstrap.servers": "localhost:9092"})``. They can also
    come from fsspec configuration, such as ``FSSPEC_KAFKA='{"bootstrap.servers": "..."}'``.
    """

    protocol = ("kafka",)
    root_marker = ""

    def __init__(
        self,
        timeout=10.0,
        window_size=10_000,
        list_format="arrow",
        batch_size=1024,
        allow_admin=False,
        partitions=-1,
        replication=-1,
        topic_config=None,
        **storage_options,
    ):
        super().__init__(**{key: storage_options.pop(key) for key in _FSSPEC_OPTIONS & storage_options.keys()})
        self._rust = RustKafkaFs(
            {key: _property(value) for key, value in storage_options.items()},
            timeout=timeout,
            window_size=window_size,
            list_format=list_format,
            batch_size=batch_size,
            allow_admin=allow_admin,
            partitions=partitions,
            replication=replication,
            topic_config={key: _property(value) for key, value in (topic_config or {}).items()},
        )

    def ls(self, path, detail=True, **kwargs):
        entries = self._rust.ls(self._strip_protocol(path))
        return entries if detail else [entry["name"] for entry in entries]

    def info(self, path, **kwargs):
        return self._rust.info(self._strip_protocol(path))

    def _open(self, path, mode="rb", block_size=None, autocommit=True, cache_options=None, **kwargs):
        return KafkaFile(self, path, mode, self._rust.open(self._strip_protocol(path), mode))

    def cat_file(self, path, start=None, end=None, **kwargs):
        return bytes(self._rust.cat_file(self._strip_protocol(path), start, end))

    def pipe_file(self, path, value, mode="overwrite", **kwargs):
        self._rust.pipe_file(self._strip_protocol(path), bytes(value))

    def rm_file(self, path):
        self._rust.rm_file(self._strip_protocol(path))

    def mkdir(self, path, create_parents=True, **kwargs):
        self._rust.mkdir(self._strip_protocol(path))

    def makedirs(self, path, exist_ok=False):
        # Directories inside topic/records exist only while they hold keys, so checking
        # first lets mappers write without allow_admin.
        if self.isdir(path):
            if not exist_ok:
                raise FileExistsError(path)
            return
        self.mkdir(path)

    def rmdir(self, path):
        self._rust.rmdir(self._strip_protocol(path))


class KafkaFile(io.RawIOBase):
    """An open Kafka file. Closing a writer produces its value; leaving a ``with`` block
    because of an exception discards it."""

    def __init__(self, fs, path, mode, file):
        super().__init__()
        self.fs = fs
        self.path = path
        self.mode = mode
        self._file = file

    def readable(self):
        return "r" in self.mode

    def writable(self):
        return "r" not in self.mode

    def seekable(self):
        return self.readable()

    def read(self, size=-1):
        return bytes(self._file.read(-1 if size is None else size))

    def readall(self):
        return self.read()

    def readinto(self, buffer):
        data = self._file.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def write(self, data):
        return self._file.write(bytes(data))

    def seek(self, offset, whence=io.SEEK_SET):
        return self._file.seek(offset, whence)

    def tell(self):
        return self._file.tell()

    def flush(self):
        if not self._file.closed:
            self._file.flush()

    def close(self):
        if not self.closed:
            try:
                self._file.close()
            finally:
                super().close()

    def discard(self):
        """Drops pending writes and closes the file."""
        if not self.closed:
            try:
                self._file.discard()
            finally:
                super().close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.discard()
        else:
            self.close()
