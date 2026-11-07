import io
import json
from collections.abc import Mapping
from datetime import datetime

import fsspec
from fsspec import AbstractFileSystem

from .fsspec_kafka import RustKafkaFs

__all__ = ("KafkaFile", "KafkaFileSystem", "KafkaStream")

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

    With ``value_format="json"``, values are JSON objects decoded with ``value_schema``.
    Decoded fields are nested in a ``value`` struct column, or placed beside the message
    columns with ``flatten_value=True``. ``value_format="confluent"`` reads JSON in Confluent
    wire format, with JSON Schema or Avro subjects. ``value_format="avro"`` reads bare Avro
    records. ``value_schema`` is a ``pyarrow.Schema`` or a schema reference that fsspec-data
    resolves, such as ``"confluent://orders-value/versions/latest"`` with
    ``storage_options={"registry_url": ...}`` in a ``fsspec_data.SchemaRef``. For Avro it is
    an ``.avsc`` reference, an Avro schema dict, or a Confluent Avro subject, whose registered
    versions are used to read older messages. A Confluent reference also gives the schema ID
    written when appending.

    Opening ``topic.format`` or ``topic/partition.format`` with mode ``"ab"`` appends a file
    as one message per row. ``key_column``, ``partition_column``, ``timestamp_column``, and
    ``headers_column`` name the input columns that set those message fields. The remaining
    columns are the value: one JSON object per row with ``value_format="json"``, otherwise a
    single binary or string column.
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
        value_format="binary",
        value_schema=None,
        flatten_value=False,
        key_column=None,
        partition_column=None,
        timestamp_column=None,
        headers_column=None,
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
            value_format=value_format,
            **_value_schema(value_format, value_schema),
            flatten_value=flatten_value,
            key_column=key_column,
            partition_column=partition_column,
            timestamp_column=timestamp_column,
            headers_column=headers_column,
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

    def stream(self, path, start="end"):
        """Follow a topic or one partition, including messages produced later.

        ``start`` is ``"end"`` for new messages only, ``"beginning"`` for every retained
        message, a ``datetime`` or ISO 8601 string for the first message at or after that
        time, or a mapping from partition to the next offset to read, such as
        ``KafkaStream.positions`` saved from an earlier stream. Partitions left out of the
        mapping start at the beginning. Partitions added to the topic later are not followed.
        """
        return KafkaStream(self._rust.stream(self._strip_protocol(path), _stream_start(start)))


def _value_schema(value_format, value_schema):
    """Keyword arguments for RustKafkaFs describing ``value_schema``: an Arrow schema for JSON
    values, or Avro schema JSON for Avro values, and the Confluent schema ID if it has one."""
    if value_schema is None:
        return {}
    import pyarrow as pa

    if isinstance(value_schema, pa.Schema):
        return {"value_schema": _schema_bytes(value_schema)}
    if value_format == "avro" and isinstance(value_schema, Mapping) and "url" not in value_schema:
        return {"avro_schema": json.dumps(value_schema)}
    from fsspec_data import DEFAULT_SCHEMA_RESOLVERS, SchemaRef, json_schema_to_arrow
    from fsspec_data.confluent import fetch_confluent_schema, fetch_confluent_versions
    from fsspec_data.schema import normalize_schema

    schema = normalize_schema(value_schema)
    if not isinstance(schema, SchemaRef):
        return {"value_schema": _schema_bytes(schema)}
    if schema.url.startswith("confluent://"):
        document = fetch_confluent_schema(schema.url, schema.storage_options)
        if document.references:
            raise ValueError("Confluent schema references are not supported")
        schema_id = None if document.schema_id is None else int(document.schema_id)
        if document.schema_type != "AVRO":
            return {"value_schema": _schema_bytes(json_schema_to_arrow(json.loads(document.schema))), "value_schema_id": schema_id}
        # Messages may have been written with any registered version of the subject.
        versions = fetch_confluent_versions(schema.url, schema.storage_options)
        writers = [(int(version.schema_id), version.schema) for version in versions if version.schema_id is not None]
        return {"avro_schema": document.schema, "avro_writers": writers, "value_schema_id": schema_id}
    if value_format == "avro":
        with fsspec.open(schema.url, "rb", **schema.storage_options) as file:
            return {"avro_schema": file.read().decode()}
    return {"value_schema": _schema_bytes(DEFAULT_SCHEMA_RESOLVERS.resolve(schema).schema)}


def _schema_bytes(schema):
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    pa.ipc.new_stream(sink, schema).close()
    return sink.getvalue().to_pybytes()


def _stream_start(start):
    if isinstance(start, Mapping):
        return {int(partition): int(offset) for partition, offset in start.items()}
    if isinstance(start, str) and start not in ("beginning", "end"):
        start = datetime.fromisoformat(start)
    if isinstance(start, datetime):
        return int(start.timestamp() * 1000)
    if start not in ("beginning", "end"):
        raise TypeError(f"start must be 'beginning', 'end', a datetime, or a mapping of partition offsets, not {start!r}")
    return start


class KafkaStream:
    """Messages from followed partitions as ``pyarrow.RecordBatch`` objects, with the same
    columns as ``.arrow`` files. Iterating waits for messages and never ends; ``poll``
    returns ``None`` when nothing arrives in time."""

    def __init__(self, stream):
        import pyarrow.ipc

        self._ipc = pyarrow.ipc
        self._stream = stream

    @property
    def positions(self):
        """The next offset to read in each partition. Pass these as ``start`` to resume."""
        return self._stream.positions

    @property
    def closed(self):
        return self._stream.closed

    def poll(self, timeout=None):
        """Wait up to ``timeout`` seconds, or indefinitely when ``None``, for messages and
        return them as one record batch, or ``None`` when none arrived."""
        while True:
            # Short waits return to Python, so KeyboardInterrupt is handled promptly.
            data = self._stream.poll(0.2 if timeout is None else timeout)
            if data is not None:
                return self._ipc.open_stream(data).read_next_batch()
            if timeout is not None:
                return None

    def close(self):
        self._stream.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self.poll()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


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
