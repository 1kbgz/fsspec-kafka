"""Describes Kafka filesystems to fsspec-data, so a data view can render topics, partitions,
time buckets, and keys as files."""

from __future__ import annotations

import json
from typing import Any, BinaryIO

import pyarrow as pa
from fsspec import AbstractFileSystem
from fsspec_data import DataSource, Facet, Node
from pyarrow import ipc

_INFO_FIELDS = {"name", "type", "size", "size_known"}
# Directories whose messages also read as one table.
_TABLE_DIRS = {"topic", "partition", "bucket"}
# Files of messages in an encoding named by their extension.
_MESSAGE_FILES = {"window", "bucket"}


class KafkaDataSource(DataSource, AbstractFileSystem):
    """``DataSource`` for the Kafka filesystems, built from the ``kind`` each ``info`` entry
    reports. Topics, partitions, and time buckets are containers whose messages also read as
    a table. Windows and leaf time buckets are tables whose raw bytes are their listed
    encoding. Records are raw values. ``kafka+history`` keys are tables of their messages
    when ``list_format`` is ``arrow``, and otherwise raw bytes in that format. Topics and
    partitions add their configuration, replicas, and watermarks to their metadata."""

    _rust: Any

    def describe(self, path: str) -> Node:
        return _node(self.info(path))

    def children(self, path: str) -> list[Node]:
        return [_node(entry) for entry in self.ls(path, detail=True)]

    def schema(self, path: str) -> pa.Schema:
        with self._open_table(path) as file:
            return ipc.open_stream(file).schema

    def scan(
        self,
        path: str,
        *,
        batch_size: int = 1024,
        row_limit: int | None = None,
        byte_limit: int | None = None,
    ) -> pa.RecordBatchReader:
        del batch_size, row_limit, byte_limit
        return ipc.open_stream(self._open_table(path))

    def open_raw(self, path: str) -> BinaryIO:
        return self.open(path, "rb")

    def metadata(self, path: str) -> dict[str, Any]:
        """The ``info`` fields of ``path``. A topic adds its configuration and each partition's
        leader, replicas, in-sync replicas, and low and high watermarks; a partition adds its
        own."""
        info = self.info(path)
        metadata = {key: value for key, value in info.items() if key not in _INFO_FIELDS}
        if info.get("kind") in ("topic", "partition"):
            metadata.update(json.loads(self._rust.metadata(self._strip_protocol(path))))
        return metadata

    def _open_table(self, path: str) -> BinaryIO:
        """Opens the Arrow encoding of the messages behind ``path``."""
        info = self.info(path)
        name = self._strip_protocol(info["name"])
        if info.get("kind") == "history":
            return self.open(name, "rb")
        if info["type"] == "directory":
            return self.open(f"{name}.arrow", "rb")
        return self.open(f"{name.rpartition('.')[0]}.arrow", "rb")


def _node(info: dict[str, Any]) -> Node:
    path = info["name"]
    kind = info.get("kind")
    if info["type"] == "directory":
        if kind in _TABLE_DIRS:
            return Node(path, [Facet.CHILDREN, Facet.TABLE, Facet.METADATA])
        return Node(path, [Facet.CHILDREN])
    if kind in _MESSAGE_FILES:
        return Node(path, [Facet.TABLE, Facet.RAW, Facet.METADATA], info["format"])
    if kind == "history" and info["format"] == "arrow":
        return Node(path, [Facet.TABLE, Facet.METADATA])
    return Node(path, [Facet.RAW, Facet.METADATA])
