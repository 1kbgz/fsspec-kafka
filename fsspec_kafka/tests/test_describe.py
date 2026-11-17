import io
import json
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec_data import DataViewFileSystem, Facet

from fsspec_kafka.describe import KafkaDataSource

MESSAGES = pa.table({"offset": [0, 1], "value": [b"a", b"b"]})


def _arrow(table):
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


class _Rust:
    def metadata(self, path):
        if path == "orders":
            return json.dumps({"config": {"cleanup.policy": "delete"}, "partitions": [{"partition": 0, "low": 0, "high": 2}]})
        return json.dumps({"partition": 0, "low": 0, "high": 2})


class FakeKafka(KafkaDataSource):
    """Listings and Arrow files shaped like the Kafka filesystems."""

    protocol = "fake"
    root_marker = ""
    cachable = False

    entries: ClassVar[dict[str, dict]] = {
        "": {"name": "", "type": "directory", "size": 0},
        "orders": {"name": "orders", "type": "directory", "size": 0, "kind": "topic", "partitions": "1"},
        "orders/0": {"name": "orders/0", "type": "directory", "size": 0, "kind": "partition", "partition": "0"},
        "orders/0/0-.arrow": {"name": "orders/0/0-.arrow", "type": "file", "size": 0, "kind": "window", "format": "arrow", "start": "0"},
        "orders/weeks": {"name": "orders/weeks", "type": "directory", "size": 0, "kind": "weeks"},
        "orders/2026": {"name": "orders/2026", "type": "directory", "size": 0, "kind": "bucket", "start": "2026-01-01T00:00:00.000Z"},
        "state/a": {"name": "state/a", "type": "file", "size": 1, "kind": "record", "offset": "3"},
        "events/a": {"name": "events/a", "type": "file", "size": 0, "kind": "history", "format": "jsonl"},
        "events/b": {"name": "events/b", "type": "file", "size": 0, "kind": "history", "format": "arrow"},
    }
    children_of: ClassVar[dict[str, list[str]]] = {"": ["orders"], "orders": ["orders/0"], "orders/0": ["orders/0/0-.arrow"]}

    def __init__(self):
        super().__init__()
        self._rust = _Rust()
        self.opened = []

    def info(self, path, **kwargs):
        return dict(self.entries[self._strip_protocol(path)])

    def ls(self, path, detail=True, **kwargs):
        return [dict(self.entries[child]) for child in self.children_of[self._strip_protocol(path)]]

    def _open(self, path, mode="rb", block_size=None, autocommit=True, cache_options=None, **kwargs):
        self.opened.append(path)
        if path == "state/a":
            return io.BytesIO(b"a")
        return io.BytesIO(_arrow(MESSAGES))


@pytest.fixture
def fs():
    return FakeKafka()


def test_nodes_follow_the_kind_of_each_entry(fs):
    facets = {path: (node.facets, node.native_format) for path in fs.entries for node in [fs.describe(path)]}
    container = frozenset({Facet.CHILDREN, Facet.TABLE, Facet.METADATA})

    assert facets[""] == (frozenset({Facet.CHILDREN}), None)
    assert facets["orders"] == facets["orders/0"] == facets["orders/2026"] == (container, None)
    assert facets["orders/weeks"] == (frozenset({Facet.CHILDREN}), None)
    assert facets["orders/0/0-.arrow"][0] == {Facet.TABLE, Facet.RAW, Facet.METADATA}
    assert facets["orders/0/0-.arrow"][1].value == "arrow"
    assert facets["state/a"] == (frozenset({Facet.RAW, Facet.METADATA}), None)
    assert facets["events/a"] == (frozenset({Facet.RAW, Facet.METADATA}), None)
    assert facets["events/b"] == (frozenset({Facet.TABLE, Facet.METADATA}), None)
    assert [node.path for node in fs.children("orders")] == ["orders/0"]


def test_tables_read_the_arrow_encoding_of_each_node(fs):
    for path in ("orders", "orders/0", "orders/2026", "orders/0/0-.arrow", "events/b"):
        assert fs.schema(path) == MESSAGES.schema
        assert fs.scan(path).read_all() == MESSAGES
    assert fs.opened == [
        "orders.arrow",
        "orders.arrow",
        "orders/0.arrow",
        "orders/0.arrow",
        "orders/2026.arrow",
        "orders/2026.arrow",
        "orders/0/0-.arrow",
        "orders/0/0-.arrow",
        "events/b",
        "events/b",
    ]
    assert fs.open_raw("state/a").read() == b"a"


def test_metadata_adds_topic_and_partition_details_to_the_entry(fs):
    assert fs.metadata("orders") == {
        "kind": "topic",
        "partitions": [{"partition": 0, "low": 0, "high": 2}],
        "config": {"cleanup.policy": "delete"},
    }
    assert fs.metadata("orders/0") == {"kind": "partition", "partition": 0, "low": 0, "high": 2}
    assert fs.metadata("state/a") == {"kind": "record", "offset": "3"}


def test_data_views_render_kafka_nodes_as_files(fs):
    view = DataViewFileSystem("orders", fs=fs)

    assert view.ls("", detail=False) == [".metadata.json", ".parquet", ".schema.json", "0", "0.metadata.json", "0.parquet", "0.schema.json"]
    assert view.ls("0", detail=False) == ["0/0-.arrow", "0/0-.metadata.json", "0/0-.parquet", "0/0-.schema.json"]
    assert pq.read_table(view.open("0/0-.parquet")).column("value").to_pylist() == [b"a", b"b"]
    assert json.loads(view.cat_file(".metadata.json"))["config"] == {"cleanup.policy": "delete"}
