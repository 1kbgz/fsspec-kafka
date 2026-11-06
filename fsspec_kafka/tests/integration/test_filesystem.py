import json
import os
import threading
import uuid
from datetime import UTC, datetime, timedelta

import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.csv
import pyarrow.ipc
import pyarrow.parquet
import pytest

BOOTSTRAP = os.environ.get("KAFKA_TEST_BOOTSTRAP", "")

pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_TEST_BOOTSTRAP not set")


@pytest.fixture
def options():
    return {"bootstrap.servers": BOOTSTRAP}


@pytest.fixture
def topic(options):
    """A compacted single-partition topic holding records a, b and c."""
    name = f"fsspec-kafka-py-{uuid.uuid4().hex}"
    fs = fsspec.filesystem("kafka", **options, allow_admin=True, partitions=1, skip_instance_cache=True)
    fs.mkdir(f"{name}/records")
    for key in "abc":
        fs.pipe_file(f"kafka://{name}/records/{key}", key.encode())
    yield name
    fs.rmdir(name)


@pytest.fixture
def empty_topic(options):
    name = f"fsspec-kafka-py-{uuid.uuid4().hex}"
    fs = fsspec.filesystem("kafka", **options, allow_admin=True, partitions=2, skip_instance_cache=True)
    fs.mkdir(name)
    yield name
    fs.rmdir(name)


ROWS = pa.table({"user": ["a", "b", "c"], "id": [1, 2, 3], "name": ["x", None, "z\nz"]})


def _encode(table, format):
    sink = pa.BufferOutputStream()
    if format == "arrow":
        with pyarrow.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
    elif format == "parquet":
        pyarrow.parquet.write_table(table, sink)
    elif format == "csv":
        pyarrow.csv.write_csv(table, sink)
    else:
        return table.to_pandas().to_json(orient="records", lines=True).encode()
    return sink.getvalue().to_pybytes()


def test_lists_topics_partitions_and_windows(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    assert topic in fs.ls("", detail=False)
    assert fs.isdir(f"kafka://{topic}")
    assert fs.ls(f"kafka://{topic}/0", detail=False) == [f"{topic}/0/0-.arrow"]
    info = fs.info(f"{topic}/0")
    assert info["type"] == "directory"
    assert (info["low"], info["high"]) == ("0", "3")


def test_reads_windows_with_pyarrow(options, topic):
    with fsspec.open(f"kafka://{topic}/0/0-.arrow", **options) as file:
        table = pyarrow.ipc.open_stream(file).read_all()
    assert table.column("offset").to_pylist() == [0, 1, 2]
    assert table.column("key").to_pylist() == [b"a", b"b", b"c"]

    with fsspec.open(f"kafka://{topic}/0/0-.parquet", **options) as file:
        assert pyarrow.parquet.read_table(file).column("value").to_pylist() == [b"a", b"b", b"c"]


def test_reads_topic_snapshots_with_pandas(options, topic):
    frame = pd.read_parquet(f"kafka://{topic}.parquet", storage_options=options)
    assert frame["value"].tolist() == [b"a", b"b", b"c"]

    frame = pd.read_csv(f"kafka://{topic}.csv", storage_options=options)
    assert frame["offset"].tolist() == [0, 1, 2]


def test_reads_json_values(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    fs.pipe_file(f"{topic}/records/d", b'{"id": 1, "name": "d"}')
    fs.pipe_file(f"{topic}/records/e", b'{\n  "id": 2,\n  "name": "e"\n}\n')
    fs.rm_file(f"{topic}/records/d")
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    path = f"kafka://{topic}/0.parquet?start=3"

    frame = pd.read_parquet(path, storage_options={**options, "value_format": "json", "value_schema": schema, "flatten_value": True})
    assert frame["key"].tolist() == [b"d", b"e", b"d"]
    assert frame["id"].tolist()[:2] == [1, 2]
    assert frame["name"].tolist()[:2] == ["d", "e"]
    assert frame["name"].isna().tolist() == [False, False, True]

    with fsspec.open(path, **options, value_format="json", value_schema=schema) as file:
        values = pyarrow.parquet.read_table(file).column("value").to_pylist()
    assert values == [{"id": 1, "name": "d"}, {"id": 2, "name": "e"}, None]

    with fsspec.open(f"kafka://{topic}/0.arrow", **options, value_format="json", value_schema=schema) as file, pytest.raises(OSError, match="JSON"):
        file.read()


@pytest.mark.parametrize("format", ["arrow", "parquet", "csv", "jsonl"])
def test_appended_files_read_back(options, empty_topic, format):
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    fs = fsspec.filesystem(
        "kafka", **options, value_format="json", value_schema=schema, flatten_value=True, key_column="user", skip_instance_cache=True
    )
    with fs.open(f"{empty_topic}/1.{format}", "ab") as file:
        file.write(_encode(ROWS, format))
    with fs.open(f"{empty_topic}.arrow") as file:
        table = pyarrow.ipc.open_stream(file).read_all()
    assert table.column("partition").to_pylist() == [1, 1, 1]
    assert table.column("key").to_pylist() == [b"a", b"b", b"c"]
    assert table.select(["id", "name"]) == ROWS.select(["id", "name"])


def test_appends_binary_values(options, empty_topic):
    fs = fsspec.filesystem("kafka", **options, key_column="key", partition_column="partition", skip_instance_cache=True)
    rows = pa.table({"partition": [0, 1], "key": [b"k0", b"k1"], "value": [b"v0", None]})
    with fs.open(f"kafka://{empty_topic}.parquet", "ab") as file:
        file.write(_encode(rows, "parquet"))
    table = pd.read_parquet(f"kafka://{empty_topic}.parquet", storage_options=options)
    assert table[["partition", "key", "value"]].to_dict("list") == {"partition": [0, 1], "key": [b"k0", b"k1"], "value": [b"v0", None]}

    with pytest.raises(NotImplementedError, match="append-only"):
        fs.open(f"{empty_topic}.parquet", "wb")
    with pytest.raises(ValueError, match="exactly one value column"), fs.open(f"{empty_topic}.arrow", "ab") as file:
        file.write(_encode(rows.append_column("other", pa.array([1, 2])), "arrow"))
        file.close()


def test_records_through_open_and_get_mapper(options, topic):
    with fsspec.open(f"kafka://{topic}/records/d", "wb", **options) as file:
        file.write(b"d")
    with fsspec.open(f"kafka://{topic}/records/d", "rb", **options) as file:
        assert file.read() == b"d"

    mapper = fsspec.get_mapper(f"kafka://{topic}/records", **options)
    mapper["ns/e"] = b"e"
    del mapper["a"]
    assert dict(mapper) == {"b": b"b", "c": b"c", "d": b"d", "ns/e": b"e"}


def test_failed_writes_are_discarded(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    with pytest.raises(RuntimeError), fs.open(f"{topic}/records/a", "wb") as file:
        file.write(b"partial")
        raise RuntimeError
    assert fs.cat_file(f"{topic}/records/a") == b"a"


def test_topic_admin_needs_allow_admin(options):
    fs = fsspec.filesystem("kafka", **options)
    with pytest.raises(PermissionError):
        fs.mkdir(f"fsspec-kafka-py-{uuid.uuid4().hex}")


def test_missing_paths_raise_file_not_found(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    with pytest.raises(FileNotFoundError):
        fs.cat_file(f"{topic}/records/missing")
    with pytest.raises(FileNotFoundError):
        fs.info(f"{topic}-missing")
    assert not fs.exists(f"{topic}/records/missing")


def test_files_support_raw_io(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    with fs.open(f"{topic}/records/f", "wb") as file:
        assert file.writable() and not file.seekable()
        file.write(memoryview(b"value"))
    file.discard()  # already closed, so the value stays committed
    with fs.open(f"{topic}/records/f") as file:
        buffer = bytearray(2)
        assert file.readinto(buffer) == 2 and buffer == b"va"
        assert file.tell() == 2
        assert file.readall() == b"lue"
    with pytest.raises(FileExistsError):
        fs.makedirs(f"{topic}/records")


def test_streams_follow_new_messages_and_resume(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    with fs.stream(f"kafka://{topic}") as stream:
        assert stream.positions == {0: 3}
        assert stream.poll(timeout=0.5) is None
        # Produce while iteration is already waiting.
        threading.Timer(0.5, fs.pipe_file, (f"{topic}/records/d", b"d")).start()
        batch = next(iter(stream))
        assert batch.column("value").to_pylist() == [b"d"]
        assert batch.schema.names == ["topic", "partition", "offset", "timestamp", "key", "headers", "value"]
        saved = json.loads(json.dumps(stream.positions))
    assert stream.closed

    fs.pipe_file(f"{topic}/records/e", b"e")
    with fs.stream(f"{topic}/0", start=saved) as stream:
        assert stream.poll(timeout=10).column("value").to_pylist() == [b"e"]

    with fs.stream(topic, start="beginning") as stream:
        values = []
        while len(values) < 5:
            values += stream.poll(timeout=10).column("value").to_pylist()
    assert values == [b"a", b"b", b"c", b"d", b"e"]

    with fs.stream(topic, start=datetime.now(UTC) + timedelta(hours=1)) as stream:
        assert stream.positions == {0: 5}
