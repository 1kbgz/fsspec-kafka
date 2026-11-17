import json
import os
import threading
import urllib.request
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
SCHEMA_REGISTRY = os.environ.get("KAFKA_TEST_SCHEMA_REGISTRY", "")

pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_TEST_BOOTSTRAP not set")


@pytest.fixture
def options():
    return {"bootstrap.servers": BOOTSTRAP}


@pytest.fixture
def topic(options):
    """A compacted single-partition topic holding records a, b and c."""
    name = f"fsspec-kafka-py-{uuid.uuid4().hex}"
    fs = fsspec.filesystem("kafka+records", **options, allow_admin=True, partitions=1, skip_instance_cache=True)
    fs.mkdir(name)
    for key in "abc":
        fs.pipe_file(f"kafka+records://{name}/{key}", key.encode())
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
    assert (info["partition"], info["low"], info["high"]) == ("0", "0", "3")
    assert fs.ls(topic)[0]["partition"] == "0"


def test_metadata_describes_topics_and_partitions(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    metadata = fs.metadata(f"kafka://{topic}")
    assert metadata["config"]["cleanup.policy"] == "compact"
    assert (metadata["partitions"][0]["low"], metadata["partitions"][0]["high"]) == (0, 3)
    assert fs.metadata(f"{topic}/0") == metadata["partitions"][0]


def test_offsets_for_time(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    assert fs.offsets_for_time(topic, datetime(2000, 1, 1, tzinfo=UTC)) == {0: 0}
    assert fs.offsets_for_time(f"{topic}/0", (datetime.now(UTC) + timedelta(hours=1)).isoformat()) == {0: 3}


def test_time_buckets_hold_messages_by_timestamp(options, topic):
    fs = fsspec.filesystem("kafka+time", **options, time_bucket="day")
    now = datetime.now(UTC)
    year = f"{topic}/{now:%Y}"
    assert fs.ls(f"kafka+time://{topic}", detail=False) == [f"{topic}/weeks", year]
    assert fs.isdir(f"{year}/{now:%m}")
    with fs.open(f"{year}.arrow") as file:
        assert pyarrow.ipc.open_stream(file).read_all().column("value").to_pylist() == [b"a", b"b", b"c"]
    week = fs.ls(f"{topic}/weeks", detail=False)
    assert week == [f"{topic}/weeks/{now:%G-W%V}.arrow"]
    assert pd.read_parquet(f"kafka+time://{topic}/weeks/{now:%G-W%V}.parquet", storage_options=options)["key"].tolist() == [b"a", b"b", b"c"]


def test_history_keeps_every_message_of_a_key(options, topic):
    fsspec.filesystem("kafka+records", **options).rm_file(f"{topic}/a")
    fs = fsspec.filesystem("kafka+history", **options)
    assert fs.ls(topic, detail=False) == [f"{topic}/a", f"{topic}/b", f"{topic}/c"]
    with fs.open(f"kafka+history://{topic}/a") as file:
        table = pyarrow.ipc.open_stream(file).read_all()
    assert table.column("value").to_pylist() == [b"a", None]


def test_records_topics_read_as_their_latest_values(options, topic):
    fs = fsspec.filesystem("kafka+records", **options)
    fs.pipe_file(f"{topic}/a", b"a2")
    fs.rm_file(f"{topic}/b")
    frame = pd.read_parquet(f"kafka+records://{topic}.parquet", storage_options=options)
    assert dict(zip(frame["key"], frame["value"], strict=True)) == {b"c": b"c", b"a": b"a2"}


def test_reads_windows_with_pyarrow(options, topic):
    with fsspec.open(f"kafka://{topic}/0/0-.arrow", **options) as file:
        table = pyarrow.ipc.open_stream(file).read_all()
    assert table.column("offset").to_pylist() == [0, 1, 2]
    assert table.column("key").to_pylist() == [b"a", b"b", b"c"]
    data = fsspec.filesystem("kafka", **options).cat_file(f"{topic}/0/0-.arrow")
    assert pyarrow.ipc.open_stream(data).read_all() == table

    with fsspec.open(f"kafka://{topic}/0/0-.parquet", **options) as file:
        assert pyarrow.parquet.read_table(file).column("value").to_pylist() == [b"a", b"b", b"c"]


def test_reads_topic_snapshots_with_pandas(options, topic):
    frame = pd.read_parquet(f"kafka://{topic}.parquet", storage_options=options)
    assert frame["value"].tolist() == [b"a", b"b", b"c"]

    frame = pd.read_csv(f"kafka://{topic}.csv", storage_options=options)
    assert frame["offset"].tolist() == [0, 1, 2]


def test_reads_json_values(options, topic):
    fs = fsspec.filesystem("kafka+records", **options)
    fs.pipe_file(f"{topic}/d", b'{"id": 1, "name": "d"}')
    fs.pipe_file(f"{topic}/e", b'{\n  "id": 2,\n  "name": "e"\n}\n')
    fs.rm_file(f"{topic}/d")
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


@pytest.mark.skipif(not SCHEMA_REGISTRY, reason="KAFKA_TEST_SCHEMA_REGISTRY not set")
def test_confluent_values_with_registry_schemas(options, empty_topic):
    from fsspec_data import SchemaRef

    schema = {"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}}
    request = urllib.request.Request(
        f"{SCHEMA_REGISTRY}/subjects/{empty_topic}-value/versions",
        data=json.dumps({"schemaType": "JSON", "schema": json.dumps(schema)}).encode(),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(request) as response:
        schema_id = json.load(response)["id"]
    reference = SchemaRef(f"confluent://{empty_topic}-value/versions/latest", storage_options={"registry_url": SCHEMA_REGISTRY})
    fs = fsspec.filesystem(
        "kafka", **options, value_format="confluent", value_schema=reference, flatten_value=True, key_column="user", skip_instance_cache=True
    )

    with fs.open(f"{empty_topic}/0.parquet", "ab") as file:
        file.write(_encode(ROWS, "parquet"))
    with fs.open(f"{empty_topic}/0.arrow") as file:
        table = pyarrow.ipc.open_stream(file).read_all()
    assert table.select(["id", "name"]) == ROWS.select(["id", "name"])

    with fsspec.open(f"kafka://{empty_topic}/0.arrow", **options) as file:
        value = pyarrow.ipc.open_stream(file).read_all().column("value")[0].as_py()
    assert value[:5] == b"\0" + schema_id.to_bytes(4, "big")


ORDER_V1 = {"type": "record", "name": "Order", "fields": [{"name": "id", "type": "long"}]}
ORDER_V2 = {"type": "record", "name": "Order", "fields": [{"name": "id", "type": "long"}, {"name": "note", "type": "string", "default": "none"}]}


def test_avro_values_round_trip(options, empty_topic):
    fs = fsspec.filesystem("kafka", **options, value_format="avro", value_schema=ORDER_V2, flatten_value=True, skip_instance_cache=True)
    rows = pa.table({"id": [1, 2], "note": ["a", "b"]})

    with fs.open(f"{empty_topic}/0.parquet", "ab") as file:
        file.write(_encode(rows, "parquet"))
    with fs.open(f"{empty_topic}/0.arrow") as file:
        table = pyarrow.ipc.open_stream(file).read_all()

    assert table.select(["id", "note"]).to_pylist() == rows.to_pylist()


@pytest.mark.skipif(not SCHEMA_REGISTRY, reason="KAFKA_TEST_SCHEMA_REGISTRY not set")
def test_confluent_avro_values_resolve_older_versions(options, empty_topic):
    from fsspec_data import SchemaRef

    def register(schema):
        request = urllib.request.Request(
            f"{SCHEMA_REGISTRY}/subjects/{empty_topic}-value/versions",
            data=json.dumps({"schema": json.dumps(schema)}).encode(),
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        with urllib.request.urlopen(request) as response:
            return json.load(response)["id"]

    def filesystem(version):
        reference = SchemaRef(f"confluent://{empty_topic}-value/versions/{version}", storage_options={"registry_url": SCHEMA_REGISTRY})
        return fsspec.filesystem("kafka", **options, value_format="confluent", value_schema=reference, flatten_value=True, skip_instance_cache=True)

    old_id = register(ORDER_V1)
    with filesystem(1).open(f"{empty_topic}/0.jsonl", "ab") as file:
        file.write(b'{"id": 1}\n')
    new_id = register(ORDER_V2)
    with filesystem("latest").open(f"{empty_topic}/0.jsonl", "ab") as file:
        file.write(b'{"id": 2, "note": "b"}\n')

    with filesystem("latest").open(f"{empty_topic}/0.arrow") as file:
        table = pyarrow.ipc.open_stream(file).read_all()
    assert table.select(["id", "note"]).to_pylist() == [{"id": 1, "note": "none"}, {"id": 2, "note": "b"}]

    with fsspec.open(f"kafka://{empty_topic}/0.arrow", **options) as file:
        values = pyarrow.ipc.open_stream(file).read_all().column("value").to_pylist()
    assert [value[:5] for value in values] == [b"\0" + schema_id.to_bytes(4, "big") for schema_id in (old_id, new_id)]


def test_records_through_open_and_get_mapper(options, topic):
    with fsspec.open(f"kafka+records://{topic}/d", "wb", **options) as file:
        file.write(b"d")
    with fsspec.open(f"kafka+records://{topic}/d", "rb", **options) as file:
        assert file.read() == b"d"

    mapper = fsspec.get_mapper(f"kafka+records://{topic}", **options)
    mapper["ns/e"] = b"e"
    del mapper["a"]
    assert dict(mapper) == {"b": b"b", "c": b"c", "d": b"d", "ns/e": b"e"}


def test_failed_writes_are_discarded(options, topic):
    fs = fsspec.filesystem("kafka+records", **options)
    with pytest.raises(RuntimeError), fs.open(f"{topic}/a", "wb") as file:
        file.write(b"partial")
        raise RuntimeError
    assert fs.cat_file(f"{topic}/a") == b"a"


def test_topic_admin_needs_allow_admin(options):
    fs = fsspec.filesystem("kafka", **options)
    with pytest.raises(PermissionError):
        fs.mkdir(f"fsspec-kafka-py-{uuid.uuid4().hex}")


def test_missing_paths_raise_file_not_found(options, topic):
    fs = fsspec.filesystem("kafka+records", **options)
    with pytest.raises(FileNotFoundError):
        fs.cat_file(f"{topic}/missing")
    with pytest.raises(FileNotFoundError):
        fsspec.filesystem("kafka", **options).info(f"{topic}-missing")
    assert not fs.exists(f"{topic}/missing")


def test_files_support_raw_io(options, topic):
    fs = fsspec.filesystem("kafka+records", **options)
    with fs.open(f"{topic}/f", "wb") as file:
        assert file.writable() and not file.seekable()
        file.write(memoryview(b"value"))
    file.discard()  # already closed, so the value stays committed
    with fs.open(f"{topic}/f") as file:
        buffer = bytearray(2)
        assert file.readinto(buffer) == 2 and buffer == b"va"
        assert file.tell() == 2
        assert file.readall() == b"lue"
    with pytest.raises(FileExistsError):
        fs.makedirs(topic)


def test_streams_follow_new_messages_and_resume(options, topic):
    fs = fsspec.filesystem("kafka", **options)
    records = fsspec.filesystem("kafka+records", **options)
    with fs.stream(f"kafka://{topic}") as stream:
        assert stream.positions == {0: 3}
        assert stream.poll(timeout=0.5) is None
        # Produce while iteration is already waiting.
        threading.Timer(0.5, records.pipe_file, (f"{topic}/d", b"d")).start()
        batch = next(iter(stream))
        assert batch.column("value").to_pylist() == [b"d"]
        assert batch.schema.names == ["topic", "partition", "offset", "timestamp", "key", "headers", "value"]
        saved = json.loads(json.dumps(stream.positions))
    assert stream.closed

    records.pipe_file(f"{topic}/e", b"e")
    with fs.stream(f"{topic}/0", start=saved) as stream:
        assert stream.poll(timeout=10).column("value").to_pylist() == [b"e"]

    with fs.stream(topic, start="beginning") as stream:
        values = []
        while len(values) < 5:
            values += stream.poll(timeout=10).column("value").to_pylist()
    assert values == [b"a", b"b", b"c", b"d", b"e"]

    with fs.stream(topic, start=datetime.now(UTC) + timedelta(hours=1)) as stream:
        assert stream.positions == {0: 5}
