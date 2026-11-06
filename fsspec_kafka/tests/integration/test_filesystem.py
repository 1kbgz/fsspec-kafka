import os
import uuid

import fsspec
import pandas as pd
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
