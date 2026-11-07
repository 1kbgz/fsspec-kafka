import pickle

import fsspec
import pyarrow as pa
import pytest

from fsspec_kafka import KafkaFileSystem

# Nothing listens here; librdkafka connects only when a request is made.
OPTIONS = {"bootstrap.servers": "127.0.0.1:1", "skip_instance_cache": True}


def test_kafka_protocol_is_registered():
    assert fsspec.get_filesystem_class("kafka") is KafkaFileSystem


def test_storage_options_are_librdkafka_properties():
    fs = fsspec.filesystem("kafka", **OPTIONS, **{"enable.ssl.certificate.verification": False, "message.max.bytes": 2_000_000})
    assert isinstance(fs, KafkaFileSystem)


def test_unknown_librdkafka_properties_are_rejected():
    with pytest.raises(OSError, match="no.such.property"):
        KafkaFileSystem(**OPTIONS, **{"no.such.property": "1"})


def test_invalid_options_are_rejected():
    with pytest.raises(ValueError):
        KafkaFileSystem(**OPTIONS, list_format="xlsx")
    with pytest.raises(ValueError):
        KafkaFileSystem(**OPTIONS, window_size=0)


@pytest.mark.parametrize(
    "options",
    [
        {"value_format": "json"},
        {"value_format": "confluent"},
        {"value_format": "avro", "value_schema": pa.schema([("id", pa.int64())])},
        {"value_schema": pa.schema([("id", pa.int64())])},
        {"flatten_value": True},
        {"value_format": "json", "value_schema": pa.schema([("offset", pa.int64())]), "flatten_value": True},
    ],
)
def test_invalid_value_options_are_rejected(options):
    with pytest.raises(ValueError):
        KafkaFileSystem(**OPTIONS, **options)


def test_value_schemas_can_be_fsspec_data_schemas(tmp_path):
    fields = [{"name": "id", "type": "int64"}, {"name": "name", "type": "string"}]
    KafkaFileSystem(**OPTIONS, value_format="json", value_schema=fields)

    path = tmp_path / "value.json"
    path.write_text('{"type": "object", "properties": {"id": {"type": "integer"}}}')
    KafkaFileSystem(**OPTIONS, value_format="confluent", value_schema={"url": str(path), "format": "json-schema"})

    with pytest.raises(FileNotFoundError):
        KafkaFileSystem(**OPTIONS, value_format="json", value_schema={"url": str(tmp_path / "missing.json"), "format": "json-schema"})


def test_filesystem_pickles_from_its_options():
    fs = KafkaFileSystem(**OPTIONS, window_size=100, value_format="json", value_schema=pa.schema([("id", pa.int64())]))
    assert pickle.loads(pickle.dumps(fs)).storage_options == fs.storage_options


@pytest.mark.parametrize("start", ["latest", 5, None])
def test_invalid_stream_starts_are_rejected(start):
    fs = KafkaFileSystem(**OPTIONS)
    with pytest.raises((TypeError, ValueError)):
        fs.stream("orders", start=start)
