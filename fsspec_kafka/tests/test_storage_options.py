import pickle

import fsspec
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


def test_filesystem_pickles_from_its_options():
    fs = KafkaFileSystem(**OPTIONS, window_size=100)
    assert pickle.loads(pickle.dumps(fs)).storage_options == fs.storage_options


@pytest.mark.parametrize("start", ["latest", 5, None])
def test_invalid_stream_starts_are_rejected(start):
    fs = KafkaFileSystem(**OPTIONS)
    with pytest.raises((TypeError, ValueError)):
        fs.stream("orders", start=start)
