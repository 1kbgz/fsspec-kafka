import os
import uuid

import fsspec
import pytest

BOOTSTRAP = os.environ.get("KAFKA_TEST_BOOTSTRAP", "")

if not BOOTSTRAP:
    pytest.skip("KAFKA_TEST_BOOTSTRAP not set", allow_module_level=True)

from fsspec_rs.testing.records import *


@pytest.fixture
def records_storage_options():
    return {"bootstrap.servers": BOOTSTRAP}


@pytest.fixture
def records_url(records_storage_options):
    topic = f"fsspec-kafka-records-{uuid.uuid4().hex}"
    fs = fsspec.filesystem("kafka", **records_storage_options, allow_admin=True, partitions=3, skip_instance_cache=True)
    fs.mkdir(f"{topic}/records")
    yield f"kafka://{topic}/records"
    fs.rmdir(topic)
