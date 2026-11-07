# fsspec-kafka

Kafka topics and partitions through fsspec

[![Build Status](https://github.com/1kbgz/fsspec-kafka/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/fsspec-kafka/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/fsspec-kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/fsspec-kafka)
[![License](https://img.shields.io/github/license/1kbgz/fsspec-kafka)](https://github.com/1kbgz/fsspec-kafka)
[![PyPI](https://img.shields.io/pypi/v/fsspec-kafka.svg)](https://pypi.python.org/pypi/fsspec-kafka)

## Overview

`fsspec-kafka` registers the `kafka://` protocol with fsspec. Topics are directories, and each partition is a directory of fixed offset windows that read as Arrow, Parquet, CSV, or JSON Lines.

```python
import fsspec
import pandas as pd

options = {"bootstrap.servers": "localhost:9092"}
fs = fsspec.filesystem("kafka", **options)
fs.ls("orders/0", detail=False)  # ["orders/0/0-10000.arrow", ..., "orders/0/20000-.arrow"]

frame = pd.read_parquet("kafka://orders.parquet", storage_options=options)
```

Storage options are librdkafka properties, except for these:

| Option                      | Default    | Meaning                                                                 |
| --------------------------- | ---------- | ----------------------------------------------------------------------- |
| `timeout`                   | `10.0`     | Seconds to wait for broker requests                                     |
| `window_size`               | `10000`    | Offsets per window file                                                 |
| `list_format`               | `"arrow"`  | Extension used when listing windows                                     |
| `batch_size`                | `1024`     | Messages per Arrow record batch                                         |
| `allow_admin`               | `False`    | Lets `mkdir` create topics and `rmdir` delete them                      |
| `partitions`, `replication` | `-1`       | Settings for created topics; `-1` uses the broker default               |
| `topic_config`              | `{}`       | Configuration for created topics                                        |
| `value_format`              | `"binary"` | `"json"`, `"avro"`, or `"confluent"` decodes values with `value_schema` |
| `value_schema`              | `None`     | `pyarrow.Schema` or fsspec-data schema reference                        |
| `flatten_value`             | `False`    | Puts JSON fields beside the message columns                             |
| `key_column`                | `None`     | Input column for message keys when appending                            |
| `partition_column`          | `None`     | Input column for partitions when appending                              |
| `timestamp_column`          | `None`     | Input column for timestamps when appending                              |
| `headers_column`            | `None`     | Input column for headers, a map, when appending                         |

Messages are read with `topic`, `partition`, `offset`, `timestamp`, `key`, `headers`, and `value` columns. With `value_format="json"`, `value` is a struct of the `value_schema` fields, which are null for tombstones. `flatten_value=True` puts the fields beside the other columns instead, which CSV needs because it cannot hold structs:

```python
schema = pa.schema([("id", pa.int64()), ("status", pa.string())])
frame = pd.read_csv("kafka://orders.csv", storage_options={**options, "value_format": "json", "value_schema": schema, "flatten_value": True})
```

`value_schema` can also be anything [fsspec-data](https://github.com/1kbgz/fsspec-data) resolves when it is installed, such as a list of `{"name": ..., "type": ...}` fields or a `SchemaRef` to a schema file or Confluent Schema Registry subject. `value_format="avro"` reads bare Avro records with an `.avsc` reference or an Avro schema dict. `value_format="confluent"` reads and writes values in Confluent wire format, with JSON Schema or Avro subjects. For Avro subjects, every registered version is fetched so older messages resolve to the referenced version's schema. Appends write the schema ID of the Confluent reference:

```python
from fsspec_data import SchemaRef

schema = SchemaRef("confluent://orders-value/versions/latest", storage_options={"registry_url": "http://localhost:8081"})
fs = fsspec.filesystem("kafka", **options, value_format="confluent", value_schema=schema, flatten_value=True)
```

Topics are append-only. Opening `topic.format` or `topic/partition.format` with mode `"ab"` produces one message per row of the file when it closes. The mapped columns set the message fields, and the remaining columns are the value: a JSON object with `value_format="json"`, an Avro record with Avro values, otherwise a single binary or string column. CSV and JSON Lines columns are typed from `value_schema`.

```python
fs = fsspec.filesystem("kafka", **options, value_format="json", value_schema=schema, key_column="order_id")
with fs.open("orders.parquet", "ab") as file:
    frame.to_parquet(file)
```

Options can also come from fsspec configuration, for example `FSSPEC_KAFKA='{"bootstrap.servers": "localhost:9092"}'`.

On a compacted topic, `topic/records` holds the latest value of each key, so it works as a key-value store:

```python
state = fsspec.get_mapper("kafka://state/records", **options)
state["user/42"] = b"..."
```

`fs.stream` follows a topic or one partition as it grows, returning `pyarrow.RecordBatch` objects with the same columns as `.arrow` files. It starts at `"end"` by default; `"beginning"`, a `datetime`, or the `positions` of an earlier stream are also accepted.

```python
with fs.stream("orders", start="beginning") as stream:
    for batch in stream:  # waits for new messages
        handle(batch)
        save(stream.positions)  # {partition: next offset}, to resume with start=positions
```

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
