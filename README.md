# fsspec-kafka

Kafka topics and partitions through fsspec

[![Build Status](https://github.com/1kbgz/fsspec-kafka/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/fsspec-kafka/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/fsspec-kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/fsspec-kafka)
[![License](https://img.shields.io/github/license/1kbgz/fsspec-kafka)](https://github.com/1kbgz/fsspec-kafka)
[![PyPI](https://img.shields.io/pypi/v/fsspec-kafka.svg)](https://pypi.python.org/pypi/fsspec-kafka)

## Overview

`fsspec-kafka` registers Kafka with fsspec. Topics are directories, and messages read as Arrow, Parquet, CSV, or JSON Lines. Each protocol arranges a topic's messages in one way:

| Protocol                      | Layout                                              | Contents                                          |
| ----------------------------- | --------------------------------------------------- | ------------------------------------------------- |
| `kafka+offset://`, `kafka://` | `topic/{partition}/{start}-{end}.arrow`             | Fixed offset windows of each partition            |
| `kafka+time://`               | `topic/{YYYY}/{MM}/{DD}/{hh}/{mm}/{ss}/{mmm}.arrow` | Messages by UTC timestamp, plus `topic/weeks/`    |
| `kafka+records://`            | `topic/{key...}`                                    | The latest value of each key in a compacted topic |
| `kafka+history://`            | `topic/{key...}`                                    | Every message of each key                         |

In every protocol, `topic.parquet` (or another extension) reads the whole topic; in `kafka+records`, it holds the latest message of each key.

```python
import fsspec
import pandas as pd

options = {"bootstrap.servers": "localhost:9092"}
fs = fsspec.filesystem("kafka", **options)
fs.ls("orders/0", detail=False)  # ["orders/0/0-10000.arrow", ..., "orders/0/20000-.arrow"]

frame = pd.read_parquet("kafka://orders.parquet", storage_options=options)
```

Storage options are librdkafka properties, except for these:

| Option                      | Default         | Meaning                                                                 |
| --------------------------- | --------------- | ----------------------------------------------------------------------- |
| `timeout`                   | `10.0`          | Seconds to wait for broker requests                                     |
| `window_size`               | `10000`         | Offsets per `kafka+offset` window                                       |
| `time_bucket`               | `"millisecond"` | Deepest `kafka+time` level: `year` through `millisecond`                |
| `list_format`               | `"arrow"`       | Extension used when listing message files                               |
| `batch_size`                | `1024`          | Messages per Arrow record batch                                         |
| `allow_admin`               | `False`         | Lets `mkdir` create topics and `rmdir` delete them                      |
| `partitions`, `replication` | `-1`            | Settings for created topics; `-1` uses the broker default               |
| `topic_config`              | `{}`            | Configuration for created topics                                        |
| `value_format`              | `"binary"`      | `"json"`, `"avro"`, or `"confluent"` decodes values with `value_schema` |
| `value_schema`              | `None`          | `pyarrow.Schema` or fsspec-data schema reference                        |
| `flatten_value`             | `False`         | Puts JSON fields beside the message columns                             |
| `key_column`                | `None`          | Input column for message keys when appending                            |
| `partition_column`          | `None`          | Input column for partitions when appending                              |
| `timestamp_column`          | `None`          | Input column for timestamps when appending                              |
| `headers_column`            | `None`          | Input column for headers, a map, when appending                         |

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

Topics are append-only. Opening `topic.format`, or `topic/partition.format` in `kafka+offset`, with mode `"ab"` produces one message per row of the file when it closes. The mapped columns set the message fields, and the remaining columns are the value: a JSON object with `value_format="json"`, an Avro record with Avro values, otherwise a single binary or string column. CSV and JSON Lines columns are typed from `value_schema`.

```python
fs = fsspec.filesystem("kafka", **options, value_format="json", value_schema=schema, key_column="order_id")
with fs.open("orders.parquet", "ab") as file:
    frame.to_parquet(file)
```

Options can also come from fsspec configuration, for example `FSSPEC_KAFKA='{"bootstrap.servers": "localhost:9092"}'`.

`fs.metadata("orders")` returns a topic's configuration and, for each partition, its leader, replicas, in-sync replicas, and low and high watermarks. `fs.metadata("orders/0")` returns one partition's. `fs.offsets_for_time("orders", time)` returns the first offset at or after a time in each partition.

`kafka+time` nests UTC time buckets from years down to `time_bucket`, which defaults to milliseconds. Each bucket is a directory of the next level, and with an extension, such as `orders/2026/11.parquet`, a file of its messages. Buckets at the `time_bucket` level and the ISO weeks in `topic/weeks/` are listed as files. Listings cover the time between a topic's first and last messages. Reads use the broker's time index, which assumes timestamps increase with offsets within a partition.

```python
fs = fsspec.filesystem("kafka+time", **options, time_bucket="hour")
fs.ls("orders/2026/11/17", detail=False)  # ["orders/2026/11/17/00.arrow", ...]
frame = pd.read_parquet("kafka+time://orders/weeks/2026-W47.parquet", storage_options=options)
```

`kafka+records` keeps the latest value of each key in a compacted topic, so it works as a key-value store. Writing a key produces a message and deleting it produces a tombstone. With `allow_admin=True`, `mkdir("state")` creates a compacted topic without time retention.

```python
state = fsspec.get_mapper("kafka+records://state", **options)
state["user/42"] = b"..."
```

`kafka+history` lists the same keys in any topic, and each key reads as a `list_format` file of every retained message with that key, including tombstones. Listing keys and reading a key both read the whole topic.

Every Kafka filesystem is an [fsspec-data](https://github.com/1kbgz/fsspec-data) data source, so a `dataview` renders its topics, partitions, time buckets, and keys as Parquet, CSV, or JSON files, and each topic's and partition's metadata as `.metadata.json`:

```python
frame = pd.read_parquet("dataview://orders/2026.parquet::kafka+time://", storage_options={"kafka+time": options})
```

`fs.stream` follows a topic or one partition as it grows, returning `pyarrow.RecordBatch` objects with the same columns as `.arrow` files. It starts at `"end"` by default; `"beginning"`, a `datetime`, or the `positions` of an earlier stream are also accepted.

```python
with fs.stream("orders", start="beginning") as stream:
    for batch in stream:  # waits for new messages
        handle(batch)
        save(stream.positions)  # {partition: next offset}, to resume with start=positions
```

From Rust, register the Kafka protocols with an fsspec-rs `FilesystemRegistry` and open URLs with the same storage options. `value_format` accepts only `binary` here, and `topic_config.<name>` keys set the configuration of created topics; construct `KafkaFs` with `KafkaFsOptions` for decoded values.

```rust
let mut registry = fsspec_rs::FilesystemRegistry::default();
fsspec_kafka::register(&mut registry);
let options = [("bootstrap.servers".to_string(), "localhost:9092".to_string())].into();
let (fs, path) = registry.url_to_fs("kafka://orders/0", &options)?;
```

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
