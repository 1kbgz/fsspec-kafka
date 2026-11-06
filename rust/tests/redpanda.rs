//! Discovery and reads against a live broker. Set `FSSPEC_KAFKA_BOOTSTRAP` (see `ci/docker-compose.yml`).

use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fsspec_data::DataFormat;
use fsspec_kafka::{KafkaBackend, KafkaFs, KafkaFsOptions, RdKafkaBackend};
use fsspec_rs::{FileSystem, FsError, OpenMode};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

const TIMEOUT: Duration = Duration::from_secs(10);

struct Topic {
    name: String,
    admin: AdminClient<DefaultClientContext>,
    producer: BaseProducer,
}

impl Topic {
    /// Creates a uniquely named topic and writes `messages` records to partition 0.
    fn create(bootstrap: &str, partitions: i32, messages: usize) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!(
            "fsspec-kafka-test-{nanos}-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", bootstrap);
        let admin: AdminClient<DefaultClientContext> = config.create().unwrap();
        let created = futures_executor::block_on(admin.create_topics(
            &[NewTopic::new(&name, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        ))
        .unwrap();
        created.into_iter().for_each(|result| {
            result.unwrap();
        });
        let topic = Self {
            name,
            admin,
            producer: config.create().unwrap(),
        };
        topic.produce(0, messages);
        topic
    }

    /// Writes `messages` records with key `key`, header `source=test`, and JSON values.
    fn produce(&self, partition: i32, messages: usize) {
        for index in 0..messages {
            let payload = format!("{{\"index\": {index}}}");
            self.producer
                .send(
                    BaseRecord::to(&self.name)
                        .partition(partition)
                        .key("key")
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "source",
                            value: Some("test"),
                        }))
                        .payload(&payload),
                )
                .unwrap();
        }
        self.producer.flush(TIMEOUT).unwrap();
    }
}

impl Drop for Topic {
    fn drop(&mut self) {
        let _ = futures_executor::block_on(
            self.admin
                .delete_topics(&[self.name.as_str()], &AdminOptions::new()),
        );
    }
}

fn bootstrap() -> Option<String> {
    std::env::var("FSSPEC_KAFKA_BOOTSTRAP").ok()
}

fn filesystem(bootstrap: &str) -> KafkaFs<RdKafkaBackend> {
    let config = HashMap::from([("bootstrap.servers".to_string(), bootstrap.to_string())]);
    KafkaFs::new(
        RdKafkaBackend::new(&config, TIMEOUT).unwrap(),
        KafkaFsOptions {
            window_size: 10,
            list_format: DataFormat::Arrow,
            batch_size: 4,
        },
    )
    .unwrap()
}

fn names(entries: Vec<fsspec_rs::FileInfo>) -> Vec<String> {
    entries.into_iter().map(|entry| entry.name).collect()
}

#[test]
fn lists_topics_partitions_and_windows() {
    let Some(bootstrap) = bootstrap() else { return };
    let topic = Topic::create(&bootstrap, 2, 25);
    let fs = filesystem(&bootstrap);
    let name = &topic.name;

    assert!(names(fs.ls("", true).unwrap()).contains(name));
    assert_eq!(
        names(fs.ls(name, true).unwrap()),
        [
            format!("{name}/_meta"),
            format!("{name}/0"),
            format!("{name}/1")
        ]
    );
    assert_eq!(
        names(fs.ls(&format!("kafka://{name}/0"), true).unwrap()),
        [
            format!("{name}/0/0-9.arrow"),
            format!("{name}/0/10-19.arrow"),
            format!("{name}/0/20-.arrow"),
        ]
    );
    assert!(fs.ls(&format!("{name}/1"), true).unwrap().is_empty());

    let head = fs.info(&format!("{name}/0/20-.parquet")).unwrap();
    assert_eq!(head.extra["end"], "24");
    let partition = fs.info(&format!("{name}/0")).unwrap();
    assert_eq!(partition.extra["high"], "25");
}

#[test]
fn reads_meta_files() {
    let Some(bootstrap) = bootstrap() else { return };
    let topic = Topic::create(&bootstrap, 1, 3);
    let fs = filesystem(&bootstrap);
    let read = |file: &str| {
        let mut text = String::new();
        fs.open(
            &format!("{}/_meta/{file}", topic.name),
            OpenMode::Read,
            None,
        )
        .unwrap()
        .read_to_string(&mut text)
        .unwrap();
        serde_json::from_str::<serde_json::Value>(&text).unwrap()
    };

    assert!(read("config.json")["cleanup.policy"].is_string());
    assert_eq!(read("watermarks.json")[0]["high"], 3);
    assert_eq!(read("partitions.json")[0]["partition"], 0);
}

#[test]
fn missing_topics_are_not_found_and_not_created() {
    let Some(bootstrap) = bootstrap() else { return };
    let fs = filesystem(&bootstrap);
    let name = "fsspec-kafka-test-missing";

    assert!(matches!(fs.info(name), Err(FsError::NotFound(_))));
    assert!(matches!(
        fs.ls(&format!("{name}/0"), true),
        Err(FsError::NotFound(_))
    ));
    assert!(!names(fs.ls("", true).unwrap()).contains(&name.to_string()));
}

#[test]
fn finds_offsets_by_time() {
    let Some(bootstrap) = bootstrap() else { return };
    let topic = Topic::create(&bootstrap, 1, 5);
    let fs = filesystem(&bootstrap);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let backend = fs.backend();
    assert_eq!(
        backend
            .offset_for_time(&topic.name, 0, now - 3_600_000)
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        backend
            .offset_for_time(&topic.name, 0, now + 3_600_000)
            .unwrap(),
        None
    );
}

#[test]
fn reads_from_a_timestamp() {
    let Some(bootstrap) = bootstrap() else { return };
    let topic = Topic::create(&bootstrap, 1, 0);
    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 3_600_000;
    for index in 0..5 {
        topic
            .producer
            .send(
                BaseRecord::<(), _>::to(&topic.name)
                    .partition(0)
                    .timestamp(start + index * 1_000)
                    .payload("x"),
            )
            .unwrap();
    }
    topic.producer.flush(TIMEOUT).unwrap();
    let fs = filesystem(&bootstrap);
    let path = |ms: i64| {
        let time = chrono::DateTime::from_timestamp_millis(ms)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        format!("{}/0/@{time}.parquet", topic.name)
    };

    assert_eq!(
        column_i64(&read_parquet(&fs, &path(start + 2_500)), "offset"),
        [3, 4]
    );
    assert_eq!(
        column_i64(&read_parquet(&fs, &path(start)), "offset"),
        [0, 1, 2, 3, 4]
    );
    assert!(read_parquet(&fs, &path(start + 60_000)).is_empty());
}

fn read_parquet(fs: &KafkaFs<RdKafkaBackend>, path: &str) -> Vec<RecordBatch> {
    let mut data = Vec::new();
    fs.open(path, OpenMode::Read, None)
        .unwrap()
        .read_to_end(&mut data)
        .unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn column_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column_by_name(name)
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn reads_window_messages_as_parquet() {
    let Some(bootstrap) = bootstrap() else { return };
    let topic = Topic::create(&bootstrap, 1, 25);
    let fs = filesystem(&bootstrap);

    let batches = read_parquet(&fs, &format!("{}/0/10-19.parquet", topic.name));

    assert_eq!(column_i64(&batches, "offset"), (10..20).collect::<Vec<_>>());
    let batch = &batches[0];
    assert_eq!(
        batch
            .column_by_name("key")
            .unwrap()
            .as_binary::<i32>()
            .value(0),
        b"key"
    );
    assert_eq!(
        batch
            .column_by_name("value")
            .unwrap()
            .as_binary::<i32>()
            .value(0),
        br#"{"index": 10}"#
    );
    let headers = batch.column_by_name("headers").unwrap().as_map();
    assert_eq!(headers.keys().as_string::<i32>().value(0), "source");
    assert_eq!(headers.values().as_binary::<i32>().value(0), b"test");

    // Reads assign partitions without joining or creating a consumer group.
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    let groups = consumer
        .fetch_group_list(Some("fsspec-kafka"), TIMEOUT)
        .unwrap();
    assert!(groups.groups().is_empty());
}

#[test]
fn topic_snapshot_reads_all_partitions() {
    let Some(bootstrap) = bootstrap() else { return };
    let topic = Topic::create(&bootstrap, 2, 3);
    topic.produce(1, 4);
    let fs = filesystem(&bootstrap);

    let batches = read_parquet(&fs, &format!("{}.parquet", topic.name));

    let mut rows = batches
        .iter()
        .flat_map(|b| {
            let partitions = b
                .column_by_name("partition")
                .unwrap()
                .as_primitive::<arrow::datatypes::Int32Type>();
            let offsets = b
                .column_by_name("offset")
                .unwrap()
                .as_primitive::<Int64Type>();
            (0..b.num_rows())
                .map(|i| (partitions.value(i), offsets.value(i)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(
        rows,
        [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (1, 3)]
    );
}

#[test]
fn messages_produced_after_open_are_excluded() {
    let Some(bootstrap) = bootstrap() else { return };
    let topic = Topic::create(&bootstrap, 1, 5);
    let fs = filesystem(&bootstrap);

    let mut file = fs
        .open(&format!("{}/0.parquet", topic.name), OpenMode::Read, None)
        .unwrap();
    topic.produce(0, 5);
    let mut data = Vec::new();
    file.read_to_end(&mut data).unwrap();
    let batches: Vec<RecordBatch> =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

    assert_eq!(column_i64(&batches, "offset"), [0, 1, 2, 3, 4]);
    assert_eq!(
        read_parquet(&fs, &format!("{}/0.parquet", topic.name))
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        10
    );
}

#[test]
fn empty_partition_reads_as_empty_file() {
    let Some(bootstrap) = bootstrap() else { return };
    let topic = Topic::create(&bootstrap, 2, 1);
    let fs = filesystem(&bootstrap);

    assert!(read_parquet(&fs, &format!("{}/1.parquet", topic.name)).is_empty());
}
