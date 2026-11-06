//! Discovery against a live broker. Set `FSSPEC_KAFKA_BOOTSTRAP` (see `ci/docker-compose.yml`).

use std::collections::HashMap;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fsspec_data::DataFormat;
use fsspec_kafka::{KafkaBackend, KafkaFs, KafkaFsOptions, RdKafkaBackend};
use fsspec_rs::{FileSystem, FsError, OpenMode};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

const TIMEOUT: Duration = Duration::from_secs(10);

struct Topic {
    name: String,
    admin: AdminClient<DefaultClientContext>,
}

impl Topic {
    /// Creates a uniquely named topic and writes `messages` records to partition 0.
    fn create(bootstrap: &str, partitions: i32, messages: usize) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("fsspec-kafka-test-{nanos}");
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
        let producer: BaseProducer = config.create().unwrap();
        for index in 0..messages {
            let payload = format!("{{\"index\": {index}}}");
            producer
                .send(
                    BaseRecord::to(&name)
                        .partition(0)
                        .key("key")
                        .payload(&payload),
                )
                .unwrap();
        }
        producer.flush(TIMEOUT).unwrap();
        Self { name, admin }
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
