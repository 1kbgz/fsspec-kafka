use std::collections::{BTreeMap, HashMap};

use fsspec_rs::{FsError, FsResult};

use crate::backend::{KafkaBackend, PartitionRange};

/// Topic configuration applied by `kafka+records` `mkdir("topic")` over
/// `KafkaFsOptions::topic_config`.
/// Compaction keeps the latest value of every key without time retention. Tombstones stay
/// readable for a day, so readers that catch up at least that often see every delete.
pub const RECORDS_TOPIC_CONFIG: [(&str, &str); 3] = [
    ("cleanup.policy", "compact"),
    ("retention.ms", "-1"),
    ("delete.retention.ms", "86400000"),
];

/// Where the latest value of a key is stored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordLocation {
    pub partition: i32,
    pub offset: i64,
    pub size: u64,
}

/// The latest location of every live key in a compacted topic, or with `history`, of every
/// key with a retained message in any topic.
#[derive(Debug, Default)]
pub struct RecordIndex {
    /// Keeps keys whose latest message is a tombstone.
    history: bool,
    /// Next offset to replay in each partition.
    next: HashMap<i32, i64>,
    /// Low watermark of each partition when it was last replayed.
    low: HashMap<i32, i64>,
    keys: BTreeMap<String, RecordLocation>,
}

impl RecordIndex {
    /// Fails with `NotSupported` unless the topic is compacted without time retention,
    /// since otherwise live values could age out.
    pub fn open<B: KafkaBackend>(backend: &B, topic: &str) -> FsResult<Self> {
        backend.topic(topic)?;
        let config = backend.topic_config(topic)?;
        let policy = config.get("cleanup.policy").map(String::as_str);
        if policy != Some("compact") {
            return Err(FsError::NotSupported(format!(
                "records need cleanup.policy=compact, but {topic} has {}",
                policy.unwrap_or("no cleanup.policy")
            )));
        }
        let mut index = Self::default();
        index.catch_up(backend, topic)?;
        Ok(index)
    }

    /// Indexes every key with a retained message, including keys whose latest message is a
    /// tombstone. Works on any topic.
    pub fn open_history<B: KafkaBackend>(backend: &B, topic: &str) -> FsResult<Self> {
        backend.topic(topic)?;
        let mut index = Self {
            history: true,
            ..Self::default()
        };
        index.catch_up(backend, topic)?;
        Ok(index)
    }

    /// Replays every partition from where the last replay stopped to its current high
    /// watermark. Keys whose latest value lies below a raised low watermark, for example
    /// after `DeleteRecords`, are dropped.
    pub fn catch_up<B: KafkaBackend>(&mut self, backend: &B, topic: &str) -> FsResult<()> {
        let mut ranges = Vec::new();
        for partition in backend.topic(topic)?.partitions {
            let marks = backend.watermarks(topic, partition.id)?;
            let low = self
                .low
                .insert(partition.id, marks.low)
                .unwrap_or(marks.low);
            if marks.low > low {
                self.keys.retain(|_, location| {
                    location.partition != partition.id || location.offset >= marks.low
                });
            }
            let start = self
                .next
                .get(&partition.id)
                .map_or(marks.low, |next| (*next).max(marks.low));
            if start < marks.high {
                ranges.push(PartitionRange {
                    partition: partition.id,
                    start,
                    end: marks.high,
                });
            }
        }
        for message in backend.read(topic, ranges.clone())? {
            let message = message?;
            let Some(key) = message.key.and_then(|key| String::from_utf8(key).ok()) else {
                continue;
            };
            match message.value {
                None if !self.history => {
                    self.keys.remove(&key);
                }
                value => {
                    self.keys.insert(
                        key,
                        RecordLocation {
                            partition: message.partition,
                            offset: message.offset,
                            size: value.map_or(0, |value| value.len() as u64),
                        },
                    );
                }
            }
        }
        // Ranges can end in transaction markers or compacted offsets that hold no message.
        for range in ranges {
            self.next.insert(range.partition, range.end);
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<RecordLocation> {
        self.keys.get(key).copied()
    }

    pub fn remove(&mut self, key: &str) {
        self.keys.remove(key);
    }

    /// Whether any key lies under `prefix` treated as a directory. Every key lies under the
    /// empty prefix.
    pub fn is_dir(&self, prefix: &str) -> bool {
        let prefix = dir_prefix(prefix);
        self.keys
            .range(prefix.clone()..)
            .next()
            .is_some_and(|(key, _)| key.starts_with(&prefix))
    }

    /// Keys and directories directly under `prefix`, with the location of each key and `None`
    /// for directories. Keys with empty path segments cannot be addressed by path and are
    /// left out.
    pub fn children(&self, prefix: &str) -> Vec<(String, Option<RecordLocation>)> {
        let prefix = dir_prefix(prefix);
        let mut entries = Vec::new();
        let mut last_dir: Option<&str> = None;
        for (key, location) in self.keys.range(prefix.clone()..) {
            let Some(rest) = key.strip_prefix(&prefix) else {
                break;
            };
            if rest.split('/').any(str::is_empty) {
                continue;
            }
            match rest.split_once('/') {
                Some((dir, _)) => {
                    if last_dir != Some(dir) {
                        entries.push((format!("{prefix}{dir}"), None));
                        last_dir = Some(dir);
                    }
                }
                None => entries.push((key.clone(), Some(*location))),
            }
        }
        entries
    }
}

fn dir_prefix(prefix: &str) -> String {
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    }
}

/// Reads the value stored at `location`, or `None` when compaction has removed it.
pub fn fetch<B: KafkaBackend>(
    backend: &B,
    topic: &str,
    location: RecordLocation,
) -> FsResult<Option<Vec<u8>>> {
    let range = PartitionRange {
        partition: location.partition,
        start: location.offset,
        end: location.offset + 1,
    };
    for message in backend.read(topic, vec![range])? {
        let message = message?;
        if message.offset == location.offset {
            return Ok(message.value);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use fsspec_rs::{FileInfo, FileSystem, OpenMode};

    use super::*;
    use crate::backend::{
        LiveMessages, Message, MessageOffset, MessageStream, NewMessage, PartitionInfo, TopicInfo,
        Watermarks,
    };
    use crate::fs::{KafkaFs, KafkaFsOptions};
    use crate::path::KafkaView;

    /// Partitions are offset-indexed logs; `None` entries were removed by compaction.
    #[derive(Default)]
    struct MemoryTopic {
        config: BTreeMap<String, String>,
        partitions: Vec<(i64, Vec<Option<Message>>)>,
    }

    #[derive(Clone, Default)]
    struct MemoryBackend {
        topics: Arc<Mutex<HashMap<String, MemoryTopic>>>,
    }

    impl MemoryBackend {
        fn with_topic(name: &str, partitions: usize, policy: &str) -> Self {
            let backend = Self::default();
            backend.topics.lock().unwrap().insert(
                name.into(),
                MemoryTopic {
                    config: BTreeMap::from([("cleanup.policy".into(), policy.into())]),
                    partitions: vec![(0, Vec::new()); partitions],
                },
            );
            backend
        }

        fn compact(&self, topic: &str, partition: i32, offset: i64) {
            self.topics
                .lock()
                .unwrap()
                .get_mut(topic)
                .unwrap()
                .partitions[partition as usize]
                .1[offset as usize] = None;
        }

        fn set_low(&self, topic: &str, partition: i32, low: i64) {
            self.topics
                .lock()
                .unwrap()
                .get_mut(topic)
                .unwrap()
                .partitions[partition as usize]
                .0 = low;
        }
    }

    impl KafkaBackend for MemoryBackend {
        fn list_topics(&self) -> FsResult<Vec<TopicInfo>> {
            unimplemented!()
        }

        fn topic(&self, topic: &str) -> FsResult<TopicInfo> {
            let topics = self.topics.lock().unwrap();
            let found = topics
                .get(topic)
                .ok_or_else(|| FsError::NotFound(topic.into()))?;
            Ok(TopicInfo {
                name: topic.into(),
                partitions: (0..found.partitions.len() as i32)
                    .map(|id| PartitionInfo {
                        id,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    })
                    .collect(),
            })
        }

        fn watermarks(&self, topic: &str, partition: i32) -> FsResult<Watermarks> {
            let (low, log) = &self.topics.lock().unwrap()[topic].partitions[partition as usize];
            Ok(Watermarks {
                low: *low,
                high: log.len() as i64,
            })
        }

        fn offset_for_time(&self, _: &str, _: i32, _: i64) -> FsResult<Option<i64>> {
            unimplemented!()
        }

        fn topic_config(&self, topic: &str) -> FsResult<BTreeMap<String, String>> {
            Ok(self.topics.lock().unwrap()[topic].config.clone())
        }

        fn read(&self, topic: &str, ranges: Vec<PartitionRange>) -> FsResult<MessageStream> {
            let topics = self.topics.lock().unwrap();
            let mut messages = Vec::new();
            for range in ranges {
                let (low, log) = &topics[topic].partitions[range.partition as usize];
                let end = range.end.min(log.len() as i64);
                for offset in range.start.max(*low)..end {
                    messages.extend(log[offset as usize].clone().map(Ok));
                }
            }
            Ok(Box::new(messages.into_iter()))
        }

        fn follow(
            &self,
            topic: &str,
            _starts: Vec<MessageOffset>,
        ) -> FsResult<Box<dyn LiveMessages>> {
            Err(FsError::NotSupported(topic.into()))
        }

        fn produce(&self, topic: &str, messages: Vec<NewMessage>) -> FsResult<Vec<MessageOffset>> {
            let mut topics = self.topics.lock().unwrap();
            let partitions = &mut topics
                .get_mut(topic)
                .ok_or_else(|| FsError::NotFound(topic.into()))?
                .partitions;
            let count = partitions.len();
            Ok(messages
                .into_iter()
                .map(|message| {
                    let partition = message.key.as_ref().map_or(0, |key| {
                        key.iter().map(|byte| *byte as usize).sum::<usize>() % count
                    }) as i32;
                    let log = &mut partitions[partition as usize].1;
                    let offset = log.len() as i64;
                    log.push(Some(Message {
                        partition,
                        offset,
                        key: message.key,
                        value: message.value,
                        ..Message::default()
                    }));
                    MessageOffset { partition, offset }
                })
                .collect())
        }

        fn create_topic(
            &self,
            topic: &str,
            partitions: i32,
            _: i32,
            config: &BTreeMap<String, String>,
        ) -> FsResult<()> {
            self.topics.lock().unwrap().insert(
                topic.into(),
                MemoryTopic {
                    config: config.clone(),
                    partitions: vec![(0, Vec::new()); partitions.max(1) as usize],
                },
            );
            Ok(())
        }

        fn delete_topic(&self, _: &str) -> FsResult<()> {
            unimplemented!()
        }
    }

    fn records_fs(backend: &MemoryBackend) -> KafkaFs<MemoryBackend> {
        KafkaFs::new(
            backend.clone(),
            KafkaFsOptions {
                view: KafkaView::Records,
                ..KafkaFsOptions::default()
            },
        )
        .unwrap()
    }

    fn names(entries: Vec<FileInfo>) -> Vec<String> {
        entries.into_iter().map(|entry| entry.name).collect()
    }

    #[test]
    fn writes_reads_lists_and_deletes_records() {
        let backend = MemoryBackend::with_topic("state", 3, "compact");
        let fs = records_fs(&backend);
        fs.pipe_file("kafka+records://state/one/a", b"a1").unwrap();
        fs.pipe_file("state/one/b", b"").unwrap();
        fs.pipe_file("state/two/a%2Fb", b"x").unwrap();
        fs.pipe_file("state/one/a", b"a2").unwrap();

        assert_eq!(fs.cat_file("state/one/a", None, None).unwrap(), b"a2");
        assert_eq!(fs.cat_file("state/one/b", None, None).unwrap(), b"");
        assert_eq!(fs.info("state/one/a").unwrap().size, 2);
        assert!(fs.info("state/one").unwrap().is_dir());
        assert!(fs.info("state").unwrap().is_dir());
        assert_eq!(
            names(fs.ls("state", true).unwrap()),
            ["state/one", "state/two"]
        );
        assert_eq!(
            names(fs.ls("state/one", true).unwrap()),
            ["state/one/a", "state/one/b"]
        );
        assert_eq!(
            fs.find("state", None, false).unwrap(),
            ["state/one/a", "state/one/b", "state/two/a%2Fb"]
        );

        fs.rm_file("state/one/a").unwrap();
        assert!(!fs.exists("state/one/a").unwrap());
        assert!(matches!(
            fs.cat_file("state/one/a", None, None),
            Err(FsError::NotFound(_))
        ));
        assert!(matches!(
            fs.rm_file("state/one/a"),
            Err(FsError::NotFound(_))
        ));
        assert!(matches!(
            fs.ls("state/missing", true),
            Err(FsError::NotFound(_))
        ));
    }

    #[test]
    fn uncommitted_writes_are_not_visible() {
        let backend = MemoryBackend::with_topic("state", 1, "compact");
        let fs = records_fs(&backend);
        fs.pipe_file("state/key", b"old").unwrap();

        let mut file = fs.open("state/key", OpenMode::Write, None).unwrap();
        file.write_all(b"new").unwrap();
        file.flush().unwrap();
        assert_eq!(fs.cat_file("state/key", None, None).unwrap(), b"old");
        file.commit().unwrap();
        assert_eq!(fs.cat_file("state/key", None, None).unwrap(), b"new");

        let mut file = fs.open("state/key", OpenMode::Write, None).unwrap();
        file.write_all(b"dropped").unwrap();
        file.discard().unwrap();
        drop(file);
        assert_eq!(fs.cat_file("state/key", None, None).unwrap(), b"new");
    }

    #[test]
    fn other_writers_are_visible_after_catching_up() {
        let backend = MemoryBackend::with_topic("state", 2, "compact");
        let reader = records_fs(&backend);
        let writer = records_fs(&backend);
        assert!(reader.ls("state", true).unwrap().is_empty());

        writer.pipe_file("state/key", b"v1").unwrap();
        assert_eq!(reader.cat_file("state/key", None, None).unwrap(), b"v1");
        writer.rm_file("state/key").unwrap();
        assert!(!reader.exists("state/key").unwrap());
    }

    #[test]
    fn compacted_values_are_found_again_or_reported_missing() {
        let backend = MemoryBackend::with_topic("state", 1, "compact");
        let reader = records_fs(&backend);
        let writer = records_fs(&backend);
        writer.pipe_file("state/kept", b"v1").unwrap();
        writer.pipe_file("state/gone", b"v1").unwrap();
        assert_eq!(reader.find("state", None, false).unwrap().len(), 2);

        // A newer value replaces offset 0; a tombstone replaces offset 1 and is removed too.
        writer.pipe_file("state/kept", b"v2").unwrap();
        backend.compact("state", 0, 0);
        backend.compact("state", 0, 1);
        assert_eq!(reader.cat_file("state/kept", None, None).unwrap(), b"v2");
        assert!(matches!(
            reader.cat_file("state/gone", None, None),
            Err(FsError::NotFound(_))
        ));
        assert_eq!(reader.find("state", None, false).unwrap(), ["state/kept"]);
    }

    #[test]
    fn keys_below_a_raised_low_watermark_are_dropped() {
        let backend = MemoryBackend::with_topic("state", 1, "compact");
        let fs = records_fs(&backend);
        fs.pipe_file("state/a", b"1").unwrap();
        fs.pipe_file("state/b", b"1").unwrap();
        assert_eq!(fs.find("state", None, false).unwrap().len(), 2);

        fs.pipe_file("state/c", b"1").unwrap();
        backend.set_low("state", 0, 2);
        assert_eq!(fs.find("state", None, false).unwrap(), ["state/c"]);
    }

    #[test]
    fn records_need_a_compacted_topic() {
        let backend = MemoryBackend::with_topic("events", 1, "compact,delete");
        let fs = records_fs(&backend);
        assert!(matches!(
            fs.ls("events", true),
            Err(FsError::NotSupported(_))
        ));
        assert!(matches!(
            fs.pipe_file("events/key", b"x"),
            Err(FsError::NotSupported(_))
        ));
        assert!(matches!(fs.ls("missing", true), Err(FsError::NotFound(_))));
    }

    #[test]
    fn mkdir_creates_a_compacted_records_topic() {
        let backend = MemoryBackend::default();
        let fs = KafkaFs::new(
            backend.clone(),
            KafkaFsOptions {
                view: KafkaView::Records,
                allow_admin: true,
                topic_config: BTreeMap::from([
                    ("cleanup.policy".to_string(), "delete".to_string()),
                    ("max.message.bytes".to_string(), "1048576".to_string()),
                ]),
                ..KafkaFsOptions::default()
            },
        )
        .unwrap();
        fs.makedirs("state", false).unwrap();
        fs.makedirs("state/ns", false).unwrap();

        let config = backend.topic_config("state").unwrap();
        for (name, value) in RECORDS_TOPIC_CONFIG {
            assert_eq!(config[name], value);
        }
        assert_eq!(config["max.message.bytes"], "1048576");
        fs.pipe_file("state/ns/key", b"x").unwrap();
        assert_eq!(fs.cat_file("state/ns/key", None, None).unwrap(), b"x");
    }

    fn read_rows(fs: &KafkaFs<MemoryBackend>, path: &str) -> Vec<(String, Option<String>)> {
        use arrow::array::{Array, AsArray};
        let data = fs.cat_file(path, None, None).unwrap();
        arrow::ipc::reader::StreamReader::try_new(data.as_slice(), None)
            .unwrap()
            .flat_map(|batch| {
                let batch = batch.unwrap();
                let keys = batch
                    .column_by_name("key")
                    .unwrap()
                    .as_binary::<i32>()
                    .clone();
                let values = batch
                    .column_by_name("value")
                    .unwrap()
                    .as_binary::<i32>()
                    .clone();
                (0..batch.num_rows())
                    .map(|row| {
                        let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).unwrap();
                        let value = (!values.is_null(row)).then(|| text(values.value(row)));
                        (text(keys.value(row)), value)
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn a_records_topic_reads_as_its_latest_values() {
        let backend = MemoryBackend::with_topic("state", 1, "compact");
        let fs = records_fs(&backend);
        fs.pipe_file("state/a", b"a1").unwrap();
        fs.pipe_file("state/b", b"b1").unwrap();
        fs.pipe_file("state/a", b"a2").unwrap();
        fs.pipe_file("state/c", b"c1").unwrap();
        fs.rm_file("state/c").unwrap();

        assert_eq!(
            read_rows(&fs, "kafka+records://state.arrow"),
            [
                ("b".into(), Some("b1".into())),
                ("a".into(), Some("a2".into()))
            ]
        );
        assert_eq!(fs.info("state/a").unwrap().extra["kind"], "record");
    }

    #[test]
    fn history_lists_every_key_and_reads_its_messages() {
        let backend = MemoryBackend::with_topic("events", 1, "delete");
        let message = |key: &str, value: Option<&str>| NewMessage {
            key: Some(key.into()),
            value: value.map(Into::into),
            ..NewMessage::default()
        };
        backend
            .produce(
                "events",
                vec![
                    message("user/a", Some("1")),
                    message("user/b", Some("1")),
                    message("user/a", Some("2")),
                    message("user/a", None),
                ],
            )
            .unwrap();
        let fs = KafkaFs::new(
            backend.clone(),
            KafkaFsOptions {
                view: KafkaView::History,
                ..KafkaFsOptions::default()
            },
        )
        .unwrap();

        assert_eq!(fs.protocol(), ["kafka+history"]);
        assert_eq!(names(fs.ls("events", true).unwrap()), ["events/user"]);
        assert_eq!(fs.info("events/user").unwrap().extra["kind"], "keys");
        assert_eq!(
            names(fs.ls("kafka+history://events/user", true).unwrap()),
            ["events/user/a", "events/user/b"]
        );
        let info = fs.info("events/user/a").unwrap();
        assert!(info.is_file());
        assert_eq!(
            (info.extra["kind"].as_str(), info.extra["format"].as_str()),
            ("history", "arrow")
        );
        assert_eq!(
            read_rows(&fs, "events/user/a"),
            [
                ("user/a".into(), Some("1".into())),
                ("user/a".into(), Some("2".into())),
                ("user/a".into(), None)
            ]
        );
        assert!(matches!(
            fs.open("events/user", OpenMode::Read, None),
            Err(FsError::IsADirectory(_))
        ));
        assert!(matches!(
            fs.pipe_file("events/user/a", b"x"),
            Err(FsError::NotSupported(_))
        ));
        assert!(matches!(
            fs.info("events/user/missing"),
            Err(FsError::NotFound(_))
        ));
    }
}
