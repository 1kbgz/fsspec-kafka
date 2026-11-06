use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use fsspec_data::DataFormat;
use fsspec_rs::{FileInfo, FileSystem, FsError, FsFile, FsResult, OpenMode, OpenOptions};
use serde_json::json;

use crate::backend::{KafkaBackend, PartitionRange, TopicInfo, Watermarks};
use crate::path::{KafkaPath, MetaFile, Window};
use crate::read::{EncodedFile, MessageBatches};

const PROTOCOLS: &[&str] = &["kafka"];

#[derive(Clone, Debug)]
pub struct KafkaFsOptions {
    /// Offsets per window file. Windows start at multiples of this size.
    pub window_size: i64,
    /// Extension used when listing window files. Any supported extension can be opened.
    pub list_format: DataFormat,
    /// Messages per Arrow record batch when reading.
    pub batch_size: usize,
}

impl Default for KafkaFsOptions {
    fn default() -> Self {
        Self {
            window_size: 10_000,
            list_format: DataFormat::Arrow,
            batch_size: 1024,
        }
    }
}

/// Kafka topics as directories, partitions as directories of fixed offset windows.
pub struct KafkaFs<B> {
    backend: Arc<B>,
    options: KafkaFsOptions,
}

impl<B: KafkaBackend> KafkaFs<B> {
    pub fn new(backend: B, options: KafkaFsOptions) -> FsResult<Self> {
        if options.window_size <= 0 || options.batch_size == 0 {
            return Err(FsError::InvalidArgument(
                "window_size and batch_size must be positive".to_string(),
            ));
        }
        Ok(Self {
            backend: Arc::new(backend),
            options,
        })
    }

    pub fn backend(&self) -> &B {
        self.backend.as_ref()
    }

    /// Parses `path`, treating `name.format` as a topic directory when a topic has that
    /// exact name.
    fn resolve(&self, path: &str) -> FsResult<KafkaPath> {
        let parsed = KafkaPath::parse(path)?;
        if let KafkaPath::TopicSnapshot { .. } = parsed {
            let name = self.strip_protocol(path);
            if self.backend.topic(&name).is_ok() {
                return Ok(KafkaPath::Topic(name));
            }
        }
        Ok(parsed)
    }

    fn partition(&self, topic: &str, partition: i32) -> FsResult<TopicInfo> {
        let info = self.backend.topic(topic)?;
        if !info.partitions.iter().any(|p| p.id == partition) {
            return Err(FsError::NotFound(format!("{topic}/{partition}")));
        }
        Ok(info)
    }

    fn meta_content(&self, topic: &str, file: MetaFile) -> FsResult<Vec<u8>> {
        let info = self.backend.topic(topic)?;
        let value = match file {
            MetaFile::Config => json!(self.backend.topic_config(topic)?),
            MetaFile::Partitions => json!(info
                .partitions
                .iter()
                .map(|p| json!({
                    "partition": p.id,
                    "leader": p.leader,
                    "replicas": p.replicas,
                    "isr": p.isr,
                }))
                .collect::<Vec<_>>()),
            MetaFile::Watermarks => json!(info
                .partitions
                .iter()
                .map(|p| {
                    let marks = self.backend.watermarks(topic, p.id)?;
                    Ok(json!({"partition": p.id, "low": marks.low, "high": marks.high}))
                })
                .collect::<FsResult<Vec<_>>>()?),
        };
        serde_json::to_vec_pretty(&value).map_err(|e| FsError::Other(e.to_string()))
    }

    /// Opens a message file. Offsets are bounded by the watermarks read here, so messages
    /// produced after opening are not included.
    fn open_data(&self, path: KafkaPath) -> FsResult<Box<dyn FsFile>> {
        let info = self.info_kafka(path.clone())?;
        let (topic, format, ranges) = match path {
            KafkaPath::Window {
                topic,
                partition,
                window,
                format,
            } => {
                let marks = self.backend.watermarks(&topic, partition)?;
                let end = window
                    .end
                    .map_or(marks.high, |end| (end + 1).min(marks.high));
                let range = PartitionRange {
                    partition,
                    start: window.start.max(marks.low),
                    end,
                };
                (topic, format, vec![range])
            }
            KafkaPath::PartitionRead {
                topic,
                partition,
                format,
                start,
                limit,
            } => {
                let marks = self.backend.watermarks(&topic, partition)?;
                let start = start.unwrap_or(marks.low).max(marks.low);
                let end = limit.map_or(marks.high, |limit| {
                    start
                        .saturating_add(i64::try_from(limit).unwrap_or(i64::MAX))
                        .min(marks.high)
                });
                (
                    topic,
                    format,
                    vec![PartitionRange {
                        partition,
                        start,
                        end,
                    }],
                )
            }
            KafkaPath::TimeRead {
                topic,
                partition,
                timestamp_ms,
                format,
            } => {
                let marks = self.backend.watermarks(&topic, partition)?;
                // Timestamps need not increase with offsets, so later messages may be older.
                let start = self
                    .backend
                    .offset_for_time(&topic, partition, timestamp_ms)?
                    .map_or(marks.high, |start| start.clamp(marks.low, marks.high));
                (
                    topic,
                    format,
                    vec![PartitionRange {
                        partition,
                        start,
                        end: marks.high,
                    }],
                )
            }
            KafkaPath::TopicSnapshot { topic, format } => {
                let ranges = self
                    .backend
                    .topic(&topic)?
                    .partitions
                    .iter()
                    .map(|p| {
                        let marks = self.backend.watermarks(&topic, p.id)?;
                        Ok(PartitionRange {
                            partition: p.id,
                            start: marks.low,
                            end: marks.high,
                        })
                    })
                    .collect::<FsResult<Vec<_>>>()?;
                (topic, format, ranges)
            }
            path => return Err(FsError::IsADirectory(path.to_path())),
        };
        let messages = self.backend.read(&topic, ranges)?;
        let batches = MessageBatches::new(&topic, messages, format, self.options.batch_size);
        Ok(Box::new(EncodedFile::new(batches, format, info)?))
    }

    fn ls_kafka(&self, path: KafkaPath) -> FsResult<Vec<FileInfo>> {
        match path {
            KafkaPath::Root => Ok(self.backend.list_topics()?.iter().map(topic_dir).collect()),
            KafkaPath::Topic(topic) => {
                let info = self.backend.topic(&topic)?;
                let mut entries = vec![FileInfo::directory(
                    KafkaPath::MetaDir(topic.clone()).to_path(),
                )];
                entries.extend(info.partitions.iter().map(|p| {
                    FileInfo::directory(KafkaPath::Partition(topic.clone(), p.id).to_path())
                }));
                Ok(entries)
            }
            KafkaPath::MetaDir(topic) => MetaFile::ALL
                .into_iter()
                .map(|file| self.info_kafka(KafkaPath::Meta(topic.clone(), file)))
                .collect(),
            KafkaPath::Partition(topic, partition) => {
                self.partition(&topic, partition)?;
                let marks = self.backend.watermarks(&topic, partition)?;
                Ok(windows(marks, self.options.window_size)
                    .into_iter()
                    .map(|window| {
                        window_file(&topic, partition, window, self.options.list_format, marks)
                    })
                    .collect())
            }
            path => Err(FsError::NotADirectory(path.to_path())),
        }
    }

    fn info_kafka(&self, path: KafkaPath) -> FsResult<FileInfo> {
        match &path {
            KafkaPath::Root => Ok(FileInfo::directory("")),
            KafkaPath::Topic(topic) => Ok(topic_dir(&self.backend.topic(topic)?)),
            KafkaPath::MetaDir(topic) => {
                self.backend.topic(topic)?;
                Ok(FileInfo::directory(path.to_path()))
            }
            KafkaPath::Meta(topic, file) => {
                let size = self.meta_content(topic, *file)?.len() as u64;
                Ok(FileInfo::file(path.to_path(), size))
            }
            KafkaPath::Partition(topic, partition) => {
                self.partition(topic, *partition)?;
                let marks = self.backend.watermarks(topic, *partition)?;
                let mut info = FileInfo::directory(path.to_path());
                info.extra.insert("low".into(), marks.low.to_string());
                info.extra.insert("high".into(), marks.high.to_string());
                Ok(info)
            }
            KafkaPath::Window {
                topic,
                partition,
                window,
                format,
            } => {
                self.partition(topic, *partition)?;
                let marks = self.backend.watermarks(topic, *partition)?;
                if !windows(marks, self.options.window_size).contains(window) {
                    return Err(FsError::NotFound(path.to_path()));
                }
                Ok(window_file(topic, *partition, *window, *format, marks))
            }
            KafkaPath::PartitionRead {
                topic,
                partition,
                format,
                ..
            }
            | KafkaPath::TimeRead {
                topic,
                partition,
                format,
                ..
            } => {
                self.partition(topic, *partition)?;
                Ok(data_file(path.to_path(), *format))
            }
            KafkaPath::TopicSnapshot { topic, format } => {
                self.backend.topic(topic)?;
                Ok(data_file(path.to_path(), *format))
            }
        }
    }
}

impl<B: KafkaBackend> FileSystem for KafkaFs<B> {
    fn protocol(&self) -> &[&str] {
        PROTOCOLS
    }

    fn ls(&self, path: &str, _detail: bool) -> FsResult<Vec<FileInfo>> {
        self.ls_kafka(self.resolve(path)?)
    }

    fn info(&self, path: &str) -> FsResult<FileInfo> {
        self.info_kafka(self.resolve(path)?)
    }

    fn open(
        &self,
        path: &str,
        mode: OpenMode,
        _opts: Option<OpenOptions>,
    ) -> FsResult<Box<dyn FsFile>> {
        if mode != OpenMode::Read {
            return Err(FsError::NotSupported(format!(
                "writing to Kafka is not supported yet: {path}"
            )));
        }
        match self.resolve(path)? {
            KafkaPath::Meta(topic, file) => {
                let content = self.meta_content(&topic, file)?;
                let info =
                    FileInfo::file(KafkaPath::Meta(topic, file).to_path(), content.len() as u64);
                Ok(Box::new(MemoryFile {
                    cursor: Cursor::new(content),
                    info,
                }))
            }
            KafkaPath::Root
            | KafkaPath::Topic(_)
            | KafkaPath::MetaDir(_)
            | KafkaPath::Partition(..) => Err(FsError::IsADirectory(path.to_string())),
            path => self.open_data(path),
        }
    }

    fn rm_file(&self, path: &str) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "Kafka topics are append-only: {path}"
        )))
    }

    fn cp_file(&self, src: &str, dst: &str) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "copying within Kafka is not supported: {src} -> {dst}"
        )))
    }

    fn mkdir(&self, path: &str, _create_parents: bool) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "creating topics is not supported yet: {path}"
        )))
    }

    fn rmdir(&self, path: &str) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "deleting topics is not supported yet: {path}"
        )))
    }
}

/// Windows covering `[low, high)`, aligned to multiples of `size`. The last window is
/// open (`end: None`) unless it is full.
pub fn windows(marks: Watermarks, size: i64) -> Vec<Window> {
    if marks.high <= marks.low {
        return Vec::new();
    }
    let mut start = marks.low - marks.low.rem_euclid(size);
    let mut result = Vec::new();
    while start < marks.high {
        let end = start + size - 1;
        result.push(Window {
            start,
            end: (end < marks.high).then_some(end),
        });
        start += size;
    }
    result
}

fn topic_dir(topic: &TopicInfo) -> FileInfo {
    let mut info = FileInfo::directory(KafkaPath::Topic(topic.name.clone()).to_path());
    info.extra
        .insert("partitions".into(), topic.partitions.len().to_string());
    info
}

fn data_file(name: String, format: DataFormat) -> FileInfo {
    let mut info = FileInfo::file(name, 0);
    info.extra.insert("size_known".into(), "false".into());
    info.extra.insert("format".into(), format.as_str().into());
    info
}

fn window_file(
    topic: &str,
    partition: i32,
    window: Window,
    format: DataFormat,
    marks: Watermarks,
) -> FileInfo {
    let path = KafkaPath::Window {
        topic: topic.to_string(),
        partition,
        window,
        format,
    };
    let mut info = data_file(path.to_path(), format);
    // Retention can remove the start of the first window; compaction can leave gaps.
    let first = window.start.max(marks.low);
    let last = window.end.unwrap_or(marks.high - 1).min(marks.high - 1);
    info.extra.insert("start".into(), first.to_string());
    info.extra.insert("end".into(), last.to_string());
    info.extra
        .insert("closed".into(), window.end.is_some().to_string());
    info
}

struct MemoryFile {
    cursor: Cursor<Vec<u8>>,
    info: FileInfo,
}

impl Read for MemoryFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl Write for MemoryFile {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "file is read-only",
        ))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for MemoryFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.cursor.seek(pos)
    }
}

impl FsFile for MemoryFile {
    fn info(&self) -> FsResult<FileInfo> {
        Ok(self.info.clone())
    }

    fn size(&self) -> FsResult<Option<u64>> {
        Ok(Some(self.info.size))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use arrow::array::{Array, AsArray, RecordBatch};
    use arrow::datatypes::Int64Type;
    use arrow::ipc::reader::StreamReader;

    use super::*;
    use crate::backend::{Message, MessageStream, PartitionInfo};
    use crate::read::message_schema;

    struct MockBackend {
        topics: Vec<TopicInfo>,
        marks: HashMap<(String, i32), Watermarks>,
    }

    impl MockBackend {
        fn new() -> Self {
            let partition = |id| PartitionInfo {
                id,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
            };
            let topics = vec![
                TopicInfo {
                    name: "orders".into(),
                    partitions: vec![partition(0), partition(1)],
                },
                TopicInfo {
                    name: "events.csv".into(),
                    partitions: vec![partition(0)],
                },
            ];
            let marks = HashMap::from([
                (("orders".into(), 0), Watermarks { low: 0, high: 25 }),
                (("orders".into(), 1), Watermarks { low: 7, high: 7 }),
                (("events.csv".into(), 0), Watermarks { low: 0, high: 20 }),
            ]);
            Self { topics, marks }
        }
    }

    impl KafkaBackend for MockBackend {
        fn list_topics(&self) -> FsResult<Vec<TopicInfo>> {
            Ok(self.topics.clone())
        }

        fn topic(&self, topic: &str) -> FsResult<TopicInfo> {
            self.topics
                .iter()
                .find(|t| t.name == topic)
                .cloned()
                .ok_or_else(|| FsError::NotFound(topic.into()))
        }

        fn watermarks(&self, topic: &str, partition: i32) -> FsResult<Watermarks> {
            Ok(self.marks[&(topic.to_string(), partition)])
        }

        /// Matches `read`, where the message at `offset` has timestamp `1_000 + offset`.
        fn offset_for_time(
            &self,
            topic: &str,
            partition: i32,
            timestamp_ms: i64,
        ) -> FsResult<Option<i64>> {
            let marks = self.watermarks(topic, partition)?;
            let offset = (timestamp_ms - 1_000).max(marks.low);
            Ok((offset < marks.high).then_some(offset))
        }

        fn topic_config(&self, _topic: &str) -> FsResult<BTreeMap<String, String>> {
            Ok(BTreeMap::from([(
                "retention.ms".into(),
                "604800000".into(),
            )]))
        }

        /// Every offset below the high watermark holds a message.
        fn read(&self, topic: &str, ranges: Vec<PartitionRange>) -> FsResult<MessageStream> {
            let messages = ranges
                .into_iter()
                .flat_map(|range| {
                    (range.start..range.end).map(move |offset| {
                        Ok(Message {
                            partition: range.partition,
                            offset,
                            timestamp_ms: Some(1_000 + offset),
                            key: Some(format!("k{offset}").into_bytes()),
                            headers: vec![("h".into(), Some(b"x".to_vec()))],
                            value: Some(format!("v{offset}").into_bytes()),
                        })
                    })
                })
                .collect::<Vec<_>>();
            assert!(self.topics.iter().any(|t| t.name == topic));
            Ok(Box::new(messages.into_iter()))
        }
    }

    fn fs() -> KafkaFs<MockBackend> {
        KafkaFs::new(
            MockBackend::new(),
            KafkaFsOptions {
                window_size: 10,
                list_format: DataFormat::Arrow,
                batch_size: 4,
            },
        )
        .unwrap()
    }

    fn names(entries: Vec<FileInfo>) -> Vec<String> {
        entries.into_iter().map(|e| e.name).collect()
    }

    #[test]
    fn windows_align_and_leave_head_open() {
        let w = |start, end| Window { start, end };
        assert_eq!(windows(Watermarks { low: 0, high: 0 }, 10), vec![]);
        assert_eq!(
            windows(Watermarks { low: 7, high: 25 }, 10),
            vec![w(0, Some(9)), w(10, Some(19)), w(20, None)]
        );
        assert_eq!(
            windows(Watermarks { low: 10, high: 30 }, 10),
            vec![w(10, Some(19)), w(20, Some(29))]
        );
    }

    #[test]
    fn lists_topics_partitions_and_windows() {
        let fs = fs();
        assert_eq!(names(fs.ls("", true).unwrap()), ["orders", "events.csv"]);
        assert_eq!(
            names(fs.ls("kafka://orders", true).unwrap()),
            ["orders/_meta", "orders/0", "orders/1"]
        );
        assert_eq!(
            names(fs.ls("orders/0", true).unwrap()),
            [
                "orders/0/0-9.arrow",
                "orders/0/10-19.arrow",
                "orders/0/20-.arrow"
            ]
        );
        assert!(fs.ls("orders/1", true).unwrap().is_empty());
        assert_eq!(
            names(fs.ls("orders/_meta", true).unwrap()),
            [
                "orders/_meta/config.json",
                "orders/_meta/partitions.json",
                "orders/_meta/watermarks.json"
            ]
        );
    }

    #[test]
    fn window_info_reports_offsets() {
        let info = fs().info("orders/0/20-.parquet").unwrap();
        assert!(info.is_file());
        assert_eq!(info.extra["start"], "20");
        assert_eq!(info.extra["end"], "24");
        assert_eq!(info.extra["closed"], "false");
        assert_eq!(info.extra["size_known"], "false");
        assert_eq!(info.extra["format"], "parquet");
    }

    #[test]
    fn unknown_paths_are_not_found() {
        let fs = fs();
        for path in [
            "missing",
            "orders/9",
            "orders/0/30-.arrow",
            "orders/0/20-29.arrow",
            "orders/0/5-14.arrow",
            "missing.parquet",
        ] {
            assert!(matches!(fs.info(path), Err(FsError::NotFound(_))), "{path}");
        }
    }

    #[test]
    fn topic_named_like_a_file_resolves_to_directory() {
        let fs = fs();
        assert!(fs.info("events.csv").unwrap().is_dir());
        assert_eq!(
            names(fs.ls("events.csv", true).unwrap()),
            ["events.csv/_meta", "events.csv/0"]
        );
        assert!(fs.info("orders.csv").unwrap().is_file());
    }

    #[test]
    fn meta_files_open_as_json() {
        let fs = fs();
        let read = |path| {
            let mut text = String::new();
            fs.open(path, OpenMode::Read, None)
                .unwrap()
                .read_to_string(&mut text)
                .unwrap();
            serde_json::from_str::<serde_json::Value>(&text).unwrap()
        };
        assert_eq!(
            read("orders/_meta/config.json"),
            json!({"retention.ms": "604800000"})
        );
        assert_eq!(
            read("orders/_meta/watermarks.json"),
            json!([{"partition": 0, "low": 0, "high": 25}, {"partition": 1, "low": 7, "high": 7}])
        );
        assert_eq!(
            read("orders/_meta/partitions.json")[0],
            json!({"partition": 0, "leader": 1, "replicas": [1], "isr": [1]})
        );
        let size = fs.info("orders/_meta/config.json").unwrap().size;
        assert_eq!(
            size,
            fs.cat_file("orders/_meta/config.json", None, None)
                .unwrap()
                .len() as u64
        );
    }

    fn read_bytes(fs: &KafkaFs<MockBackend>, path: &str) -> Vec<u8> {
        let mut data = Vec::new();
        fs.open(path, OpenMode::Read, None)
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        data
    }

    fn read_arrow(fs: &KafkaFs<MockBackend>, path: &str) -> Vec<RecordBatch> {
        StreamReader::try_new(read_bytes(fs, path).as_slice(), None)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn offsets(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("offset")
                    .unwrap()
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[test]
    fn reads_closed_and_head_windows() {
        let fs = fs();
        let batches = read_arrow(&fs, "orders/0/10-19.arrow");
        assert_eq!(offsets(&batches), (10..20).collect::<Vec<_>>());
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [4, 4, 2]
        );
        assert_eq!(batches[0].schema(), message_schema(DataFormat::Arrow));
        assert_eq!(
            offsets(&read_arrow(&fs, "orders/0/20-.arrow")),
            [20, 21, 22, 23, 24]
        );
    }

    #[test]
    fn reads_message_fields() {
        let batch = read_arrow(&fs(), "orders/0/0-9.ipc").remove(0);
        assert_eq!(
            batch
                .column_by_name("topic")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            "orders"
        );
        assert_eq!(
            batch
                .column_by_name("key")
                .unwrap()
                .as_binary::<i32>()
                .value(1),
            b"k1"
        );
        assert_eq!(
            batch
                .column_by_name("value")
                .unwrap()
                .as_binary::<i32>()
                .value(1),
            b"v1"
        );
        let headers = batch.column_by_name("headers").unwrap().as_map();
        assert_eq!(headers.value(0).len(), 1);
        assert_eq!(headers.keys().as_string::<i32>().value(0), "h");
    }

    #[test]
    fn bounded_partition_reads_clamp_to_watermarks() {
        let fs = fs();
        assert_eq!(
            offsets(&read_arrow(&fs, "orders/0.arrow?start=22")),
            [22, 23, 24]
        );
        assert_eq!(
            offsets(&read_arrow(&fs, "orders/0.arrow?start=5&limit=3")),
            [5, 6, 7]
        );
        assert_eq!(offsets(&read_arrow(&fs, "orders/0.arrow?limit=2")), [0, 1]);
        assert!(read_arrow(&fs, "orders/0.arrow?start=99").is_empty());
        assert!(read_arrow(&fs, "orders/1.arrow").is_empty());
    }

    #[test]
    fn time_reads_start_at_the_first_later_message() {
        let fs = fs();
        let at = |ms| {
            KafkaPath::TimeRead {
                topic: "orders".into(),
                partition: 0,
                timestamp_ms: ms,
                format: DataFormat::Arrow,
            }
            .to_path()
        };
        assert_eq!(offsets(&read_arrow(&fs, &at(1_022))), [22, 23, 24]);
        assert_eq!(
            offsets(&read_arrow(&fs, &at(0))),
            (0..25).collect::<Vec<_>>()
        );
        assert!(read_arrow(&fs, &at(1_025)).is_empty());
        assert!(fs.info(&at(0)).unwrap().is_file());
        assert!(matches!(
            fs.info("orders/9/@2026-11-01T00:00:00Z.arrow"),
            Err(FsError::NotFound(_))
        ));
    }

    #[test]
    fn topic_snapshot_reads_every_partition() {
        let batches = read_arrow(&fs(), "orders.arrow");
        assert_eq!(offsets(&batches), (0..25).collect::<Vec<_>>());
    }

    #[test]
    fn csv_omits_headers_and_jsonl_keeps_them() {
        let fs = fs();
        let csv = String::from_utf8(read_bytes(&fs, "orders/0.csv?limit=1")).unwrap();
        assert_eq!(
            csv.lines().next().unwrap(),
            "topic,partition,offset,timestamp,key,value"
        );
        let jsonl = String::from_utf8(read_bytes(&fs, "orders/0.jsonl?limit=1")).unwrap();
        let row: serde_json::Value = serde_json::from_str(jsonl.trim()).unwrap();
        assert_eq!(row["offset"], 0);
        assert!(row["headers"].is_object());
    }

    #[test]
    fn file_size_is_known_after_reading() {
        let fs = fs();
        let mut file = fs
            .open("orders/0/0-9.parquet", OpenMode::Read, None)
            .unwrap();
        assert_eq!(file.size().unwrap(), None);
        let end = file.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(file.size().unwrap(), Some(end));
        assert_eq!(file.info().unwrap().extra["size_known"], "true");
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut magic = [0; 4];
        file.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, b"PAR1");
    }

    #[test]
    fn writes_and_directories_cannot_be_opened() {
        let fs = fs();
        assert!(matches!(
            fs.open("orders/0.jsonl", OpenMode::Write, None),
            Err(FsError::NotSupported(_))
        ));
        assert!(matches!(
            fs.open("orders/0", OpenMode::Read, None),
            Err(FsError::IsADirectory(_))
        ));
        assert!(matches!(
            fs.open("orders/0/30-.arrow", OpenMode::Read, None),
            Err(FsError::NotFound(_))
        ));
    }
}
