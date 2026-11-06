use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use fsspec_data::DataFormat;
use fsspec_rs::{FileInfo, FileSystem, FsError, FsFile, FsResult, OpenMode, OpenOptions};
use serde_json::json;

use crate::backend::{KafkaBackend, TopicInfo, Watermarks};
use crate::path::{KafkaPath, MetaFile, Window};

const PROTOCOLS: &[&str] = &["kafka"];

#[derive(Clone, Debug)]
pub struct KafkaFsOptions {
    /// Offsets per window file. Windows start at multiples of this size.
    pub window_size: i64,
    /// Extension used when listing window files. Any supported extension can be opened.
    pub list_format: DataFormat,
}

impl Default for KafkaFsOptions {
    fn default() -> Self {
        Self {
            window_size: 10_000,
            list_format: DataFormat::Arrow,
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
        if options.window_size <= 0 {
            return Err(FsError::InvalidArgument(
                "window_size must be positive".to_string(),
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
            _ => Err(FsError::NotSupported(format!(
                "reading Kafka messages is not supported yet: {path}"
            ))),
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

    use super::*;
    use crate::backend::PartitionInfo;

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

        fn offset_for_time(&self, _: &str, _: i32, _: i64) -> FsResult<Option<i64>> {
            Ok(None)
        }

        fn topic_config(&self, _topic: &str) -> FsResult<BTreeMap<String, String>> {
            Ok(BTreeMap::from([(
                "retention.ms".into(),
                "604800000".into(),
            )]))
        }
    }

    fn fs() -> KafkaFs<MockBackend> {
        KafkaFs::new(
            MockBackend::new(),
            KafkaFsOptions {
                window_size: 10,
                list_format: DataFormat::Arrow,
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

    #[test]
    fn data_and_writes_are_not_supported_yet() {
        let fs = fs();
        assert!(matches!(
            fs.open("orders/0/0-9.arrow", OpenMode::Read, None),
            Err(FsError::NotSupported(_))
        ));
        assert!(matches!(
            fs.open("orders/0.jsonl", OpenMode::Write, None),
            Err(FsError::NotSupported(_))
        ));
        assert!(matches!(
            fs.open("orders/0", OpenMode::Read, None),
            Err(FsError::IsADirectory(_))
        ));
    }
}
