use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use fsspec_data::DataFormat;
use fsspec_rs::{FileInfo, FileSystem, FsError, FsFile, FsResult, OpenMode, OpenOptions};
use serde_json::json;

use crate::backend::{
    KafkaBackend, MessageOffset, NewMessage, PartitionRange, TopicInfo, Watermarks,
};
use crate::path::{KafkaPath, MetaFile, Window};
use crate::read::{message_schema, EncodedFile, MessageBatches, ValueFormat};
use crate::records::{fetch, record_file, RecordIndex, RECORDS_TOPIC_CONFIG};
use crate::stream::{MessageTail, StreamStart};
use crate::write::{file_messages, MessageColumns, MessageWriter};

const PROTOCOLS: &[&str] = &["kafka"];

#[derive(Clone, Debug)]
pub struct KafkaFsOptions {
    /// Offsets per window file. Windows start at multiples of this size.
    pub window_size: i64,
    /// Extension used when listing window files. Any supported extension can be opened.
    pub list_format: DataFormat,
    /// Messages per Arrow record batch when reading.
    pub batch_size: usize,
    /// Allows `mkdir` to create topics and `rmdir` to delete them. `mkdir("topic/records")`
    /// creates a compacted topic for the records view.
    pub allow_admin: bool,
    /// Partition count for topics created by `mkdir`. `-1` uses the broker default.
    pub partitions: i32,
    /// Replication factor for topics created by `mkdir`. `-1` uses the broker default.
    pub replication: i32,
    /// Topic configuration for topics created by `mkdir`, such as `retention.ms`.
    pub topic_config: BTreeMap<String, String>,
    /// How message values are read and written.
    pub value_format: ValueFormat,
    /// Columns of appended files that set message keys, partitions, timestamps, and headers.
    pub columns: MessageColumns,
}

impl Default for KafkaFsOptions {
    fn default() -> Self {
        Self {
            window_size: 10_000,
            list_format: DataFormat::Arrow,
            batch_size: 1024,
            allow_admin: false,
            partitions: -1,
            replication: -1,
            topic_config: BTreeMap::new(),
            value_format: ValueFormat::Binary,
            columns: MessageColumns::default(),
        }
    }
}

/// Kafka topics as directories, partitions as directories of fixed offset windows.
pub struct KafkaFs<B> {
    backend: Arc<B>,
    options: KafkaFsOptions,
    /// Record indexes by topic, built on first access to `topic/records`.
    records: Mutex<HashMap<String, RecordIndex>>,
}

impl<B: KafkaBackend> KafkaFs<B> {
    pub fn new(backend: B, options: KafkaFsOptions) -> FsResult<Self> {
        if options.window_size <= 0 || options.batch_size == 0 {
            return Err(FsError::InvalidArgument(
                "window_size and batch_size must be positive".to_string(),
            ));
        }
        if let ValueFormat::Json {
            schema,
            flatten: true,
        } = &options.value_format
        {
            let envelope = message_schema(DataFormat::Arrow, &ValueFormat::Binary);
            if let Some(field) = schema
                .fields()
                .iter()
                .find(|field| envelope.column_with_name(field.name()).is_some())
            {
                return Err(FsError::InvalidArgument(format!(
                    "flattened value field {} conflicts with a message column",
                    field.name()
                )));
            }
        }
        Ok(Self {
            backend: Arc::new(backend),
            options,
            records: Mutex::default(),
        })
    }

    pub fn backend(&self) -> &B {
        self.backend.as_ref()
    }

    /// Follows a topic or one partition from `start`, including messages produced later.
    /// Start offsets are clamped to each partition's watermarks. Partitions added to the
    /// topic afterwards are not followed.
    pub fn stream(&self, path: &str, start: StreamStart) -> FsResult<MessageTail> {
        let (topic, partitions) = match self.resolve(path)? {
            KafkaPath::Topic(topic) => {
                let info = self.backend.topic(&topic)?;
                let ids = info.partitions.iter().map(|p| p.id).collect::<Vec<_>>();
                (topic, ids)
            }
            KafkaPath::Partition(topic, partition) => {
                self.partition(&topic, partition)?;
                (topic, vec![partition])
            }
            _ => {
                return Err(FsError::InvalidArgument(format!(
                    "only topics and partitions can be streamed: {path}"
                )))
            }
        };
        let mut positions = BTreeMap::new();
        for partition in partitions {
            let marks = self.backend.watermarks(&topic, partition)?;
            let offset = match &start {
                StreamStart::Beginning => marks.low,
                StreamStart::End => marks.high,
                StreamStart::Time(timestamp_ms) => self
                    .backend
                    .offset_for_time(&topic, partition, *timestamp_ms)?
                    .unwrap_or(marks.high),
                StreamStart::Offsets(offsets) => {
                    offsets.get(&partition).copied().unwrap_or(marks.low)
                }
            };
            positions.insert(partition, offset.clamp(marks.low, marks.high));
        }
        let starts = positions
            .iter()
            .map(|(&partition, &offset)| MessageOffset { partition, offset })
            .collect();
        let messages = self.backend.follow(&topic, starts)?;
        Ok(MessageTail::new(
            topic,
            messages,
            self.options.value_format.clone(),
            self.options.batch_size,
            positions,
        ))
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

    fn require_admin(&self, path: &str) -> FsResult<()> {
        if self.options.allow_admin {
            Ok(())
        } else {
            Err(FsError::PermissionDenied(format!(
                "creating and deleting topics requires allow_admin: {path}"
            )))
        }
    }

    /// The topic named by a directory path for `mkdir` or `rmdir`. A missing topic named
    /// like a file, such as `events.csv`, parses as a snapshot, so it is mapped back here.
    fn admin_topic(&self, path: &str) -> FsResult<String> {
        self.require_admin(path)?;
        match KafkaPath::parse(path)? {
            KafkaPath::Topic(topic) => Ok(topic),
            KafkaPath::TopicSnapshot { .. } => Ok(self.strip_protocol(path)),
            _ => Err(FsError::NotSupported(format!(
                "only topics can be created or deleted: {path}"
            ))),
        }
    }

    /// Runs `f` on the topic's record index, building it on first use. With `catch_up`, an
    /// existing index first replays messages produced since it was last used.
    fn with_records<T>(
        &self,
        topic: &str,
        catch_up: bool,
        f: impl FnOnce(&mut RecordIndex) -> FsResult<T>,
    ) -> FsResult<T> {
        let mut indexes = self
            .records
            .lock()
            .map_err(|_| FsError::Other("record index lock is poisoned".to_string()))?;
        let index = match indexes.entry(topic.to_string()) {
            Entry::Occupied(entry) => {
                let index = entry.into_mut();
                if catch_up {
                    index.catch_up(self.backend.as_ref(), topic)?;
                }
                index
            }
            Entry::Vacant(entry) => entry.insert(RecordIndex::open(self.backend.as_ref(), topic)?),
        };
        f(index)
    }

    /// Reads the latest value of `key`. A value missing from its indexed offset was removed
    /// by compaction after a newer value or tombstone; catching up finds the newer value, and
    /// if the key still points at the missing offset, the key was deleted.
    fn read_record(&self, topic: &str, key: &str) -> FsResult<(FileInfo, Vec<u8>)> {
        let backend = self.backend.as_ref();
        self.with_records(topic, true, |index| loop {
            let path = KafkaPath::Records {
                topic: topic.to_string(),
                key: key.to_string(),
            };
            let location = index
                .get(key)
                .ok_or_else(|| FsError::NotFound(path.to_path()))?;
            if let Some(value) = fetch(backend, topic, location)? {
                return Ok((record_file(topic, key, location), value));
            }
            index.catch_up(backend, topic)?;
            if index.get(key) == Some(location) {
                index.remove(key);
                return Err(FsError::NotFound(path.to_path()));
            }
        })
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
        let batches = MessageBatches::new(
            &topic,
            messages,
            format,
            self.options.value_format.clone(),
            self.options.batch_size,
        );
        Ok(Box::new(EncodedFile::new(batches, format, info)?))
    }

    /// Appends a file to `topic.format` or `topic/partition.format` as one message per row.
    fn append(&self, path: KafkaPath) -> FsResult<Box<dyn FsFile>> {
        let name = path.to_path();
        let (topic, partition, format) = match path {
            KafkaPath::TopicSnapshot { topic, format } => {
                self.backend.topic(&topic)?;
                (topic, None, format)
            }
            KafkaPath::PartitionRead {
                topic,
                partition,
                format,
                start: None,
                limit: None,
            } => {
                self.partition(&topic, partition)?;
                (topic, Some(partition), format)
            }
            _ => {
                return Err(FsError::NotSupported(format!(
                    "only topic.format and topic/partition.format can be appended to: {name}"
                )))
            }
        };
        let value = self.options.value_format.clone();
        let columns = self.options.columns.clone();
        Ok(Box::new(MessageWriter::new(
            self.backend.clone(),
            topic,
            name,
            Box::new(move |data| file_messages(&data, format, &value, &columns, partition)),
        )))
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
            KafkaPath::Records { topic, key } => self.with_records(&topic, true, |index| {
                if let Some(location) = index.get(&key).filter(|_| !key.is_empty()) {
                    Ok(vec![record_file(&topic, &key, location)])
                } else if key.is_empty() || index.is_dir(&key) {
                    Ok(index.children(&topic, &key))
                } else {
                    Err(FsError::NotFound(
                        KafkaPath::Records {
                            topic: topic.clone(),
                            key: key.clone(),
                        }
                        .to_path(),
                    ))
                }
            }),
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
            KafkaPath::Records { topic, key } => self.with_records(topic, true, |index| {
                if key.is_empty() || index.get(key).is_none() && index.is_dir(key) {
                    Ok(FileInfo::directory(path.to_path()))
                } else {
                    let location = index
                        .get(key)
                        .ok_or_else(|| FsError::NotFound(path.to_path()))?;
                    Ok(record_file(topic, key, location))
                }
            }),
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
        let parsed = self.resolve(path)?;
        if let KafkaPath::Records { topic, key } = &parsed {
            if key.is_empty() {
                return Err(FsError::IsADirectory(path.to_string()));
            }
            return match mode {
                OpenMode::Read => {
                    let (info, value) = self.read_record(topic, key)?;
                    Ok(Box::new(MemoryFile {
                        cursor: Cursor::new(value),
                        info,
                    }))
                }
                OpenMode::Write => {
                    // Check the topic is compacted before accepting the value.
                    self.with_records(topic, false, |_| Ok(()))?;
                    let key = key.clone().into_bytes();
                    Ok(Box::new(MessageWriter::new(
                        self.backend.clone(),
                        topic.clone(),
                        parsed.to_path(),
                        Box::new(move |value| {
                            Ok(vec![NewMessage {
                                key: Some(key),
                                value: Some(value),
                                ..NewMessage::default()
                            }])
                        }),
                    )))
                }
                _ => Err(FsError::NotSupported(format!(
                    "records can only be opened for reading or writing: {path}"
                ))),
            };
        }
        match mode {
            OpenMode::Read => {}
            OpenMode::Append => return self.append(parsed),
            _ => {
                return Err(FsError::NotSupported(format!(
                    "Kafka topics are append-only; open with \"ab\" to append: {path}"
                )))
            }
        }
        match parsed {
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

    /// Produces the value and returns once the broker acknowledges it.
    fn pipe_file(&self, path: &str, data: &[u8]) -> FsResult<()> {
        let mut file = self.open(path, OpenMode::Write, None)?;
        file.write_all(data)?;
        file.commit()
    }

    /// Deletes a record by producing a tombstone for its key.
    fn rm_file(&self, path: &str) -> FsResult<()> {
        match self.resolve(path)? {
            KafkaPath::Records { topic, key } if !key.is_empty() => {
                self.with_records(&topic, true, |index| {
                    index
                        .get(&key)
                        .map(|_| ())
                        .ok_or_else(|| FsError::NotFound(path.to_string()))
                })?;
                self.backend.produce(
                    &topic,
                    vec![NewMessage {
                        key: Some(key.into_bytes()),
                        ..NewMessage::default()
                    }],
                )?;
                Ok(())
            }
            _ => Err(FsError::NotSupported(format!(
                "Kafka topics are append-only: {path}"
            ))),
        }
    }

    fn cp_file(&self, src: &str, dst: &str) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "copying within Kafka is not supported: {src} -> {dst}"
        )))
    }

    /// Creates a topic with the configured partitions, replication, and topic config.
    /// Topics have no parent directories, so `create_parents` has no effect.
    fn mkdir(&self, path: &str, _create_parents: bool) -> FsResult<()> {
        match KafkaPath::parse(path)? {
            KafkaPath::Records { topic, key } if key.is_empty() => {
                self.require_admin(path)?;
                let mut config = self.options.topic_config.clone();
                config.extend(
                    RECORDS_TOPIC_CONFIG.map(|(name, value)| (name.to_string(), value.to_string())),
                );
                return self.backend.create_topic(
                    &topic,
                    self.options.partitions,
                    self.options.replication,
                    &config,
                );
            }
            // Record directories exist while they hold keys, like S3 prefixes.
            KafkaPath::Records { .. } => return Ok(()),
            _ => {}
        }
        let topic = self.admin_topic(path)?;
        self.backend.create_topic(
            &topic,
            self.options.partitions,
            self.options.replication,
            &self.options.topic_config,
        )
    }

    /// Deletes a topic and all of its messages.
    fn rmdir(&self, path: &str) -> FsResult<()> {
        if let KafkaPath::Records { .. } = KafkaPath::parse(path)? {
            return Ok(());
        }
        let topic = self.admin_topic(path)?;
        self.backend.delete_topic(&topic)
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
    use std::collections::HashMap;
    use std::sync::Mutex;

    use arrow::array::{Array, AsArray, RecordBatch};
    use arrow::datatypes::Int64Type;
    use arrow::ipc::reader::StreamReader;

    use super::*;
    use std::time::Duration;

    use crate::backend::{
        LiveMessages, Message, MessageOffset, MessageStream, NewMessage, PartitionInfo,
    };

    type CreatedTopic = (String, i32, i32, BTreeMap<String, String>);

    struct MockBackend {
        topics: Vec<TopicInfo>,
        marks: HashMap<(String, i32), Watermarks>,
        created: Mutex<Vec<CreatedTopic>>,
        deleted: Mutex<Vec<String>>,
        produced: Mutex<Vec<NewMessage>>,
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
            Self {
                topics,
                marks,
                created: Mutex::default(),
                deleted: Mutex::default(),
                produced: Mutex::default(),
            }
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

        fn produce(&self, _topic: &str, messages: Vec<NewMessage>) -> FsResult<Vec<MessageOffset>> {
            let offsets = messages
                .iter()
                .map(|message| MessageOffset {
                    partition: message.partition.unwrap_or(0),
                    offset: 0,
                })
                .collect();
            self.produced.lock().unwrap().extend(messages);
            Ok(offsets)
        }

        fn create_topic(
            &self,
            topic: &str,
            partitions: i32,
            replication: i32,
            config: &BTreeMap<String, String>,
        ) -> FsResult<()> {
            self.created.lock().unwrap().push((
                topic.into(),
                partitions,
                replication,
                config.clone(),
            ));
            Ok(())
        }

        fn delete_topic(&self, topic: &str) -> FsResult<()> {
            self.deleted.lock().unwrap().push(topic.into());
            Ok(())
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

        /// The messages `read` returns up to the high watermark; nothing arrives later.
        fn follow(
            &self,
            topic: &str,
            starts: Vec<MessageOffset>,
        ) -> FsResult<Box<dyn LiveMessages>> {
            let ranges = starts
                .into_iter()
                .map(|start| {
                    let marks = self.watermarks(topic, start.partition)?;
                    Ok(PartitionRange {
                        partition: start.partition,
                        start: start.offset,
                        end: marks.high,
                    })
                })
                .collect::<FsResult<Vec<_>>>()?;
            Ok(Box::new(RecordedMessages(self.read(topic, ranges)?)))
        }
    }

    struct RecordedMessages(MessageStream);

    impl LiveMessages for RecordedMessages {
        fn poll(&mut self, _timeout: Duration) -> FsResult<Option<Message>> {
            self.0.next().transpose()
        }
    }

    fn fs() -> KafkaFs<MockBackend> {
        KafkaFs::new(
            MockBackend::new(),
            KafkaFsOptions {
                window_size: 10,
                list_format: DataFormat::Arrow,
                batch_size: 4,
                ..KafkaFsOptions::default()
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
        assert_eq!(
            batches[0].schema(),
            message_schema(DataFormat::Arrow, &ValueFormat::Binary)
        );
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
    fn topic_admin_requires_allow_admin() {
        let fs = fs();
        assert!(matches!(
            fs.mkdir("new", false),
            Err(FsError::PermissionDenied(_))
        ));
        assert!(matches!(
            fs.rmdir("orders"),
            Err(FsError::PermissionDenied(_))
        ));
        assert!(fs.backend().created.lock().unwrap().is_empty());
        assert!(fs.backend().deleted.lock().unwrap().is_empty());
    }

    #[test]
    fn mkdir_and_rmdir_create_and_delete_topics() {
        let config = BTreeMap::from([("retention.ms".to_string(), "1000".to_string())]);
        let fs = KafkaFs::new(
            MockBackend::new(),
            KafkaFsOptions {
                allow_admin: true,
                partitions: 3,
                topic_config: config.clone(),
                ..KafkaFsOptions::default()
            },
        )
        .unwrap();
        fs.mkdir("kafka://new/", true).unwrap();
        fs.makedirs("logs.csv", false).unwrap();
        fs.rmdir("orders").unwrap();
        assert_eq!(
            *fs.backend().created.lock().unwrap(),
            [
                ("new".to_string(), 3, -1, config.clone()),
                ("logs.csv".to_string(), 3, -1, config)
            ]
        );
        assert_eq!(*fs.backend().deleted.lock().unwrap(), ["orders"]);
        for path in ["", "orders/0", "orders/_meta", "orders/0/0-9.arrow"] {
            assert!(
                matches!(fs.mkdir(path, true), Err(FsError::NotSupported(_))),
                "{path}"
            );
        }
    }

    fn stream_offsets(fs: &KafkaFs<MockBackend>, path: &str, start: StreamStart) -> Vec<i64> {
        let mut tail = fs.stream(path, start).unwrap();
        let mut offsets = Vec::new();
        while let Some(batch) = tail.poll(Duration::ZERO).unwrap() {
            assert_eq!(batch.schema(), tail.schema());
            offsets.extend(batch.column(2).as_primitive::<Int64Type>().values());
        }
        offsets
    }

    #[test]
    fn streams_start_where_asked() {
        let fs = fs();
        assert_eq!(
            stream_offsets(&fs, "orders/0", StreamStart::Beginning),
            (0..25).collect::<Vec<_>>()
        );
        assert!(stream_offsets(&fs, "orders", StreamStart::End).is_empty());
        assert_eq!(
            stream_offsets(&fs, "kafka://orders/0", StreamStart::Time(1_020)),
            (20..25).collect::<Vec<_>>()
        );
        // Offsets are clamped to the watermarks, and missing partitions start at the beginning.
        assert_eq!(
            stream_offsets(
                &fs,
                "orders",
                StreamStart::Offsets(BTreeMap::from([(0, 23), (1, 100)]))
            ),
            vec![23, 24]
        );
        assert_eq!(
            stream_offsets(&fs, "events.csv", StreamStart::Offsets(BTreeMap::new())),
            (0..20).collect::<Vec<_>>()
        );
    }

    #[test]
    fn stream_positions_follow_returned_messages() {
        let fs = fs();
        let mut tail = fs.stream("orders", StreamStart::Time(1_010)).unwrap();
        assert_eq!(tail.positions(), &BTreeMap::from([(0, 10), (1, 7)]));
        // Batches hold at most `batch_size` messages.
        let batch = tail.poll(Duration::ZERO).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(tail.positions(), &BTreeMap::from([(0, 14), (1, 7)]));
        while tail.poll(Duration::ZERO).unwrap().is_some() {}
        assert_eq!(tail.positions(), &BTreeMap::from([(0, 25), (1, 7)]));
    }

    #[test]
    fn only_topics_and_partitions_stream() {
        let fs = fs();
        for path in ["", "orders/_meta", "orders/0/0-10.arrow", "orders/records"] {
            assert!(matches!(
                fs.stream(path, StreamStart::End),
                Err(FsError::InvalidArgument(_))
            ));
        }
        assert!(matches!(
            fs.stream("orders/5", StreamStart::End),
            Err(FsError::NotFound(_))
        ));
        assert!(matches!(
            fs.stream("missing", StreamStart::End),
            Err(FsError::NotFound(_))
        ));
    }

    #[test]
    fn flattened_values_cannot_shadow_message_columns() {
        let options = |name| KafkaFsOptions {
            value_format: ValueFormat::Json {
                schema: Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true),
                ])),
                flatten: true,
            },
            ..KafkaFsOptions::default()
        };
        assert!(matches!(
            KafkaFs::new(MockBackend::new(), options("offset")),
            Err(FsError::InvalidArgument(_))
        ));
        assert!(KafkaFs::new(MockBackend::new(), options("id")).is_ok());
    }

    #[test]
    fn appends_files_as_messages() {
        let fs = KafkaFs::new(
            MockBackend::new(),
            KafkaFsOptions {
                columns: MessageColumns {
                    key: Some("id".into()),
                    ..MessageColumns::default()
                },
                ..KafkaFsOptions::default()
            },
        )
        .unwrap();
        let append = |path, data: &[u8]| {
            let mut file = fs.open(path, OpenMode::Append, None).unwrap();
            file.write_all(data).unwrap();
            file.commit().unwrap();
        };
        append("orders.csv", b"value,id\nx,a\ny,b\n");
        append(
            "kafka://orders/1.jsonl",
            b"{\"id\": \"c\", \"value\": \"z\"}\n",
        );
        let message = |partition, key: &str, value: &str| NewMessage {
            partition,
            key: Some(key.into()),
            value: Some(value.into()),
            ..NewMessage::default()
        };
        assert_eq!(
            *fs.backend().produced.lock().unwrap(),
            [
                message(None, "a", "x"),
                message(None, "b", "y"),
                message(Some(1), "c", "z"),
            ]
        );

        for path in [
            "orders/0/0-9.arrow",
            "orders/0.arrow?start=3",
            "orders/_meta/config.json",
        ] {
            assert!(matches!(
                fs.open(path, OpenMode::Append, None),
                Err(FsError::NotSupported(_))
            ));
        }
        for path in ["missing.csv", "orders/5.csv"] {
            assert!(matches!(
                fs.open(path, OpenMode::Append, None),
                Err(FsError::NotFound(_))
            ));
        }
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
