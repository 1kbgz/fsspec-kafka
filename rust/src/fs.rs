use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use fsspec_data::DataFormat;
use fsspec_rs::{FileInfo, FileSystem, FsError, FsFile, FsResult, OpenMode, OpenOptions};
use serde_json::{json, Value};

use crate::backend::{
    KafkaBackend, Message, MessageOffset, MessageStream, NewMessage, PartitionInfo, PartitionRange,
    TopicInfo, Watermarks,
};
use crate::path::{Bucket, KafkaPath, KafkaView, TimeLevel, Window};
use crate::read::{message_schema, EncodedFile, MessageBatches, ValueFormat};
use crate::records::{fetch, RecordIndex, RecordLocation, RECORDS_TOPIC_CONFIG};
use crate::stream::{MessageTail, StreamStart};
use crate::write::{file_messages, MessageColumns, MessageWriter};

#[derive(Clone, Debug)]
pub struct KafkaFsOptions {
    /// How topics are arranged, which also sets the protocol.
    pub view: KafkaView,
    /// The deepest level of `kafka+time` buckets. Buckets at this level are files.
    pub time_bucket: TimeLevel,
    /// Offsets per window file. Windows start at multiples of this size.
    pub window_size: i64,
    /// Extension used when listing message files, and the encoding of `kafka+history` keys.
    /// Any supported extension can be opened.
    pub list_format: DataFormat,
    /// Messages per Arrow record batch when reading.
    pub batch_size: usize,
    /// Allows `mkdir` to create topics and `rmdir` to delete them. In `kafka+records`,
    /// `mkdir` creates a compacted topic.
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
            view: KafkaView::Offset,
            time_bucket: TimeLevel::Millisecond,
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

/// Kafka topics as directories, arranged by the view in [`KafkaFsOptions::view`]: partitions
/// of fixed offset windows, time buckets, or keys.
pub struct KafkaFs<B> {
    backend: Arc<B>,
    options: KafkaFsOptions,
    /// Key indexes by topic, built on first access to a topic's keys.
    records: Mutex<HashMap<String, RecordIndex>>,
}

impl<B: KafkaBackend> KafkaFs<B> {
    pub fn new(backend: B, options: KafkaFsOptions) -> FsResult<Self> {
        if options.window_size <= 0 || options.batch_size == 0 {
            return Err(FsError::InvalidArgument(
                "window_size and batch_size must be positive".to_string(),
            ));
        }
        if let Some((schema, true)) = options.value_format.decoded() {
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

    /// Follows a topic, or in `kafka+offset` one partition, from `start`, including messages
    /// produced later. Start offsets are clamped to each partition's watermarks. Partitions
    /// added to the topic afterwards are not followed.
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

    /// The first offset of each partition of a topic, or of one `kafka+offset` partition,
    /// whose timestamp is at or after `timestamp_ms`. Partitions without such a message map to
    /// their high watermark. Offsets come from the broker's time index.
    pub fn offsets_for_time(&self, path: &str, timestamp_ms: i64) -> FsResult<BTreeMap<i32, i64>> {
        let (topic, partitions) = match self.resolve(path)? {
            KafkaPath::Topic(topic) => {
                let ids = self
                    .backend
                    .topic(&topic)?
                    .partitions
                    .iter()
                    .map(|p| p.id)
                    .collect();
                (topic, ids)
            }
            KafkaPath::Partition(topic, partition) => {
                self.partition(&topic, partition)?;
                (topic, vec![partition])
            }
            path => {
                return Err(FsError::InvalidArgument(format!(
                    "only topics and partitions have offsets: {}",
                    path.to_path()
                )))
            }
        };
        partitions
            .into_iter()
            .map(|partition| {
                let marks = self.backend.watermarks(&topic, partition)?;
                let offset = self
                    .backend
                    .offset_for_time(&topic, partition, timestamp_ms)?
                    .map_or(marks.high, |offset| offset.clamp(marks.low, marks.high));
                Ok((partition, offset))
            })
            .collect()
    }

    /// Metadata of a topic or `kafka+offset` partition: leaders, replicas, in-sync replicas,
    /// and watermarks, plus the configuration of a topic.
    pub fn metadata(&self, path: &str) -> FsResult<Value> {
        match self.resolve(path)? {
            KafkaPath::Topic(topic) => {
                let info = self.backend.topic(&topic)?;
                let partitions = info
                    .partitions
                    .iter()
                    .map(|p| self.partition_metadata(&topic, p))
                    .collect::<FsResult<Vec<_>>>()?;
                Ok(json!({
                    "partitions": partitions,
                    "config": self.backend.topic_config(&topic)?,
                }))
            }
            KafkaPath::Partition(topic, partition) => {
                let info = self.partition(&topic, partition)?;
                let partition = info
                    .partitions
                    .iter()
                    .find(|p| p.id == partition)
                    .expect("partition was found");
                self.partition_metadata(&topic, partition)
            }
            path => Err(FsError::NotSupported(format!(
                "only topics and partitions have metadata: {}",
                path.to_path()
            ))),
        }
    }

    fn partition_metadata(&self, topic: &str, partition: &PartitionInfo) -> FsResult<Value> {
        let marks = self.backend.watermarks(topic, partition.id)?;
        Ok(json!({
            "partition": partition.id,
            "leader": partition.leader,
            "replicas": partition.replicas,
            "isr": partition.isr,
            "low": marks.low,
            "high": marks.high,
        }))
    }

    /// Parses `path`, treating `name.format` as a topic directory when a topic has that
    /// exact name.
    fn resolve(&self, path: &str) -> FsResult<KafkaPath> {
        let parsed = KafkaPath::parse(path, self.options.view)?;
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
        match KafkaPath::parse(path, self.options.view)? {
            KafkaPath::Topic(topic) => Ok(topic),
            KafkaPath::TopicSnapshot { .. } => Ok(self.strip_protocol(path)),
            _ => Err(FsError::NotSupported(format!(
                "only topics can be created or deleted: {path}"
            ))),
        }
    }

    /// Runs `f` on the topic's key index, building it on first use. With `catch_up`, an
    /// existing index first replays messages produced since it was last used.
    fn with_keys<T>(
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
            Entry::Vacant(entry) => entry.insert(match self.options.view {
                KafkaView::History => RecordIndex::open_history(self.backend.as_ref(), topic)?,
                _ => RecordIndex::open(self.backend.as_ref(), topic)?,
            }),
        };
        f(index)
    }

    /// Reads the latest value of `key`. A value missing from its indexed offset was removed
    /// by compaction after a newer value or tombstone; catching up finds the newer value, and
    /// if the key still points at the missing offset, the key was deleted.
    fn read_record(&self, topic: &str, key: &str) -> FsResult<(FileInfo, Vec<u8>)> {
        let backend = self.backend.as_ref();
        self.with_keys(topic, true, |index| loop {
            let location = index
                .get(key)
                .ok_or_else(|| FsError::NotFound(self.key_path(topic, key)))?;
            if let Some(value) = fetch(backend, topic, location)? {
                return Ok((self.key_file(topic, key, location), value));
            }
            index.catch_up(backend, topic)?;
            if index.get(key) == Some(location) {
                index.remove(key);
                return Err(FsError::NotFound(self.key_path(topic, key)));
            }
        })
    }

    fn key_path(&self, topic: &str, key: &str) -> String {
        format!("{topic}/{key}")
    }

    fn key_file(&self, topic: &str, key: &str, location: RecordLocation) -> FileInfo {
        let path = self.key_path(topic, key);
        let mut info = match self.options.view {
            KafkaView::History => {
                let mut info = data_file(path, self.options.list_format);
                info.extra.insert("kind".into(), "history".into());
                info
            }
            _ => {
                let mut info = FileInfo::file(path, location.size);
                info.extra.insert("kind".into(), "record".into());
                info
            }
        };
        info.extra
            .insert("partition".into(), location.partition.to_string());
        info.extra
            .insert("offset".into(), location.offset.to_string());
        info
    }

    fn key_dir(&self, topic: &str, key: &str) -> FileInfo {
        let mut info = FileInfo::directory(self.key_path(topic, key));
        info.extra.insert("kind".into(), "keys".into());
        info
    }

    /// Files and directories of keys under `key`; `key` itself when it names a key.
    fn ls_keys(&self, topic: &str, key: &str) -> FsResult<Vec<FileInfo>> {
        self.with_keys(topic, true, |index| {
            if let Some(location) = index.get(key).filter(|_| !key.is_empty()) {
                Ok(vec![self.key_file(topic, key, location)])
            } else if key.is_empty() || index.is_dir(key) {
                Ok(index
                    .children(key)
                    .into_iter()
                    .map(|(child, location)| match location {
                        Some(location) => self.key_file(topic, &child, location),
                        None => self.key_dir(topic, &child),
                    })
                    .collect())
            } else {
                Err(FsError::NotFound(self.key_path(topic, key)))
            }
        })
    }

    fn info_key(&self, topic: &str, key: &str) -> FsResult<FileInfo> {
        self.with_keys(topic, true, |index| match index.get(key) {
            Some(location) => Ok(self.key_file(topic, key, location)),
            None if index.is_dir(key) => Ok(self.key_dir(topic, key)),
            None => Err(FsError::NotFound(self.key_path(topic, key))),
        })
    }

    fn partition(&self, topic: &str, partition: i32) -> FsResult<TopicInfo> {
        let info = self.backend.topic(topic)?;
        if !info.partitions.iter().any(|p| p.id == partition) {
            return Err(FsError::NotFound(format!("{topic}/{partition}")));
        }
        Ok(info)
    }

    /// Every retained offset of every partition.
    fn topic_ranges(&self, topic: &str) -> FsResult<Vec<PartitionRange>> {
        self.backend
            .topic(topic)?
            .partitions
            .iter()
            .map(|p| {
                let marks = self.backend.watermarks(topic, p.id)?;
                Ok(PartitionRange {
                    partition: p.id,
                    start: marks.low,
                    end: marks.high,
                })
            })
            .collect()
    }

    /// Offsets that may hold messages with timestamps in `start_ms..end_ms`. Offsets come
    /// from the broker's time index, so they assume timestamps increase with offsets.
    fn time_ranges(
        &self,
        topic: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> FsResult<Vec<PartitionRange>> {
        let mut ranges = Vec::new();
        for partition in self.backend.topic(topic)?.partitions {
            let marks = self.backend.watermarks(topic, partition.id)?;
            let Some(start) = self
                .backend
                .offset_for_time(topic, partition.id, start_ms)?
            else {
                continue;
            };
            let end = self
                .backend
                .offset_for_time(topic, partition.id, end_ms)?
                .unwrap_or(marks.high);
            if start < end {
                ranges.push(PartitionRange {
                    partition: partition.id,
                    start: start.max(marks.low),
                    end: end.min(marks.high),
                });
            }
        }
        Ok(ranges)
    }

    /// The timestamps of the first and last retained messages of the topic, read from the
    /// start and end of each partition.
    fn time_bounds(&self, topic: &str) -> FsResult<Option<(i64, i64)>> {
        let mut bounds: Option<(i64, i64)> = None;
        for range in self.topic_ranges(topic)? {
            if range.start >= range.end {
                continue;
            }
            let first = self
                .backend
                .read(topic, vec![range])?
                .find_map(|message| message.map(|m| m.timestamp_ms).transpose())
                .transpose()?;
            // The last offsets may be transaction markers or compacted away.
            let mut last = None;
            let mut span = 1;
            while last.is_none() {
                let start = (range.end - span).max(range.start);
                for message in self
                    .backend
                    .read(topic, vec![PartitionRange { start, ..range }])?
                {
                    if let Some(timestamp) = message?.timestamp_ms {
                        last = Some(timestamp);
                    }
                }
                if start == range.start {
                    break;
                }
                span *= 16;
            }
            if let (Some(first), Some(last)) = (first, last) {
                bounds = Some(match bounds {
                    Some((low, high)) => (low.min(first), high.max(last)),
                    None => (first, last),
                });
            }
        }
        Ok(bounds)
    }

    /// Buckets under `parent` (the topic for years, or the weeks directory) that overlap the
    /// topic's messages.
    fn ls_buckets(
        &self,
        topic: &str,
        parent: Option<&Bucket>,
        weeks: bool,
    ) -> FsResult<Vec<FileInfo>> {
        let Some((first, last)) = self.time_bounds(topic)? else {
            return Ok(Vec::new());
        };
        let (low, high) = match parent {
            Some(parent) => (first.max(parent.start_ms()), last.min(parent.end_ms() - 1)),
            None => (first, last),
        };
        let at = |timestamp| match (parent, weeks) {
            (Some(parent), _) => parent.child_at(timestamp),
            (None, true) => Bucket::week_at(timestamp),
            (None, false) => Bucket::year_at(timestamp),
        };
        let mut entries = Vec::new();
        let mut timestamp = low;
        while timestamp <= high {
            let Some(bucket) = at(timestamp) else { break };
            timestamp = bucket.end_ms();
            entries.push(self.bucket_info(topic, bucket, None, true));
        }
        Ok(entries)
    }

    /// A bucket as listed: a directory above the configured `time_bucket`, otherwise a file
    /// in `list_format`. With `listed` false, `format` says which one was asked for.
    fn bucket_info(
        &self,
        topic: &str,
        bucket: Bucket,
        format: Option<DataFormat>,
        listed: bool,
    ) -> FileInfo {
        let leaf = bucket
            .level()
            .is_none_or(|level| level >= self.options.time_bucket);
        let format = if listed && leaf {
            Some(self.options.list_format)
        } else {
            format
        };
        let (start, end) = (bucket.start_ms(), bucket.end_ms());
        let path = KafkaPath::Bucket {
            topic: topic.to_string(),
            bucket,
            format,
        }
        .to_path();
        let mut info = match format {
            Some(format) => data_file(path, format),
            None => FileInfo::directory(path),
        };
        info.extra.insert("kind".into(), "bucket".into());
        info.extra.insert("start".into(), rfc3339(start));
        info.extra.insert("end".into(), rfc3339(end));
        info
    }

    /// Opens a message file. Offsets are bounded by the watermarks read here, so messages
    /// produced after opening are not included.
    fn open_data(&self, path: KafkaPath) -> FsResult<Box<dyn FsFile>> {
        let info = self.info_kafka(path.clone())?;
        let (topic, format, messages): (String, DataFormat, MessageStream) = match path {
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
                let messages = self.backend.read(&topic, vec![range])?;
                (topic, format, messages)
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
                let range = PartitionRange {
                    partition,
                    start,
                    end,
                };
                let messages = self.backend.read(&topic, vec![range])?;
                (topic, format, messages)
            }
            KafkaPath::TopicSnapshot { topic, format }
                if self.options.view == KafkaView::Records =>
            {
                let messages = self.latest_records(&topic)?;
                (topic, format, Box::new(messages.into_iter().map(Ok)))
            }
            KafkaPath::TopicSnapshot { topic, format } => {
                let messages = self.backend.read(&topic, self.topic_ranges(&topic)?)?;
                (topic, format, messages)
            }
            KafkaPath::Bucket {
                topic,
                bucket,
                format: Some(format),
            } => {
                let (start, end) = (bucket.start_ms(), bucket.end_ms());
                let messages = self
                    .backend
                    .read(&topic, self.time_ranges(&topic, start, end)?)?
                    .filter(move |message| match message {
                        Ok(message) => message
                            .timestamp_ms
                            .is_some_and(|timestamp| (start..end).contains(&timestamp)),
                        Err(_) => true,
                    });
                (topic, format, Box::new(messages))
            }
            KafkaPath::History { topic, key } => {
                let key = key.into_bytes();
                let messages = self
                    .backend
                    .read(&topic, self.topic_ranges(&topic)?)?
                    .filter(move |message| match message {
                        Ok(message) => message.key.as_deref() == Some(key.as_slice()),
                        Err(_) => true,
                    });
                (topic, self.options.list_format, Box::new(messages))
            }
            path => return Err(FsError::IsADirectory(path.to_path())),
        };
        let batches = MessageBatches::new(
            &topic,
            messages,
            format,
            self.options.value_format.clone(),
            self.options.batch_size,
        );
        Ok(Box::new(EncodedFile::new(batches, format, info)?))
    }

    /// The latest message of each key with a value, in offset order within each partition.
    fn latest_records(&self, topic: &str) -> FsResult<Vec<Message>> {
        // Checks that the topic is compacted.
        self.with_keys(topic, false, |_| Ok(()))?;
        let mut latest = HashMap::new();
        for message in self.backend.read(topic, self.topic_ranges(topic)?)? {
            let message = message?;
            let Some(key) = message.key.clone() else {
                continue;
            };
            if message.value.is_some() {
                latest.insert(key, message);
            } else {
                latest.remove(&key);
            }
        }
        let mut messages = latest.into_values().collect::<Vec<_>>();
        messages.sort_by_key(|message| (message.partition, message.offset));
        Ok(messages)
    }

    /// Appends a file to `topic.format`, or in `kafka+offset` to `topic/partition.format`, as
    /// one message per row.
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
            KafkaPath::Topic(topic) => match self.options.view {
                KafkaView::Offset => {
                    let info = self.backend.topic(&topic)?;
                    Ok(info
                        .partitions
                        .iter()
                        .map(|p| partition_dir(&topic, p.id))
                        .collect())
                }
                KafkaView::Time => {
                    self.backend.topic(&topic)?;
                    let mut entries = vec![weeks_dir(&topic)];
                    entries.extend(self.ls_buckets(&topic, None, false)?);
                    Ok(entries)
                }
                KafkaView::Records | KafkaView::History => self.ls_keys(&topic, ""),
            },
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
            KafkaPath::Weeks(topic) => {
                self.backend.topic(&topic)?;
                self.ls_buckets(&topic, None, true)
            }
            KafkaPath::Bucket {
                ref topic,
                ref bucket,
                format: None,
            } => {
                let info = self.info_kafka(path.clone())?;
                if !info.is_dir() {
                    return Ok(vec![info]);
                }
                self.ls_buckets(topic, Some(bucket), false)
            }
            KafkaPath::Records { topic, key } | KafkaPath::History { topic, key } => {
                self.ls_keys(&topic, &key)
            }
            path => Err(FsError::NotADirectory(path.to_path())),
        }
    }

    fn info_kafka(&self, path: KafkaPath) -> FsResult<FileInfo> {
        match &path {
            KafkaPath::Root => Ok(FileInfo::directory("")),
            KafkaPath::Topic(topic) => Ok(topic_dir(&self.backend.topic(topic)?)),
            KafkaPath::Partition(topic, partition) => {
                self.partition(topic, *partition)?;
                let marks = self.backend.watermarks(topic, *partition)?;
                let mut info = partition_dir(topic, *partition);
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
            KafkaPath::Weeks(topic) => {
                self.backend.topic(topic)?;
                Ok(weeks_dir(topic))
            }
            KafkaPath::Bucket {
                topic,
                bucket,
                format,
            } => {
                self.backend.topic(topic)?;
                let too_deep = bucket
                    .level()
                    .is_some_and(|level| level > self.options.time_bucket);
                let leaf = bucket
                    .level()
                    .is_none_or(|level| level == self.options.time_bucket);
                // Leaf buckets are files, so they have no directory form.
                if too_deep || leaf && format.is_none() {
                    return Err(FsError::NotFound(path.to_path()));
                }
                Ok(self.bucket_info(topic, bucket.clone(), *format, false))
            }
            KafkaPath::Records { topic, key } | KafkaPath::History { topic, key } => {
                self.info_key(topic, key)
            }
        }
    }
}

impl<B: KafkaBackend> FileSystem for KafkaFs<B> {
    fn protocol(&self) -> &[&str] {
        self.options.view.protocols()
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
                    self.with_keys(topic, false, |_| Ok(()))?;
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
            KafkaPath::Root
            | KafkaPath::Topic(_)
            | KafkaPath::Partition(..)
            | KafkaPath::Weeks(_)
            | KafkaPath::Bucket { format: None, .. } => {
                self.info_kafka(parsed)?;
                Err(FsError::IsADirectory(path.to_string()))
            }
            KafkaPath::History { ref topic, ref key } => {
                if self.info_key(topic, key)?.is_dir() {
                    return Err(FsError::IsADirectory(path.to_string()));
                }
                self.open_data(parsed)
            }
            path => self.open_data(path),
        }
    }

    /// Data files have no size until they are fully encoded, so the range is taken from the
    /// whole file.
    fn cat_file(&self, path: &str, start: Option<i64>, end: Option<i64>) -> FsResult<Vec<u8>> {
        let mut data = Vec::new();
        self.open(path, OpenMode::Read, None)?
            .read_to_end(&mut data)?;
        let len = data.len() as i64;
        let bound = |index: i64| (if index < 0 { len + index } else { index }).clamp(0, len);
        let start = start.map_or(0, bound) as usize;
        let end = (end.map_or(len, bound) as usize).max(start);
        Ok(data[start..end].to_vec())
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
            KafkaPath::Records { topic, key } => {
                self.with_keys(&topic, true, |index| {
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

    /// Creates a topic with the configured partitions, replication, and topic config. In
    /// `kafka+records`, the topic is compacted. Topics have no parent directories, so
    /// `create_parents` has no effect.
    fn mkdir(&self, path: &str, _create_parents: bool) -> FsResult<()> {
        // Record directories exist while they hold keys, like S3 prefixes.
        if let KafkaPath::Records { .. } = KafkaPath::parse(path, self.options.view)? {
            return Ok(());
        }
        let topic = self.admin_topic(path)?;
        let mut config = self.options.topic_config.clone();
        if self.options.view == KafkaView::Records {
            config.extend(
                RECORDS_TOPIC_CONFIG.map(|(name, value)| (name.to_string(), value.to_string())),
            );
        }
        self.backend.create_topic(
            &topic,
            self.options.partitions,
            self.options.replication,
            &config,
        )
    }

    /// Deletes a topic and all of its messages.
    fn rmdir(&self, path: &str) -> FsResult<()> {
        if let KafkaPath::Records { .. } = KafkaPath::parse(path, self.options.view)? {
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

fn rfc3339(timestamp_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_default()
}

fn topic_dir(topic: &TopicInfo) -> FileInfo {
    let mut info = FileInfo::directory(KafkaPath::Topic(topic.name.clone()).to_path());
    info.extra
        .insert("partitions".into(), topic.partitions.len().to_string());
    info.extra.insert("kind".into(), "topic".into());
    info
}

fn partition_dir(topic: &str, partition: i32) -> FileInfo {
    let mut info =
        FileInfo::directory(KafkaPath::Partition(topic.to_string(), partition).to_path());
    info.extra.insert("partition".into(), partition.to_string());
    info.extra.insert("kind".into(), "partition".into());
    info
}

fn weeks_dir(topic: &str) -> FileInfo {
    let mut info = FileInfo::directory(KafkaPath::Weeks(topic.to_string()).to_path());
    info.extra.insert("kind".into(), "weeks".into());
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
    info.extra.insert("kind".into(), "window".into());
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
            ["orders/0", "orders/1"]
        );
        assert_eq!(fs.ls("orders", true).unwrap()[1].extra["partition"], "1");
        assert_eq!(fs.info("orders").unwrap().extra["kind"], "topic");
        let info = fs.info("kafka+offset://orders/1").unwrap();
        assert_eq!(
            (
                &info.extra["partition"],
                &info.extra["low"],
                &info.extra["high"],
                &info.extra["kind"],
            ),
            (
                &"1".to_string(),
                &"7".to_string(),
                &"7".to_string(),
                &"partition".to_string()
            )
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
        assert_eq!(info.extra["kind"], "window");
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
        assert_eq!(names(fs.ls("events.csv", true).unwrap()), ["events.csv/0"]);
        assert!(fs.info("orders.csv").unwrap().is_file());
    }

    #[test]
    fn offsets_for_time_cover_each_partition() {
        let fs = fs();
        assert_eq!(
            fs.offsets_for_time("orders", 1_022).unwrap(),
            BTreeMap::from([(0, 22), (1, 7)])
        );
        assert_eq!(
            fs.offsets_for_time("kafka://orders/0", 9_999).unwrap(),
            BTreeMap::from([(0, 25)])
        );
        assert!(matches!(
            fs.offsets_for_time("orders/0/0-9.arrow", 0),
            Err(FsError::InvalidArgument(_))
        ));
    }

    #[test]
    fn topics_and_partitions_have_metadata() {
        let fs = fs();
        let partition =
            json!({"partition": 0, "leader": 1, "replicas": [1], "isr": [1], "low": 0, "high": 25});
        assert_eq!(fs.metadata("orders/0").unwrap(), partition);
        let topic = fs.metadata("kafka://orders").unwrap();
        assert_eq!(topic["config"], json!({"retention.ms": "604800000"}));
        assert_eq!(topic["partitions"][0], partition);
        assert_eq!(topic["partitions"][1]["low"], 7);
        assert!(matches!(
            fs.metadata("orders/0/0-9.arrow"),
            Err(FsError::NotSupported(_))
        ));
        assert!(matches!(fs.metadata("orders/9"), Err(FsError::NotFound(_))));
    }

    #[test]
    fn cat_file_reads_data_files_of_unknown_size() {
        let fs = fs();
        let data = read_bytes(&fs, "orders/0/10-19.arrow");
        assert!(!data.is_empty());
        assert_eq!(
            fs.cat_file("orders/0/10-19.arrow", None, None).unwrap(),
            data
        );
        assert_eq!(
            fs.cat_file("orders/0/10-19.arrow", Some(2), Some(-3))
                .unwrap(),
            data[2..data.len() - 3]
        );
        assert!(fs
            .cat_file("orders/0/10-19.arrow", Some(-1), Some(1))
            .unwrap()
            .is_empty());
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

    fn time_fs(time_bucket: TimeLevel) -> KafkaFs<MockBackend> {
        KafkaFs::new(
            MockBackend::new(),
            KafkaFsOptions {
                view: KafkaView::Time,
                time_bucket,
                batch_size: 4,
                ..KafkaFsOptions::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn time_buckets_list_down_to_the_bucket_size() {
        // The mock's messages are at 1970-01-01T00:00:01.000Z plus their offset in milliseconds.
        let fs = time_fs(TimeLevel::Millisecond);
        assert_eq!(fs.protocol(), ["kafka+time"]);
        assert_eq!(
            names(fs.ls("kafka+time://orders", true).unwrap()),
            ["orders/weeks", "orders/1970"]
        );
        assert_eq!(
            names(fs.ls("orders/1970", true).unwrap()),
            ["orders/1970/01"]
        );
        assert_eq!(
            names(fs.ls("orders/1970/01/01/00/00", true).unwrap()),
            ["orders/1970/01/01/00/00/01"]
        );
        let leaves = names(fs.ls("orders/1970/01/01/00/00/01", true).unwrap());
        assert_eq!(leaves.len(), 25);
        assert_eq!(leaves[5], "orders/1970/01/01/00/00/01/005.arrow");
        let info = fs.info("orders/1970/01/01").unwrap();
        assert!(info.is_dir());
        assert_eq!(info.extra["kind"], "bucket");
        assert_eq!(info.extra["start"], "1970-01-01T00:00:00.000Z");
        assert_eq!(info.extra["end"], "1970-01-02T00:00:00.000Z");
        assert_eq!(
            names(fs.ls("orders/weeks", true).unwrap()),
            ["orders/weeks/1970-W01.arrow"]
        );
        assert_eq!(
            names(fs.ls("events.csv", true).unwrap())[1],
            "events.csv/1970"
        );
        assert!(fs.ls("orders/1971", true).unwrap().is_empty());
        for path in [
            "orders/1970/01/01/00/00/01/005",
            "orders/weeks/1970-W01",
            "missing/1970",
        ] {
            assert!(matches!(fs.info(path), Err(FsError::NotFound(_))), "{path}");
        }

        let fs = time_fs(TimeLevel::Day);
        assert_eq!(
            names(fs.ls("orders/1970/01", true).unwrap()),
            ["orders/1970/01/01.arrow"]
        );
        assert!(matches!(
            fs.info("orders/1970/01/01/00"),
            Err(FsError::NotFound(_))
        ));
        assert!(fs.info("orders/1970/01/01.parquet").unwrap().is_file());
    }

    #[test]
    fn time_buckets_read_their_messages() {
        let fs = time_fs(TimeLevel::Millisecond);
        assert_eq!(
            offsets(&read_arrow(&fs, "orders/1970/01/01/00/00/01/005.arrow")),
            [5]
        );
        assert_eq!(
            offsets(&read_arrow(&fs, "orders/1970.arrow")),
            (0..25).collect::<Vec<_>>()
        );
        assert_eq!(
            offsets(&read_arrow(&fs, "orders/weeks/1970-W01.arrow")),
            (0..25).collect::<Vec<_>>()
        );
        assert!(read_arrow(&fs, "orders/1971.arrow").is_empty());
        assert!(matches!(
            fs.open("orders/1970", OpenMode::Read, None),
            Err(FsError::IsADirectory(_))
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
        for path in ["", "orders/0", "orders/0/0-9.arrow"] {
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
        for path in ["", "orders/0/0-10.arrow", "orders/records"] {
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

        for path in ["orders/0/0-9.arrow", "orders/0.arrow?start=3"] {
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
