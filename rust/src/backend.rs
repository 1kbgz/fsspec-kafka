use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use fsspec_rs::{FsError, FsResult};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, Message as _};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaRespErr;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
}

/// Offsets bounding a partition: `low` is the first retained offset and `high` is the
/// offset the next message will receive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Watermarks {
    pub low: i64,
    pub high: i64,
}

/// Offsets `start..end` of one partition to read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionRange {
    pub partition: i32,
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Message {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub value: Option<Vec<u8>>,
}

pub type MessageStream = Box<dyn Iterator<Item = FsResult<Message>> + Send>;

/// Cluster operations used by `KafkaFs`.
pub trait KafkaBackend: Send + Sync {
    fn list_topics(&self) -> FsResult<Vec<TopicInfo>>;

    /// Fails with `FsError::NotFound` when the topic does not exist.
    fn topic(&self, topic: &str) -> FsResult<TopicInfo>;

    fn watermarks(&self, topic: &str, partition: i32) -> FsResult<Watermarks>;

    /// First offset whose timestamp is at or after `timestamp_ms`, or `None` when no such
    /// message exists yet.
    fn offset_for_time(
        &self,
        topic: &str,
        partition: i32,
        timestamp_ms: i64,
    ) -> FsResult<Option<i64>>;

    fn topic_config(&self, topic: &str) -> FsResult<BTreeMap<String, String>>;

    /// Messages in `ranges`. Each partition stops at its range end or at the end of the
    /// partition, whichever comes first, so offsets missing from compacted or
    /// transactional partitions do not block the read.
    fn read(&self, topic: &str, ranges: Vec<PartitionRange>) -> FsResult<MessageStream>;
}

/// `KafkaBackend` over librdkafka. Reads never join a consumer group or commit offsets.
pub struct RdKafkaBackend {
    config: ClientConfig,
    consumer: BaseConsumer,
    admin: AdminClient<DefaultClientContext>,
    timeout: Duration,
}

impl RdKafkaBackend {
    /// `config` holds librdkafka properties such as `bootstrap.servers`.
    pub fn new(config: &HashMap<String, String>, timeout: Duration) -> FsResult<Self> {
        let mut client_config = ClientConfig::new();
        for (key, value) in config {
            client_config.set(key, value);
        }
        client_config
            .set("enable.auto.commit", "false")
            .set("allow.auto.create.topics", "false");
        Ok(Self {
            consumer: client_config.create().map_err(kafka_error)?,
            admin: client_config.create().map_err(kafka_error)?,
            config: client_config,
            timeout,
        })
    }
}

impl KafkaBackend for RdKafkaBackend {
    fn list_topics(&self) -> FsResult<Vec<TopicInfo>> {
        let metadata = self
            .consumer
            .fetch_metadata(None, self.timeout)
            .map_err(kafka_error)?;
        let mut topics = metadata
            .topics()
            .iter()
            .filter(|topic| topic.error().is_none())
            .map(topic_info)
            .collect::<Vec<_>>();
        topics.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(topics)
    }

    fn topic(&self, topic: &str) -> FsResult<TopicInfo> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(topic), self.timeout)
            .map_err(kafka_error)?;
        let found = metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .ok_or_else(|| FsError::NotFound(topic.to_string()))?;
        match found.error() {
            None => Ok(topic_info(found)),
            Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) => {
                Err(FsError::NotFound(topic.to_string()))
            }
            Some(error) => Err(FsError::Other(format!("topic {topic}: {error:?}"))),
        }
    }

    fn watermarks(&self, topic: &str, partition: i32) -> FsResult<Watermarks> {
        let (low, high) = self
            .consumer
            .fetch_watermarks(topic, partition, self.timeout)
            .map_err(kafka_error)?;
        Ok(Watermarks { low, high })
    }

    fn offset_for_time(
        &self,
        topic: &str,
        partition: i32,
        timestamp_ms: i64,
    ) -> FsResult<Option<i64>> {
        let mut request = TopicPartitionList::new();
        request
            .add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))
            .map_err(kafka_error)?;
        let response = self
            .consumer
            .offsets_for_times(request, self.timeout)
            .map_err(kafka_error)?;
        let element = response
            .find_partition(topic, partition)
            .ok_or_else(|| FsError::NotFound(format!("{topic}/{partition}")))?;
        element.error().map_err(kafka_error)?;
        Ok(match element.offset() {
            Offset::Offset(offset) => Some(offset),
            _ => None,
        })
    }

    fn read(&self, topic: &str, ranges: Vec<PartitionRange>) -> FsResult<MessageStream> {
        self.read_messages(topic, ranges)
    }

    fn topic_config(&self, topic: &str) -> FsResult<BTreeMap<String, String>> {
        let options = AdminOptions::new().request_timeout(Some(self.timeout));
        let results = futures_executor::block_on(
            self.admin
                .describe_configs(&[ResourceSpecifier::Topic(topic)], &options),
        )
        .map_err(kafka_error)?;
        let resource = results
            .into_iter()
            .next()
            .ok_or_else(|| FsError::NotFound(topic.to_string()))?
            .map_err(|code| match code {
                rdkafka::types::RDKafkaErrorCode::UnknownTopicOrPartition => {
                    FsError::NotFound(topic.to_string())
                }
                code => FsError::Other(format!("topic {topic} config: {code}")),
            })?;
        Ok(resource
            .entries
            .into_iter()
            .filter_map(|entry| entry.value.map(|value| (entry.name, value)))
            .collect())
    }
}

impl RdKafkaBackend {
    fn read_messages(&self, topic: &str, ranges: Vec<PartitionRange>) -> FsResult<MessageStream> {
        let ranges = ranges
            .into_iter()
            .filter(|range| range.start < range.end)
            .collect::<Vec<_>>();
        if ranges.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let mut config = self.config.clone();
        // librdkafka requires a group id to assign partitions. Assignment never joins the
        // group, and offsets are never committed, so the group is not created on the broker.
        if config.get("group.id").is_none() {
            config.set("group.id", "fsspec-kafka");
        }
        let consumer: BaseConsumer = config
            .set("enable.partition.eof", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(kafka_error)?;
        let mut assignment = TopicPartitionList::new();
        for range in &ranges {
            assignment
                .add_partition_offset(topic, range.partition, Offset::Offset(range.start))
                .map_err(kafka_error)?;
        }
        consumer.assign(&assignment).map_err(kafka_error)?;
        Ok(Box::new(RdKafkaMessages {
            consumer,
            topic: topic.to_string(),
            ends: ranges.iter().map(|r| (r.partition, r.end)).collect(),
            timeout: self.timeout,
        }))
    }
}

struct RdKafkaMessages {
    consumer: BaseConsumer,
    topic: String,
    /// Exclusive end offset of each partition still being read.
    ends: HashMap<i32, i64>,
    timeout: Duration,
}

impl RdKafkaMessages {
    fn finish_partition(&mut self, partition: i32) {
        self.ends.remove(&partition);
        let mut paused = TopicPartitionList::new();
        paused.add_partition(&self.topic, partition);
        let _ = self.consumer.pause(&paused);
    }
}

impl Iterator for RdKafkaMessages {
    type Item = FsResult<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.ends.is_empty() {
            let message = match self.consumer.poll(self.timeout) {
                None => {
                    self.ends.clear();
                    return Some(Err(FsError::Other(format!(
                        "timed out reading {} after {:?}",
                        self.topic, self.timeout
                    ))));
                }
                Some(Err(KafkaError::PartitionEOF(partition))) => {
                    self.finish_partition(partition);
                    continue;
                }
                Some(Err(error)) => {
                    self.ends.clear();
                    return Some(Err(kafka_error(error)));
                }
                Some(Ok(message)) => owned_message(&message),
            };
            let Some(&end) = self.ends.get(&message.partition) else {
                continue;
            };
            if message.offset >= end {
                self.finish_partition(message.partition);
                continue;
            }
            if message.offset + 1 >= end {
                self.finish_partition(message.partition);
            }
            return Some(Ok(message));
        }
        None
    }
}

fn owned_message(message: &rdkafka::message::BorrowedMessage<'_>) -> Message {
    Message {
        partition: message.partition(),
        offset: message.offset(),
        timestamp_ms: message.timestamp().to_millis(),
        key: message.key().map(<[u8]>::to_vec),
        headers: message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|header| (header.key.to_string(), header.value.map(<[u8]>::to_vec)))
                    .collect()
            })
            .unwrap_or_default(),
        value: message.payload().map(<[u8]>::to_vec),
    }
}

fn topic_info(topic: &rdkafka::metadata::MetadataTopic) -> TopicInfo {
    let mut partitions = topic
        .partitions()
        .iter()
        .map(|partition| PartitionInfo {
            id: partition.id(),
            leader: partition.leader(),
            replicas: partition.replicas().to_vec(),
            isr: partition.isr().to_vec(),
        })
        .collect::<Vec<_>>();
    partitions.sort_by_key(|partition| partition.id);
    TopicInfo {
        name: topic.name().to_string(),
        partitions,
    }
}

fn kafka_error(error: KafkaError) -> FsError {
    FsError::Other(error.to_string())
}
