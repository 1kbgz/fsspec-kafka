use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use fsspec_rs::{FsError, FsResult};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
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
}

/// `KafkaBackend` over librdkafka. Reads never join a consumer group or commit offsets.
pub struct RdKafkaBackend {
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
