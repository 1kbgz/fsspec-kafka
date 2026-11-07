use std::collections::HashMap;
use std::time::Duration;

use fsspec_rs::FsResult;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::util::get_rdkafka_version;

mod backend;
mod fs;
mod path;
mod read;
mod records;
mod stream;
mod write;

pub use backend::{
    KafkaBackend, LiveMessages, Message, MessageOffset, MessageStream, NewMessage, PartitionInfo,
    PartitionRange, RdKafkaBackend, TopicInfo, Watermarks,
};
pub use fs::{windows, KafkaFs, KafkaFsOptions};
pub use path::{KafkaPath, MetaFile, Window};
pub use read::{message_schema, EncodedFile, MessageBatches, ValueFormat};
pub use records::{RecordIndex, RecordLocation, RECORDS_TOPIC_CONFIG};
pub use stream::{MessageTail, StreamStart};
pub use write::{MessageColumns, MessageWriter};

/// Version of the linked librdkafka.
pub fn librdkafka_version() -> String {
    get_rdkafka_version().1
}

/// Features compiled into the linked librdkafka (`builtin.features`).
pub fn builtin_features() -> KafkaResult<Vec<String>> {
    let config = ClientConfig::new().create_native_config()?;
    Ok(config
        .get("builtin.features")?
        .split(',')
        .map(str::to_owned)
        .collect())
}

/// Topic names in the cluster. `config` holds librdkafka properties.
pub fn list_topics(config: &HashMap<String, String>, timeout: Duration) -> FsResult<Vec<String>> {
    Ok(RdKafkaBackend::new(config, timeout)?
        .list_topics()?
        .into_iter()
        .map(|topic| topic.name)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_version() {
        assert!(librdkafka_version().starts_with("2."));
    }

    #[test]
    fn reports_required_features() {
        let features = builtin_features().unwrap();
        for feature in [
            "ssl",
            "sasl_gssapi",
            "sasl_scram",
            "sasl_oauthbearer",
            "zstd",
        ] {
            assert!(
                features.iter().any(|f| f == feature),
                "missing {feature}: {features:?}"
            );
        }
    }
}
