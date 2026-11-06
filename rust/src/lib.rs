use std::collections::HashMap;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::util::get_rdkafka_version;

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
pub fn list_topics(
    config: &HashMap<String, String>,
    timeout: Duration,
) -> KafkaResult<Vec<String>> {
    let mut client_config = ClientConfig::new();
    for (key, value) in config {
        client_config.set(key, value);
    }
    let consumer: BaseConsumer = client_config.create()?;
    let metadata = consumer.fetch_metadata(None, timeout)?;
    Ok(metadata
        .topics()
        .iter()
        .map(|topic| topic.name().to_owned())
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
