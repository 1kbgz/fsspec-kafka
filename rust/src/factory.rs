use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use fsspec_data::DataFormat;
use fsspec_rs::{
    FilesystemFactory, FilesystemRegistry, FsError, FsOptions, FsResult, SharedFileSystem,
};

use crate::{KafkaFs, KafkaFsOptions, KafkaView, RdKafkaBackend, TimeLevel};

/// Builds [`KafkaFs`] over librdkafka from string options, like the Python storage options.
///
/// The factory's view sets [`KafkaFsOptions::view`]. `timeout` (seconds), `time_bucket`,
/// `window_size`, `list_format`, `batch_size`, `allow_admin`, `partitions`, `replication`,
/// `key_column`, `partition_column`, `timestamp_column`, and `headers_column` set
/// [`KafkaFsOptions`]. Keys starting with `topic_config.` set the
/// configuration of created topics. `value_format` accepts only `binary`, since decoded
/// values need a schema; construct [`KafkaFs`] directly for those. Every other key is a
/// librdkafka property, such as `bootstrap.servers`.
pub struct KafkaFsFactory(pub KafkaView);

impl FilesystemFactory for KafkaFsFactory {
    fn create(&self, _path: &str, options: &FsOptions) -> FsResult<SharedFileSystem> {
        let mut config = HashMap::new();
        let mut timeout = Duration::from_secs(10);
        let mut fs_options = KafkaFsOptions {
            view: self.0,
            ..KafkaFsOptions::default()
        };
        for (key, value) in options {
            match key.as_str() {
                "timeout" => {
                    timeout = Duration::try_from_secs_f64(parse(key, value)?).map_err(|_| {
                        FsError::InvalidArgument("timeout must be a non-negative number".into())
                    })?
                }
                "time_bucket" => fs_options.time_bucket = TimeLevel::parse(value)?,
                "window_size" => fs_options.window_size = parse(key, value)?,
                "list_format" => {
                    fs_options.list_format = DataFormat::parse(value)
                        .map_err(|e| FsError::InvalidArgument(e.to_string()))?
                }
                "batch_size" => fs_options.batch_size = parse(key, value)?,
                "allow_admin" => fs_options.allow_admin = parse(key, value)?,
                "partitions" => fs_options.partitions = parse(key, value)?,
                "replication" => fs_options.replication = parse(key, value)?,
                "key_column" => fs_options.columns.key = Some(value.clone()),
                "partition_column" => fs_options.columns.partition = Some(value.clone()),
                "timestamp_column" => fs_options.columns.timestamp = Some(value.clone()),
                "headers_column" => fs_options.columns.headers = Some(value.clone()),
                "value_format" if value == "binary" => {}
                "value_format" | "value_schema" | "flatten_value" => {
                    return Err(FsError::NotSupported(format!(
                        "{key} needs a value schema; construct KafkaFs with KafkaFsOptions instead"
                    )))
                }
                _ => match key.strip_prefix("topic_config.") {
                    Some(name) => {
                        fs_options
                            .topic_config
                            .insert(name.to_string(), value.clone());
                    }
                    None => {
                        config.insert(key.clone(), value.clone());
                    }
                },
            }
        }
        Ok(Arc::new(KafkaFs::new(
            RdKafkaBackend::new(&config, timeout)?,
            fs_options,
        )?))
    }
}

/// Every Kafka protocol and the view it opens. `kafka` is `kafka+offset`.
pub const PROTOCOLS: [(&str, KafkaView); 5] = [
    ("kafka", KafkaView::Offset),
    ("kafka+offset", KafkaView::Offset),
    ("kafka+time", KafkaView::Time),
    ("kafka+records", KafkaView::Records),
    ("kafka+history", KafkaView::History),
];

/// Registers [`KafkaFsFactory`] for every Kafka protocol.
pub fn register(registry: &mut FilesystemRegistry) {
    for (protocol, view) in PROTOCOLS {
        registry.register(protocol, KafkaFsFactory(view));
    }
}

fn parse<T: FromStr>(key: &str, value: &str) -> FsResult<T> {
    value
        .parse()
        .map_err(|_| FsError::InvalidArgument(format!("invalid {key}: {value:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> FsOptions {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn error(pairs: &[(&str, &str)]) -> String {
        match KafkaFsFactory(KafkaView::Offset).create("", &options(pairs)) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected an error"),
        }
    }

    #[test]
    fn registry_resolves_kafka_urls() {
        let mut registry = FilesystemRegistry::empty();
        register(&mut registry);
        let settings = options(&[
            ("bootstrap.servers", "localhost:1"),
            ("window_size", "100"),
            ("allow_admin", "true"),
            ("topic_config.retention.ms", "1000"),
        ]);

        let (fs, path) = registry.url_to_fs("kafka://orders/0", &settings).unwrap();
        let (same, _) = registry.url_to_fs("kafka://events", &settings).unwrap();

        assert_eq!(path, "orders/0");
        assert_eq!(fs.protocol(), ["kafka", "kafka+offset"]);
        assert!(Arc::ptr_eq(&fs, &same));
        for (protocol, view) in PROTOCOLS {
            let (fs, path) = registry
                .url_to_fs(&format!("{protocol}://orders/2026"), &settings)
                .unwrap();
            assert_eq!(path, "orders/2026");
            assert_eq!(fs.protocol(), view.protocols());
        }
    }

    #[test]
    fn rejects_invalid_options() {
        assert_eq!(
            error(&[("window_size", "big")]),
            "invalid argument: invalid window_size: \"big\""
        );
        assert_eq!(
            error(&[("window_size", "0")]),
            "invalid argument: window_size and batch_size must be positive"
        );
        assert_eq!(
            error(&[("timeout", "-1")]),
            "invalid argument: timeout must be a non-negative number"
        );
        assert!(error(&[("list_format", "xlsx")]).starts_with("invalid argument: "));
        assert!(error(&[("time_bucket", "week")]).starts_with("invalid argument: time_bucket"));
        assert!(error(&[("value_format", "json")])
            .starts_with("not supported: value_format needs a value schema"));
        assert!(!error(&[("no.such.property", "1")]).is_empty());
    }
}
