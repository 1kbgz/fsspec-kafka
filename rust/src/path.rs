use fsspec_data::DataFormat;
use fsspec_rs::{FsError, FsResult};

pub const META_DIR: &str = "_meta";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetaFile {
    Config,
    Partitions,
    Watermarks,
}

impl MetaFile {
    pub const ALL: [MetaFile; 3] = [Self::Config, Self::Partitions, Self::Watermarks];

    pub fn file_name(self) -> &'static str {
        match self {
            Self::Config => "config.json",
            Self::Partitions => "partitions.json",
            Self::Watermarks => "watermarks.json",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|meta| meta.file_name() == name)
    }
}

/// An inclusive offset range. `end` is `None` for the head window, which ends at the
/// high watermark captured when it is read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Window {
    pub start: i64,
    pub end: Option<i64>,
}

impl Window {
    fn name(self, format: DataFormat) -> String {
        match self.end {
            Some(end) => format!("{}-{end}.{}", self.start, format.as_str()),
            None => format!("{}-.{}", self.start, format.as_str()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KafkaPath {
    Root,
    Topic(String),
    MetaDir(String),
    Meta(String, MetaFile),
    Partition(String, i32),
    Window {
        topic: String,
        partition: i32,
        window: Window,
        format: DataFormat,
    },
    /// `topic/partition.format?start=&limit=`
    PartitionRead {
        topic: String,
        partition: i32,
        format: DataFormat,
        start: Option<i64>,
        limit: Option<u64>,
    },
    /// `topic.format`: every partition of the topic.
    TopicSnapshot {
        topic: String,
        format: DataFormat,
    },
}

impl KafkaPath {
    pub fn parse(path: &str) -> FsResult<Self> {
        let path = path.strip_prefix("kafka://").unwrap_or(path);
        let (path, query) = match path.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (path, None),
        };
        let segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let parsed = match segments.as_slice() {
            [] => Self::Root,
            [name] => match split_format(name) {
                Some((topic, format)) => Self::TopicSnapshot {
                    topic: topic_name(topic)?,
                    format,
                },
                None => Self::Topic(topic_name(name)?),
            },
            [topic, META_DIR] => Self::MetaDir(topic_name(topic)?),
            [topic, META_DIR, file] => Self::Meta(
                topic_name(topic)?,
                MetaFile::parse(file).ok_or_else(|| invalid(path))?,
            ),
            [topic, name] => match split_format(name) {
                Some((partition, format)) => {
                    let (start, limit) = parse_query(query.unwrap_or(""))?;
                    return Ok(Self::PartitionRead {
                        topic: topic_name(topic)?,
                        partition: partition_id(partition).ok_or_else(|| invalid(path))?,
                        format,
                        start,
                        limit,
                    });
                }
                None => Self::Partition(
                    topic_name(topic)?,
                    partition_id(name).ok_or_else(|| invalid(path))?,
                ),
            },
            [topic, partition, name] => {
                let (window, format) = split_format(name)
                    .and_then(|(range, format)| Some((parse_window(range)?, format)))
                    .ok_or_else(|| invalid(path))?;
                Self::Window {
                    topic: topic_name(topic)?,
                    partition: partition_id(partition).ok_or_else(|| invalid(path))?,
                    window,
                    format,
                }
            }
            _ => return Err(invalid(path)),
        };
        if query.is_some() {
            return Err(FsError::InvalidArgument(format!(
                "query parameters are only accepted on partition reads: {path}"
            )));
        }
        Ok(parsed)
    }

    pub fn to_path(&self) -> String {
        match self {
            Self::Root => String::new(),
            Self::Topic(topic) => topic.clone(),
            Self::MetaDir(topic) => format!("{topic}/{META_DIR}"),
            Self::Meta(topic, file) => format!("{topic}/{META_DIR}/{}", file.file_name()),
            Self::Partition(topic, partition) => format!("{topic}/{partition}"),
            Self::Window {
                topic,
                partition,
                window,
                format,
            } => format!("{topic}/{partition}/{}", window.name(*format)),
            Self::PartitionRead {
                topic,
                partition,
                format,
                start,
                limit,
            } => {
                let query = [
                    start.map(|start| format!("start={start}")),
                    limit.map(|limit| format!("limit={limit}")),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join("&");
                let path = format!("{topic}/{partition}.{}", format.as_str());
                if query.is_empty() {
                    path
                } else {
                    format!("{path}?{query}")
                }
            }
            Self::TopicSnapshot { topic, format } => format!("{topic}.{}", format.as_str()),
        }
    }
}

fn invalid(path: &str) -> FsError {
    FsError::InvalidArgument(format!("not a Kafka path: {path}"))
}

/// Kafka topic names are 1 to 249 characters from `[a-zA-Z0-9._-]`.
fn topic_name(name: &str) -> FsResult<String> {
    let valid = !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if valid {
        Ok(name.to_string())
    } else {
        Err(FsError::InvalidArgument(format!(
            "invalid topic name: {name:?}"
        )))
    }
}

fn partition_id(value: &str) -> Option<i32> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn offset(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn split_format(name: &str) -> Option<(&str, DataFormat)> {
    let (stem, extension) = name.rsplit_once('.')?;
    let format = match extension {
        "arrow" | "ipc" => DataFormat::Arrow,
        "parquet" | "pq" => DataFormat::Parquet,
        "csv" => DataFormat::Csv,
        "jsonl" | "ndjson" => DataFormat::JsonLines,
        _ => return None,
    };
    Some((stem, format))
}

fn parse_window(range: &str) -> Option<Window> {
    let (start, end) = range.split_once('-')?;
    let start = offset(start)?;
    let end = if end.is_empty() {
        None
    } else {
        Some(offset(end)?).filter(|end| *end >= start)
    };
    if end.is_none() && !range.ends_with('-') {
        return None;
    }
    Some(Window { start, end })
}

fn parse_query(query: &str) -> FsResult<(Option<i64>, Option<u64>)> {
    let mut start = None;
    let mut limit = None;
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let bad = || FsError::InvalidArgument(format!("invalid query parameter: {pair}"));
        match key {
            "start" => start = Some(offset(value).ok_or_else(bad)?),
            "limit" => limit = Some(value.parse::<u64>().map_err(|_| bad())?),
            _ => return Err(bad()),
        }
    }
    Ok((start, limit))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(path: &str, expected: KafkaPath) {
        let parsed = KafkaPath::parse(path).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(KafkaPath::parse(&parsed.to_path()).unwrap(), expected);
    }

    #[test]
    fn parses_directories() {
        round_trip("", KafkaPath::Root);
        round_trip("kafka://", KafkaPath::Root);
        round_trip("orders", KafkaPath::Topic("orders".into()));
        round_trip("kafka://orders/", KafkaPath::Topic("orders".into()));
        round_trip("orders/_meta", KafkaPath::MetaDir("orders".into()));
        round_trip("orders/3", KafkaPath::Partition("orders".into(), 3));
    }

    #[test]
    fn parses_meta_files() {
        for meta in MetaFile::ALL {
            round_trip(
                &format!("orders/_meta/{}", meta.file_name()),
                KafkaPath::Meta("orders".into(), meta),
            );
        }
    }

    #[test]
    fn parses_windows() {
        round_trip(
            "orders/0/10000-19999.parquet",
            KafkaPath::Window {
                topic: "orders".into(),
                partition: 0,
                window: Window {
                    start: 10000,
                    end: Some(19999),
                },
                format: DataFormat::Parquet,
            },
        );
        round_trip(
            "orders/0/20000-.arrow",
            KafkaPath::Window {
                topic: "orders".into(),
                partition: 0,
                window: Window {
                    start: 20000,
                    end: None,
                },
                format: DataFormat::Arrow,
            },
        );
    }

    #[test]
    fn parses_reads_and_snapshots() {
        round_trip(
            "orders/1.jsonl?start=5000&limit=100",
            KafkaPath::PartitionRead {
                topic: "orders".into(),
                partition: 1,
                format: DataFormat::JsonLines,
                start: Some(5000),
                limit: Some(100),
            },
        );
        round_trip(
            "orders/1.csv",
            KafkaPath::PartitionRead {
                topic: "orders".into(),
                partition: 1,
                format: DataFormat::Csv,
                start: None,
                limit: None,
            },
        );
        round_trip(
            "orders.v1.parquet",
            KafkaPath::TopicSnapshot {
                topic: "orders.v1".into(),
                format: DataFormat::Parquet,
            },
        );
        round_trip("orders.v1", KafkaPath::Topic("orders.v1".into()));
    }

    #[test]
    fn accepts_extension_aliases() {
        assert_eq!(
            KafkaPath::parse("orders.ndjson").unwrap(),
            KafkaPath::TopicSnapshot {
                topic: "orders".into(),
                format: DataFormat::JsonLines,
            }
        );
    }

    #[test]
    fn rejects_invalid_paths() {
        for path in [
            "bad topic",
            "orders/-1",
            "orders/x",
            "orders/_meta/other.json",
            "orders/0/10-5.arrow",
            "orders/0/10.arrow",
            "orders/0/a-b.arrow",
            "orders/0/10-20.txt",
            "orders/0/10-20.arrow/extra",
            "orders?start=1",
            "orders/0.arrow?offset=1",
            "orders/0.arrow?limit=-1",
        ] {
            assert!(
                matches!(KafkaPath::parse(path), Err(FsError::InvalidArgument(_))),
                "{path}"
            );
        }
    }
}
